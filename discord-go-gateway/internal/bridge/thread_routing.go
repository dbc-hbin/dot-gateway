package bridge

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/bwmarrin/discordgo"
)

// These types intentionally require explicit archived/locked metadata. Missing
// booleans are not proof of an active, writable thread.
type ThreadMetadata struct {
	Archived *bool `json:"archived"`
	Locked   *bool `json:"locked"`
}
type ThreadMember struct {
	ID     string `json:"id"`
	UserID string `json:"user_id"`
}
type PermissionOverwrite struct {
	ID    string `json:"id"`
	Type  int    `json:"type"`
	Allow string `json:"allow"`
	Deny  string `json:"deny"`
}
type guildRole struct {
	ID          string `json:"id"`
	Permissions string `json:"permissions"`
}
type guildMember struct {
	User    User       `json:"user"`
	Roles   []string   `json:"roles"`
	Timeout *time.Time `json:"communication_disabled_until"`
}

const (
	permissionAdministrator uint64 = 1 << 3
	permissionAddReactions  uint64 = 1 << 6
	permissionViewChannel   uint64 = 1 << 10
	permissionReadHistory   uint64 = 1 << 16
	permissionManageThreads uint64 = 1 << 34
	permissionSendThreads   uint64 = 1 << 38
)

func guildRoutePermissions(s *discordgo.Session, c Settings, e Envelope) bool {
	// Owner/recipient binding is enforced by Policy and REST ValidateChannel.
	// An independent DM never requires access to the configured guild.
	if e.RouteKind == "dm" {
		return true
	}
	if c.Policy.GuildID == "" || !guildPermissions(s, c) {
		return false
	}
	p, err := s.State.UserChannelPermissions(c.ExpectedBotID, c.Policy.GuildChannelID)
	need := int64(discordgo.PermissionSendMessages)
	if e.RouteKind == "guild_thread" {
		need = discordgo.PermissionSendMessagesInThreads
	}
	return err == nil && (p&discordgo.PermissionAdministrator != 0 || p&need == need)
}

func (r *RESTClient) validateIngressRoute(ctx context.Context, c Channel, e Envelope) (Envelope, error) {
	if e.RouteKind != "guild_thread_candidate" {
		return e, r.validateRoute(ctx, c, e)
	}
	if !r.settings.Policy.Stages(e) {
		return e, errors.New("preflight_channel_mismatch")
	}
	verified := e
	verified.RouteKind, verified.ParentChannelID, verified.ThreadType = "guild_thread", c.ParentID, c.Type
	if !utf8.ValidString(c.Name) || utf8.RuneCountInString(c.Name) > 100 {
		return e, errors.New("preflight_channel_mismatch")
	}
	verified.ThreadName = c.Name
	if err := r.validateRoute(ctx, c, verified); err != nil {
		return e, err
	}
	return verified, nil
}

func (r *RESTClient) validateRoute(ctx context.Context, c Channel, e Envelope) error {
	if err := r.ValidateChannel(c, e); err != nil {
		return err
	}
	if r.routePermission != nil && !r.routePermission(e) {
		return errors.New("preflight_route_permissions_missing")
	}
	if e.RouteKind == "guild_thread" {
		return r.validateThreadRoute(ctx, c, e, 0)
	}
	return nil
}

func (r *RESTClient) validateThreadRoute(ctx context.Context, c Channel, e Envelope, extra uint64) error {
	if err := r.ValidateChannel(c, e); err != nil {
		return err
	}
	if c.ThreadMetadata == nil || c.ThreadMetadata.Archived == nil || c.ThreadMetadata.Locked == nil {
		return errors.New("preflight_thread_metadata_missing")
	}
	if *c.ThreadMetadata.Archived {
		return errors.New("preflight_thread_archived")
	}
	// Never implicitly join a private thread, even if the bot has administrator
	// or MANAGE_THREADS. A fresh GET must show its existing membership.
	if c.Type == 12 && (c.Member == nil || c.Member.ID != c.ID || c.Member.UserID != r.settings.ExpectedBotID) {
		return errors.New("preflight_thread_membership_required")
	}
	parent, err := r.Channel(ctx, r.settings.Policy.GuildChannelID)
	if err != nil {
		return err
	}
	if !Snowflake(parent.GuildID) {
		return errors.New("preflight_channel_invalid")
	}
	if parent.PermissionOverwrites == nil {
		return errors.New("preflight_permissions_invalid")
	}
	if parent.Type != 0 || parent.GuildID != r.settings.Policy.GuildID || parent.ID != c.ParentID {
		return errors.New("preflight_channel_mismatch")
	}
	permissions, err := r.freshThreadPermissions(ctx, parent)
	if err != nil {
		return err
	}
	need := permissionViewChannel | permissionReadHistory | permissionSendThreads | extra
	if permissions&need != need {
		return errors.New("preflight_thread_permissions_missing")
	}
	// Permission/identity GETs can wait or be slow. Re-read the actual thread
	// after them so an archive, parent/type mutation or lost private membership
	// during those reads cannot turn the following write into an auto-unarchive.
	latest, err := r.Channel(ctx, e.ConversationID)
	if err != nil {
		return err
	}
	if err = r.ValidateChannel(latest, e); err != nil {
		return err
	}
	if latest.ThreadMetadata == nil || latest.ThreadMetadata.Archived == nil || latest.ThreadMetadata.Locked == nil {
		return errors.New("preflight_thread_metadata_missing")
	}
	if *latest.ThreadMetadata.Archived {
		return errors.New("preflight_thread_archived")
	}
	if latest.Type == 12 && (latest.Member == nil || latest.Member.ID != latest.ID || latest.Member.UserID != r.settings.ExpectedBotID) {
		return errors.New("preflight_thread_membership_required")
	}
	c = latest
	if *c.ThreadMetadata.Locked && permissions&permissionManageThreads == 0 {
		return errors.New("preflight_thread_locked")
	}
	return nil
}

func (r *RESTClient) freshThreadPermissions(ctx context.Context, parent Channel) (uint64, error) {
	// Read only fixed, policy-bound guild and bot IDs, never externally supplied
	// URLs. REST has no redirects and still enforces the fixed Discord origin.
	var member guildMember
	if err := r.get(ctx, "/guilds/"+r.settings.Policy.GuildID+"/members/"+r.settings.ExpectedBotID, &member); err != nil {
		return 0, err
	}
	if member.User.ID != r.settings.ExpectedBotID || !member.User.Bot || member.Roles == nil {
		return 0, errors.New("preflight_permissions_invalid")
	}
	var roles []guildRole
	if err := r.get(ctx, "/guilds/"+r.settings.Policy.GuildID+"/roles", &roles); err != nil {
		return 0, err
	}
	return effectiveThreadPermissions(parent, member, roles, time.Now())
}

func permissionBits(s string) (uint64, error) {
	if s == "" {
		return 0, errors.New("preflight_permissions_invalid")
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0, errors.New("preflight_permissions_invalid")
		}
	}
	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, errors.New("preflight_permissions_invalid")
	}
	return n, nil
}

func effectiveThreadPermissions(parent Channel, member guildMember, roles []guildRole, now time.Time) (uint64, error) {
	invalid := errors.New("preflight_permissions_invalid")
	assigned := map[string]bool{parent.GuildID: true}
	for _, id := range member.Roles {
		if !Snowflake(id) {
			return 0, invalid
		}
		assigned[id] = true
	}
	seen := map[string]bool{}
	var bits uint64
	for _, role := range roles {
		if !Snowflake(role.ID) || seen[role.ID] {
			return 0, invalid
		}
		seen[role.ID] = true
		value, err := permissionBits(role.Permissions)
		if err != nil {
			return 0, err
		}
		if assigned[role.ID] {
			bits |= value
		}
	}
	for id := range assigned {
		if !seen[id] {
			return 0, invalid
		}
	}
	// Validate the complete parent overwrite response before honoring admin.
	var everyoneAllow, everyoneDeny, roleAllow, roleDeny, memberAllow, memberDeny uint64
	overwritten := map[string]bool{}
	for _, overwrite := range parent.PermissionOverwrites {
		if !Snowflake(overwrite.ID) || (overwrite.Type != 0 && overwrite.Type != 1) {
			return 0, invalid
		}
		key := strconv.Itoa(overwrite.Type) + ":" + overwrite.ID
		if overwritten[key] {
			return 0, invalid
		}
		overwritten[key] = true
		allow, err := permissionBits(overwrite.Allow)
		if err != nil {
			return 0, err
		}
		deny, err := permissionBits(overwrite.Deny)
		if err != nil {
			return 0, err
		}
		if overwrite.Type == 0 && overwrite.ID == parent.GuildID {
			everyoneAllow, everyoneDeny = allow, deny
		} else if overwrite.Type == 0 && assigned[overwrite.ID] {
			roleAllow |= allow
			roleDeny |= deny
		} else if overwrite.Type == 1 && overwrite.ID == member.User.ID {
			memberAllow, memberDeny = allow, deny
		}
	}
	if bits&permissionAdministrator != 0 {
		return ^uint64(0), nil
	}
	bits = bits&^everyoneDeny | everyoneAllow
	bits = bits&^roleDeny | roleAllow
	bits = bits&^memberDeny | memberAllow
	if member.Timeout != nil && member.Timeout.After(now) {
		bits &= permissionViewChannel | permissionReadHistory
	}
	return bits, nil
}

type threadWriteContextKey struct{}

func (r *RESTClient) threadWriteContext(ctx context.Context, e Envelope, extra uint64) context.Context {
	if e.RouteKind != "guild_thread" {
		return ctx
	}
	return context.WithValue(ctx, threadWriteContextKey{}, func(check context.Context) error {
		c, err := r.Channel(check, e.ConversationID)
		if err != nil {
			return err
		}
		if r.routePermission != nil && !r.routePermission(e) {
			return errors.New("preflight_route_permissions_missing")
		}
		return r.validateThreadRoute(check, c, e, extra)
	})
}

func (r *RESTClient) waitWriteBudget(ctx context.Context, method, path string, measurement *requestMeasurement) error {
	for {
		waited, err := r.waitLimitObserved(ctx, method, path, measurement)
		if err != nil {
			return err
		}
		if !waited || method == http.MethodGet {
			return nil
		}
		if source, ok := ctx.Value(controlTargetContextKey{}).(Envelope); ok {
			if err := r.verifyReactionTarget(unmeasuredValidationContext{ctx}, source); err != nil {
				return err
			}
		}
		check, ok := ctx.Value(threadWriteContextKey{}).(func(context.Context) error)
		if !ok {
			return nil
		}
		// Sending auto-unarchives on Discord. Any budget wait invalidates the earlier
		// active/parent/permission observation. Recheck before attempting the write,
		// then honor any new rate limit learned while doing those read-only checks.
		if err := check(unmeasuredValidationContext{ctx}); err != nil {
			return err
		}
	}
}

// Read-only write-guard checks must not inherit the POST trace. Otherwise a
// successful GET could make a definitely unattempted POST appear attempted.
// Cancellation/deadline and the connection-epoch guard still propagate.
type unmeasuredValidationContext struct{ context.Context }

func (c unmeasuredValidationContext) Value(key any) any {
	if _, ok := key.(sendGuardContextKey); ok {
		return c.Context.Value(key)
	}
	return nil
}

func (p *PermissionOverwrite) UnmarshalJSON(raw []byte) error {
	var wire struct {
		ID    string `json:"id"`
		Type  *int   `json:"type"`
		Allow string `json:"allow"`
		Deny  string `json:"deny"`
	}
	if json.Unmarshal(raw, &wire) != nil || wire.Type == nil {
		return errors.New("preflight_permissions_invalid")
	}
	*p = PermissionOverwrite{ID: wire.ID, Type: *wire.Type, Allow: wire.Allow, Deny: wire.Deny}
	return nil
}
