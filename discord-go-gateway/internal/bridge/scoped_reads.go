package bridge

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/bwmarrin/discordgo"
)

// ReadRoute is an immutable, policy-bound channel. It grants no write authority.
// Callers obtain it only through ResolveReadRoute; every operation refreshes it.
type ReadRoute struct {
	ChannelID  string `json:"channel_id"`
	GuildID    string `json:"guild_id,omitempty"`
	Kind       string `json:"kind"`
	ParentID   string `json:"parent_id,omitempty"`
	ThreadType int    `json:"thread_type,omitempty"`
}

func (v ReadRoute) envelope() Envelope {
	return Envelope{ConversationID: v.ChannelID, GuildID: v.GuildID, RouteKind: v.Kind, ParentChannelID: v.ParentID, ThreadType: v.ThreadType}
}

// ResolveReadRoute does not discover DMs or enumerate threads. Besides the
// configured parent, the exact route must already occur in this ledger.
func (r *RESTClient) ResolveReadRoute(ctx context.Context, s *Store, channel string) (ReadRoute, error) {
	var route ReadRoute
	p := r.settings.Policy
	if !Snowflake(channel) {
		return route, errors.New("invalid_read_route")
	}
	if channel == p.GuildChannelID && p.GuildID != "" {
		route = ReadRoute{ChannelID: channel, GuildID: p.GuildID, Kind: "guild_text"}
	} else {
		v, err := s.call(func(db *storeConn) (any, error) {
			var raw string
			err := db.QueryRow(readRouteLookupSQL, channel, p.OwnerID).Scan(&raw)
			if err == sql.ErrNoRows {
				return nil, errors.New("read_route_not_verified")
			}
			if err != nil {
				return nil, err
			}
			var e Envelope
			if json.Unmarshal([]byte(raw), &e) != nil || (e.RouteKind != "dm" && e.RouteKind != "guild_thread") {
				return nil, errors.New("invalid_read_route")
			}
			return ReadRoute{e.ConversationID, e.GuildID, e.RouteKind, e.ParentChannelID, e.ThreadType}, nil
		})
		if err != nil {
			return route, err
		}
		route = v.(ReadRoute)
	}
	return route, r.validateReadRoute(ctx, route)
}

// READ authorization deliberately does not require send readiness, an active
// thread, or MANAGE_THREADS. Archived threads are readable without unarchiving.
func (r *RESTClient) validateReadRoute(ctx context.Context, route ReadRoute) error {
	if !Snowflake(route.ChannelID) {
		return errors.New("invalid_read_route")
	}
	if _, err := r.Identity(ctx); err != nil {
		return err
	}
	c, err := r.Channel(ctx, route.ChannelID)
	if err != nil {
		return err
	}
	if err = r.ValidateChannel(c, route.envelope()); err != nil {
		return err
	}
	if route.Kind == "dm" {
		return nil
	}
	parent := c
	if route.Kind == "guild_thread" {
		if err = readThreadMembership(c, r.settings.ExpectedBotID); err != nil {
			return err
		}
		parent, err = r.Channel(ctx, r.settings.Policy.GuildChannelID)
		if err != nil {
			return err
		}
	}
	if parent.ID != r.settings.Policy.GuildChannelID || parent.Type != 0 || parent.GuildID != r.settings.Policy.GuildID || parent.PermissionOverwrites == nil {
		return errors.New("read_parent_invalid")
	}
	snapshot, err := r.readPermissionSnapshot(ctx)
	if err != nil {
		return err
	}
	bits, err := effectiveThreadPermissions(parent, snapshot.member, snapshot.roles, time.Now())
	if err != nil {
		return err
	}
	need := permissionViewChannel | permissionReadHistory
	if bits&need != need {
		return errors.New("read_permissions_missing")
	}
	// The final observed parent can differ from the first one. Apply its actual
	// overwrites with the fresh validated member/roles; binding alone is not an
	// authorization check, especially after slow REST/rate-limit waits.
	latestParent, err := r.Channel(ctx, r.settings.Policy.GuildChannelID)
	if err != nil {
		return err
	}
	if latestParent.ID != parent.ID || latestParent.Type != 0 || latestParent.GuildID != parent.GuildID || latestParent.PermissionOverwrites == nil {
		return errors.New("read_parent_invalid")
	}
	bits, err = effectiveThreadPermissions(latestParent, snapshot.member, snapshot.roles, time.Now())
	if err != nil {
		return err
	}
	if bits&need != need {
		return errors.New("read_permissions_missing")
	}
	c = latestParent
	if route.Kind == "guild_thread" {
		c, err = r.Channel(ctx, route.ChannelID)
		if err != nil {
			return err
		}
	}
	if err = r.ValidateChannel(c, route.envelope()); err != nil {
		return err
	}
	if route.Kind == "guild_thread" {
		return readThreadMembership(c, r.settings.ExpectedBotID)
	}

	return nil
}
func readThreadMembership(c Channel, bot string) error {
	if c.ThreadMetadata == nil || c.ThreadMetadata.Archived == nil || c.ThreadMetadata.Locked == nil {
		return errors.New("read_thread_metadata_missing")
	}
	if c.Type == 12 && (c.Member == nil || c.Member.ID != c.ID || c.Member.UserID != bot) {
		return errors.New("read_thread_membership_required")
	}
	return nil
}

type ReadMessage struct {
	ID        string        `json:"message_id"`
	ChannelID string        `json:"channel_id"`
	AuthorID  string        `json:"author_id"`
	Text      string        `json:"text"`
	Media     MediaSnapshot `json:"media,omitempty"`
	EditedAt  *time.Time    `json:"edited_at,omitempty"`
	Type      int           `json:"type"`
	PinnedAt  string        `json:"pinned_at,omitempty"`
	Truncated bool          `json:"truncated,omitempty"`
}
type ReadPage struct {
	Trust      string        `json:"trust"`
	Provenance string        `json:"provenance"`
	Route      ReadRoute     `json:"route"`
	Messages   []ReadMessage `json:"messages"`
	NextBefore string        `json:"next_before,omitempty"`
	NextOffset int           `json:"next_offset,omitempty"`
	HasMore    bool          `json:"has_more"`
	Status     string        `json:"status"`
	RetryAfter float64       `json:"retry_after,omitempty"`
	Complete   bool          `json:"complete"`
}

func readPage(route ReadRoute, provenance string) ReadPage {
	return ReadPage{Trust: "untrusted_external_context", Provenance: provenance, Route: route, Messages: []ReadMessage{}, Status: "available"}
}
func readMessage(m *discordgo.Message) ReadMessage {
	text, truncated := boundedContextText(m.Content)
	return ReadMessage{ID: m.ID, ChannelID: m.ChannelID, AuthorID: m.Author.ID, Text: text, Media: ProjectMedia(m), EditedAt: m.EditedTimestamp, Type: int(m.Type), Truncated: truncated}
}
func snowLess(a, b string) bool {
	if len(a) != len(b) {
		return len(a) < len(b)
	}
	return a < b
}
func decodeReadMessage(raw json.RawMessage, route ReadRoute) (*discordgo.Message, error) {
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil || fields == nil {
		return nil, errors.New("read_message_invalid")
	}
	for _, key := range []string{"id", "channel_id", "author", "type", "content", "mentions", "attachments", "embeds"} {
		if val, ok := fields[key]; !ok || string(val) == "null" {
			return nil, errors.New("read_message_incomplete")
		}
	}
	var m discordgo.Message
	if json.Unmarshal(raw, &m) != nil || !Snowflake(m.ID) || m.ChannelID != route.ChannelID || (m.GuildID != "" && m.GuildID != route.GuildID) || m.Author == nil || !Snowflake(m.Author.ID) || !utf8.ValidString(m.Content) || TextUnits(m.Content) > 8000 || m.Mentions == nil || m.Attachments == nil || m.Embeds == nil {
		return nil, errors.New("read_message_invalid")
	}
	m.GuildID = route.GuildID
	return &m, nil
}
func (r *RESTClient) ReadMessage(ctx context.Context, s *Store, channel, id string) (ReadPage, error) {
	if !Snowflake(id) {
		return ReadPage{}, errors.New("invalid_message_id")
	}
	route, err := r.ResolveReadRoute(ctx, s, channel)
	if err != nil {
		return ReadPage{}, err
	}
	out := readPage(route, "discord_rest_exact")
	var raw json.RawMessage
	if err = r.get(ctx, "/channels/"+channel+"/messages/"+id, &raw); err != nil {
		return out, err
	}
	m, err := decodeReadMessage(raw, route)
	if err != nil {
		return out, err
	}
	if m.ID != id {
		return out, errors.New("read_message_mismatch")
	}
	if err = r.validateReadRoute(ctx, route); err != nil {
		return out, err
	}
	out.Messages = append(out.Messages, readMessage(m))
	out.Complete = true
	return out, nil
}
func (r *RESTClient) historyPage(ctx context.Context, route ReadRoute, before string, limit int) ([]*discordgo.Message, error) {
	if limit < 1 || limit > 100 || (before != "" && !Snowflake(before)) {
		return nil, errors.New("invalid_history_bounds")
	}
	if err := r.validateReadRoute(ctx, route); err != nil {
		return nil, err
	}
	q := url.Values{"limit": {strconv.Itoa(limit)}}
	if before != "" {
		q.Set("before", before)
	}
	var raw []json.RawMessage
	if err := r.get(ctx, "/channels/"+route.ChannelID+"/messages?"+q.Encode(), &raw); err != nil {
		// Distinguish this message-page body failure from an invalid identity
		// or permission proof. Only this endpoint may trigger page shrinking.
		if err.Error() == "preflight_invalid_ack" {
			return nil, errors.New("history_body_incomplete")
		}
		return nil, err
	}
	if raw == nil || len(raw) > limit {
		return nil, errors.New("read_page_invalid")
	}
	out := make([]*discordgo.Message, 0, len(raw))
	last := before
	for _, v := range raw {
		m, err := decodeReadMessage(v, route)
		if err != nil {
			return nil, err
		}
		if last != "" && !snowLess(m.ID, last) {
			return nil, errors.New("read_page_nonprogressing")
		}
		last = m.ID
		out = append(out, m)
	}
	if err := r.validateReadRoute(ctx, route); err != nil {
		return nil, err
	}
	return out, nil
}
func (r *RESTClient) ReadHistory(ctx context.Context, s *Store, channel, before string, limit int) (ReadPage, error) {
	if limit < 1 || limit > 100 || (before != "" && !Snowflake(before)) {
		return ReadPage{}, errors.New("invalid_history_bounds")
	}
	route, err := r.ResolveReadRoute(ctx, s, channel)
	if err != nil {
		return ReadPage{}, err
	}
	out := readPage(route, "discord_rest_history")
	messages, err := r.historyPage(ctx, route, before, limit)
	if err != nil {
		return out, err
	}
	for _, m := range messages {
		out.Messages = append(out.Messages, readMessage(m))
	}
	out.HasMore = len(messages) == limit
	out.Complete = !out.HasMore
	if len(messages) > 0 {
		out.NextBefore = messages[len(messages)-1].ID
	}
	return out, nil
}
func (r *RESTClient) ReadPins(ctx context.Context, s *Store, channel, before string, limit int) (ReadPage, error) {
	var boundary time.Time
	var err error
	if limit < 1 || limit > 50 || len(before) > 64 {
		return ReadPage{}, errors.New("invalid_pins_bounds")
	}
	if before != "" {
		boundary, err = time.Parse(time.RFC3339Nano, before)
		if err != nil {
			return ReadPage{}, errors.New("invalid_pins_bounds")
		}
	}
	route, err := r.ResolveReadRoute(ctx, s, channel)
	if err != nil {
		return ReadPage{}, err
	}
	out := readPage(route, "discord_rest_pins")
	q := url.Values{"limit": {strconv.Itoa(limit)}}
	if before != "" {
		q.Set("before", before)
	}
	var raw struct {
		Items []struct {
			PinnedAt string          `json:"pinned_at"`
			Message  json.RawMessage `json:"message"`
		} `json:"items"`
		HasMore *bool `json:"has_more"`
	}
	if err = r.get(ctx, "/channels/"+channel+"/messages/pins?"+q.Encode(), &raw); err != nil {
		return out, err
	}
	if raw.Items == nil || raw.HasMore == nil || len(raw.Items) > limit || (*raw.HasMore && len(raw.Items) == 0) {
		return out, errors.New("read_pins_invalid")
	}
	seen := map[string]bool{}
	messages := []ReadMessage{}
	nextBefore := ""
	last := boundary
	for _, item := range raw.Items {
		at, e := time.Parse(time.RFC3339Nano, item.PinnedAt)
		if e != nil || len(item.PinnedAt) > 64 || (!last.IsZero() && at.After(last)) || (!boundary.IsZero() && !at.Before(boundary)) {
			return out, errors.New("read_pins_invalid")
		}
		m, e := decodeReadMessage(item.Message, route)
		if e != nil {
			return out, e
		}
		if seen[m.ID] {
			return out, errors.New("read_pins_duplicate")
		}
		seen[m.ID] = true
		v := readMessage(m)
		v.PinnedAt = item.PinnedAt
		messages = append(messages, v)
		nextBefore = item.PinnedAt
		last = at
	}
	if err = r.validateReadRoute(ctx, route); err != nil {
		return out, err
	}
	out.Messages = messages
	out.NextBefore = nextBefore
	out.HasMore = *raw.HasMore
	out.Complete = !out.HasMore
	return out, nil
}
func (r *RESTClient) SearchMessages(ctx context.Context, s *Store, channel, content string, offset, limit int) (ReadPage, error) {
	if !utf8.ValidString(content) || utf8.RuneCountInString(content) > 1024 || offset < 0 || offset > 9975 || limit < 1 || limit > 25 {
		return ReadPage{}, errors.New("invalid_search_bounds")
	}
	route, err := r.ResolveReadRoute(ctx, s, channel)
	if err != nil {
		return ReadPage{}, err
	}
	out := readPage(route, "discord_rest_search")
	if route.GuildID == "" {
		return out, errors.New("search_guild_required")
	}
	q := url.Values{"channel_id": {channel}, "author_id": {r.settings.Policy.OwnerID}, "author_type": {"user"}, "content": {content}, "offset": {strconv.Itoa(offset)}, "limit": {strconv.Itoa(limit)}, "sort_by": {"timestamp"}, "sort_order": {"desc"}}
	resp, err := r.request(ctx, http.MethodGet, "/guilds/"+route.GuildID+"/messages/search?"+q.Encode(), nil)
	if err != nil {
		return out, errors.New("preflight_transport_failed")
	}
	if resp.StatusCode == 202 {
		var wait struct {
			RetryAfter *float64 `json:"retry_after"`
		}
		if readJSON(resp, &wait) != nil || wait.RetryAfter == nil || math.IsNaN(*wait.RetryAfter) || math.IsInf(*wait.RetryAfter, 0) || *wait.RetryAfter < 0 {
			return out, errors.New("search_index_response_invalid")
		}
		out.Status = "indexing"
		out.RetryAfter = math.Max(1, *wait.RetryAfter)
		out.HasMore = true
		return out, nil
	}
	if resp.StatusCode != 200 {
		resp.Body.Close()
		return out, errors.New("search_http_" + strconv.Itoa(resp.StatusCode))
	}
	var raw struct {
		Messages [][]json.RawMessage `json:"messages"`
		Total    *int                `json:"total_results"`
		Indexing *bool               `json:"doing_deep_historical_index"`
	}
	if readJSON(resp, &raw) != nil || raw.Messages == nil || raw.Total == nil || *raw.Total < 0 || raw.Indexing == nil || len(raw.Messages) > limit {
		return out, errors.New("read_search_invalid")
	}
	seen := map[string]bool{}
	messages := []ReadMessage{}
	last := ""
	for _, group := range raw.Messages {
		if len(group) != 1 {
			return out, errors.New("read_search_invalid")
		}
		m, e := decodeReadMessage(group[0], route)
		if e != nil {
			return out, e
		}
		if m.Author.ID != r.settings.Policy.OwnerID || m.Author.Bot || m.WebhookID != "" || seen[m.ID] || (last != "" && !snowLess(m.ID, last)) {
			return out, errors.New("read_search_scope_mismatch")
		}
		seen[m.ID] = true
		last = m.ID
		messages = append(messages, readMessage(m))
	}
	if err = r.validateReadRoute(ctx, route); err != nil {
		return out, err
	}
	out.Messages = messages
	out.NextOffset = offset + limit
	out.HasMore = out.NextOffset <= 9975 && (*raw.Indexing || out.NextOffset < *raw.Total)
	out.Status = "index_not_exhaustive"
	out.Complete = false
	return out, nil
}

type readPermissionProof struct {
	member guildMember
	roles  []guildRole
}

func (r *RESTClient) readPermissionSnapshot(ctx context.Context) (readPermissionProof, error) {
	var proof readPermissionProof
	if err := r.get(ctx, "/guilds/"+r.settings.Policy.GuildID+"/members/"+r.settings.ExpectedBotID, &proof.member); err != nil {
		return proof, err
	}
	if proof.member.User.ID != r.settings.ExpectedBotID || !proof.member.User.Bot || proof.member.Roles == nil {
		return proof, errors.New("preflight_permissions_invalid")
	}
	if err := r.get(ctx, "/guilds/"+r.settings.Policy.GuildID+"/roles", &proof.roles); err != nil {
		return proof, err
	}
	return proof, nil
}

const readRouteLookupSQL = `SELECT envelope FROM inbound INDEXED BY inbound_read_route_lookup WHERE json_valid(envelope) AND platform='discord' AND json_extract(envelope,'$.conversation_id')=? AND json_extract(envelope,'$.sender_id')=? ORDER BY created DESC LIMIT 1`
