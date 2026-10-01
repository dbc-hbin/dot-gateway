package bridge

import (
	"encoding/json"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
)

func permissionTestBits(bits uint64) string { return strconv.FormatUint(bits, 10) }

func permissionTestFixture() (Channel, guildMember, []guildRole, time.Time) {
	return Channel{ID: "2", Type: 0, GuildID: "5", PermissionOverwrites: []PermissionOverwrite{}},
		guildMember{User: User{ID: "4", Bot: true}, Roles: []string{}},
		[]guildRole{{ID: "5", Permissions: permissionTestBits(permissionViewChannel | permissionReadHistory | permissionSendThreads)}},
		time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
}

func TestThreadPermissionOverwriteHierarchy(t *testing.T) {
	const required = permissionViewChannel | permissionReadHistory | permissionSendThreads
	for _, tc := range []struct {
		name       string
		base       uint64
		assigned   []string
		roles      []guildRole
		overwrites []PermissionOverwrite
		want       uint64
	}{
		{name: "everyone base", base: required, want: required},
		{name: "assigned role union", base: permissionViewChannel, assigned: []string{"6", "7"}, roles: []guildRole{{ID: "6", Permissions: permissionTestBits(permissionReadHistory)}, {ID: "7", Permissions: permissionTestBits(permissionSendThreads)}}, want: required},
		{name: "unassigned grants ignored", base: permissionViewChannel, roles: []guildRole{{ID: "6", Permissions: permissionTestBits(permissionReadHistory | permissionSendThreads)}}, want: permissionViewChannel},
		{name: "everyone deny", base: required, overwrites: []PermissionOverwrite{{ID: "5", Allow: "0", Deny: permissionTestBits(permissionSendThreads)}}, want: required &^ permissionSendThreads},
		{name: "everyone allow follows deny", base: required, overwrites: []PermissionOverwrite{{ID: "5", Allow: permissionTestBits(permissionSendThreads), Deny: permissionTestBits(permissionSendThreads)}}, want: required},
		{name: "role allow follows everyone deny", base: required, assigned: []string{"6"}, roles: []guildRole{{ID: "6", Permissions: "0"}}, overwrites: []PermissionOverwrite{{ID: "5", Allow: "0", Deny: permissionTestBits(permissionSendThreads)}, {ID: "6", Allow: permissionTestBits(permissionSendThreads), Deny: "0"}}, want: required},
		{name: "role allow wins aggregate deny", base: required, assigned: []string{"6", "7"}, roles: []guildRole{{ID: "6", Permissions: "0"}, {ID: "7", Permissions: "0"}}, overwrites: []PermissionOverwrite{{ID: "6", Allow: "0", Deny: permissionTestBits(permissionSendThreads)}, {ID: "7", Allow: permissionTestBits(permissionSendThreads), Deny: "0"}}, want: required},
		{name: "role allow order independent", base: required, assigned: []string{"6", "7"}, roles: []guildRole{{ID: "6", Permissions: "0"}, {ID: "7", Permissions: "0"}}, overwrites: []PermissionOverwrite{{ID: "7", Allow: permissionTestBits(permissionSendThreads), Deny: "0"}, {ID: "6", Allow: "0", Deny: permissionTestBits(permissionSendThreads)}}, want: required},
		{name: "member deny wins role allow", base: required, assigned: []string{"6"}, roles: []guildRole{{ID: "6", Permissions: "0"}}, overwrites: []PermissionOverwrite{{ID: "6", Allow: permissionTestBits(permissionSendThreads), Deny: "0"}, {ID: "4", Type: 1, Allow: "0", Deny: permissionTestBits(permissionSendThreads)}}, want: required &^ permissionSendThreads},
		{name: "member allow wins role deny", base: required, assigned: []string{"6"}, roles: []guildRole{{ID: "6", Permissions: "0"}}, overwrites: []PermissionOverwrite{{ID: "6", Allow: "0", Deny: permissionTestBits(permissionSendThreads)}, {ID: "4", Type: 1, Allow: permissionTestBits(permissionSendThreads), Deny: "0"}}, want: required},
		{name: "other member ignored", base: required, overwrites: []PermissionOverwrite{{ID: "8", Type: 1, Allow: "0", Deny: permissionTestBits(required)}}, want: required},
		{name: "unassigned role overwrite ignored", base: required, overwrites: []PermissionOverwrite{{ID: "8", Allow: "0", Deny: permissionTestBits(required)}}, want: required},
		{name: "ordinary send denial leaves thread send", base: required | uint64(discordgo.PermissionSendMessages), overwrites: []PermissionOverwrite{{ID: "4", Type: 1, Allow: "0", Deny: permissionTestBits(uint64(discordgo.PermissionSendMessages))}}, want: required},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parent, member, _, now := permissionTestFixture()
			member.Roles = append(member.Roles, tc.assigned...)
			parent.PermissionOverwrites = append(parent.PermissionOverwrites, tc.overwrites...)
			roles := append([]guildRole{{ID: "5", Permissions: permissionTestBits(tc.base)}}, tc.roles...)
			got, err := effectiveThreadPermissions(parent, member, roles, now)
			if err != nil || got != tc.want {
				t.Fatalf("permissions=%#x want=%#x error=%v", got, tc.want, err)
			}
		})
	}
}

func TestThreadPermissionMalformedInputsFailClosed(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*Channel, *guildMember, *[]guildRole)
	}{
		{"missing everyone", func(_ *Channel, _ *guildMember, r *[]guildRole) { *r = nil }},
		{"missing assigned role", func(_ *Channel, m *guildMember, _ *[]guildRole) { m.Roles = []string{"6"} }},
		{"invalid assigned role", func(_ *Channel, m *guildMember, _ *[]guildRole) { m.Roles = []string{"../6"} }},
		{"duplicate role", func(_ *Channel, _ *guildMember, r *[]guildRole) { *r = append(*r, (*r)[0]) }},
		{"invalid role id", func(_ *Channel, _ *guildMember, r *[]guildRole) {
			*r = append(*r, guildRole{ID: "0", Permissions: "0"})
		}},
		{"malformed unused role", func(_ *Channel, _ *guildMember, r *[]guildRole) {
			*r = append(*r, guildRole{ID: "6", Permissions: "not-bits"})
		}},
		{"invalid overwrite id", func(p *Channel, _ *guildMember, _ *[]guildRole) {
			p.PermissionOverwrites = []PermissionOverwrite{{ID: "../4", Type: 1, Allow: "0", Deny: "0"}}
		}},
		{"unknown overwrite type", func(p *Channel, _ *guildMember, _ *[]guildRole) {
			p.PermissionOverwrites = []PermissionOverwrite{{ID: "4", Type: 2, Allow: "0", Deny: "0"}}
		}},
		{"duplicate overwrite", func(p *Channel, _ *guildMember, _ *[]guildRole) {
			p.PermissionOverwrites = []PermissionOverwrite{{ID: "4", Type: 1, Allow: "0", Deny: "0"}, {ID: "4", Type: 1, Allow: "0", Deny: "0"}}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parent, member, roles, now := permissionTestFixture()
			tc.mutate(&parent, &member, &roles)
			if got, err := effectiveThreadPermissions(parent, member, roles, now); err == nil || got != 0 {
				t.Fatalf("malformed response allowed: permissions=%#x error=%v", got, err)
			}
		})
	}
	for _, raw := range []string{"", "-1", "+1", " 1", "1 ", "1\n", "1.0", "0x10", "18446744073709551616", "１"} {
		t.Run("bits_"+strconv.Quote(raw), func(t *testing.T) {
			if _, err := permissionBits(raw); err == nil {
				t.Fatalf("invalid bits %q allowed", raw)
			}
			for _, target := range []string{"role", "allow", "deny"} {
				parent, member, roles, now := permissionTestFixture()
				parent.PermissionOverwrites = []PermissionOverwrite{{ID: "4", Type: 1, Allow: "0", Deny: "0"}}
				switch target {
				case "role":
					roles[0].Permissions = raw
				case "allow":
					parent.PermissionOverwrites[0].Allow = raw
				case "deny":
					parent.PermissionOverwrites[0].Deny = raw
				}
				if got, err := effectiveThreadPermissions(parent, member, roles, now); err == nil || got != 0 {
					t.Fatalf("invalid %s bits allowed: permissions=%#x error=%v", target, got, err)
				}
			}
		})
	}
	for _, raw := range []string{"0", "274877906944", "18446744073709551615"} {
		if _, err := permissionBits(raw); err != nil {
			t.Fatalf("valid bits %q denied: %v", raw, err)
		}
	}
}

func TestThreadPermissionTimeoutAndAdministrator(t *testing.T) {
	for _, tc := range []struct {
		name      string
		offset    time.Duration
		admin     bool
		malformed bool
		want      uint64
		wantError bool
	}{
		{name: "active timeout", offset: time.Second, want: permissionViewChannel | permissionReadHistory},
		{name: "expired timeout", offset: -time.Second, want: permissionViewChannel | permissionReadHistory | permissionSendThreads},
		{name: "timeout exact boundary", want: permissionViewChannel | permissionReadHistory | permissionSendThreads},
		{name: "administrator bypasses overwrite and timeout", offset: time.Second, admin: true, want: ^uint64(0)},
		{name: "administrator does not bypass malformed response", offset: time.Second, admin: true, malformed: true, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parent, member, roles, now := permissionTestFixture()
			timeout := now.Add(tc.offset)
			member.Timeout = &timeout
			if tc.admin {
				member.Roles = []string{"6"}
				roles = append(roles, guildRole{ID: "6", Permissions: permissionTestBits(permissionAdministrator)})
				parent.PermissionOverwrites = []PermissionOverwrite{{ID: "4", Type: 1, Allow: "0", Deny: permissionTestBits(permissionViewChannel | permissionReadHistory | permissionSendThreads)}}
			}
			if tc.malformed {
				parent.PermissionOverwrites[0].Deny = "invalid"
			}
			got, err := effectiveThreadPermissions(parent, member, roles, now)
			if (err != nil) != tc.wantError || got != tc.want {
				t.Fatalf("permissions=%#x want=%#x error=%v", got, tc.want, err)
			}
		})
	}
}

func TestThreadPermissionOverwriteRequiresExplicitJSONType(t *testing.T) {
	for _, tc := range []struct {
		name string
		raw  string
	}{
		{"missing", `{"id":"5","allow":"274877906944","deny":"0"}`},
		{"null", `{"id":"5","type":null,"allow":"274877906944","deny":"0"}`},
		{"string", `{"id":"5","type":"0","allow":"274877906944","deny":"0"}`},
		{"object", `{"id":"5","type":{},"allow":"274877906944","deny":"0"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var overwrite PermissionOverwrite
			if err := json.Unmarshal([]byte(tc.raw), &overwrite); err == nil {
				t.Fatal("malformed overwrite type decoded as a role overwrite")
			}
			var parent Channel
			if err := json.Unmarshal([]byte(`{"id":"2","type":0,"guild_id":"5","permission_overwrites":[`+tc.raw+`]}`), &parent); err == nil {
				t.Fatal("malformed nested overwrite type decoded in parent channel")
			}
		})
	}
	for _, typ := range []int{0, 1} {
		raw := `{"id":"5","type":` + strconv.Itoa(typ) + `,"allow":"274877906944","deny":"0"}`
		var overwrite PermissionOverwrite
		if err := json.Unmarshal([]byte(raw), &overwrite); err != nil || overwrite.Type != typ || overwrite.ID != "5" || overwrite.Allow != "274877906944" || overwrite.Deny != "0" {
			t.Fatalf("valid overwrite type %d failed: %+v %v", typ, overwrite, err)
		}
	}
}

func TestThreadPolicySeparatesCandidateFromAdmittedRoute(t *testing.T) {
	p := testPolicy()
	p.GuildID, p.GuildChannelID = "5", "2"
	candidate := testEnvelope()
	candidate.RouteKind, candidate.GuildID, candidate.ConversationID, candidate.BotMentioned = "guild_thread_candidate", "5", "6", true
	if p.Allows(candidate) || p.Accepts(candidate) || !p.Stages(candidate) {
		t.Fatal("candidate crossed admission boundary or failed staging")
	}
	verified := candidate
	verified.RouteKind, verified.ParentChannelID, verified.ThreadType = "guild_thread", "2", 11
	for _, typ := range []int{11, 12} {
		verified.ThreadType = typ
		if !p.Accepts(verified) {
			t.Fatalf("verified thread type %d denied", typ)
		}
	}
	for _, tc := range []struct {
		name   string
		mutate func(*Envelope)
	}{
		{"wrong parent", func(e *Envelope) { e.ParentChannelID = "7" }},
		{"missing parent", func(e *Envelope) { e.ParentChannelID = "" }},
		{"wrong guild", func(e *Envelope) { e.GuildID = "7" }},
		{"parent as thread", func(e *Envelope) { e.ConversationID = "2" }},
		{"announcement thread", func(e *Envelope) { e.ThreadType = 10 }},
		{"ordinary channel", func(e *Envelope) { e.ThreadType = 0 }},
		{"forum channel", func(e *Envelope) { e.ThreadType = 15 }},
		{"media channel", func(e *Envelope) { e.ThreadType = 16 }},
		{"unmentioned", func(e *Envelope) { e.BotMentioned = false }},
		{"other owner", func(e *Envelope) { e.SenderID = "7" }},
		{"bot", func(e *Envelope) { e.SenderIsBot = true }},
		{"bad channel id", func(e *Envelope) { e.ConversationID = "../6" }},
		{"bad event id", func(e *Envelope) { e.EventID = "0" }},
		{"wrong platform", func(e *Envelope) { e.Platform = "slack" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := verified
			tc.mutate(&e)
			if p.Allows(e) || p.Accepts(e) {
				t.Fatal("forged route admitted")
			}
		})
	}
	for _, tc := range []struct {
		name   string
		mutate func(*Envelope)
	}{
		{"forged parent", func(e *Envelope) { e.ParentChannelID = "2" }},
		{"forged type", func(e *Envelope) { e.ThreadType = 11 }},
		{"forged name", func(e *Envelope) { e.ThreadName = "verified" }},
		{"wrong guild", func(e *Envelope) { e.GuildID = "7" }},
		{"parent id", func(e *Envelope) { e.ConversationID = "2" }},
		{"unmentioned", func(e *Envelope) { e.BotMentioned = false }},
		{"non-owner", func(e *Envelope) { e.SenderID = "7" }},
		{"empty text", func(e *Envelope) { e.Text = " " }},
		{"oversize text", func(e *Envelope) { e.Text = strings.Repeat("x", 8001) }},
	} {
		t.Run("candidate_"+tc.name, func(t *testing.T) {
			e := candidate
			tc.mutate(&e)
			if p.Stages(e) {
				t.Fatal("unsafe candidate staged")
			}
		})
	}
	for _, route := range []string{"dm", "guild_text"} {
		e := testEnvelope()
		e.RouteKind = route
		if route == "guild_text" {
			e.GuildID, e.BotMentioned = "5", true
		}
		for _, field := range []string{"parent", "type", "name"} {
			v := e
			switch field {
			case "parent":
				v.ParentChannelID = "2"
			case "type":
				v.ThreadType = 11
			case "name":
				v.ThreadName = "forged"
			}
			if p.Allows(v) || p.Stages(v) {
				t.Fatalf("thread metadata crossed into %s route: %s", route, field)
			}
		}
	}
	p.GuildMode, p.MessageContentApproved = "all", true
	candidate.BotMentioned = false
	if !p.Stages(candidate) {
		t.Fatal("approved all mode requires mention")
	}
	p.MessageContentApproved = false
	if p.Stages(candidate) {
		t.Fatal("unapproved all mode admitted")
	}
}

func TestGuildThreadStartupAndPerRoutePermissionGates(t *testing.T) {
	const read = discordgo.PermissionViewChannel | discordgo.PermissionReadMessageHistory
	for _, tc := range []struct {
		name                  string
		permissions           int64
		deny                  int64
		ready, parent, thread bool
	}{
		{name: "administrator only", permissions: discordgo.PermissionAdministrator, ready: true, parent: true, thread: true},
		{name: "administrator bypasses channel denials", permissions: discordgo.PermissionAdministrator, deny: read | discordgo.PermissionSendMessages | discordgo.PermissionSendMessagesInThreads, ready: true, parent: true, thread: true},
		{name: "both sends", permissions: read | discordgo.PermissionSendMessages | discordgo.PermissionSendMessagesInThreads, ready: true, parent: true, thread: true},
		{name: "parent only", permissions: read | discordgo.PermissionSendMessages, ready: true, parent: true},
		{name: "thread only", permissions: read | discordgo.PermissionSendMessagesInThreads, ready: true, thread: true},
		{name: "parent denied thread allowed", permissions: read | discordgo.PermissionSendMessages | discordgo.PermissionSendMessagesInThreads, deny: discordgo.PermissionSendMessages, ready: true, thread: true},
		{name: "thread denied parent allowed", permissions: read | discordgo.PermissionSendMessages | discordgo.PermissionSendMessagesInThreads, deny: discordgo.PermissionSendMessagesInThreads, ready: true, parent: true},
		{name: "no send", permissions: read},
		{name: "view denied", permissions: read | discordgo.PermissionSendMessagesInThreads, deny: discordgo.PermissionViewChannel},
		{name: "history denied", permissions: read | discordgo.PermissionSendMessagesInThreads, deny: discordgo.PermissionReadMessageHistory},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, err := discordgo.New("Bot offline")
			if err != nil {
				t.Fatal(err)
			}
			cfg := Settings{ExpectedBotID: "4", Policy: Policy{OwnerID: "1", AllowedDMIDs: []string{"1"}, GuildID: "5", GuildChannelID: "2", GuildMode: "mention"}}
			channel := &discordgo.Channel{ID: "2", GuildID: "5", Type: discordgo.ChannelTypeGuildText}
			if tc.deny != 0 {
				channel.PermissionOverwrites = []*discordgo.PermissionOverwrite{{ID: "4", Type: discordgo.PermissionOverwriteTypeMember, Deny: tc.deny}}
			}
			guild := &discordgo.Guild{ID: "5", Roles: []*discordgo.Role{{ID: "5", Permissions: tc.permissions}}, Channels: []*discordgo.Channel{channel}, Members: []*discordgo.Member{{GuildID: "5", User: &discordgo.User{ID: "4", Bot: true}}}}
			if err = s.State.OnInterface(s, &discordgo.GuildCreate{Guild: guild}); err != nil {
				t.Fatal(err)
			}
			if got := guildPermissions(s, cfg); got != tc.ready {
				t.Fatalf("startup ready=%v want=%v", got, tc.ready)
			}
			if got := guildRoutePermissions(s, cfg, Envelope{RouteKind: "guild_text"}); got != tc.parent {
				t.Fatalf("parent route=%v want=%v", got, tc.parent)
			}
			if got := guildRoutePermissions(s, cfg, Envelope{RouteKind: "guild_thread"}); got != tc.thread {
				t.Fatalf("thread route=%v want=%v", got, tc.thread)
			}
		})
	}
}
