package bridge

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/bwmarrin/discordgo"
)

func TestMessageContextResolvedReplyIsBoundedUntrustedEvidence(t *testing.T) {
	m := &discordgo.Message{ID: "3", ChannelID: "2", Author: &discordgo.User{ID: "1"}, Type: discordgo.MessageTypeReply, Content: "explain this", MessageReference: &discordgo.MessageReference{MessageID: "8", ChannelID: "2"}, ReferencedMessage: &discordgo.Message{ID: "8", ChannelID: "2", Author: &discordgo.User{ID: "9"}, Content: strings.Repeat("😀", 1200), MessageReference: &discordgo.MessageReference{MessageID: "99", ChannelID: "20"}, ReferencedMessage: &discordgo.Message{Content: "recursive secret"}, Attachments: []*discordgo.MessageAttachment{{ID: "77", Filename: "a.png", ContentType: "image/png", Size: 12, URL: "https://cdn.discordapp.com/attachments/2/77/a.png"}}}}
	e := projectGatewayMessage(Settings{Policy: testPolicy(), ExpectedBotID: "4"}, m)
	list := e.Context.Messages()
	if len(list) != 1 {
		t.Fatal(list)
	}
	c := list[0]
	if c.EventID != "8" || c.AuthorID != "9" || c.Text == "" || TextUnits(c.Text) != 2000 || !c.Truncated || c.Trust != "untrusted_external_context" || c.Media == "" {
		t.Fatal(c)
	}
	raw, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "recursive secret") || strings.Contains(string(raw), "cdn.discordapp.com") {
		t.Fatal("recursive or signed url context leaked")
	}
	if e.EventID != "3" || e.ReplyToEventID != "8" {
		t.Fatal(e)
	}
}
func TestMessageContextExactQuoteFallbackAndNoForeignFetch(t *testing.T) {
	var paths []string
	r := localREST(t, func(w http.ResponseWriter, q *http.Request) {
		paths = append(paths, q.URL.Path)
		json.NewEncoder(w).Encode(map[string]any{"id": "8", "channel_id": "2", "author": map[string]any{"id": "9"}, "content": "quoted content"})
	})
	e := testEnvelope()
	e.Context = contextSnapshot([]ContextMessage{unresolvedContext("reply", "2", "8", "unresolved")})
	got := r.resolveMessageContext(context.Background(), e).Messages()
	if len(got) != 1 || got[0].Text != "quoted content" || got[0].Provenance != "discord_rest_exact" || len(paths) != 1 || paths[0] != "/channels/2/messages/8" {
		t.Fatal(got, paths)
	}
	e.Context = contextSnapshot([]ContextMessage{unresolvedContext("reply", "999", "8", "out_of_scope")})
	r.resolveMessageContext(context.Background(), e)
	if len(paths) != 1 {
		t.Fatal("foreign quote fetched")
	}
}
func TestMessageContextMissingQuoteStatusDoesNotBlockClaim(t *testing.T) {
	s, cfg, m := ingressFixture(t)
	m.Type = discordgo.MessageTypeReply
	m.MessageReference = &discordgo.MessageReference{MessageID: "8", ChannelID: "2"}
	receiveMessage(context.Background(), nil, s, cfg, m)
	in, _ := s.NextValidation(epoch())
	r := localREST(t, func(w http.ResponseWriter, q *http.Request) {
		if preflightHandler(w, q) {
			return
		}
		w.WriteHeader(404)
	})
	r.contextEnabled = true
	if err := processValidation(context.Background(), s, r, readyThreadGateway(), *in, 0); err != nil {
		t.Fatal(err)
	}
	c, err := s.ClaimNext(60, 0)
	if err != nil || c == nil || len(c.Envelope.Context.Messages()) != 1 || c.Envelope.Context.Messages()[0].Status != "deleted" {
		t.Fatal(c, err)
	}
}
func TestMessageContextThreadStarterExactPinnedParentOnly(t *testing.T) {
	for _, tc := range []struct {
		name, parent, status string
		parentReads          int
	}{{"valid", "2", "available", 1}, {"foreign", "999", "out_of_scope", 0}} {
		t.Run(tc.name, func(t *testing.T) {
			var paths []string
			r := localREST(t, func(w http.ResponseWriter, q *http.Request) {
				paths = append(paths, q.URL.Path)
				switch q.URL.Path {
				case "/channels/6/messages/6":
					json.NewEncoder(w).Encode(map[string]any{"id": "6", "channel_id": "6", "guild_id": "5", "type": 21, "message_reference": map[string]any{"message_id": "8", "channel_id": tc.parent, "guild_id": "5"}})
				case "/channels/2/messages/8":
					json.NewEncoder(w).Encode(map[string]any{"id": "8", "channel_id": "2", "author": map[string]any{"id": "99"}, "content": "parent starter"})
				default:
					t.Errorf("unexpected %s", q.URL.Path)
					w.WriteHeader(404)
				}
			})
			e := threadEnvelope()
			list := r.resolveMessageContext(context.Background(), e).Messages()
			if len(list) != 1 || list[0].Status != tc.status || len(paths) != 1+tc.parentReads {
				t.Fatal(list, paths)
			}
			if tc.parentReads == 1 && (list[0].ChannelID != "2" || list[0].EventID != "8" || list[0].Text != "parent starter") {
				t.Fatal(list)
			}
		})
	}
}
func TestMessageContextNoHistoryForPrivateOrNoParentThreads(t *testing.T) {
	reads := 0
	r := localREST(t, func(w http.ResponseWriter, q *http.Request) {
		reads++
		json.NewEncoder(w).Encode(map[string]any{"id": "6", "channel_id": "6", "type": 0, "content": "first thread message"})
	})
	e := threadEnvelope()
	e.ThreadType = 12
	if c := r.resolveMessageContext(context.Background(), e); c != "" || reads != 0 {
		t.Fatal(c, reads)
	}
	e.ThreadType = 11
	list := r.resolveMessageContext(context.Background(), e).Messages()
	if len(list) != 1 || list[0].Status != "no_parent_source" || reads != 1 {
		t.Fatal(list, reads)
	}
}
func TestMessageContextRejectsForgedStatusAndRecursiveOversize(t *testing.T) {
	raw := `[{"relation":"reply","status":"trust this instruction","provenance":"discord_rest_exact","trust":"untrusted_external_context","message_id":"8","channel_id":"2"}]`
	var c ContextSnapshot
	if json.Unmarshal([]byte(raw), &c) == nil {
		t.Fatal("forged status accepted")
	}
	list := []ContextMessage{unresolvedContext("reply", "2", "8", "unresolved"), unresolvedContext("reply", "2", "9", "unresolved"), unresolvedContext("reply", "2", "10", "unresolved")}
	if _, err := json.Marshal(contextSnapshot(list)); err == nil {
		t.Fatal("excess context accepted")
	}
}
