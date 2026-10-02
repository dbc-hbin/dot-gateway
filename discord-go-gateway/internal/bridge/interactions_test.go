package bridge

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
)

func interactionID() string { return fmt.Sprint(uint64(time.Now().UnixMilli()-1420070400000) << 22) }
func ownerInteraction(name, prompt string) *discordgo.Interaction {
	d := discordgo.ApplicationCommandInteractionData{ID: "7", Name: name, CommandType: discordgo.ChatApplicationCommand}
	if prompt != "" {
		key := "prompt"
		if name != "ask" {
			key = "request"
		}
		d.Options = []*discordgo.ApplicationCommandInteractionDataOption{{Name: key, Type: discordgo.ApplicationCommandOptionString, Value: prompt}}
	}
	return &discordgo.Interaction{ID: interactionID(), AppID: "4", Type: discordgo.InteractionApplicationCommand, Data: d, ChannelID: "2", User: &discordgo.User{ID: "1"}, Token: "TOKEN_CANARY", Version: 1}
}
func interactionFixture(t *testing.T, h http.HandlerFunc) (*Store, *InteractionService, *atomic.Int32) {
	t.Helper()
	store, cfg, _ := ingressFixture(t)
	store.call(func(db *storeConn) (any, error) {
		for _, name := range []string{"ask", "status", "cancel"} {
			if _, err := db.Exec("INSERT INTO control_command_ids VALUES(?,?,?)", "", name, "7"); err != nil {
				return nil, err
			}
		}
		return nil, nil
	})
	var callbacks atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if preflightHandler(w, r) {
			return
		}
		if r.URL.Path == "/channels/2/messages/3" {
			json.NewEncoder(w).Encode(map[string]any{"id": "3", "channel_id": "2", "author": User{ID: "1"}, "content": testEnvelope().Text})
			return
		}
		if strings.Contains(r.URL.Path, "/callback") {
			callbacks.Add(1)
		}
		if strings.HasPrefix(r.URL.Path, "/interactions/") || strings.HasPrefix(r.URL.Path, "/webhooks/") {
			if r.Header.Get("Authorization") != "" {
				t.Error("bot authorization leaked to token transport")
			}
		}
		if h != nil {
			h(w, r)
			return
		}
		if strings.Contains(r.URL.Path, "/callback") {
			w.WriteHeader(204)
			return
		}
		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)
		json.NewEncoder(w).Encode(map[string]any{"id": "9", "channel_id": "2", "author": User{ID: "4", Bot: true}, "webhook_id": "4", "content": body["content"], "flags": 64})
	}))
	t.Cleanup(server.Close)
	service, err := NewInteractionService(cfg, store, nil)
	if err != nil {
		t.Fatal(err)
	}
	service.rest.baseURL = server.URL
	service.rest.client.Transport = http.DefaultTransport.(*http.Transport).Clone()
	service.transport.base = server.URL
	service.transport.client.Transport = http.DefaultTransport.(*http.Transport).Clone()
	t.Cleanup(service.Close)
	return store, service, &callbacks
}
func TestInteractionAskRoundTripNoTokenExport(t *testing.T) {
	store, svc, calls := interactionFixture(t, nil)
	i := ownerInteraction("ask", "actual user request")
	if out := svc.Handle(context.Background(), i, time.Now()); out != "queued" {
		t.Fatal(out)
	}
	if out := svc.Handle(context.Background(), i, time.Now()); out != "duplicate" {
		t.Fatal(out)
	}
	if calls.Load() != 1 {
		t.Fatal("replayed callback")
	}
	claim, err := store.ClaimNext(60, 0)
	if err != nil || claim == nil || claim.Envelope.Text != "actual user request" || claim.Envelope.ReplyKind != "interaction" {
		t.Fatal(claim, err)
	}
	raw, _ := json.Marshal(claim)
	if strings.Contains(string(raw), i.Token) || !strings.Contains(string(raw), `"control":{`) {
		t.Fatal("bad structured claim")
	}
	reply, err := store.QueueReply(claim.InboundID, claim.Claim, "authored answer")
	if err != nil {
		t.Fatal(err)
	}
	chunk, err := store.NextChunk()
	if err != nil || chunk == nil {
		t.Fatal(err)
	}
	result, _ := svc.Send(context.Background(), *chunk, func() bool { return true })
	if result.State != "sent" {
		t.Fatal(result)
	}
	if err = store.RecordResult(*chunk, result); err != nil {
		t.Fatal(err)
	}
	d, _ := store.Delivery(reply)
	if d.State != "sent" {
		t.Fatal(d)
	}
	feedback, err := store.FeedbackRows()
	if err != nil || len(feedback) != 0 {
		t.Fatal("interaction used message feedback", feedback, err)
	}
	_, err = store.call(func(db *storeConn) (any, error) {
		var raw string
		err := db.QueryRow("SELECT envelope FROM inbound WHERE id=?", claim.InboundID).Scan(&raw)
		if strings.Contains(raw, i.Token) {
			t.Fatal("persisted token")
		}
		return nil, err
	})
	if err != nil {
		t.Fatal(err)
	}
}
func TestInteractionIdentityScopeAndDeadline(t *testing.T) {
	for _, tc := range []string{"owner", "app", "channel", "guild", "expired", "future", "bot", "type", "option", "long", "token"} {
		t.Run(tc, func(t *testing.T) {
			_, svc, calls := interactionFixture(t, nil)
			i := ownerInteraction("ask", "request")
			switch tc {
			case "owner":
				i.User.ID = "8"
			case "app":
				i.AppID = "8"
			case "channel":
				i.ChannelID = "8"
			case "guild":
				i.GuildID = "8"
			case "expired":
				i.ID = "3"
			case "future":
				i.ID = fmt.Sprint(uint64(time.Now().Add(time.Minute).UnixMilli()-1420070400000) << 22)
			case "bot":
				i.User.Bot = true
			case "type":
				i.Type = discordgo.InteractionApplicationCommandAutocomplete
			case "option":
				d := i.Data.(discordgo.ApplicationCommandInteractionData)
				d.Options[0].Name = "admin"
				i.Data = d
			case "long":
				d := i.Data.(discordgo.ApplicationCommandInteractionData)
				d.Options[0].Value = strings.Repeat("x", 4001)
				i.Data = d
			case "token":
				i.Token = "secret/path"
			}
			if out := svc.Handle(context.Background(), i, time.Now()); out == "queued" || calls.Load() != 0 {
				t.Fatal(out, calls.Load())
			}
		})
	}
}
func TestInteractionLostAckIsNeverReplayed(t *testing.T) {
	store, svc, calls := interactionFixture(t, func(w http.ResponseWriter, r *http.Request) { conn, _, _ := w.(http.Hijacker).Hijack(); conn.Close() })
	i := ownerInteraction("ask", "request")
	if out := svc.Handle(context.Background(), i, time.Now()); out != "ack_uncertain" {
		t.Fatal(out)
	}
	if out := svc.Handle(context.Background(), i, time.Now()); out != "duplicate" {
		t.Fatal(out)
	}
	claim, _ := store.ClaimNext(60, 0)
	if claim != nil || calls.Load() != 1 {
		t.Fatal("uncertain ack replayed")
	}
}
func TestInteractionRestartTokenLossAndNoPublicFallback(t *testing.T) {
	store, svc, _ := interactionFixture(t, nil)
	if out := svc.Handle(context.Background(), ownerInteraction("ask", "request"), time.Now()); out != "queued" {
		t.Fatal(out)
	}
	claim, _ := store.ClaimNext(60, 0)
	store.QueueReply(claim.InboundID, claim.Claim, "answer")
	chunk, _ := store.NextChunk()
	svc.transport.close()
	got, _ := svc.Send(context.Background(), *chunk, nil)
	if got.State != "failed" || got.Code != "interaction_token_unavailable_reissue_required" {
		t.Fatal(got)
	}
	if got := svc.rest.Send(context.Background(), *chunk); got.Code != "interaction_transport_required" {
		t.Fatal(got)
	}
}
func TestInteractionModalSingleUseCrossRoute(t *testing.T) {
	var modalID string
	store, svc, _ := interactionFixture(t, func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)
		if body["type"] == float64(9) {
			modalID = body["data"].(map[string]any)["custom_id"].(string)
		}
		w.WriteHeader(204)
	})
	if out := svc.Handle(context.Background(), ownerInteraction("ask", ""), time.Now()); out != "sent" || modalID == "" {
		t.Fatal(out)
	}
	makeModal := func() *discordgo.Interaction {
		i := ownerInteraction("ask", "")
		i.ID = fmt.Sprint(mustUint(interactionID()) + 1)
		i.Type = discordgo.InteractionModalSubmit
		i.Data = discordgo.ModalSubmitInteractionData{CustomID: modalID, Components: []discordgo.MessageComponent{&discordgo.ActionsRow{Components: []discordgo.MessageComponent{&discordgo.TextInput{CustomID: "prompt", Value: "modal request"}}}}}
		return i
	}
	i := makeModal()
	if out := svc.Handle(context.Background(), i, time.Now()); out != "queued" {
		t.Fatal(out)
	}
	j := makeModal()
	j.ID = fmt.Sprint(mustUint(i.ID) + 1)
	if out := svc.Handle(context.Background(), j, time.Now()); out != "rejected" {
		t.Fatal(out)
	}
	claim, _ := store.ClaimNext(60, 0)
	if claim == nil || claim.Envelope.Text != "modal request" {
		t.Fatal(claim)
	}
	b, err := store.newBinding(testEnvelope(), "ask", "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	wrong := testEnvelope()
	wrong.ConversationID = "8"
	if _, err = store.consumeBinding(b.ID, wrong, "ask", ""); err == nil {
		t.Fatal("cross route accepted")
	}
	if _, err = store.consumeBinding("forged", testEnvelope(), "ask", ""); err == nil {
		t.Fatal("forged accepted")
	}
}
func mustUint(s string) uint64 { var n uint64; fmt.Sscan(s, &n); return n }
func TestControlCancellationRevokesClaimsAndPreservesUncertainty(t *testing.T) {
	for _, state := range []string{"claimed", "pending", "sending", "uncertain", "sent"} {
		t.Run(state, func(t *testing.T) {
			store, _, _ := ingressFixture(t)
			store.Ingest(testEnvelope())
			claim, _ := store.ClaimNextForConsumer(60, 0, "test")
			var chunk *Chunk
			if state != "claimed" {
				store.QueueReply(claim.InboundID, claim.Claim, "answer")
				if state != "pending" {
					chunk, _ = store.NextChunk()
					if state != "sending" {
						store.RecordResult(*chunk, SendResult{State: state, MessageID: "9"})
					}
				}
			}
			r, err := store.CancelControlRequest(testEnvelope(), claim.InboundID, controlRevision(claim.Envelope))
			if err != nil {
				t.Fatal(err)
			}
			switch state {
			case "claimed":
				if r.State != "cancel_requested" || r.Cancellation == nil || r.Cancellation.ExecutionState != "worker_unbound" {
					t.Fatal(r)
				}
			case "sent":
				if r.State != "delivered" {
					t.Fatal(r)
				}
			case "sending", "uncertain":
				if r.State != state {
					t.Fatal(r)
				}
			default:
				if r.State != "cancelled" {
					t.Fatal(r)
				}
			}
			if state != "sent" {
				if store.Renew(claim.InboundID, claim.Claim, 60) == nil {
					t.Fatal("cancelled claim renewed")
				}
				if _, err = store.QueueReply(claim.InboundID, claim.Claim, "answer"); err == nil {
					t.Fatal("cancelled reply queued")
				}
				if store.controlRequestCurrent(claim.Envelope) {
					t.Fatal("cancelled pre-post fence remained valid")
				}
			}
		})
	}
}
func TestCommandDryRunPreservesUnrelatedAndRejectsCollision(t *testing.T) {
	cfg := Settings{Policy: testPolicy(), ExpectedBotID: "4"}
	cfg.Policy.GuildID = "5"
	snapshot := CommandSnapshot{ApplicationID: "4", GuildID: "5", OwnedIDs: map[string]string{}, Commands: []GuildCommand{{ID: "8", ApplicationID: "4", GuildID: "5", Name: "other", Type: 1}}}
	plan, err := PlanGuildCommands(cfg, snapshot)
	if err != nil || len(plan.Changes) != 4 || plan.Changes[3].Action != "preserve" || !plan.DryRun || plan.NetworkUsed {
		t.Fatal(plan, err)
	}
	snapshot.Commands = append(snapshot.Commands, GuildCommand{ID: "9", ApplicationID: "4", GuildID: "5", Name: "ask", Type: 1})
	if _, err = PlanGuildCommands(cfg, snapshot); err == nil {
		t.Fatal("unowned collision overwritten")
	}
	snapshot.OwnedIDs["ask"] = "9"
	if _, err = PlanGuildCommands(cfg, snapshot); err != nil {
		t.Fatal(err)
	}
}
func TestInteractionDedupSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	os.Chmod(dir, 0700)
	path := filepath.Join(dir, "controls.db")
	store, err := OpenStore(path, testPolicy())
	if err != nil {
		t.Fatal(err)
	}
	e := testEnvelope()
	fresh, err := store.reserveInteraction("99", e, "ask")
	if err != nil || !fresh {
		t.Fatal(err)
	}
	store.Close()
	store, err = OpenStore(path, testPolicy())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	fresh, err = store.reserveInteraction("99", e, "ask")
	if err != nil || fresh {
		t.Fatal("replayed after restart", err)
	}
}
func TestReactionTransitionsAreContextNotApproval(t *testing.T) {
	store, svc, _ := interactionFixture(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/channels/2/messages/9" {
			io.WriteString(w, `{"id":"9","channel_id":"2","author":{"id":"4","bot":true},"content":"precise question"}`)
			return
		}
		t.Error("unexpected request")
	})
	store.Ingest(testEnvelope())
	claim, _ := store.ClaimNext(60, 0)
	store.QueueReply(claim.InboundID, claim.Claim, "precise question")
	chunk, _ := store.NextChunk()
	store.RecordResult(*chunk, SendResult{State: "sent", MessageID: "9"})
	binding, err := store.BindPendingResponse(claim.InboundID, claim.Claim, "9")
	if err != nil {
		t.Fatal(err)
	}
	m := &discordgo.MessageReaction{UserID: "1", MessageID: "9", ChannelID: "2", Emoji: discordgo.Emoji{Name: "👍"}}
	for n, added := range []bool{true, true, false, true} {
		out := svc.HandleReaction(context.Background(), m, added)
		want := "accepted"
		if n == 1 {
			want = "duplicate"
		}
		if out != want {
			t.Fatal(n, out)
		}
	}
	for n := 0; n < 3; n++ {
		c, _ := store.ClaimNext(60, 0)
		if c == nil {
			t.Fatal("missing reaction")
		}
		event, err := c.Envelope.ControlEvent()
		if err != nil || event.ApprovalGranted || event.TargetText != "precise question" || c.Envelope.Text != "" || event.TargetRequestID != claim.InboundID || event.TargetRevision != controlRevision(claim.Envelope) {
			t.Fatal(event, err)
		}
		if n == 0 && event.PendingResponseID != binding || n > 0 && event.PendingResponseID != "" {
			t.Fatal("pending binding replayed", event)
		}
		store.Ignore(c.InboundID, c.Claim)
	}
	m.UserID = "7"
	if out := svc.HandleReaction(context.Background(), m, true); out != "rejected" {
		t.Fatal(out)
	}
	m.UserID = "1"
	m.MessageID = "8"
	if out := svc.HandleReaction(context.Background(), m, true); out != "rejected" {
		t.Fatal(out)
	}
}

func TestTokenExpiryCancelsKnownUnsentWithoutQueueDeadlock(t *testing.T) {
	store, svc, _ := interactionFixture(t, nil)
	i := ownerInteraction("ask", "request")
	if out := svc.Handle(context.Background(), i, time.Now()); out != "queued" {
		t.Fatal(out)
	}
	claim, _ := store.ClaimNext(60, 0)
	store.QueueReply(claim.InboundID, claim.Claim, "answer")
	chunk, _ := store.NextChunk()
	svc.transport.mu.Lock()
	svc.transport.tokens[i.ID] = interactionToken{value: "TOKEN_CANARY", expires: time.Now().Add(-time.Second)}
	svc.transport.mu.Unlock()
	result, _ := svc.Send(context.Background(), *chunk, nil)
	if result.State != "failed" {
		t.Fatal(result)
	}
	if err := store.RecordResult(*chunk, result); err != nil {
		t.Fatal(err)
	}
	requests, err := store.ControlRequests(claim.Envelope, claim.InboundID)
	if err != nil || requests[0].State != "unavailable_reissue_required" {
		t.Fatal(requests, err)
	}
	normal := testEnvelope()
	normal.EventID = "33"
	store.Ingest(normal)
	next, _ := store.ClaimNext(60, 0)
	if next == nil {
		t.Fatal("claim blocked")
	}
	store.QueueReply(next.InboundID, next.Claim, "later answer")
	later, err := store.NextChunk()
	if err != nil || later == nil || later.Source.EventID != "33" {
		t.Fatal("known unavailable blocked queue", later, err)
	}
}
func TestRestartMidDeferKeepsUncertainDelivery(t *testing.T) {
	for _, uncertain := range []bool{false, true} {
		t.Run(fmt.Sprint(uncertain), func(t *testing.T) {
			store, svc, _ := interactionFixture(t, nil)
			i := ownerInteraction("ask", "request")
			svc.Handle(context.Background(), i, time.Now())
			claim, _ := store.ClaimNext(60, 0)
			if uncertain {
				store.QueueReply(claim.InboundID, claim.Claim, "answer")
				chunk, _ := store.NextChunk()
				store.RecordResult(*chunk, SendResult{State: "uncertain", Code: "lost_ack"})
			}
			if err := store.RecoverInteractionTokens(); err != nil {
				t.Fatal(err)
			}
			r, err := store.ControlRequests(claim.Envelope, claim.InboundID)
			if err != nil {
				t.Fatal(err)
			}
			want := "unavailable_reissue_required"
			if uncertain {
				want = "uncertain"
			}
			if r[0].State != want {
				t.Fatal(r)
			}
		})
	}
}
func TestOpaqueBindingExpiryActionAndMessageChecks(t *testing.T) {
	store, _, _ := ingressFixture(t)
	store.Ingest(testEnvelope())
	claim, _ := store.ClaimNext(60, 0)
	b, err := store.newBinding(testEnvelope(), "cancel", claim.InboundID, controlRevision(claim.Envelope), "99")
	if err != nil {
		t.Fatal(err)
	}
	for _, input := range []struct{ action, message string }{{"ask", "99"}, {"cancel", "98"}, {"cancel", ""}} {
		if _, err = store.consumeBinding(b.ID, testEnvelope(), input.action, input.message); err == nil {
			t.Fatal("unbound accepted")
		}
	}
	store.call(func(db *storeConn) (any, error) {
		_, err := db.Exec("UPDATE control_bindings SET expires=? WHERE id=?", epoch()-1, b.ID)
		return nil, err
	})
	if _, err = store.consumeBinding(b.ID, testEnvelope(), "cancel", "99"); err == nil {
		t.Fatal("expired binding accepted")
	}
}
func TestReactionChangedOrDeletedTargetNeverClaims(t *testing.T) {
	for _, status := range []int{200, 404} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			store, svc, _ := interactionFixture(t, func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(status)
				io.WriteString(w, `{"id":"9","channel_id":"2","author":{"id":"4","bot":true},"content":"changed"}`)
			})
			store.Ingest(testEnvelope())
			claim, _ := store.ClaimNext(60, 0)
			store.QueueReply(claim.InboundID, claim.Claim, "original")
			chunk, _ := store.NextChunk()
			store.RecordResult(*chunk, SendResult{State: "sent", MessageID: "9"})
			m := &discordgo.MessageReaction{UserID: "1", MessageID: "9", ChannelID: "2", Emoji: discordgo.Emoji{Name: "👍"}}
			if out := svc.HandleReaction(context.Background(), m, true); out != "rejected" {
				t.Fatal(out)
			}
			claim, _ = store.ClaimNext(60, 0)
			if claim != nil {
				t.Fatal("changed target admitted")
			}
		})
	}
}
func TestReviewedRegistrationBoundedWritesAndReplay(t *testing.T) {
	store, _, _ := ingressFixture(t)
	cfg := Settings{Policy: testPolicy(), ExpectedBotID: "4"}
	cfg.Policy.GuildID, cfg.Policy.GuildChannelID = "5", "6"
	var calls atomic.Int32
	commands := []GuildCommand{{ID: "77", ApplicationID: "4", GuildID: "5", Name: "unrelated", Type: 1}}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/users/@me":
			io.WriteString(w, `{"id":"4","bot":true}`)
			return
		case "/oauth2/applications/@me":
			io.WriteString(w, `{"id":"4","bot":{"id":"4","bot":true}}`)
			return
		case "/channels/6":
			io.WriteString(w, `{"id":"6","type":0,"guild_id":"5"}`)
			return
		case "/applications/4/guilds/5/commands":
			if r.Method == http.MethodGet {
				json.NewEncoder(w).Encode(commands)
				return
			}
			if r.Method != http.MethodPost {
				t.Error("bulk or other write")
				w.WriteHeader(405)
				return
			}
			var command GuildCommand
			json.NewDecoder(r.Body).Decode(&command)
			command.ID = fmt.Sprint(80 + calls.Add(1))
			command.ApplicationID, command.GuildID = "4", "5"
			commands = append(commands, command)
			json.NewEncoder(w).Encode(command)
		default:
			t.Error("unexpected route", r.URL.Path)
			w.WriteHeader(404)
		}
	}))
	defer server.Close()
	r, err := NewRESTClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	r.baseURL = server.URL
	r.client.Transport = http.DefaultTransport.(*http.Transport).Clone()
	snapshot, err := r.CommandSnapshot(context.Background(), store)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := PlanGuildCommands(cfg, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	result, err := r.ApplyGuildCommandPlan(context.Background(), store, plan)
	if err != nil || len(result) != 3 || calls.Load() != 3 {
		t.Fatal(result, err, calls.Load())
	}
	if _, err = r.ApplyGuildCommandPlan(context.Background(), store, plan); err == nil {
		t.Fatal("stale plan replayed")
	}
	if calls.Load() != 3 {
		t.Fatal("repeated write")
	}
}
func TestRegistrationLostAckTombstoneBlocksRetry(t *testing.T) {
	store, _, _ := ingressFixture(t)
	cfg := Settings{Policy: testPolicy(), ExpectedBotID: "4"}
	cfg.Policy.GuildID, cfg.Policy.GuildChannelID = "5", "6"
	var writes atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/users/@me":
			io.WriteString(w, `{"id":"4","bot":true}`)
		case "/oauth2/applications/@me":
			io.WriteString(w, `{"id":"4","bot":{"id":"4","bot":true}}`)
		case "/channels/6":
			io.WriteString(w, `{"id":"6","type":0,"guild_id":"5"}`)
		case "/applications/4/guilds/5/commands":
			if r.Method == "GET" {
				io.WriteString(w, `[]`)
			} else {
				writes.Add(1)
				conn, _, _ := w.(http.Hijacker).Hijack()
				conn.Close()
			}
		default:
			t.Error("unexpected")
		}
	}))
	defer server.Close()
	r, _ := NewRESTClient(cfg)
	defer r.Close()
	r.baseURL = server.URL
	r.client.Transport = http.DefaultTransport.(*http.Transport).Clone()
	snapshot, _ := r.CommandSnapshot(context.Background(), store)
	plan, _ := PlanGuildCommands(cfg, snapshot)
	if _, err := r.ApplyGuildCommandPlan(context.Background(), store, plan); err == nil {
		t.Fatal("lost ack ignored")
	}
	if _, err := r.ApplyGuildCommandPlan(context.Background(), store, plan); err == nil {
		t.Fatal("blind replay allowed")
	}
	if writes.Load() != 1 {
		t.Fatal("repeated uncertain write")
	}
}

func TestInteractionAckRequiresEphemeralAndExactBinding(t *testing.T) {
	for _, field := range []string{"flags", "author", "channel_id", "webhook_id", "content"} {
		t.Run(field, func(t *testing.T) {
			_, svc, _ := interactionFixture(t, func(w http.ResponseWriter, r *http.Request) {
				var body map[string]any
				json.NewDecoder(r.Body).Decode(&body)
				ack := map[string]any{"id": "9", "channel_id": "2", "author": User{ID: "4", Bot: true}, "webhook_id": "4", "content": body["content"], "flags": 64}
				ack[field] = "wrong"
				if field == "flags" {
					ack[field] = 0
				}
				json.NewEncoder(w).Encode(ack)
			})
			svc.transport.remember("99", "TOKEN_CANARY")
			if _, state := svc.transport.edit(context.Background(), "99", "2", "status", nil); state != "uncertain" {
				t.Fatal("invalid status ACK accepted", state)
			}
			e := testEnvelope()
			e.ReplyKind = "interaction"
			e.Control = encodeControl(ControlEvent{Version: 1, Kind: "ask", ID: "test", InteractionID: e.EventID, ActorID: e.SenderID})
			svc.transport.remember(e.EventID, "TOKEN_CANARY")
			got := svc.transport.send(context.Background(), Chunk{Source: e, ReplyID: "test", Text: "answer"}, nil)
			if got.State != "uncertain" {
				t.Fatal("invalid answer ACK accepted", got)
			}
		})
	}
}
func TestRichReactionRequiresStoredOutputReceiptVerifier(t *testing.T) {
	store, svc, _ := interactionFixture(t, func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"id":"9","channel_id":"2","author":{"id":"4","bot":true},"content":"question"}`)
	})
	store.Ingest(testEnvelope())
	claim, _ := store.ClaimNext(60, 0)
	reply, _ := store.QueueReply(claim.InboundID, claim.Claim, "question")
	chunk, _ := store.NextChunk()
	store.RecordResult(*chunk, SendResult{State: "sent", MessageID: "9"})
	_, err := store.call(func(db *storeConn) (any, error) {
		_, err := db.Exec(`CREATE TABLE IF NOT EXISTS reply_outputs(reply_id TEXT PRIMARY KEY,payload TEXT);CREATE TABLE IF NOT EXISTS reply_output_receipts(reply_id TEXT,idx INTEGER,receipt TEXT,PRIMARY KEY(reply_id,idx));`)
		if err != nil {
			return nil, err
		}
		_, err = db.Exec("INSERT INTO reply_outputs VALUES(?,?)", reply, `{"version":1,"embeds":[{"title":"original question"}]}`)
		if err != nil {
			return nil, err
		}
		_, err = db.Exec("INSERT INTO reply_output_receipts VALUES(?,0,'[]')", reply)
		return nil, err
	})
	if err != nil {
		t.Fatal(err)
	}
	target, err := store.sentControlTarget("2", "9")
	if err != nil || target.Output == "" || target.Revision == controlRevision(claim.Envelope) {
		t.Fatal("rich output omitted from revision", target, err)
	}
	binding, err := store.BindPendingResponse(claim.InboundID, claim.Claim, "9")
	if err != nil {
		t.Fatal(err)
	}
	m := &discordgo.MessageReaction{UserID: "1", MessageID: "9", ChannelID: "2", Emoji: discordgo.Emoji{Name: "👍"}}
	if out := svc.HandleReaction(context.Background(), m, true); out != "rejected" {
		t.Fatal("unverified rich output admitted", out)
	}
	store.call(func(db *storeConn) (any, error) {
		var used int
		err := db.QueryRow("SELECT used FROM control_pending_responses WHERE id=?", binding).Scan(&used)
		if used != 0 {
			t.Fatal("rejected rich context consumed pending question")
		}
		return nil, err
	})
}
