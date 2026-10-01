package bridge

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func reactionOnQuestion(t *testing.T, store *Store, claim *Claim) Envelope {
	t.Helper()
	if _, err := store.QueueReply(claim.InboundID, claim.Claim, "question needing context"); err != nil {
		t.Fatal(err)
	}
	chunk, err := store.NextChunk()
	if err != nil || chunk == nil {
		t.Fatal(chunk, err)
	}
	if err = store.RecordResult(*chunk, SendResult{State: "sent", MessageID: "9"}); err != nil {
		t.Fatal(err)
	}
	target, err := store.sentControlTarget(claim.Envelope.ConversationID, "9")
	if err != nil {
		t.Fatal(err)
	}
	e := claim.Envelope
	e.EventID = "9"
	e.SourceRevision = 0
	e.ContentHash = ""
	e.Text = ""
	e.ReplyKind = "message"
	e.Control = encodeControl(ControlEvent{Version: 1, Kind: "reaction", ID: "fixture", ActorID: e.SenderID, TargetMessageID: "9", TargetRequestID: target.Request, TargetRevision: target.Revision, TargetText: target.Text, Emoji: "👍", Added: true})
	if out, err := store.ingestReaction(e); err != nil || out != "accepted" {
		t.Fatal(out, err)
	}
	return e
}
func TestReactionTargetRevisionEditDeleteBeforeClaim(t *testing.T) {
	for _, kind := range []string{"edit", "delete"} {
		t.Run(kind, func(t *testing.T) {
			store, cfg, m, claim := sourceFixture(t)
			reactionOnQuestion(t, store, claim)
			if kind == "edit" {
				applyEdit(t, store, cfg, m, "revised owner question")
			} else {
				store.DeleteSource(m.ChannelID, m.GuildID, m.ID)
			}
			next, err := store.ClaimNext(60, 0)
			if err != nil {
				t.Fatal(err)
			}
			if next != nil && next.Envelope.Control != "" {
				t.Fatal("stale reaction claimed", next)
			}
			var n int
			store.call(func(db *storeConn) (any, error) {
				err := db.QueryRow("SELECT count(*) FROM message_sources WHERE event_id='9'").Scan(&n)
				return nil, err
			})
			if n != 0 {
				t.Fatal("reaction registered as source message")
			}
		})
	}
}
func TestReactionTargetRevisionRevokesClaimAndQueuedReply(t *testing.T) {
	for _, queued := range []bool{false, true} {
		t.Run(map[bool]string{false: "claimed", true: "queued"}[queued], func(t *testing.T) {
			store, cfg, m, claim := sourceFixture(t)
			reactionOnQuestion(t, store, claim)
			control, err := store.ClaimNextForConsumer(60, 30, "reaction-consumer")
			if err != nil || control == nil {
				t.Fatal(control, err)
			}
			if queued {
				if _, err = store.QueueReply(control.InboundID, control.Claim, "response"); err != nil {
					t.Fatal(err)
				}
			}
			applyEdit(t, store, cfg, m, "changed question")
			if store.Renew(control.InboundID, control.Claim, 60) == nil || store.BeginProcessing(control.InboundID, control.Claim, 30) == nil {
				t.Fatal("stale reaction claim renewed")
			}
			if _, err = store.QueueReply(control.InboundID, control.Claim, "response"); err == nil {
				t.Fatal("stale reaction reply queued")
			}
			chunk, err := store.NextChunk()
			if err != nil || chunk != nil {
				t.Fatal("stale reaction dispatched", chunk, err)
			}
		})
	}
}
func TestReactionTargetRevisionEditDuringWriteWait(t *testing.T) {
	store, _, m, claim := sourceFixture(t)
	reactionOnQuestion(t, store, claim)
	control, _ := store.ClaimNext(60, 0)
	store.QueueReply(control.InboundID, control.Claim, "response")
	chunk, _ := store.NextChunk()
	var posts atomic.Int32
	rest := localREST(t, func(w http.ResponseWriter, r *http.Request) {
		if preflightHandler(w, r) {
			return
		}
		switch r.URL.Path {
		case "/channels/2/messages/3":
			json.NewEncoder(w).Encode(m)
		case "/channels/2/messages/9":
			json.NewEncoder(w).Encode(map[string]any{"id": "9", "channel_id": "2", "author": User{ID: "4", Bot: true}, "content": "question needing context"})
		default:
			posts.Add(1)
			w.WriteHeader(500)
		}
	})
	rest.controlStore = store
	rest.limitMu.Lock()
	route, _ := limitRoute(http.MethodPost, "/channels/2/messages")
	rest.limits = map[string]time.Time{route: time.Now().Add(300 * time.Millisecond)}
	rest.limitMu.Unlock()
	done := make(chan SendResult, 1)
	measured := make(chan Diagnostics, 1)
	go func() {
		result, diagnostics := rest.SendCurrentSourceMeasured(context.Background(), store, *chunk, nil)
		measured <- diagnostics
		done <- result
	}()
	time.Sleep(100 * time.Millisecond)
	if _, err := store.DeleteSource(m.ChannelID, m.GuildID, m.ID); err != nil {
		t.Fatal(err)
	}
	result := <-done
	diagnostics := <-measured
	if diagnostics.Post.RateLimitWaitSeconds <= 0 {
		t.Fatal("fixture did not reach write budget", diagnostics)
	}
	if posts.Load() != 0 || result.State != "failed" {
		t.Fatal(result, posts.Load())
	}
	if err := store.RecordResult(*chunk, result); err != nil {
		t.Fatal("record stale control result", err)
	}
}

func TestAdmittedReactionBotTargetUpdateRevokesBeforeClaim(t *testing.T) {
	store, _, _, claim := sourceFixture(t)
	reactionOnQuestion(t, store, claim)
	if changed, err := store.InvalidateControlTarget("2", "", "9", "target_updated"); err != nil || !changed {
		t.Fatal(changed, err)
	}
	if next, err := store.ClaimNext(60, 0); err != nil || next != nil {
		t.Fatal("updated bot question context claimed", next, err)
	}
}
func TestStaleReactionUncertaintyRequiresExplicitResolution(t *testing.T) {
	store, _, m, claim := sourceFixture(t)
	reactionOnQuestion(t, store, claim)
	control, _ := store.ClaimNext(60, 0)
	reply, err := store.QueueReply(control.InboundID, control.Claim, strings.Repeat("x", 2000))
	if err != nil {
		t.Fatal(err)
	}
	chunk, _ := store.NextChunk()
	store.RecordResult(*chunk, SendResult{State: "uncertain", Code: "lost_ack"})
	store.DeleteSource(m.ChannelID, m.GuildID, m.ID)
	e := testEnvelope()
	e.EventID = "44"
	store.Ingest(e)
	later, _ := store.ClaimNext(60, 0)
	if later == nil {
		t.Fatal("later claim absent")
	}
	store.QueueReply(later.InboundID, later.Claim, "later answer")
	if next, err := store.NextChunk(); err != nil || next != nil {
		t.Fatal("uncertainty blocker released", next, err)
	}
	if err = store.ResolveSent(reply, 0, "99"); err != nil {
		t.Fatal(err)
	}
	if next, err := store.NextChunk(); err != nil || next == nil || next.Source.EventID != "44" {
		t.Fatal("safe queue failed to advance", next, err)
	}
}
