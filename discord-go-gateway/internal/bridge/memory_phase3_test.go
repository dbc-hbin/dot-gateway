package bridge

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func memoryDocumentCount(t *testing.T, s *Store) int {
	t.Helper()
	v, err := s.call(func(db *storeConn) (any, error) {
		var n int
		err := db.QueryRow(`SELECT count(*) FROM memory_documents`).Scan(&n)
		return n, err
	})
	if err != nil {
		t.Fatal(err)
	}
	return v.(int)
}
func TestMemoryIntegratedEphemeralAskNeverIndexed(t *testing.T) {
	s, svc, _ := interactionFixture(t, nil)
	interaction := ownerInteraction("ask", "private ephemeral prompt phrase")
	if out := svc.Handle(context.Background(), interaction, time.Now()); out != "queued" {
		t.Fatal(out)
	}
	c := claimLedger(t, s)
	if c.Memory == nil || c.Memory.Status != "excluded" || len(c.Memory.Items) != 0 {
		t.Fatal("interaction recalled public memory", c.Memory)
	}
	if _, err := s.PutMemory(c.InboundID, c.Claim, MemoryMutation{Key: "ephemeral", Kind: "fact", Text: "private ephemeral fact", Sources: []MemoryRef{{"user:" + c.InboundID, 0}}}); !errors.Is(err, ErrClaim) {
		t.Fatal("ephemeral fact allowed", err)
	}
	id := replyLedger(t, s, c, "private ephemeral response phrase")
	chunk, err := s.NextChunk()
	if err != nil {
		t.Fatal(err)
	}
	result, _ := svc.Send(context.Background(), *chunk, func() bool { return true })
	if result.State != "sent" {
		t.Fatal(result)
	}
	if err = s.RecordResult(*chunk, result); err != nil {
		t.Fatal(err)
	}
	delivery, err := s.Delivery(id)
	if err != nil || delivery.State != "sent" {
		t.Fatal(delivery, err)
	}
	if _, err = s.BackfillMemory(100); err != nil {
		t.Fatal(err)
	}
	if memoryDocumentCount(t, s) != 0 {
		t.Fatal("ephemeral history indexed")
	}
	dir := t.TempDir()
	os.Chmod(dir, 0700)
	archive, err := s.ExportMemory(filepath.Join(dir, "ephemeral.memory.jsonl"))
	if err != nil || archive["documents"] != 0 {
		t.Fatal(archive, err)
	}
	ordinary := testEnvelope()
	ordinary.EventID = "99"
	ordinary.Text = "ordinary public follow-up"
	if st, err := s.Ingest(ordinary); err != nil || st != "accepted" {
		t.Fatal(st, err)
	}
	next := claimLedger(t, s)
	if memoryContains(next.Memory, "ephemeral") {
		t.Fatal("ephemeral leaked into ordinary claim")
	}
}
func TestMemoryIntegratedReactionAndControlReplyExcluded(t *testing.T) {
	s, _, _, owner := sourceFixture(t)
	reactionOnQuestion(t, s, owner)
	before := memoryDocumentCount(t, s)
	if before != 2 {
		t.Fatal("ordinary sent history missing", before)
	}
	control := claimLedger(t, s)
	if control.Envelope.Control == "" || control.Memory.Status != "excluded" {
		t.Fatal(control.Memory)
	}
	replyLedger(t, s, control, "reaction context answer")
	chunk, err := s.NextChunk()
	if err != nil {
		t.Fatal(err)
	}
	if err = s.RecordResult(*chunk, SendResult{State: "sent", MessageID: "19"}); err != nil {
		t.Fatal(err)
	}
	if _, err = s.BackfillMemory(100); err != nil {
		t.Fatal(err)
	}
	if memoryDocumentCount(t, s) != before {
		t.Fatal("reaction/control bodies indexed")
	}
}
func TestMemoryIntegratedRichACKAndReconcileSurviveBrokenIndex(t *testing.T) {
	for _, mode := range []string{"file", "rich", "reconcile"} {
		t.Run(mode, func(t *testing.T) {
			s, path, c, dir := outputFixture(t)
			os.WriteFile(filepath.Join(dir, "answer.txt"), []byte("artifact bytes"), 0600)
			manifest := ReplyManifest{Version: 1, Attachments: []ReplyFile{{Path: "answer.txt"}}}
			if mode != "file" {
				manifest.Text = "real delivered description"
				manifest.Embeds = []ReplyEmbed{{Title: "A result", Description: "authored details"}}
			}
			id, chunk := queueOutput(t, s, c, manifest)
			ack := outputACK(*chunk)
			raw, _ := json.Marshal(ack)
			var fields map[string]json.RawMessage
			json.Unmarshal(raw, &fields)
			receipt, err := ReplyOutputAckReceipt(fields, *chunk)
			if err != nil {
				t.Fatal(err)
			}
			_, err = s.call(func(db *storeConn) (any, error) { _, err := db.Exec(`DROP TABLE memory_terms`); return nil, err })
			if err != nil {
				t.Fatal(err)
			}
			if mode == "reconcile" {
				if err = s.RecordResult(*chunk, SendResult{State: "uncertain", Code: "lost_ack"}); err != nil {
					t.Fatal(err)
				}
				r := localREST(t, func(w http.ResponseWriter, req *http.Request) {
					if req.Method != "GET" {
						t.Error("reconcile posted")
					}
					if preflightHandler(w, req) {
						return
					}
					json.NewEncoder(w).Encode(ack)
				})
				r.settings.DBPath = path
				if result, err := r.ReconcileReply(context.Background(), s, id, 0, "9"); err != nil || result.State != "sent" {
					t.Fatal(result, err)
				}
			} else {
				if err = s.RecordResult(*chunk, SendResult{State: "sent", MessageID: "9", OutputReceipt: receipt}); err != nil {
					t.Fatal("memory rolled back file ACK", err)
				}
			}
			delivery, err := s.Delivery(id)
			if err != nil || delivery.State != "sent" || delivery.Chunks[0].OutputReceipt == "" || delivery.Chunks[0].Attempts != 1 {
				t.Fatal(delivery, err)
			}
		})
	}
}
func TestMemoryIntegratedCancelledSourceAndFactsExcluded(t *testing.T) {
	s, _, _, c := sourceFixture(t)
	if _, err := s.PutMemory(c.InboundID, c.Claim, MemoryMutation{Key: "pending", Kind: "project", Text: "project before cancellation", Sources: []MemoryRef{c.Memory.CurrentSource}}); err != nil {
		t.Fatal(err)
	}
	requests, err := s.ControlRequests(c.Envelope, c.InboundID)
	if err != nil || len(requests) != 1 {
		t.Fatal(requests, err)
	}
	if _, err = s.CancelControlRequest(c.Envelope, c.InboundID, requests[0].Revision); err != nil {
		t.Fatal(err)
	}
	v, err := s.call(func(db *storeConn) (any, error) { return recallMemoryDB(db, "future", c.Envelope, "project") })
	if err != nil {
		t.Fatal(err)
	}
	if len(v.(*MemoryRecall).Items) != 0 {
		t.Fatal("cancelled source recalled", v)
	}
	if _, err = s.QueueReply(c.InboundID, c.Claim, "must not send"); err == nil {
		t.Fatal("cancelled source replied")
	}
}
