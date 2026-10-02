package bridge

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestFollowupBoundIdempotentAndUncertain(t *testing.T) {
	s, path := testStore(t)
	ingestLedger(t, s, "one", "c")
	c := claimLedger(t, s)
	rid := replyLedger(t, s, c, "initial")
	if _, e := s.QueueFollowup(c.InboundID, c.Claim, "done", "result"); e == nil {
		t.Fatal("accepted before initial delivery")
	}
	ch, e := s.NextChunk()
	if e != nil || ch == nil {
		t.Fatal(ch, e)
	}
	if e = s.RecordResult(*ch, SendResult{State: "sent", MessageID: "remote-1"}); e != nil {
		t.Fatal(e)
	}
	if _, e = s.QueueFollowup(c.InboundID, "wrong", "done", "result"); e == nil {
		t.Fatal("wrong claim")
	}
	if _, e = s.QueueFollowup(c.InboundID, c.Claim, "", "result"); e == nil {
		t.Fatal("missing key")
	}
	id, e := s.QueueFollowup(c.InboundID, c.Claim, "done", "result")
	if e != nil || id != rid {
		t.Fatal(id, e)
	}
	if _, e = s.QueueFollowup(c.InboundID, c.Claim, "done", "changed"); e == nil {
		t.Fatal("mutable key")
	}
	// A separately opened CLI shares the unchanged running dispatcher's queue.
	other, e := OpenStore(path, ledgerPolicy{})
	if e != nil {
		t.Fatal(e)
	}
	defer other.Close()
	if _, e = other.QueueFollowup(c.InboundID, c.Claim, "done", "result"); e != nil {
		t.Fatal(e)
	}
	ch, e = s.NextChunk()
	if e != nil || ch == nil || ch.Index != 1 || ch.Text != "result" || ch.Source.ConversationID != "c" || ch.Source.EventID != "one" {
		t.Fatal(ch, e)
	}
	if e = s.RecordResult(*ch, SendResult{State: "uncertain", Code: "lost_ack"}); e != nil {
		t.Fatal(e)
	}
	if _, e = other.QueueFollowup(c.InboundID, c.Claim, "done", "result"); e != nil {
		t.Fatal(e)
	}
	if _, e = other.QueueFollowup(c.InboundID, c.Claim, "second", "more"); e == nil {
		t.Fatal("bypassed uncertainty")
	}
	if ch, e = s.NextChunk(); e != nil || ch != nil {
		t.Fatal("replayed", ch, e)
	}
	d, e := s.Delivery(rid)
	if e != nil || len(d.Chunks) != 2 || d.Chunks[0].MessageID == nil || *d.Chunks[0].MessageID != "remote-1" || d.Chunks[1].Attempts != 1 {
		t.Fatal(d, e)
	}
	if _, e = s.QueueReply(c.InboundID, c.Claim, "initial"); e != nil {
		t.Fatal("original changed", e)
	}
}

func TestFollowupRejectsRevokedSourceAndCancellation(t *testing.T) {
	for _, mode := range []string{"cancelled", "revoked", "interaction"} {
		t.Run(mode, func(t *testing.T) {
			s, _ := testStore(t)
			ingestLedger(t, s, "one", "c")
			c := claimLedger(t, s)
			rid := replyLedger(t, s, c, "initial")
			ch, e := s.NextChunk()
			if e != nil || ch == nil {
				t.Fatal(ch, e)
			}
			if e = s.RecordResult(*ch, SendResult{State: "sent", MessageID: "remote"}); e != nil {
				t.Fatal(e)
			}
			_, e = s.call(func(db *storeConn) (any, error) {
				switch mode {
				case "cancelled":
					_, e := db.Exec("INSERT INTO reply_cancellations VALUES(?,?,?)", rid, epoch(), "test")
					return nil, e
				case "revoked":
					_, e := db.Exec("UPDATE inbound SET claim=NULL WHERE id=?", c.InboundID)
					return nil, e
				default:
					_, e := db.Exec(`UPDATE inbound SET envelope=json_set(envelope,'$.reply_kind','interaction') WHERE id=?`, c.InboundID)
					return nil, e
				}
			})
			if e != nil {
				t.Fatal(e)
			}
			if _, e = s.QueueFollowup(c.InboundID, c.Claim, "done", "result"); e == nil {
				t.Fatal("accepted", mode)
			}
		})
	}
}

// deliverFollowupInitial keeps the original receipt separate from appended work.
func deliverFollowupInitial(t *testing.T, s *Store, cl *Claim) string {
	t.Helper()
	id, err := s.QueueReply(cl.InboundID, cl.Claim, "initial")
	if err != nil {
		t.Fatal(err)
	}
	c, err := s.NextChunk()
	if err != nil || c == nil {
		t.Fatal(c, err)
	}
	if err = s.RecordResult(*c, SendResult{State: "sent", MessageID: "5"}); err != nil {
		t.Fatal(err)
	}
	return id
}

func TestRichFollowupMultipartReceiptsAndChunkIsolation(t *testing.T) {
	s, path, cl, dir := outputFixture(t)
	id := deliverFollowupInitial(t, s, cl)
	original := tinyReplyPNG(t)
	if err := os.WriteFile(filepath.Join(dir, "chart.png"), original, 0600); err != nil {
		t.Fatal(err)
	}
	m := ReplyManifest{Version: 1, Text: strings.Repeat("x", 2000), Attachments: []ReplyFile{{Path: "chart.png", Description: "Requested chart"}}, Embeds: []ReplyEmbed{{Title: "Result", Image: &ReplyEmbedImage{URL: "attachment://chart.png"}}}}
	raw, _ := json.Marshal(m)
	if got, err := s.QueueFollowupManifest(cl.InboundID, cl.Claim, "complete", raw); err != nil || got != id {
		t.Fatal(got, err)
	}
	c, err := s.NextChunk()
	if err != nil || c == nil || c.Index != 1 || c.Output == "" || c.Source.EventID != cl.Envelope.EventID {
		t.Fatal(c, err)
	}
	if err = os.WriteFile(filepath.Join(dir, "chart.png"), []byte("changed source"), 0600); err != nil {
		t.Fatal(err)
	}
	var posts atomic.Int32
	r := localREST(t, func(w http.ResponseWriter, req *http.Request) {
		if preflightHandler(w, req) {
			return
		}
		if req.Method == "POST" {
			posts.Add(1)
			typ, params, err := mime.ParseMediaType(req.Header.Get("Content-Type"))
			if err != nil || typ != "multipart/form-data" {
				t.Error("missing multipart", typ, err)
				return
			}
			reader := multipart.NewReader(req.Body, params["boundary"])
			part, err := reader.NextPart()
			if err != nil || part.FormName() != "payload_json" {
				t.Error("missing payload", err)
				return
			}
			var payload map[string]any
			if err = json.NewDecoder(part).Decode(&payload); err != nil {
				t.Error(err)
				return
			}
			if payload["nonce"] != Nonce(*c) || payload["enforce_nonce"] != true || payload["content"] != c.Text || payload["message_reference"].(map[string]any)["message_id"] != cl.Envelope.EventID {
				t.Error("follow-up route/nonce changed", payload)
			}
			part, err = reader.NextPart()
			if err != nil || part.FileName() != "chart.png" || part.Header.Get("Content-Type") != "image/png" {
				t.Error("bad image", err)
				return
			}
			data, err := io.ReadAll(part)
			if err != nil || !bytes.Equal(data, original) {
				t.Error("source mutation leaked", err)
			}
		}
		json.NewEncoder(w).Encode(outputACK(*c))
	})
	r.settings.DBPath = path
	result := r.Send(context.Background(), *c)
	if result.State != "sent" || result.OutputReceipt == "" || posts.Load() != 1 {
		t.Fatal(result, posts.Load())
	}
	if err = s.RecordResult(*c, result); err != nil {
		t.Fatal(err)
	}
	stored, receipt, err := s.ReplyReadback(id, 1)
	if err != nil || stored.Output != c.Output || receipt.OutputReceipt != result.OutputReceipt {
		t.Fatal(stored, receipt, err)
	}
	if _, err = r.VerifyStoredReply(context.Background(), stored, receipt); err != nil || posts.Load() != 1 {
		t.Fatal("readback mutated or omitted output", err, posts.Load())
	}
	target, err := s.sentControlTarget(cl.Envelope.ConversationID, result.MessageID)
	if err != nil || target.Output != string(c.Output) || target.Receipt != string(result.OutputReceipt) {
		t.Fatal("reaction context omitted output", target, err)
	}
	_, err = s.call(func(db *storeConn) (any, error) {
		owned, got, err := operationOwnedChunkDB(db, cl.Envelope.ConversationID, result.MessageID)
		if err == nil && (owned.Output != c.Output || got.OutputReceipt != result.OutputReceipt) {
			return nil, errors.New("operation context omitted followup output")
		}
		return nil, err
	})
	if err != nil {
		t.Fatal(err)
	}
	next, err := s.NextChunk()
	if err != nil || next == nil || next.Index != 2 || next.Output != "" {
		t.Fatal("output repeated on tail chunk", next, err)
	}
	if err = s.RecordResult(*next, SendResult{State: "sent", MessageID: "10"}); err != nil {
		t.Fatal(err)
	}
	initial, initialReceipt, err := s.ReplyReadback(id, 0)
	if err != nil || initial.Text != "initial" || initial.Output != "" || initialReceipt.MessageID != "5" || initialReceipt.OutputReceipt != "" {
		t.Fatal("original reply mutated", initial, initialReceipt, err)
	}
	if _, err = s.QueueReply(cl.InboundID, cl.Claim, "initial"); err != nil {
		t.Fatal(err)
	}
}

func TestRichFollowupRestartIdempotenceAndReconciliation(t *testing.T) {
	s, path, cl, dir := outputFixture(t)
	id := deliverFollowupInitial(t, s, cl)
	if err := os.WriteFile(filepath.Join(dir, "result.txt"), []byte("stable result"), 0600); err != nil {
		t.Fatal(err)
	}
	raw := []byte(`{"version":1,"attachments":[{"path":"result.txt"}]}`)
	if _, err := s.QueueFollowupManifest(cl.InboundID, cl.Claim, "result", raw); err != nil {
		t.Fatal(err)
	}
	if again, err := s.QueueFollowupManifest(cl.InboundID, cl.Claim, "result", raw); err != nil || again != id {
		t.Fatal(again, err)
	}
	for _, conflict := range [][]byte{
		[]byte(`{"version":1,"text":"changed","attachments":[{"path":"result.txt"}]}`),
		[]byte(`{"version":1,"attachments":[{"path":"result.txt","description":"changed"}]}`),
		[]byte(`{"version":1,"attachments":[{"path":"result.txt"}],"embeds":[{"title":"changed"}]}`),
	} {
		if _, err := s.QueueFollowupManifest(cl.InboundID, cl.Claim, "result", conflict); err == nil || err.Error() != "followup_key_content_conflict" {
			t.Fatal("key accepted changed content", err)
		}
	}
	c, err := s.NextChunk()
	if err != nil || c == nil || c.Index != 1 || c.Text != "" || c.Output == "" {
		t.Fatal(c, err)
	}
	if err = os.WriteFile(filepath.Join(dir, "result.txt"), []byte("different bytes"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = s.QueueFollowupManifest(cl.InboundID, cl.Claim, "result", raw); err == nil || err.Error() != "followup_key_content_conflict" {
		t.Fatal("key accepted changed bytes", err)
	}
	if err = os.WriteFile(filepath.Join(dir, "result.txt"), []byte("stable result"), 0600); err != nil {
		t.Fatal(err)
	}
	s.Close()
	other, err := OpenStore(path, testPolicy())
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if n, err := other.RecoverInterrupted(); err != nil || n != 1 {
		t.Fatal(n, err)
	}
	if again, err := other.QueueFollowupManifest(cl.InboundID, cl.Claim, "result", raw); err != nil || again != id {
		t.Fatal(again, err)
	}
	if _, err = other.QueueFollowupManifest(cl.InboundID, cl.Claim, "another", raw); err == nil || err.Error() != "followup_requires_confirmed_delivery" {
		t.Fatal("bypassed uncertainty", err)
	}
	if n, err := other.RetryFailed(id); err != nil || n != 0 {
		t.Fatal("retried uncertainty", n, err)
	}
	if err = other.ResolveSent(id, 1, "9"); err == nil || err.Error() != "rich_reply_requires_reconcile_reply" {
		t.Fatal("resolved without receipt", err)
	}
	if next, err := other.NextChunk(); err != nil || next != nil {
		t.Fatal("replayed after restart", next, err)
	}
	var writes atomic.Int32
	r := localREST(t, func(w http.ResponseWriter, req *http.Request) {
		if req.Method != "GET" {
			writes.Add(1)
		}
		if preflightHandler(w, req) {
			return
		}
		json.NewEncoder(w).Encode(outputACK(*c))
	})
	if result, err := r.ReconcileReply(context.Background(), other, id, 1, "9"); err != nil || result.OutputReceipt == "" || writes.Load() != 0 {
		t.Fatal(result, err, writes.Load())
	}
	d, err := other.Delivery(id)
	if err != nil || d.State != "sent" || len(d.Chunks) != 2 || d.Chunks[1].Attempts != 1 || d.Chunks[1].OutputReceipt == "" {
		t.Fatal(d, err)
	}
	if _, err = other.QueueFollowup(cl.InboundID, cl.Claim, "after", "finished"); err != nil {
		t.Fatal(err)
	}
}

func TestRichFollowupAuthorizationBeforeStagingAndRevocation(t *testing.T) {
	for _, mode := range []string{"wrong_claim", "edit", "delete", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			s, cfg, source, cl := sourceFixture(t)
			id := deliverFollowupInitial(t, s, cl)
			raw := []byte(`{"version":1,"embeds":[{"title":"ready"}]}`)
			if _, err := s.QueueFollowupManifest(cl.InboundID, cl.Claim, "done", raw); err != nil {
				t.Fatal(err)
			}
			claim := cl.Claim
			switch mode {
			case "wrong_claim":
				claim = "wrong"
			case "edit":
				applyEdit(t, s, cfg, source, "changed question")
			case "delete":
				if _, err := s.DeleteSource(source.ChannelID, source.GuildID, source.ID); err != nil {
					t.Fatal(err)
				}
			case "cancel":
				if _, err := s.CancelReply(id); err != nil {
					t.Fatal(err)
				}
			}
			for _, key := range []string{"done", "new"} {
				_, err := s.QueueFollowupManifest(cl.InboundID, claim, key, []byte(`{"version":1,"attachments":[{"path":"missing.txt"}]}`))
				if err == nil || !errors.Is(err, ErrClaim) && err.Error() != "followup_reply_cancelled" {
					t.Fatal("reached staging or replay before authorization", err)
				}
			}
			if mode != "wrong_claim" {
				if c, err := s.NextChunk(); err != nil || c != nil {
					t.Fatal("revoked rich followup dispatched", c, err)
				}
			}
		})
	}
}

func TestRichFollowupSpoolProtection(t *testing.T) {
	for _, state := range []string{"pending", "sending", "failed", "uncertain", "cancelled", "sent"} {
		t.Run(state, func(t *testing.T) {
			s, path, cl, dir := outputFixture(t)
			id := deliverFollowupInitial(t, s, cl)
			if err := os.WriteFile(filepath.Join(dir, "result.txt"), []byte("result"), 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := s.QueueFollowupManifest(cl.InboundID, cl.Claim, "result", []byte(`{"version":1,"attachments":[{"path":"result.txt"}]}`)); err != nil {
				t.Fatal(err)
			}
			_, err := s.call(func(db *storeConn) (any, error) {
				_, err := db.Exec("UPDATE chunks SET state=? WHERE reply_id=? AND idx=1", state, id)
				return nil, err
			})
			if err != nil {
				t.Fatal(err)
			}
			blobs, err := filepath.Glob(filepath.Join(ReplyStateDir(path), "reply-spool", "*.blob"))
			if err != nil || len(blobs) != 1 {
				t.Fatal(blobs, err)
			}
			old := time.Now().Add(-8 * 24 * time.Hour)
			if err = os.Chtimes(blobs[0], old, old); err != nil {
				t.Fatal(err)
			}
			n, err := s.PruneReplySpool()
			want := 0
			if state == "sent" {
				want = 1
			}
			if err != nil || n != want {
				t.Fatal("wrong retention", n, err)
			}
		})
	}
}

func TestRichFollowupOutputCommitIsAtomic(t *testing.T) {
	s, _, cl, _ := outputFixture(t)
	id := deliverFollowupInitial(t, s, cl)
	_, err := s.call(func(db *storeConn) (any, error) {
		_, err := db.Exec(`CREATE TRIGGER fail_followup_output BEFORE INSERT ON reply_followup_outputs BEGIN SELECT RAISE(ABORT,'test failure'); END`)
		return nil, err
	})
	if err != nil {
		t.Fatal(err)
	}
	manifest := []byte(`{"version":1,"text":"result","embeds":[{"title":"Result"}]}`)
	if _, err = s.QueueFollowupManifest(cl.InboundID, cl.Claim, "result", manifest); err == nil {
		t.Fatal("accepted failed output commit")
	}
	d, err := s.Delivery(id)
	if err != nil || d.State != "sent" || len(d.Chunks) != 1 {
		t.Fatal("text-only chunks survived rollback", d, err)
	}
	_, err = s.call(func(db *storeConn) (any, error) {
		var count int
		if err := db.QueryRow("SELECT count(*) FROM reply_followups WHERE reply_id=?", id).Scan(&count); err != nil {
			return nil, err
		}
		if count != 0 {
			return nil, errors.New("followup key survived rollback")
		}
		_, err := db.Exec("DROP TRIGGER fail_followup_output")
		return nil, err
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.QueueFollowupManifest(cl.InboundID, cl.Claim, "result", manifest); err != nil {
		t.Fatal(err)
	}
}
