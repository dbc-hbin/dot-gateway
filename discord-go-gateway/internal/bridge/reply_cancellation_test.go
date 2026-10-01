package bridge

import (
	"reflect"
	"strings"
	"testing"
)

func TestCancelFailedReplyAdvancesConversationDurably(t *testing.T) {
	s, path := testStore(t)
	ingestLedger(t, s, "deleted-original", "same")
	first := replyLedger(t, s, claimLedger(t, s), strings.Repeat("a", 3801))
	ingestLedger(t, s, "later", "same")
	second := replyLedger(t, s, claimLedger(t, s), "next answer")
	chunk, err := s.NextChunk()
	if err != nil || chunk == nil || chunk.ReplyID != first {
		t.Fatal(chunk, err)
	}
	if err := s.RecordResult(*chunk, SendResult{State: "failed", Code: "http_400"}); err != nil {
		t.Fatal(err)
	}
	before, err := s.Delivery(first)
	if err != nil {
		t.Fatal(err)
	}
	if next, err := s.NextChunk(); err != nil || next != nil {
		t.Fatal("failed head must remain blocked until explicit cancellation", next, err)
	}
	d, err := s.CancelReply(first)
	if err != nil || d.State != "cancelled" || !reflect.DeepEqual(d.Chunks[0], before.Chunks[0]) {
		t.Fatal("cancellation changed failed evidence", d, before, err)
	}
	for _, c := range d.Chunks[1:] {
		if c.State != "cancelled" || c.Attempts != 0 || c.MessageID != nil {
			t.Fatal("unsent suffix was not cancelled", c)
		}
	}
	if again, err := s.CancelReply(first); err != nil || !reflect.DeepEqual(again, d) {
		t.Fatal("cancellation is not idempotent", again, err)
	}
	if d.Cancellation == nil || d.Cancellation.Code != "operator_cancelled" || d.Cancellation.At <= 0 {
		t.Fatal("missing durable cancellation reason", d)
	}
	if status, err := s.Status(); err != nil || status["cancelled_replies"] != 1 {
		t.Fatal("status hides cancellation", status, err)
	}
	if n, err := s.RetryFailed(first); err == nil || n != 0 {
		t.Fatal("cancelled failed chunk resurrected", n, err)
	}
	if err := s.ResolveSent(first, 0, "invented"); err == nil {
		t.Fatal("failed cancellation marked sent")
	}
	if rows, err := s.FeedbackRows(); err != nil || rows[0].Delivery != "failed" || rows[0].Thinking {
		t.Fatal("cancellation must not report success or typing", rows, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenStore(path, ledgerPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if after, err := reopened.Delivery(first); err != nil || !reflect.DeepEqual(d, after) {
		t.Fatal("restart changed cancellation", after, err)
	}
	if n, err := reopened.RetryFailed(first); err == nil || n != 0 {
		t.Fatal("restart resurrected cancellation", n, err)
	}
	next, err := reopened.NextChunk()
	if err != nil || next == nil || next.ReplyID != second || next.Source.EventID != "later" || next.Source.ConversationID != "same" {
		t.Fatal("later reply did not retain exact route", next, err)
	}
}

func TestCancelReplyPreservesSentPrefixAndRejectsAmbiguous(t *testing.T) {
	for _, state := range []string{"pending", "sending", "uncertain", "failed", "sent"} {
		t.Run(state, func(t *testing.T) {
			s, _ := testStore(t)
			ingestLedger(t, s, "one", "same")
			id := replyLedger(t, s, claimLedger(t, s), strings.Repeat("a", 2000))
			first, _ := s.NextChunk()
			if err := s.RecordResult(*first, SendResult{State: "sent", MessageID: "verified-first"}); err != nil {
				t.Fatal(err)
			}
			if state != "pending" {
				second, _ := s.NextChunk()
				if state != "sending" {
					if err := s.RecordResult(*second, SendResult{State: state, MessageID: map[string]string{"sent": "verified-second"}[state], Code: "test_result"}); err != nil {
						t.Fatal(err)
					}
				}
			}
			before, _ := s.Delivery(id)
			d, err := s.CancelReply(id)
			if state == "pending" || state == "failed" {
				if err != nil || d.State != "cancelled" || !reflect.DeepEqual(before.Chunks[0], d.Chunks[0]) {
					t.Fatal("sent prefix changed", before, d, err)
				}
			} else {
				if err == nil {
					t.Fatal("cancelled unsafe or completed reply", state)
				}
				after, _ := s.Delivery(id)
				if !reflect.DeepEqual(before, after) {
					t.Fatal("rejected cancellation mutated ledger", before, after)
				}
			}
		})
	}
}

func TestCancelReplyTransactionRollbackAndOrdering(t *testing.T) {
	s, _ := testStore(t)
	ingestLedger(t, s, "first", "same")
	first := replyLedger(t, s, claimLedger(t, s), strings.Repeat("a", 2000))
	ingestLedger(t, s, "later", "same")
	replyLedger(t, s, claimLedger(t, s), "later")
	_, err := s.call(func(db *storeConn) (any, error) {
		_, err := db.Exec("CREATE TRIGGER fail_cancel BEFORE UPDATE ON chunks WHEN NEW.state='cancelled' BEGIN SELECT RAISE(ABORT,'test_failure'); END")
		return nil, err
	})
	if err != nil {
		t.Fatal(err)
	}
	before, _ := s.Delivery(first)
	if _, err := s.CancelReply(first); err == nil {
		t.Fatal("missing injected failure")
	}
	after, _ := s.Delivery(first)
	if !reflect.DeepEqual(before, after) {
		t.Fatal("partial cancellation commit", before, after)
	}
	next, err := s.NextChunk()
	if err != nil || next == nil || next.ReplyID != first {
		t.Fatal("rollback released later reply", next, err)
	}
	if _, err := s.CancelReply("unknown"); err == nil {
		t.Fatal("unknown reply accepted")
	}
}

func TestCancelReplyRacesDispatcherAtomically(t *testing.T) {
	for i := 0; i < 12; i++ {
		s, path := testStore(t)
		other, err := OpenStore(path, ledgerPolicy{})
		if err != nil {
			t.Fatal(err)
		}
		ingestLedger(t, s, "one", "same")
		id := replyLedger(t, s, claimLedger(t, s), "answer")
		start := make(chan struct{})
		cancelled := make(chan error, 1)
		go func() {
			<-start
			_, err := other.CancelReply(id)
			cancelled <- err
		}()
		close(start)
		chunk, err := s.NextChunk()
		if err != nil {
			t.Fatal(err)
		}
		cancelErr := <-cancelled
		d, err := s.Delivery(id)
		if err != nil {
			t.Fatal(err)
		}
		if chunk == nil {
			if cancelErr != nil || d.State != "cancelled" || d.Chunks[0].Attempts != 0 {
				t.Fatal("cancel won but dispatch state changed", d, cancelErr)
			}
		} else if cancelErr == nil || d.State != "sending" || d.Cancellation != nil || d.Chunks[0].Attempts != 1 {
			t.Fatal("dispatch won but in-flight chunk was cancelled", d, cancelErr)
		}
		other.Close()
		s.Close()
	}
}
