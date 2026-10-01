package bridge

import (
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type ledgerPolicy struct{}

func (ledgerPolicy) Allows(e Envelope) bool {
	return e.Platform == "discord" && e.SenderID == "owner" && !e.SenderIsBot
}
func (p ledgerPolicy) Accepts(e Envelope) bool { return p.Allows(e) && e.Text != "" }
func testStore(t *testing.T) (*Store, string) {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "queue.db")
	s, err := OpenStore(path, ledgerPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s, path
}
func ledgerEvent(id, conv string) Envelope {
	return Envelope{Platform: "discord", EventID: id, ConversationID: conv, SenderID: "owner", Text: "untrusted secret inbound", ReceivedAt: epoch(), RouteKind: "dm"}
}
func ingestLedger(t *testing.T, s *Store, id, conv string) {
	t.Helper()
	st, err := s.Ingest(ledgerEvent(id, conv))
	if err != nil || st != "accepted" {
		t.Fatalf("ingest %s %v", st, err)
	}
}
func claimLedger(t *testing.T, s *Store) *Claim {
	t.Helper()
	c, err := s.ClaimNext(300, 60)
	if err != nil || c == nil {
		t.Fatalf("claim %#v %v", c, err)
	}
	return c
}
func replyLedger(t *testing.T, s *Store, c *Claim, text string) string {
	t.Helper()
	id, err := s.QueueReply(c.InboundID, c.Claim, text)
	if err != nil {
		t.Fatal(err)
	}
	return id
}
func TestStoreAdmissionClaimAndImmutableBinding(t *testing.T) {
	s, _ := testStore(t)
	bad := ledgerEvent("bad", "c")
	bad.SenderID = "other"
	if st, err := s.Ingest(bad); err != nil || st != "rejected" {
		t.Fatalf("rejected %s %v", st, err)
	}
	ingestLedger(t, s, "one", "c")
	if st, err := s.Ingest(ledgerEvent("one", "c")); err != nil || st != "duplicate" {
		t.Fatalf("duplicate %s %v", st, err)
	}
	c := claimLedger(t, s)
	if c.Trust != "untrusted_message_text" {
		t.Fatal(c)
	}
	if err := s.Renew(c.InboundID, "wrong", 300); !errors.Is(err, ErrClaim) {
		t.Fatal(err)
	}
	if _, err := s.QueueReply(c.InboundID, "wrong", "response"); !errors.Is(err, ErrClaim) {
		t.Fatal(err)
	}
	id := replyLedger(t, s, c, "response")
	id2, err := s.QueueReply(c.InboundID, "expired or wrong", "response")
	if err != nil || id != id2 {
		t.Fatal(id2, err)
	}
	if _, err = s.QueueReply(c.InboundID, c.Claim, "different"); err == nil {
		t.Fatal("rebound reply")
	}
	ch, err := s.NextChunk()
	if err != nil || ch == nil || ch.Source.EventID != "one" || ch.Source.ConversationID != "c" {
		t.Fatal(ch, err)
	}
	if err = s.RecordResult(*ch, SendResult{State: "sent", MessageID: "remote"}); err != nil {
		t.Fatal(err)
	}
	d, err := s.Delivery(id)
	if err != nil || d.State != "sent" || d.Chunks[0].Attempts != 1 {
		t.Fatal(d, err)
	}
}
func TestStoreConcurrentClaimsAndClose(t *testing.T) {
	s, _ := testStore(t)
	for i := 0; i < 10; i++ {
		ingestLedger(t, s, string(rune('a'+i)), "same")
	}
	var wg sync.WaitGroup
	var claimed atomic.Int32
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, err := s.ClaimNext(300, 0)
			if err != nil {
				t.Error(err)
			}
			if c != nil {
				claimed.Add(1)
			}
		}()
	}
	wg.Wait()
	if claimed.Load() != 1 {
		t.Fatalf("claims=%d", claimed.Load())
	}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _ = s.Close() }()
	}
	wg.Wait()
	if _, err := s.Status(); !errors.Is(err, ErrStoreClosed) {
		t.Fatal(err)
	}
}
func TestStoreLeaseReclaimAndProcessing(t *testing.T) {
	s, _ := testStore(t)
	ingestLedger(t, s, "one", "c")
	c := claimLedger(t, s)
	rows, err := s.FeedbackRows()
	if err != nil || len(rows) != 1 || !rows[0].Thinking {
		t.Fatal(rows, err)
	}
	_, err = s.call(func(db *storeConn) (any, error) {
		_, err := db.Exec("UPDATE inbound SET lease_until=0 WHERE id=?", c.InboundID)
		return nil, err
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Renew(c.InboundID, c.Claim, 300); !errors.Is(err, ErrClaim) {
		t.Fatal(err)
	}
	newClaim := claimLedger(t, s)
	if newClaim.Claim == c.Claim {
		t.Fatal("reused claim")
	}
	if err = s.Ignore(c.InboundID, c.Claim); !errors.Is(err, ErrClaim) {
		t.Fatal(err)
	}
	if err = s.Ignore(newClaim.InboundID, newClaim.Claim); err != nil {
		t.Fatal(err)
	}
}
func TestStoreChunkOrderRecoveryAndNoUncertainRetry(t *testing.T) {
	s, path := testStore(t)
	ingestLedger(t, s, "one", "c")
	first := replyLedger(t, s, claimLedger(t, s), strings.Repeat("x", 2000))
	ingestLedger(t, s, "two", "c")
	second := replyLedger(t, s, claimLedger(t, s), "later")
	ch, err := s.NextChunk()
	if err != nil || ch == nil || ch.ReplyID != first || ch.Index != 0 {
		t.Fatal(ch, err)
	}
	if next, err := s.NextChunk(); err != nil || next != nil {
		t.Fatal("parallel order violated", next, err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = OpenStore(path, ledgerPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if n, err := s.RecoverInterrupted(); err != nil || n != 1 {
		t.Fatal(n, err)
	}
	if n, err := s.RetryFailed(first); err != nil || n != 0 {
		t.Fatal(n, err)
	}
	if next, err := s.NextChunk(); err != nil || next != nil {
		t.Fatal(next, err)
	}
	if err = s.ResolveSent(first, 0, "verified"); err != nil {
		t.Fatal(err)
	}
	ch, err = s.NextChunk()
	if err != nil || ch == nil || ch.ReplyID != first || ch.Index != 1 {
		t.Fatal(ch, err)
	}
	if err = s.RecordResult(*ch, SendResult{State: "failed", Code: "HTTP response contains sensitive content"}); err != nil {
		t.Fatal(err)
	}
	d, _ := s.Delivery(first)
	if d.State != "failed" || d.Chunks[1].Code == nil || *d.Chunks[1].Code != "" {
		t.Fatal(d)
	}
	if n, err := s.RetryFailed(first); err != nil || n != 1 {
		t.Fatal(n, err)
	}
	ch, _ = s.NextChunk()
	if err = s.RecordResult(*ch, SendResult{State: "sent", MessageID: "ack"}); err != nil {
		t.Fatal(err)
	}
	ch, err = s.NextChunk()
	if err != nil || ch == nil || ch.ReplyID != second {
		t.Fatal(ch, err)
	}
	d, _ = s.Delivery(first)
	if d.Chunks[0].Attempts != 1 || d.Chunks[1].Attempts != 2 {
		t.Fatal(d)
	}
}
func TestStoreReplyTransactionRollback(t *testing.T) {
	s, _ := testStore(t)
	ingestLedger(t, s, "one", "c")
	c := claimLedger(t, s)
	_, err := s.call(func(db *storeConn) (any, error) {
		_, err := db.Exec("CREATE TRIGGER abort_chunk BEFORE INSERT ON chunks WHEN NEW.idx=1 BEGIN SELECT RAISE(ABORT,'injected'); END")
		return nil, err
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.QueueReply(c.InboundID, c.Claim, strings.Repeat("x", 2000)); err == nil {
		t.Fatal("expected trigger failure")
	}
	_, err = s.call(func(db *storeConn) (any, error) {
		var n int
		if err := db.QueryRow("SELECT count(*) FROM replies").Scan(&n); err != nil {
			return nil, err
		}
		if n != 0 {
			t.Errorf("partial reply committed")
		}
		if _, err := db.Exec("DROP TRIGGER abort_chunk"); err != nil {
			return nil, err
		}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	replyLedger(t, s, c, "retry after safe local rollback")
}
func TestStoreMigrationPreservesLegacyHistory(t *testing.T) {
	s, path := testStore(t)
	ingestLedger(t, s, "one", "c")
	c := claimLedger(t, s)
	if err := s.Ignore(c.InboundID, c.Claim); err != nil {
		t.Fatal(err)
	}
	ingestLedger(t, s, "two", "c")
	_, err := s.call(func(db *storeConn) (any, error) {
		_, err := db.Exec("DROP TABLE feedback; DROP TABLE timings")
		return nil, err
	})
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
	s, err = OpenStore(path, ledgerPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	rows, err := s.FeedbackRows()
	if err != nil || len(rows) != 1 || rows[0].Event.EventID != "two" {
		t.Fatal(rows, err)
	}
	var history string
	_, err = s.call(func(db *storeConn) (any, error) {
		return nil, db.QueryRow("SELECT stage FROM feedback WHERE inbound_id=?", c.InboundID).Scan(&history)
	})
	if err != nil || history != "history" {
		t.Fatal(history, err)
	}
}
func TestStoreMigrationFailureRollsBack(t *testing.T) {
	dir := t.TempDir()
	os.Chmod(dir, 0700)
	path := filepath.Join(dir, "queue.db")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec("CREATE TABLE chunks(broken TEXT)"); err != nil {
		t.Fatal(err)
	}
	db.Close()
	if s, err := OpenStore(path, ledgerPolicy{}); err == nil {
		s.Close()
		t.Fatal("invalid schema migration succeeded")
	}
	db, err = sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var n int
	if err = db.QueryRow("SELECT count(*) FROM sqlite_master WHERE type='table' AND name IN ('inbound','processing','feedback','timings')").Scan(&n); err != nil || n != 0 {
		t.Fatal("partial migration persisted", n, err)
	}
}
func TestStoreRuntimeFeedbackAndBoundedTimings(t *testing.T) {
	s, _ := testStore(t)
	status, err := s.Status()
	if err != nil || status["gateway"].(map[string]any)["heartbeat_stale"] != true {
		t.Fatal(status, err)
	}
	if err = s.SetRuntime("ready", map[string]int{"reconnect": 1}, map[string]any{"state": "should_not_override"}); err != nil {
		t.Fatal(err)
	}
	status, err = s.Status()
	if err != nil || status["gateway"].(map[string]any)["state"] != "ready" || status["gateway"].(map[string]any)["heartbeat_stale"] != false {
		t.Fatal(status, err)
	}
	ingestLedger(t, s, "one", "c")
	c := claimLedger(t, s)
	if err = s.RecordFeedback(c.InboundID, "sent"); err != nil {
		t.Fatal(err)
	}
	rows, err := s.FeedbackRows()
	if err != nil || len(rows) != 0 {
		t.Fatal(rows, err)
	}
	if err = s.RecordTiming("private inbound text", time.Second); err == nil {
		t.Fatal("unbounded timing label")
	}
	_, err = s.call(func(db *storeConn) (any, error) {
		_, err := db.Exec("DELETE FROM timings; WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM seq WHERE n<2048) INSERT INTO timings(inbound_id,stage,at,duration_ms) SELECT ?, 'send', ?, 1 FROM seq", c.InboundID, epoch())
		return nil, err
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 7; i++ {
		if err = s.RecordMessageTiming(c.InboundID, "send", epoch(), time.Millisecond); err != nil {
			t.Fatal(err)
		}
	}
	_, err = s.call(func(db *storeConn) (any, error) {
		var n int
		err := db.QueryRow("SELECT count(*) FROM timings").Scan(&n)
		if n != 2048 {
			t.Errorf("timings count %d", n)
		}
		return nil, err
	})
	if err != nil {
		t.Fatal(err)
	}
}
func TestStoreRejectsInsecurePaths(t *testing.T) {
	dir := t.TempDir()
	os.Chmod(dir, 0755)
	if s, err := OpenStore(filepath.Join(dir, "bad.db"), ledgerPolicy{}); err == nil {
		s.Close()
		t.Fatal("public directory allowed")
	}
	os.Chmod(dir, 0700)
	target := filepath.Join(dir, "target")
	os.WriteFile(target, []byte{}, 0600)
	link := filepath.Join(dir, "link")
	os.Symlink(target, link)
	if s, err := OpenStore(link, ledgerPolicy{}); err == nil {
		s.Close()
		t.Fatal("symlink allowed")
	}
}

func TestStoreSeparateActorsAtomicClaim(t *testing.T) {
	s, path := testStore(t)
	other, err := OpenStore(path, ledgerPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	ingestLedger(t, s, "one", "c")
	var wg sync.WaitGroup
	var claims atomic.Int32
	for _, store := range []*Store{s, other} {
		wg.Add(1)
		go func(s *Store) {
			defer wg.Done()
			c, err := s.ClaimNext(300, 0)
			if err != nil {
				t.Error(err)
			}
			if c != nil {
				claims.Add(1)
			}
		}(store)
	}
	wg.Wait()
	if claims.Load() != 1 {
		t.Fatalf("cross-connection double claim: %d", claims.Load())
	}
}
func TestStoreAutomaticContentFreeLifecycleTimings(t *testing.T) {
	s, _ := testStore(t)
	ingestLedger(t, s, "one", "c")
	c := claimLedger(t, s)
	replyLedger(t, s, c, "secret outbound")
	ch, err := s.NextChunk()
	if err != nil || ch == nil {
		t.Fatal(ch, err)
	}
	if err = s.RecordResult(*ch, SendResult{State: "sent", MessageID: "ack"}); err != nil {
		t.Fatal(err)
	}
	status, err := s.Status()
	if err != nil {
		t.Fatal(err)
	}
	timings := status["timings"].(map[string]any)
	for _, stage := range []string{"ingested", "claimed", "reply_queued", "send_started", "send_completed", "end_to_end"} {
		if _, ok := timings[stage]; !ok {
			t.Errorf("missing timing %s", stage)
		}
	}
	_, err = s.call(func(db *storeConn) (any, error) {
		rows, err := db.Query("SELECT stage,inbound_id,duration_ms FROM timings")
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		for rows.Next() {
			var stage, id string
			var ms float64
			if err = rows.Scan(&stage, &id, &ms); err != nil {
				return nil, err
			}
			if !timingStages[stage] || id != c.InboundID || ms < 0 {
				t.Errorf("invalid timing record")
			}
		}
		return nil, rows.Err()
	})
	if err != nil {
		t.Fatal(err)
	}
}
func TestStoreLegacyTestSendStatusReadOnly(t *testing.T) {
	s, _ := testStore(t)
	cfg := Settings{ExpectedBotID: "222222222222222222", Policy: Policy{OwnerID: "111111111111111111", GuildID: "333333333333333333", GuildChannelID: "444444444444444444"}}
	status, err := s.TestSendStatus(cfg)
	if err != nil || status["state"] != "not_started" {
		t.Fatal(status, err)
	}
	_, err = s.call(func(db *storeConn) (any, error) {
		var n int
		if err := db.QueryRow("SELECT count(*) FROM sqlite_master WHERE name='test_sends'").Scan(&n); err != nil {
			return nil, err
		}
		if n != 0 {
			t.Error("status created schema")
		}
		_, err := db.Exec(`CREATE TABLE test_sends(bot_id TEXT,guild_id TEXT,channel_id TEXT,state TEXT,attempted INTEGER,message_id TEXT,code TEXT,nonce TEXT,created REAL,updated REAL,verified_at REAL); INSERT INTO test_sends VALUES('222222222222222222','333333333333333333','444444444444444444','attempted',1,NULL,NULL,'stable',1,2,NULL)`)
		return nil, err
	})
	if err != nil {
		t.Fatal(err)
	}
	status, err = s.TestSendStatus(cfg)
	if err != nil || status["state"] != "uncertain" || status["code"] != "in_progress_or_interrupted_attempted" || status["attempted"] != true {
		t.Fatal(status, err)
	}
	_, err = s.call(func(db *storeConn) (any, error) {
		var state string
		if err := db.QueryRow("SELECT state FROM test_sends").Scan(&state); err != nil {
			return nil, err
		}
		if state != "attempted" {
			t.Error("status mutated persisted state")
		}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
