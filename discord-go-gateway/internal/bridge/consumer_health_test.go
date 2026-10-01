package bridge

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func healthStatus(t *testing.T, s *Store) (map[string]any, map[string]any) {
	t.Helper()
	out, e := s.Status()
	if e != nil {
		t.Fatal(e)
	}
	return out["consumer"].(map[string]any), out["response_path"].(map[string]any)
}
func setPollObservation(t *testing.T, s *Store, key string, updated, deadline float64) {
	t.Helper()
	raw, _ := json.Marshal(consumerPollRecord{"offline", updated, deadline})
	_, e := s.call(func(db *storeConn) (any, error) {
		_, err := db.Exec("INSERT OR REPLACE INTO runtime VALUES(?,?)", key, string(raw))
		return nil, err
	})
	if e != nil {
		t.Fatal(e)
	}
}
func TestConsumerHealthAbsentStaleAndPendingBacklog(t *testing.T) {
	s, _ := testStore(t)
	if e := s.SetRuntime("connected", nil, nil); e != nil {
		t.Fatal(e)
	}
	h, path := healthStatus(t, s)
	if h["state"] != "absent" || h["waiting_cli_fresh"] != false || path["state"] != "consumer_unavailable" || path["end_to_end_ready"] != false {
		t.Fatal(h, path)
	}
	setPollObservation(t, s, consumerPollPrefix+"old", epoch()-30, epoch()+300)
	h, _ = healthStatus(t, s)
	if h["state"] != "stale_cli" || h["waiting_cli_fresh"] != false {
		t.Fatal(h)
	}
	ingestLedger(t, s, "one", "c")
	_, e := s.call(func(db *storeConn) (any, error) {
		_, err := db.Exec("UPDATE inbound SET created=?", epoch()-20)
		return nil, err
	})
	if e != nil {
		t.Fatal(e)
	}
	h, path = healthStatus(t, s)
	if h["pending_count"] != 1 || h["backlog_overdue"] != true || h["oldest_pending_age_seconds"].(float64) < 20 || path["state"] != "degraded_backlog" {
		t.Fatal(h, path)
	}
	p, e := s.StartConsumerPoll("offline", time.Now().Add(time.Minute))
	if e != nil {
		t.Fatal(e)
	}
	defer p.Close()
	h, path = healthStatus(t, s)
	if h["state"] != "waiting_cli" || h["waiting_cli_fresh"] != true || path["state"] != "degraded_backlog" || path["end_to_end_ready"] != false {
		t.Fatal(h, path)
	}
}
func TestConsumerPollOverlapCleanupAndBoundedHeartbeat(t *testing.T) {
	s, _ := testStore(t)
	first, e := s.StartConsumerPoll("same-consumer", time.Now().Add(time.Minute))
	if e != nil {
		t.Fatal(e)
	}
	second, e := s.StartConsumerPoll("same-consumer", time.Now().Add(time.Minute))
	if e != nil {
		t.Fatal(e)
	}
	defer second.Close()
	if first.key == second.key {
		t.Fatal("instance identity reused")
	}
	h, _ := healthStatus(t, s)
	if h["fresh_waiting_calls"] != 2 {
		t.Fatal(h)
	}
	var before string
	_, e = s.call(func(db *storeConn) (any, error) {
		return nil, db.QueryRow("SELECT value FROM runtime WHERE key=?", second.key).Scan(&before)
	})
	if e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 20; i++ {
		if e = second.Touch(); e != nil {
			t.Fatal(e)
		}
	}
	var after string
	_, e = s.call(func(db *storeConn) (any, error) {
		return nil, db.QueryRow("SELECT value FROM runtime WHERE key=?", second.key).Scan(&after)
	})
	if e != nil {
		t.Fatal(e)
	}
	if before != after {
		t.Fatal("heartbeat persisted more often than interval")
	}
	if e = first.Close(); e != nil {
		t.Fatal(e)
	}
	if e = first.Touch(); e != nil {
		t.Fatal(e)
	}
	if e = first.Close(); e != nil {
		t.Fatal(e)
	}
	h, _ = healthStatus(t, s)
	if h["fresh_waiting_calls"] != 1 {
		t.Fatal("old invocation clobbered successor", h)
	}
	if e = second.Close(); e != nil {
		t.Fatal(e)
	}
	h, _ = healthStatus(t, s)
	if h["state"] != "absent" || h["fresh_waiting_calls"] != 0 {
		t.Fatal(h)
	}
}
func TestConsumerHealthClaimRecoveryAndTerminalLease(t *testing.T) {
	s, _ := testStore(t)
	s.SetRuntime("connected", nil, nil)
	ingestLedger(t, s, "one", "c")
	first, e := s.ClaimNextForConsumer(300, 60, "stable")
	if e != nil || first == nil {
		t.Fatal(first, e)
	}
	recovered, e := s.ClaimNextForConsumer(300, 60, "stable")
	if e != nil || recovered.Claim != first.Claim {
		t.Fatal(recovered, e)
	}
	h, path := healthStatus(t, s)
	if h["state"] != "claim_committed" || h["active_claims"] != 1 || h["active_processing_leases"] != 1 || h["processing_lease_is_model_liveness"] != false || h["reasoning_liveness"] != "not_observable" || h["waiting_cli_fresh"] != false || path["end_to_end_ready"] != false {
		t.Fatal(h, path)
	}
	if _, e = s.QueueReply(first.InboundID, first.Claim, "offline reply"); e != nil {
		t.Fatal(e)
	}
	h, path = healthStatus(t, s)
	if h["state"] != "absent" || h["active_claims"] != 0 || h["active_processing_leases"] != 0 || h["last_reply_queued_at"] == nil || path["state"] != "consumer_unavailable" {
		t.Fatal(h, path)
	}
	raw, _ := json.Marshal(h)
	if strings.Contains(string(raw), "untrusted secret inbound") || strings.Contains(string(raw), first.Claim) || strings.Contains(string(raw), "offline reply") {
		t.Fatal("content or claim leaked into health")
	}
}
func TestConsumerHealthDeadlineInvalidAndExpiredClaim(t *testing.T) {
	s, _ := testStore(t)
	s.SetRuntime("connected", nil, nil)
	setPollObservation(t, s, consumerPollPrefix+"expired", epoch(), epoch()-1)
	setPollObservation(t, s, consumerPollPrefix+"future", epoch()+100, epoch()+300)
	h, _ := healthStatus(t, s)
	if h["fresh_waiting_calls"] != 0 || h["stale_waiting_calls"] != 1 || h["invalid_observations"] != 1 {
		t.Fatal(h)
	}
	ingestLedger(t, s, "one", "c")
	claimLedger(t, s)
	_, e := s.call(func(db *storeConn) (any, error) {
		_, err := db.Exec("UPDATE inbound SET lease_until=?", epoch()-1)
		return nil, err
	})
	if e != nil {
		t.Fatal(e)
	}
	h, path := healthStatus(t, s)
	if h["expired_claims"] != 1 || h["active_processing_leases"] != 0 || path["state"] != "degraded_backlog" {
		t.Fatal(h, path)
	}
}
func TestConsumerPollPruningIsScopedAndBounded(t *testing.T) {
	s, _ := testStore(t)
	setPollObservation(t, s, "consumerXpoll:foreign", epoch()-100, epoch()-1)
	setPollObservation(t, s, consumerPollPrefix+"stale", epoch()-100, epoch()-1)
	p, e := s.StartConsumerPoll("offline", time.Now().Add(time.Minute))
	if e != nil {
		t.Fatal(e)
	}
	defer p.Close()
	var foreign, old int
	_, e = s.call(func(db *storeConn) (any, error) {
		if err := db.QueryRow("SELECT count(*) FROM runtime WHERE key='consumerXpoll:foreign'").Scan(&foreign); err != nil {
			return nil, err
		}
		return nil, db.QueryRow("SELECT count(*) FROM runtime WHERE key=?", consumerPollPrefix+"stale").Scan(&old)
	})
	if e != nil || foreign != 1 || old != 0 {
		t.Fatal(foreign, old, e)
	}
	for i := 0; i < maxConsumerPollRecords-1; i++ {
		q, err := s.StartConsumerPoll("offline", time.Now().Add(time.Minute))
		if err != nil {
			t.Fatal(err)
		}
		defer q.Close()
	}
	if _, e = s.StartConsumerPoll("offline", time.Now().Add(time.Minute)); e == nil {
		t.Fatal("unbounded telemetry records")
	}
}

func TestConsumerLastReplySurvivesTimingPruning(t *testing.T) {
	s, _ := testStore(t)
	ingestLedger(t, s, "one", "c")
	c := claimLedger(t, s)
	replyLedger(t, s, c, "offline reply")
	before, _ := healthStatus(t, s)
	if before["last_reply_queued_at"] == nil {
		t.Fatal("durable reply timestamp missing")
	}
	var old int
	_, err := s.call(func(db *storeConn) (any, error) {
		return transact(db, func(db *storeConn) (any, error) {
			// Seed later samples efficiently, then exercise the actual pruning path.
			if _, err := db.Exec("WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM seq WHERE n<2048) INSERT INTO timings(stage,at,duration_ms) SELECT 'dispatch',?,0 FROM seq", epoch()); err != nil {
				return nil, err
			}
			if err := insertTiming(db, "", "dispatch", epoch(), 0); err != nil {
				return nil, err
			}
			if err := db.QueryRow("SELECT count(*) FROM timings WHERE stage='reply_queued'").Scan(&old); err != nil {
				return nil, err
			}
			return nil, nil
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	if old != 0 {
		t.Fatal("test did not evict reply timing")
	}
	after, _ := healthStatus(t, s)
	if after["last_reply_queued_at"] != before["last_reply_queued_at"] {
		t.Fatal("pruning erased durable last reply", before, after)
	}
}

func TestConsumerHealthIncludesValidationQuarantine(t *testing.T) {
	s, _ := testStore(t)
	s.SetRuntime("connected", nil, nil)
	// Works both with the additive quarantine schema and an older ledger.
	_, err := s.call(func(db *storeConn) (any, error) {
		if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS ingress_validation(id TEXT PRIMARY KEY,platform TEXT,event_id TEXT,envelope TEXT,created REAL,state TEXT DEFAULT 'pending',attempts INTEGER DEFAULT 0,next_attempt REAL DEFAULT 0,code TEXT,UNIQUE(platform,event_id))`); err != nil {
			return nil, err
		}
		_, err := db.Exec("INSERT INTO ingress_validation(id,platform,event_id,envelope,created,state) VALUES('offline','discord','3','{}',?,'pending')", epoch())
		return nil, err
	})
	if err != nil {
		t.Fatal(err)
	}
	h, path := healthStatus(t, s)
	if h["validation_pending"] != 1 || h["validation_blocked"] != 0 || h["validation_backlog_overdue"] != false || path["state"] != "validating_ingress" {
		t.Fatal(h, path)
	}
	poll, err := s.StartConsumerPoll("offline", time.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	defer poll.Close()
	_, err = s.call(func(db *storeConn) (any, error) {
		_, err := db.Exec("UPDATE ingress_validation SET created=?", epoch()-20)
		return nil, err
	})
	if err != nil {
		t.Fatal(err)
	}
	h, path = healthStatus(t, s)
	if h["waiting_cli_fresh"] != true || h["validation_pending"] != 1 || h["validation_backlog_overdue"] != true || h["oldest_validation_age_seconds"].(float64) < 20 || path["state"] != "degraded_backlog" {
		t.Fatal(h, path)
	}
	_, err = s.call(func(db *storeConn) (any, error) {
		_, err := db.Exec("UPDATE ingress_validation SET state='blocked',created=?", epoch())
		return nil, err
	})
	if err != nil {
		t.Fatal(err)
	}
	h, path = healthStatus(t, s)
	if h["validation_pending"] != 0 || h["validation_blocked"] != 1 || h["validation_backlog_overdue"] != false || path["state"] != "blocked_ingress" || path["end_to_end_ready"] != false {
		t.Fatal(h, path)
	}
}
