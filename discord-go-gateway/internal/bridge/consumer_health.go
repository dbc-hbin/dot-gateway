package bridge

import (
	"database/sql"
	"encoding/json"
	"errors"
	"sync"
	"time"
)

const consumerPollPrefix = "consumer_poll:"
const ConsumerPollHeartbeatInterval = 10 * time.Second
const consumerPollStaleSeconds = 25.0
const pendingBacklogWarningSeconds = 15.0
const maxConsumerPollRecords = 128

// ConsumerPoll records only the liveness of one waiting CLI invocation. It does
// not establish that the reasoning consumer is alive or received CLI stdout.
// Every invocation has its own key, so overlapping calls cannot erase or renew
// one another. The existing consumer identity remains the claim recovery key.
type ConsumerPoll struct {
	store     *Store
	key       string
	consumer  string
	deadline  float64
	mu        sync.Mutex
	lastWrite time.Time
	closed    bool
}

type consumerPollRecord struct {
	Consumer  string  `json:"consumer"`
	UpdatedAt float64 `json:"updated_at"`
	Deadline  float64 `json:"deadline"`
}

func (s *Store) StartConsumerPoll(consumer string, deadline time.Time) (*ConsumerPoll, error) {
	id, err := uuidHex()
	if err != nil {
		return nil, err
	}
	p := &ConsumerPoll{store: s, key: consumerPollPrefix + id, consumer: consumer, deadline: float64(deadline.UnixNano()) / 1e9}
	now := epoch()
	raw, err := json.Marshal(consumerPollRecord{consumer, now, p.deadline})
	if err != nil {
		return nil, err
	}
	_, err = s.call(func(db *storeConn) (any, error) {
		return transact(db, func(db *storeConn) (any, error) {
			// Prune only our own expired observation rows; never touch claims or work.
			_, e := db.Exec(`DELETE FROM runtime WHERE key GLOB 'consumer_poll:*' AND CASE WHEN json_valid(value) THEN COALESCE(json_extract(value,'$.updated_at'),0)<? OR COALESCE(json_extract(value,'$.deadline'),0)<? ELSE 1 END`, now-consumerPollStaleSeconds, now)
			if e != nil {
				return nil, e
			}
			var n int
			if e = db.QueryRow("SELECT count(*) FROM runtime WHERE key GLOB 'consumer_poll:*'").Scan(&n); e != nil {
				return nil, e
			}
			if n >= maxConsumerPollRecords {
				return nil, errors.New("consumer_poll_observation_capacity")
			}
			_, e = db.Exec("INSERT INTO runtime(key,value) VALUES(?,?)", p.key, string(raw))
			return nil, e
		})
	})
	if err != nil {
		return nil, err
	}
	p.lastWrite = time.Now()
	return p, nil
}

// Touch is cheap on every poll iteration and persists at most once per 10s.
// UPDATE rather than upsert prevents a pruned/finished invocation reappearing.
func (p *ConsumerPoll) Touch() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || time.Since(p.lastWrite) < ConsumerPollHeartbeatInterval {
		return nil
	}
	now := epoch()
	raw, err := json.Marshal(consumerPollRecord{p.consumer, now, p.deadline})
	if err != nil {
		return err
	}
	_, err = p.store.call(func(db *storeConn) (any, error) {
		_, e := db.Exec("UPDATE runtime SET value=? WHERE key=?", string(raw), p.key)
		return nil, e
	})
	// Also bound retries when an observation write fails; ledger claims are not
	// dependent on this diagnostic, and absent/stale telemetry fails closed.
	p.lastWrite = time.Now()
	return err
}
func (p *ConsumerPoll) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	p.closed = true
	_, err := p.store.call(func(db *storeConn) (any, error) {
		_, e := db.Exec("DELETE FROM runtime WHERE key=?", p.key)
		return nil, e
	})
	return err
}

func consumerHealth(db *storeConn, now float64) (map[string]any, error) {
	rows, err := db.Query("SELECT value FROM runtime WHERE key GLOB 'consumer_poll:*'")
	if err != nil {
		return nil, err
	}
	fresh, stale, invalid := 0, 0, 0
	for rows.Next() {
		var raw string
		if err = rows.Scan(&raw); err != nil {
			rows.Close()
			return nil, err
		}
		var p consumerPollRecord
		if json.Unmarshal([]byte(raw), &p) != nil || p.UpdatedAt <= 0 || p.Deadline <= 0 || p.UpdatedAt > now+5 {
			invalid++
			continue
		}
		if now-p.UpdatedAt <= consumerPollStaleSeconds && now < p.Deadline {
			fresh++
		} else {
			stale++
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	var pending, claimed, expired, processing int
	var oldest sql.NullFloat64
	if err = db.QueryRow("SELECT count(*),min(created) FROM inbound WHERE state='pending'").Scan(&pending, &oldest); err != nil {
		return nil, err
	}
	if err = db.QueryRow("SELECT count(*) FROM inbound WHERE state='claimed' AND lease_until>?", now).Scan(&claimed); err != nil {
		return nil, err
	}
	if err = db.QueryRow("SELECT count(*) FROM inbound WHERE state='claimed' AND lease_until<=?", now).Scan(&expired); err != nil {
		return nil, err
	}
	if err = db.QueryRow("SELECT count(*) FROM processing p JOIN inbound i ON i.id=p.inbound_id AND i.claim=p.claim WHERE i.state='claimed' AND i.lease_until>? AND p.until>?", now, now).Scan(&processing); err != nil {
		return nil, err
	}
	var oldestAge any
	overdue := false
	if oldest.Valid {
		age := now - oldest.Float64
		if age < 0 {
			age = 0
		}
		oldestAge = age
		overdue = age >= pendingBacklogWarningSeconds
	}
	// Validation quarantine is durable ingress work, but cannot be claimed
	// until the gateway has revalidated its exact route and permissions.
	var validationTable, validationPending, validationBlocked int
	var validationOldest sql.NullFloat64
	if err = db.QueryRow("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='ingress_validation'").Scan(&validationTable); err != nil {
		return nil, err
	}
	if validationTable > 0 {
		if err = db.QueryRow("SELECT COALESCE(sum(state='pending'),0),COALESCE(sum(state='blocked'),0),min(created) FROM ingress_validation WHERE state IN ('pending','blocked')").Scan(&validationPending, &validationBlocked, &validationOldest); err != nil {
			return nil, err
		}
	}
	var validationAge any
	validationOverdue := false
	if validationOldest.Valid {
		age := now - validationOldest.Float64
		if age < 0 {
			age = 0
		}
		validationAge = age
		validationOverdue = age >= pendingBacklogWarningSeconds
	}
	var lastReply sql.NullFloat64
	if err = db.QueryRow("SELECT max(created) FROM replies").Scan(&lastReply); err != nil {
		return nil, err
	}
	var replyAt any
	if lastReply.Valid {
		replyAt = lastReply.Float64
	}
	state := "absent"
	switch {
	case fresh > 0:
		state = "waiting_cli"
	case claimed > 0:
		state = "claim_committed"
	case stale > 0:
		state = "stale_cli"
	case invalid > 0:
		state = "unknown"
	}
	return map[string]any{
		"state": state, "waiting_cli_fresh": fresh > 0, "fresh_waiting_calls": fresh, "stale_waiting_calls": stale, "invalid_observations": invalid,
		"active_claims": claimed, "expired_claims": expired, "active_processing_leases": processing,
		"processing_lease_is_model_liveness": false, "reasoning_liveness": "not_observable", "result_receipt": "not_observable",
		"pending_count": pending, "oldest_pending_age_seconds": oldestAge, "pending_backlog_warning_seconds": pendingBacklogWarningSeconds, "backlog_overdue": overdue,
		"validation_pending": validationPending, "validation_blocked": validationBlocked, "oldest_validation_age_seconds": validationAge, "validation_backlog_overdue": validationOverdue,
		"last_reply_queued_at": replyAt, "heartbeat_interval_seconds": ConsumerPollHeartbeatInterval.Seconds(), "heartbeat_stale_after_seconds": consumerPollStaleSeconds,
	}, nil
}
