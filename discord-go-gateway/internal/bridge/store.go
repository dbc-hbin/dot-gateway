package bridge

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"
	"unicode"

	_ "modernc.org/sqlite"
)

var ErrStoreClosed = errors.New("store closed")
var ErrClaim = errors.New("claim expired or is not owned by this consumer")

type Claim struct {
	InboundID  string        `json:"inbound_id"`
	Claim      string        `json:"claim"`
	LeaseUntil float64       `json:"lease_until"`
	Trust      string        `json:"trust"`
	Envelope   Envelope      `json:"envelope"`
	Memory     *MemoryRecall `json:"memory,omitempty"`
}
type DeliveryChunk struct {
	OutputReceipt ReplyOutputReceipt `json:"output_receipt,omitempty"`
	Index         int                `json:"idx"`
	State         string             `json:"state"`
	MessageID     *string            `json:"message_id"`
	Code          *string            `json:"code"`
	Attempts      int                `json:"attempts"`
}
type Delivery struct {
	ReplyID      string             `json:"reply_id"`
	InboundID    string             `json:"inbound_id"`
	State        string             `json:"state"`
	Chunks       []DeliveryChunk    `json:"chunks"`
	Cancellation *ReplyCancellation `json:"cancellation,omitempty"`
}
type FeedbackRow struct {
	ID              string   `json:"id"`
	State           string   `json:"state"`
	Claim           string   `json:"claim"`
	LeaseUntil      float64  `json:"lease_until"`
	ProcessingUntil float64  `json:"processing_until"`
	ProcessingClaim string   `json:"processing_claim"`
	FeedbackStage   string   `json:"feedback_stage"`
	ReplyID         string   `json:"reply_id"`
	Event           Envelope `json:"event"`
	Delivery        string   `json:"delivery"`
	Thinking        bool     `json:"thinking"`
}

// storeConn pins the actor to one SQLite connection, including explicit transactions.
type storeConn struct {
	conn *sql.Conn
	path string
}

func (c *storeConn) Exec(q string, args ...any) (sql.Result, error) {
	return c.conn.ExecContext(context.Background(), q, args...)
}
func (c *storeConn) Query(q string, args ...any) (*sql.Rows, error) {
	return c.conn.QueryContext(context.Background(), q, args...)
}
func (c *storeConn) QueryRow(q string, args ...any) *sql.Row {
	return c.conn.QueryRowContext(context.Background(), q, args...)
}

type storeRequest struct {
	fn     func(*storeConn) (any, error)
	result chan storeResponse
}
type storeResponse struct {
	value any
	err   error
}

// Store serializes all database work in one actor. No caller ever owns the connection.
type StorePolicy interface {
	Allows(Envelope) bool
	Accepts(Envelope) bool
}
type Store struct {
	path          string
	replyStateDir string
	policy        StorePolicy
	requests      chan storeRequest
	done          chan struct{}
	mu            sync.RWMutex
	closed        bool
	closeErr      error
}

func epoch() float64 { return float64(time.Now().UnixNano()) / 1e9 }
func uuidHex() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	b[6] = (b[6] & 15) | 64
	b[8] = (b[8] & 63) | 128
	return hex.EncodeToString(b[:]), nil
}
func secureDBPath(path string) (string, error) {
	p, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	dir := filepath.Dir(p)
	if err = os.MkdirAll(dir, 0700); err != nil {
		return "", err
	}
	info, err := os.Lstat(dir)
	if err != nil {
		return "", err
	}
	if !info.IsDir() || info.Mode()&0077 != 0 {
		return "", errors.New("database directory must be private")
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok || int(st.Uid) != os.Getuid() {
		return "", errors.New("database directory must be owner-owned")
	}
	fd, err := syscall.Open(p, syscall.O_CREAT|syscall.O_RDWR|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0600)
	if err != nil {
		return "", err
	}
	f := os.NewFile(uintptr(fd), p)
	defer f.Close()
	info, err = f.Stat()
	if err != nil {
		return "", err
	}
	st, ok = info.Sys().(*syscall.Stat_t)
	if !info.Mode().IsRegular() || info.Mode()&0077 != 0 || !ok || int(st.Uid) != os.Getuid() {
		return "", errors.New("database must be a private owner-owned regular file")
	}
	return p, nil
}
func OpenStore(path string, policy StorePolicy) (*Store, error) {
	if policy == nil {
		return nil, errors.New("policy required")
	}
	p, err := secureDBPath(path)
	if err != nil {
		return nil, err
	}
	s := &Store{path: p, replyStateDir: ReplyStateDir(p), policy: policy, requests: make(chan storeRequest), done: make(chan struct{})}
	ready := make(chan error, 1)
	go s.run(p, ready)
	if err = <-ready; err != nil {
		return nil, err
	}
	return s, nil
}
func (s *Store) run(path string, ready chan error) {
	defer close(s.done)
	db, err := sql.Open("sqlite", path)
	if err != nil {
		ready <- err
		return
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	conn, connErr := db.Conn(context.Background())
	if connErr != nil {
		db.Close()
		ready <- connErr
		return
	}
	owned := &storeConn{conn: conn, path: path}
	if err = initializeStore(owned); err != nil {
		conn.Close()
		db.Close()
		ready <- err
		return
	}
	ready <- nil
	for req := range s.requests {
		v, e := req.fn(owned)
		req.result <- storeResponse{v, e}
	}
	s.closeErr = conn.Close()
	if e := db.Close(); s.closeErr == nil {
		s.closeErr = e
	}
}
func (s *Store) call(fn func(*storeConn) (any, error)) (any, error) {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return nil, ErrStoreClosed
	}
	r := storeRequest{fn: fn, result: make(chan storeResponse, 1)}
	s.requests <- r
	s.mu.RUnlock()
	out := <-r.result
	return out.value, out.err
}
func (s *Store) Close() error {
	s.mu.Lock()
	if !s.closed {
		s.closed = true
		close(s.requests)
	}
	s.mu.Unlock()
	<-s.done
	return s.closeErr
}
func transact(db *storeConn, fn func(*storeConn) (any, error), guards ...func(*storeConn) error) (any, error) {
	if _, err := db.Exec("BEGIN IMMEDIATE"); err != nil {
		return nil, err
	}
	defer db.Exec("ROLLBACK")
	v, err := fn(db)
	if err != nil {
		return nil, err
	}
	if err = sealCatchupDB(db); err != nil {
		return nil, err
	}
	for _, guard := range guards {
		if err = guard(db); err != nil {
			return nil, err
		}
	}
	if _, err = db.Exec("COMMIT"); err != nil {
		return nil, err
	}
	return v, nil
}
func initializeStore(db *storeConn) error {
	for _, p := range []string{"PRAGMA busy_timeout=5000", "PRAGMA foreign_keys=ON", "PRAGMA journal_mode=WAL", "PRAGMA synchronous=FULL"} {
		if _, err := db.Exec(p); err != nil {
			return err
		}
	}
	_, err := transact(db, func(db *storeConn) (any, error) {
		var had int
		if err := db.QueryRow("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='feedback'").Scan(&had); err != nil {
			return nil, err
		}
		_, err := db.Exec(`
CREATE TABLE IF NOT EXISTS inbound(id TEXT PRIMARY KEY,platform TEXT NOT NULL,event_id TEXT NOT NULL,envelope TEXT NOT NULL,state TEXT NOT NULL DEFAULT 'pending',claim TEXT,lease_until REAL,created REAL NOT NULL,UNIQUE(platform,event_id));
CREATE TABLE IF NOT EXISTS replies(id TEXT PRIMARY KEY,inbound_id TEXT NOT NULL UNIQUE REFERENCES inbound(id),text TEXT NOT NULL,created REAL NOT NULL);
CREATE TABLE IF NOT EXISTS chunks(reply_id TEXT NOT NULL REFERENCES replies(id),idx INTEGER NOT NULL,text TEXT NOT NULL,state TEXT NOT NULL DEFAULT 'pending',message_id TEXT,code TEXT,attempts INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(reply_id,idx));
CREATE TABLE IF NOT EXISTS reply_followups(reply_id TEXT NOT NULL REFERENCES replies(id),key TEXT NOT NULL,text TEXT NOT NULL,start_idx INTEGER NOT NULL,chunk_count INTEGER NOT NULL,PRIMARY KEY(reply_id,key));
CREATE TABLE IF NOT EXISTS reply_output_receipts(reply_id TEXT NOT NULL REFERENCES replies(id),idx INTEGER NOT NULL,receipt TEXT NOT NULL,PRIMARY KEY(reply_id,idx));
CREATE TABLE IF NOT EXISTS reply_outputs(reply_id TEXT PRIMARY KEY REFERENCES replies(id),payload TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS reply_followup_outputs(reply_id TEXT NOT NULL,idx INTEGER NOT NULL CHECK(idx>0),payload TEXT NOT NULL,PRIMARY KEY(reply_id,idx),FOREIGN KEY(reply_id,idx) REFERENCES chunks(reply_id,idx));
CREATE TABLE IF NOT EXISTS reply_cancellations(reply_id TEXT PRIMARY KEY REFERENCES replies(id),cancelled_at REAL NOT NULL,code TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS processing(inbound_id TEXT PRIMARY KEY REFERENCES inbound(id),claim TEXT NOT NULL,until REAL NOT NULL);
CREATE TABLE IF NOT EXISTS consumer_claims(consumer TEXT PRIMARY KEY,inbound_id TEXT NOT NULL REFERENCES inbound(id),claim TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS feedback(inbound_id TEXT PRIMARY KEY REFERENCES inbound(id),stage TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS runtime(key TEXT PRIMARY KEY,value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS timings(id INTEGER PRIMARY KEY AUTOINCREMENT,inbound_id TEXT,stage TEXT NOT NULL,at REAL NOT NULL,duration_ms REAL NOT NULL);
CREATE TABLE IF NOT EXISTS send_measurements(id INTEGER PRIMARY KEY AUTOINCREMENT,reply_id TEXT NOT NULL,chunk_index INTEGER NOT NULL,attempt INTEGER NOT NULL,at REAL NOT NULL,measurement TEXT NOT NULL,UNIQUE(reply_id,chunk_index,attempt));
CREATE INDEX IF NOT EXISTS inbound_claim_order ON inbound(state,created,id);
CREATE INDEX IF NOT EXISTS chunks_state ON chunks(state,reply_id,idx);
`)
		if err != nil {
			return nil, err
		}
		if err = initMessageOperations(db); err != nil {
			return nil, err
		}
		if err = initWorkerControl(db); err != nil {
			return nil, err
		}
		if err = initControlStore(db); err != nil {
			return nil, err
		}
		if err = initIngressValidation(db); err != nil {
			return nil, err
		}
		if err = initReadRouteRegistry(db); err != nil {
			return nil, err
		}
		if err = initCatchup(db); err != nil {
			return nil, err
		}
		if err = initMessageSources(db); err != nil {
			return nil, err
		}
		memoryBestEffort(db, func() error { return initMemory(db) })
		if had == 0 {
			_, err = db.Exec(`INSERT OR IGNORE INTO feedback SELECT i.id,'history' FROM inbound i WHERE i.state IN ('ignored','blocked') OR (i.state='replied' AND NOT EXISTS(SELECT 1 FROM chunks c JOIN replies r ON r.id=c.reply_id WHERE r.inbound_id=i.id AND c.state IN ('pending','sending')))`)
		}
		return nil, err
	})
	return err
}
func (s *Store) Ingest(e Envelope) (string, error) {
	// Even a caller supplying claimed thread metadata cannot bypass REST
	// validation. All thread arrivals enter the same durable quarantine.
	if e.RouteKind == "guild_thread" || e.RouteKind == "guild_thread_candidate" {
		return s.StageIngress(e)
	}
	if !s.policy.Accepts(e) {
		return "rejected", nil
	}
	v, err := s.call(func(db *storeConn) (any, error) {
		return transact(db, func(db *storeConn) (any, error) {
			known, err := sourceKnownDB(db, e)
			if err != nil {
				return nil, err
			}
			if known {
				return "duplicate", catchupLiveProgressDB(db, e, "duplicate")
			}
			var n int
			if err := db.QueryRow("SELECT (SELECT count(*) FROM inbound WHERE platform=? AND event_id=?) + (SELECT count(*) FROM ingress_validation WHERE platform=? AND event_id=?)", e.Platform, sourceLedgerKey(e), e.Platform, sourceLedgerKey(e)).Scan(&n); err != nil {
				return nil, err
			}
			if n > 0 {
				return "duplicate", catchupLiveProgressDB(db, e, "duplicate")
			}
			n, err = activeInboundCount(db)
			if err != nil {
				return nil, err
			}
			capacity := 1000
			enabled, err := catchupEnabledDB(db)
			if err != nil {
				return nil, err
			}
			if enabled {
				capacity = catchupLiveCapacity
			}
			if n >= capacity {
				return "queue_full", catchupLiveProgressDB(db, e, "queue_full")
			}
			if err = registerSourceDB(db, e); err != nil {
				return nil, err
			}
			id, err := uuidHex()
			if err != nil {
				return nil, err
			}
			raw, err := json.Marshal(e)
			if err != nil {
				return nil, err
			}
			_, err = db.Exec("INSERT INTO inbound(id,platform,event_id,envelope,created) VALUES(?,?,?,?,?)", id, e.Platform, sourceLedgerKey(e), string(raw), epoch())
			if err == nil {
				memoryBestEffort(db, func() error { return rememberInboundDB(db, id, e) })
				err = insertTiming(db, id, "ingested", epoch(), 0)
			}
			if err == nil {
				err = catchupLiveProgressDB(db, e, "accepted")
			}
			if err == nil {
				err = rememberReadRouteDB(db, e)
			}
			return "accepted", err
		})
	})
	if err != nil {
		return "", err
	}
	return v.(string), nil
}

type inboundRow struct {
	id, state, claim string
	lease, created   float64
	event            Envelope
}

func readInbound(db *storeConn, query string, args ...any) ([]inboundRow, error) {
	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []inboundRow{}
	for rows.Next() {
		var r inboundRow
		var raw string
		var cl sql.NullString
		var le sql.NullFloat64
		if err = rows.Scan(&r.id, &r.state, &cl, &le, &raw); err != nil {
			return nil, err
		}
		r.claim = cl.String
		r.lease = le.Float64
		if err = json.Unmarshal([]byte(raw), &r.event); err != nil {
			return nil, errors.New("invalid stored envelope")
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// Bound envelope materialization independently of the queue capacity. Candidate
// selection reads only metadata, so SQLite never sorts the entire payload backlog.
const pendingClaimPageSize = 16

func readPendingClaimPage(db *storeConn, now float64, after *inboundRow) ([]inboundRow, error) {
	query := `WITH candidates AS MATERIALIZED (
		SELECT id,created FROM inbound
		WHERE (state='pending' OR (state='claimed' AND lease_until<=?))
		AND NOT EXISTS(SELECT 1 FROM (` + unresolvedWorkerBindingsSQL + `) b WHERE b.inbound_id=inbound.id)`
	args := []any{now}
	if after != nil {
		query += ` AND (created,id)>(?,?)`
		args = append(args, after.created, after.id)
	}
	query += ` ORDER BY created,id LIMIT ?)
		SELECT i.id,c.created,i.envelope FROM candidates c JOIN inbound i ON i.id=c.id
		ORDER BY c.created,c.id`
	args = append(args, pendingClaimPageSize)
	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]inboundRow, 0, pendingClaimPageSize)
	for rows.Next() {
		var r inboundRow
		var raw string
		if err := rows.Scan(&r.id, &r.created, &raw); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(raw), &r.event); err != nil {
			return nil, errors.New("invalid stored envelope")
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *Store) ClaimNext(leaseSeconds, beginSeconds int) (*Claim, error) {
	return s.ClaimNextForConsumer(leaseSeconds, beginSeconds, "")
}

// ClaimNextForConsumer recovers a committed result whose CLI output was lost.
// A consumer identifies one logical reasoning worker, not a process or request.
// Repeating next replays its one still-owned claim without extending its lease.
// This is an idempotency namespace within the existing trusted OS-owner boundary,
// not authentication; independent workers must use different consumer identities.
// Empty identity preserves the legacy claim-only behavior.
func (s *Store) ClaimNextForConsumer(leaseSeconds, beginSeconds int, consumer string) (*Claim, error) {
	if leaseSeconds < 1 || leaseSeconds > 3600 || beginSeconds < 0 || beginSeconds > 300 {
		return nil, errors.New("invalid lease")
	}
	if len(consumer) > 128 {
		return nil, errors.New("invalid consumer identity")
	}
	for _, c := range consumer {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-' || c == '.' || c == ':') {
			return nil, errors.New("invalid consumer identity")
		}
	}
	v, err := s.call(func(db *storeConn) (any, error) {
		return transact(db, func(db *storeConn) (any, error) {
			now := epoch()
			if consumer != "" {
				owned, err := readInbound(db, `SELECT i.id,i.state,i.claim,i.lease_until,i.envelope FROM consumer_claims c JOIN inbound i ON i.id=c.inbound_id AND i.claim=c.claim WHERE c.consumer=? AND i.state='claimed' AND i.lease_until>?`, consumer, now)
				if err != nil {
					return nil, err
				}
				ownedCurrent := false
				if len(owned) == 1 {
					ownedCurrent, err = sourceCurrentDB(db, owned[0].event)
					if err != nil {
						return nil, err
					}
				}
				if len(owned) == 1 && ownedCurrent && s.policy.Accepts(owned[0].event) {
					r := owned[0]
					return claimWithMemoryDB(db, r.id, r.claim, r.lease, r.event), nil
				}
				if len(owned) == 1 {
					// Apply the ordinary acquisition policy gate to replay too, and
					// invalidate the token rather than leaving revoked work renewable.
					if _, err := db.Exec("UPDATE inbound SET state='blocked',claim=NULL,lease_until=NULL WHERE id=?", owned[0].id); err != nil {
						return nil, err
					}
				}
				// Never resurrect expired, superseded, terminal or revoked ownership.
				if _, err := db.Exec("DELETE FROM consumer_claims WHERE consumer=?", consumer); err != nil {
					return nil, err
				}
			}
			var after *inboundRow
			for {
				rows, err := readPendingClaimPage(db, now, after)
				if err != nil {
					return nil, err
				}
				if len(rows) == 0 {
					break
				}
				for _, r := range rows {
					fenced, err := catchupFencedDB(db, r.event)
					if err != nil {
						return nil, err
					}
					if fenced {
						continue
					}
					earlier, err := catchupEarlierPendingDB(db, r.event, now)
					if err != nil {
						return nil, err
					}
					if earlier {
						continue
					}
					current, err := sourceCurrentDB(db, r.event)
					if err != nil {
						return nil, err
					}
					if !current {
						continue
					}
					if !s.policy.Accepts(r.event) {
						if _, err = db.Exec("UPDATE inbound SET state='blocked',claim=NULL WHERE id=?", r.id); err != nil {
							return nil, err
						}
						continue
					}
					// Filter execution metadata before decoding conversation JSON;
					// unrelated pending payloads must remain outside the claim page.
					var busy int
					if err = db.QueryRow(`WITH busy AS MATERIALIZED (
						SELECT id,platform,envelope FROM inbound WHERE state='worker_recovery_pending'
						OR (state='claimed' AND lease_until>?)
						OR EXISTS(SELECT 1 FROM (`+unresolvedWorkerBindingsSQL+`) b WHERE b.inbound_id=inbound.id))
						SELECT count(*) FROM busy WHERE id!=? AND platform=? AND json_extract(envelope,'$.conversation_id')=?`, now, r.id, r.event.Platform, r.event.ConversationID).Scan(&busy); err != nil {
						return nil, err
					}
					if busy > 0 {
						continue
					}
					cl, err := uuidHex()
					if err != nil {
						return nil, err
					}
					until := now + float64(leaseSeconds)
					if _, err = db.Exec("UPDATE inbound SET state='claimed',claim=?,lease_until=? WHERE id=?", cl, until, r.id); err != nil {
						return nil, err
					}
					if _, err = db.Exec("DELETE FROM consumer_claims WHERE inbound_id=?", r.id); err != nil {
						return nil, err
					}
					if consumer != "" {
						if _, err = db.Exec("INSERT INTO consumer_claims(consumer,inbound_id,claim) VALUES(?,?,?)", consumer, r.id, cl); err != nil {
							return nil, err
						}
					}
					if beginSeconds > 0 {
						sec := beginSeconds
						if sec > leaseSeconds {
							sec = leaseSeconds
						}
						if _, err = db.Exec("INSERT INTO processing VALUES(?,?,?) ON CONFLICT(inbound_id) DO UPDATE SET claim=excluded.claim,until=excluded.until", r.id, cl, now+float64(sec)); err != nil {
							return nil, err
						}
					}
					if err = insertTiming(db, r.id, "claimed", now, now-r.created); err != nil {
						return nil, err
					}
					return claimWithMemoryDB(db, r.id, cl, until, r.event), nil
				}
				// Advance by immutable ordering keys, not OFFSET: revoked candidates
				// may have been removed from the eligible set while scanning this page.
				if len(rows) < pendingClaimPageSize {
					break
				}
				last := rows[len(rows)-1]
				after = &inboundRow{id: last.id, created: last.created}
			}
			return (*Claim)(nil), nil
		})
	})
	if err != nil {
		return nil, err
	}
	return v.(*Claim), nil
}
func changedOne(res sql.Result, err error) error {
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return ErrClaim
	}
	return nil
}
func (s *Store) Renew(id, claim string, seconds int) error {
	if seconds < 1 || seconds > 3600 {
		return errors.New("invalid lease")
	}
	_, err := s.call(func(db *storeConn) (any, error) {
		if err := claimSourceCurrentDB(db, id, claim); err != nil {
			return nil, err
		}
		now := epoch()
		return nil, changedOne(db.Exec("UPDATE inbound SET lease_until=? WHERE id=? AND claim=? AND state='claimed' AND lease_until>?", now+float64(seconds), id, claim, now))
	})
	return err
}
func (s *Store) BeginProcessing(id, claim string, seconds int) error {
	if seconds < 1 || seconds > 300 {
		return errors.New("invalid processing lease")
	}
	_, err := s.call(func(db *storeConn) (any, error) {
		return transact(db, func(db *storeConn) (any, error) {
			if err := claimSourceCurrentDB(db, id, claim); err != nil {
				return nil, err
			}
			now := epoch()
			var lease float64
			if err := db.QueryRow("SELECT lease_until FROM inbound WHERE id=? AND claim=? AND state='claimed' AND lease_until>?", id, claim, now).Scan(&lease); err != nil {
				if err == sql.ErrNoRows {
					return nil, ErrClaim
				}
				return nil, err
			}
			until := now + float64(seconds)
			if until > lease {
				until = lease
			}
			_, err := db.Exec("INSERT INTO processing VALUES(?,?,?) ON CONFLICT(inbound_id) DO UPDATE SET claim=excluded.claim,until=excluded.until", id, claim, until)
			return nil, err
		})
	})
	return err
}
func (s *Store) QueueReply(id, claim, text string) (string, error) {
	return s.queueReply(id, claim, text, nil)
}

func (s *Store) QueueReplyManifest(id, claim string, data []byte) (string, error) {
	m, err := ParseReplyManifest(data)
	if err != nil {
		return "", err
	}
	return s.queueReply(id, claim, m.Text, &m)
}

func (s *Store) queueReply(id, claim, text string, manifest *ReplyManifest) (string, error) {
	parts, err := SplitText(text)
	if manifest != nil && trimText(text) == "" {
		parts, err = []string{""}, nil
	}
	if err != nil {
		return "", err
	}
	v, err := s.call(func(db *storeConn) (any, error) {
		return transact(db, func(db *storeConn) (any, error) {
			rows, err := readInbound(db, "SELECT id,state,claim,lease_until,envelope FROM inbound WHERE id=?", id)
			if err != nil {
				return nil, err
			}
			if len(rows) == 0 || !s.policy.Accepts(rows[0].event) {
				return nil, errors.New("unknown or no longer authorized inbound message")
			}
			current, err := sourceCurrentDB(db, rows[0].event)
			if err != nil {
				return nil, err
			}
			if !current || rows[0].state == "cancelled" || rows[0].event.Control != "" && (rows[0].claim == "" || rows[0].claim != claim || rows[0].state != "claimed" && rows[0].state != "replied") {
				return nil, ErrClaim
			}
			var output ReplyOutputSnapshot
			if manifest != nil {
				r := rows[0]
				if r.claim != claim || (r.state != "replied" && (r.state != "claimed" || r.lease <= epoch())) {
					return nil, ErrClaim
				}
				output, err = stageReplyOutput(s.replyStateDir, *manifest)
				if err != nil {
					return nil, err
				}
			}
			var existing, oldText, oldOutput string
			err = db.QueryRow("SELECT r.id,r.text,COALESCE(o.payload,'') FROM replies r LEFT JOIN reply_outputs o ON o.reply_id=r.id WHERE r.inbound_id=?", id).Scan(&existing, &oldText, &oldOutput)
			if err == nil {
				if oldText != text || oldOutput != string(output) {
					return nil, errors.New("this inbound already has a different reply")
				}
				return existing, nil
			}
			if err != sql.ErrNoRows {
				return nil, err
			}
			r := rows[0]
			if r.state != "claimed" || r.claim != claim || r.lease <= epoch() {
				return nil, ErrClaim
			}
			rid, err := uuidHex()
			if err != nil {
				return nil, err
			}
			if _, err = db.Exec("INSERT INTO replies VALUES(?,?,?,?)", rid, id, text, epoch()); err != nil {
				return nil, err
			}
			if output != "" {
				if _, err = db.Exec("INSERT INTO reply_outputs(reply_id,payload) VALUES(?,?)", rid, string(output)); err != nil {
					return nil, err
				}
			}
			for i, p := range parts {
				if _, err = db.Exec("INSERT INTO chunks(reply_id,idx,text) VALUES(?,?,?)", rid, i, p); err != nil {
					return nil, err
				}
			}
			_, err = db.Exec("UPDATE inbound SET state='replied',lease_until=NULL WHERE id=?", id)
			if err == nil {
				var started float64
				err = db.QueryRow("SELECT COALESCE((SELECT max(at) FROM timings WHERE inbound_id=? AND stage='claimed'),created) FROM inbound WHERE id=?", id, id).Scan(&started)
				if err == nil {
					err = insertTiming(db, id, "reply_queued", epoch(), epoch()-started)
				}
			}
			return rid, err
		})
	})
	if err != nil {
		return "", err
	}
	return v.(string), nil
}
func (s *Store) Ignore(id, claim string) error {
	_, err := s.call(func(db *storeConn) (any, error) {
		return nil, changedOne(db.Exec("UPDATE inbound SET state='ignored',claim=NULL,lease_until=NULL WHERE id=? AND claim=? AND state='claimed' AND lease_until>?", id, claim, epoch()))
	})
	return err
}
func affected(res sql.Result, err error) (any, error) {
	if err != nil {
		return nil, err
	}
	n, err := res.RowsAffected()
	return int(n), err
}

// RecoverInterrupted requires the caller to hold the exclusive dispatcher OS lock.
// Sending rows become terminally uncertain; they are never reset to pending.
func (s *Store) RecoverInterrupted() (int, error) {
	v, err := s.call(func(db *storeConn) (any, error) {
		return affected(db.Exec("UPDATE chunks SET state='uncertain',code='interrupted_before_ack' WHERE state='sending'"))
	})
	if err != nil {
		return 0, err
	}
	return v.(int), nil
}
func (s *Store) NextChunk() (*Chunk, error) {
	v, err := s.call(func(db *storeConn) (any, error) {
		return transact(db, func(db *storeConn) (any, error) {
			rows, err := db.Query(`SELECT c.reply_id,c.idx,c.text,i.envelope,` + chunkReplyOutputSQL + ` FROM chunks c JOIN replies r ON r.id=c.reply_id JOIN inbound i ON i.id=r.inbound_id WHERE c.state='pending' AND i.state!='cancelled' AND NOT EXISTS(SELECT 1 FROM reply_cancellations cancelled WHERE cancelled.reply_id=c.reply_id) AND NOT EXISTS(SELECT 1 FROM chunks p WHERE p.reply_id=c.reply_id AND p.idx<c.idx AND p.state!='sent') AND NOT EXISTS(SELECT 1 FROM replies older JOIN inbound source ON source.id=older.inbound_id JOIN chunks unfinished ON unfinished.reply_id=older.id WHERE (older.created<r.created OR (older.created=r.created AND older.rowid<r.rowid)) AND source.platform=i.platform AND json_extract(source.envelope,'$.conversation_id')=json_extract(i.envelope,'$.conversation_id') AND unfinished.state NOT IN ('sent','cancelled') AND (source.state!='cancelled' OR unfinished.state IN ('sending','uncertain')) AND NOT EXISTS(SELECT 1 FROM reply_cancellations cancelled WHERE cancelled.reply_id=older.id)) ORDER BY r.created,r.rowid,c.idx`)
			if err != nil {
				return nil, err
			}
			chunks := []Chunk{}
			for rows.Next() {
				var c Chunk
				var raw string
				if err = rows.Scan(&c.ReplyID, &c.Index, &c.Text, &raw, &c.Output); err != nil {
					rows.Close()
					return nil, err
				}
				if err = json.Unmarshal([]byte(raw), &c.Source); err != nil {
					rows.Close()
					return nil, errors.New("invalid stored envelope")
				}
				chunks = append(chunks, c)
			}
			err = rows.Err()
			rows.Close()
			if err != nil {
				return nil, err
			}
			for _, c := range chunks {
				current, err := sourceCurrentDB(db, c.Source)
				if err != nil {
					return nil, err
				}
				if !current {
					continue
				}
				if !s.policy.Allows(c.Source) {
					if _, err = db.Exec("UPDATE chunks SET state='failed',code='authorization_revoked' WHERE reply_id=? AND state='pending'", c.ReplyID); err != nil {
						return nil, err
					}
					continue
				}
				if _, err = db.Exec("UPDATE chunks SET state='sending',attempts=attempts+1,code=NULL WHERE reply_id=? AND idx=? AND state='pending'", c.ReplyID, c.Index); err != nil {
					return nil, err
				}
				var id string
				var created float64
				if err = db.QueryRow("SELECT inbound_id,created FROM replies WHERE id=?", c.ReplyID).Scan(&id, &created); err != nil {
					return nil, err
				}
				if err = insertTiming(db, id, "send_started", epoch(), epoch()-created); err != nil {
					return nil, err
				}
				return &c, nil
			}
			return (*Chunk)(nil), nil
		})
	})
	if err != nil {
		return nil, err
	}
	return v.(*Chunk), nil
}
func symbolicCode(code string) string {
	for _, r := range code {
		if !unicode.IsLetter(r) && !unicode.IsNumber(r) && r != '_' {
			return ""
		}
	}
	r := []rune(code)
	if len(r) > 80 {
		r = r[:80]
	}
	return string(r)
}
func nullable(v string) any {
	if v == "" {
		return nil
	}
	return v
}
func (s *Store) RecordResult(c Chunk, r SendResult) error {
	return s.recordResult(c, r, nil)
}
func (s *Store) recordResult(c Chunk, r SendResult, measurement *SendMeasurement) error {
	if (r.State != "sent" && r.State != "failed" && r.State != "uncertain") || (r.State == "sent" && r.MessageID == "") {
		return errors.New("invalid delivery result")
	}
	_, err := s.call(func(db *storeConn) (any, error) {
		return transact(db, func(db *storeConn) (any, error) {
			if err := persistReplyOutputReceipt(db, c, r); err != nil {
				return nil, err
			}
			res, err := db.Exec("UPDATE chunks SET state=?,message_id=?,code=? WHERE reply_id=? AND idx=? AND state='sending'", r.State, nullable(r.MessageID), symbolicCode(r.Code), c.ReplyID, c.Index)
			if err = changedOne(res, err); err != nil {
				if err == ErrClaim {
					err = errors.New("chunk is not in sending state")
				}
				return nil, err
			}
			memoryBestEffort(db, func() error { return rememberSentDB(db, c.ReplyID, c.Index) })
			if err = restoreCurrentSourceResultDB(db, c, r); err != nil {
				return nil, err
			}
			if err = cancelStaleReplyAfterResultDB(db, c); err != nil {
				return nil, err
			}
			if c.Source.ReplyKind == "interaction" && r.State == "failed" && r.Code == "interaction_token_unavailable_reissue_required" {
				var inbound string
				if err := db.QueryRow("SELECT inbound_id FROM replies WHERE id=?", c.ReplyID).Scan(&inbound); err != nil {
					return nil, err
				}
				if err := expireInteractionDB(db, inbound); err != nil {
					return nil, err
				}
			}
			var id string
			var created, started float64
			if err = db.QueryRow("SELECT i.id,i.created,COALESCE((SELECT max(at) FROM timings WHERE inbound_id=i.id AND stage='send_started'),r.created) FROM replies r JOIN inbound i ON i.id=r.inbound_id WHERE r.id=?", c.ReplyID).Scan(&id, &created, &started); err != nil {
				return nil, err
			}
			now := epoch()
			if err = insertTiming(db, id, "send_completed", now, now-started); err != nil {
				return nil, err
			}
			d, err := delivery(db, c.ReplyID)
			if err != nil {
				return nil, err
			}
			if d.State == "sent" {
				err = insertTiming(db, id, "end_to_end", now, now-created)
			}
			if err == nil {
				insertSendMeasurement(db, c, measurement)
			}
			return nil, err
		})
	})
	return err
}

func delivery(db *storeConn, id string) (Delivery, error) {
	d := Delivery{ReplyID: id, Chunks: []DeliveryChunk{}}
	if err := db.QueryRow("SELECT inbound_id FROM replies WHERE id=?", id).Scan(&d.InboundID); err != nil {
		if err == sql.ErrNoRows {
			err = errors.New("unknown reply")
		}
		return d, err
	}
	rows, err := db.Query("SELECT c.idx,c.state,c.message_id,c.code,c.attempts,COALESCE(o.receipt,'') FROM chunks c LEFT JOIN reply_output_receipts o ON o.reply_id=c.reply_id AND o.idx=c.idx WHERE c.reply_id=? ORDER BY c.idx", id)
	if err != nil {
		return d, err
	}
	defer rows.Close()
	states := map[string]bool{}
	for rows.Next() {
		var c DeliveryChunk
		if err = rows.Scan(&c.Index, &c.State, &c.MessageID, &c.Code, &c.Attempts, &c.OutputReceipt); err != nil {
			return d, err
		}
		d.Chunks = append(d.Chunks, c)
		states[c.State] = true
	}
	d.State = "sent"
	for _, st := range []string{"uncertain", "failed", "sending", "pending", "cancelled"} {
		if states[st] {
			d.State = st
			break
		}
	}
	if d.State == "pending" {
		d.State = "queued"
	}
	if err := rows.Err(); err != nil {
		return d, err
	}
	// Close the cursor before a second query on the store's pinned connection.
	if err := rows.Close(); err != nil {
		return d, err
	}
	cancelled, err := replyCancelled(db, id)
	if err != nil {
		return d, err
	}
	if cancelled {
		d.State = "cancelled"
		d.Cancellation = &ReplyCancellation{}
		if err := db.QueryRow("SELECT cancelled_at,code FROM reply_cancellations WHERE reply_id=?", id).Scan(&d.Cancellation.At, &d.Cancellation.Code); err != nil {
			return d, err
		}
	}
	return d, nil
}
func (s *Store) Delivery(id string) (Delivery, error) {
	v, err := s.call(func(db *storeConn) (any, error) { return delivery(db, id) })
	if err != nil {
		return Delivery{}, err
	}
	return v.(Delivery), nil
}
func (s *Store) RetryFailed(id string) (int, error) {
	v, err := s.call(func(db *storeConn) (any, error) {
		return transact(db, func(db *storeConn) (any, error) {
			var cancelled int
			if err := db.QueryRow("SELECT count(*) FROM replies r JOIN inbound i ON i.id=r.inbound_id WHERE r.id=? AND i.state='cancelled'", id).Scan(&cancelled); err != nil {
				return nil, err
			}
			if cancelled != 0 {
				return nil, errors.New("request_cancelled")
			}
			d, err := delivery(db, id)
			if err != nil {
				return nil, err
			}
			if d.State == "cancelled" {
				return nil, errors.New("cancelled replies cannot be retried")
			}
			return affected(db.Exec("UPDATE chunks SET state='pending',code=NULL WHERE reply_id=? AND state='failed'", id))
		})
	})
	if err != nil {
		return 0, err
	}
	return v.(int), nil
}
func (s *Store) ResolveSent(id string, index int, messageID string) error {
	if messageID == "" || len([]rune(messageID)) > 128 {
		return errors.New("a verified remote message ID is required")
	}
	_, err := s.call(func(db *storeConn) (any, error) {
		return transact(db, func(db *storeConn) (any, error) {
			output, err := replyChunkOutputDB(db, id, index)
			if err != nil && err != sql.ErrNoRows {
				return nil, err
			}
			if output != "" {
				return nil, errors.New("rich_reply_requires_reconcile_reply")
			}
			err = changedOne(db.Exec("UPDATE chunks SET state='sent',message_id=?,code='operator_verified' WHERE reply_id=? AND idx=? AND state='uncertain'", messageID, id, index))
			if err == ErrClaim {
				err = errors.New("only uncertain chunks can be resolved")
			}
			if err == nil {
				var raw string
				err = db.QueryRow(`SELECT i.envelope FROM replies r JOIN inbound i ON i.id=r.inbound_id WHERE r.id=?`, id).Scan(&raw)
				if err == nil {
					var e Envelope
					if json.Unmarshal([]byte(raw), &e) != nil {
						return nil, errors.New("invalid source envelope")
					}
					err = cancelStaleReplyAfterResultDB(db, Chunk{ReplyID: id, Index: index, Source: e})
					memoryBestEffort(db, func() error { return rememberSentDB(db, id, index) })
				}
			}
			return nil, err
		})
	})
	return err
}
func (s *Store) FeedbackRows() ([]FeedbackRow, error) {
	v, err := s.call(func(db *storeConn) (any, error) {
		return transact(db, func(db *storeConn) (any, error) {
			rows, err := db.Query(`SELECT i.id,i.state,COALESCE(i.claim,''),COALESCE(i.lease_until,0),i.envelope,COALESCE(p.until,0),COALESCE(p.claim,''),COALESCE(f.stage,''),COALESCE(r.id,'') FROM inbound i LEFT JOIN processing p ON p.inbound_id=i.id LEFT JOIN feedback f ON f.inbound_id=i.id LEFT JOIN replies r ON r.inbound_id=i.id WHERE f.stage IS NULL OR f.stage IN ('received','failed','uncertain') ORDER BY i.created,i.id`)
			if err != nil {
				return nil, err
			}
			out := []FeedbackRow{}
			for rows.Next() {
				var r FeedbackRow
				var raw string
				if err = rows.Scan(&r.ID, &r.State, &r.Claim, &r.LeaseUntil, &raw, &r.ProcessingUntil, &r.ProcessingClaim, &r.FeedbackStage, &r.ReplyID); err != nil {
					rows.Close()
					return nil, err
				}
				if err = json.Unmarshal([]byte(raw), &r.Event); err != nil {
					rows.Close()
					return nil, errors.New("invalid stored envelope")
				}
				current, er := sourceCurrentDB(db, r.Event)
				if er != nil {
					rows.Close()
					return nil, er
				}
				if current && s.policy.Accepts(r.Event) && r.Event.Control == "" {
					out = append(out, r)
				}
			}
			err = rows.Err()
			rows.Close()
			if err != nil {
				return nil, err
			}
			now := epoch()
			for i := range out {
				r := &out[i]
				if r.ReplyID != "" {
					d, err := delivery(db, r.ReplyID)
					if err != nil {
						return nil, err
					}
					r.Delivery = d.State
					if d.State == "cancelled" {
						// Cancellation is a terminal non-delivery, never success.
						r.Delivery = "failed"
					}
				}
				r.Thinking = r.State == "claimed" && r.ProcessingClaim == r.Claim && r.ProcessingUntil > now && r.LeaseUntil > now
			}
			return out, nil
		})
	})
	if err != nil {
		return nil, err
	}
	return v.([]FeedbackRow), nil
}
func (s *Store) RecordFeedback(id, stage string) error {
	switch stage {
	case "received", "sent", "failed", "uncertain", "ignored":
	default:
		return errors.New("invalid feedback stage")
	}
	_, err := s.call(func(db *storeConn) (any, error) {
		_, err := db.Exec("INSERT INTO feedback VALUES(?,?) ON CONFLICT(inbound_id) DO UPDATE SET stage=excluded.stage", id, stage)
		return nil, err
	})
	return err
}
func (s *Store) SetRuntime(state string, warnings map[string]int, details map[string]any) error {
	value := map[string]any{}
	for k, v := range details {
		value[k] = v
	}
	value["state"] = state
	value["updated_at"] = epoch()
	if warnings == nil {
		warnings = map[string]int{}
	}
	value["warning_counts"] = warnings
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	_, err = s.call(func(db *storeConn) (any, error) {
		_, err := db.Exec("INSERT INTO runtime VALUES('gateway',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", string(raw))
		return nil, err
	})
	return err
}
func (s *Store) Status() (map[string]any, error) {
	v, err := s.call(func(db *storeConn) (any, error) {
		return transact(db, func(db *storeConn) (any, error) {
			var raw string
			gateway := map[string]any{"state": "never_started", "updated_at": nil}
			err := db.QueryRow("SELECT value FROM runtime WHERE key='gateway'").Scan(&raw)
			if err != nil && err != sql.ErrNoRows {
				return nil, err
			}
			if err == nil {
				if err = json.Unmarshal([]byte(raw), &gateway); err != nil {
					return nil, errors.New("invalid runtime state")
				}
			}
			at, ok := gateway["updated_at"].(float64)
			gateway["heartbeat_stale"] = !ok || epoch()-at > 15
			out := map[string]any{"gateway": gateway}
			consumer, err := consumerHealth(db, epoch())
			if err != nil {
				return nil, err
			}
			out["consumer"] = consumer
			workers, err := workerHealthDB(db)
			if err != nil {
				return nil, err
			}
			out["worker_control"] = workers
			// A fresh transport or waiting subprocess cannot establish that the
			// assistant received a tool result or is currently reasoning.
			pathState := "unverified"
			if gateway["state"] != "connected" || gateway["heartbeat_stale"] == true {
				pathState = "gateway_unavailable"
			} else if consumer["validation_blocked"].(int) > 0 {
				pathState = "blocked_ingress"
			} else if consumer["backlog_overdue"] == true || consumer["validation_backlog_overdue"] == true || consumer["expired_claims"].(int) > 0 {
				pathState = "degraded_backlog"
			} else if consumer["validation_pending"].(int) > 0 {
				pathState = "validating_ingress"
			} else if consumer["state"] == "absent" || consumer["state"] == "stale_cli" || consumer["state"] == "unknown" {
				pathState = "consumer_unavailable"
			}
			out["response_path"] = map[string]any{"state": pathState, "end_to_end_ready": false, "readiness": "not_verifiable_from_gateway"}
			for key, table := range map[string]string{"inbound": "inbound", "outbound_chunks": "chunks"} {
				rows, err := db.Query("SELECT state,count(*) FROM " + table + " GROUP BY state")
				if err != nil {
					return nil, err
				}
				counts := map[string]int{}
				for rows.Next() {
					var st string
					var n int
					if err = rows.Scan(&st, &n); err != nil {
						rows.Close()
						return nil, err
					}
					counts[st] = n
				}
				err = rows.Err()
				rows.Close()
				if err != nil {
					return nil, err
				}
				out[key] = counts
			}
			var cancelledReplies int
			if err := db.QueryRow("SELECT count(*) FROM reply_cancellations").Scan(&cancelledReplies); err != nil {
				return nil, err
			}
			out["cancelled_replies"] = cancelledReplies
			rows, err := db.Query("SELECT stage,count(*),avg(duration_ms),max(duration_ms) FROM timings GROUP BY stage ORDER BY stage")
			if err != nil {
				return nil, err
			}
			metrics := map[string]any{}
			for rows.Next() {
				var stage string
				var count int
				var avg, max float64
				if err = rows.Scan(&stage, &count, &avg, &max); err != nil {
					rows.Close()
					return nil, err
				}
				metrics[stage] = map[string]any{"count": count, "mean_ms": avg, "max_ms": max}
			}
			err = rows.Err()
			rows.Close()
			if err != nil {
				return nil, err
			}
			out["timings"] = metrics
			return out, nil
		})
	})
	if err != nil {
		return nil, err
	}
	return v.(map[string]any), nil
}

// Timings contain no message text or route/user identifiers; records are bounded.
var timingStages = map[string]bool{"ingest": true, "claim": true, "processing": true, "reply": true, "queue_reply": true, "send": true, "feedback": true, "dispatch": true, "ipc": true, "gateway_receive": true, "gateway_ingest": true, "claim_wait": true, "first_send": true, "delivery": true, "received": true, "claimed": true, "reply_queued": true, "send_started": true, "send_completed": true, "ingested": true, "queued": true, "sent": true, "inbound_to_claim": true, "claim_to_reply": true, "reply_to_send": true, "end_to_end": true}

func (s *Store) RecordTiming(stage string, duration time.Duration) error {
	return s.RecordMessageTiming("", stage, epoch(), duration)
}
func (s *Store) RecordMessageTiming(inboundID, stage string, at float64, duration time.Duration) error {
	if !timingStages[stage] || duration < 0 || duration > 24*time.Hour || at <= 0 || at > epoch()+3600 {
		return errors.New("invalid timing")
	}
	if inboundID != "" {
		if len(inboundID) != 32 {
			return errors.New("invalid inbound timing ID")
		}
		if _, err := hex.DecodeString(inboundID); err != nil {
			return errors.New("invalid inbound timing ID")
		}
	}
	_, err := s.call(func(db *storeConn) (any, error) {
		return transact(db, func(db *storeConn) (any, error) {
			if inboundID != "" {
				var n int
				if err := db.QueryRow("SELECT count(*) FROM inbound WHERE id=?", inboundID).Scan(&n); err != nil {
					return nil, err
				}
				if n != 1 {
					return nil, errors.New("unknown inbound timing ID")
				}
			}
			return nil, insertTiming(db, inboundID, stage, at, duration.Seconds())
		})
	})
	return err
}

// insertTiming must run within the caller's transaction. It caps old or clock-skewed durations.
func insertTiming(db *storeConn, id, stage string, at, seconds float64) error {
	if seconds < 0 {
		seconds = 0
	}
	if seconds > 86400 {
		seconds = 86400
	}
	if _, err := db.Exec("INSERT INTO timings(inbound_id,stage,at,duration_ms) VALUES(?,?,?,?)", nullable(id), stage, at, seconds*1000); err != nil {
		return err
	}
	_, err := db.Exec("DELETE FROM timings WHERE id NOT IN (SELECT id FROM timings ORDER BY id DESC LIMIT 2048)")
	return err
}

// TestSendStatus never sends, retries, or mutates the legacy test-send ledger.
func (s *Store) TestSendStatus(settings Settings) (map[string]any, error) {
	bot, guild, channel := settings.ExpectedBotID, settings.Policy.GuildID, settings.Policy.GuildChannelID
	if !Snowflake(bot) || !Snowflake(guild) || !Snowflake(channel) || !Snowflake(settings.Policy.OwnerID) || bot == settings.Policy.OwnerID {
		return nil, errors.New("test-send requires pinned owner, bot, guild, and text channel IDs")
	}
	v, err := s.call(func(db *storeConn) (any, error) {
		result := map[string]any{"bot_id": bot, "guild_id": guild, "channel_id": channel, "state": "not_started", "attempted": false, "acknowledged": false, "verified": false, "message_id": nil, "code": nil, "nonce": nil, "created_at": nil, "updated_at": nil, "verified_at": nil}
		var exists int
		if err := db.QueryRow("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='test_sends'").Scan(&exists); err != nil {
			return nil, err
		}
		if exists == 0 {
			return result, nil
		}
		var state, nonce string
		var attempted int
		var message, code sql.NullString
		var created, updated float64
		var verified sql.NullFloat64
		err := db.QueryRow("SELECT state,attempted,message_id,code,nonce,created,updated,verified_at FROM test_sends WHERE bot_id=? AND guild_id=? AND channel_id=?", bot, guild, channel).Scan(&state, &attempted, &message, &code, &nonce, &created, &updated, &verified)
		if err == sql.ErrNoRows {
			return result, nil
		}
		if err != nil {
			return nil, err
		}
		if state == "preflight" || state == "attempted" {
			code = sql.NullString{String: "in_progress_or_interrupted_" + state, Valid: true}
			state = "uncertain"
		}
		result["state"] = state
		result["attempted"] = attempted != 0
		result["acknowledged"] = message.Valid
		result["verified"] = state == "verified"
		result["nonce"] = nonce
		result["created_at"] = created
		result["updated_at"] = updated
		if message.Valid {
			result["message_id"] = message.String
			result["message_url"] = "https://discord.com/channels/" + guild + "/" + channel + "/" + message.String
		}
		if code.Valid {
			result["code"] = code.String
		}
		if verified.Valid {
			result["verified_at"] = verified.Float64
		}
		return result, nil
	})
	if err != nil {
		return nil, err
	}
	return v.(map[string]any), nil
}
