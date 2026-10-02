package bridge

// The gateway cannot observe or interrupt a native reasoning agent. This is a
// durable, OS-owner-trusted protocol for its actual controller. Attestations are
// never synthesized from transport, subprocess, claim, or processing heartbeats.
import (
	"database/sql"
	"errors"
	"strings"
)

// A binding is an execution-generation association, not a mutable queue lease.
// Source invalidation may clear inbound.claim while the native turn still runs.
// Only an attested stop (or its explicitly registered successor) releases it;
// neither inbound nor worker lease expiry establishes that execution stopped.
const unresolvedWorkerBindingsSQL = `SELECT b.inbound_id,b.claim,b.worker,b.incarnation,b.controller
 FROM worker_bindings b JOIN worker_runtime w ON w.worker=b.worker AND w.incarnation=b.incarnation
 WHERE w.state NOT IN ('completed','interrupted','failed')`

func initWorkerControl(db *storeConn) error {
	_, err := db.Exec(`CREATE TABLE IF NOT EXISTS worker_incarnations(
 worker TEXT NOT NULL,incarnation TEXT NOT NULL,controller TEXT NOT NULL,previous TEXT NOT NULL,evidence TEXT NOT NULL,created REAL NOT NULL,PRIMARY KEY(worker,incarnation));
 CREATE TABLE IF NOT EXISTS worker_runtime(
 worker TEXT PRIMARY KEY,incarnation TEXT NOT NULL,controller TEXT NOT NULL,state TEXT NOT NULL,observed REAL NOT NULL,lease_until REAL NOT NULL,evidence TEXT NOT NULL);
 CREATE TABLE IF NOT EXISTS worker_bindings(
 inbound_id TEXT NOT NULL REFERENCES inbound(id),claim TEXT NOT NULL,worker TEXT NOT NULL,incarnation TEXT NOT NULL,controller TEXT NOT NULL,bound REAL NOT NULL,evidence TEXT NOT NULL,PRIMARY KEY(inbound_id,claim));
 CREATE INDEX IF NOT EXISTS worker_bindings_worker ON worker_bindings(worker,incarnation);
 CREATE TABLE IF NOT EXISTS worker_cancellations(
 inbound_id TEXT PRIMARY KEY REFERENCES inbound(id),claim TEXT NOT NULL,revision TEXT NOT NULL,worker TEXT NOT NULL,incarnation TEXT NOT NULL,controller TEXT NOT NULL,state TEXT NOT NULL,requested REAL NOT NULL,acknowledged REAL NOT NULL DEFAULT 0,evidence TEXT NOT NULL DEFAULT '');`)
	return err
}

type WorkerStatus struct {
	Worker            string  `json:"worker"`
	Incarnation       string  `json:"incarnation"`
	Controller        string  `json:"controller"`
	AttestedState     string  `json:"attested_state"`
	State             string  `json:"state"`
	ObservedAt        float64 `json:"observed_at"`
	LeaseUntil        float64 `json:"lease_until"`
	EvidenceRef       string  `json:"evidence_ref"`
	Fresh             bool    `json:"fresh"`
	ObservationSource string  `json:"observation_source"`
}

type WorkerCancellation struct {
	Request        string  `json:"request"`
	Claim          string  `json:"-"`
	Revision       string  `json:"revision"`
	Worker         string  `json:"worker,omitempty"`
	Incarnation    string  `json:"incarnation,omitempty"`
	Controller     string  `json:"controller,omitempty"`
	State          string  `json:"state"`
	RequestedAt    float64 `json:"requested_at"`
	AcknowledgedAt float64 `json:"acknowledged_at,omitempty"`
	EvidenceRef    string  `json:"evidence_ref,omitempty"`
	ExecutionState string  `json:"execution_state"`
}

func workerIdentity(v string) bool {
	if v == "" || len(v) > 256 {
		return false
	}
	for _, r := range v {
		if r <= 32 || r >= 127 {
			return false
		}
	}
	return true
}
func workerEvidence(v string) bool { return workerIdentity(v) && !strings.Contains(v, "@") }
func workerState(v string) bool {
	return v == "running" || v == "idle" || v == "interrupted" || v == "failed" || v == "completed"
}
func workerStopped(v string) bool { return v == "interrupted" || v == "failed" || v == "completed" }
func workerStatusDB(db *storeConn, worker string, now float64) (WorkerStatus, error) {
	var w WorkerStatus
	err := db.QueryRow(`SELECT worker,incarnation,controller,state,observed,lease_until,evidence FROM worker_runtime WHERE worker=?`, worker).Scan(&w.Worker, &w.Incarnation, &w.Controller, &w.AttestedState, &w.ObservedAt, &w.LeaseUntil, &w.EvidenceRef)
	if err != nil {
		return w, err
	}
	w.ObservationSource = "controller_attestation"
	w.Fresh = w.ObservedAt > 0 && w.ObservedAt <= now+5 && w.LeaseUntil > now
	w.State = "unknown"
	if w.Fresh {
		w.State = w.AttestedState
	}
	return w, nil
}
func exactWorkerDB(db *storeConn, worker, incarnation, controller string) (WorkerStatus, error) {
	w, err := workerStatusDB(db, worker, epoch())
	if err != nil {
		if err == sql.ErrNoRows {
			err = errors.New("worker_not_registered")
		}
		return w, err
	}
	if w.Incarnation != incarnation || w.Controller != controller {
		return w, errors.New("worker_incarnation_fenced")
	}
	return w, nil
}

// RegisterWorker registers an observed native-worker incarnation, not a process.
// Recovery requires an exact prior incarnation, its controller's verified stop
// observation, and an evidence reference for the real replacement/resume. Lost
// stdout retries are idempotent and do not renew leases. Incarnations cannot be
// reused after retirement, including after a database reopen.
func (s *Store) RegisterWorker(worker, incarnation, controller, state string, seconds int, previous, evidence string) (WorkerStatus, error) {
	if !workerIdentity(worker) || !workerIdentity(incarnation) || !workerIdentity(controller) || !workerEvidence(evidence) || (state != "running" && state != "idle") || seconds < 1 || seconds > 300 {
		return WorkerStatus{}, errors.New("invalid_worker_registration")
	}
	v, err := s.call(func(db *storeConn) (any, error) {
		return transact(db, func(db *storeConn) (any, error) {
			now := epoch()
			old, e := workerStatusDB(db, worker, now)
			if e != nil && e != sql.ErrNoRows {
				return nil, e
			}
			if e == nil && old.Incarnation == incarnation {
				var p, proof string
				if old.Controller != controller {
					return nil, errors.New("worker_incarnation_fenced")
				}
				if e = db.QueryRow(`SELECT previous,evidence FROM worker_incarnations WHERE worker=? AND incarnation=?`, worker, incarnation).Scan(&p, &proof); e != nil {
					return nil, e
				}
				if previous != p || evidence != proof {
					return nil, errors.New("worker_registration_conflict")
				}
				return old, nil
			}
			var used int
			if e2 := db.QueryRow(`SELECT count(*) FROM worker_incarnations WHERE worker=? AND incarnation=?`, worker, incarnation).Scan(&used); e2 != nil {
				return nil, e2
			}
			if used != 0 {
				return nil, errors.New("worker_incarnation_retired")
			}
			if e == sql.ErrNoRows {
				if previous != "" {
					return nil, errors.New("worker_previous_incarnation_mismatch")
				}
			} else {
				if previous != old.Incarnation || controller != old.Controller {
					return nil, errors.New("worker_previous_incarnation_mismatch")
				}
				if !workerStopped(old.AttestedState) {
					return nil, errors.New("worker_recovery_requires_stopped_observation")
				}
				var pending int
				if e = db.QueryRow(`SELECT count(*) FROM worker_cancellations WHERE worker=? AND incarnation=? AND state='cancel_requested'`, worker, previous).Scan(&pending); e != nil {
					return nil, e
				}
				if pending > 0 {
					return nil, errors.New("worker_cancellation_ack_required")
				}
				// Only explicit recovery releases claims quarantined by the verified stop.
				if _, e = db.Exec(`UPDATE inbound SET state='pending' WHERE state='worker_recovery_pending' AND id IN (SELECT inbound_id FROM worker_bindings WHERE worker=? AND incarnation=?)`, worker, previous); e != nil {
					return nil, e
				}
			}
			if _, e = db.Exec(`INSERT INTO worker_incarnations VALUES(?,?,?,?,?,?)`, worker, incarnation, controller, previous, evidence, now); e != nil {
				return nil, e
			}
			if _, e = db.Exec(`INSERT INTO worker_runtime VALUES(?,?,?,?,?,?,?) ON CONFLICT(worker) DO UPDATE SET incarnation=excluded.incarnation,controller=excluded.controller,state=excluded.state,observed=excluded.observed,lease_until=excluded.lease_until,evidence=excluded.evidence`, worker, incarnation, controller, state, now, now+float64(seconds), evidence); e != nil {
				return nil, e
			}
			return workerStatusDB(db, worker, now)
		})
	})
	if err != nil {
		return WorkerStatus{}, err
	}
	return v.(WorkerStatus), nil
}

// fenceWorkerClaimsDB revokes effects after an attested real stop. Queued output
// and unresolved deliveries are preserved; cancellation is a separate operation.
func fenceWorkerClaimsDB(db *storeConn, worker, incarnation string) error {
	if _, err := db.Exec(`UPDATE inbound SET state=CASE WHEN state='claimed' THEN 'worker_recovery_pending' ELSE state END,claim=NULL,lease_until=NULL WHERE state IN ('claimed','replied') AND EXISTS(SELECT 1 FROM worker_bindings b WHERE b.inbound_id=inbound.id AND b.claim=inbound.claim AND b.worker=? AND b.incarnation=?)`, worker, incarnation); err != nil {
		return err
	}
	for _, table := range []string{"processing", "consumer_claims"} {
		if _, err := db.Exec(`DELETE FROM `+table+` WHERE EXISTS(SELECT 1 FROM worker_bindings b WHERE b.inbound_id=`+table+`.inbound_id AND b.claim=`+table+`.claim AND b.worker=? AND b.incarnation=?)`, worker, incarnation); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) ObserveWorker(worker, incarnation, controller, state string, seconds int, evidence string) (WorkerStatus, error) {
	if !workerState(state) || !workerEvidence(evidence) || seconds < 1 || seconds > 300 {
		return WorkerStatus{}, errors.New("invalid_worker_observation")
	}
	v, err := s.call(func(db *storeConn) (any, error) {
		return transact(db, func(db *storeConn) (any, error) {
			w, e := exactWorkerDB(db, worker, incarnation, controller)
			if e != nil {
				return nil, e
			}
			if workerStopped(w.AttestedState) && state != w.AttestedState {
				return nil, errors.New("worker_recovery_registration_required")
			}
			if state == "completed" {
				var unfinished int
				if e = db.QueryRow(`SELECT count(*) FROM worker_bindings b JOIN inbound i ON i.id=b.inbound_id WHERE b.worker=? AND b.incarnation=? AND (i.state IN ('claimed','worker_recovery_pending') OR EXISTS(SELECT 1 FROM worker_cancellations c WHERE c.inbound_id=i.id AND c.state='cancel_requested'))`, worker, incarnation).Scan(&unfinished); e != nil {
					return nil, e
				}
				if unfinished > 0 {
					return nil, errors.New("worker_completion_has_unfinished_claim")
				}
			}
			if workerStopped(state) {
				if e = fenceWorkerClaimsDB(db, worker, incarnation); e != nil {
					return nil, e
				}
			}
			now := epoch()
			if _, e = db.Exec(`UPDATE worker_runtime SET state=?,observed=?,lease_until=?,evidence=? WHERE worker=?`, state, now, now+float64(seconds), evidence, worker); e != nil {
				return nil, e
			}
			return workerStatusDB(db, worker, now)
		})
	})
	if err != nil {
		return WorkerStatus{}, err
	}
	return v.(WorkerStatus), nil
}

// BindWorkerClaim is an assertion by the actual controller that this exact
// native turn owns the claim. It cannot move a binding to a different worker.
// An unbound cancellation may be reconciled using its preserved original claim
// and explicit ownership evidence; the cancelled claim is never made usable.
func (s *Store) BindWorkerClaim(id, claim, worker, incarnation, controller, evidence string) (WorkerStatus, error) {
	if claim == "" || !workerEvidence(evidence) {
		return WorkerStatus{}, errors.New("worker_binding_evidence_required")
	}
	v, err := s.call(func(db *storeConn) (any, error) {
		return transact(db, func(db *storeConn) (any, error) {
			w, e := exactWorkerDB(db, worker, incarnation, controller)
			if e != nil {
				return nil, e
			}
			if !w.Fresh || w.AttestedState != "running" {
				return nil, errors.New("worker_observation_not_active")
			}
			rows, e := readInbound(db, `SELECT id,state,claim,lease_until,envelope FROM inbound WHERE id=?`, id)
			if e != nil {
				return nil, e
			}
			if len(rows) != 1 || !s.policy.Accepts(rows[0].event) {
				return nil, ErrClaim
			}
			r := rows[0]
			c, e := workerCancellationDB(db, id)
			if e != nil && e != sql.ErrNoRows {
				return nil, e
			}
			reconciling := e == nil && c.State == "cancel_requested" && c.Claim == claim
			if !reconciling {
				current, err := sourceCurrentDB(db, r.event)
				if err != nil {
					return nil, err
				}
				if !current {
					return nil, ErrClaim
				}
			}
			if !reconciling && (r.state != "claimed" || r.claim != claim || r.lease <= epoch()) {
				return nil, ErrClaim
			}
			if reconciling && c.Worker != "" && (c.Worker != worker || c.Incarnation != incarnation || c.Controller != controller) {
				return nil, errors.New("worker_binding_conflict")
			}
			var bw, bi, bc string
			e = db.QueryRow(`SELECT worker,incarnation,controller FROM worker_bindings WHERE inbound_id=? AND claim=?`, id, claim).Scan(&bw, &bi, &bc)
			if e == nil {
				if bw != worker || bi != incarnation || bc != controller {
					return nil, errors.New("worker_binding_conflict")
				}
			} else if e != sql.ErrNoRows {
				return nil, e
			} else {
				var busy int
				if e = db.QueryRow(`SELECT count(*) FROM worker_bindings WHERE worker=? AND incarnation=?`, worker, incarnation).Scan(&busy); e != nil {
					return nil, e
				}
				if busy > 0 {
					return nil, errors.New("worker_already_bound")
				}
				if _, e = db.Exec(`INSERT INTO worker_bindings VALUES(?,?,?,?,?,?,?)`, id, claim, worker, incarnation, controller, epoch(), evidence); e != nil {
					return nil, e
				}
			}
			if reconciling {
				if _, e = db.Exec(`UPDATE worker_cancellations SET worker=?,incarnation=?,controller=? WHERE inbound_id=? AND state='cancel_requested'`, worker, incarnation, controller, id); e != nil {
					return nil, e
				}
			}
			return w, nil
		})
	})
	if err != nil {
		return WorkerStatus{}, err
	}
	return v.(WorkerStatus), nil
}

func workerCancellationDB(db *storeConn, id string) (WorkerCancellation, error) {
	var c WorkerCancellation
	err := db.QueryRow(`SELECT inbound_id,claim,revision,worker,incarnation,controller,state,requested,acknowledged,evidence FROM worker_cancellations WHERE inbound_id=?`, id).Scan(&c.Request, &c.Claim, &c.Revision, &c.Worker, &c.Incarnation, &c.Controller, &c.State, &c.RequestedAt, &c.AcknowledgedAt, &c.EvidenceRef)
	c.ExecutionState = "unknown"
	if c.State == "acknowledged" {
		c.ExecutionState = "interrupted_controller_attested"
	} else if c.State == "not_required" {
		c.ExecutionState = "no_active_claim"
	} else if c.Worker == "" {
		c.ExecutionState = "worker_unbound"
	}
	return c, err
}

// requestWorkerCancellationDB records execution cancellation before revoking the
// queue claim. Queue suppression and this record commit in the same transaction.
func requestWorkerCancellationDB(db *storeConn, id, revision string) error {
	var existing int
	if err := db.QueryRow(`SELECT count(*) FROM worker_cancellations WHERE inbound_id=?`, id).Scan(&existing); err != nil || existing > 0 {
		return err
	}
	var state, claim, worker, incarnation, controller string
	if err := db.QueryRow(`SELECT state,COALESCE(claim,'') FROM inbound WHERE id=?`, id).Scan(&state, &claim); err != nil {
		return err
	}
	err := db.QueryRow(`SELECT b.claim,b.worker,b.incarnation,b.controller FROM (`+unresolvedWorkerBindingsSQL+`) b WHERE b.inbound_id=?`, id).Scan(&claim, &worker, &incarnation, &controller)
	if err != nil && err != sql.ErrNoRows {
		return err
	}
	needsAck := state == "claimed" || worker != ""
	cancellationState := "not_required"
	if needsAck {
		cancellationState = "cancel_requested"
	}
	_, err = db.Exec(`INSERT INTO worker_cancellations(inbound_id,claim,revision,worker,incarnation,controller,state,requested) VALUES(?,?,?,?,?,?,?,?)`, id, claim, revision, worker, incarnation, controller, cancellationState, epoch())
	return err
}

// AcknowledgeWorkerCancellation MUST be called only after the controller's real
// interrupt completed. The gateway cannot verify tool evidence itself. Exact
// controller, incarnation and immutable binding are required; expiration alone
// never acknowledges a stop. Repeating the same evidence is idempotent.
func (s *Store) AcknowledgeWorkerCancellation(id, worker, incarnation, controller, evidence string) (WorkerCancellation, error) {
	if !workerEvidence(evidence) {
		return WorkerCancellation{}, errors.New("worker_interrupt_evidence_required")
	}
	v, err := s.call(func(db *storeConn) (any, error) {
		return transact(db, func(db *storeConn) (any, error) {
			if _, e := exactWorkerDB(db, worker, incarnation, controller); e != nil {
				return nil, e
			}
			c, e := workerCancellationDB(db, id)
			if e != nil {
				return nil, errors.New("worker_cancellation_not_found")
			}
			if c.Worker == "" || c.Worker != worker || c.Incarnation != incarnation || c.Controller != controller {
				return nil, errors.New("worker_cancellation_target_mismatch")
			}
			if c.State == "acknowledged" {
				if c.EvidenceRef != evidence {
					return nil, errors.New("worker_cancellation_ack_conflict")
				}
				return c, nil
			}
			if c.State != "cancel_requested" {
				return nil, errors.New("worker_cancellation_not_pending")
			}
			var exact int
			if e = db.QueryRow(`SELECT count(*) FROM worker_bindings WHERE inbound_id=? AND claim=? AND worker=? AND incarnation=? AND controller=?`, id, c.Claim, worker, incarnation, controller).Scan(&exact); e != nil {
				return nil, e
			}
			if exact != 1 {
				return nil, errors.New("worker_cancellation_target_mismatch")
			}
			now := epoch()
			if e = fenceWorkerClaimsDB(db, worker, incarnation); e != nil {
				return nil, e
			}
			if _, e = db.Exec(`UPDATE worker_cancellations SET state='acknowledged',acknowledged=?,evidence=? WHERE inbound_id=?`, now, evidence, id); e != nil {
				return nil, e
			}
			if _, e = db.Exec(`UPDATE worker_runtime SET state='interrupted',observed=?,lease_until=?,evidence=? WHERE worker=?`, now, now+60, evidence, worker); e != nil {
				return nil, e
			}
			return workerCancellationDB(db, id)
		})
	})
	if err != nil {
		return WorkerCancellation{}, err
	}
	return v.(WorkerCancellation), nil
}

func (s *Store) WorkerCancellations(worker, incarnation, controller string) ([]WorkerCancellation, error) {
	v, err := s.call(func(db *storeConn) (any, error) {
		if _, e := exactWorkerDB(db, worker, incarnation, controller); e != nil {
			return nil, e
		}
		rows, e := db.Query(`SELECT inbound_id FROM worker_cancellations WHERE worker=? AND incarnation=? AND controller=? AND state='cancel_requested' ORDER BY requested,inbound_id`, worker, incarnation, controller)
		if e != nil {
			return nil, e
		}
		ids := []string{}
		for rows.Next() {
			var id string
			if e = rows.Scan(&id); e != nil {
				rows.Close()
				return nil, e
			}
			ids = append(ids, id)
		}
		e = rows.Err()
		rows.Close()
		if e != nil {
			return nil, e
		}
		out := []WorkerCancellation{}
		for _, id := range ids {
			c, e := workerCancellationDB(db, id)
			if e != nil {
				return nil, e
			}
			out = append(out, c)
		}
		return out, nil
	})
	if err != nil {
		return nil, err
	}
	return v.([]WorkerCancellation), nil
}
func workerHealthDB(db *storeConn) (map[string]any, error) {
	rows, err := db.Query(`SELECT worker FROM worker_runtime ORDER BY worker`)
	if err != nil {
		return nil, err
	}
	ids := []string{}
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	list := []WorkerStatus{}
	for _, id := range ids {
		w, e := workerStatusDB(db, id, epoch())
		if e != nil {
			return nil, e
		}
		list = append(list, w)
	}
	var pending, unbound, recovery int
	if err = db.QueryRow(`SELECT count(*),COALESCE(sum(worker=''),0) FROM worker_cancellations WHERE state='cancel_requested'`).Scan(&pending, &unbound); err != nil {
		return nil, err
	}
	if err = db.QueryRow(`SELECT count(*) FROM inbound WHERE state='worker_recovery_pending'`).Scan(&recovery); err != nil {
		return nil, err
	}
	return map[string]any{"workers": list, "observation_source": "controller_attestation", "native_control_available": false, "automatic_restart": false, "pending_cancellations": pending, "unbound_cancellations": unbound, "recovery_pending_claims": recovery}, nil
}
func (s *Store) WorkerStatus() (map[string]any, error) {
	v, err := s.call(func(db *storeConn) (any, error) { return workerHealthDB(db) })
	if err != nil {
		return nil, err
	}
	return v.(map[string]any), nil
}
