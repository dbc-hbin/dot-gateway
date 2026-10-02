package main

// Native execution is outside the restored gateway's control. Preserve only
// request-level reconciliation markers, never worker identities, claim tokens,
// leases, observations or evidence that could be mistaken for fresh authority.
import (
	"database/sql"
	"errors"
	"sort"
)

type WorkerCancellationMetadata struct {
	InboundID    string  `json:"inbound_id"`
	State        string  `json:"state"`
	Requested    float64 `json:"requested"`
	Acknowledged float64 `json:"acknowledged"`
}

type WorkerControlState struct {
	Version            int                          `json:"version"`
	ApplicationID      string                       `json:"application_id"`
	GuildID            string                       `json:"guild_id"`
	OwnerID            string                       `json:"owner_id"`
	UnresolvedRequests []string                     `json:"unresolved_requests,omitempty"`
	RecoveryPending    []string                     `json:"recovery_pending,omitempty"`
	Cancellations      []WorkerCancellationMetadata `json:"cancellations,omitempty"`
}

func snapshotWorkerControl(tx *sql.Tx, s *Snapshot) error {
	out := &WorkerControlState{Version: 1, ApplicationID: s.Operation.BotID, GuildID: s.Operation.GuildID, OwnerID: s.Operation.OwnerID}
	unresolved, pending := map[string]bool{}, map[string]bool{}
	readIDs := func(table, query string, dst map[string]bool) error {
		return phase3Read(tx, table, query, phase3RowLimit, func(r *sql.Rows) error {
			var id string
			if err := r.Scan(&id); err != nil {
				return err
			}
			dst[id] = true
			return nil
		})
	}
	// Match the immutable binding, not inbound.claim: source refresh can revoke
	// the queue claim while its native execution is still running. An expired
	// observation does not establish a stop. A successor runtime means the old
	// incarnation was explicitly retired; missing runtime is conservatively held.
	if err := readIDs("worker_bindings", `SELECT DISTINCT b.inbound_id FROM worker_bindings b LEFT JOIN worker_runtime w ON w.worker=b.worker WHERE w.worker IS NULL OR (w.incarnation=b.incarnation AND (w.controller!=b.controller OR w.state NOT IN('completed','interrupted','failed'))) ORDER BY b.inbound_id`, unresolved); err != nil {
		return err
	}
	if err := readIDs("inbound", `SELECT id FROM inbound WHERE state='worker_recovery_pending' ORDER BY id`, pending); err != nil {
		return err
	}
	if err := phase3Read(tx, "recovery_worker_execution_fences", `SELECT inbound_id,kind FROM recovery_worker_execution_fences ORDER BY inbound_id,kind`, phase3RowLimit, func(r *sql.Rows) error {
		var id, kind string
		if err := r.Scan(&id, &kind); err != nil {
			return err
		}
		switch kind {
		case "unresolved_execution":
			unresolved[id] = true
		case "recovery_pending":
			pending[id] = true
		default:
			return errors.New("invalid worker execution recovery kind")
		}
		return nil
	}); err != nil {
		return err
	}
	cancellations := map[string]WorkerCancellationMetadata{}
	for _, table := range []string{"worker_cancellations", "recovery_worker_cancellation_fences"} {
		if err := phase3Read(tx, table, `SELECT inbound_id,state,requested,acknowledged FROM `+table+` ORDER BY inbound_id`, phase3RowLimit, func(r *sql.Rows) error {
			var v WorkerCancellationMetadata
			if err := r.Scan(&v.InboundID, &v.State, &v.Requested, &v.Acknowledged); err != nil {
				return err
			}
			if old, ok := cancellations[v.InboundID]; ok && old != v {
				return errors.New("conflicting worker cancellation recovery metadata")
			}
			cancellations[v.InboundID] = v
			return nil
		}); err != nil {
			return err
		}
	}
	for id := range unresolved {
		out.UnresolvedRequests = append(out.UnresolvedRequests, id)
	}
	for id := range pending {
		out.RecoveryPending = append(out.RecoveryPending, id)
	}
	for _, v := range cancellations {
		out.Cancellations = append(out.Cancellations, v)
	}
	sort.Strings(out.UnresolvedRequests)
	sort.Strings(out.RecoveryPending)
	sort.Slice(out.Cancellations, func(i, j int) bool { return out.Cancellations[i].InboundID < out.Cancellations[j].InboundID })
	if len(out.UnresolvedRequests)+len(out.RecoveryPending)+len(out.Cancellations) > 0 {
		s.WorkerControl = out
	}
	return validateWorkerControl(*s)
}

func validateWorkerControl(s Snapshot) error {
	p := s.WorkerControl
	if p == nil {
		return nil
	}
	if p.Version != 1 || p.ApplicationID != s.Operation.BotID || p.GuildID != s.Operation.GuildID || p.OwnerID != s.Operation.OwnerID || !snowflakeID(p.ApplicationID) || !snowflakeID(p.GuildID) || !snowflakeID(p.OwnerID) {
		return errors.New("worker control recovery scope mismatch")
	}
	if len(p.UnresolvedRequests)+len(p.RecoveryPending) > phase3RowLimit || len(p.Cancellations) > phase3RowLimit {
		return errors.New("worker control recovery limit exceeded")
	}
	inbound := map[string]bool{}
	for _, v := range s.Events {
		inbound[v.ID] = true
	}
	for _, ids := range [][]string{p.UnresolvedRequests, p.RecoveryPending} {
		seen := map[string]bool{}
		for _, id := range ids {
			if !validID(id) || !inbound[id] || seen[id] {
				return errors.New("invalid worker execution recovery fence")
			}
			seen[id] = true
		}
	}
	seen := map[string]bool{}
	for _, v := range p.Cancellations {
		if !validID(v.InboundID) || !inbound[v.InboundID] || seen[v.InboundID] || !finitePositive(v.Requested) {
			return errors.New("invalid worker cancellation recovery metadata")
		}
		switch v.State {
		case "cancel_requested", "not_required":
			if v.Acknowledged != 0 {
				return errors.New("invalid worker cancellation acknowledgement")
			}
		case "acknowledged":
			if !finitePositive(v.Acknowledged) || v.Acknowledged < v.Requested {
				return errors.New("invalid worker cancellation acknowledgement")
			}
		default:
			return errors.New("invalid worker cancellation state")
		}
		seen[v.InboundID] = true
	}
	return nil
}

func restoreWorkerControl(tx *sql.Tx, s Snapshot) error {
	p := s.WorkerControl
	if p == nil {
		return nil
	}
	if err := validateWorkerControl(s); err != nil {
		return err
	}
	_, err := tx.Exec(`CREATE TABLE recovery_worker_execution_fences(inbound_id TEXT NOT NULL REFERENCES inbound(id),kind TEXT NOT NULL,PRIMARY KEY(inbound_id,kind));
 CREATE TABLE recovery_worker_cancellation_fences(inbound_id TEXT PRIMARY KEY REFERENCES inbound(id),state TEXT NOT NULL,requested REAL NOT NULL,acknowledged REAL NOT NULL);`)
	if err != nil {
		return err
	}
	for kind, ids := range map[string][]string{"unresolved_execution": p.UnresolvedRequests, "recovery_pending": p.RecoveryPending} {
		for _, id := range ids {
			if _, err = tx.Exec(`INSERT INTO recovery_worker_execution_fences VALUES(?,?)`, id, kind); err != nil {
				return err
			}
		}
	}
	for _, v := range p.Cancellations {
		if _, err = tx.Exec(`INSERT INTO recovery_worker_cancellation_fences VALUES(?,?,?,?)`, v.InboundID, v.State, v.Requested, v.Acknowledged); err != nil {
			return err
		}
	}
	return nil
}
