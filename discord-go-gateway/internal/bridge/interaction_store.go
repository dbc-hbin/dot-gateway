package bridge

import (
	"database/sql"
	"encoding/json"
	"errors"
	"time"
)

func initControlStore(db *storeConn) error {
	_, err := db.Exec(`CREATE TABLE IF NOT EXISTS control_interactions(id TEXT PRIMARY KEY,owner TEXT NOT NULL,channel TEXT NOT NULL,action TEXT NOT NULL,state TEXT NOT NULL,created REAL NOT NULL,inbound_id TEXT);
 CREATE TABLE IF NOT EXISTS control_bindings(id TEXT PRIMARY KEY,owner TEXT NOT NULL,channel TEXT NOT NULL,guild TEXT NOT NULL,action TEXT NOT NULL,request TEXT NOT NULL,revision TEXT NOT NULL,message TEXT NOT NULL DEFAULT '',expires REAL NOT NULL,used INTEGER NOT NULL DEFAULT 0);
 CREATE TABLE IF NOT EXISTS control_target_invalidations(channel TEXT NOT NULL,message TEXT NOT NULL,code TEXT NOT NULL,PRIMARY KEY(channel,message));
 CREATE TABLE IF NOT EXISTS control_reactions(channel TEXT NOT NULL,message TEXT NOT NULL,owner TEXT NOT NULL,emoji TEXT NOT NULL,present INTEGER NOT NULL,sequence INTEGER NOT NULL,PRIMARY KEY(channel,message,owner,emoji));
 CREATE TABLE IF NOT EXISTS control_pending_responses(id TEXT PRIMARY KEY,request TEXT NOT NULL,revision TEXT NOT NULL,message TEXT NOT NULL,expires REAL NOT NULL,used INTEGER NOT NULL DEFAULT 0);
 CREATE TABLE IF NOT EXISTS control_registration_attempts(key TEXT PRIMARY KEY,name TEXT NOT NULL,action TEXT NOT NULL,state TEXT NOT NULL,command_id TEXT NOT NULL,created REAL NOT NULL);
 CREATE UNIQUE INDEX IF NOT EXISTS control_pending_exact_question ON control_pending_responses(request,revision,message);
 CREATE TABLE IF NOT EXISTS control_command_ids(guild TEXT NOT NULL,name TEXT NOT NULL,id TEXT NOT NULL,PRIMARY KEY(guild,name));`)
	return err
}
func (s *Store) reserveInteraction(id string, e Envelope, action string) (bool, error) {
	v, err := s.call(func(db *storeConn) (any, error) {
		res, err := db.Exec("INSERT OR IGNORE INTO control_interactions VALUES(?,?,?,?,?,?,NULL)", id, e.SenderID, e.ConversationID, action, "received", epoch())
		if err != nil {
			return false, err
		}
		n, err := res.RowsAffected()
		return n == 1, err
	})
	if err != nil {
		return false, err
	}
	return v.(bool), nil
}
func (s *Store) interactionState(id, state string) error {
	_, err := s.call(func(db *storeConn) (any, error) {
		_, err := db.Exec("UPDATE control_interactions SET state=? WHERE id=?", state, id)
		return nil, err
	})
	return err
}
func (s *Store) ingestControl(e Envelope, interactionID string) (string, error) {
	if !s.policy.Accepts(e) || e.Control == "" {
		return "", errors.New("invalid_control")
	}
	v, err := s.call(func(db *storeConn) (any, error) {
		return transact(db, func(db *storeConn) (any, error) {
			n, err := activeInboundCount(db)
			if err != nil {
				return nil, err
			}
			if n >= 1000 {
				return nil, errors.New("queue_full")
			}
			c, _ := e.ControlEvent()
			key := "control:" + c.ID
			var existing string
			err = db.QueryRow("SELECT id FROM inbound WHERE platform=? AND event_id=?", e.Platform, key).Scan(&existing)
			if err == nil {
				return existing, nil
			}
			if err != sql.ErrNoRows {
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
			if interactionID != "" {
				var state string
				if err = db.QueryRow("SELECT state FROM control_interactions WHERE id=? AND owner=? AND channel=?", interactionID, e.SenderID, e.ConversationID).Scan(&state); err != nil || state != "acknowledged" {
					return nil, errors.New("interaction_not_acknowledged")
				}
			}
			_, err = db.Exec("INSERT INTO inbound(id,platform,event_id,envelope,created) VALUES(?,?,?,?,?)", id, e.Platform, key, string(raw), epoch())
			if err != nil {
				return nil, err
			}
			if interactionID != "" {
				_, err = db.Exec("UPDATE control_interactions SET state='queued',inbound_id=? WHERE id=?", id, interactionID)
			}
			return id, err
		})
	})
	if err != nil {
		return "", err
	}
	return v.(string), nil
}

type ControlRequest struct {
	ID            string              `json:"id"`
	Revision      string              `json:"revision"`
	State         string              `json:"state"`
	Source        Envelope            `json:"-"`
	DeliveryState string              `json:"delivery_state,omitempty"`
	Cancellation  *WorkerCancellation `json:"cancellation,omitempty"`
	Worker        *WorkerStatus       `json:"worker,omitempty"`
}

func requestStatusDB(db *storeConn, id string, route Envelope) (ControlRequest, error) {
	var r ControlRequest
	var raw string
	err := db.QueryRow("SELECT id,state,envelope FROM inbound WHERE id=?", id).Scan(&r.ID, &r.State, &raw)
	if err != nil {
		return r, errors.New("request_not_found")
	}
	if json.Unmarshal([]byte(raw), &r.Source) != nil {
		return r, errors.New("invalid_request")
	}
	if r.Source.SenderID != route.SenderID || r.Source.ConversationID != route.ConversationID || r.Source.GuildID != route.GuildID || r.Source.Platform != route.Platform {
		return r, errors.New("request_not_found")
	}
	r.Revision = controlRevision(r.Source)
	var worker, incarnation string
	err = db.QueryRow(`SELECT b.worker,b.incarnation FROM (`+unresolvedWorkerBindingsSQL+`) b WHERE b.inbound_id=?`, id).Scan(&worker, &incarnation)
	if err == nil {
		w, e := workerStatusDB(db, worker, epoch())
		if e != nil {
			return r, e
		}
		if w.Incarnation == incarnation {
			r.Worker = &w
		}
	} else if err != sql.ErrNoRows {
		return r, err
	}
	var reply string
	err = db.QueryRow("SELECT id FROM replies WHERE inbound_id=?", id).Scan(&reply)
	if err == nil {
		d, err := delivery(db, reply)
		if err != nil {
			return r, err
		}
		for _, c := range d.Chunks {
			switch c.State {
			case "pending", "sending", "sent", "failed", "uncertain", "cancelled":
			default:
				return r, errors.New("invalid_request_delivery_state")
			}
		}
		if r.State != "cancelled" || d.State == "uncertain" || d.State == "sending" || d.State == "sent" {
			r.State = d.State
		}
		r.DeliveryState = d.State
	} else if err != sql.ErrNoRows {
		return r, err
	}
	var unavailable int
	if err := db.QueryRow("SELECT count(*) FROM control_interactions WHERE inbound_id=? AND state='token_unavailable'", id).Scan(&unavailable); err != nil {
		return r, err
	}
	if unavailable > 0 && r.State != "uncertain" && r.State != "sending" {
		r.State = "unavailable_reissue_required"
	}
	switch r.State {
	case "worker_recovery_pending":
		r.State = "recovery_required"
	case "claimed":
		r.State = "processing"
	case "replied", "queued":
		r.State = "pending"
	case "sent":
		r.State = "delivered"
	case "ignored":
		r.State = "cancelled"
	}
	c, err := workerCancellationDB(db, id)
	if err == nil {
		r.Cancellation = &c
		if c.State == "cancel_requested" {
			r.State = "cancel_requested"
		}
	} else if err != sql.ErrNoRows {
		return r, err
	}
	return r, nil
}
func (s *Store) ControlRequests(route Envelope, id string) ([]ControlRequest, error) {
	v, err := s.call(func(db *storeConn) (any, error) {
		ids := []string{}
		if id != "" {
			ids = append(ids, id)
		} else {
			rows, err := db.Query(`SELECT i.id FROM inbound i WHERE json_extract(i.envelope,'$.sender_id')=? AND json_extract(i.envelope,'$.conversation_id')=? AND COALESCE(json_extract(i.envelope,'$.guild_id'),'')=? ORDER BY i.created DESC,i.id DESC LIMIT 1001`, route.SenderID, route.ConversationID, route.GuildID)
			if err != nil {
				return nil, err
			}
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
		}
		out := []ControlRequest{}
		for _, id := range ids {
			r, err := requestStatusDB(db, id, route)
			if err != nil {
				return nil, err
			}
			out = append(out, r)
		}
		return out, nil
	})
	if err != nil {
		return nil, err
	}
	return v.([]ControlRequest), nil
}

// Cancel only this exact immutable request. In-flight and uncertain outcomes are
// preserved and continue blocking later replies; cancelling cannot undo actions.
func (s *Store) CancelControlRequest(route Envelope, id, revision string) (ControlRequest, error) {
	v, err := s.call(func(db *storeConn) (any, error) {
		return transact(db, func(db *storeConn) (any, error) {
			r, err := requestStatusDB(db, id, route)
			if err != nil {
				return nil, err
			}
			if r.Revision != revision {
				return nil, errors.New("request_revision_changed")
			}
			if r.Cancellation != nil || r.State == "cancelled" {
				return r, nil
			}
			// A completed delivery can still have an explicitly bound native turn.
			var bound int
			if err = db.QueryRow(`SELECT count(*) FROM (`+unresolvedWorkerBindingsSQL+`) b WHERE b.inbound_id=?`, id).Scan(&bound); err != nil {
				return nil, err
			}
			if r.State == "delivered" && bound == 0 {
				return r, nil
			}
			if err = requestWorkerCancellationDB(db, id, revision); err != nil {
				return nil, err
			}
			if _, err = db.Exec("UPDATE inbound SET state='cancelled',claim=NULL,lease_until=NULL WHERE id=?", id); err != nil {
				return nil, err
			}
			for _, q := range []string{"DELETE FROM processing WHERE inbound_id=?", "DELETE FROM consumer_claims WHERE inbound_id=?"} {
				if _, err = db.Exec(q, id); err != nil {
					return nil, err
				}
			}
			if _, err = db.Exec("UPDATE chunks SET state='cancelled',code='owner_cancelled' WHERE reply_id IN (SELECT id FROM replies WHERE inbound_id=?) AND state='pending'", id); err != nil {
				return nil, err
			}
			if _, err = db.Exec("UPDATE control_pending_responses SET used=1 WHERE request=?", id); err != nil {
				return nil, err
			}
			if _, err = db.Exec("UPDATE control_bindings SET used=1 WHERE request=?", id); err != nil {
				return nil, err
			}
			return requestStatusDB(db, id, route)
		})
	})
	if err != nil {
		return ControlRequest{}, err
	}
	return v.(ControlRequest), nil
}
func (s *Store) controlRequestCurrent(e Envelope) bool {
	v, err := s.call(func(db *storeConn) (any, error) {
		current, err := sourceCurrentDB(db, e)
		if err != nil || !current {
			return false, err
		}
		var n int
		err = db.QueryRow("SELECT count(*) FROM inbound WHERE platform=? AND state!='cancelled' AND envelope=?", e.Platform, mustEnvelopeJSON(e)).Scan(&n)
		return n == 1, err
	})
	return err == nil && v.(bool)
}
func mustEnvelopeJSON(e Envelope) string { b, _ := json.Marshal(e); return string(b) }

type controlBinding struct {
	ID, Owner, Channel, Guild, Action, Request, Revision, Message string
	Expires                                                       float64
}

func (s *Store) newBinding(route Envelope, action, request, revision, message string) (controlBinding, error) {
	id, err := uuidHex()
	if err != nil {
		return controlBinding{}, err
	}
	b := controlBinding{"dot:" + id, route.SenderID, route.ConversationID, route.GuildID, action, request, revision, message, epoch() + 300}
	_, err = s.call(func(db *storeConn) (any, error) {
		_, _ = db.Exec("DELETE FROM control_bindings WHERE expires<?", epoch())
		_, err := db.Exec("INSERT INTO control_bindings VALUES(?,?,?,?,?,?,?,?,?,0)", b.ID, b.Owner, b.Channel, b.Guild, b.Action, b.Request, b.Revision, b.Message, b.Expires)
		return nil, err
	})
	return b, err
}
func (s *Store) consumeBinding(id string, route Envelope, action, message string) (controlBinding, error) {
	v, err := s.call(func(db *storeConn) (any, error) {
		return transact(db, func(db *storeConn) (any, error) {
			var b controlBinding
			var used int
			err := db.QueryRow("SELECT id,owner,channel,guild,action,request,revision,message,expires,used FROM control_bindings WHERE id=?", id).Scan(&b.ID, &b.Owner, &b.Channel, &b.Guild, &b.Action, &b.Request, &b.Revision, &b.Message, &b.Expires, &used)
			if err != nil || used != 0 || b.Expires < epoch() || b.Owner != route.SenderID || b.Channel != route.ConversationID || b.Guild != route.GuildID || b.Action != action || (action == "cancel" && (b.Message == "" || b.Message != message)) {
				return nil, errors.New("invalid_or_expired_control")
			}
			if b.Request != "" {
				r, err := requestStatusDB(db, b.Request, route)
				if err != nil || r.Revision != b.Revision || r.State == "cancelled" {
					return nil, errors.New("request_revision_changed")
				}
			}
			if err = changedOne(db.Exec("UPDATE control_bindings SET used=1 WHERE id=? AND used=0", id)); err != nil {
				return nil, err
			}
			return b, nil
		})
	})
	if err != nil {
		return controlBinding{}, err
	}
	return v.(controlBinding), nil
}

// Optional assistant-authored pending-question binding. A match is consumed once
// when a reaction transition is recorded; it conveys context, never permission.
func (s *Store) BindPendingResponse(request, claim, message string) (string, error) {
	id, err := uuidHex()
	if err != nil {
		return "", err
	}
	v, err := s.call(func(db *storeConn) (any, error) {
		return transact(db, func(db *storeConn) (any, error) {
			rows, err := readInbound(db, "SELECT id,state,claim,lease_until,envelope FROM inbound WHERE id=?", request)
			if err != nil || len(rows) != 1 {
				return nil, ErrClaim
			}
			r := rows[0]
			if r.claim != claim || r.state != "replied" {
				return nil, ErrClaim
			}
			var n int
			if err = db.QueryRow("SELECT count(*) FROM chunks c JOIN replies r ON r.id=c.reply_id WHERE r.inbound_id=? AND c.message_id=? AND c.state='sent'", request, message).Scan(&n); err != nil || n != 1 {
				return nil, errors.New("unknown_sent_message")
			}
			target, err := sentControlTargetDB(db, r.event.ConversationID, message)
			if err != nil || target.Request != request {
				return nil, errors.New("unknown_sent_message")
			}
			revision := target.Revision
			if _, err = db.Exec("INSERT OR IGNORE INTO control_pending_responses VALUES(?,?,?,?,?,0)", id, request, revision, message, epoch()+float64(15*time.Minute/time.Second)); err != nil {
				return nil, err
			}
			var existing string
			err = db.QueryRow("SELECT id FROM control_pending_responses WHERE request=? AND revision=? AND message=?", request, revision, message).Scan(&existing)
			return existing, err
		})
	})
	if err != nil {
		return "", err
	}
	return v.(string), nil
}

// RecoverInteractionTokens runs only when the gateway starts with an empty token
// vault. Definitely-unsent requests become terminally unavailable; existing
// sending/uncertain evidence remains unresolved and never advances silently.
func (s *Store) RecoverInteractionTokens() error {
	_, err := s.call(func(db *storeConn) (any, error) {
		return transact(db, func(db *storeConn) (any, error) {
			rows, err := db.Query(`SELECT id FROM inbound WHERE json_extract(envelope,'$.reply_kind')='interaction' AND state IN ('pending','claimed','replied') AND (state!='replied' OR EXISTS(SELECT 1 FROM replies r JOIN chunks c ON c.reply_id=r.id WHERE r.inbound_id=inbound.id AND c.state!='sent'))`)
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
			for _, id := range ids {
				if err = expireInteractionDB(db, id); err != nil {
					return nil, err
				}
			}
			return nil, nil
		})
	})
	return err
}
func expireInteractionDB(db *storeConn, id string) error {
	for _, q := range []string{"UPDATE inbound SET state='cancelled',claim=NULL,lease_until=NULL WHERE id=?", "DELETE FROM processing WHERE inbound_id=?", "DELETE FROM consumer_claims WHERE inbound_id=?", "UPDATE control_bindings SET used=1 WHERE request=?", "UPDATE control_pending_responses SET used=1 WHERE request=?", "UPDATE control_interactions SET state='token_unavailable' WHERE inbound_id=?", "UPDATE chunks SET state='cancelled',code='interaction_token_unavailable_reissue_required' WHERE reply_id IN (SELECT id FROM replies WHERE inbound_id=?) AND state='pending'"} {
		if _, err := db.Exec(q, id); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) commandIDMatches(guild, name, id string) bool {
	v, err := s.call(func(db *storeConn) (any, error) {
		var n int
		err := db.QueryRow("SELECT count(*) FROM control_command_ids WHERE guild=? AND name=? AND id=?", guild, name, id).Scan(&n)
		return n == 1, err
	})
	return err == nil && v.(bool)
}

func (s *Store) expireInteractionID(id string) error {
	_, err := s.call(func(db *storeConn) (any, error) {
		return transact(db, func(db *storeConn) (any, error) {
			var inbound string
			err := db.QueryRow("SELECT inbound_id FROM control_interactions WHERE id=? AND inbound_id IS NOT NULL", id).Scan(&inbound)
			if err == sql.ErrNoRows {
				return nil, nil
			}
			if err != nil {
				return nil, err
			}
			var n int
			err = db.QueryRow("SELECT count(*) FROM inbound i WHERE i.id=? AND (i.state IN ('pending','claimed','worker_recovery_pending') OR EXISTS(SELECT 1 FROM replies r JOIN chunks c ON c.reply_id=r.id WHERE r.inbound_id=i.id AND c.state!='sent'))", inbound).Scan(&n)
			if err != nil || n == 0 {
				return nil, err
			}
			return nil, expireInteractionDB(db, inbound)
		})
	})
	return err
}

// Cancellation's empty-ID selector examines all active candidates, not merely
// recent history. A truncated status page must never turn many into "exactly one".
func (s *Store) activeControlRequests(route Envelope) ([]ControlRequest, error) {
	v, err := s.call(func(db *storeConn) (any, error) {
		rows, err := db.Query(`SELECT i.id FROM inbound i WHERE json_extract(i.envelope,'$.sender_id')=? AND json_extract(i.envelope,'$.conversation_id')=? AND COALESCE(json_extract(i.envelope,'$.guild_id'),'')=? AND (i.state IN ('pending','claimed','worker_recovery_pending') OR EXISTS(SELECT 1 FROM worker_cancellations wc WHERE wc.inbound_id=i.id AND wc.state='cancel_requested') OR EXISTS(SELECT 1 FROM (`+unresolvedWorkerBindingsSQL+`) wb WHERE wb.inbound_id=i.id) OR EXISTS(SELECT 1 FROM replies r JOIN chunks c ON c.reply_id=r.id WHERE r.inbound_id=i.id AND (c.state IN ('sending','uncertain') OR (i.state!='cancelled' AND c.state IN ('pending','failed'))))) ORDER BY i.created LIMIT 2`, route.SenderID, route.ConversationID, route.GuildID)
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
		out := []ControlRequest{}
		for _, id := range ids {
			r, err := requestStatusDB(db, id, route)
			if err != nil {
				return nil, err
			}
			out = append(out, r)
		}
		return out, nil
	})
	if err != nil {
		return nil, err
	}
	return v.([]ControlRequest), nil
}

// sourceCurrentDB dispatches controls here before the ordinary-message head
// check. Both the control row and every underlying question revision must remain
// current. The bounded recursion permits a reply to a prior reaction without
// admitting cycles or unbounded historical chains.
func controlSourceCurrentDB(db *storeConn, e Envelope, depth int) (bool, error) {
	if depth >= 32 {
		return false, nil
	}
	ce, err := e.ControlEvent()
	if err != nil {
		return false, nil
	}
	var n int
	if err = db.QueryRow("SELECT count(*) FROM inbound WHERE platform=? AND event_id=? AND envelope=? AND state IN ('pending','claimed','replied')", e.Platform, "control:"+ce.ID, mustEnvelopeJSON(e)).Scan(&n); err != nil || n != 1 {
		return false, err
	}
	if ce.Kind == "ask" {
		return true, nil
	}
	target, err := sentControlTargetDB(db, e.ConversationID, ce.TargetMessageID)
	if err != nil {
		return false, nil
	}
	if target.Request != ce.TargetRequestID || target.Revision != ce.TargetRevision || target.Text != ce.TargetText || target.Output != string(ce.TargetOutput) || target.Receipt != string(ce.TargetReceipt) {
		return false, nil
	}
	if err = db.QueryRow("SELECT count(*) FROM inbound WHERE id=? AND state='replied'", target.Request).Scan(&n); err != nil || n != 1 {
		return false, err
	}
	return sourceCurrentDBDepth(db, target.Source, depth+1)
}
func claimSourceCurrentDB(db *storeConn, id, claim string) error {
	rows, err := readInbound(db, "SELECT id,state,claim,lease_until,envelope FROM inbound WHERE id=?", id)
	if err != nil {
		return err
	}
	if len(rows) != 1 || rows[0].claim != claim {
		return ErrClaim
	}
	current, err := sourceCurrentDB(db, rows[0].event)
	if err != nil {
		return err
	}
	if !current {
		return ErrClaim
	}
	return nil
}

func dependentControlIDsDB(db *storeConn, e Envelope) ([]string, error) {
	rows, err := db.Query(`WITH RECURSIVE affected(id) AS (
 SELECT child.id FROM inbound source JOIN inbound child ON json_extract(child.envelope,'$.control.target_request_id')=source.id WHERE source.platform=? AND source.event_id=?
 UNION SELECT child.id FROM inbound child JOIN affected parent ON json_extract(child.envelope,'$.control.target_request_id')=parent.id)
 SELECT id FROM affected`, e.Platform, sourceLedgerKey(e))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ids := []string{}
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}
func revokeDependentControlClaimsDB(db *storeConn, e Envelope, terminal bool) error {
	ids, err := dependentControlIDsDB(db, e)
	if err != nil {
		return err
	}
	for _, id := range ids {
		for _, q := range []string{"DELETE FROM consumer_claims WHERE inbound_id=?", "DELETE FROM processing WHERE inbound_id=?", "UPDATE inbound SET state='pending',claim=NULL,lease_until=NULL WHERE id=? AND state='claimed'", "UPDATE control_pending_responses SET used=1 WHERE request=?", "UPDATE control_bindings SET used=1 WHERE request=?"} {
			if _, err = db.Exec(q, id); err != nil {
				return err
			}
		}
		if terminal {
			if _, err = db.Exec("UPDATE inbound SET state='cancelled',claim=NULL,lease_until=NULL WHERE id=?", id); err != nil {
				return err
			}
			if _, err = db.Exec("UPDATE chunks SET state='cancelled',code='control_source_not_current' WHERE reply_id IN (SELECT id FROM replies WHERE inbound_id=?) AND state='pending'", id); err != nil {
				return err
			}
		}
	}
	return nil
}

// A known sent bot question changed/deleted after reaction admission must not
// remain usable as approval context. Even cosmetic target updates fail closed;
// send a new question rather than resurrecting an old single-use binding.
func (s *Store) InvalidateControlTarget(channel, guild, message, code string) (bool, error) {
	if code != "target_updated" && code != "target_deleted" {
		return false, errors.New("invalid_target_reason")
	}
	v, err := s.call(func(db *storeConn) (any, error) {
		return transact(db, func(db *storeConn) (any, error) {
			target, _, ownedErr := operationOwnedChunkDB(db, channel, message)
			owned := ownedErr == nil && (guild == "" || target.Source.GuildID == guild)
			var known int
			if err := db.QueryRow(`SELECT count(*) FROM message_operation_targets WHERE channel=? AND message=? AND (?='' OR guild=?)`, channel, message, guild, guild).Scan(&known); err != nil {
				return nil, err
			}
			if !owned && known == 0 {
				return false, nil
			}
			if err := invalidateEditedProjectionDB(db, channel, guild, message); err != nil {
				return nil, err
			}
			if owned {
				// Original messages edited externally also become unknown memory evidence.
				if _, err := db.Exec(`INSERT OR IGNORE INTO message_edit_projection(channel,message,revision,state,operation_id,memory_key) VALUES(?,?,0,'unknown','','')`, channel, message); err != nil {
					return nil, err
				}
				if _, err := db.Exec("INSERT OR IGNORE INTO control_target_invalidations VALUES(?,?,?)", channel, message, code); err != nil {
					return nil, err
				}
				if err := invalidateOperationBindingsDB(db, channel, message); err != nil {
					return nil, err
				}
			}
			return true, nil
		})
	})
	if err != nil {
		return false, err
	}
	return v.(bool), nil
}
