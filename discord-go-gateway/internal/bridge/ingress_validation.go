package bridge

import (
	"database/sql"
	"encoding/json"
	"errors"
)

// ValidationInput is private quarantined work. It is never returned to a
// reasoning consumer or feedback loop before validated atomic promotion.
type ValidationInput struct {
	ID       string
	Event    Envelope
	Created  float64
	Attempts int
}

func initIngressValidation(db *storeConn) error {
	_, err := db.Exec(`CREATE TABLE IF NOT EXISTS ingress_validation(
		id TEXT PRIMARY KEY, platform TEXT NOT NULL, event_id TEXT NOT NULL,
		envelope TEXT NOT NULL, created REAL NOT NULL,
		state TEXT NOT NULL DEFAULT 'pending', attempts INTEGER NOT NULL DEFAULT 0,
		next_attempt REAL NOT NULL DEFAULT 0, code TEXT,
		UNIQUE(platform,event_id));
		CREATE INDEX IF NOT EXISTS ingress_validation_due ON ingress_validation(state,next_attempt,created,id);`)
	return err
}

func activeInboundCount(db *storeConn) (int, error) {
	var n int
	err := db.QueryRow(`SELECT (SELECT count(*) FROM inbound WHERE state IN ('pending','claimed')) + (SELECT count(*) FROM ingress_validation)`).Scan(&n)
	return n, err
}

func (s *Store) StageIngress(e Envelope) (string, error) {
	if !s.policy.Accepts(e) {
		return "rejected", nil
	}
	v, err := s.call(func(db *storeConn) (any, error) {
		return transact(db, func(db *storeConn) (any, error) {
			var n int
			if err := db.QueryRow(`SELECT (SELECT count(*) FROM inbound WHERE platform=? AND event_id=?) + (SELECT count(*) FROM ingress_validation WHERE platform=? AND event_id=?)`, e.Platform, e.EventID, e.Platform, e.EventID).Scan(&n); err != nil {
				return nil, err
			}
			if n != 0 {
				return "duplicate", nil
			}
			n, err := activeInboundCount(db)
			if err != nil {
				return nil, err
			}
			if n >= 1000 {
				return "queue_full", nil
			}
			id, err := uuidHex()
			if err != nil {
				return nil, err
			}
			raw, err := json.Marshal(e)
			if err != nil {
				return nil, err
			}
			_, err = db.Exec(`INSERT INTO ingress_validation(id,platform,event_id,envelope,created) VALUES(?,?,?,?,?)`, id, e.Platform, e.EventID, string(raw), epoch())
			return "validation_staged", err
		})
	})
	if err != nil {
		return "", err
	}
	return v.(string), nil
}

func (s *Store) NextValidation(now float64) (*ValidationInput, error) {
	v, err := s.call(func(db *storeConn) (any, error) {
		var in ValidationInput
		var raw string
		// A deferred/blocked conversation cannot hold up another conversation.
		// Within a conversation, later events may not overtake unvalidated work.
		err := db.QueryRow(`SELECT v.id,v.envelope,v.created,v.attempts FROM ingress_validation v
			WHERE v.state='pending' AND v.next_attempt<=? AND NOT EXISTS(
				SELECT 1 FROM ingress_validation older WHERE older.platform=v.platform
				AND json_extract(older.envelope,'$.conversation_id')=json_extract(v.envelope,'$.conversation_id')
				AND (older.created<v.created OR (older.created=v.created AND older.rowid<v.rowid)))
			ORDER BY v.created,v.rowid LIMIT 1`, now).Scan(&in.ID, &raw, &in.Created, &in.Attempts)
		if err == sql.ErrNoRows {
			return (*ValidationInput)(nil), nil
		}
		if err != nil {
			return nil, err
		}
		if json.Unmarshal([]byte(raw), &in.Event) != nil {
			return nil, errors.New("invalid quarantined envelope")
		}
		return &in, nil
	})
	if err != nil {
		return nil, err
	}
	return v.(*ValidationInput), nil
}

func (s *Store) DeferValidation(id, code string, until float64, blocked bool) error {
	state := "pending"
	if blocked {
		state = "blocked"
	}
	_, err := s.call(func(db *storeConn) (any, error) {
		return nil, changedOne(db.Exec(`UPDATE ingress_validation SET state=?,attempts=attempts+1,next_attempt=?,code=? WHERE id=? AND state='pending'`, state, until, symbolicCode(code), id))
	})
	return err
}

// Only call after a new connection epoch has passed identity/channel validation.
func (s *Store) ResumeValidation() error {
	_, err := s.call(func(db *storeConn) (any, error) {
		_, err := db.Exec(`UPDATE ingress_validation SET state='pending',next_attempt=0 WHERE state='blocked'`)
		return nil, err
	})
	return err
}

// PromoteValidation is called only after exact channel validation. It rechecks
// current admission policy and promotes the identical durable envelope/id/time.
func (s *Store) PromoteValidation(in ValidationInput) (string, error) {
	v, err := s.call(func(db *storeConn) (any, error) {
		return transact(db, func(db *storeConn) (any, error) {
			var raw, state string
			var created float64
			if err := db.QueryRow(`SELECT envelope,created,state FROM ingress_validation WHERE id=?`, in.ID).Scan(&raw, &created, &state); err != nil {
				return nil, err
			}
			var event Envelope
			if json.Unmarshal([]byte(raw), &event) != nil {
				return nil, errors.New("invalid quarantined envelope")
			}
			if state != "pending" || event != in.Event || created != in.Created {
				return nil, errors.New("validation_input_changed")
			}
			if !s.policy.Accepts(event) {
				_, err := db.Exec(`UPDATE ingress_validation SET state='blocked',code='authorization_revoked' WHERE id=?`, in.ID)
				return "validation_blocked", err
			}
			var n int
			if err := db.QueryRow(`SELECT count(*) FROM inbound WHERE platform=? AND event_id=?`, event.Platform, event.EventID).Scan(&n); err != nil {
				return nil, err
			}
			outcome := "duplicate"
			if n == 0 {
				// Staging reserved capacity; promotion does not admit another item.
				if _, err := db.Exec(`INSERT INTO inbound(id,platform,event_id,envelope,created) VALUES(?,?,?,?,?)`, in.ID, event.Platform, event.EventID, raw, created); err != nil {
					return nil, err
				}
				if err := insertTiming(db, in.ID, "ingested", epoch(), epoch()-created); err != nil {
					return nil, err
				}
				outcome = "accepted"
			}
			_, err := db.Exec(`DELETE FROM ingress_validation WHERE id=?`, in.ID)
			return outcome, err
		})
	})
	if err != nil {
		return "", err
	}
	return v.(string), nil
}
