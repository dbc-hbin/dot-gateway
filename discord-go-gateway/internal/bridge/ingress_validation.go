package bridge

import (
	"database/sql"
	"encoding/json"
	"errors"
	"time"
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
	err := db.QueryRow(`SELECT (SELECT count(*) FROM inbound WHERE state IN ('pending','claimed','worker_recovery_pending')) + (SELECT count(*) FROM ingress_validation WHERE state IN ('pending','blocked'))`).Scan(&n)
	return n, err
}

func (s *Store) stages(e Envelope) bool {
	if policy, ok := s.policy.(interface{ Stages(Envelope) bool }); ok {
		return policy.Stages(e)
	}
	return s.policy.Accepts(e)
}

func (s *Store) StageIngress(e Envelope) (string, error) {
	if e.RouteKind == "guild_thread" {
		e.RouteKind, e.ParentChannelID, e.ThreadType, e.ThreadName = "guild_thread_candidate", "", 0, ""
	}
	if !s.stages(e) {
		return "rejected", nil
	}
	v, err := s.call(func(db *storeConn) (any, error) {
		return transact(db, func(db *storeConn) (any, error) {
			capacity := 1000
			enabled, err := catchupEnabledDB(db)
			if err != nil {
				return nil, err
			}
			if enabled {
				capacity = catchupLiveCapacity
			}
			created := epoch()
			fenced, err := catchupFencedDB(db, e)
			if err != nil {
				return nil, err
			}
			if fenced {
				created = snowCreated(e.EventID)
			}
			outcome, err := s.stageIngressDB(db, e, created, capacity)
			if err == nil {
				err = catchupLiveProgressDB(db, e, outcome)
			}
			return outcome, err
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
			WHERE v.state='pending' AND v.next_attempt<=? AND NOT EXISTS(SELECT 1 FROM catchup_routes h WHERE h.channel_id=json_extract(v.envelope,'$.conversation_id') AND h.state NOT IN ('idle','disarmed') AND (length(h.floor)<length(json_extract(v.envelope,'$.event_id')) OR (length(h.floor)=length(json_extract(v.envelope,'$.event_id')) AND h.floor<json_extract(v.envelope,'$.event_id')))) AND NOT EXISTS(SELECT 1 FROM message_sources src WHERE src.platform=v.platform AND src.event_id=json_extract(v.envelope,'$.event_id') AND (src.state!='current' OR src.revision!=COALESCE(json_extract(v.envelope,'$.source_revision'),0))) AND NOT EXISTS(
				SELECT 1 FROM ingress_validation older WHERE older.state IN ('pending','blocked') AND older.platform=v.platform
				AND json_extract(older.envelope,'$.conversation_id')=json_extract(v.envelope,'$.conversation_id')
				AND (
                    (EXISTS(SELECT 1 FROM catchup_routes h WHERE h.channel_id=json_extract(v.envelope,'$.conversation_id')) AND COALESCE(json_extract(older.envelope,'$.control'),'')='' AND COALESCE(json_extract(v.envelope,'$.control'),'')='' AND
                        (length(json_extract(older.envelope,'$.event_id'))<length(json_extract(v.envelope,'$.event_id')) OR (length(json_extract(older.envelope,'$.event_id'))=length(json_extract(v.envelope,'$.event_id')) AND json_extract(older.envelope,'$.event_id')<json_extract(v.envelope,'$.event_id')) OR (json_extract(older.envelope,'$.event_id')=json_extract(v.envelope,'$.event_id') AND older.rowid<v.rowid)))
                    OR ((NOT EXISTS(SELECT 1 FROM catchup_routes h WHERE h.channel_id=json_extract(v.envelope,'$.conversation_id')) OR COALESCE(json_extract(older.envelope,'$.control'),'')!='' OR COALESCE(json_extract(v.envelope,'$.control'),'')!='') AND (older.created<v.created OR (older.created=v.created AND older.rowid<v.rowid)))))
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
		_, err := db.Exec(`UPDATE ingress_validation SET state=?,attempts=attempts+1,next_attempt=?,code=? WHERE id=? AND state='pending'`, state, until, symbolicCode(code), id)
		return nil, err
	})
	return err
}

// A proven out-of-scope candidate is terminal, not unavailable trusted work.
// Keep only bounded, content-free dedup tombstones outside active capacity.
func (s *Store) rejectValidation(id string) error {
	_, err := s.call(func(db *storeConn) (any, error) {
		return transact(db, func(db *storeConn) (any, error) {
			var raw, state string
			if err := db.QueryRow(`SELECT envelope,state FROM ingress_validation WHERE id=?`, id).Scan(&raw, &state); err != nil {
				return nil, err
			}
			if state != "pending" {
				return nil, nil
			}
			var source Envelope
			if json.Unmarshal([]byte(raw), &source) != nil {
				return nil, errors.New("invalid quarantined envelope")
			}
			if _, err := db.Exec(`DELETE FROM message_sources WHERE platform=? AND event_id=? AND revision=? AND state!='deleted'`, source.Platform, source.EventID, source.SourceRevision); err != nil {
				return nil, err
			}
			if err := changedOne(db.Exec(`UPDATE ingress_validation SET state='rejected',envelope='{}',code='out_of_scope' WHERE id=? AND state='pending'`, id)); err != nil {
				return nil, err
			}
			_, err := db.Exec(`DELETE FROM ingress_validation WHERE state='rejected' AND id NOT IN (SELECT id FROM ingress_validation WHERE state='rejected' ORDER BY created DESC,rowid DESC LIMIT 1000)`)
			return nil, err
		})
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
	return s.promoteValidation(in, nil)
}

// Thread proof is local to the validator. Only routing metadata can change;
// original message identity, channel, sender, content and receipt time stay bound.
func (s *Store) promoteThreadValidation(in ValidationInput, verified Envelope) (string, error) {
	original := verified
	original.Context = in.Event.Context
	original.RouteKind, original.ParentChannelID, original.ThreadType, original.ThreadName = "guild_thread_candidate", "", 0, ""
	if in.Event.RouteKind != "guild_thread_candidate" || original != in.Event || verified.RouteKind != "guild_thread" || !s.policy.Accepts(verified) {
		return "", errors.New("validation_input_changed")
	}
	return s.promoteValidation(in, &verified)
}

func (s *Store) promoteContextValidation(in ValidationInput, verified Envelope) (string, error) {
	original := verified
	original.Context = in.Event.Context
	if original != in.Event {
		return "", errors.New("validation_input_changed")
	}
	return s.promoteValidation(in, &verified)
}

func (s *Store) promoteValidation(in ValidationInput, verified *Envelope) (string, error) {
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
			current, err := sourceCurrentDB(db, event)
			if err != nil {
				return nil, err
			}
			if !current || state == "superseded" {
				return "superseded", nil
			}
			if state != "pending" || event != in.Event || created != in.Created {
				return nil, errors.New("validation_input_changed")
			}
			if verified != nil {
				event = *verified
				encoded, err := json.Marshal(event)
				if err != nil {
					return nil, err
				}
				raw = string(encoded)
			}
			// Retain the verified route/context in the current head for exact
			// refreshes; the inbound revision snapshot is still never mutated.
			if _, err = db.Exec(`UPDATE message_sources SET envelope=? WHERE platform=? AND event_id=? AND revision=? AND state='current'`, raw, event.Platform, event.EventID, event.SourceRevision); err != nil {
				return nil, err
			}
			if !s.policy.Accepts(event) {
				_, err := db.Exec(`UPDATE ingress_validation SET state='blocked',code='authorization_revoked' WHERE id=?`, in.ID)
				return "validation_blocked", err
			}
			var n int
			if err := db.QueryRow(`SELECT count(*) FROM inbound WHERE platform=? AND event_id=?`, event.Platform, sourceLedgerKey(event)).Scan(&n); err != nil {
				return nil, err
			}
			outcome := "duplicate"
			if n == 0 {
				// Staging reserved capacity; promotion does not admit another item.
				if _, err := db.Exec(`INSERT INTO inbound(id,platform,event_id,envelope,created) VALUES(?,?,?,?,?)`, in.ID, event.Platform, sourceLedgerKey(event), raw, created); err != nil {
					return nil, err
				}
				memoryBestEffort(db, func() error { return rememberInboundDB(db, in.ID, event) })
				if err := insertTiming(db, in.ID, "ingested", epoch(), epoch()-created); err != nil {
					return nil, err
				}
				outcome = "accepted"
			}
			if err = rememberReadRouteDB(db, event); err != nil {
				return nil, err
			}
			if err = catchupRegisterDB(db, event, snowPrevious(snowAt(time.Now().Add(time.Millisecond)))); err != nil {
				return nil, err
			}
			_, err = db.Exec(`DELETE FROM ingress_validation WHERE id=?`, in.ID)
			return outcome, err
		})
	})
	if err != nil {
		return "", err
	}
	return v.(string), nil
}

func (s *Store) stageIngressDB(db *storeConn, e Envelope, created float64, capacity int) (string, error) {
	known, err := sourceKnownDB(db, e)
	if err != nil {
		return "", err
	}
	if known {
		return "duplicate", nil
	}
	var n int
	if err := db.QueryRow(`SELECT (SELECT count(*) FROM inbound WHERE platform=? AND event_id=?) + (SELECT count(*) FROM ingress_validation WHERE platform=? AND event_id=?)`, e.Platform, sourceLedgerKey(e), e.Platform, sourceLedgerKey(e)).Scan(&n); err != nil {
		return "", err
	}
	if n != 0 {
		return "duplicate", nil
	}
	n, err = activeInboundCount(db)
	if err != nil {
		return "", err
	}
	if n >= capacity {
		return "queue_full", nil
	}
	if err = registerSourceDB(db, e); err != nil {
		return "", err
	}
	id, err := uuidHex()
	if err != nil {
		return "", err
	}
	raw, err := json.Marshal(e)
	if err != nil {
		return "", err
	}
	_, err = db.Exec(`INSERT INTO ingress_validation(id,platform,event_id,envelope,created) VALUES(?,?,?,?,?)`, id, e.Platform, sourceLedgerKey(e), string(raw), created)
	return "validation_staged", err
}
