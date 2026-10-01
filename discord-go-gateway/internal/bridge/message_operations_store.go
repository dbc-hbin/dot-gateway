package bridge

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// The same transaction reserves an edit and retires every old question binding.
// Old delivery rows remain immutable forensic evidence, including sent chunks.
func invalidateOperationBindingsDB(db *storeConn, channel, message string) error {
	if _, err := db.Exec("INSERT OR IGNORE INTO control_target_invalidations VALUES(?,?,'target_updated')", channel, message); err != nil {
		return err
	}
	for _, q := range []string{
		`DELETE FROM consumer_claims WHERE inbound_id IN(SELECT id FROM inbound WHERE json_extract(envelope,'$.conversation_id')=? AND json_extract(envelope,'$.control.target_message_id')=?)`,
		`DELETE FROM processing WHERE inbound_id IN(SELECT id FROM inbound WHERE json_extract(envelope,'$.conversation_id')=? AND json_extract(envelope,'$.control.target_message_id')=?)`,
		`UPDATE inbound SET state='cancelled',claim=NULL,lease_until=NULL WHERE json_extract(envelope,'$.conversation_id')=? AND json_extract(envelope,'$.control.target_message_id')=?`,
		`UPDATE chunks SET state='cancelled',code='control_target_changed' WHERE state='pending' AND reply_id IN(SELECT r.id FROM replies r JOIN inbound i ON i.id=r.inbound_id WHERE json_extract(i.envelope,'$.conversation_id')=? AND json_extract(i.envelope,'$.control.target_message_id')=?)`,
		`UPDATE control_bindings SET used=1 WHERE channel=? AND message=?`,
		`UPDATE control_pending_responses SET used=1 WHERE request IN(SELECT r.inbound_id FROM chunks c JOIN replies r ON r.id=c.reply_id JOIN inbound i ON i.id=r.inbound_id WHERE json_extract(i.envelope,'$.conversation_id')=? AND c.message_id=?)`,
	} {
		if _, err := db.Exec(q, channel, message); err != nil {
			return err
		}
	}
	return nil
}
func (s *Store) finishMessageOperation(id, state, code string, verified bool, observedGeneration ...int64) (MessageOperation, error) {
	_, err := s.call(func(db *storeConn) (any, error) {
		return transact(db, func(db *storeConn) (any, error) {
			o, err := loadOperationDB(db, id)
			if err != nil {
				return nil, err
			}
			if verified {
				if len(observedGeneration) != 1 {
					return nil, errors.New("operation_verification_generation_required")
				}
				if err := requireOperationGenerationDB(db, o.Spec.ChannelID, o.Spec.MessageID, observedGeneration[0]); err != nil {
					return nil, err
				}
			}
			if state != "verified" && state != "uncertain" && state != "failed" {
				return nil, errors.New("invalid_operation_result")
			}
			if o.State == "failed" {
				return nil, errors.New("operation_terminal")
			}
			if o.State == "verified" && state != "verified" {
				return nil, errors.New("operation_terminal")
			}
			if o.Spec.Action == "edit_text" && o.Attempts == 1 {
				var projectionID string
				var currentRevision int
				if err = db.QueryRow(`SELECT operation_id,revision FROM message_edit_projection WHERE channel=? AND message=?`, o.Spec.ChannelID, o.Spec.MessageID).Scan(&projectionID, &currentRevision); err != nil {
					return nil, err
				}
				if projectionID != id {
					return nil, errors.New("operation_projection_superseded")
				}
				if verified {
					// An explicit reconciliation of the same attempt reuses its revision.
					revision := 0
					err = db.QueryRow(`SELECT revision FROM message_edit_revisions WHERE operation_id=?`, id).Scan(&revision)
					if err == sql.ErrNoRows {
						revision = currentRevision + 1
						_, err = db.Exec(`INSERT INTO message_edit_revisions(channel,message,revision,operation_id,snapshot,created) VALUES(?,?,?,?,?,?)`, o.Spec.ChannelID, o.Spec.MessageID, revision, id, operationJSON(o.after), epoch())
					}
					if err != nil {
						return nil, err
					}
					_, err = db.Exec(`UPDATE message_edit_projection SET revision=?,state='verified',memory_key=? WHERE channel=? AND message=?`, revision, "assistant-edit:"+id, o.Spec.ChannelID, o.Spec.MessageID)
					if err != nil {
						return nil, err
					}
					_, err = db.Exec(`UPDATE message_operations SET revision=? WHERE id=?`, revision, id)
					if err != nil {
						return nil, err
					}
					memoryBestEffort(db, func() error {
						c, _, err := operationOwnedChunkDB(db, o.Spec.ChannelID, o.Spec.MessageID)
						if err != nil {
							return err
						}
						return storeMemoryDocumentDB(db, "assistant-edit:"+id, "assistant", c.Source, o.after.Content, c.ReplyID, o.Spec.MessageID, epoch())
					})
				} else if state == "failed" && requireOperationGenerationDB(db, o.Spec.ChannelID, o.Spec.MessageID, o.generation) == nil {
					// Zero-attempt/definitive rejection leaves the prior projection current.
					// The retired approvals deliberately stay retired even on failed edits.
					_, err = db.Exec(`UPDATE message_edit_projection SET state='verified',operation_id=COALESCE((SELECT operation_id FROM message_edit_revisions WHERE channel=? AND message=? AND revision=?),'') WHERE channel=? AND message=?`, o.Spec.ChannelID, o.Spec.MessageID, currentRevision, o.Spec.ChannelID, o.Spec.MessageID)
					if err != nil {
						return nil, err
					}
				}
			}
			_, err = db.Exec(`UPDATE message_operations SET state=?,code=?,updated=? WHERE id=?`, state, code, epoch(), id)
			return nil, err
		})
	})
	if err != nil {
		return MessageOperation{}, err
	}
	return s.MessageOperation(id)
}

// Include this live projection predicate on EVERY memory read, including facts'
// source joins, so a best-effort memory side effect can never reveal old text.
// Original chunk text is never replaced or reclassified as a verified edit.
func operationMemoryCurrentSQL(alias string) string {
	return fmt.Sprintf(`NOT EXISTS(SELECT 1 FROM message_edit_recovery_projections rp WHERE %[1]s.role='assistant' AND rp.channel=json_extract(%[1]s.origin,'$.conversation_id') AND rp.message=%[1]s.remote_id) AND NOT EXISTS(SELECT 1 FROM message_edit_projection op WHERE %[1]s.role='assistant' AND op.channel=json_extract(%[1]s.origin,'$.conversation_id') AND op.message=%[1]s.remote_id AND (op.state!='verified' OR op.memory_key!=%[1]s.record_key))`, alias)
}

// Gateway updates do not contain a complete authoritative target snapshot. They
// invalidate the projection; only an explicit exact readback may restore it.
func invalidateEditedProjectionDB(db *storeConn, channel, guild, message string) error {
	if _, err := db.Exec(`UPDATE message_operation_targets SET generation=generation+1 WHERE channel=? AND message=? AND (?='' OR guild=?)`, channel, message, guild, guild); err != nil {
		return err
	}
	_, err := db.Exec(`UPDATE message_edit_projection SET state='unknown' WHERE channel=? AND message=?`, channel, message)
	return err
}

// Baseline revision zero is immutable and comes from a ledger-owned readback.
func baselineEditProjectionDB(db *storeConn, o MessageOperation) error {
	c, _, err := operationOwnedChunkDB(db, o.Spec.ChannelID, o.Spec.MessageID)
	if err != nil {
		return err
	}
	baselineID := "original:" + o.Spec.ChannelID + ":" + o.Spec.MessageID
	_, err = db.Exec(`INSERT OR IGNORE INTO message_edit_revisions(channel,message,revision,operation_id,snapshot,created) VALUES(?,?,0,?,?,?)`, o.Spec.ChannelID, o.Spec.MessageID, baselineID, operationJSON(o.before), epoch())
	if err != nil {
		return err
	}
	_, err = db.Exec(`INSERT INTO message_edit_projection(channel,message,revision,state,operation_id,memory_key) VALUES(?,?,0,'unknown',?,?) ON CONFLICT(channel,message) DO UPDATE SET state='unknown',operation_id=excluded.operation_id`, o.Spec.ChannelID, o.Spec.MessageID, o.ID, fmt.Sprintf("assistant:%s:%d", c.ReplyID, c.Index))
	return err
}

// CurrentEditedMessage is local evidence only, not fresh remote authorization.
// Unknown projections must not be used for reactions, quotes, or remembered facts.
func (s *Store) CurrentEditedMessage(channel, message string) (string, int, error) {
	type result struct {
		Text     string
		Revision int
	}
	v, err := s.call(func(db *storeConn) (any, error) {
		var raw, state string
		var out result
		err := db.QueryRow(`SELECT p.state,p.revision,r.snapshot FROM message_edit_projection p JOIN message_edit_revisions r ON r.channel=p.channel AND r.message=p.message AND r.revision=p.revision WHERE p.channel=? AND p.message=?`, channel, message).Scan(&state, &out.Revision, &raw)
		if err != nil {
			return nil, err
		}
		var snap operationSnapshot
		if state != "verified" || json.Unmarshal([]byte(raw), &snap) != nil {
			return nil, errors.New("operation_edit_projection_unknown")
		}
		out.Text = snap.Content
		return out, nil
	})
	if err != nil {
		return "", 0, err
	}
	o := v.(result)
	return o.Text, o.Revision, nil
}

// A concurrent caller may have consumed the attempt already. Never overwrite
// its uncertainty merely because this caller lost a claim or reservation race.
func (s *Store) failPreparedMessageOperation(id, code string) (MessageOperation, error) {
	_, err := s.call(func(db *storeConn) (any, error) {
		_, err := db.Exec(`UPDATE message_operations SET state='failed',code=?,updated=? WHERE id=? AND state='prepared' AND attempts=0`, code, epoch(), id)
		return nil, err
	})
	if err != nil {
		return MessageOperation{}, err
	}
	return s.MessageOperation(id)
}

func (s *Store) operationTargetGeneration(channel, message string) (int64, error) {
	v, err := s.call(func(db *storeConn) (any, error) {
		var generation int64
		err := db.QueryRow(`SELECT generation FROM message_operation_targets WHERE channel=? AND message=?`, channel, message).Scan(&generation)
		return generation, err
	})
	if err != nil {
		return 0, err
	}
	return v.(int64), nil
}
func requireOperationGenerationDB(db *storeConn, channel, message string, want int64) error {
	var actual int64
	if err := db.QueryRow(`SELECT generation FROM message_operation_targets WHERE channel=? AND message=?`, channel, message).Scan(&actual); err != nil {
		return err
	}
	if actual != want {
		return errors.New("operation_target_invalidated")
	}
	return nil
}
func (s *Store) operationGenerationCurrent(o MessageOperation) bool {
	_, err := s.call(func(db *storeConn) (any, error) {
		return nil, requireOperationGenerationDB(db, o.Spec.ChannelID, o.Spec.MessageID, o.generation)
	})
	return err == nil
}

// Abandon is an explicit cancellation of only provably unattempted local work.
// It cannot release an uncertain attempt, even after a process restart.
func (s *Store) AbandonMessageOperation(request, claim, id string) (MessageOperation, error) {
	_, err := s.call(func(db *storeConn) (any, error) {
		return transact(db, func(db *storeConn) (any, error) {
			if _, err := operationClaimDB(db, s.policy, request, claim); err != nil {
				return nil, err
			}
			o, err := loadOperationDB(db, id)
			if err != nil {
				return nil, err
			}
			if o.State != "prepared" || o.Attempts != 0 {
				return nil, errors.New("operation_may_have_been_attempted")
			}
			_, err = db.Exec(`UPDATE message_operations SET state='failed',code='explicitly_abandoned_unattempted',updated=? WHERE id=? AND state='prepared' AND attempts=0`, epoch(), id)
			return nil, err
		})
	})
	if err != nil {
		return MessageOperation{}, err
	}
	return s.MessageOperation(id)
}

func (s *Store) bindOperationTarget(route ReadRoute, message string) (int64, error) {
	v, err := s.call(func(db *storeConn) (any, error) {
		if _, err := db.Exec(`INSERT OR IGNORE INTO message_operation_targets(channel,message,guild) VALUES(?,?,?)`, route.ChannelID, message, route.GuildID); err != nil {
			return nil, err
		}
		var generation int64
		var guild string
		err := db.QueryRow(`SELECT generation,guild FROM message_operation_targets WHERE channel=? AND message=?`, route.ChannelID, message).Scan(&generation, &guild)
		if err != nil {
			return nil, err
		}
		if guild != route.GuildID {
			return nil, errors.New("operation_route_changed")
		}
		return generation, nil
	})
	if err != nil {
		return 0, err
	}
	return v.(int64), nil
}

func operationRecoveryAllowedDB(db *storeConn, id, channel, message string) error {
	var blocked int
	err := db.QueryRow(`SELECT (SELECT count(*) FROM message_operation_recovery_fences WHERE id=? OR (channel=? AND message=? AND hold_target=1))+(SELECT count(*) FROM message_edit_recovery_projections WHERE channel=? AND message=?)`, id, channel, message, channel, message).Scan(&blocked)
	if err != nil {
		return err
	}
	if blocked != 0 {
		return errors.New("operation_recovery_fence")
	}
	return nil
}
func (s *Store) operationRecoveryAllowed(id, channel, message string) error {
	_, err := s.call(func(db *storeConn) (any, error) { return nil, operationRecoveryAllowedDB(db, id, channel, message) })
	return err
}
func verifiedOperationControlSnapshotDB(db *storeConn, channel, message string) (operationSnapshot, string, error) {
	var snap operationSnapshot
	var state, raw, id string
	err := db.QueryRow(`SELECT p.state,r.snapshot,p.operation_id FROM message_edit_projection p JOIN message_edit_revisions r ON r.channel=p.channel AND r.message=p.message AND r.revision=p.revision WHERE p.channel=? AND p.message=?`, channel, message).Scan(&state, &raw, &id)
	if err != nil {
		return snap, "", err
	}
	if state != "verified" || json.Unmarshal([]byte(raw), &snap) != nil {
		return snap, "", errors.New("operation_edit_projection_unknown")
	}
	return snap, id, nil
}

func operationIDFromMemoryKey(key string) (string, bool) {
	const prefix = "assistant-edit:"
	if len(key) != len(prefix)+64 || !strings.HasPrefix(key, prefix) {
		return "", false
	}
	id := strings.TrimPrefix(key, prefix)
	for _, c := range id {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return "", false
		}
	}
	return id, true
}
