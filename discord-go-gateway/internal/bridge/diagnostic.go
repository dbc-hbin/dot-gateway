package bridge

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
)

// Diagnostic is an operator-approved transport probe, never an inbound event.
// Exactly three fixed intent slots exist in this ledger for this migration.
type Diagnostic struct {
	ID, Nonce, Text, BotID, OwnerID, GuildID, ChannelID string
	Created                                             float64
	Index                                               int
}

func diagnosticSchema(db *storeConn) error {
	_, e := db.Exec(`CREATE TABLE IF NOT EXISTS go_transport_diagnostics (
 id TEXT PRIMARY KEY, idx INTEGER NOT NULL UNIQUE CHECK(idx BETWEEN 0 AND 2),
 nonce TEXT NOT NULL, content TEXT NOT NULL, bot_id TEXT NOT NULL, owner_id TEXT NOT NULL,
 guild_id TEXT NOT NULL, channel_id TEXT NOT NULL, state TEXT NOT NULL,
 attempts INTEGER NOT NULL DEFAULT 0 CHECK(attempts BETWEEN 0 AND 1),
 message_id TEXT, code TEXT, created REAL NOT NULL, send_started REAL,
 acknowledged REAL, remote_created REAL, timings TEXT
)`)
	return e
}
func (s *Store) QueueDiagnostic(settings Settings, index int) (map[string]any, error) {
	if settings.Policy.Validate() != nil || settings.ExpectedBotID == settings.Policy.OwnerID {
		return nil, errors.New("invalid_diagnostic_target")
	}
	if index < 0 || index > 2 || settings.Policy.GuildID == "" || settings.Policy.GuildChannelID == "" || !Snowflake(settings.ExpectedBotID) {
		return nil, errors.New("invalid_diagnostic_target")
	}
	d := Diagnostic{ID: fmt.Sprintf("go-cutover-transport-%d", index+1), Index: index, BotID: settings.ExpectedBotID, OwnerID: settings.Policy.OwnerID, GuildID: settings.Policy.GuildID, ChannelID: settings.Policy.GuildChannelID, Text: fmt.Sprintf("Go transport test %d/3. This measures gateway delivery only, not model response time.", index+1), Created: wall()}
	d.Nonce = Nonce(Chunk{ReplyID: d.ID, Index: d.Index})
	_, e := s.call(func(db *storeConn) (any, error) {
		return transact(db, func(db *storeConn) (any, error) {
			if e := diagnosticSchema(db); e != nil {
				return nil, e
			}
			var bot, owner, guild, channel, text, nonce string
			e := db.QueryRow("SELECT bot_id,owner_id,guild_id,channel_id,content,nonce FROM go_transport_diagnostics WHERE idx=?", index).Scan(&bot, &owner, &guild, &channel, &text, &nonce)
			if e == nil {
				if bot != d.BotID || owner != d.OwnerID || guild != d.GuildID || channel != d.ChannelID || text != d.Text || nonce != d.Nonce {
					return nil, errors.New("diagnostic_intent_conflict")
				}
				return nil, nil
			}
			if e != sql.ErrNoRows {
				return nil, e
			}
			_, e = db.Exec("INSERT INTO go_transport_diagnostics(id,idx,nonce,content,bot_id,owner_id,guild_id,channel_id,state,created) VALUES(?,?,?,?,?,?,?,?,'pending',?)", d.ID, d.Index, d.Nonce, d.Text, d.BotID, d.OwnerID, d.GuildID, d.ChannelID, d.Created)
			return nil, e
		})
	})
	if e != nil {
		return nil, e
	}
	return s.DiagnosticStatus(index)
}
func (s *Store) NextDiagnostic() (*Diagnostic, error) {
	v, e := s.call(func(db *storeConn) (any, error) {
		return transact(db, func(db *storeConn) (any, error) {
			if e := diagnosticSchema(db); e != nil {
				return nil, e
			}
			var d Diagnostic
			e := db.QueryRow("SELECT id,idx,nonce,content,bot_id,owner_id,guild_id,channel_id,created FROM go_transport_diagnostics WHERE state='pending' AND attempts=0 ORDER BY idx LIMIT 1").Scan(&d.ID, &d.Index, &d.Nonce, &d.Text, &d.BotID, &d.OwnerID, &d.GuildID, &d.ChannelID, &d.Created)
			if e == sql.ErrNoRows {
				return (*Diagnostic)(nil), nil
			}
			if e != nil {
				return nil, e
			}
			_, e = db.Exec("UPDATE go_transport_diagnostics SET state='sending',attempts=1,send_started=? WHERE id=? AND attempts=0", wall(), d.ID)
			return &d, e
		})
	})
	if e != nil {
		return nil, e
	}
	return v.(*Diagnostic), nil
}
func (s *Store) RecoverDiagnostics() error {
	_, e := s.call(func(db *storeConn) (any, error) {
		return transact(db, func(db *storeConn) (any, error) {
			if e := diagnosticSchema(db); e != nil {
				return nil, e
			}
			_, e := db.Exec("UPDATE go_transport_diagnostics SET state='uncertain',code='interrupted_before_ack' WHERE state='sending'")
			return nil, e
		})
	})
	return e
}
func (s *Store) RecordDiagnosticResult(d Diagnostic, result SendResult, timings Diagnostics) error {
	if result.State != "sent" && result.State != "failed" && result.State != "uncertain" {
		return errors.New("invalid_diagnostic_result")
	}
	if result.State == "sent" && !Snowflake(result.MessageID) {
		return errors.New("invalid_diagnostic_ack")
	}
	timings.Code = symbolicCode(timings.Code)
	data, _ := json.Marshal(timings)
	var remote, acknowledged any
	if result.State == "sent" {
		acknowledged = wall()
		id, _ := strconv.ParseUint(result.MessageID, 10, 64)
		remote = float64((id>>22)+1420070400000) / 1000
	}
	_, e := s.call(func(db *storeConn) (any, error) {
		r, e := db.Exec("UPDATE go_transport_diagnostics SET state=?,message_id=?,code=?,acknowledged=?,remote_created=?,timings=? WHERE id=? AND state='sending' AND attempts=1", result.State, nullable(result.MessageID), nullable(symbolicCode(result.Code)), acknowledged, remote, string(data), d.ID)
		if e != nil {
			return nil, e
		}
		n, e := r.RowsAffected()
		if e == nil && n != 1 {
			e = errors.New("diagnostic_not_sending")
		}
		return nil, e
	})
	return e
}
func (s *Store) DiagnosticStatus(index int) (map[string]any, error) {
	if index < 0 || index > 2 {
		return nil, errors.New("invalid_diagnostic_index")
	}
	v, e := s.call(func(db *storeConn) (any, error) {
		var exists int
		if e := db.QueryRow("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='go_transport_diagnostics'").Scan(&exists); e != nil {
			return nil, e
		}
		out := map[string]any{"index": index + 1, "state": "not_started", "attempts": 0, "transport_test_only": true}
		if exists == 0 {
			return out, nil
		}
		var id, state string
		var attempts int
		var msg, code, timing sql.NullString
		var created float64
		var started, acked, remote sql.NullFloat64
		e := db.QueryRow("SELECT id,state,attempts,message_id,code,created,send_started,acknowledged,remote_created,timings FROM go_transport_diagnostics WHERE idx=?", index).Scan(&id, &state, &attempts, &msg, &code, &created, &started, &acked, &remote, &timing)
		if e == sql.ErrNoRows {
			return out, nil
		}
		if e != nil {
			return nil, e
		}
		out["intent_id"] = id
		out["state"] = state
		out["attempts"] = attempts
		out["created_at"] = created
		for k, v := range map[string]sql.NullFloat64{"send_started_at": started, "acknowledged_at": acked, "remote_created_at": remote} {
			if v.Valid {
				out[k] = v.Float64
			} else {
				out[k] = nil
			}
		}
		if started.Valid {
			out["queue_seconds"] = started.Float64 - created
		}
		if acked.Valid {
			out["intent_to_result_seconds"] = acked.Float64 - created
		}
		if msg.Valid {
			out["message_id"] = msg.String
		}
		if code.Valid {
			out["code"] = code.String
		}
		if timing.Valid {
			var t Diagnostics
			if json.Unmarshal([]byte(timing.String), &t) == nil {
				out["timings"] = t
			}
		}
		return out, nil
	})
	if e != nil {
		return nil, e
	}
	return v.(map[string]any), nil
}
