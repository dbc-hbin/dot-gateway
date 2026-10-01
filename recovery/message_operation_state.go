package main

import (
	"database/sql"
	"errors"
	"sort"
)

// These are no-replay fences, never runnable operations or reconstructed specs.
type MessageOperationFence struct {
	ID         string  `json:"id"`
	Channel    string  `json:"channel"`
	Message    string  `json:"message"`
	HoldTarget bool    `json:"hold_target"`
	Action     string  `json:"action"`
	State      string  `json:"state"`
	Attempts   int     `json:"attempts"`
	Created    float64 `json:"created"`
}
type MessageEditProjectionFence struct {
	Channel  string `json:"channel"`
	Message  string `json:"message"`
	Revision int64  `json:"revision"`
}
type MessageOperationState struct {
	Version       int                          `json:"version"`
	ApplicationID string                       `json:"application_id"`
	GuildID       string                       `json:"guild_id"`
	OwnerID       string                       `json:"owner_id"`
	Operations    []MessageOperationFence      `json:"operations,omitempty"`
	Projections   []MessageEditProjectionFence `json:"projections,omitempty"`
}

func snapshotMessageOperations(tx *sql.Tx, s *Snapshot) error {
	out := &MessageOperationState{Version: 1, ApplicationID: s.Operation.BotID, GuildID: s.Operation.GuildID, OwnerID: s.Operation.OwnerID}
	operations := map[string]MessageOperationFence{}
	accept := func(v MessageOperationFence) error {
		if old, ok := operations[v.ID]; ok && old != v {
			return errors.New("conflicting operation recovery fence")
		}
		operations[v.ID] = v
		return nil
	}
	err := phase3Read(tx, "message_operations", `SELECT id,channel,message,json_extract(spec,'$.action'),state,attempts,created FROM message_operations ORDER BY id`, phase3RowLimit, func(r *sql.Rows) error {
		var v MessageOperationFence
		if err := r.Scan(&v.ID, &v.Channel, &v.Message, &v.Action, &v.State, &v.Attempts, &v.Created); err != nil {
			return err
		}
		v.HoldTarget = v.State == "prepared" || v.State == "uncertain"
		return accept(v)
	})
	if err != nil {
		return err
	}
	err = phase3Read(tx, "message_operation_recovery_fences", `SELECT id,channel,message,hold_target,action,state,attempts,created FROM message_operation_recovery_fences ORDER BY id`, phase3RowLimit, func(r *sql.Rows) error {
		var v MessageOperationFence
		if err := r.Scan(&v.ID, &v.Channel, &v.Message, &v.HoldTarget, &v.Action, &v.State, &v.Attempts, &v.Created); err != nil {
			return err
		}
		return accept(v)
	})
	if err != nil {
		return err
	}
	projections := map[string]MessageEditProjectionFence{}
	readProjection := func(r *sql.Rows) error {
		var v MessageEditProjectionFence
		if err := r.Scan(&v.Channel, &v.Message, &v.Revision); err != nil {
			return err
		}
		key := v.Channel + ":" + v.Message
		if old, ok := projections[key]; !ok || v.Revision > old.Revision {
			projections[key] = v
		}
		return nil
	}
	if err = phase3Read(tx, "message_edit_projection", `SELECT channel,message,revision FROM message_edit_projection ORDER BY channel,message`, phase3RowLimit, readProjection); err != nil {
		return err
	}
	if err = phase3Read(tx, "message_edit_recovery_projections", `SELECT channel,message,revision FROM message_edit_recovery_projections ORDER BY channel,message`, phase3RowLimit, readProjection); err != nil {
		return err
	}
	for _, v := range operations {
		out.Operations = append(out.Operations, v)
	}
	sort.Slice(out.Operations, func(i, j int) bool { return out.Operations[i].ID < out.Operations[j].ID })
	for _, v := range projections {
		out.Projections = append(out.Projections, v)
	}
	sort.Slice(out.Projections, func(i, j int) bool {
		a, b := out.Projections[i], out.Projections[j]
		return a.Channel < b.Channel || a.Channel == b.Channel && a.Message < b.Message
	})
	if len(out.Operations)+len(out.Projections) > 0 {
		s.MessageOperations = out
	}
	return validateMessageOperations(*s)
}
func validateMessageOperations(s Snapshot) error {
	p := s.MessageOperations
	if p == nil {
		return nil
	}
	if p.Version != 1 || p.ApplicationID != s.Operation.BotID || p.GuildID != s.Operation.GuildID || p.OwnerID != s.Operation.OwnerID || !snowflakeID(p.ApplicationID) || !snowflakeID(p.GuildID) || !snowflakeID(p.OwnerID) {
		return errors.New("message operation recovery scope mismatch")
	}
	if len(p.Operations) > phase3RowLimit || len(p.Projections) > phase3RowLimit {
		return errors.New("message operation recovery limit exceeded")
	}
	actions := map[string]bool{"edit_text": true, "add_reaction": true, "remove_own_reaction": true, "pin": true, "unpin": true}
	states := map[string]bool{"prepared": true, "uncertain": true, "verified": true, "failed": true}
	seen := map[string]bool{}
	for _, v := range p.Operations {
		if !hashPattern.MatchString(v.ID) || !snowflakeID(v.Channel) || !snowflakeID(v.Message) || !actions[v.Action] || !states[v.State] || v.Attempts < 0 || v.Attempts > 1 || !finitePositive(v.Created) || seen[v.ID] || v.HoldTarget != (v.State == "prepared" || v.State == "uncertain") || v.State == "prepared" && v.Attempts != 0 || (v.State == "verified" || v.State == "uncertain") && v.Attempts != 1 {
			return errors.New("invalid message operation recovery fence")
		}
		seen[v.ID] = true
	}
	seen = map[string]bool{}
	for _, v := range p.Projections {
		key := v.Channel + ":" + v.Message
		if !snowflakeID(v.Channel) || !snowflakeID(v.Message) || v.Revision < 0 || seen[key] {
			return errors.New("invalid edit projection recovery fence")
		}
		seen[key] = true
	}
	return nil
}
func restoreMessageOperations(tx *sql.Tx, s Snapshot) error {
	p := s.MessageOperations
	if p == nil {
		return nil
	}
	if err := validateMessageOperations(s); err != nil {
		return err
	}
	_, err := tx.Exec(`CREATE TABLE message_operation_recovery_fences(id TEXT PRIMARY KEY,channel TEXT NOT NULL,message TEXT NOT NULL,hold_target INTEGER NOT NULL CHECK(hold_target IN(0,1)),action TEXT NOT NULL,state TEXT NOT NULL,attempts INTEGER NOT NULL,created REAL NOT NULL);
 CREATE TABLE message_edit_recovery_projections(channel TEXT NOT NULL,message TEXT NOT NULL,revision INTEGER NOT NULL,PRIMARY KEY(channel,message));`)
	if err != nil {
		return err
	}
	for _, v := range p.Operations {
		if _, err = tx.Exec(`INSERT INTO message_operation_recovery_fences VALUES(?,?,?,?,?,?,?,?)`, v.ID, v.Channel, v.Message, v.HoldTarget, v.Action, v.State, v.Attempts, v.Created); err != nil {
			return err
		}
	}
	for _, v := range p.Projections {
		if _, err = tx.Exec(`INSERT INTO message_edit_recovery_projections VALUES(?,?,?)`, v.Channel, v.Message, v.Revision); err != nil {
			return err
		}
	}
	return nil
}
