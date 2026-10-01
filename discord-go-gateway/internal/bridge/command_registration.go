package bridge

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"
)

func commandSnapshotDigest(snapshot CommandSnapshot) string {
	raw, _ := json.Marshal(snapshot)
	h := sha256.Sum256(raw)
	return hex.EncodeToString(h[:])
}
func (r *RESTClient) commandIdentity(ctx context.Context) error {
	if !Snowflake(r.settings.ExpectedBotID) || !Snowflake(r.settings.Policy.GuildID) {
		return errors.New("command_scope_mismatch")
	}
	if _, err := r.Identity(ctx); err != nil {
		return err
	}
	var application struct {
		ID  string `json:"id"`
		Bot User   `json:"bot"`
	}
	if err := r.get(ctx, "/oauth2/applications/@me", &application); err != nil {
		return err
	}
	if application.ID != r.settings.ExpectedBotID || application.Bot.ID != r.settings.ExpectedBotID || !application.Bot.Bot {
		return errors.New("command_application_mismatch")
	}
	ch, err := r.Channel(ctx, r.settings.Policy.GuildChannelID)
	if err != nil {
		return err
	}
	return r.ValidateChannel(ch, Envelope{RouteKind: "guild_text", ConversationID: r.settings.Policy.GuildChannelID, GuildID: r.settings.Policy.GuildID})
}
func (r *RESTClient) CommandSnapshot(ctx context.Context, store *Store) (CommandSnapshot, error) {
	snapshot := CommandSnapshot{ApplicationID: r.settings.ExpectedBotID, GuildID: r.settings.Policy.GuildID, OwnedIDs: map[string]string{}}
	if err := r.commandIdentity(ctx); err != nil {
		return snapshot, err
	}
	path := "/applications/" + snapshot.ApplicationID + "/guilds/" + snapshot.GuildID + "/commands"
	if err := r.get(ctx, path, &snapshot.Commands); err != nil {
		return snapshot, err
	}
	v, err := store.call(func(db *storeConn) (any, error) {
		rows, err := db.Query("SELECT name,id FROM control_command_ids WHERE guild=?", snapshot.GuildID)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		owned := map[string]string{}
		for rows.Next() {
			var name, id string
			if err = rows.Scan(&name, &id); err != nil {
				return nil, err
			}
			owned[name] = id
		}
		return owned, rows.Err()
	})
	if err != nil {
		return snapshot, err
	}
	snapshot.OwnedIDs = v.(map[string]string)
	return snapshot, nil
}
func (s *Store) beginCommandWrite(key, name, action string) error {
	_, err := s.call(func(db *storeConn) (any, error) {
		_, err := db.Exec("INSERT INTO control_registration_attempts VALUES(?,?,?,'attempted','',?)", key, name, action, epoch())
		if err != nil {
			return nil, errors.New("command_attempt_exists_review_remote_before_reissue")
		}
		return nil, nil
	})
	return err
}
func (s *Store) recordCommandWrite(key, guild, name, id, state string) error {
	_, err := s.call(func(db *storeConn) (any, error) {
		return transact(db, func(db *storeConn) (any, error) {
			if _, err := db.Exec("UPDATE control_registration_attempts SET state=?,command_id=? WHERE key=?", state, id, key); err != nil {
				return nil, err
			}
			if state == "acknowledged" {
				_, err := db.Exec("INSERT INTO control_command_ids VALUES(?,?,?) ON CONFLICT(guild,name) DO UPDATE SET id=excluded.id", guild, name, id)
				return nil, err
			}
			return nil, nil
		})
	})
	return err
}

type RegistrationResult struct {
	State string `json:"state"`
	Name  string `json:"name,omitempty"`
	ID    string `json:"id,omitempty"`
	Code  string `json:"code,omitempty"`
}

// ApplyGuildCommandPlan executes only a previously reviewed exact diff after a
// fresh identity/snapshot comparison. Every write has a durable attempt tombstone
// before the network call. Interrupted/uncertain writes cannot be blindly replayed.
func (r *RESTClient) ApplyGuildCommandPlan(ctx context.Context, store *Store, plan CommandPlan) ([]RegistrationResult, error) {
	snapshot, err := r.CommandSnapshot(ctx, store)
	if err != nil {
		return nil, err
	}
	fresh, err := PlanGuildCommands(r.settings, snapshot)
	if err != nil {
		return nil, err
	}
	fresh.NetworkUsed = plan.NetworkUsed
	if !reflect.DeepEqual(fresh, plan) {
		return nil, errors.New("reviewed_command_plan_changed")
	}
	results := []RegistrationResult{}
	for _, change := range plan.Changes {
		if change.Action == "preserve" || change.Action == "unchanged" {
			continue
		}
		if change.Desired == nil || (change.Action != "create" && change.Action != "update") {
			return results, errors.New("invalid_command_plan")
		}
		key := plan.BeforeDigest + ":" + change.Name + ":" + change.Action
		if err = store.beginCommandWrite(key, change.Name, change.Action); err != nil {
			return results, err
		}
		path := "/applications/" + plan.ApplicationID + "/guilds/" + plan.GuildID + "/commands"
		method := http.MethodPost
		if change.Action == "update" {
			if !Snowflake(change.ID) {
				return results, errors.New("invalid_command_id")
			}
			method = http.MethodPatch
			path += "/" + change.ID
		}
		raw, _ := json.Marshal(change.Desired)
		resp, err := r.request(ctx, method, path, raw)
		if err != nil {
			_ = store.recordCommandWrite(key, plan.GuildID, change.Name, "", "uncertain")
			results = append(results, RegistrationResult{State: "uncertain", Name: change.Name, Code: "request_or_ack_failed"})
			return results, errors.New("command_registration_uncertain")
		}
		if resp.StatusCode != 200 && resp.StatusCode != 201 {
			status := resp.StatusCode
			resp.Body.Close()
			state := "uncertain"
			switch status {
			case 400, 401, 403, 404, 405, 413, 429:
				state = "failed"
			}
			_ = store.recordCommandWrite(key, plan.GuildID, change.Name, "", state)
			results = append(results, RegistrationResult{State: state, Name: change.Name, Code: fmt.Sprintf("http_%d", status)})
			return results, errors.New("command_registration_stopped")
		}
		var ack GuildCommand
		if readJSON(resp, &ack) != nil || !Snowflake(ack.ID) || ack.ApplicationID != plan.ApplicationID || ack.GuildID != plan.GuildID || change.Action == "update" && ack.ID != change.ID {
			_ = store.recordCommandWrite(key, plan.GuildID, change.Name, "", "uncertain")
			return results, errors.New("command_registration_invalid_ack")
		}
		id := ack.ID
		ack.ID, ack.ApplicationID, ack.GuildID = "", "", ""
		if !reflect.DeepEqual(ack, *change.Desired) {
			_ = store.recordCommandWrite(key, plan.GuildID, change.Name, "", "uncertain")
			return results, errors.New("command_registration_invalid_ack")
		}
		if err = store.recordCommandWrite(key, plan.GuildID, change.Name, id, "acknowledged"); err != nil {
			return results, errors.New("command_registration_persistence_uncertain")
		}
		results = append(results, RegistrationResult{State: "acknowledged", Name: change.Name, ID: id})
	}
	return results, nil
}
