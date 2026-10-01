package bridge

import (
	"encoding/json"
	"errors"
	"reflect"
	"sort"
)

// The manifest is intentionally guild-only. Runtime owner and channel checks
// remain necessary even when Discord displays a command to another guild member.
type GuildCommand struct {
	ID                       string          `json:"id,omitempty"`
	ApplicationID            string          `json:"application_id,omitempty"`
	GuildID                  string          `json:"guild_id,omitempty"`
	Name                     string          `json:"name"`
	Type                     int             `json:"type"`
	Description              string          `json:"description"`
	Options                  []CommandOption `json:"options"`
	DefaultMemberPermissions string          `json:"default_member_permissions"`
}
type CommandOption struct {
	Type        int    `json:"type"`
	Name        string `json:"name"`
	Description string `json:"description"`
	Required    bool   `json:"required"`
	MaxLength   int    `json:"max_length,omitempty"`
}
type CommandSnapshot struct {
	ApplicationID string            `json:"application_id"`
	GuildID       string            `json:"guild_id"`
	Commands      []GuildCommand    `json:"commands"`
	OwnedIDs      map[string]string `json:"owned_ids"`
}
type CommandChange struct {
	Action  string        `json:"action"`
	Name    string        `json:"name"`
	ID      string        `json:"id,omitempty"`
	Desired *GuildCommand `json:"desired,omitempty"`
}
type CommandPlan struct {
	BeforeDigest  string          `json:"before_digest"`
	Version       int             `json:"version"`
	ApplicationID string          `json:"application_id"`
	GuildID       string          `json:"guild_id"`
	DryRun        bool            `json:"dry_run"`
	NetworkUsed   bool            `json:"network_used"`
	Changes       []CommandChange `json:"changes"`
}

func OwnerCommandManifest() []GuildCommand {
	return []GuildCommand{
		{Name: "ask", Type: 1, Description: "Queue an owner request for dot", Options: []CommandOption{{Type: 3, Name: "prompt", Description: "Request, or omit to open the Ask modal", MaxLength: 4000}}, DefaultMemberPermissions: "0"},
		{Name: "status", Type: 1, Description: "Show delivery state for owner requests in this conversation", Options: []CommandOption{{Type: 3, Name: "request", Description: "Exact request ID, or omit for recent requests", MaxLength: 32}}, DefaultMemberPermissions: "0"},
		{Name: "cancel", Type: 1, Description: "Cancel an exact request's unsent work", Options: []CommandOption{{Type: 3, Name: "request", Description: "Exact request ID, or omit when only one is active", MaxLength: 32}}, DefaultMemberPermissions: "0"},
	}
}

// PlanGuildCommands performs no network or writes. Name collisions without an
// exact previously-owned command ID are blocked, not silently adopted. Unrelated
// commands are preserved; no bulk overwrite endpoint exists in this candidate.
func PlanGuildCommands(s Settings, snapshot CommandSnapshot) (CommandPlan, error) {
	plan := CommandPlan{Version: 1, ApplicationID: s.ExpectedBotID, GuildID: s.Policy.GuildID, DryRun: true, BeforeDigest: commandSnapshotDigest(snapshot), Changes: []CommandChange{}}
	if !Snowflake(s.ExpectedBotID) || !Snowflake(s.Policy.GuildID) || snapshot.ApplicationID != s.ExpectedBotID || snapshot.GuildID != s.Policy.GuildID {
		return plan, errors.New("command_scope_mismatch")
	}
	existing := map[string]GuildCommand{}
	seen := map[string]bool{}
	for _, c := range snapshot.Commands {
		if !Snowflake(c.ID) || c.ApplicationID != snapshot.ApplicationID || c.GuildID != snapshot.GuildID || seen[c.ID] {
			return plan, errors.New("invalid_command_snapshot")
		}
		seen[c.ID] = true
		if _, ok := existing[c.Name]; ok {
			return plan, errors.New("ambiguous_command_name")
		}
		existing[c.Name] = c
	}
	for _, desired := range OwnerCommandManifest() {
		current, ok := existing[desired.Name]
		if !ok {
			if snapshot.OwnedIDs[desired.Name] != "" {
				return plan, errors.New("owned_command_missing")
			}
			copy := desired
			plan.Changes = append(plan.Changes, CommandChange{Action: "create", Name: desired.Name, Desired: &copy})
			continue
		}
		delete(existing, desired.Name)
		if snapshot.OwnedIDs[desired.Name] != current.ID {
			return plan, errors.New("command_name_collision")
		}
		id := current.ID
		current.ID, current.ApplicationID, current.GuildID = "", "", ""
		action := "unchanged"
		var want *GuildCommand
		if !reflect.DeepEqual(current, desired) {
			action = "update"
			copy := desired
			want = &copy
		}
		plan.Changes = append(plan.Changes, CommandChange{Action: action, Name: desired.Name, ID: id, Desired: want})
	}
	names := []string{}
	for name := range existing {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		plan.Changes = append(plan.Changes, CommandChange{Action: "preserve", Name: name, ID: existing[name].ID})
	}
	return plan, nil
}
func ParseCommandSnapshot(raw []byte) (CommandSnapshot, error) {
	var snapshot CommandSnapshot
	if len(raw) > 1048576 {
		return snapshot, errors.New("command_snapshot_too_large")
	}
	err := json.Unmarshal(raw, &snapshot)
	return snapshot, err
}
