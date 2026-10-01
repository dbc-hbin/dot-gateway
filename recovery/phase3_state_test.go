package main

import (
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func phase3Fixture() Snapshot {
	s := stateFixture()
	s.Events[0].EventID = "control:" + strings.Repeat("a", 32)
	s.Phase3 = &Phase3State{Version: 1, ApplicationID: s.Operation.BotID, GuildID: s.Operation.GuildID, OwnerID: s.Operation.OwnerID,
		Commands:             []OwnedCommand{{"ask", "4001"}, {"status", "4002"}, {"cancel", "4003"}},
		RegistrationAttempts: []CommandAttempt{{strings.Repeat("a", 64) + ":ask:create", "ask", "create", "acknowledged", "4001", 1}, {strings.Repeat("b", 64) + ":status:update", "status", "update", "attempted", "", 2}},
		Interactions:         []InteractionFence{{"5001", "1001", "1004", "ask", "queued", 1, "in1"}},
		Reactions:            []ReactionFence{{"1004", "8001", "1001", "👍", true, 1}, {"1004", "8001", "1001", "id:7777", false, 2}},
		TargetInvalidations:  []TargetFence{{"1004", "8001", "target_updated"}},
		ReplyCancellations:   []CancellationFence{{"reply1", 1, "operator_cancelled"}},
		CancelledRequests:    []string{"in1"},
		RichReceipts:         []RichReceiptMetadata{{"reply1", 0, []AttachmentMetadata{{"8002", 4096}}}}}
	return s
}
func TestPhase3MetadataRestoreNoReplayAndSecondGeneration(t *testing.T) {
	s := phase3Fixture()
	root := filepath.Join(privateTemp(t), "restored")
	if err := restoreState(s, root, -1); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", filepath.Join(root, "bridge", "bridge.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var command, state string
	if err = db.QueryRow(`SELECT id FROM control_command_ids WHERE guild='1003' AND name='ask'`).Scan(&command); err != nil || command != "4001" {
		t.Fatal(command, err)
	}
	if err = db.QueryRow(`SELECT state FROM control_registration_attempts WHERE key=?`, strings.Repeat("b", 64)+":status:update").Scan(&state); err != nil || state != "uncertain" {
		t.Fatal(state, err)
	}
	if _, err = db.Exec(`INSERT INTO control_registration_attempts VALUES(?,'status','update','attempted','',3)`, strings.Repeat("b", 64)+":status:update"); err == nil {
		t.Fatal("registration attempt replayed")
	}
	if err = db.QueryRow(`SELECT state FROM control_interactions WHERE id='5001'`).Scan(&state); err != nil || state != "token_unavailable" {
		t.Fatal(state, err)
	}
	var n int
	if err = db.QueryRow(`SELECT count(*) FROM inbound WHERE state!='blocked' OR envelope!='{}' OR claim IS NOT NULL`).Scan(&n); err != nil || n != 0 {
		t.Fatal("runnable historical inbound", n, err)
	}
	if err = db.QueryRow(`SELECT count(*) FROM chunks WHERE state NOT IN('sent','uncertain')`).Scan(&n); err != nil || n != 0 {
		t.Fatal("runnable historical output", n, err)
	}
	if err = db.QueryRow(`SELECT count(*) FROM sqlite_master WHERE name IN('control_bindings','control_pending_responses','reply_outputs','reply_output_receipts')`).Scan(&n); err != nil || n != 0 {
		t.Fatal("restored active controls/output", n, err)
	}
	if _, err = db.Exec(`CREATE TABLE ingress_validation(id TEXT,platform TEXT,event_id TEXT,state TEXT,created REAL)`); err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	second := Snapshot{Schema: 1, Created: s.Created, ManifestSHA: s.ManifestSHA, Operation: s.Operation}
	if err = snapshotDB(filepath.Join(root, "bridge", "bridge.sqlite3"), &second); err != nil {
		t.Fatal(err)
	}
	if second.Phase3 == nil || len(second.Phase3.Commands) != 3 || len(second.Phase3.Reactions) != 2 || len(second.Phase3.RichReceipts) != 1 || len(second.Phase3.CancelledRequests) != 1 {
		t.Fatalf("metadata lost after rebackup: %#v", second.Phase3)
	}
	raw, _ := json.Marshal(second.Phase3)
	for _, forbidden := range []string{"filename", "description", "https://", "token_value", "payload"} {
		if strings.Contains(string(raw), forbidden) {
			t.Fatal("unsafe metadata", forbidden)
		}
	}
}
func TestPhase3StrictValidationAndBackwardCompatibility(t *testing.T) {
	for _, change := range []func(*Snapshot){
		func(s *Snapshot) { s.Phase3.ApplicationID = "777" }, func(s *Snapshot) { s.Phase3.GuildID = "777" }, func(s *Snapshot) { s.Phase3.OwnerID = "777" },
		func(s *Snapshot) { s.Phase3.Commands[0].Name = "other" }, func(s *Snapshot) { s.Phase3.Commands[1].ID = s.Phase3.Commands[0].ID },
		func(s *Snapshot) { s.Phase3.RegistrationAttempts[0].Key = "TOKEN" }, func(s *Snapshot) { s.Phase3.RegistrationAttempts[0].Action = "delete" }, func(s *Snapshot) { s.Phase3.RegistrationAttempts[0].State = "queued" },
		func(s *Snapshot) { s.Phase3.Interactions[0].Owner = "999" }, func(s *Snapshot) { s.Phase3.Interactions[0].InboundID = "missing" },
		func(s *Snapshot) { s.Phase3.Reactions[0].Emoji = "password=hunter2" }, func(s *Snapshot) { s.Phase3.Reactions[0].Sequence = 0 },
		func(s *Snapshot) { s.Phase3.RichReceipts[0].Index = 1 }, func(s *Snapshot) { s.Phase3.RichReceipts[0].Files[0].ID = "https://example.invalid" },
	} {
		s := phase3Fixture()
		change(&s)
		if err := validateSnapshot(s); err == nil {
			t.Fatal("accepted malformed phase3 metadata")
		}
	}
	old := stateFixture()
	if err := validateSnapshot(old); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(phase3Fixture())
	var value map[string]any
	json.Unmarshal(raw, &value)
	value["phase3"].(map[string]any)["token"] = "DO_NOT_EXPORT"
	raw, _ = json.Marshal(value)
	var decoded Snapshot
	if strict(raw, &decoded) == nil {
		t.Fatal("unknown phase3 field accepted")
	}
}
func TestPhase3ActualReactionKeyForms(t *testing.T) {
	for _, key := range []string{"control:9001:1001:👍:1", "control:9001:1001:id:7777:2", "control:9001:1001:1️⃣:3"} {
		s := stateFixture()
		s.Events = append(s.Events, Event{"reaction", "discord", key, "ignored", 1})
		if err := validateSnapshot(s); err != nil {
			t.Fatal(key, err)
		}
	}
	for _, key := range []string{"control:9001:1001:secret:1", "control:9001:1001:id:7777:0", "control:9001:1001:👍:01", "control:9001:9999:👍:1", "control:9001:1001:api_key=secret:1"} {
		s := stateFixture()
		s.Events = append(s.Events, Event{"reaction", "discord", key, "ignored", 1})
		if err := validateSnapshot(s); err == nil {
			t.Fatal("invalid key accepted", key)
		}
	}
}
func TestPhase3SnapshotRichMetadataExcludesBodiesAndURLs(t *testing.T) {
	s := stateFixture()
	root := filepath.Join(privateTemp(t), "restored")
	if err := restoreState(s, root, -1); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", filepath.Join(root, "bridge", "bridge.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`CREATE TABLE ingress_validation(id TEXT,platform TEXT,event_id TEXT,state TEXT,created REAL);CREATE TABLE reply_output_receipts(reply_id TEXT,idx INTEGER,receipt TEXT);INSERT INTO reply_output_receipts VALUES('reply1',0,'[{"id":"8002","size":4096,"filename":"PRIVATE_FILENAME","description":"PASSWORD_SECRET","content_type":"text/plain","url":"https://example.invalid/signed?secret=SECRET"}]')`)
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
	snapshot := Snapshot{Schema: 1, Created: s.Created, ManifestSHA: s.ManifestSHA, Operation: s.Operation}
	if err = snapshotDB(filepath.Join(root, "bridge", "bridge.sqlite3"), &snapshot); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(snapshot)
	for _, secret := range []string{"PRIVATE_FILENAME", "PASSWORD_SECRET", "SECRET", "example.invalid"} {
		if strings.Contains(string(raw), secret) {
			t.Fatal("exported private receipt body", secret)
		}
	}
	if snapshot.Phase3 == nil || snapshot.Phase3.RichReceipts[0].Files[0].ID != "8002" {
		t.Fatal(snapshot.Phase3)
	}
	path := filepath.Join(privateTemp(t), "snapshot.json")
	if err = os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
}

func TestSafeReactionPunctuationAndSubdivisionFlags(t *testing.T) {
	for _, emoji := range []string{"‼️", "⁉️", "〰️", "〽️", "\U0001f3f4\U000e0067\U000e0062\U000e0065\U000e006e\U000e0067\U000e007f"} {
		if !safeReactionEmoji(emoji) {
			t.Fatal("valid emoji rejected", emoji)
		}
	}
	for _, emoji := range []string{"\U000e0061", "\U0001f3f4\U000e0061", "\U0001f3f4\U000e0061\U000e007f", "‼️secret", "👍\u200b"} {
		if safeReactionEmoji(emoji) {
			t.Fatal("unsafe emoji accepted", emoji)
		}
	}
}
