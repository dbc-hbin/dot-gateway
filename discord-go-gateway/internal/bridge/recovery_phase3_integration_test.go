package bridge

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
)

// Actual snapshot -> metadata-only restore -> gateway open -> registration plan.
// All HTTP is a local fake; there are no credentials or live Discord calls.
func TestRecoveryPhase3CommandOwnershipAndNoReplay(t *testing.T) {
	recovery := os.Getenv("DOT_MEMORY_RECOVERY_SOURCE")
	if recovery == "" {
		recovery = filepath.Join("..", "..", "..", "recovery")
	}
	recovery, _ = filepath.Abs(recovery)
	if _, err := os.Stat(filepath.Join(recovery, "phase3_state.go")); err != nil {
		t.Skip("extended recovery module not present")
	}
	dir := t.TempDir()
	os.Chmod(dir, 0700)
	binary := filepath.Join(dir, "dot-recovery")
	build := exec.Command(filepath.Join(runtime.GOROOT(), "bin", "go"), "build", "-buildvcs=false", "-o", binary, ".")
	build.Dir = recovery
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("recovery build %v %s", err, out)
	}
	policy := Policy{OwnerID: "1001", AllowedDMIDs: []string{"1001"}, Platform: "discord", GuildID: "1003", GuildChannelID: "1004", GuildMode: "mention"}
	cfg := Settings{Policy: policy, ExpectedBotID: "1002"}
	path := filepath.Join(dir, "source.db")
	s, err := OpenStore(path, policy)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	e := Envelope{Platform: "discord", EventID: "9001", ConversationID: "1004", SenderID: "1001", GuildID: "1003", RouteKind: "guild_text", BotMentioned: true, Text: "RAW_OWNER_BODY_NOT_BACKED_UP", ReceivedAt: epoch()}
	if out, err := s.Ingest(e); err != nil || out != "accepted" {
		t.Fatal(out, err)
	}
	c := claimLedger(t, s)
	files, err := s.ReplyOutputDir()
	if err != nil {
		t.Fatal(err)
	}
	os.WriteFile(filepath.Join(files, "PRIVATE_FILENAME.txt"), []byte("PRIVATE_FILE_BYTES"), 0600)
	manifest, _ := json.Marshal(ReplyManifest{Version: 1, Text: "RAW_REPLY_BODY_NOT_BACKED_UP", Attachments: []ReplyFile{{Path: "PRIVATE_FILENAME.txt", Description: "PRIVATE_DESCRIPTION"}}})
	reply, err := s.QueueReplyManifest(c.InboundID, c.Claim, manifest)
	if err != nil {
		t.Fatal(err)
	}
	chunk, err := s.NextChunk()
	if err != nil {
		t.Fatal(err)
	}
	ack := outputACK(*chunk)
	raw, _ := json.Marshal(ack)
	var fields map[string]json.RawMessage
	json.Unmarshal(raw, &fields)
	receipt, err := ReplyOutputAckReceipt(fields, *chunk)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.RecordResult(*chunk, SendResult{State: "sent", MessageID: "8001", OutputReceipt: receipt}); err != nil {
		t.Fatal(err)
	}
	cancelled := e
	cancelled.EventID = "9002"
	cancelled.Text = "cancellable"
	s.Ingest(cancelled)
	cc := claimLedger(t, s)
	cancelReply := replyLedger(t, s, cc, "never sent")
	if _, err = s.CancelReply(cancelReply); err != nil {
		t.Fatal(err)
	}
	target, err := s.sentControlTarget(e.ConversationID, "8001")
	if err != nil {
		t.Fatal(err)
	}
	reaction := e
	reaction.EventID = "8001"
	reaction.Text = ""
	reaction.ReplyKind = "message"
	reaction.Control = encodeControl(ControlEvent{Version: 1, Kind: "reaction", ID: "fixture", ActorID: e.SenderID, TargetMessageID: "8001", TargetRequestID: target.Request, TargetRevision: target.Revision, TargetText: target.Text, TargetOutput: json.RawMessage(target.Output), TargetReceipt: json.RawMessage(target.Receipt), Emoji: "👍", Added: true})
	if outcome, err := s.ingestReaction(reaction); err != nil || outcome != "accepted" {
		t.Fatal(outcome, err)
	}
	interaction := e
	interaction.EventID = "5001"
	interaction.ReplyKind = "interaction"
	interaction.Control = encodeControl(ControlEvent{Version: 1, Kind: "ask", ID: strings.Repeat("c", 32), InteractionID: "5001", ActorID: e.SenderID})
	if fresh, err := s.reserveInteraction("5001", interaction, "ask"); err != nil || !fresh {
		t.Fatal(fresh, err)
	}
	s.interactionState("5001", "acknowledged")
	if _, err = s.ingestControl(interaction, "5001"); err != nil {
		t.Fatal(err)
	}
	owned := map[string]string{"ask": "4001", "status": "4002", "cancel": "4003"}
	commands := OwnerCommandManifest()
	for i := range commands {
		commands[i].ID = owned[commands[i].Name]
		commands[i].ApplicationID = cfg.ExpectedBotID
		commands[i].GuildID = policy.GuildID
	}
	oldCommands := append([]GuildCommand{}, commands...)
	oldCommands[0].Description = "older reviewed command contract"
	before := CommandSnapshot{ApplicationID: cfg.ExpectedBotID, GuildID: policy.GuildID, Commands: oldCommands, OwnedIDs: owned}
	attemptKey := commandSnapshotDigest(before) + ":ask:update"
	_, err = s.call(func(db *storeConn) (any, error) {
		for name, id := range owned {
			if _, err := db.Exec(`INSERT INTO control_command_ids VALUES(?,?,?)`, policy.GuildID, name, id); err != nil {
				return nil, err
			}
		}
		if err := diagnosticSchema(db); err != nil {
			return nil, err
		}
		_, err := db.Exec(`CREATE TABLE test_sends(bot_id TEXT,guild_id TEXT,channel_id TEXT,owner_id TEXT,nonce TEXT,state TEXT,attempted INTEGER,message_id TEXT,created REAL,updated REAL)`)
		return nil, err
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = s.beginCommandWrite(attemptKey, "ask", "update"); err != nil {
		t.Fatal(err)
	}
	if err = s.recordCommandWrite(attemptKey, policy.GuildID, "ask", "", "uncertain"); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	reports := filepath.Join(dir, "reports")
	os.Mkdir(reports, 0700)
	os.WriteFile(filepath.Join(reports, "target.json"), []byte(`{"bot_id":"1002","guild_id":"1003","channel_id":"1005"}`), 0600)
	operation := filepath.Join(dir, "operation.json")
	os.WriteFile(operation, []byte(`{"owner_id":"1001","bot_id":"1002","guild_id":"1003","gateway_channel_id":"1004","report_channel_id":"1005","guild_mode":"mention","message_content_approved":false}`), 0600)
	snapshot := filepath.Join(dir, "snapshot.json")
	pin := strings.Repeat("a", 64)
	run := func(args ...string) {
		t.Helper()
		if out, err := exec.Command(binary, args...).CombinedOutput(); err != nil {
			t.Fatalf("recovery %v %s", err, out)
		}
	}
	run("snapshot", "--db", path, "--reports", reports, "--operation", operation, "--manifest-sha", pin, "--output", snapshot, "--apply")
	bytes, err := os.ReadFile(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"RAW_OWNER_BODY", "RAW_REPLY_BODY", "PRIVATE_FILENAME", "PRIVATE_DESCRIPTION", "PRIVATE_FILE_BYTES", "https://cdn"} {
		if strings.Contains(string(bytes), forbidden) {
			t.Fatal("snapshot exported private body", forbidden)
		}
	}
	root := filepath.Join(dir, "restored")
	run("restore-state", "--snapshot", snapshot, "--snapshot-sha", memoryFileSHA(t, snapshot), "--manifest-sha", pin, "--new-root", root, "--apply")
	restored, err := OpenStore(filepath.Join(root, "bridge", "bridge.sqlite3"), policy)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	for name, id := range owned {
		if !restored.commandIDMatches(policy.GuildID, name, id) || restored.commandIDMatches("9999", name, id) {
			t.Fatal("command ownership lost or broadened", name)
		}
	}
	if next, err := restored.ClaimNext(60, 0); err != nil || next != nil {
		t.Fatal("historical work runnable", next, err)
	}
	if next, err := restored.NextChunk(); err != nil || next != nil {
		t.Fatal("historical reply runnable", next, err)
	}
	if fresh, err := restored.reserveInteraction("5001", interaction, "ask"); err != nil || fresh {
		t.Fatal("old callback admitted", fresh, err)
	}
	if delivery, err := restored.Delivery(cancelReply); err != nil || delivery.State != "cancelled" {
		t.Fatal("cancellation lost", delivery, err)
	}
	_, err = restored.call(func(db *storeConn) (any, error) {
		var count int
		if err := db.QueryRow(`SELECT count(*) FROM recovery_rich_receipts WHERE reply_id=?`, reply).Scan(&count); err != nil {
			return nil, err
		}
		if count != 1 {
			t.Fatal("rich attachment metadata missing")
		}
		var state string
		if err := db.QueryRow(`SELECT value FROM catchup_meta WHERE key='state'`).Scan(&state); err != nil {
			return nil, err
		}
		if state != "disarmed_restore" {
			t.Fatal("catchup rearmed")
		}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	var writes atomic.Int32
	remoteCommands := commands
	rest := localREST(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			writes.Add(1)
			w.WriteHeader(500)
			return
		}
		switch r.URL.Path {
		case "/users/@me":
			json.NewEncoder(w).Encode(User{ID: "1002", Bot: true})
		case "/oauth2/applications/@me":
			json.NewEncoder(w).Encode(map[string]any{"id": "1002", "bot": User{ID: "1002", Bot: true}})
		case "/channels/1004":
			json.NewEncoder(w).Encode(Channel{ID: "1004", GuildID: "1003", Type: 0})
		case "/applications/1002/guilds/1003/commands":
			json.NewEncoder(w).Encode(remoteCommands)
		default:
			t.Errorf("unexpected fake GET %s", r.URL.Path)
			w.WriteHeader(404)
		}
	})
	rest.settings.ExpectedBotID = cfg.ExpectedBotID
	rest.settings.Policy = policy
	current, err := rest.CommandSnapshot(context.Background(), restored)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := PlanGuildCommands(cfg, current)
	if err != nil {
		t.Fatal("restored owned commands treated as collisions", err)
	}
	for _, change := range plan.Changes {
		if change.Action != "unchanged" {
			t.Fatal("unnecessary registration write", change)
		}
	}
	remoteCommands = oldCommands
	current, err = rest.CommandSnapshot(context.Background(), restored)
	if err != nil {
		t.Fatal(err)
	}
	plan, err = PlanGuildCommands(cfg, current)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = rest.ApplyGuildCommandPlan(context.Background(), restored, plan); err == nil || err.Error() != "command_attempt_exists_review_remote_before_reissue" {
		t.Fatal("registration uncertainty replayed", err)
	}
	if writes.Load() != 0 {
		t.Fatal("registration performed fake network write", writes.Load())
	}
}
