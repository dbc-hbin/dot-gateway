package bridge

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// Compose the real recovery CLI with gateway activation. An empty old snapshot
// has no historical rows or memory ledger marker to incidentally signal restore.
func TestCatchupActualEmptyRestoreNeverArms(t *testing.T) {
	source := os.Getenv("DOT_MEMORY_RECOVERY_SOURCE")
	if source == "" {
		source = filepath.Join("..", "..", "..", "recovery")
	}
	if _, err := os.Stat(filepath.Join(source, "state.go")); err != nil {
		t.Skip("recovery module not present; set DOT_MEMORY_RECOVERY_SOURCE")
	}
	source, _ = filepath.Abs(source)
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(dir, "dot-recovery")
	build := exec.Command(filepath.Join(runtime.GOROOT(), "bin", "go"), "build", "-buildvcs=false", "-o", binary, ".")
	build.Dir = source
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("recovery build: %v %s", err, out)
	}
	pin := strings.Repeat("a", 64)
	snapshot := map[string]any{"schema": 1, "created_at": time.Now().UTC().Format(time.RFC3339Nano), "source_manifest_sha256": pin, "operation": map[string]any{"owner_id": "1001", "bot_id": "1002", "guild_id": "1003", "gateway_channel_id": "1004", "report_channel_id": "1005", "guild_mode": "mention", "message_content_approved": false}}
	for _, key := range []string{"events", "ingress", "replies", "chunks", "diagnostics", "test_sends", "reports"} {
		snapshot[key] = []any{}
	}
	raw, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(dir, "empty.snapshot.json")
	if err = os.WriteFile(file, raw, 0600); err != nil {
		t.Fatal(err)
	}
	dest := filepath.Join(dir, "restored")
	cmd := exec.Command(binary, "restore-state", "--snapshot", file, "--snapshot-sha", memoryFileSHA(t, file), "--manifest-sha", pin, "--new-root", dest, "--apply")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("real empty restore: %v %s", err, out)
	}
	path := filepath.Join(dest, "bridge", "bridge.sqlite3")
	policy := Policy{Platform: "discord", OwnerID: "1001", AllowedDMIDs: []string{"1001"}, GuildID: "1003", GuildChannelID: "1004", GuildMode: "mention"}
	store, err := OpenStore(path, policy)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err = store.ActivateCatchup(Settings{DBPath: path, Policy: policy, ExpectedBotID: "1002"}, time.Now()); err != nil {
		t.Fatal(err)
	}
	status, err := store.CatchupStatus()
	if err != nil || status["state"] != "disarmed_restore" || len(status["routes"].([]CatchupStatus)) != 0 {
		t.Fatal(status, err)
	}
	if claim, err := store.ClaimNext(30, 0); err != nil || claim != nil {
		t.Fatal("restore became a task", claim, err)
	}
	if chunk, err := store.NextChunk(); err != nil || chunk != nil {
		t.Fatal("restore became a reply", chunk, err)
	}
	if in, err := store.nextCatchup(epoch() + 100); err != nil || in != nil {
		t.Fatal("restore became a recovery scan", in, err)
	}
	if _, err = os.Lstat(path + ".catchup-live"); !os.IsNotExist(err) {
		t.Fatal("restore created a live witness", err)
	}
}
