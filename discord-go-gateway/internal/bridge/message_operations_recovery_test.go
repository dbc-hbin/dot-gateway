package bridge

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// Exercises the actual independent recovery module against this schema. The
// recovered ledger has inert fences, never runnable operations or live claims.
func TestMessageOperationActualMetadataAndMemoryRecovery(t *testing.T) {
	for _, unknown := range []bool{false, true} {
		t.Run(fmt.Sprint(unknown), func(t *testing.T) { testOperationRecovery(t, unknown) })
	}
}
func testOperationRecovery(t *testing.T, unknown bool) {
	recovery := os.Getenv("DOT_MEMORY_RECOVERY_SOURCE")
	if recovery == "" {
		t.Skip("set DOT_MEMORY_RECOVERY_SOURCE for recovery integration")
	}
	if _, err := os.Stat(filepath.Join(recovery, "message_operation_state.go")); err != nil {
		t.Skip("operation-aware recovery module required")
	}
	dir := t.TempDir()
	os.Chmod(dir, 0700)
	binary := filepath.Join(dir, "dot-recovery")
	build := exec.Command(filepath.Join(runtime.GOROOT(), "bin", "go"), "build", "-buildvcs=false", "-o", binary, ".")
	build.Dir = recovery
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %v %s", err, out)
	}
	f := newOperationFixture(t)
	original := MemoryRef{DocumentID: "assistant:" + f.chunk.ReplyID + ":0", SourceRevision: 0}
	if _, err := f.store.PutMemory(f.claim.InboundID, f.claim.Claim, MemoryMutation{Key: "old-answer", Kind: "fact", Text: "original answer fact", Sources: []MemoryRef{original, f.claim.Memory.CurrentSource}}); err != nil {
		t.Fatal(err)
	}
	oldArchive := filepath.Join(dir, "preedit-memory.jsonl")
	if _, err := f.store.ExportMemory(oldArchive); err != nil {
		t.Fatal(err)
	}
	edit, err := f.execute(f.spec("edit_text", "restore-edit"))
	if err != nil || edit.State != "verified" {
		t.Fatal(edit, err)
	}
	if _, err = f.store.PutMemory(f.claim.InboundID, f.claim.Claim, MemoryMutation{Key: "edited-answer", Kind: "fact", Text: "revised answer fact", Sources: []MemoryRef{{DocumentID: "assistant-edit:" + edit.ID, SourceRevision: 0}, f.claim.Memory.CurrentSource}}); err != nil {
		t.Fatal(err)
	}
	archive := filepath.Join(dir, "memory.jsonl")
	if _, err = f.store.ExportMemory(archive); err != nil {
		t.Fatal(err)
	}
	_, err = f.store.call(func(db *storeConn) (any, error) {
		if err := diagnosticSchema(db); err != nil {
			return nil, err
		}
		_, err := db.Exec(`CREATE TABLE test_sends(bot_id TEXT,guild_id TEXT,channel_id TEXT,owner_id TEXT,nonce TEXT,state TEXT,attempted INTEGER,message_id TEXT,created REAL,updated REAL)`)
		return nil, err
	})
	if err != nil {
		t.Fatal(err)
	}
	if unknown {
		if _, err = f.store.InvalidateControlTarget("2", "", "9", "target_updated"); err != nil {
			t.Fatal(err)
		}
	}
	reports := filepath.Join(dir, "reports")
	os.Mkdir(reports, 0700)
	os.WriteFile(filepath.Join(reports, "target.json"), []byte(`{"bot_id":"4","guild_id":"5","channel_id":"6"}`), 0600)
	config := filepath.Join(dir, "operation.json")
	os.WriteFile(config, []byte(`{"owner_id":"1","bot_id":"4","guild_id":"5","gateway_channel_id":"7","report_channel_id":"6","guild_mode":"mention","message_content_approved":false}`), 0600)
	snapshot := filepath.Join(dir, "snapshot.json")
	pin := strings.Repeat("a", 64)
	run := func(args ...string) {
		t.Helper()
		if out, err := exec.Command(binary, args...).CombinedOutput(); err != nil {
			t.Fatalf("recovery %s: %v %s", args[0], err, out)
		}
	}
	run("snapshot", "--db", f.store.path, "--reports", reports, "--operation", config, "--manifest-sha", pin, "--output", snapshot, "--apply")
	bytes, err := os.ReadFile(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(bytes), "revised answer") || strings.Contains(string(bytes), "original answer") {
		t.Fatal("operation payload leaked into metadata")
	}
	root := filepath.Join(dir, "restored")
	run("restore-state", "--snapshot", snapshot, "--snapshot-sha", memoryFileSHA(t, snapshot), "--manifest-sha", pin, "--new-root", root, "--apply")
	policy := testPolicy()
	policy.GuildID = "5"
	policy.GuildChannelID = "7"
	restored, err := OpenStore(filepath.Join(root, "bridge", "bridge.sqlite3"), policy)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	if _, err = restored.ImportMemory(oldArchive, memoryFileSHA(t, oldArchive)); err != nil {
		t.Fatal("import pre-edit memory", err)
	}
	if _, err = restored.ImportMemory(archive, memoryFileSHA(t, archive)); err != nil {
		t.Fatal("import edited memory", err)
	}
	var count int
	restored.call(func(db *storeConn) (any, error) {
		return nil, db.QueryRow(`SELECT count(*) FROM memory_documents WHERE record_key=?`, "assistant-edit:"+edit.ID).Scan(&count)
	})
	if count != 1 {
		t.Fatal("edited memory silently discarded")
	}
	if c, err := restored.ClaimNext(300, 0); err != nil || c != nil {
		t.Fatal("restored runnable claim", c, err)
	}
	e := testEnvelope()
	e.EventID = "12"
	e.Text = "Review message 9"
	if out, err := restored.Ingest(e); err != nil || out != "accepted" {
		t.Fatal(out, err)
	}
	claim, err := restored.ClaimNext(300, 0)
	if err != nil || claim == nil {
		t.Fatal(claim, err)
	}
	recall, err := restored.RecallMemory(claim.InboundID, claim.Claim, "")
	if err != nil || memoryContains(recall, "original answer") || memoryContains(recall, "revised answer") {
		encoded, _ := json.Marshal(recall)
		t.Fatal(string(encoded), err)
	}
	if _, err = f.rest.ExecuteMessageOperation(context.Background(), restored, f.claim.InboundID, f.claim.Claim, edit.Spec); err == nil || err.Error() != "operation_recovery_fence" {
		t.Fatal("restored ID fence", err)
	}
	if _, err = f.rest.ExecuteMessageOperation(context.Background(), restored, claim.InboundID, claim.Claim, f.spec("pin", "new")); err == nil || err.Error() != "operation_recovery_fence" {
		t.Fatal("restored projection fence", err)
	}
	if _, err = f.rest.ReconcileMessageOperation(context.Background(), restored, claim.InboundID, claim.Claim, edit.ID, true); err == nil {
		t.Fatal("restored inert op became reconcilable")
	}
	if f.writes.Load() != 1 {
		t.Fatal("recovery repeated mutation", f.writes.Load())
	}
}
