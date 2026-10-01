package bridge

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// Runs the real metadata-only recovery CLI, not a memory-only mock restore.
func TestMemoryActualRecoveryPath(t *testing.T) {
	recovery := os.Getenv("DOT_MEMORY_RECOVERY_SOURCE")
	if recovery == "" {
		recovery = filepath.Join("..", "..", "..", "recovery")
	}
	if _, err := os.Stat(filepath.Join(recovery, "state.go")); err != nil {
		t.Skip("recovery module not present; set DOT_MEMORY_RECOVERY_SOURCE")
	}
	recovery, _ = filepath.Abs(recovery)
	dir := t.TempDir()
	os.Chmod(dir, 0700)
	binary := filepath.Join(dir, "dot-recovery")
	build := exec.Command(filepath.Join(runtime.GOROOT(), "bin", "go"), "build", "-buildvcs=false", "-o", binary, ".")
	build.Dir = recovery
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("recovery build %v %s", err, out)
	}
	policy := Policy{Platform: "discord", OwnerID: "1001", AllowedDMIDs: []string{"1001"}, GuildID: "1003", GuildChannelID: "1004", GuildMode: "mention"}
	path := filepath.Join(dir, "source.db")
	s, err := OpenStore(path, policy)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	seed := func(id, conv, text, route string) Envelope {
		e := Envelope{Platform: "discord", EventID: id, ConversationID: conv, SenderID: "1001", Text: text, ReceivedAt: epoch(), RouteKind: route}
		if route != "dm" {
			e.GuildID = "1003"
			e.BotMentioned = true
		}
		if route == "guild_thread" {
			e.ParentChannelID = "1004"
			e.ThreadType = 12
		}
		// A synthetic already-validated thread fixture, no Discord permission calls.
		if route == "guild_thread" {
			_, err := s.call(func(db *storeConn) (any, error) {
				return transact(db, func(db *storeConn) (any, error) {
					raw, _ := json.Marshal(e)
					if err := registerSourceDB(db, e); err != nil {
						return nil, err
					}
					if _, err := db.Exec(`INSERT INTO inbound(id,platform,event_id,envelope,created) VALUES(?,'discord',?,?,?)`, id, id, string(raw), epoch()); err != nil {
						return nil, err
					}
					return nil, rememberInboundDB(db, id, e)
				})
			})
			if err != nil {
				t.Fatal(err)
			}
		} else {
			if st, err := s.Ingest(e); err != nil || st != "accepted" {
				t.Fatal(st, err)
			}
		}
		return e
	}
	seed("9001", "7001", "private itinerary Friday", "dm")
	first := claimLedger(t, s)
	f, err := s.PutMemory(first.InboundID, first.Claim, MemoryMutation{Key: "trip", Kind: "project", Text: "private itinerary Friday", Sources: []MemoryRef{first.Memory.CurrentSource}})
	if err != nil {
		t.Fatal(err)
	}
	replyLedger(t, s, first, "private confirmed reply")
	chunk, err := s.NextChunk()
	if err != nil {
		t.Fatal(err)
	}
	if err = s.RecordResult(*chunk, SendResult{State: "sent", MessageID: "8001"}); err != nil {
		t.Fatal(err)
	}
	seed("9002", "7001", "forgotten-detail", "dm")
	forgot := claimLedger(t, s)
	s.Ignore(forgot.InboundID, forgot.Claim)
	seed("9003", "7001", "deleted-detail", "dm")
	deleted := claimLedger(t, s)
	s.Ignore(deleted.InboundID, deleted.Claim)
	publicEvent := seed("9010", "1004", "public release knowledge", "guild_text")
	public := claimLedger(t, s)
	s.Ignore(public.InboundID, public.Claim)
	threadEvent := seed("9020", "7002", "private-thread knowledge", "guild_thread")
	thread := claimLedger(t, s)
	s.Ignore(thread.InboundID, thread.Claim)
	// A real source edit creates an internal revision ledger key.
	revised := seed("9030", "7001", "old revision text", "dm")
	revClaim := claimLedger(t, s)
	s.Ignore(revClaim.InboundID, revClaim.Claim)
	if _, err = s.InvalidateSourceUpdate("7001", "", "9030"); err != nil {
		t.Fatal(err)
	}
	refresh, err := s.NextSourceRefresh(epoch())
	if err != nil {
		t.Fatal(err)
	}
	revised.Text = "current revision knowledge"
	if st, err := s.ApplySourceRefresh(*refresh, revised); err != nil || st != "revised" {
		t.Fatal(st, err)
	}
	validation, err := s.NextValidation(epoch())
	if err != nil {
		t.Fatal(err)
	}
	if st, err := s.PromoteValidation(*validation); err != nil || st != "accepted" {
		t.Fatal(st, err)
	}
	revClaim = claimLedger(t, s)
	s.Ignore(revClaim.InboundID, revClaim.Claim)
	cancelEvent := seed("9040", "7001", "cancelled-detail", "dm")
	cancelClaim := claimLedger(t, s)
	replyLedger(t, s, cancelClaim, "queued but never sent")
	archive := filepath.Join(dir, "old.memory.jsonl")
	exported, err := s.ExportMemory(archive)
	if err != nil {
		t.Fatal(err)
	}
	if exported["sha256"] != memoryFileSHA(t, archive) {
		t.Fatal("export hash isn't whole archive")
	}
	seed("9004", "7001", "private itinerary changed to Monday", "dm")
	current := claimLedger(t, s)
	if err = s.ForgetMemory(current.InboundID, current.Claim, forgot.Memory.CurrentSource); err != nil {
		t.Fatal(err)
	}
	if _, err = s.DeleteSource("7001", "", "9003"); err != nil {
		t.Fatal(err)
	}
	if _, err = s.PutMemory(current.InboundID, current.Claim, MemoryMutation{Key: "trip", Kind: "project", Text: "private itinerary Monday", ExpectedVersion: f.Version, Sources: []MemoryRef{current.Memory.CurrentSource}}); err != nil {
		t.Fatal(err)
	}
	requests, err := s.ControlRequests(cancelEvent, cancelClaim.InboundID)
	if err != nil || len(requests) != 1 {
		t.Fatal(requests, err)
	}
	if _, err = s.CancelControlRequest(cancelEvent, cancelClaim.InboundID, requests[0].Revision); err != nil {
		t.Fatal(err)
	}
	control := cancelEvent
	control.EventID = "9050"
	control.Text = "excluded control history"
	control.ReplyKind = "interaction"
	control.Control = encodeControl(ControlEvent{Version: 1, Kind: "ask", ID: "9050", InteractionID: "9050", ActorID: "1001"})
	if _, err = s.ingestControl(control, ""); err != nil {
		t.Fatal(err)
	}
	_, err = s.call(func(db *storeConn) (any, error) {
		if err := diagnosticSchema(db); err != nil {
			return nil, err
		}
		_, err := db.Exec(`CREATE TABLE test_sends(bot_id TEXT,guild_id TEXT,channel_id TEXT,owner_id TEXT,nonce TEXT,state TEXT,attempted INTEGER,message_id TEXT,created REAL,updated REAL);`)
		return nil, err
	})
	if err != nil {
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
		out, err := exec.Command(binary, args...).CombinedOutput()
		if err != nil {
			t.Fatalf("recovery %s: %v %s", args[0], err, out)
		}
	}
	run("snapshot", "--db", path, "--reports", reports, "--operation", operation, "--manifest-sha", pin, "--output", snapshot, "--apply")
	restoredRoot := filepath.Join(dir, "restored")
	run("restore-state", "--snapshot", snapshot, "--snapshot-sha", memoryFileSHA(t, snapshot), "--manifest-sha", pin, "--new-root", restoredRoot, "--apply")
	restoredPath := filepath.Join(restoredRoot, "bridge", "bridge.sqlite3")
	restored, err := OpenStore(restoredPath, policy)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	if _, err = restored.ImportMemory(archive, memoryFileSHA(t, archive)); err != nil {
		t.Fatal(err)
	}
	if claim, err := restored.ClaimNext(300, 0); err != nil || claim != nil {
		t.Fatal("restored runnable claim", claim, err)
	}
	if chunk, err := restored.NextChunk(); err != nil || chunk != nil {
		t.Fatal("restored runnable reply", chunk, err)
	}
	_, err = restored.call(func(db *storeConn) (any, error) {
		var n int
		if err := db.QueryRow(`SELECT count(*) FROM message_sources`).Scan(&n); err != nil {
			return nil, err
		}
		if n != 0 {
			t.Fatal("historical import created source heads")
		}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range []Envelope{first.Envelope, publicEvent, threadEvent} {
		v, err := restored.call(func(db *storeConn) (any, error) { return recallMemoryDB(db, "new", e, "knowledge itinerary") })
		if err != nil {
			t.Fatal(err)
		}
		memory := v.(*MemoryRecall)
		if len(memory.Items) == 0 {
			t.Fatalf("missing restored scope %s", e.RouteKind)
		}
		for _, item := range memory.Items {
			if item.ProvenanceState != "restored_unverified_history" {
				t.Fatal("restored falsely current", item)
			}
			if strings.Contains(item.Text, "forgotten-detail") || strings.Contains(item.Text, "deleted-detail") || strings.Contains(item.Text, "cancelled-detail") || strings.Contains(item.Text, "excluded control history") || item.Role == "fact" {
				t.Fatal("old archive defeated newer metadata fence", item)
			}
		}
		if e.RouteKind != "dm" && memoryContains(memory, "private itinerary") {
			t.Fatal("private DM leaked after restore")
		}
		if e.RouteKind != "guild_thread" && memoryContains(memory, "private-thread") {
			t.Fatal("thread leaked after restore")
		}
	}
	// Second-generation backups preserve source/deletion/version fences too.
	archive2 := filepath.Join(dir, "second.memory.jsonl")
	if _, err = restored.ExportMemory(archive2); err != nil {
		t.Fatal(err)
	}
	snapshot2 := filepath.Join(dir, "snapshot2.json")
	run("snapshot", "--db", restoredPath, "--reports", filepath.Join(restoredRoot, "reports"), "--operation", filepath.Join(restoredRoot, "operation.json"), "--manifest-sha", pin, "--output", snapshot2, "--apply")
	secondRoot := filepath.Join(dir, "second-restored")
	run("restore-state", "--snapshot", snapshot2, "--snapshot-sha", memoryFileSHA(t, snapshot2), "--manifest-sha", pin, "--new-root", secondRoot, "--apply")
	second, err := OpenStore(filepath.Join(secondRoot, "bridge", "bridge.sqlite3"), policy)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if _, err = second.ImportMemory(archive2, memoryFileSHA(t, archive2)); err != nil {
		t.Fatal(err)
	}
	recalled, err := second.call(func(db *storeConn) (any, error) { return recallMemoryDB(db, "new", first.Envelope, "itinerary") })
	if err != nil || !memoryContains(recalled.(*MemoryRecall), "private itinerary Friday") {
		t.Fatal("second generation lost history", recalled, err)
	}
	if memoryContains(recalled.(*MemoryRecall), "forgotten-detail") || memoryContains(recalled.(*MemoryRecall), "deleted-detail") || memoryContains(recalled.(*MemoryRecall), "cancelled-detail") {
		t.Fatal("second generation lost deletion fence")
	}
	// Source heads absent is allowed only for explicitly restored evidence.
	_, err = restored.call(func(db *storeConn) (any, error) {
		_, err := db.Exec(`UPDATE memory_meta SET value='9999' WHERE key='restored_owner_id';DELETE FROM memory_fact_sources;DELETE FROM memory_facts;DELETE FROM memory_documents`)
		return nil, err
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = restored.ImportMemory(archive, memoryFileSHA(t, archive)); err == nil || err.Error() != "memory_backup_owner_mismatch" {
		t.Fatal("owner mismatch accepted", err)
	}
	if _, err = restored.ImportMemory(archive, fmt.Sprintf("%064d", 0)); err == nil || err.Error() != "memory_backup_hash_mismatch" {
		t.Fatal("hash mismatch accepted", err)
	}
}
