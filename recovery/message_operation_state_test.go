package main

import (
	"database/sql"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
)

func TestMessageOperationRecoveryFencesNoRunnableRows(t *testing.T) {
	s := stateFixture()
	s.MessageOperations = &MessageOperationState{Version: 1, ApplicationID: s.Operation.BotID, GuildID: s.Operation.GuildID, OwnerID: s.Operation.OwnerID,
		Operations:  []MessageOperationFence{{strings.Repeat("a", 64), "1004", "8001", true, "edit_text", "prepared", 0, 1}, {strings.Repeat("b", 64), "1004", "8002", true, "pin", "uncertain", 1, 2}, {strings.Repeat("c", 64), "1004", "8003", false, "remove_own_reaction", "verified", 1, 3}},
		Projections: []MessageEditProjectionFence{{"1004", "8001", 0}, {"1004", "8004", 3}}}
	s.MemoryLedgerID = strings.Repeat("e", 32)
	s.MemoryFences = []MemoryDocumentFence{{DocumentID: "assistant-edit:" + strings.Repeat("f", 64), Forgotten: true}}
	root := filepath.Join(privateTemp(t), "restored")
	if err := restoreState(s, root, -1); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", filepath.Join(root, "bridge", "bridge.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	var n int
	if err = db.QueryRow(`SELECT count(*) FROM message_operation_recovery_fences WHERE hold_target=1`).Scan(&n); err != nil || n != 2 {
		t.Fatal(n, err)
	}
	if err = db.QueryRow(`SELECT count(*) FROM sqlite_master WHERE name IN('message_operations','message_edit_projection','message_edit_revisions','message_operation_targets')`).Scan(&n); err != nil || n != 0 {
		t.Fatal("runnable operation restored", n, err)
	}
	if _, err = db.Exec(`CREATE TABLE ingress_validation(id TEXT,platform TEXT,event_id TEXT,state TEXT,created REAL);CREATE TABLE message_sources(platform TEXT,event_id TEXT,revision INTEGER,state TEXT);CREATE TABLE memory_documents(id INTEGER PRIMARY KEY,record_key TEXT,platform TEXT,event_id TEXT,source_revision INTEGER,scope TEXT,role TEXT,active INTEGER,body TEXT);CREATE TABLE memory_facts(doc_id INTEGER,version INTEGER)`); err != nil {
		t.Fatal(err)
	}
	db.Close()
	second := Snapshot{Schema: 1, Created: s.Created, ManifestSHA: s.ManifestSHA, Operation: s.Operation}
	if err = snapshotDB(filepath.Join(root, "bridge", "bridge.sqlite3"), &second); err != nil {
		t.Fatal(err)
	}
	if second.MessageOperations == nil || len(second.MessageOperations.Operations) != 3 || len(second.MessageOperations.Projections) != 2 {
		t.Fatal("operation fences lost on rebackup", second.MessageOperations)
	}
	raw, _ := json.Marshal(second.MessageOperations)
	for _, key := range []string{"spec", "route", "snapshot", "operation_key", "memory_key"} {
		if strings.Contains(string(raw), key) {
			t.Fatal("unsafe operation metadata", key)
		}
	}
}
func TestMessageOperationSnapshotSelectsOnlySafeFields(t *testing.T) {
	s := stateFixture()
	root := filepath.Join(privateTemp(t), "restored")
	if err := restoreState(s, root, -1); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", filepath.Join(root, "bridge", "bridge.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`CREATE TABLE ingress_validation(id TEXT,platform TEXT,event_id TEXT,state TEXT,created REAL);CREATE TABLE message_operations(id TEXT,channel TEXT,message TEXT,spec TEXT,state TEXT,attempts INTEGER,created REAL);CREATE TABLE message_edit_projection(channel TEXT,message TEXT,revision INTEGER,state TEXT,operation_id TEXT,memory_key TEXT);INSERT INTO message_operations VALUES(?,'1004','8001','{"action":"edit_text","text":"SECRET_BODY","emoji":"SECRET_EMOJI"}','uncertain',1,1);INSERT INTO message_edit_projection VALUES('1004','8001',1,'unknown',?,'SECRET_MEMORY_KEY')`, strings.Repeat("a", 64), strings.Repeat("a", 64))
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
	snap := Snapshot{Schema: 1, Created: s.Created, ManifestSHA: s.ManifestSHA, Operation: s.Operation}
	if err = snapshotDB(filepath.Join(root, "bridge", "bridge.sqlite3"), &snap); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(snap)
	if strings.Contains(string(raw), "SECRET") {
		t.Fatal("operation payload exported")
	}
	if !snap.MessageOperations.Operations[0].HoldTarget {
		t.Fatal("uncertain target not held")
	}
}
