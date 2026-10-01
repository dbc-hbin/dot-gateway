package main

import (
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
)

func TestMemoryRecoveryFencesAndRevisionKeys(t *testing.T) {
	s := stateFixture()
	s.MemoryLedgerID = strings.Repeat("a", 32)
	s.Events[0].EventID = "9001:revision:2"
	s.MemorySources = []MemorySourceFence{{Platform: "discord", EventID: "9001", Revision: 2, State: "deleted", Scope: strings.Repeat("b", 64)}}
	s.MemoryFences = []MemoryDocumentFence{{"user:in1", 0, true}, {"fact:" + strings.Repeat("a", 64) + ":trip", 4, false}}
	root := filepath.Join(privateTemp(t), "restored")
	if err := restoreState(s, root, -1); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", filepath.Join(root, "bridge", "bridge.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var body, state, identity string
	if err = db.QueryRow(`SELECT envelope,state FROM inbound WHERE id='in1'`).Scan(&body, &state); err != nil || body != "{}" || state != "blocked" {
		t.Fatal(body, state, err)
	}
	if err = db.QueryRow(`SELECT value FROM memory_meta WHERE key='ledger_identity'`).Scan(&identity); err != nil || identity != s.MemoryLedgerID {
		t.Fatal(identity, err)
	}
	var n int
	if err = db.QueryRow(`SELECT count(*) FROM memory_restore_fences`).Scan(&n); err != nil || n != 2 {
		t.Fatal(n, err)
	}
	if err = db.QueryRow(`SELECT count(*) FROM sqlite_master WHERE type='table' AND name='message_sources'`).Scan(&n); err != nil || n != 0 {
		t.Fatal("created runnable source heads", n, err)
	}
	for _, id := range []string{"9001:revision:0", "9001:revision:-1", "9001:revision:01", "9001:revision:1:revision:2", "bad:revision:1", "9001:revision:+1", "9001:revision:9223372036854775808"} {
		if sourceEventID(id) {
			t.Fatal("invalid revision accepted", id)
		}
	}
	old := stateFixture()
	if err = validateSnapshot(old); err != nil {
		t.Fatal("old snapshot invalid", err)
	}
}
func TestMemoryRecoveryStrictControlEventKeys(t *testing.T) {
	for _, id := range []string{"control:9001", "control:" + strings.Repeat("a", 32)} {
		s := stateFixture()
		s.Events[0].EventID = id
		if err := validateSnapshot(s); err != nil {
			t.Fatal(id, err)
		}
	}
	for _, id := range []string{"control:fixture", "control:../secret", "control:000", "control:9001:revision:1", "control:" + strings.Repeat("g", 32)} {
		s := stateFixture()
		s.Events[0].EventID = id
		if err := validateSnapshot(s); err == nil {
			t.Fatal("invalid control key", id)
		}
	}
}
