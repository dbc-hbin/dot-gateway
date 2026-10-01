package main

import (
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestEveryRestoredLedgerDisarmsCatchup(t *testing.T) {
	for _, kind := range []string{"empty", "legacy", "memory"} {
		t.Run(kind, func(t *testing.T) {
			s := stateFixture()
			if kind == "empty" {
				s.Events = nil
				s.Ingress = nil
				s.Replies = nil
				s.Chunks = nil
				s.Diagnostics = nil
				s.TestSends = nil
			}
			if kind == "memory" {
				s.MemoryLedgerID = strings.Repeat("b", 32)
			}
			root := filepath.Join(privateTemp(t), "restored")
			if err := restoreState(s, root, -1); err != nil {
				t.Fatal(err)
			}
			db, err := sql.Open("sqlite", filepath.Join(root, "bridge", "bridge.sqlite3"))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			var state string
			if err = db.QueryRow(`SELECT value FROM catchup_meta WHERE key='state'`).Scan(&state); err != nil || state != "disarmed_restore" {
				t.Fatal(state, err)
			}
			var n int
			if err = db.QueryRow(`SELECT count(*) FROM sqlite_master WHERE type='table' AND name IN('catchup_routes','catchup_requested')`).Scan(&n); err != nil || n != 0 {
				t.Fatal("restored runnable catchup state", n, err)
			}
		})
	}
}
func TestSnapshotExcludesCatchupOriginsCursorsAndWitness(t *testing.T) {
	dir := privateTemp(t)
	path := filepath.Join(dir, "ledger.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(restoreSchema + `CREATE TABLE ingress_validation(id TEXT,platform TEXT,event_id TEXT,state TEXT,created REAL);CREATE TABLE catchup_routes(channel_id TEXT,route TEXT,floor TEXT);INSERT INTO catchup_routes VALUES('1004','PRIVATE_RECOVERY_BODY','123456789');INSERT INTO catchup_meta VALUES('origin','PRIVATE_ORIGIN'),('witness','PRIVATE_WITNESS');`); err != nil {
		t.Fatal(err)
	}
	db.Close()
	if err = os.Chmod(path, 0600); err != nil {
		t.Fatal(err)
	}
	s := stateFixture()
	s.Events = nil
	s.Ingress = nil
	s.Replies = nil
	s.Chunks = nil
	s.Diagnostics = nil
	s.TestSends = nil
	if err = snapshotDB(path, &s); err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	for _, needle := range []string{"PRIVATE_RECOVERY_BODY", "PRIVATE_ORIGIN", "PRIVATE_WITNESS", "catchup_routes", "catchup_meta", "123456789"} {
		if strings.Contains(string(raw), needle) {
			t.Fatal("included live recovery state", needle)
		}
	}
}
