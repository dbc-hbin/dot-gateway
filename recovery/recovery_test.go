package main

import (
	"archive/zip"
	"bytes"
	"database/sql"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func privateTemp(t *testing.T) string {
	t.Helper()
	p := t.TempDir()
	if e := os.Chmod(p, 0700); e != nil {
		t.Fatal(e)
	}
	return p
}
func put(t *testing.T, p string, b []byte) {
	t.Helper()
	if e := os.WriteFile(p, b, 0600); e != nil {
		t.Fatal(e)
	}
}
func sourceFixture(t *testing.T) (string, Source, string) {
	t.Helper()
	dir := privateTemp(t)
	b := []byte("safe sample source\n")
	put(t, filepath.Join(dir, "hello.go"), b)
	m := Manifest{1, []FileEntry{{"hello.go", int64(len(b)), digest(b)}}}
	raw := jsonBytes(m)
	put(t, filepath.Join(dir, "SOURCE_MANIFEST.json"), raw)
	s, e := loadSource(dir, "", digest(raw))
	if e != nil {
		t.Fatal(e)
	}
	return dir, s, digest(raw)
}
func stateFixture() Snapshot {
	now := "2026-10-01T00:00:00Z"
	s := Snapshot{Schema: 1, Created: now, ManifestSHA: strings.Repeat("a", 64), Operation: Operation{"1001", "1002", "1003", "1004", "1005", "all", true}, Events: []Event{{"in1", "discord", "9001", "replied", 1}, {"in2", "discord", "9002", "pending", 2}}, Ingress: []Event{{"in3", "discord", "9003", "pending", 3}}, Replies: []Reply{{"reply1", "in1", 1}}, Chunks: []Chunk{{"reply1", 0, "sent", "8001", 1}, {"reply1", 1, "sending", "", 1}}, Diagnostics: []Diagnostic{{"diag1", 0, "nonce1", "1002", "1001", "1003", "1004", "pending", 0, "", 1}}, TestSends: []TestSend{{"1002", "1003", "1004", "1001", "nonce2", "pending", 0, "", 1, 1}}}
	target := Target{"1002", "1003", "1005"}
	for _, run := range []string{"complete", "incomplete"} {
		r := Receipt{Version: 1, RunID: run, Target: target, PayloadHash: digest([]byte(run)), CreatedAt: now, Complete: run == "complete"}
		c := ReportChunk{Index: 0, ContentHash: digest([]byte(run)), State: "prepared"}
		b, _ := json.Marshal([]any{"discord-report/v1", target, run, r.PayloadHash, 0})
		c.Nonce = digest(b)[:24]
		if r.Complete {
			c.State = "confirmed"
			c.AttemptedAt = now
			c.ConfirmedAt = now
			c.MessageID = "7001"
			c.MessageURL = "https://discord.com/channels/1003/1005/7001"
		}
		r.Chunks = []ReportChunk{c}
		s.Reports = append(s.Reports, r)
	}
	return s
}
func TestSourceRestoreRoundTripAndNoOverwrite(t *testing.T) {
	dir, s, pin := sourceFixture(t)
	dest := filepath.Join(privateTemp(t), "fresh")
	if e := restoreSource(s, dest, -1); e != nil {
		t.Fatal(e)
	}
	if _, e := loadSource(dest, "", pin); e != nil {
		t.Fatal(e)
	}
	if e := restoreSource(s, dest, -1); e == nil {
		t.Fatal("overwrote destination")
	}
	if e := execute([]string{"restore-source", "--source", dir, "--manifest-sha", pin, "--new-root", filepath.Join(privateTemp(t), "dry")}); e != nil {
		t.Fatal(e)
	}
}
func TestSourceInterruptedWrite(t *testing.T) {
	_, s, _ := sourceFixture(t)
	dest := filepath.Join(privateTemp(t), "new")
	if e := restoreSource(s, dest, 0); e == nil {
		t.Fatal("missing failure")
	}
	if _, e := os.Lstat(dest); !os.IsNotExist(e) {
		t.Fatal("partial destination exposed")
	}
	entries, _ := os.ReadDir(filepath.Dir(dest))
	if len(entries) != 0 {
		t.Fatal("stage not cleaned")
	}
}
func TestSourceTamperAndUnsafePaths(t *testing.T) {
	dir, _, pin := sourceFixture(t)
	put(t, filepath.Join(dir, "hello.go"), []byte("tampered"))
	if _, e := loadSource(dir, "", pin); e == nil {
		t.Fatal("tamper accepted")
	}
	for _, p := range []string{"../escape", "/escape", "a/../b", "a\\b", "a//b", ".env", "x/.discord-private/bot-token"} {
		if allowedSource(p) {
			t.Fatal("unsafe path", p)
		}
	}
}
func TestSourceSymlinkHardlinkRejected(t *testing.T) {
	for _, link := range []string{"symlink", "hardlink"} {
		t.Run(link, func(t *testing.T) {
			dir, _, pin := sourceFixture(t)
			os.Remove(filepath.Join(dir, "hello.go"))
			outside := filepath.Join(privateTemp(t), "safe")
			put(t, outside, []byte("safe sample source\n"))
			var e error
			if link == "symlink" {
				e = os.Symlink(outside, filepath.Join(dir, "hello.go"))
			} else {
				e = os.Link(outside, filepath.Join(dir, "hello.go"))
			}
			if e != nil {
				t.Fatal(e)
			}
			if _, e = loadSource(dir, "", pin); e == nil {
				t.Fatal("link accepted")
			}
		})
	}
}
func TestArchiveRoundTripDuplicateTraversal(t *testing.T) {
	_, s, pin := sourceFixture(t)
	b, e := packSource(s)
	if e != nil {
		t.Fatal(e)
	}
	p := filepath.Join(privateTemp(t), "source.zip")
	put(t, p, b)
	if _, e = loadSource("", p, pin); e != nil {
		t.Fatal(e)
	}
	for _, name := range []string{"../bad", "SOURCE_MANIFEST.json"} {
		var b bytes.Buffer
		w := zip.NewWriter(&b)
		for _, n := range []string{"SOURCE_MANIFEST.json", name} {
			f, _ := w.Create(n)
			f.Write(s.Raw)
		}
		w.Close()
		put(t, p, b.Bytes())
		if _, e = loadSource("", p, pin); e == nil {
			t.Fatal("unsafe ZIP accepted")
		}
	}
}
func TestStateRestoreNoReplayAndPrivacy(t *testing.T) {
	s := stateFixture()
	dest := filepath.Join(privateTemp(t), "state")
	if e := restoreState(s, dest, -1); e != nil {
		t.Fatal(e)
	}
	db, e := dbRO(filepath.Join(dest, "bridge/bridge.sqlite3"))
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close()
	for _, q := range []string{"SELECT count(*) FROM inbound WHERE state!='blocked' OR envelope!='{}' OR claim IS NOT NULL OR lease_until IS NOT NULL", "SELECT count(*) FROM chunks WHERE state IN ('pending','sending') OR text!=''", "SELECT count(*) FROM replies WHERE text!=''", "SELECT count(*) FROM go_transport_diagnostics WHERE attempts!=1 OR state!='uncertain' OR content!=''", "SELECT count(*) FROM test_sends WHERE attempted!=1 OR state!='uncertain' OR content!=''"} {
		var n int
		if e = db.QueryRow(q).Scan(&n); e != nil || n != 0 {
			t.Fatalf("replay/privacy failure %s %d %v", q, n, e)
		}
	}
	var n int
	db.QueryRow("SELECT count(*) FROM inbound").Scan(&n)
	if n != 3 {
		t.Fatal("missing ingress tombstone")
	}
	for _, r := range s.Reports {
		b, e := os.ReadFile(filepath.Join(dest, "reports/run-"+digest([]byte(r.RunID))+".json"))
		if e != nil {
			t.Fatal(e)
		}
		var actual Receipt
		if strict(b, &actual) != nil {
			t.Fatal("bad receipt")
		}
		if r.Complete && !bytes.Equal(b, jsonBytes(r)) {
			t.Fatal("confirmed receipt changed")
		}
		if !r.Complete && (actual.Chunks[0].State != "uncertain" || !stampOK(actual.Chunks[0].AttemptedAt)) {
			t.Fatal("nonterminal not quarantined")
		}
	}
	filepath.WalkDir(dest, func(p string, d os.DirEntry, e error) error {
		if e != nil {
			t.Fatal(e)
		}
		st, _ := d.Info()
		if st.Mode().Perm()&0077 != 0 {
			t.Fatal("public restored file")
		}
		return nil
	})
}
func TestStateDuplicateLedgerAndInterrupt(t *testing.T) {
	s := stateFixture()
	s.Reports = append(s.Reports, s.Reports[0])
	if validateSnapshot(s) == nil {
		t.Fatal("duplicate ledger accepted")
	}
	s = stateFixture()
	s.Events = append(s.Events, s.Events[0])
	if validateSnapshot(s) == nil {
		t.Fatal("duplicate event accepted")
	}
	s = stateFixture()
	dest := filepath.Join(privateTemp(t), "state")
	if restoreState(s, dest, 1) == nil {
		t.Fatal("missing interrupted fault")
	}
	if _, e := os.Lstat(dest); !os.IsNotExist(e) {
		t.Fatal("partial state exposed")
	}
}
func TestProjectionDoesNotReadContent(t *testing.T) {
	dir := privateTemp(t)
	p := filepath.Join(dir, "fixture.sqlite3")
	put(t, p, nil)
	db, e := sql.Open("sqlite", p)
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close()
	if _, e = db.Exec(restoreSchema + `CREATE TABLE ingress_validation(id TEXT,platform TEXT,event_id TEXT,state TEXT,created REAL,envelope TEXT); INSERT INTO inbound VALUES('in1','discord','9001','SECRET RAW CONVERSATION','pending',NULL,NULL,1); INSERT INTO replies VALUES('r1','in1','SECRET REPLY',1); INSERT INTO chunks VALUES('r1',0,'SECRET CHUNK','sent','8001',NULL,1);`); e != nil {
		t.Fatal(e)
	}
	s := stateFixture()
	s.Events = nil
	s.Ingress = nil
	s.Replies = nil
	s.Chunks = nil
	s.Diagnostics = nil
	s.TestSends = nil
	if e = snapshotDB(p, &s); e != nil {
		t.Fatal(e)
	}
	if strings.Contains(string(jsonBytes(s)), "SECRET") {
		t.Fatal("content leaked")
	}
	if len(s.Events) != 1 {
		t.Fatal("missing projection")
	}
}
func TestCredentialMetadataDoesNotRead(t *testing.T) {
	dir := privateTemp(t)
	p := filepath.Join(dir, "token")
	put(t, p, []byte("SYNTHETIC-DO-NOT-READ"))
	if credentialStatus(p) != "present_metadata_only" {
		t.Fatal("metadata")
	}
	os.Chmod(p, 0644)
	if credentialStatus(p) != "insecure" {
		t.Fatal("mode not rejected")
	}
	os.Remove(p)
	os.Symlink("/dev/null", p)
	if credentialStatus(p) != "insecure" {
		t.Fatal("symlink not rejected")
	}
}
func TestDisposableBrokerHealth(t *testing.T) {
	cmd := exec.Command("sleep", "30")
	if e := cmd.Start(); e != nil {
		t.Fatal(e)
	}
	defer func() { cmd.Process.Kill(); cmd.Wait() }()
	id, ok := identity(cmd.Process.Pid)
	if !ok {
		t.Fatal("fixture process identity")
	}
	q := privateTemp(t)
	s := BrokerStatus{PID: id.PID, Identity: id, Ready: true, UpdatedAt: time.Now().UnixMilli(), Backend: "playwright_headed_chromium", PlaywrightVersion: "1.63.0", Sandbox: true}
	put(t, filepath.Join(q, "status.json"), jsonBytes(s))
	if health("", q, "", "")["broker"] != "fresh_headed_process_identity_verified" {
		t.Fatal("fixture not recognized")
	}
	s.Identity.StartTicks = "bad"
	put(t, filepath.Join(q, "status.json"), jsonBytes(s))
	if health("", q, "", "")["broker"] != "stale_or_identity_mismatch" {
		t.Fatal("PID reuse accepted")
	}
}
func TestWrongSnapshotBinding(t *testing.T) {
	s := stateFixture()
	p := filepath.Join(privateTemp(t), "snapshot.json")
	b := jsonBytes(s)
	put(t, p, b)
	if _, e := readSnapshot(p, digest(b), strings.Repeat("b", 64)); e == nil {
		t.Fatal("wrong source binding accepted")
	}
	if _, e := readSnapshot(p, strings.Repeat("c", 64), s.ManifestSHA); e == nil {
		t.Fatal("wrong snapshot hash accepted")
	}
}
func TestActivationMissingRootBlocked(t *testing.T) {
	s := stateFixture()
	if _, e := activationEnvironment(filepath.Join(privateTemp(t), "missing"), "/missing", digest(jsonBytes(s)), s.ManifestSHA, "gateway"); e == nil {
		t.Fatal("missing state root activated")
	}
}
func TestConfiguredSourceAndPositiveActivation(t *testing.T) {
	dir := privateTemp(t)
	files := map[string][]byte{
		"insane-search-migration/internal/reporting/recovery.go":     []byte(`package reporting; const restoredGatewayRoot = "/opt/assistant-recovery/UNCONFIGURED"; var t=Target{BotID: "100000000000000001", GuildID: "100000000000000002", ChannelID: "100000000000000003"}`),
		"insane-search-migration/internal/reporting/rest.go":         []byte(`package reporting; var t=Target{BotID: "100000000000000001", GuildID: "100000000000000002", ChannelID: "100000000000000003"}`),
		"insane-search-migration/internal/reporting/config.go":       []byte("package reporting\nconst TokenPath = \"/opt/assistant-shared/.discord-private/bot-token\"\nconst DefaultProxyPath = \"/opt/assistant-project/gateway_native_proxy_config.json\"\n"),
		"insane-search-migration/runtime/browser/native_service.cjs": []byte("// synthetic inert browser script"),
	}
	m := Manifest{Schema: 1}
	for k, v := range files {
		if e := writeFile(dir, k, v); e != nil {
			t.Fatal(e)
		}
		m.Files = append(m.Files, FileEntry{k, int64(len(v)), digest(v)})
	}
	raw := jsonBytes(m)
	put(t, filepath.Join(dir, "SOURCE_MANIFEST.json"), raw)
	s, e := loadSource(dir, "", digest(raw))
	if e != nil {
		t.Fatal(e)
	}
	state := stateFixture()
	state.ManifestSHA = digest(raw)
	root := filepath.Join(privateTemp(t), "state")
	if e = restoreState(state, root, -1); e != nil {
		t.Fatal(e)
	}
	dest := filepath.Join(privateTemp(t), "source")
	sp := digest(jsonBytes(state))
	if e = configureSource(s, state, dest, root, sp); e != nil {
		t.Fatal(e)
	}
	for _, kind := range []string{"gateway", "headed"} {
		if _, e = activationEnvironment(root, dest, sp, state.ManifestSHA, kind); e == nil {
			t.Fatal("block bypassed")
		}
	}
	// Fixture marker is synthetic authorization. No real process is ever launched.
	os.Remove(filepath.Join(root, "RECOVERY_BLOCK.json"))
	gateway := []byte("synthetic inert binary")
	writeFile(dest, "discord-go-gateway/bin/dot-gateway", gateway)
	writeFile(root, "secrets/bot-token", []byte("synthetic-never-read"))
	writeFile(root, "proxy.json", jsonBytes(Proxy{"http://proxy.invalid:8080", "localhost,127.0.0.1,::1"}))
	a := Activation{1, sp, state.ManifestSHA, digest(gateway), digest(files["insane-search-migration/runtime/browser/native_service.cjs"]), state.Created, state.Created, true, true, true}
	writeFile(root, "ACTIVATION.json", jsonBytes(a))
	env, e := activationEnvironment(root, dest, sp, state.ManifestSHA, "gateway")
	if e != nil {
		t.Fatal(e)
	}
	if !strings.Contains(env, "unset DISCORD_BOT_TOKEN") || !strings.Contains(env, "export BRIDGE_HTTPS_PROXY='http://proxy.invalid:8080'") {
		t.Fatal("explicit credential/proxy environment not applied")
	}

	// Gateway startup uses actual runtime configuration, not recovery attestations.
	os.Remove(filepath.Join(root, "ACTIVATION.json"))
	writeFile(root, "RECOVERY_BLOCK.json", []byte("{}"))
	if _, e = activationEnvironment(root, dest, "", "", "gateway"); e != nil {
		t.Fatal(e)
	}
	if _, e = activationEnvironment(root, dest, sp, state.ManifestSHA, "headed"); e == nil {
		t.Fatal("unrelated headed gate changed")
	}
	var blocked int
	db, e := dbRO(filepath.Join(root, "bridge/bridge.sqlite3"))
	if e != nil {
		t.Fatal(e)
	}
	if e = db.QueryRow("SELECT count(*) FROM inbound WHERE state='blocked'").Scan(&blocked); e != nil || blocked != len(state.Events)+len(state.Ingress) {
		t.Fatal("historical event fences changed")
	}
	db.Close()
	os.Remove(filepath.Join(root, "bridge/bridge.sqlite3"))
	if _, e = activationEnvironment(root, dest, sp, state.ManifestSHA, "gateway"); e == nil {
		t.Fatal("missing bridge DB activated")
	}
}
func TestReportSnapshotRequiresPinnedTarget(t *testing.T) {
	dir := privateTemp(t)
	s := stateFixture()
	s.Reports = nil
	if e := snapshotReports(dir, &s); e == nil {
		t.Fatal("wrong parent accepted as empty outbox")
	}
	put(t, filepath.Join(dir, "target.json"), jsonBytes(Target{s.Operation.BotID, s.Operation.GuildID, s.Operation.ReportChannelID}))
	if e := snapshotReports(dir, &s); e != nil {
		t.Fatal(e)
	}
	put(t, filepath.Join(dir, "target.json"), jsonBytes(Target{"1002", "1003", "9999"}))
	if e := snapshotReports(dir, &s); e == nil {
		t.Fatal("wrong pinned target accepted")
	}
}
