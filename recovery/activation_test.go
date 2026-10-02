package main

import (
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func activationFixture(t *testing.T, bootstrap bool) (root, source string, snapshot Snapshot, activation Activation) {
	t.Helper()
	snapshot = stateFixture()
	root = filepath.Join(privateTemp(t), "state")
	if err := restoreState(snapshot, root, -1); err != nil {
		t.Fatal(err)
	}
	source = privateTemp(t)
	gateway := []byte("synthetic inert gateway")
	broker := []byte("// synthetic inert broker")
	for _, file := range []struct {
		root, name string
		data       []byte
	}{
		{source, "discord-go-gateway/bin/dot-gateway", gateway},
		{source, "insane-search-migration/runtime/browser/native_service.cjs", broker},
		{root, "secrets/bot-token", []byte("synthetic-token-never-read")},
		{root, "proxy.json", jsonBytes(Proxy{"http://proxy.invalid:8080", "localhost"})},
	} {
		if err := writeFile(file.root, file.name, file.data); err != nil {
			t.Fatal(err)
		}
	}
	activation = Activation{Schema: 1, SnapshotSHA: digest(jsonBytes(snapshot)), ManifestSHA: snapshot.ManifestSHA,
		GatewaySHA: digest(gateway), BrokerSHA: digest(broker), AuthorizedAt: snapshot.Created, HistoryVerifiedAt: snapshot.Created,
		IdentityVerified: true, ConsumerVerified: !bootstrap, PreviousSenderStopped: true}
	marker := "ACTIVATION.json"
	if bootstrap {
		marker = "BOOTSTRAP_RECEIVE_ONLY.json"
	} else if err := os.Remove(filepath.Join(root, "RECOVERY_BLOCK.json")); err != nil {
		t.Fatal(err)
	}
	if err := writeFile(root, marker, jsonBytes(activation)); err != nil {
		t.Fatal(err)
	}
	return
}

func activationFixtureSQL(t *testing.T, root, query string) {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(root, "bridge/bridge.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err = db.Exec(query); err != nil {
		t.Fatal(err)
	}
}

func TestReceiveOnlyBootstrapIsSeparateFromActivation(t *testing.T) {
	root, source, snapshot, a := activationFixture(t, true)
	env, err := activationEnvironment(root, source, a.SnapshotSHA, snapshot.ManifestSHA, "gateway-receive-only")
	if err != nil || !strings.Contains(env, "export BRIDGE_RECEIVE_ONLY='true'\n") || !strings.Contains(env, "export BRIDGE_KEEP_CATCHUP_DISARMED='true'\n") {
		t.Fatalf("bootstrap gate: env=%q error=%v", env, err)
	}
	// Headed launch still requires completed recovery; normal gateway uses runtime inputs.
	full := a
	full.ConsumerVerified = true
	put(t, filepath.Join(root, "ACTIVATION.json"), jsonBytes(full))
	for _, component := range []string{"headed"} {
		if _, err := activationEnvironment(root, source, a.SnapshotSHA, snapshot.ManifestSHA, component); err == nil {
			t.Fatalf("bootstrap satisfied normal %s activation", component)
		}
	}
	if _, err := os.Stat(filepath.Join(root, "RECOVERY_BLOCK.json")); err != nil {
		t.Fatal("bootstrap removed the recovery block", err)
	}
}

func TestReceiveOnlyBootstrapRejectsInvalidMarkers(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*Activation, *receiveOnlyRecoveryBlock)
	}{
		{"consumer_claimed_verified", func(a *Activation, _ *receiveOnlyRecoveryBlock) { a.ConsumerVerified = true }},
		{"identity_unverified", func(a *Activation, _ *receiveOnlyRecoveryBlock) { a.IdentityVerified = false }},
		{"previous_sender_running", func(a *Activation, _ *receiveOnlyRecoveryBlock) { a.PreviousSenderStopped = false }},
		{"missing_authorization_time", func(a *Activation, _ *receiveOnlyRecoveryBlock) { a.AuthorizedAt = "" }},
		{"missing_history_verification", func(a *Activation, _ *receiveOnlyRecoveryBlock) { a.HistoryVerifiedAt = "" }},
		{"wrong_snapshot", func(a *Activation, _ *receiveOnlyRecoveryBlock) { a.SnapshotSHA = strings.Repeat("b", 64) }},
		{"wrong_manifest", func(a *Activation, _ *receiveOnlyRecoveryBlock) { a.ManifestSHA = strings.Repeat("b", 64) }},
		{"wrong_binary", func(a *Activation, _ *receiveOnlyRecoveryBlock) { a.GatewaySHA = strings.Repeat("b", 64) }},
		{"wrong_broker", func(a *Activation, _ *receiveOnlyRecoveryBlock) { a.BrokerSHA = strings.Repeat("b", 64) }},
		{"block_deliverable", func(_ *Activation, b *receiveOnlyRecoveryBlock) { b.DeliveryEnabled = true }},
		{"block_consumer_ready", func(_ *Activation, b *receiveOnlyRecoveryBlock) { b.ConsumerReady = true }},
		{"block_claims_history", func(_ *Activation, b *receiveOnlyRecoveryBlock) { b.HistoryContentRestored = true }},
		{"block_wrong_manifest", func(_ *Activation, b *receiveOnlyRecoveryBlock) { b.ManifestSHA = strings.Repeat("b", 64) }},
		{"block_wrong_snapshot_time", func(_ *Activation, b *receiveOnlyRecoveryBlock) { b.SnapshotCreated = "2026-09-30T00:00:00Z" }},
		{"block_empty_requirement", func(_ *Activation, b *receiveOnlyRecoveryBlock) { b.Requires = "" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root, source, snapshot, a := activationFixture(t, true)
			pin := a.SnapshotSHA
			block := receiveOnlyRecoveryBlock{Schema: 1, ManifestSHA: snapshot.ManifestSHA, SnapshotCreated: snapshot.Created, Requires: "synthetic recovery review"}
			tc.mutate(&a, &block)
			put(t, filepath.Join(root, "BOOTSTRAP_RECEIVE_ONLY.json"), jsonBytes(a))
			put(t, filepath.Join(root, "RECOVERY_BLOCK.json"), jsonBytes(block))
			if _, err := activationEnvironment(root, source, pin, snapshot.ManifestSHA, "gateway-receive-only"); err == nil {
				t.Fatal("invalid bootstrap accepted")
			}
		})
	}
	for _, file := range []string{"RECOVERY_BLOCK.json", "BOOTSTRAP_RECEIVE_ONLY.json"} {
		for _, mode := range []string{"missing", "public", "symlink", "unknown_field", "trailing_json"} {
			t.Run(file+"/"+mode, func(t *testing.T) {
				root, source, snapshot, a := activationFixture(t, true)
				path := filepath.Join(root, file)
				switch mode {
				case "missing", "symlink":
					if err := os.Remove(path); err != nil {
						t.Fatal(err)
					}
					if mode == "symlink" {
						if err := os.Symlink("ACTIVATION.json", path); err != nil {
							t.Fatal(err)
						}
					}
				case "public":
					if err := os.Chmod(path, 0644); err != nil {
						t.Fatal(err)
					}
				case "unknown_field", "trailing_json":
					raw, err := os.ReadFile(path)
					if err != nil {
						t.Fatal(err)
					}
					if mode == "unknown_field" {
						raw = []byte(strings.Replace(string(raw), "{", `{"unknown":true,`, 1))
					} else {
						raw = append(raw, []byte("{}")...)
					}
					put(t, path, raw)
				}
				if _, err := activationEnvironment(root, source, a.SnapshotSHA, snapshot.ManifestSHA, "gateway-receive-only"); err == nil {
					t.Fatal("unsafe bootstrap marker accepted")
				}
			})
		}
	}
}

func TestReceiveOnlyBootstrapRequiresDisarmedCatchup(t *testing.T) {
	for _, tc := range []struct {
		name, sql string
		allowed   bool
	}{
		{"missing_state", `DELETE FROM catchup_meta`, false},
		{"armed_state", `UPDATE catchup_meta SET value='armed' WHERE key='state'`, false},
		{"other_disarm_reason", `UPDATE catchup_meta SET value='disarmed_continuity' WHERE key='state'`, false},
		{"idle_route", `CREATE TABLE catchup_routes(state TEXT); INSERT INTO catchup_routes VALUES('idle')`, false},
		{"gap_route", `CREATE TABLE catchup_routes(state TEXT); INSERT INTO catchup_routes VALUES('gap')`, false},
		{"pending_request", `CREATE TABLE catchup_requested(channel_id TEXT); INSERT INTO catchup_requested VALUES('2')`, false},
		{"disarmed_route", `CREATE TABLE catchup_routes(state TEXT); INSERT INTO catchup_routes VALUES('disarmed'); CREATE TABLE catchup_requested(channel_id TEXT)`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root, source, snapshot, a := activationFixture(t, true)
			activationFixtureSQL(t, root, tc.sql)
			_, err := activationEnvironment(root, source, a.SnapshotSHA, snapshot.ManifestSHA, "gateway-receive-only")
			if (err == nil) != tc.allowed {
				t.Fatalf("allowed=%v error=%v", tc.allowed, err)
			}
		})
	}
}

func TestActivationRequiresSnapshotBoundOperation(t *testing.T) {
	for _, component := range []string{"headed", "gateway-receive-only"} {
		for _, mutation := range []string{"missing", "malformed", "owner", "bot", "guild", "gateway_channel", "report_channel", "mode", "intent"} {
			t.Run(component+"/"+mutation, func(t *testing.T) {
				root, source, snapshot, a := activationFixture(t, component == "gateway-receive-only")
				if _, err := activationEnvironment(root, source, a.SnapshotSHA, snapshot.ManifestSHA, component); err != nil {
					t.Fatal("valid fixture rejected", err)
				}
				operation := snapshot.Operation
				path := filepath.Join(root, "operation.json")
				switch mutation {
				case "missing":
					if err := os.Remove(path); err != nil {
						t.Fatal(err)
					}
				case "malformed":
					put(t, path, []byte("{}"))
				case "owner":
					operation.OwnerID = "9991"
				case "bot":
					operation.BotID = "9992"
				case "guild":
					operation.GuildID = "9993"
				case "gateway_channel":
					operation.GatewayChannelID = "9994"
				case "report_channel":
					operation.ReportChannelID = "9995"
				case "mode":
					operation.GuildMode = "mention"
				case "intent":
					operation.MessageContent = false
				}
				if mutation != "missing" && mutation != "malformed" {
					put(t, path, jsonBytes(operation))
				}
				if _, err := activationEnvironment(root, source, a.SnapshotSHA, snapshot.ManifestSHA, component); err == nil || err.Error() != "restored operation binding mismatch" {
					t.Fatalf("operation replacement accepted: %v", err)
				}
			})
		}
	}
}
