package main

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

type Activation struct {
	Schema                int    `json:"schema"`
	SnapshotSHA           string `json:"snapshot_sha256"`
	ManifestSHA           string `json:"source_manifest_sha256"`
	GatewaySHA            string `json:"gateway_binary_sha256"`
	BrokerSHA             string `json:"broker_script_sha256"`
	AuthorizedAt          string `json:"authorized_at"`
	HistoryVerifiedAt     string `json:"latest_history_verified_at"`
	IdentityVerified      bool   `json:"discord_identity_verified"`
	ConsumerVerified      bool   `json:"assistant_consumer_verified"`
	PreviousSenderStopped bool   `json:"previous_sender_stopped"`
}
type RecoveryState struct {
	Schema      int    `json:"schema"`
	SnapshotSHA string `json:"snapshot_sha256"`
	ManifestSHA string `json:"source_manifest_sha256"`
	Events      int    `json:"event_tombstones"`
}
type Proxy struct {
	URL     string `json:"BRIDGE_HTTPS_PROXY"`
	NoProxy string `json:"BRIDGE_NO_PROXY"`
}

// A bootstrap is intentionally separate from completed activation. The marker
// uses Activation's schema, but must explicitly retain an unverified consumer.
type receiveOnlyRecoveryBlock struct {
	Schema                 int    `json:"schema"`
	ManifestSHA            string `json:"source_manifest_sha256"`
	SnapshotCreated        string `json:"snapshot_created_at"`
	DeliveryEnabled        bool   `json:"delivery_enabled"`
	Requires               string `json:"requires"`
	ConsumerReady          bool   `json:"consumer_ready"`
	HistoryContentRestored bool   `json:"history_content_restored"`
}

func activationEnvironment(stateRoot, sourceRoot, snapshotSHA, manifestSHA, component string) (string, error) {
	// Normal gateway startup validates runtime inputs, not manual attestations.
	// The transport still checks expected identity and exact configured routes.
	if component == "gateway" {
		return gatewayRuntimeEnvironment(stateRoot)
	}
	bootstrap := component == "gateway-receive-only"
	if component != "gateway" && component != "headed" && !bootstrap {
		return "", errors.New("component must be gateway, gateway-receive-only or headed")
	}
	if !hashPattern.MatchString(snapshotSHA) || !hashPattern.MatchString(manifestSHA) {
		return "", errors.New("activation binding required")
	}
	marker := "ACTIVATION.json"
	var block receiveOnlyRecoveryBlock
	if bootstrap {
		marker = "BOOTSTRAP_RECEIVE_ONLY.json"
		b, e := readFile(filepath.Join(stateRoot, "RECOVERY_BLOCK.json"), true, 16<<10)
		if e != nil || strict(b, &block) != nil || block.Schema != 1 || block.ManifestSHA != manifestSHA || !stampOK(block.SnapshotCreated) || block.DeliveryEnabled || block.ConsumerReady || block.HistoryContentRestored || block.Requires == "" {
			return "", errors.New("receive-only bootstrap requires intact private recovery block")
		}
	} else if _, e := os.Lstat(filepath.Join(stateRoot, "RECOVERY_BLOCK.json")); !os.IsNotExist(e) {
		return "", errors.New("recovery block remains or cannot be checked")
	}
	b, e := readFile(filepath.Join(stateRoot, marker), true, 16<<10)
	if e != nil {
		return "", errors.New("explicit private activation or bootstrap record missing")
	}
	var a Activation
	if strict(b, &a) != nil || a.Schema != 1 || a.SnapshotSHA != snapshotSHA || a.ManifestSHA != manifestSHA || !stampOK(a.AuthorizedAt) || !stampOK(a.HistoryVerifiedAt) || !a.IdentityVerified || !a.PreviousSenderStopped || (!bootstrap && !a.ConsumerVerified) || (bootstrap && a.ConsumerVerified) {
		return "", errors.New("activation record invalid or not fully verified for requested mode")
	}
	b, e = readFile(filepath.Join(stateRoot, "RECOVERY_STATE.json"), true, 16<<10)
	if e != nil {
		return "", e
	}
	var r RecoveryState
	if strict(b, &r) != nil || r.Schema != 1 || r.SnapshotSHA != snapshotSHA || r.ManifestSHA != manifestSHA || r.Events < 0 {
		return "", errors.New("restored state provenance mismatch")
	}
	db, e := dbRO(filepath.Join(stateRoot, "bridge/bridge.sqlite3"))
	if e != nil {
		return "", errors.New("restored bridge database missing or unsafe")
	}
	defer db.Close()
	if bootstrap {
		var state string
		if db.QueryRow(`SELECT value FROM catchup_meta WHERE key='state'`).Scan(&state) != nil || state != "disarmed_restore" {
			return "", errors.New("receive-only bootstrap requires restored disarmed catchup")
		}
		for _, table := range []string{"catchup_routes", "catchup_requested"} {
			var present, runnable int
			if db.QueryRow(`SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?`, table).Scan(&present) != nil {
				return "", errors.New("receive-only catchup state cannot be checked")
			}
			if present == 0 {
				continue
			}
			query := `SELECT count(*) FROM ` + table
			if table == "catchup_routes" {
				query += ` WHERE state!='disarmed'`
			}
			if db.QueryRow(query).Scan(&runnable) != nil || runnable != 0 {
				return "", errors.New("receive-only bootstrap has runnable catchup state")
			}
		}
	}
	var n int
	if e = db.QueryRow("SELECT count(*) FROM inbound").Scan(&n); e != nil || n < r.Events {
		return "", errors.New("restored tombstones missing")
	}
	snapshot, e := readSnapshot(filepath.Join(stateRoot, "RESTORED_SNAPSHOT.json"), snapshotSHA, manifestSHA)
	if e != nil {
		return "", errors.New("restored snapshot binding missing")
	}
	if bootstrap && block.SnapshotCreated != snapshot.Created {
		return "", errors.New("receive-only recovery block snapshot mismatch")
	}
	// Launchers load the current operation file after this gate. Bind every
	// routing and intent setting to the reviewed snapshot, not an ambient scope.
	b, e = readFile(filepath.Join(stateRoot, "operation.json"), true, 16<<10)
	var operation Operation
	if e != nil || strict(b, &operation) != nil || operation != snapshot.Operation {
		return "", errors.New("restored operation binding mismatch")
	}
	for _, v := range append(append([]Event{}, snapshot.Events...), snapshot.Ingress...) {
		var state string
		var id string
		if db.QueryRow("SELECT id,state FROM inbound WHERE platform=? AND event_id=?", v.Platform, v.EventID).Scan(&id, &state) != nil || id != v.ID || state != "blocked" {
			return "", errors.New("historical event tombstone missing or replayable")
		}
	}
	for _, v := range snapshot.Chunks {
		var state string
		if db.QueryRow("SELECT state FROM chunks WHERE reply_id=? AND idx=?", v.ReplyID, v.Index).Scan(&state) != nil || (state != "sent" && state != "uncertain") {
			return "", errors.New("historical chunk missing or replayable")
		}
	}
	for _, v := range snapshot.Diagnostics {
		var state string
		var attempts int
		if db.QueryRow("SELECT state,attempts FROM go_transport_diagnostics WHERE id=? AND idx=?", v.ID, v.Index).Scan(&state, &attempts) != nil || attempts != 1 || (state != "sent" && state != "uncertain") {
			return "", errors.New("historical diagnostic missing or replayable")
		}
	}
	for _, v := range snapshot.TestSends {
		var state string
		var attempts int
		if db.QueryRow("SELECT state,attempted FROM test_sends WHERE bot_id=? AND guild_id=? AND channel_id=?", v.BotID, v.GuildID, v.ChannelID).Scan(&state, &attempts) != nil || attempts != 1 || (state != "verified" && state != "uncertain") {
			return "", errors.New("historical test-send missing or replayable")
		}
	}
	for _, v := range snapshot.Reports {
		b, e := readFile(filepath.Join(stateRoot, "reports/run-"+digest([]byte(v.RunID))+".json"), true, 256<<10)
		if e != nil {
			return "", errors.New("historical report ledger missing")
		}
		var actual Receipt
		if strict(b, &actual) != nil {
			return "", errors.New("invalid restored ledger")
		}
		check := snapshot
		check.Reports = []Receipt{actual}
		if validateSnapshot(check) != nil || actual.RunID != v.RunID || actual.PayloadHash != v.PayloadHash || actual.Target != v.Target || len(actual.Chunks) != len(v.Chunks) {
			return "", errors.New("historical report binding mismatch")
		}
		for i, c := range actual.Chunks {
			if c.ContentHash != v.Chunks[i].ContentHash || c.Nonce != v.Chunks[i].Nonce || (c.State != "confirmed" && c.State != "uncertain") || v.Chunks[i].State == "confirmed" && c != v.Chunks[i] {
				return "", errors.New("historical report is replayable or regressed")
			}
		}
	}

	// Both launch paths verify the existing gateway binary and broker script. An
	// activation marker cannot silently authorize a newly replaced binary.
	gateway, e := readFile(filepath.Join(sourceRoot, "discord-go-gateway/bin/dot-gateway"), false, 128<<20)
	if e != nil || digest(gateway) != a.GatewaySHA {
		return "", errors.New("reviewed gateway binary hash mismatch")
	}
	broker, e := readFile(filepath.Join(sourceRoot, "insane-search-migration/runtime/browser/native_service.cjs"), false, maxFile)
	if e != nil || digest(broker) != a.BrokerSHA {
		return "", errors.New("reviewed broker script hash mismatch")
	}
	env, e := runtimeEnvironment(stateRoot)
	if e != nil {
		return "", e
	}
	if bootstrap {
		env += "export BRIDGE_RECEIVE_ONLY='true'\nexport BRIDGE_KEEP_CATCHUP_DISARMED='true'\n"
	}
	return env, nil
}

func gatewayRuntimeEnvironment(stateRoot string) (string, error) {
	db, err := dbRO(filepath.Join(stateRoot, "bridge/bridge.sqlite3"))
	if err != nil {
		return "", errors.New("existing private bridge database required")
	}
	defer db.Close()
	var n int
	if err = db.QueryRow("SELECT count(*) FROM inbound").Scan(&n); err != nil {
		return "", errors.New("existing bridge schema required")
	}
	return runtimeEnvironment(stateRoot)
}

func runtimeEnvironment(stateRoot string) (string, error) {
	if credentialStatus(filepath.Join(stateRoot, "secrets/bot-token")) != "present_metadata_only" {
		return "", errors.New("credential missing or insecure")
	}
	b, e := readFile(filepath.Join(stateRoot, "proxy.json"), true, 16<<10)
	if e != nil {
		return "", errors.New("explicit private proxy config required")
	}
	var p Proxy
	if strict(b, &p) != nil {
		return "", errors.New("invalid proxy config")
	}
	// All three existing components require explicit credential-free proxy routing.
	if p.URL == "" {
		return "", errors.New("explicit approved proxy required")
	}
	if p.URL != "" {
		u, e := url.Parse(p.URL)
		if e != nil || u.Scheme != "http" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path != "" && u.Path != "/" {
			return "", errors.New("proxy must be credential-free HTTP URL")
		}
	}
	u, _ := url.Parse(p.URL)
	host := u.Hostname()
	if strings.Contains(host, "..") || strings.Contains(host, "%") || net.ParseIP(host) == nil && !regexp.MustCompile(`^[A-Za-z0-9](?:[A-Za-z0-9.-]*[A-Za-z0-9])?$`).MatchString(host) {
		return "", errors.New("invalid proxy host")
	}
	port := u.Port()
	if port == "" {
		port = "80"
	}
	pn, e := strconv.Atoi(port)
	if e != nil || pn < 1 || pn > 65535 {
		return "", errors.New("invalid proxy port")
	}
	for _, item := range strings.Split(p.NoProxy, ",") {
		v := strings.TrimPrefix(strings.ToLower(strings.TrimSpace(item)), ".")
		for _, host := range []string{"discord.com", "gateway.discord.gg"} {
			if v == "*" || host == v || host+":443" == v || strings.HasSuffix(host, "."+v) && v != "" {
				return "", errors.New("Discord proxy bypass forbidden")
			}
		}
	}

	for _, v := range []string{p.URL, p.NoProxy} {
		for _, c := range v {
			if c < 32 || c >= 127 {
				return "", errors.New("invalid proxy characters")
			}
		}
	}
	env := "unset DISCORD_BOT_TOKEN BRIDGE_HTTPS_PROXY BRIDGE_NO_PROXY http_proxy https_proxy all_proxy no_proxy HTTP_PROXY HTTPS_PROXY ALL_PROXY NO_PROXY\n"
	if p.URL != "" {
		env += "export BRIDGE_HTTPS_PROXY=" + shQuote(p.URL) + "\n"
		env += "export https_proxy=" + shQuote(p.URL) + "\n"
	}
	env += "export BRIDGE_NO_PROXY=" + shQuote(p.NoProxy) + "\nexport no_proxy=" + shQuote(p.NoProxy) + "\n"
	return env, nil
}
func printActivationEnv(root, source, snapshot, manifest, component string) error {
	env, e := activationEnvironment(root, source, snapshot, manifest, component)
	if e != nil {
		return e
	}
	_, e = fmt.Print(strings.TrimSpace(env) + "\n")
	return e
}
