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

func activationEnvironment(stateRoot, sourceRoot, snapshotSHA, manifestSHA, component string) (string, error) {
	if component != "gateway" && component != "headed" {
		return "", errors.New("component must be gateway or headed")
	}
	if !hashPattern.MatchString(snapshotSHA) || !hashPattern.MatchString(manifestSHA) {
		return "", errors.New("activation binding required")
	}
	if _, e := os.Lstat(filepath.Join(stateRoot, "RECOVERY_BLOCK.json")); !os.IsNotExist(e) {
		return "", errors.New("recovery block remains or cannot be checked")
	}
	b, e := readFile(filepath.Join(stateRoot, "ACTIVATION.json"), true, 16<<10)
	if e != nil {
		return "", errors.New("explicit private activation record missing")
	}
	var a Activation
	if strict(b, &a) != nil || a.Schema != 1 || a.SnapshotSHA != snapshotSHA || a.ManifestSHA != manifestSHA || !stampOK(a.AuthorizedAt) || !stampOK(a.HistoryVerifiedAt) || !a.IdentityVerified || !a.ConsumerVerified || !a.PreviousSenderStopped {
		return "", errors.New("activation record invalid or not fully verified")
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
	var n int
	if e = db.QueryRow("SELECT count(*) FROM inbound").Scan(&n); e != nil || n < r.Events {
		return "", errors.New("restored tombstones missing")
	}
	snapshot, e := readSnapshot(filepath.Join(stateRoot, "RESTORED_SNAPSHOT.json"), snapshotSHA, manifestSHA)
	if e != nil {
		return "", errors.New("restored snapshot binding missing")
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
	if credentialStatus(filepath.Join(stateRoot, "secrets/bot-token")) != "present_metadata_only" {
		return "", errors.New("credential missing or insecure")
	}
	b, e = readFile(filepath.Join(stateRoot, "proxy.json"), true, 16<<10)
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
