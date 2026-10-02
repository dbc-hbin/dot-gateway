package main

import (
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// Configured sources are PRIVATE derived work, never a public-source replacement.
// Exact source substitutions fail closed when upstream layouts change.
func configureSource(s Source, state Snapshot, dest, stateRoot, snapshotSHA string) error {
	if e := validateSnapshot(state); e != nil {
		return e
	}
	if state.ManifestSHA != digest(s.Raw) || snapshotSHA != digest(jsonBytes(state)) {
		return errors.New("source/private snapshot binding mismatch")
	}
	if !filepath.IsAbs(stateRoot) || filepath.Clean(stateRoot) != stateRoot || stateRoot == "/" || strings.ContainsAny(stateRoot, "\n\r\x00") {
		return errors.New("absolute clean state root required")
	}
	return atomicDir(dest, func(stage string) error {
		derived := map[string][]byte{}
		for k, v := range s.Files {
			derived[k] = v
		}
		replace := func(file, old, new string) error {
			v, ok := derived[file]
			if !ok || strings.Count(string(v), old) != 1 {
				return errors.New("reviewed source substitution no longer matches")
			}
			derived[file] = []byte(strings.Replace(string(v), old, new, 1))
			return nil
		}
		o := state.Operation
		// The reporter checks this root even when --state-dir names an unrelated
		// empty directory. These values are source-derived, never environment or
		// command-line overrides in the live publisher.
		if e := replace("insane-search-migration/internal/reporting/recovery.go", `const restoredGatewayRoot = "/opt/assistant-recovery/UNCONFIGURED"`, "const restoredGatewayRoot = "+strconv.Quote(stateRoot)); e != nil {
			return e
		}
		if e := replace("insane-search-migration/internal/reporting/recovery.go", `Target{BotID: "100000000000000001", GuildID: "100000000000000002", ChannelID: "100000000000000003"}`, fmt.Sprintf("Target{BotID: %s, GuildID: %s, ChannelID: %s}", strconv.Quote(o.BotID), strconv.Quote(o.GuildID), strconv.Quote(o.ReportChannelID))); e != nil {
			return e
		}
		if e := replace("insane-search-migration/internal/reporting/rest.go", `Target{BotID: "100000000000000001", GuildID: "100000000000000002", ChannelID: "100000000000000003"}`, fmt.Sprintf("Target{BotID: %s, GuildID: %s, ChannelID: %s}", strconv.Quote(o.BotID), strconv.Quote(o.GuildID), strconv.Quote(o.ReportChannelID))); e != nil {
			return e
		}
		if e := replace("insane-search-migration/internal/reporting/config.go", `const TokenPath = "/opt/assistant-shared/.discord-private/bot-token"`, "const TokenPath = "+strconv.Quote(rootJoin(stateRoot, "secrets/bot-token"))); e != nil {
			return e
		}
		if e := replace("insane-search-migration/internal/reporting/config.go", `const DefaultProxyPath = "/opt/assistant-project/gateway_native_proxy_config.json"`, "const DefaultProxyPath = "+strconv.Quote(rootJoin(stateRoot, "proxy.json"))); e != nil {
			return e
		}
		derived["insane-search-migration/config/discord-report.json"] = jsonBytes(Target{o.BotID, o.GuildID, o.ReportChannelID})
		// Gateway launch validates runtime inputs; headed launch retains recovery gates.
		// Launchers are generated
		// as mode 0600 text; explicit bash invocation works after authorized review.
		gatewayEnv := map[string]string{"DISCORD_OWNER_ID": o.OwnerID, "DISCORD_ALLOWED_DM_IDS": o.OwnerID, "DISCORD_EXPECTED_BOT_ID": o.BotID, "DISCORD_GUILD_ID": o.GuildID, "DISCORD_GUILD_CHANNEL_ID": o.GatewayChannelID, "DISCORD_GUILD_MODE": o.GuildMode, "DISCORD_ENABLE_MESSAGE_CONTENT": strconv.FormatBool(o.MessageContent), "BRIDGE_DB": rootJoin(stateRoot, "bridge/bridge.sqlite3"), "DISCORD_BOT_TOKEN_FILE": rootJoin(stateRoot, "secrets/bot-token")}
		// Do not use eval directly on a failing substitution: capture first so set -e fails closed.
		check := func(component string) string {
			if component == "gateway" {
				return "RECOVERY_ENV=$(" + shQuote(rootJoin(dest, "recovery/dot-recovery")) + " activation-env --state-root " + shQuote(stateRoot) + " --component gateway)\n" + "eval \"$RECOVERY_ENV\"\n"
			}
			return "RECOVERY_ENV=$(" + shQuote(rootJoin(dest, "recovery/dot-recovery")) + " activation-env --state-root " + shQuote(stateRoot) + " --source " + shQuote(dest) + " --snapshot-sha " + shQuote(snapshotSHA) + " --manifest-sha " + shQuote(state.ManifestSHA) + " --component " + component + ")\n" + "eval \"$RECOVERY_ENV\"\n"
		}
		script := "#!/bin/sh\nset -eu\n" + check("gateway")
		keys := make([]string, 0, len(gatewayEnv))
		for k := range gatewayEnv {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			script += "export " + k + "=" + shQuote(gatewayEnv[k]) + "\n"
		}
		script += "exec " + shQuote(rootJoin(dest, "discord-go-gateway/bin/dot-gateway")) + " gateway\n"
		derived["deployment/launch-gateway.sh"] = []byte(script)
		browser := "#!/bin/sh\nset -eu\n" + check("headed") + "[ -n \"${DISPLAY:-}${WAYLAND_DISPLAY:-}\" ] || { echo 'Real graphical desktop required' >&2; exit 1; }\nexport INSANE_BROWSER_QUEUE=" + shQuote(rootJoin(stateRoot, "browser-queue")) + "\nexport INSANE_BROWSER_PROXY_CONFIG=" + shQuote(rootJoin(stateRoot, "proxy.json")) + "\nexec node " + shQuote(rootJoin(dest, "insane-search-migration/runtime/browser/native_service.cjs")) + "\n"
		derived["deployment/launch-headed.sh"] = []byte(browser)
		// Retain provenance, and supply a separate manifest over derived bytes.
		if e := writeFile(stage, "BASE_SOURCE_MANIFEST.json", s.Raw); e != nil {
			return e
		}
		var files []FileEntry
		names := make([]string, 0, len(derived))
		for k := range derived {
			names = append(names, k)
		}
		sort.Strings(names)
		for _, k := range names {
			v := derived[k]
			if e := writeFile(stage, k, v); e != nil {
				return e
			}
			files = append(files, FileEntry{k, int64(len(v)), digest(v)})
		}
		if e := writeFile(stage, "SOURCE_MANIFEST.json", jsonBytes(Manifest{1, files})); e != nil {
			return e
		}
		return writeFile(stage, "PRIVATE_DERIVATION.json", jsonBytes(map[string]any{"schema": 1, "base_manifest_sha256": digest(s.Raw), "private_snapshot_sha256": snapshotSHA, "delivery_enabled": false, "changes": []string{"exact approved report target", "compile-time recovery root with matching reports fence required", "relocated credential and proxy path references", "nonsecret launch scripts with recovery block"}}))
	})
}
func shQuote(s string) string { return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'" }
func readSnapshot(file, pin, manifest string) (Snapshot, error) {
	var s Snapshot
	b, e := readFile(file, true, 64<<20)
	if e != nil {
		return s, e
	}
	if !hashPattern.MatchString(pin) || digest(b) != pin {
		return s, errors.New("trusted snapshot SHA mismatch")
	}
	if strict(b, &s) != nil {
		return s, errors.New("invalid snapshot JSON")
	}
	if e = validateSnapshot(s); e != nil {
		return s, e
	}
	if manifest == "" || s.ManifestSHA != manifest {
		return s, errors.New("source/snapshot manifest binding mismatch")
	}
	return s, nil
}
