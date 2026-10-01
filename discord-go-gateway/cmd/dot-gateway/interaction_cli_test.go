package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCLICommandRegistrationOfflineDiff(t *testing.T) {
	_, env := cliFixture(t)
	env = append(env, "DISCORD_GUILD_ID=5", "DISCORD_GUILD_CHANNEL_ID=6")
	snapshot := filepath.Join(t.TempDir(), "commands.json")
	if err := os.WriteFile(snapshot, []byte(`{"application_id":"4","guild_id":"5","commands":[],"owned_ids":{}}`), 0600); err != nil {
		t.Fatal(err)
	}
	result, err := cli(t, env, "", "register-commands", "--dry-run", "--snapshot-file", snapshot)
	if err != nil || result["dry_run"] != true || result["network_used"] != false || len(result["changes"].([]any)) != 3 {
		t.Fatal(result, err)
	}
	result, err = cli(t, env, "", "register-commands", "--snapshot-file", snapshot)
	if err == nil || result["error"] != "only_explicit_dry_run_supported" {
		t.Fatal(result, err)
	}
}
