package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"dot-gateway/internal/bridge"
)

func TestCLIProcessHelper(t *testing.T) {
	if os.Getenv("DOT_CLI_TEST_HELPER") != "1" {
		return
	}
	for i, a := range os.Args {
		if a == "--" {
			os.Exit(run(os.Args[i+1:]))
		}
	}
	os.Exit(99)
}

func TestCLIGatewayRuntimeExitCodes(t *testing.T) {
	dir := t.TempDir()
	os.Chmod(dir, 0700)
	for k, v := range map[string]string{"DISCORD_OWNER_ID": "1", "DISCORD_ALLOWED_DM_IDS": "1", "DISCORD_EXPECTED_BOT_ID": "4", "DISCORD_BOT_TOKEN": "offline", "DISCORD_BOT_TOKEN_FILE": "", "DISCORD_GUILD_ID": "", "DISCORD_GUILD_CHANNEL_ID": "", "DISCORD_GUILD_MODE": "mention", "DISCORD_ENABLE_MESSAGE_CONTENT": "false", "BRIDGE_DB": dir + "/runtime.db", "BRIDGE_HTTPS_PROXY": "http://127.0.0.1:8080", "BRIDGE_NO_PROXY": ""} {
		t.Setenv(k, v)
	}
	original := runGateway
	defer func() { runGateway = original }()
	for _, tt := range []struct {
		code string
		exit int
	}{{"gateway_connection_failed", 1}, {"gateway_startup_timeout", 1}, {"authentication_failed", 2}, {"configured_channel_permissions_missing", 2}, {"bot_identity_mismatch", 2}} {
		t.Run(tt.code, func(t *testing.T) {
			runGateway = func(context.Context, bridge.Settings, *bridge.Store) error { return errors.New(tt.code) }
			if got := run([]string{"gateway"}); got != tt.exit {
				t.Fatalf("exit %d want %d", got, tt.exit)
			}
		})
	}
}
func cli(t *testing.T, env []string, input string, args ...string) (map[string]any, error) {
	t.Helper()
	exe, e := os.Executable()
	if e != nil {
		t.Fatal(e)
	}
	cmd := exec.Command(exe, append([]string{"-test.run=^TestCLIProcessHelper$", "--"}, args...)...)
	cmd.Env = append(env, "DOT_CLI_TEST_HELPER=1", "GORACE=atexit_sleep_ms=0")
	cmd.Stdin = strings.NewReader(input)
	out, e := cmd.Output()
	var value map[string]any
	if err := json.Unmarshal(out, &value); err != nil {
		t.Fatalf("invalid JSON %q %v", out, err)
	}
	return value, e
}
func cliFixture(t *testing.T) (*bridge.Store, []string) {
	t.Helper()
	dir := t.TempDir()
	os.Chmod(dir, 0700)
	env := []string{"PATH=" + os.Getenv("PATH"), "DISCORD_OWNER_ID=1", "DISCORD_ALLOWED_DM_IDS=1", "DISCORD_EXPECTED_BOT_ID=4", "BRIDGE_DB=" + dir + "/queue.db"}
	s, e := bridge.OpenStore(dir+"/queue.db", bridge.Policy{OwnerID: "1", AllowedDMIDs: []string{"1"}, Platform: "discord", GuildMode: "mention"})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { s.Close() })
	return s, env
}
func TestCLIClaimsRepliesAndIdempotency(t *testing.T) {
	s, env := cliFixture(t)
	_, e := s.Ingest(bridge.Envelope{Platform: "discord", EventID: "3", ConversationID: "2", SenderID: "1", Text: "hello", ReceivedAt: float64(time.Now().Unix()), RouteKind: "dm"})
	if e != nil {
		t.Fatal(e)
	}
	r, e := cli(t, env, "", "next", "--begin")
	if e != nil {
		t.Fatal(r, e)
	}
	m := r["message"].(map[string]any)
	id, claim := m["inbound_id"].(string), m["claim"].(string)
	rows, e := s.FeedbackRows()
	if e != nil || len(rows) != 1 || !rows[0].Thinking {
		t.Fatal("begin was not atomic")
	}
	r, e = cli(t, env, "real answer", "reply", id, "--claim", claim)
	if e != nil || r["state"] != "queued" {
		t.Fatal(r, e)
	}
	replyID := r["reply_id"]
	r, e = cli(t, env, "real answer", "reply", id, "--claim", "stale")
	if e != nil || r["reply_id"] != replyID {
		t.Fatal("idempotency lost", r, e)
	}
}
func TestCLIRejectsInvalidWaitAndBegin(t *testing.T) {
	_, env := cliFixture(t)
	for _, args := range [][]string{{"next", "--wait", "NaN"}, {"next", "--wait", "Inf"}, {"next", "--begin", "--processing-seconds", "0"}, {"next", "--begin", "--processing-seconds", "301"}, {"next", "--unknown", "1"}, {"next", "--wait", "0", "--wait", "1"}} {
		r, e := cli(t, env, "", args...)
		if e == nil || r["error"] == nil {
			t.Fatal(args, r, e)
		}
	}
}
func TestCLINextWakesViaPrivateFile(t *testing.T) {
	s, env := cliFixture(t)
	var path string
	for _, e := range env {
		if strings.HasPrefix(e, "BRIDGE_DB=") {
			path = strings.TrimPrefix(e, "BRIDGE_DB=")
		}
	}
	done := make(chan map[string]any, 1)
	go func() {
		r, e := cli(t, env, "", "next", "--wait", "3")
		if e != nil {
			r["test_error"] = true
		}
		done <- r
	}()
	time.Sleep(100 * time.Millisecond)
	s.Ingest(bridge.Envelope{Platform: "discord", EventID: "3", ConversationID: "2", SenderID: "1", Text: "hello", ReceivedAt: float64(time.Now().Unix()), RouteKind: "dm"})
	start := time.Now()
	bridge.NotifyFile(path + ".sock.wake")
	select {
	case r := <-done:
		if r["message"] == nil || r["test_error"] != nil {
			t.Fatal(r)
		}
		if time.Since(start) > time.Second {
			t.Fatal("file notification delayed")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("next failed to wake")
	}
}
func TestCLICheckNeverNeedsTokenOrNetwork(t *testing.T) {
	_, env := cliFixture(t)
	r, e := cli(t, append(env, "DISCORD_BOT_TOKEN_FILE=/does/not/exist"), "", "check")
	if e != nil || r["token_checked"] != false || r["network_used"] != false {
		t.Fatal(r, e)
	}
}

func TestCLIMaterializeIsExplicitLiveCommand(t *testing.T) {
	_, env := cliFixture(t)
	// No inherited environment or real credential. Offline next succeeds without
	// a token; explicit materialize refuses before any network operation.
	response, err := cli(t, env, "", "next")
	if err != nil {
		t.Fatal(response, err)
	}
	response, err = cli(t, env, "", "materialize", "inbound", "--claim", "claim", "--attachment", "7")
	if err == nil || response["error"] != "secure_bot_token_required" {
		t.Fatal(response, err)
	}
	response, err = cli(t, env, "", "materialize", "inbound", "--claim", "claim", "--url", "https://example.org/file")
	if err == nil || response["error"] != "unknown_flag" {
		t.Fatal(response, err)
	}
}
