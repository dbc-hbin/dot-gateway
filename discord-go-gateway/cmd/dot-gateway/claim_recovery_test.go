package main

import (
	"dot-gateway/internal/bridge"
	"io"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// A real child commits a claim while its successful stdout is deliberately
// discarded. This models a disconnected tool transport, including the case
// where the process cannot detect the lost result at all.
func TestCLIDroppedOutputRecoversImmediately(t *testing.T) {
	for _, named := range []bool{false, true} {
		t.Run(map[bool]string{false: "legacy_reproduces_delay", true: "stable_consumer_recovers"}[named], func(t *testing.T) {
			s, env := cliFixture(t)
			if _, err := s.Ingest(bridge.Envelope{Platform: "discord", EventID: "3", ConversationID: "2", SenderID: "1", Text: "offline", ReceivedAt: float64(time.Now().Unix()), RouteKind: "dm"}); err != nil {
				t.Fatal(err)
			}
			args := []string{"next", "--lease-seconds", "300", "--begin"}
			if named {
				args = append(args, "--consumer-id", "offline.reasoning-worker")
			}
			exe, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command(exe, append([]string{"-test.run=^TestCLIProcessHelper$", "--"}, args...)...)
			cmd.Env = append(env, "DOT_CLI_TEST_HELPER=1", "GORACE=atexit_sleep_ms=0")
			cmd.Stdout = io.Discard
			if err := cmd.Run(); err != nil {
				t.Fatal(err)
			}
			rows, err := s.FeedbackRows()
			if err != nil || len(rows) != 1 || rows[0].State != "claimed" {
				t.Fatalf("claim not committed: %+v %v", rows, err)
			}
			original := rows[0]
			start := time.Now()
			r, err := cli(t, env, "", args...)
			if err != nil {
				t.Fatal(r, err)
			}
			if !named {
				if r["message"] != nil {
					t.Fatal("legacy unexpectedly recovered inaccessible claim", r)
				}
				return
			}
			if time.Since(start) > 3*time.Second {
				t.Fatal("recovery waited for lease")
			}
			m, ok := r["message"].(map[string]any)
			if !ok || m["claim"] != original.Claim || m["inbound_id"] != original.ID || m["lease_until"] != original.LeaseUntil {
				t.Fatal("claim not replayed exactly", r)
			}
			r, err = cli(t, env, "offline reply", "reply", original.ID, "--claim", original.Claim)
			if err != nil {
				t.Fatal(r, err)
			}
			chunk, err := s.NextChunk()
			if err != nil || chunk == nil {
				t.Fatal(chunk, err)
			}
			if err := s.RecordResult(*chunk, bridge.SendResult{State: "sent", MessageID: "5"}); err != nil {
				t.Fatal(err)
			}
			r, err = cli(t, env, "", args...)
			if err != nil || r["message"] != nil {
				t.Fatal("terminal claim replayed", r, err)
			}
			if _, err = cli(t, env, "offline reply", "reply", original.ID, "--claim", original.Claim); err != nil {
				t.Fatal(err)
			}
			if c, err := s.NextChunk(); err != nil || c != nil {
				t.Fatal("duplicate send", c, err)
			}
		})
	}
}

func TestCLIOutputFailureLeavesRecoverableClaim(t *testing.T) {
	s, env := cliFixture(t)
	s.Ingest(bridge.Envelope{Platform: "discord", EventID: "3", ConversationID: "2", SenderID: "1", Text: "offline", ReceivedAt: float64(time.Now().Unix()), RouteKind: "dm"})
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	full, err := os.OpenFile("/dev/full", os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer full.Close()
	cmd := exec.Command(exe, "-test.run=^TestCLIProcessHelper$", "--", "next")
	cmd.Env = append(env, "DOT_CLI_TEST_HELPER=1", "GORACE=atexit_sleep_ms=0", "BRIDGE_CONSUMER_ID=output-failure")
	cmd.Stdout = full
	var stderr strings.Builder
	cmd.Stderr = &stderr
	if err = cmd.Run(); err == nil || stderr.String() != "result_output_failed\n" {
		t.Fatal("output failure not reported", err, stderr.String())
	}
	r, err := cli(t, env, "", "next", "--consumer-id", "output-failure")
	if err != nil || r["message"] == nil {
		t.Fatal("failed output lost claim", r, err)
	}
}

func TestCLIConsumerIdentityValidationAndOverride(t *testing.T) {
	s, env := cliFixture(t)
	for _, id := range []string{"", "space invalid", strings.Repeat("x", 129), "한글"} {
		r, err := cli(t, env, "", "next", "--consumer-id", id)
		if err == nil || r["error"] == nil {
			t.Fatal("invalid identity accepted", id, r)
		}
	}
	s.Ingest(bridge.Envelope{Platform: "discord", EventID: "3", ConversationID: "2", SenderID: "1", Text: "offline", ReceivedAt: float64(time.Now().Unix()), RouteKind: "dm"})
	r, err := cli(t, append(env, "BRIDGE_CONSUMER_ID=environment"), "", "next", "--consumer-id", "explicit")
	if err != nil || r["message"] == nil {
		t.Fatal(r, err)
	}
	r, err = cli(t, append(env, "BRIDGE_CONSUMER_ID=environment"), "", "next")
	if err != nil || r["message"] != nil {
		t.Fatal("env overrode explicit consumer", r, err)
	}
	r, err = cli(t, env, "", "next", "--consumer-id", "explicit")
	if err != nil || r["message"] == nil {
		t.Fatal("explicit identity did not recover", r, err)
	}
}
