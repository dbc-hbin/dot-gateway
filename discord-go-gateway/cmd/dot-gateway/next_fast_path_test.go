package main

import (
	"dot-gateway/internal/bridge"
	"os"
	"strings"
	"testing"
	"time"
)

func TestCLINextFastPathDoesNotCreateWatcher(t *testing.T) {
	for _, ready := range []bool{false, true} {
		t.Run(map[bool]string{false: "empty_nonblocking", true: "ready_and_recovered_long_poll"}[ready], func(t *testing.T) {
			s, env := cliFixture(t)
			var db string
			for _, entry := range env {
				if strings.HasPrefix(entry, "BRIDGE_DB=") {
					db = strings.TrimPrefix(entry, "BRIDGE_DB=")
				}
			}
			args := []string{"next", "--consumer-id", "fast-path", "--begin"}
			if ready {
				if _, err := s.Ingest(bridge.Envelope{Platform: "discord", EventID: "3", ConversationID: "2", SenderID: "1", Text: "offline", ReceivedAt: float64(time.Now().Unix()), RouteKind: "dm"}); err != nil {
					t.Fatal(err)
				}
				args = append(args, "--wait", "30")
			}
			var original any
			for i := 0; i < 2; i++ {
				r, err := cli(t, env, "", args...)
				if err != nil || (r["message"] != nil) != ready {
					t.Fatal(r, err)
				}
				if ready {
					claim := r["message"].(map[string]any)["claim"]
					if i > 0 && claim != original {
						t.Fatal("fast-path recovery changed ownership")
					}
					original = claim
				}
				if _, err := os.Lstat(db + ".sock.wake"); !os.IsNotExist(err) {
					t.Fatalf("fast path unexpectedly set up a file watcher: %v", err)
				}
			}
		})
	}
}
