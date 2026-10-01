package bridge

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
)

func ingressFixture(t *testing.T) (*Store, Settings, *discordgo.Message) {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	s, err := OpenStore(filepath.Join(dir, "ingress.db"), testPolicy())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	cfg := Settings{Policy: testPolicy(), ExpectedBotID: "4"}
	m := &discordgo.Message{ID: "3", ChannelID: "2", Author: &discordgo.User{ID: "1"}, Content: "PRIVATE_MESSAGE_CANARY"}
	return s, cfg, m
}

func TestIngressLookupAndValidationFailuresRemainVisibleAndUnaccepted(t *testing.T) {
	for _, tc := range []struct {
		name, outcome, warning, body string
		status                       int
	}{
		{"lookup", "route_lookup_failed", "owner_route_lookup_failed", "PRIVATE_HTTP_CANARY", 503},
		{"validation", "route_validation_failed", "owner_route_validation_failed", `{"id":"2","type":1,"recipients":[{"id":"9"}]}`, 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, cfg, m := ingressFixture(t)
			r := localREST(t, func(w http.ResponseWriter, q *http.Request) {
				w.WriteHeader(tc.status)
				io.WriteString(w, tc.body)
			})
			outcome, err := receiveMessage(context.Background(), r, s, cfg, m)
			if err != nil || outcome != "validation_staged" {
				t.Fatalf("outcome=%s error=%v", outcome, err)
			}
			g := &gatewayState{state: "connected"}
			g.ready.Store(true)
			g.recordIngress(outcome)
			in, err := s.NextValidation(epoch())
			if err != nil || in == nil {
				t.Fatalf("missing staged input: %v", err)
			}
			if err := processValidation(context.Background(), s, r, g, *in, 0); err != nil {
				t.Fatal(err)
			}
			claim, err := s.ClaimNext(60, 0)
			if err != nil || claim != nil {
				t.Fatal("unvalidated input reached consumer")
			}
			if g.health(r)["ingress_counts"].(map[string]int)[tc.outcome] != 1 {
				t.Fatal("validation failure missing from counters")
			}
			g.recordIngress("PRIVATE_MESSAGE_CANARY") // Unrecognized data is not a diagnostic key.
			if err := persistGatewayHealth(s, g.health(r)); err != nil {
				t.Fatal(err)
			}
			status, err := s.Status()
			if err != nil {
				t.Fatal(err)
			}
			raw, err := json.Marshal(status)
			if err != nil || strings.Contains(string(raw), "PRIVATE_") {
				t.Fatal("diagnostics exposed untrusted content")
			}
			gateway := status["gateway"].(map[string]any)
			warnings := gateway["warning_counts"].(map[string]any)
			if warnings[tc.warning] != float64(1) {
				t.Fatalf("missing persisted warning: %#v", warnings)
			}
		})
	}
}

func TestIngressQueueFullIsReportedWithoutDisplacingDurableWork(t *testing.T) {
	s, cfg, m := ingressFixture(t)
	_, err := s.call(func(db *storeConn) (any, error) {
		return transact(db, func(db *storeConn) (any, error) {
			for i := 0; i < 1000; i++ {
				e := testEnvelope()
				e.EventID = fmt.Sprint(100 + i)
				raw, err := json.Marshal(e)
				if err != nil {
					return nil, err
				}
				if _, err := db.Exec("INSERT INTO inbound(id,platform,event_id,envelope,created) VALUES(?,?,?,?,?)", fmt.Sprintf("%032d", i), e.Platform, e.EventID, string(raw), epoch()); err != nil {
					return nil, err
				}
			}
			return nil, nil
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	r := localREST(t, func(w http.ResponseWriter, q *http.Request) { preflightHandler(w, q) })
	outcome, err := receiveMessage(context.Background(), r, s, cfg, m)
	if err != nil || outcome != "queue_full" {
		t.Fatalf("outcome=%s error=%v", outcome, err)
	}
	g := &gatewayState{state: "connected"}
	g.recordIngress(outcome)
	if err := persistGatewayHealth(s, g.health(r)); err != nil {
		t.Fatal(err)
	}
	status, err := s.Status()
	if err != nil {
		t.Fatal(err)
	}
	if status["inbound"].(map[string]int)["pending"] != 1000 {
		t.Fatal("queue capacity behavior changed")
	}
	gw := status["gateway"].(map[string]any)
	if gw["warning_counts"].(map[string]any)["inbound_queue_full"] != float64(1) {
		t.Fatal("queue-full warning missing")
	}
	// Snapshots cannot mutate the live counters.
	snapshot := g.health(r)
	snapshot["ingress_counts"].(map[string]int)["queue_full"] = 99
	if g.health(r)["ingress_counts"].(map[string]int)["queue_full"] != 1 {
		t.Fatal("health exposed mutable live counters")
	}
}

func TestTypingPermissionDenySurvivesRenewalUntilValidatedReconnect(t *testing.T) {
	for _, deniedRoute := range []string{"typing", "channel"} {
		for _, status := range []int{401, 403} {
			t.Run(fmt.Sprintf("%s_%d", deniedRoute, status), func(t *testing.T) {
				s, claim := typingFixture(t)
				calls := make(chan struct{}, 10)
				r := localREST(t, func(w http.ResponseWriter, q *http.Request) {
					deny := strings.HasSuffix(q.URL.Path, "/typing")
					if deniedRoute == "channel" {
						deny = q.URL.Path == "/channels/2"
					}
					if deny {
						calls <- struct{}{}
						w.WriteHeader(status)
						return
					}
					if !preflightHandler(w, q) {
						t.Error("unexpected request")
					}
				})
				hub := NewWakeHub()
				g := &gatewayState{}
				g.ready.Store(true)
				ctx, cancel := context.WithCancel(context.Background())
				done := make(chan error, 1)
				go func() { done <- typingLoop(ctx, s, r, hub, g) }()
				defer func() {
					cancel()
					if err := <-done; err != nil {
						t.Error(err)
					}
				}()
				expectCall := func() {
					t.Helper()
					select {
					case <-calls:
					case <-time.After(time.Second):
						t.Fatal("expected attempt missing")
					}
				}
				expectQuiet := func() {
					t.Helper()
					select {
					case <-calls:
						t.Fatal("permission denied endpoint retried without validated reconnect")
					case <-time.After(350 * time.Millisecond):
					}
				}
				expectCall()
				expectQuiet()
				if err := s.BeginProcessing(claim.InboundID, claim.Claim, 10); err != nil {
					t.Fatal(err)
				}
				hub.Notify()
				expectQuiet()
				g.disconnect()
				hub.Notify()
				expectQuiet()
				if !g.publishReady(g.epoch.Load()) {
					t.Fatal("validated reconnect not published")
				}
				hub.Notify()
				expectCall()
			})
		}
	}
}
