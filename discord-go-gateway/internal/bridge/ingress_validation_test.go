package bridge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/gorilla/websocket"
)

func stageValidation(t *testing.T, s *Store, e Envelope) *ValidationInput {
	t.Helper()
	if outcome, err := s.StageIngress(e); err != nil || outcome != "validation_staged" {
		t.Fatalf("stage=%s err=%v", outcome, err)
	}
	in, err := s.NextValidation(epoch())
	if err != nil || in == nil {
		t.Fatalf("next=%v err=%v", in, err)
	}
	return in
}

func TestValidationTransientFailureKeepsHiddenExactWorkThenPromotes(t *testing.T) {
	s, _, _ := ingressFixture(t)
	event := testEnvelope()
	event.ReceivedAt = epoch() - 12
	in := stageValidation(t, s, event)
	if claim, err := s.ClaimNext(60, 60); err != nil || claim != nil {
		t.Fatal("quarantined input reached consumer")
	}
	if feedback, err := s.FeedbackRows(); err != nil || len(feedback) != 0 {
		t.Fatal("quarantined input reached feedback")
	}
	for _, fn := range []func(Envelope) (string, error){s.StageIngress, s.Ingest} {
		if outcome, err := fn(event); err != nil || outcome != "duplicate" {
			t.Fatalf("dedup=%s err=%v", outcome, err)
		}
	}
	var attempts atomic.Int32
	r := localREST(t, func(w http.ResponseWriter, q *http.Request) {
		if attempts.Add(1) == 1 {
			w.WriteHeader(503)
			return
		}
		preflightHandler(w, q)
	})
	g := &gatewayState{}
	g.publishReady(0)
	if err := processValidation(context.Background(), s, r, g, *in, 0); err != nil {
		t.Fatal(err)
	}
	if next, err := s.NextValidation(epoch()); err != nil || next != nil {
		t.Fatal("transient failure retried before due time")
	}
	next, err := s.NextValidation(epoch() + 60) // Advance scheduling observation, no sleeps.
	if err != nil || next == nil || next.ID != in.ID || next.Event != in.Event || next.Created != in.Created || next.Attempts != 1 {
		t.Fatalf("lost or changed input: %+v %v", next, err)
	}
	if err = processValidation(context.Background(), s, r, g, *next, 0); err != nil {
		t.Fatal(err)
	}
	claim, err := s.ClaimNext(60, 0)
	if err != nil || claim == nil || claim.InboundID != in.ID || claim.Envelope != in.Event {
		t.Fatalf("promotion changed identity: %+v %v", claim, err)
	}
	var created float64
	_, err = s.call(func(db *storeConn) (any, error) {
		return nil, db.QueryRow("SELECT created FROM inbound WHERE id=?", in.ID).Scan(&created)
	})
	if err != nil || created != in.Created || attempts.Load() != 2 {
		t.Fatal("promotion lost original receipt time or repeated POST", created, attempts.Load(), err)
	}
}

func TestValidationDeniedAndMissingChannelsRetainWorkUntilValidatedResume(t *testing.T) {
	for _, status := range []int{401, 403, 404} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			s, _, _ := ingressFixture(t)
			in := stageValidation(t, s, testEnvelope())
			var denied atomic.Bool
			denied.Store(true)
			r := localREST(t, func(w http.ResponseWriter, q *http.Request) {
				if denied.Load() {
					w.WriteHeader(status)
					return
				}
				preflightHandler(w, q)
			})
			g := &gatewayState{}
			g.publishReady(0)
			err := processValidation(context.Background(), s, r, g, *in, 0)
			if status == 401 {
				if err == nil || err.Error() != "authentication_failed" || GatewayExitCode(err) != 2 {
					t.Fatal("authentication failure did not latch", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if next, err := s.NextValidation(epoch() + 100000); err != nil || next != nil {
				t.Fatal("blocked work retried by elapsed time alone")
			}
			if claim, err := s.ClaimNext(60, 0); err != nil || claim != nil {
				t.Fatal("denied input became consumable")
			}
			denied.Store(false)
			g.disconnect()
			g.publishReady(g.epoch.Load())
			if err := s.ResumeValidation(); err != nil {
				t.Fatal(err)
			}
			next, err := s.NextValidation(epoch())
			if err != nil || next == nil || next.ID != in.ID {
				t.Fatal("validated recovery lost work", err)
			}
			if err := processValidation(context.Background(), s, r, g, *next, g.epoch.Load()); err != nil {
				t.Fatal(err)
			}
			if claim, err := s.ClaimNext(60, 0); err != nil || claim == nil {
				t.Fatal("validated recovery did not promote", err)
			}
		})
	}
}

func TestValidationBudgetWaitCancellationRetainsPendingWork(t *testing.T) {
	s, _, _ := ingressFixture(t)
	in := stageValidation(t, s, testEnvelope())
	var requests atomic.Int32
	r := localREST(t, func(w http.ResponseWriter, q *http.Request) { requests.Add(1); preflightHandler(w, q) })
	r.limitMu.Lock()
	r.globalUntil = time.Now().Add(time.Hour)
	r.limitMu.Unlock()
	g := &gatewayState{}
	g.publishReady(0)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := processValidation(ctx, s, r, g, *in, 0); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) > time.Second || requests.Load() != 0 {
		t.Fatal("budget wait escaped cancellation or made a request")
	}
	next, err := s.NextValidation(epoch())
	if err != nil || next == nil || next.ID != in.ID || next.Event != in.Event {
		t.Fatal("cancelled validation lost durable work", err)
	}
}

func TestValidationSharedCapacityAndRevokedPolicy(t *testing.T) {
	s, _ := testStore(t)
	for i := 0; i < 1000; i++ {
		e := ledgerEvent(fmt.Sprint(i), "same")
		if outcome, err := s.StageIngress(e); err != nil || outcome != "validation_staged" {
			t.Fatalf("capacity stage=%s error=%v", outcome, err)
		}
	}
	for _, fn := range []func(Envelope) (string, error){s.StageIngress, s.Ingest} {
		if outcome, err := fn(ledgerEvent("overflow", "same")); err != nil || outcome != "queue_full" {
			t.Fatalf("quarantine bypassed capacity: %s %v", outcome, err)
		}
	}
	in, err := s.NextValidation(epoch())
	if err != nil || in == nil {
		t.Fatal(err)
	}
	// Existing policy interface is immutable during normal operation; this test
	// changes it only while no other goroutine is calling the store.
	s.policy = testPolicy()
	if outcome, err := s.PromoteValidation(*in); err != nil || outcome != "validation_blocked" {
		t.Fatalf("revoked input promoted: %s %v", outcome, err)
	}
}

func TestGatewayReadinessBoundedAndFatalClassificationPreserved(t *testing.T) {
	for _, code := range []string{"authentication_failed", "configured_channel_mismatch", "configured_channel_lookup_failed", "configured_channel_permissions_missing", "gateway_proxy_route_or_endpoint_invalid", "privileged_intents_required"} {
		t.Run(code, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			fatal := make(chan error, 1)
			fatal <- errors.New(code)
			cancel()
			err := ClassifyGatewayError(waitForGatewayReady(ctx, &gatewayState{}, 0, fatal))
			if err == nil || err.Error() != code || GatewayExitCode(err) != 2 {
				t.Fatalf("fatal code downgraded: %v", err)
			}
		})
	}
	t.Run("incomplete_state_times_out", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
		defer cancel()
		start := time.Now()
		err := ClassifyGatewayError(waitForGatewayReady(ctx, &gatewayState{}, 0, make(chan error)))
		if err == nil || err.Error() != "gateway_startup_timeout" || GatewayExitCode(err) != 1 || time.Since(start) > time.Second {
			t.Fatal("incomplete state did not produce bounded transient recovery", err)
		}
	})
	t.Run("different_epoch_never_satisfies", func(t *testing.T) {
		g := &gatewayState{}
		g.disconnect()
		g.publishReady(g.epoch.Load())
		err := waitForGatewayReady(context.Background(), g, 0, make(chan error))
		if err == nil || err.Error() != "gateway_connection_failed" {
			t.Fatal("wrong-epoch ready accepted", err)
		}
	})
}

func TestGatewayOpenWithoutGuildStateHasBoundedReconnectReadiness(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		conn.WriteJSON(map[string]any{"op": 10, "d": map[string]int{"heartbeat_interval": 100000}})
		if _, _, err := conn.ReadMessage(); err != nil { // IDENTIFY
			return
		}
		conn.WriteJSON(map[string]any{"op": 0, "t": "READY", "s": 1, "d": map[string]any{
			"v": 10, "session_id": "offline-session", "user": map[string]any{"id": "4", "bot": true},
			"guilds": []map[string]any{{"id": "5", "unavailable": true}},
		}})
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer srv.Close()
	session, _ := discordgo.New("Bot offline")
	session.LogLevel = -1
	session.ShouldReconnectOnError = false
	session.SyncEvents = true
	session.Client = &http.Client{Transport: gatewayRoundTrip(func(*http.Request) (*http.Response, error) {
		body, _ := json.Marshal(map[string]string{"url": "ws" + strings.TrimPrefix(srv.URL, "http")})
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(string(body))), Header: make(http.Header)}, nil
	})}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	dial := newGatewayDialer(ctx)
	defer dial.closeConnections()
	session.Dialer = &websocket.Dialer{NetDialContext: dial.dial}
	defer session.Close()
	g := &gatewayState{}
	g.disconnect() // This is the new reconnect epoch.
	start := time.Now()
	if err := openGateway(ctx, session, dial); err != nil {
		t.Fatal("fake Gateway Open failed before readiness stage", err)
	}
	cfg := Settings{Policy: Policy{GuildID: "5", GuildChannelID: "2"}, ExpectedBotID: "4"}
	if guildPermissions(session, cfg) {
		t.Fatal("incomplete guild cache unexpectedly passed readiness")
	}
	err := ClassifyGatewayError(waitForGatewayReady(ctx, g, g.epoch.Load(), make(chan error)))
	if err == nil || err.Error() != "gateway_startup_timeout" || GatewayExitCode(err) != 1 || time.Since(start) > time.Second {
		t.Fatal("Open success left reconnect alive forever", err)
	}
}

func TestReceiveStagesBeforeNetworkAndValidationLoopRequiresReady(t *testing.T) {
	s, cfg, message := ingressFixture(t)
	requests := make(chan struct{}, 8)
	r := localREST(t, func(w http.ResponseWriter, q *http.Request) {
		requests <- struct{}{}
		preflightHandler(w, q)
	})
	outcome, err := receiveMessage(context.Background(), r, s, cfg, message)
	if err != nil || outcome != "validation_staged" {
		t.Fatal(outcome, err)
	}
	select {
	case <-requests:
		t.Fatal("network lookup preceded durable staging")
	default:
	}
	g := &gatewayState{}
	hub := NewWakeHub()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- validationLoop(ctx, s, r, hub, g, t.TempDir()+"/offline") }()
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	select {
	case <-requests:
		t.Fatal("unready gateway started validation")
	case <-time.After(50 * time.Millisecond):
	}
	g.publishReady(0)
	hub.Notify()
	select {
	case <-requests:
	case <-time.After(time.Second):
		t.Fatal("ready gateway did not resume durable validation")
	}
	deadline := time.Now().Add(time.Second)
	for {
		claim, err := s.ClaimNext(60, 0)
		if err != nil {
			t.Fatal(err)
		}
		if claim != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("validated work not promoted")
		}
		time.Sleep(time.Millisecond)
	}
}
