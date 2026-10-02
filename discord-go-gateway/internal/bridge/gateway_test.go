package bridge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/gorilla/websocket"
)

func TestReadinessCannotResurrectOldEpoch(t *testing.T) {
	g := &gatewayState{}
	old := g.epoch.Load()
	g.disconnect()
	if g.publishReady(old) || g.ready.Load() {
		t.Fatal("stale validator restored ready")
	}
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(2)
		old := g.epoch.Load()
		go func() { defer wg.Done(); g.publishReady(old) }()
		go func() { defer wg.Done(); g.disconnect() }()
		wg.Wait()
		if g.ready.Load() {
			t.Fatal("disconnect lost")
		}
	}
}

func TestGatewaySupervisorErrorClassification(t *testing.T) {
	tests := []struct {
		err  error
		code string
		exit int
	}{
		{errors.New("preflight_http_401"), "authentication_failed", 2},
		{errors.New("preflight_http_403"), "authentication_failed", 2},
		{errors.New("preflight_identity_mismatch"), "bot_identity_mismatch", 2},
		{errors.New("preflight_channel_mismatch"), "configured_channel_mismatch", 2},
		{errors.New("preflight_http_404"), "configured_channel_lookup_failed", 2},
		{errors.New("configured_channel_permissions_missing"), "configured_channel_permissions_missing", 2},
		{errors.New("preflight_transport_failed"), "gateway_cause_preflight_transport_failed", 1},
		{errors.New("preflight_http_429"), "gateway_connection_failed", 1},
		{errors.New("preflight_http_500"), "gateway_connection_failed", 1},
		{errors.New("preflight_http_503"), "gateway_connection_failed", 1},
		{context.DeadlineExceeded, "gateway_startup_timeout", 1},
		{&websocket.CloseError{Code: 4004, Text: "not logged"}, "authentication_failed", 2},
		{&websocket.CloseError{Code: 4014}, "privileged_intents_required", 2},
		{&websocket.CloseError{Code: 1006}, "gateway_connection_failed", 1},
		{errors.New("dial failed: unexpected_gateway_endpoint"), "gateway_proxy_route_or_endpoint_invalid", 2},
	}
	for _, tt := range tests {
		t.Run(tt.code+"_"+tt.err.Error(), func(t *testing.T) {
			e := ClassifyGatewayError(tt.err)
			if e.Error() != tt.code || GatewayExitCode(e) != tt.exit {
				t.Fatalf("%v exit %d", e, GatewayExitCode(e))
			}
		})
	}
}
func TestDispatchDrainsBurstWithoutFixedSleep(t *testing.T) {
	s, _ := testStore(t)
	for i := 0; i < 12; i++ {
		ingestLedger(t, s, fmt.Sprint(i), "same")
		c := claimLedger(t, s)
		replyLedger(t, s, c, "response")
	}
	hub := NewWakeHub()
	g := &gatewayState{}
	g.ready.Store(true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var sent atomic.Int32
	start := time.Now()
	done := make(chan error, 1)
	go func() {
		done <- dispatchLoop(ctx, s, hub, g, func(context.Context, Chunk) SendResult {
			if sent.Add(1) == 12 {
				cancel()
			}
			return SendResult{State: "sent", MessageID: "123"}
		})
	}()
	select {
	case e := <-done:
		if e != nil {
			t.Fatal(e)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("burst stalled")
	}
	if sent.Load() != 12 || time.Since(start) > time.Second {
		t.Fatalf("burst count %d elapsed %s", sent.Load(), time.Since(start))
	}
}
func TestDispatchCancellationPersistsUncertain(t *testing.T) {
	s, _ := testStore(t)
	ingestLedger(t, s, "one", "same")
	c := claimLedger(t, s)
	id := replyLedger(t, s, c, "response")
	ctx, cancel := context.WithCancel(context.Background())
	g := &gatewayState{}
	g.ready.Store(true)
	err := dispatchLoop(ctx, s, NewWakeHub(), g, func(context.Context, Chunk) SendResult {
		cancel()
		return SendResult{State: "uncertain", Code: "request_or_ack_failed"}
	})
	if err != nil {
		t.Fatal(err)
	}
	d, _ := s.Delivery(id)
	if d.State != "uncertain" || d.Chunks[0].Attempts != 1 {
		t.Fatal(d)
	}
}

type gatewayRoundTrip func(*http.Request) (*http.Response, error)

func (f gatewayRoundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func TestStalledGatewayHelloCancelled(t *testing.T) {
	entered := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, e := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if e != nil {
			return
		}
		defer c.Close()
		close(entered)
		c.ReadMessage()
	}))
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	d := newGatewayDialer(ctx)
	defer d.closeConnections()
	s, _ := discordgo.New("Bot offline")
	s.LogLevel = -1
	s.ShouldReconnectOnError = false
	s.Dialer = &websocket.Dialer{NetDialContext: d.dial}
	s.Client = &http.Client{Transport: gatewayRoundTrip(func(*http.Request) (*http.Response, error) {
		b, _ := json.Marshal(map[string]string{"url": "ws" + strings.TrimPrefix(srv.URL, "http")})
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(string(b))), Header: make(http.Header)}, nil
	})}
	start := time.Now()
	e := openGateway(ctx, s, d)
	if e == nil {
		t.Fatal("expected cancellation")
	}
	if time.Since(start) > time.Second {
		t.Fatal("stalled Open deadlocked")
	}
	select {
	case <-entered:
	default:
		t.Fatal("never reached Hello wait")
	}
	s.Close()
}
func typingFixture(t *testing.T) (*Store, *Claim) {
	t.Helper()
	dir := t.TempDir()
	os.Chmod(dir, 0700)
	s, e := OpenStore(dir+"/typing.db", testPolicy())
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { s.Close() })
	if _, e = s.Ingest(testEnvelope()); e != nil {
		t.Fatal(e)
	}
	c, e := s.ClaimNext(60, 5)
	if e != nil {
		t.Fatal(e)
	}
	return s, c
}
func TestTypingBeginsOnlyProcessingAndStopsOnReply(t *testing.T) {
	s, c := typingFixture(t)
	calls := make(chan struct{}, 10)
	r := localREST(t, func(w http.ResponseWriter, q *http.Request) {
		if preflightHandler(w, q) {
			return
		}
		if strings.HasSuffix(q.URL.Path, "/typing") {
			calls <- struct{}{}
			w.WriteHeader(204)
			return
		}
		t.Error("unexpected request")
	})
	hub := NewWakeHub()
	g := &gatewayState{}
	g.ready.Store(true)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- typingLoop(ctx, s, r, hub, g) }()
	select {
	case <-calls:
	case <-time.After(time.Second):
		t.Fatal("no processing typing")
	}
	if _, e := s.QueueReply(c.InboundID, c.Claim, "done"); e != nil {
		t.Fatal(e)
	}
	hub.Notify()
	time.Sleep(30 * time.Millisecond)
	cancel()
	if e := <-done; e != nil {
		t.Fatal(e)
	}
	select {
	case <-calls:
		t.Fatal("extra typing after cancellation")
	default:
	}
}
func TestTypingRetriesTransientAndCancelsDisconnect(t *testing.T) {
	s, _ := typingFixture(t)
	calls := make(chan int, 10)
	var n atomic.Int32
	r := localREST(t, func(w http.ResponseWriter, q *http.Request) {
		if preflightHandler(w, q) {
			return
		}
		i := int(n.Add(1))
		calls <- i
		if i == 1 {
			w.WriteHeader(503)
		} else {
			w.WriteHeader(204)
		}
	})
	hub := NewWakeHub()
	g := &gatewayState{}
	g.ready.Store(true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- typingLoop(ctx, s, r, hub, g) }()
	for i := 0; i < 2; i++ {
		select {
		case <-calls:
		case <-time.After(3 * time.Second):
			t.Fatal("transient typing did not recover")
		}
	}
	g.disconnect()
	hub.Notify()
	cancel()
	if e := <-done; e != nil {
		t.Fatal(e)
	}
}
func TestFeedbackReceiptAndSentCleanup(t *testing.T) {
	var routes []string
	var mu sync.Mutex
	r := localREST(t, func(w http.ResponseWriter, q *http.Request) {
		if preflightHandler(w, q) {
			return
		}
		mu.Lock()
		routes = append(routes, q.Method+" "+q.URL.Path)
		mu.Unlock()
		w.WriteHeader(204)
	})
	e := testEnvelope()
	if err := updateFeedback(context.Background(), r, e, "", "received"); err != nil {
		t.Fatal(err)
	}
	if err := updateFeedback(context.Background(), r, e, "received", "sent"); err != nil {
		t.Fatal(err)
	}
	if len(routes) != 2 || !strings.HasPrefix(routes[0], "PUT ") || !strings.Contains(routes[0], "👀") || !strings.HasPrefix(routes[1], "DELETE ") || !strings.Contains(routes[1], "👀") {
		t.Fatal(routes)
	}
}

func TestDiscordGoReadyThenGuildCreatePermissions(t *testing.T) {
	s, e := discordgo.New("Bot offline")
	if e != nil {
		t.Fatal(e)
	}
	cfg := Settings{ExpectedBotID: "4", Policy: Policy{OwnerID: "1", AllowedDMIDs: []string{"1"}, GuildID: "5", GuildChannelID: "2", GuildMode: "mention"}}
	ready := &discordgo.Ready{User: &discordgo.User{ID: "4", Bot: true}, Guilds: []*discordgo.Guild{{ID: "5", Unavailable: true}}}
	if e = s.State.OnInterface(s, ready); e != nil {
		t.Fatal(e)
	}
	if guildPermissions(s, cfg) {
		t.Fatal("placeholder considered ready")
	}
	permissions := int64(discordgo.PermissionViewChannel | discordgo.PermissionSendMessages | discordgo.PermissionReadMessageHistory)
	guild := &discordgo.Guild{ID: "5", Roles: []*discordgo.Role{{ID: "5", Permissions: permissions}}, Channels: []*discordgo.Channel{{ID: "2", GuildID: "5", Type: discordgo.ChannelTypeGuildText}}, Members: []*discordgo.Member{{GuildID: "5", User: &discordgo.User{ID: "4", Bot: true}}}}
	if e = s.State.OnInterface(s, &discordgo.GuildCreate{Guild: guild}); e != nil {
		t.Fatal(e)
	}
	if !guildPermissions(s, cfg) {
		t.Fatal("real GUILD_CREATE not ready")
	}
	guild.Channels[0].PermissionOverwrites = []*discordgo.PermissionOverwrite{{ID: "4", Type: discordgo.PermissionOverwriteTypeMember, Deny: discordgo.PermissionSendMessages}}
	if e = s.State.OnInterface(s, &discordgo.ChannelUpdate{Channel: guild.Channels[0]}); e != nil {
		t.Fatal(e)
	}
	if guildPermissions(s, cfg) {
		t.Fatal("revoked send still allowed")
	}
}

func TestTypingDenyLatchClearedOnFastReconnect(t *testing.T) {
	s, _ := typingFixture(t)
	calls := make(chan struct{}, 10)
	r := localREST(t, func(w http.ResponseWriter, q *http.Request) {
		if preflightHandler(w, q) {
			return
		}
		calls <- struct{}{}
		w.WriteHeader(403)
	})
	hub := NewWakeHub()
	g := &gatewayState{}
	g.ready.Store(true)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- typingLoop(ctx, s, r, hub, g) }()
	defer func() { cancel(); <-done }()
	select {
	case <-calls:
	case <-time.After(time.Second):
		t.Fatal("first typing missing")
	}
	g.disconnect()
	g.publishReady(g.epoch.Load())
	hub.Notify()
	select {
	case <-calls:
	case <-time.After(time.Second):
		t.Fatal("deny latch not reset by epoch")
	}
}
