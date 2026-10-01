package bridge

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/gorilla/websocket"
)

func resilienceGuildFixture(t *testing.T, permissions int64) (*discordgo.Session, Settings) {
	t.Helper()
	s, err := discordgo.New("Bot offline")
	if err != nil {
		t.Fatal(err)
	}
	s.LogLevel = -1
	cfg := Settings{ExpectedBotID: "4", Policy: Policy{OwnerID: "1", AllowedDMIDs: []string{"1"}, Platform: "discord", GuildID: "5", GuildChannelID: "2", GuildMode: "all", MessageContentApproved: true}}
	guild := &discordgo.Guild{ID: "5", Roles: []*discordgo.Role{{ID: "5", Permissions: permissions}}, Channels: []*discordgo.Channel{{ID: "2", GuildID: "5", Type: discordgo.ChannelTypeGuildText}}, Members: []*discordgo.Member{{GuildID: "5", User: &discordgo.User{ID: "4", Bot: true}}}}
	if err := s.State.OnInterface(s, &discordgo.GuildCreate{Guild: guild}); err != nil {
		t.Fatal(err)
	}
	return s, cfg
}

const resilienceGuildPermissions = discordgo.PermissionViewChannel | discordgo.PermissionReadMessageHistory | discordgo.PermissionSendMessages

func TestGatewayValidationWakeKeepsNewestEpoch(t *testing.T) {
	ch := make(chan uint64, 1)
	queueGatewayValidation(ch, 0)
	queueGatewayValidation(ch, 1)
	if epoch := <-ch; epoch != 1 {
		t.Fatal("old validation wake displaced current connection")
	}
	queueGatewayValidation(ch, 2)
	if epoch := <-ch; epoch != 2 {
		t.Fatal("current validation wake was lost")
	}
}

func TestOwnerDMIndependentOfGuildReadiness(t *testing.T) {
	for _, state := range []string{"missing", "no_send_permission", "unavailable", "writable"} {
		t.Run(state, func(t *testing.T) {
			s, cfg := resilienceGuildFixture(t, resilienceGuildPermissions)
			switch state {
			case "missing":
				s.State = discordgo.NewState()
			case "no_send_permission":
				s, cfg = resilienceGuildFixture(t, discordgo.PermissionViewChannel|discordgo.PermissionReadMessageHistory)
			case "unavailable":
				guild, _ := s.State.Guild("5")
				guild.Unavailable = true
			}
			var guildReads atomic.Int32
			r := localREST(t, func(w http.ResponseWriter, q *http.Request) {
				if q.URL.Path == "/users/@me" {
					io.WriteString(w, `{"id":"4","bot":true}`)
				} else if q.URL.Path == "/channels/7" {
					io.WriteString(w, `{"id":"7","type":1,"recipients":[{"id":"1","bot":false}]}`)
				} else {
					guildReads.Add(1)
					w.WriteHeader(http.StatusForbidden)
				}
			})
			r.settings.Policy = cfg.Policy
			g := &gatewayState{}
			r.routePermission = func(e Envelope) bool { return gatewayRoutePermissions(g, s, cfg, e) }
			if err := refreshGatewayReadiness(context.Background(), r, s, cfg, g, 0); err != nil || !g.ready.Load() || g.guildReadiness() {
				t.Fatalf("owner DM readiness lost: err=%v ready=%v guild=%v", err, g.ready.Load(), g.guildReadiness())
			}
			dm := testEnvelope()
			dm.ConversationID = "7"
			before := guildReads.Load()
			if err := r.preflight(context.Background(), dm); err != nil {
				t.Fatalf("DM denied by unavailable guild: %v", err)
			}
			if before != guildReads.Load() {
				t.Fatal("DM preflight consulted guild route")
			}
			if gatewayRoutePermissions(g, s, cfg, Envelope{RouteKind: "guild_text"}) {
				t.Fatal("unvalidated guild route opened")
			}
			if err := r.ValidateChannel(Channel{ID: "7", Type: 1, Recipients: []User{{ID: "8"}}}, dm); err == nil {
				t.Fatal("non-owner DM accepted")
			}
			if err := r.ValidateChannel(Channel{ID: "7", Type: 3, Recipients: []User{{ID: "1"}}}, dm); err == nil {
				t.Fatal("group DM accepted")
			}
		})
	}
}

func TestGuildFailureIsolationAndRecovery(t *testing.T) {
	for _, status := range []int{403, 404, 429, 503} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			s, cfg := resilienceGuildFixture(t, resilienceGuildPermissions)
			var restored atomic.Bool
			r := localREST(t, func(w http.ResponseWriter, q *http.Request) {
				if q.URL.Path == "/users/@me" {
					io.WriteString(w, `{"id":"4","bot":true}`)
				} else if restored.Load() {
					io.WriteString(w, `{"id":"2","type":0,"guild_id":"5"}`)
				} else {
					w.WriteHeader(status)
				}
			})
			r.settings.Policy = cfg.Policy
			g := &gatewayState{}
			if err := refreshGatewayReadiness(context.Background(), r, s, cfg, g, 0); err != nil || !g.ready.Load() || g.guildReadiness() {
				t.Fatalf("guild failure killed DM service: %v", err)
			}
			restored.Store(true)
			if err := refreshGatewayReadiness(context.Background(), r, s, cfg, g, 0); err != nil || !g.guildReadiness() {
				t.Fatalf("guild recovery failed: %v", err)
			}
			if !gatewayRoutePermissions(g, s, cfg, Envelope{RouteKind: "guild_text"}) {
				t.Fatal("verified writable guild route denied")
			}
			if g.readyCount != 1 {
				t.Fatal("periodic validation counted as a reconnect")
			}
			g.disconnect()
			if g.guildReadiness() || g.setGuildReadiness(0, true, "") {
				t.Fatal("stale guild validation survived disconnect")
			}
			if err := refreshGatewayReadiness(context.Background(), r, s, cfg, g, 0); err != nil || g.ready.Load() {
				t.Fatal("queued pre-disconnect validation reopened readiness")
			}
		})
	}
}

func TestGatewayIdentityAndAuthenticationStillFailClosed(t *testing.T) {
	for _, tc := range []struct {
		name, body, path, want string
		status                 int
	}{
		{"identity401", "", "/users/@me", "authentication_failed", 401},
		{"identity403", "", "/users/@me", "authentication_failed", 403},
		{"wrong_identity", `{"id":"8","bot":true}`, "/users/@me", "bot_identity_mismatch", 200},
		{"guild401", "", "/channels/2", "authentication_failed", 401},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, cfg := resilienceGuildFixture(t, resilienceGuildPermissions)
			r := localREST(t, func(w http.ResponseWriter, q *http.Request) {
				if q.URL.Path == tc.path {
					w.WriteHeader(tc.status)
					io.WriteString(w, tc.body)
				} else {
					io.WriteString(w, `{"id":"4","bot":true}`)
				}
			})
			r.settings.Policy = cfg.Policy
			g := &gatewayState{}
			err := refreshGatewayReadiness(context.Background(), r, s, cfg, g, 0)
			if err == nil || ClassifyGatewayError(err).Error() != tc.want || g.guildReadiness() {
				t.Fatalf("global security failure not retained: %v", err)
			}
		})
	}
}

func TestResumeGatewayEndpointValidation(t *testing.T) {
	p, _ := NewProxyConfig("", "")
	for _, raw := range []string{"wss://gateway.discord.gg", "wss://us-east1-b.gateway.discord.gg/"} {
		if err := validateResumeGateway(p, raw); err != nil {
			t.Fatalf("valid resume endpoint refused: %v", err)
		}
	}
	for _, raw := range []string{"", "ws://gateway.discord.gg", "https://gateway.discord.gg", "wss://discord.gg", "wss://gateway.discord.gg.evil.test", "wss://127.0.0.1", "wss://u:p@gateway.discord.gg", "wss://gateway.discord.gg:444", "wss://gateway.discord.gg#fragment", "wss://gateway.discord.gg?", "wss://gateway.discord.gg?v=10", "wss://gateway.discord.gg\n"} {
		if err := validateResumeGateway(p, raw); err == nil {
			t.Fatalf("unsafe resume endpoint accepted: %q", raw)
		}
	}
	p, _ = NewProxyConfig("http://proxy:80", "us-east1-b.gateway.discord.gg")
	if err := validateResumeGateway(p, "wss://us-east1-b.gateway.discord.gg"); err == nil {
		t.Fatal("resume endpoint bypassed pinned proxy route")
	}
}

func TestResumeGatewayDialGuardRejectsBeforeNetwork(t *testing.T) {
	p, _ := NewProxyConfig("", "")
	var dials atomic.Int32
	dialer := &websocket.Dialer{Proxy: p.ProxyRequest, NetDialContext: func(context.Context, string, string) (net.Conn, error) {
		dials.Add(1)
		return nil, errors.New("unexpected_network_call")
	}}
	for _, raw := range []string{"wss://evil.example/resume", "ws://gateway.discord.gg/resume", "wss://u:p@gateway.discord.gg/resume", "wss://gateway.discord.gg:444/resume"} {
		_, _, err := dialer.Dial(raw, nil)
		if err == nil || dials.Load() != 0 {
			t.Fatalf("unsafe resume URL reached network: %q", raw)
		}
	}
}

// The test exercises the exact pinned SDK with fake credentials and only local
// WebSockets. Endpoint security is independently tested with the production
// ProxyRequest guard above; no local-transport exception exists in production.
func TestResumeUsesReadyEndpointAndPreservesSession(t *testing.T) {
	var mu sync.Mutex
	var paths []string
	var packets []struct {
		Op   int `json:"op"`
		Data struct {
			SessionID string `json:"session_id"`
			Sequence  int    `json:"seq"`
		} `json:"d"`
	}
	var resumeURL string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.Close()
		if err := conn.WriteJSON(map[string]any{"op": 10, "d": map[string]any{"heartbeat_interval": 60000}}); err != nil {
			return
		}
		var packet struct {
			Op   int `json:"op"`
			Data struct {
				SessionID string `json:"session_id"`
				Sequence  int    `json:"seq"`
			} `json:"d"`
		}
		if err := conn.ReadJSON(&packet); err != nil {
			return
		}
		mu.Lock()
		paths = append(paths, r.URL.Path)
		packets = append(packets, packet)
		idx := len(paths)
		mu.Unlock()
		if r.URL.Query().Get("v") != discordgo.APIVersion || r.URL.Query().Get("encoding") != "json" {
			t.Error("resume protocol query missing")
		}
		if idx == 1 {
			err = conn.WriteJSON(map[string]any{"op": 0, "t": "READY", "s": 1, "d": map[string]any{"v": 10, "session_id": "offline-session", "resume_gateway_url": resumeURL, "user": map[string]any{"id": "4", "bot": true}, "guilds": []any{}}})
		} else {
			err = conn.WriteJSON(map[string]any{"op": 0, "t": "RESUMED", "s": idx, "d": map[string]any{}})
		}
		if err != nil {
			return
		}
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer server.Close()
	originalURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/original"
	resumeURL = "ws" + strings.TrimPrefix(server.URL, "http") + "/resume"
	s, _ := discordgo.New("Bot offline")
	s.LogLevel, s.ShouldReconnectOnError, s.SyncEvents = -1, false, true
	s.Dialer = &websocket.Dialer{}
	s.Client = &http.Client{Transport: gatewayRoundTrip(func(*http.Request) (*http.Response, error) {
		body, _ := json.Marshal(map[string]string{"url": originalURL})
		return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(string(body)))}, nil
	})}
	defer s.Close()
	for i := 0; i < 3; i++ {
		if err := s.Open(); err != nil {
			t.Fatal(err)
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(paths) != 3 || strings.TrimSuffix(paths[0], "/") != "/original" || paths[1] != "/resume" || paths[2] != "/resume" {
		t.Fatalf("wrong reconnect paths: %v", paths)
	}
	if packets[0].Op != 2 || packets[1].Op != 6 || packets[2].Op != 6 || packets[1].Data.SessionID != "offline-session" || packets[2].Data.SessionID != "offline-session" || packets[1].Data.Sequence != 1 || packets[2].Data.Sequence != 2 {
		t.Fatalf("session/sequence not preserved: %+v", packets)
	}
}

func TestGatewayGuildValidationDisconnectFence(t *testing.T) {
	s, cfg := resilienceGuildFixture(t, resilienceGuildPermissions)
	entered, release := make(chan struct{}), make(chan struct{})
	r := localREST(t, func(w http.ResponseWriter, q *http.Request) {
		if q.URL.Path == "/users/@me" {
			io.WriteString(w, `{"id":"4","bot":true}`)
			return
		}
		close(entered)
		<-release
		io.WriteString(w, `{"id":"2","type":0,"guild_id":"5"}`)
	})
	r.settings.Policy = cfg.Policy
	g := &gatewayState{}
	done := make(chan error, 1)
	go func() { done <- refreshGatewayReadiness(context.Background(), r, s, cfg, g, 0) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("guild validation did not start")
	}
	if !g.ready.Load() {
		t.Fatal("slow guild check held up DM readiness")
	}
	g.disconnect()
	close(release)
	if err := <-done; err != nil || g.ready.Load() || g.guildReadiness() {
		t.Fatal("stale route validation survived disconnect")
	}
}

func TestOwnerDMQueueAndDeliveryWhileGuildUnavailable(t *testing.T) {
	s, cfg := resilienceGuildFixture(t, 0)
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	store, err := OpenStore(filepath.Join(dir, "dm.db"), cfg.Policy)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	var posts atomic.Int32
	r := localREST(t, func(w http.ResponseWriter, q *http.Request) {
		switch q.URL.Path {
		case "/users/@me":
			io.WriteString(w, `{"id":"4","bot":true}`)
		case "/channels/7":
			io.WriteString(w, `{"id":"7","type":1,"recipients":[{"id":"1"}]}`)
		case "/channels/7/messages":
			posts.Add(1)
			var body map[string]any
			if q.Method != http.MethodPost || json.NewDecoder(q.Body).Decode(&body) != nil {
				t.Error("bad DM request")
				return
			}
			body["id"], body["channel_id"], body["author"] = "9", "7", User{ID: "4", Bot: true}
			json.NewEncoder(w).Encode(body)
		default:
			t.Errorf("DM attempted guild lookup or write: %s", q.URL.Path)
			w.WriteHeader(http.StatusForbidden)
		}
	})
	r.settings.Policy = cfg.Policy
	g := &gatewayState{}
	r.routePermission = func(e Envelope) bool { return gatewayRoutePermissions(g, s, cfg, e) }
	if err := refreshGatewayReadiness(context.Background(), r, s, cfg, g, 0); err != nil {
		t.Fatal(err)
	}
	outcome, err := receiveMessage(context.Background(), r, store, cfg, &discordgo.Message{ID: "3", ChannelID: "7", Author: &discordgo.User{ID: "1"}, Content: "offline owner message"})
	if err != nil || outcome != "validation_staged" {
		t.Fatalf("DM staging failed: %s %v", outcome, err)
	}
	in, err := store.NextValidation(wall())
	if err != nil || in == nil {
		t.Fatal("DM not staged")
	}
	if err := processValidation(context.Background(), store, r, g, *in, 0); err != nil {
		t.Fatal(err)
	}
	claim := claimLedger(t, store)
	id := replyLedger(t, store, claim, "offline answer")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err = dispatchLoop(ctx, store, NewWakeHub(), g, func(ctx context.Context, chunk Chunk) SendResult {
		result := r.SendGuarded(ctx, chunk, func() bool { return g.ready.Load() && gatewayRoutePermissions(g, s, cfg, chunk.Source) })
		cancel()
		return result
	})
	if err != nil {
		t.Fatal(err)
	}
	delivery, err := store.Delivery(id)
	if err != nil || delivery.State != "sent" || posts.Load() != 1 || g.guildReadiness() {
		t.Fatalf("DM did not deliver independently: delivery=%+v error=%v posts=%d", delivery, err, posts.Load())
	}
}
