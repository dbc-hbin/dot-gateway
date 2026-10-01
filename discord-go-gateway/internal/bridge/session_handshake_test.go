package bridge

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/gorilla/websocket"
)

type handshakePacket struct {
	Op   int `json:"op"`
	Data struct {
		SessionID string `json:"session_id"`
		Sequence  int    `json:"seq"`
	} `json:"d"`
}

func localGatewaySession(t *testing.T, originalURL string) *discordgo.Session {
	t.Helper()
	s, err := discordgo.New("Bot offline")
	if err != nil {
		t.Fatal(err)
	}
	s.LogLevel, s.ShouldReconnectOnError, s.SyncEvents = -1, false, true
	s.Dialer = &websocket.Dialer{}
	s.Client = &http.Client{Transport: gatewayRoundTrip(func(*http.Request) (*http.Response, error) {
		body, _ := json.Marshal(map[string]string{"url": originalURL})
		return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(string(body)))}, nil
	})}
	return s
}

func writeHandshakeReady(c *websocket.Conn, resumeURL string) error {
	return c.WriteJSON(map[string]any{"op": 0, "t": "READY", "s": 1, "d": map[string]any{"v": 10, "session_id": "offline-session", "resume_gateway_url": resumeURL, "user": map[string]any{"id": "4", "bot": true}, "guilds": []any{}}})
}

func TestGatewayHandshakeRestartPreservesOrResetsSession(t *testing.T) {
	for _, tc := range []struct {
		name      string
		op        int
		resumable bool
		finalPath string
		finalOp   int
	}{
		{"resumable", 9, true, "/resume", 6},
		{"nonresumable", 9, false, "/original", 2},
		{"reconnect", 7, false, "/resume", 6},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var mu sync.Mutex
			var paths []string
			var packets []handshakePacket
			var resumeURL string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				c, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
				if err != nil {
					t.Error(err)
					return
				}
				defer c.Close()
				if err := c.WriteJSON(map[string]any{"op": 10, "d": map[string]int{"heartbeat_interval": 60000}}); err != nil {
					return
				}
				var packet handshakePacket
				if err := c.ReadJSON(&packet); err != nil {
					return
				}
				mu.Lock()
				paths = append(paths, strings.TrimSuffix(r.URL.Path, "/"))
				packets = append(packets, packet)
				idx := len(paths)
				mu.Unlock()
				if idx == 2 {
					err = c.WriteJSON(map[string]any{"op": tc.op, "d": tc.resumable})
				} else if packet.Op == 2 {
					err = writeHandshakeReady(c, resumeURL)
				} else {
					err = c.WriteJSON(map[string]any{"op": 0, "t": "RESUMED", "s": 2, "d": map[string]any{}})
				}
				if err != nil {
					return
				}
				for {
					if _, _, err := c.ReadMessage(); err != nil {
						return
					}
				}
			}))
			defer server.Close()
			originalURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/original"
			resumeURL = "ws" + strings.TrimPrefix(server.URL, "http") + "/resume"
			s := localGatewaySession(t, originalURL)
			defer s.Close()
			if err := s.Open(); err != nil {
				t.Fatal(err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			result := make(chan error, 1)
			go func() { result <- s.Open() }()
			select {
			case err := <-result:
				if !errors.Is(err, discordgo.ErrGatewayReconnect) {
					t.Fatalf("unexpected handshake result: %v", err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("handshake restart deadlocked under Open mutex")
			}
			if err := s.Open(); err != nil {
				t.Fatalf("retry retained lock/socket: %v", err)
			}
			mu.Lock()
			defer mu.Unlock()
			if len(paths) != 3 || paths[0] != "/original" || paths[1] != "/resume" || paths[2] != tc.finalPath || packets[1].Op != 6 || packets[2].Op != tc.finalOp {
				t.Fatalf("wrong restart binding: paths=%v packets=%+v", paths, packets)
			}
			if tc.finalOp == 6 && (packets[2].Data.SessionID != "offline-session" || packets[2].Data.Sequence != 1) {
				t.Fatal("resumable session was discarded")
			}
		})
	}
}

func TestGatewayHandshakeRestartsShareDeadlineWithoutFlood(t *testing.T) {
	var opens atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close()
		opens.Add(1)
		c.WriteJSON(map[string]any{"op": 10, "d": map[string]int{"heartbeat_interval": 60000}})
		c.ReadMessage()
		c.WriteJSON(map[string]any{"op": 9, "d": false})
		c.ReadMessage()
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	s := localGatewaySession(t, "ws"+strings.TrimPrefix(server.URL, "http"))
	defer s.Close()
	d := newGatewayDialer(ctx)
	defer d.closeConnections()
	started := time.Now()
	err := openGateway(ctx, s, d)
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(started) > time.Second || opens.Load() != 1 {
		t.Fatalf("unbounded restart: err=%v elapsed=%v opens=%d", err, time.Since(started), opens.Load())
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.conns) != 0 {
		t.Fatal("invalid-session attempt leaked an active connection")
	}
}

func TestGatewayHandshakeRestartCanCompleteWithinOriginalDeadline(t *testing.T) {
	var opens atomic.Int32
	var baseURL string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close()
		idx := opens.Add(1)
		c.WriteJSON(map[string]any{"op": 10, "d": map[string]int{"heartbeat_interval": 60000}})
		var packet handshakePacket
		if err := c.ReadJSON(&packet); err != nil {
			return
		}
		if packet.Op != 2 {
			t.Error("fresh nonresumable session did not identify")
		}
		if idx == 1 {
			c.WriteJSON(map[string]any{"op": 9, "d": false})
		} else {
			writeHandshakeReady(c, baseURL)
		}
		for {
			if _, _, err := c.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer server.Close()
	baseURL = "ws" + strings.TrimPrefix(server.URL, "http")
	s := localGatewaySession(t, baseURL)
	defer s.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	d := newGatewayDialer(ctx)
	defer d.closeConnections()
	started := time.Now()
	if err := openGateway(ctx, s, d); err != nil || opens.Load() != 2 {
		t.Fatalf("bounded retry failed: err=%v opens=%d", err, opens.Load())
	}
	if time.Since(started) < time.Second {
		t.Fatal("identify retry ignored minimum invalid-session delay")
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.conns) != 1 {
		t.Fatalf("expected one active session, got %d", len(d.conns))
	}
}

func TestGatewayUnexpectedHeartbeatAckDuringOpenDoesNotDeadlock(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close()
		c.WriteJSON(map[string]any{"op": 11, "d": nil})
		c.ReadMessage()
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	s := localGatewaySession(t, "ws"+strings.TrimPrefix(server.URL, "http"))
	defer s.Close()
	d := newGatewayDialer(ctx)
	defer d.closeConnections()
	if err := openGateway(ctx, s, d); err == nil {
		t.Fatal("invalid Hello accepted")
	}
}

func TestGatewayDoesNotFollowHandshakeRedirect(t *testing.T) {
	var targets atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { targets.Add(1) }))
	defer target.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", "ws"+strings.TrimPrefix(target.URL, "http"))
		w.WriteHeader(http.StatusFound)
	}))
	defer redirect.Close()
	s := localGatewaySession(t, "ws"+strings.TrimPrefix(redirect.URL, "http"))
	defer s.Close()
	if err := s.Open(); err == nil || targets.Load() != 0 {
		t.Fatalf("gateway redirect followed: err=%v targets=%d", err, targets.Load())
	}
}
