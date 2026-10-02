package bridge

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/gorilla/websocket"
)

// These fakes never open a network connection, including on a test failure.
type receiveOnlySpyTransport struct {
	calls  int
	closed bool
}

func (s *receiveOnlySpyTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	s.calls++
	if req.Body != nil {
		_ = req.Body.Close()
	}
	return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("{}")), Request: req}, nil
}
func (s *receiveOnlySpyTransport) CloseIdleConnections() { s.closed = true }

type receiveOnlyBody struct{ closed bool }

func (*receiveOnlyBody) Read([]byte) (int, error) { return 0, io.EOF }
func (b *receiveOnlyBody) Close() error           { b.closed = true; return nil }

func TestReceiveOnlyTransportRejectsEveryNonReadMethod(t *testing.T) {
	for _, method := range []string{"", "GET", "HEAD", "POST", "PATCH", "DELETE", "PUT", "OPTIONS", "TRACE", "CONNECT", "get"} {
		t.Run(method, func(t *testing.T) {
			next := &receiveOnlySpyTransport{}
			tr := &receiveOnlyTransport{next: next}
			body := &receiveOnlyBody{}
			req, err := http.NewRequest(http.MethodGet, "https://example.invalid/test", body)
			if err != nil {
				t.Fatal(err)
			}
			req.Method = method // Exercise the empty method too: net/http treats it as GET.
			resp, err := tr.RoundTrip(req)
			allowed := method == "" || method == "GET" || method == "HEAD"
			if allowed {
				if err != nil || resp == nil || next.calls != 1 {
					t.Fatalf("read blocked: response=%v error=%v calls=%d", resp, err, next.calls)
				}
				_ = resp.Body.Close()
			} else if !errors.Is(err, errReceiveOnly) || resp != nil || next.calls != 0 || !body.closed {
				t.Fatalf("write fence failed: response=%v error=%v calls=%d closed=%v", resp, err, next.calls, body.closed)
			}
			tr.CloseIdleConnections()
			if !next.closed {
				t.Fatal("transport did not close idle connections")
			}
		})
	}
}

func TestReceiveOnlyRESTAndInteractionFences(t *testing.T) {
	cfg := Settings{Policy: testPolicy(), ExpectedBotID: "4", Token: "synthetic", ReceiveOnly: true}
	r, err := NewRESTClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if _, ok := r.Client().Transport.(*receiveOnlyTransport); !ok {
		t.Fatal("SDK client has no final receive-only fence")
	}
	interaction, err := newInteractionTransport(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer interaction.close()
	if _, ok := interaction.client.Transport.(*receiveOnlyTransport); !ok || !interaction.receiveOnly {
		t.Fatal("interaction client did not inherit receive-only mode")
	}
	// Replacing the final transport still cannot bypass either request-level fence.
	next := &receiveOnlySpyTransport{}
	r.client.Transport = next
	interaction.client.Transport = next
	for _, method := range []string{"POST", "PATCH", "DELETE", "PUT", "OPTIONS", "TRACE", "CONNECT", "get"} {
		if _, err := r.request(context.Background(), method, "/test", nil); !errors.Is(err, errReceiveOnly) {
			t.Fatalf("REST method %s: %v", method, err)
		}
		for _, upload := range []bool{false, true} {
			guardCalled := false
			ack, status := interaction.requestBytes(context.Background(), method, "/test", nil, "application/json", upload, func() bool { guardCalled = true; return true })
			if ack != nil || status != "failed" || guardCalled {
				t.Fatalf("interaction write advanced: method=%s upload=%v status=%s", method, upload, status)
			}
		}
	}
	if next.calls != 0 {
		t.Fatal("blocked writes reached the network transport")
	}
	resp, err := r.request(context.Background(), http.MethodGet, "/test", nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if _, status := interaction.requestBytes(context.Background(), http.MethodGet, "/test", nil, "application/json", false, nil); status != "sent" || next.calls != 2 {
		t.Fatalf("read path blocked: status=%s calls=%d", status, next.calls)
	}
}

func TestReceiveOnlySkipsWriterBeforeScheduling(t *testing.T) {
	for _, receiveOnly := range []bool{true, false} {
		scheduled, called := false, false
		startGatewayWriter(receiveOnly, func(fn func()) { scheduled = true; fn() }, func() { called = true })
		if scheduled == receiveOnly || called == receiveOnly {
			t.Fatalf("receive_only=%v scheduled=%v called=%v", receiveOnly, scheduled, called)
		}
	}
}

func TestReceiveOnlySettingsRequireExactBooleans(t *testing.T) {
	for _, key := range []string{"BRIDGE_RECEIVE_ONLY", "BRIDGE_KEEP_CATCHUP_DISARMED"} {
		for _, value := range []string{"true", "false", "", "TRUE", "1", " true", "false "} {
			t.Run(key+"="+value, func(t *testing.T) {
				env := map[string]string{"DISCORD_OWNER_ID": "1", "DISCORD_ALLOWED_DM_IDS": "1", key: value}
				s, err := SettingsFromEnv(env, false, false)
				if value != "true" && value != "false" {
					if err == nil || err.Error() != "invalid_boolean_"+strings.ToLower(key) {
						t.Fatalf("ambiguous safety flag accepted: %v", err)
					}
					return
				}
				got := s.ReceiveOnly
				if key == "BRIDGE_KEEP_CATCHUP_DISARMED" {
					got = s.KeepCatchupDisarmed
				}
				if err != nil || got != (value == "true") {
					t.Fatalf("flag ignored: settings=%+v error=%v", s, err)
				}
			})
		}
	}
}

func TestKeepCatchupDisarmedPreservesStateAndRejectsRunnableLeftovers(t *testing.T) {
	for _, tc := range []struct {
		name, state, route string
		requested, reject  bool
	}{
		{name: "unarmed"},
		{name: "restored", state: "disarmed_restore"},
		{name: "disarmed_route", state: "disarmed_restore", route: "disarmed"},
		{name: "continuity", state: "disarmed_continuity"},
		{name: "armed", state: "armed", reject: true},
		{name: "unknown", state: "unknown", reject: true},
		{name: "idle_route", state: "disarmed_restore", route: "idle", reject: true},
		{name: "gap_route", state: "disarmed_restore", route: "gap", reject: true},
		{name: "request", state: "disarmed_restore", requested: true, reject: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, path := threadStore(t)
			_, err := s.call(func(db *storeConn) (any, error) {
				if tc.state != "" {
					if _, err := db.Exec(`INSERT INTO catchup_meta VALUES('state',?)`, tc.state); err != nil {
						return nil, err
					}
				}
				if tc.route != "" {
					if _, err := db.Exec(`INSERT INTO catchup_routes(channel_id,route,floor,state,updated) VALUES('2','{}','1',?,1)`, tc.route); err != nil {
						return nil, err
					}
				}
				if tc.requested {
					_, err := db.Exec(`INSERT INTO catchup_requested VALUES('2','3')`)
					return nil, err
				}
				return nil, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			cfg := Settings{DBPath: path, Policy: threadPolicy(), ExpectedBotID: "4", KeepCatchupDisarmed: true}
			err = prepareGatewayCatchup(s, cfg, time.Now())
			if (err != nil) != tc.reject || err != nil && err.Error() != "catchup_must_already_be_disarmed" {
				t.Fatalf("prepare: %v", err)
			}
			if err = s.ActivateCatchup(cfg, time.Now()); err == nil || err.Error() != "catchup_activation_disabled" {
				t.Fatalf("direct activation bypass: %v", err)
			}
			if _, err := os.Lstat(path + ".catchup-live"); !os.IsNotExist(err) {
				t.Fatalf("created catchup witness: %v", err)
			}
			// Read inside the actor, then assert on the test goroutine.
			value, err := s.call(func(db *storeConn) (any, error) {
				var state string
				err := db.QueryRow(`SELECT COALESCE((SELECT value FROM catchup_meta WHERE key='state'),'')`).Scan(&state)
				return state, err
			})
			if err != nil || value != tc.state {
				t.Fatalf("disarm reason changed: got=%v error=%v", value, err)
			}
		})
	}
}

func TestDisabledCatchupLoopDoesNotTouchStore(t *testing.T) {
	// A nil store would panic if the disabled loop attempted even a single pass.
	r := &RESTClient{settings: Settings{KeepCatchupDisarmed: true}}
	if err := catchupLoop(context.Background(), nil, r, nil, nil); err != nil {
		t.Fatal(err)
	}
}

func TestGatewayErrorsRemainSymbolic(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want string
	}{
		{&discordgo.RESTError{Response: &http.Response{StatusCode: 403}, ResponseBody: []byte("synthetic-private-detail")}, "gateway_discovery_http_403"},
		{fmt.Errorf("synthetic-private-detail: %w", websocket.ErrBadHandshake), "gateway_websocket_bad_handshake"},
		{&net.OpError{Op: "dial", Err: errors.New("synthetic-private-detail")}, "gateway_network_connection_failed"},
		{errors.New("proxyconnect: synthetic-private-detail"), "gateway_connection_proxy_connect_failed"},
		{errors.New("unexpected EOF: synthetic-private-detail"), "gateway_connection_unexpected_eof"},
		{errReceiveOnly, "gateway_cause_receive_only_write_blocked"},
	} {
		if got := ClassifyGatewayError(tc.err); got == nil || got.Error() != tc.want || strings.Contains(got.Error(), "synthetic-private-detail") {
			t.Fatalf("classification: got=%v want=%s", got, tc.want)
		}
	}
}
