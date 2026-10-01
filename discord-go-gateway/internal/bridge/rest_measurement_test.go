package bridge

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptrace"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func syntheticMeasuredREST(t *testing.T, transport roundTripFunc) *RESTClient {
	t.Helper()
	r, err := NewRESTClient(Settings{Policy: testPolicy(), ExpectedBotID: "4", Token: "offline-measurement-token"})
	if err != nil {
		t.Fatal(err)
	}
	r.baseURL = "https://synthetic.invalid"
	r.client.Transport = transport
	t.Cleanup(r.Close)
	return r
}
func measuredResponse(body string) *http.Response {
	return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body))}
}
func measuredAck(c Chunk) string { b, _ := json.Marshal(ackFor(c)); return string(b) }

type delayedMeasurementBody struct {
	io.ReadCloser
	delay time.Duration
	once  sync.Once
}

func (b *delayedMeasurementBody) Read(p []byte) (int, error) {
	b.once.Do(func() { time.Sleep(b.delay) })
	return b.ReadCloser.Read(p)
}

func TestMeasuredRequestsSeparateColdGETsRateWaitAndPOSTBody(t *testing.T) {
	c := testChunk()
	c.Text = "private-measurement-content"
	var posts atomic.Int32
	r := syntheticMeasuredREST(t, func(req *http.Request) (*http.Response, error) {
		trace := httptrace.ContextClientTrace(req.Context())
		trace.GetConn("private-host-not-retained")
		switch req.URL.Path {
		case "/users/@me":
			trace.DNSStart(httptrace.DNSStartInfo{Host: "private-host-not-retained"})
			time.Sleep(time.Millisecond)
			trace.DNSDone(httptrace.DNSDoneInfo{})
			trace.ConnectStart("tcp", "private-address-not-retained")
			time.Sleep(time.Millisecond)
			trace.ConnectDone("tcp", "private-address-not-retained", nil)
			trace.TLSHandshakeStart()
			time.Sleep(time.Millisecond)
			trace.TLSHandshakeDone(tls.ConnectionState{}, nil)
			trace.GotConn(httptrace.GotConnInfo{Reused: false})
			return measuredResponse(`{"id":"4","bot":true}`), nil
		case "/channels/2":
			trace.GotConn(httptrace.GotConnInfo{Reused: true})
			time.Sleep(60 * time.Millisecond)
			return measuredResponse(`{"id":"2","type":1,"recipients":[{"id":"1"}]}`), nil
		default:
			posts.Add(1)
			trace.GotConn(httptrace.GotConnInfo{Reused: true})
			time.Sleep(5 * time.Millisecond)
			resp := measuredResponse(measuredAck(c))
			resp.Body = &delayedMeasurementBody{ReadCloser: resp.Body, delay: 25 * time.Millisecond}
			return resp, nil
		}
	})
	now := time.Now()
	identityRoute, _ := limitRoute(http.MethodGet, "/users/@me")
	postRoute, _ := limitRoute(http.MethodPost, "/channels/2/messages")
	r.limits = map[string]time.Time{identityRoute: now.Add(30 * time.Millisecond), postRoute: now.Add(180 * time.Millisecond)}
	got, d := r.SendGuardedMeasured(context.Background(), c, nil)
	if got.State != "sent" || posts.Load() != 1 {
		t.Fatalf("result=%+v posts=%d", got, posts.Load())
	}
	if d.IdentityGET.RateLimitWaitSeconds < .015 || d.ChannelGET.RateLimitWaitSeconds != 0 || d.Post.RateLimitWaitSeconds < .025 {
		t.Fatalf("rate waits: %+v", d)
	}
	if d.IdentityGET.Reused || !d.ChannelGET.Reused || !d.Post.Reused || !d.Reused {
		t.Fatalf("connection association: %+v", d)
	}
	if !d.IdentityGET.DNSObserved || !d.IdentityGET.ConnectObserved || !d.IdentityGET.TLSObserved || d.IdentityGET.DNSSeconds <= 0 || d.IdentityGET.ConnectSeconds <= 0 || d.IdentityGET.TLSSeconds <= 0 {
		t.Fatalf("cold phases absent: %+v", d.IdentityGET)
	}
	if d.ChannelGET.DNSObserved || d.Post.ConnectObserved || d.Post.TLSObserved {
		t.Fatalf("cold trace leaked between requests: %+v", d)
	}
	if d.ChannelGET.Seconds < .05 || d.PreflightSeconds < d.ChannelGET.Seconds || d.Post.BodySeconds-d.Post.HeadersSeconds < .020 {
		t.Fatalf("durations lost: %+v", d)
	}
	for _, phase := range []RequestDiagnostics{d.IdentityGET, d.ChannelGET, d.Post} {
		if !phase.Attempted || !phase.HeadersReceived || !phase.BodyFinished || !phase.BodyComplete || !phase.ConnectionObserved || phase.Seconds < phase.BodySeconds || phase.BodySeconds < phase.HeadersSeconds {
			t.Fatalf("phase incomplete: %+v", phase)
		}
	}
	if d.PostSeconds < d.Post.BodySeconds || d.Seconds < d.PreflightSeconds+d.PostSeconds {
		t.Fatalf("legacy meanings changed: %+v", d)
	}
	b, err := json.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{c.Text, c.ReplyID, Nonce(c), "private-host", "private-address", "offline-measurement-token", "discord.com", "users/@me", "channels/2"} {
		if strings.Contains(string(b), secret) {
			t.Fatalf("measurement retained private material: %q", secret)
		}
	}
}

func TestMeasuredIdentityFailureCancelsChannelAndRetainsError(t *testing.T) {
	for _, failure := range []struct {
		name, body, code string
		status           int
	}{
		{"mismatch", `{"id":"5","bot":true}`, "preflight_identity_mismatch", 200},
		{"denied", `{}`, "preflight_http_403", 403},
		{"invalid", `not-json`, "preflight_invalid_ack", 200},
	} {
		t.Run(failure.name, func(t *testing.T) {
			channelStarted := make(chan struct{})
			channelCancelled := make(chan struct{})
			var posts atomic.Int32
			r := localREST(t, func(w http.ResponseWriter, req *http.Request) {
				switch req.URL.Path {
				case "/users/@me":
					<-channelStarted
					w.WriteHeader(failure.status)
					io.WriteString(w, failure.body)
				case "/channels/2":
					close(channelStarted)
					select {
					case <-req.Context().Done():
						close(channelCancelled)
					case <-time.After(2 * time.Second):
						t.Error("identity failure did not cancel sibling")
					}
				default:
					posts.Add(1)
				}
			})
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			started := time.Now()
			got, d := r.SendGuardedMeasured(ctx, testChunk(), nil)
			if got.State != "failed" || got.Code != failure.code || posts.Load() != 0 || d.Post.Attempted {
				t.Fatalf("result=%+v diagnostics=%+v posts=%d", got, d, posts.Load())
			}
			if time.Since(started) > time.Second {
				t.Fatal("identity failure waited for slow sibling")
			}
			select {
			case <-channelCancelled:
			case <-time.After(time.Second):
				t.Fatal("server did not observe cancellation")
			}
		})
	}
}

func TestMeasuredPreflightParentCancellationNeverPosts(t *testing.T) {
	for _, diagnostic := range []bool{false, true} {
		t.Run(fmt.Sprint(diagnostic), func(t *testing.T) {
			var gets, posts atomic.Int32
			started := make(chan struct{})
			r := syntheticMeasuredREST(t, func(req *http.Request) (*http.Response, error) {
				if req.Method != http.MethodGet {
					posts.Add(1)
					return nil, errors.New("unexpected-post")
				}
				if gets.Add(1) == 2 {
					close(started)
				}
				<-req.Context().Done()
				return nil, req.Context().Err()
			})
			r.settings.Policy.GuildID, r.settings.Policy.GuildChannelID = "5", "2"
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan SendResult, 1)
			go func() {
				if diagnostic {
					result, _ := r.SendDiagnosticMeasured(ctx, transportDiagnostic(), nil)
					done <- result
				} else {
					result, _ := r.SendGuardedMeasured(ctx, testChunk(), nil)
					done <- result
				}
			}()
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("preflights not started")
			}
			cancel()
			select {
			case got := <-done:
				if got.State != "failed" || got.Code != "preflight_transport_failed" || posts.Load() != 0 {
					t.Fatalf("result=%+v posts=%d", got, posts.Load())
				}
			case <-time.After(time.Second):
				t.Fatal("cancelled preflight did not finish")
			}
		})
	}
}

func TestMeasuredUnavailableTraceConservativelyUncertainAndNoReplay(t *testing.T) {
	for _, diagnostic := range []bool{false, true} {
		t.Run(fmt.Sprint(diagnostic), func(t *testing.T) {
			var posts atomic.Int32
			r := syntheticMeasuredREST(t, func(req *http.Request) (*http.Response, error) {
				switch req.URL.Path {
				case "/users/@me":
					return measuredResponse(`{"id":"4","bot":true}`), nil
				case "/channels/2":
					if diagnostic {
						return measuredResponse(`{"id":"2","type":0,"guild_id":"5"}`), nil
					}
					return measuredResponse(`{"id":"2","type":1,"recipients":[{"id":"1"}]}`), nil
				}
				posts.Add(1)
				if req.GetBody != nil || req.Header.Get("Idempotency-Key") != "" || req.Header.Get("X-Idempotency-Key") != "" {
					t.Error("POST replay enabled")
				}
				return nil, errors.New("ack lost after untraced write")
			})
			r.settings.Policy.GuildID, r.settings.Policy.GuildChannelID = "5", "2"
			var result SendResult
			var d Diagnostics
			if diagnostic {
				result, d = r.SendDiagnosticMeasured(context.Background(), transportDiagnostic(), nil)
			} else {
				result, d = r.SendGuardedMeasured(context.Background(), testChunk(), nil)
			}
			if result.State != "uncertain" || result.Code != "request_or_ack_failed" || posts.Load() != 1 || !d.Post.Attempted || d.Post.ConnectionObserved || d.Reused || d.Post.HeadersReceived || d.Post.BodyComplete {
				t.Fatalf("result=%+v diagnostics=%+v posts=%d", result, d, posts.Load())
			}
		})
	}
}

func TestMeasuredFreshDMRecipientCheckForEveryChunk(t *testing.T) {
	var channels, posts atomic.Int32
	r := syntheticMeasuredREST(t, func(req *http.Request) (*http.Response, error) {
		switch req.URL.Path {
		case "/users/@me":
			return measuredResponse(`{"id":"4","bot":true}`), nil
		case "/channels/2":
			if channels.Add(1) == 1 {
				return measuredResponse(`{"id":"2","type":1,"recipients":[{"id":"1"}]}`), nil
			}
			return measuredResponse(`{"id":"2","type":1,"recipients":[{"id":"7"}]}`), nil
		}
		posts.Add(1)
		return measuredResponse(measuredAck(testChunk())), nil
	})
	if got, _ := r.SendGuardedMeasured(context.Background(), testChunk(), nil); got.State != "sent" {
		t.Fatal(got)
	}
	c := testChunk()
	c.Index++
	got, d := r.SendGuardedMeasured(context.Background(), c, nil)
	if got.Code != "preflight_recipient_mismatch" || posts.Load() != 1 || channels.Load() != 2 || d.Post.Attempted {
		t.Fatalf("result=%+v diagnostics=%+v", got, d)
	}
}

func TestMeasuredConcurrentOrdinaryAndDiagnosticStayAssociated(t *testing.T) {
	c := testChunk()
	c.Source.RouteKind = "guild_text"
	c.Source.GuildID = "5"
	c.Source.BotMentioned = true
	c.Text = "ordinary\n"
	diagnostic := transportDiagnostic()
	var posts atomic.Int32
	r := diagnosticREST(t, func(w http.ResponseWriter, req *http.Request) {
		if preflightHandler(w, req) {
			return
		}
		posts.Add(1)
		var body map[string]json.RawMessage
		if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
			t.Error(err)
			return
		}
		time.Sleep(time.Millisecond)
		if _, ordinary := body["message_reference"]; ordinary {
			ack := ackFor(c)
			ack["content"] = "ordinary"
			json.NewEncoder(w).Encode(ack)
		} else {
			json.NewEncoder(w).Encode(diagnosticAck(diagnostic))
		}
	})
	var wg sync.WaitGroup
	for n := range 24 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var result SendResult
			var d Diagnostics
			operation, code := "send", "ack_terminal_lf_removed"
			if n%2 == 0 {
				result, d = r.SendGuardedMeasured(context.Background(), c, nil)
			} else {
				operation, code = "diagnostic", ""
				result, d = r.SendDiagnosticMeasured(context.Background(), diagnostic, nil)
			}
			if result.State != "sent" || d.State != result.State || d.Code != result.Code || d.Operation != operation || d.Code != code || !d.Post.BodyComplete {
				t.Errorf("result=%+v diagnostics=%+v", result, d)
			}
			frozen := d
			for range 3 {
				_ = r.Diagnostics()
				time.Sleep(time.Millisecond)
			}
			if d != frozen {
				t.Error("returned measurement changed")
			}
		}()
	}
	wg.Wait()
	if posts.Load() != 24 {
		t.Fatalf("posts=%d", posts.Load())
	}
}

func TestMeasuredSendLockWaitExcludedFromLegacySeconds(t *testing.T) {
	r := syntheticMeasuredREST(t, func(*http.Request) (*http.Response, error) {
		t.Error("invalid chunk made request")
		return nil, errors.New("unexpected")
	})
	r.sendMu.Lock()
	started := make(chan struct{})
	done := make(chan Diagnostics, 1)
	go func() { close(started); _, d := r.SendGuardedMeasured(context.Background(), Chunk{}, nil); done <- d }()
	<-started
	time.Sleep(50 * time.Millisecond)
	r.sendMu.Unlock()
	d := <-done
	if d.SendLockWaitSeconds < .025 || d.Seconds >= d.SendLockWaitSeconds || d.Post.Attempted {
		t.Fatalf("lock wait not separated: %+v", d)
	}
}

func TestMeasuredTraceSnapshotFrozenDuringLateCallbacks(t *testing.T) {
	ctx, measurement := newRequestMeasurement(context.Background())
	trace := httptrace.ContextClientTrace(ctx)
	trace.ConnectStart("tcp", "never-retained")
	frozen := measurement.finish()
	var wg sync.WaitGroup
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			trace.ConnectDone("tcp", "never-retained", nil)
			trace.GotConn(httptrace.GotConnInfo{Reused: true})
			measurement.bodyFinished(true)
		}()
	}
	wg.Wait()
	if got := measurement.finish(); got != frozen {
		t.Fatalf("late callback changed snapshot: %+v vs %+v", got, frozen)
	}
}

func TestMeasuredIdentityFailureCancelsSiblingRateWait(t *testing.T) {
	var channels, posts atomic.Int32
	r := syntheticMeasuredREST(t, func(req *http.Request) (*http.Response, error) {
		if req.URL.Path == "/users/@me" {
			return measuredResponse(`{"id":"5","bot":true}`), nil
		}
		if req.Method == http.MethodGet {
			channels.Add(1)
		} else {
			posts.Add(1)
		}
		return nil, errors.New("request should remain unsent")
	})
	route, _ := limitRoute(http.MethodGet, "/channels/2")
	r.limits = map[string]time.Time{route: time.Now().Add(2 * time.Second)}
	started := time.Now()
	result, d := r.SendGuardedMeasured(context.Background(), testChunk(), nil)
	if result.Code != "preflight_identity_mismatch" || channels.Load() != 0 || posts.Load() != 0 || d.ChannelGET.Attempted || d.Post.Attempted || d.ChannelGET.RateLimitWaitSeconds <= 0 || time.Since(started) > time.Second {
		t.Fatalf("result=%+v diagnostics=%+v", result, d)
	}
}

func TestMeasuredTracedConnectFailureRemainsDefinitelyUnsent(t *testing.T) {
	var posts atomic.Int32
	r := syntheticMeasuredREST(t, func(req *http.Request) (*http.Response, error) {
		switch req.URL.Path {
		case "/users/@me":
			return measuredResponse(`{"id":"4","bot":true}`), nil
		case "/channels/2":
			return measuredResponse(`{"id":"2","type":1,"recipients":[{"id":"1"}]}`), nil
		}
		posts.Add(1)
		trace := httptrace.ContextClientTrace(req.Context())
		trace.GetConn("not-retained")
		trace.ConnectStart("tcp", "not-retained")
		err := errors.New("synthetic dial failure")
		trace.ConnectDone("tcp", "not-retained", err)
		return nil, err
	})
	result, d := r.SendGuardedMeasured(context.Background(), testChunk(), nil)
	if result.State != "failed" || result.Code != "connect_failed" || posts.Load() != 1 || !d.Post.Attempted || d.Post.ConnectionObserved || !d.Post.ConnectObserved {
		t.Fatalf("result=%+v diagnostics=%+v", result, d)
	}
}
