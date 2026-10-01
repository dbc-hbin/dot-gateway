package bridge

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func testChunk() Chunk {
	return Chunk{ReplyID: "reply-1", Index: 0, Text: "answer", Source: testEnvelope()}
}
func ackFor(c Chunk) map[string]any {
	return map[string]any{"id": "9", "channel_id": "2", "author": User{ID: "4", Bot: true}, "content": c.Text, "nonce": Nonce(c), "message_reference": map[string]any{"message_id": "3", "channel_id": "2"}}
}
func localREST(t *testing.T, h http.HandlerFunc) *RESTClient {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	r, e := NewRESTClient(Settings{Policy: testPolicy(), ExpectedBotID: "4", Token: "offline", HTTPKeepaliveSeconds: 120})
	if e != nil {
		t.Fatal(e)
	}
	r.baseURL = srv.URL
	r.client.Transport = http.DefaultTransport.(*http.Transport).Clone()
	t.Cleanup(r.Close)
	return r
}
func preflightHandler(w http.ResponseWriter, r *http.Request) bool {
	switch r.URL.Path {
	case "/users/@me":
		io.WriteString(w, `{"id":"4","bot":true}`)
		return true
	case "/channels/2":
		io.WriteString(w, `{"id":"2","type":1,"recipients":[{"id":"1"}]}`)
		return true
	}
	return false
}
func TestSendAckAndPayload(t *testing.T) {
	c := testChunk()
	var posts atomic.Int32
	r := localREST(t, func(w http.ResponseWriter, r *http.Request) {
		if preflightHandler(w, r) {
			return
		}
		posts.Add(1)
		if r.Method != "POST" || r.URL.Path != "/channels/2/messages" {
			t.Error("wrong request")
		}
		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)
		if body["nonce"] != Nonce(c) || body["enforce_nonce"] != true {
			t.Error("nonce missing")
		}
		json.NewEncoder(w).Encode(ackFor(c))
	})
	result := r.Send(context.Background(), c)
	if result.State != "sent" || result.MessageID != "9" || posts.Load() != 1 {
		t.Fatalf("%+v posts %d", result, posts.Load())
	}
}
func TestNoRedirectOrHTTPRetry(t *testing.T) {
	for _, status := range []int{302, 307, 429, 500, 503} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			var posts atomic.Int32
			r := localREST(t, func(w http.ResponseWriter, r *http.Request) {
				if preflightHandler(w, r) {
					return
				}
				posts.Add(1)
				w.Header().Set("Location", "/other")
				w.WriteHeader(status)
			})
			got := r.Send(context.Background(), testChunk())
			if posts.Load() != 1 {
				t.Fatalf("retried %d", posts.Load())
			}
			want := "uncertain"
			if status == 429 {
				want = "failed"
			}
			if got.State != want {
				t.Fatalf("%+v", got)
			}
		})
	}
}
func TestLostAckNeverRetried(t *testing.T) {
	var posts atomic.Int32
	r := localREST(t, func(w http.ResponseWriter, r *http.Request) {
		if preflightHandler(w, r) {
			return
		}
		posts.Add(1)
		h := w.(http.Hijacker)
		c, _, _ := h.Hijack()
		c.Close()
	})
	got := r.Send(context.Background(), testChunk())
	if got.State != "uncertain" || posts.Load() != 1 || !r.Diagnostics().Reused {
		t.Fatalf("%+v posts %d", got, posts.Load())
	}
}
func TestPreflightParallel(t *testing.T) {
	var arrived atomic.Int32
	both := make(chan struct{})
	var once sync.Once
	r := localREST(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			if arrived.Add(1) == 2 {
				once.Do(func() { close(both) })
			}
			select {
			case <-both:
			case <-time.After(time.Second):
				t.Error("reads not parallel")
			}
			preflightHandler(w, r)
			return
		}
		json.NewEncoder(w).Encode(ackFor(testChunk()))
	})
	if got := r.Send(context.Background(), testChunk()); got.State != "sent" {
		t.Fatalf("%+v", got)
	}
}
func TestGuardAndBadIdentityNeverPost(t *testing.T) {
	var posts atomic.Int32
	r := localREST(t, func(w http.ResponseWriter, r *http.Request) {
		if preflightHandler(w, r) {
			return
		}
		posts.Add(1)
	})
	got := r.SendGuarded(context.Background(), testChunk(), func() bool { return false })
	if got.State != "failed" || posts.Load() != 0 {
		t.Fatal(got)
	}
	r2 := localREST(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/users/@me" {
			io.WriteString(w, `{"id":"5","bot":true}`)
			return
		}
		if preflightHandler(w, r) {
			return
		}
		posts.Add(1)
	})
	got = r2.Send(context.Background(), testChunk())
	if got.State != "failed" || posts.Load() != 0 {
		t.Fatal(got)
	}
}
func TestAckValidation(t *testing.T) {
	c := testChunk()
	for _, key := range []string{"id", "channel_id", "author", "content", "nonce", "message_reference"} {
		a := ackFor(c)
		a[key] = "wrong"
		b, _ := json.Marshal(a)
		var raw map[string]json.RawMessage
		json.Unmarshal(b, &raw)
		if id, _ := validateAck(raw, c, "4"); id != "" {
			t.Fatal("invalid accepted", key)
		}
	}
	c.Text = "hello\n"
	a := ackFor(c)
	a["content"] = "hello"
	b, _ := json.Marshal(a)
	var raw map[string]json.RawMessage
	json.Unmarshal(b, &raw)
	if id, code := validateAck(raw, c, "4"); id != "9" || code != "ack_terminal_lf_removed" {
		t.Fatal("LF failed")
	}
	c.Text = "hello \n"
	if id, _ := validateAck(raw, c, "4"); id != "" {
		t.Fatal("excess whitespace accepted")
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func TestPostBodyNotReplayable(t *testing.T) {
	r, e := NewRESTClient(Settings{Policy: testPolicy(), ExpectedBotID: "4"})
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close()
	r.client.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.GetBody != nil || req.Header.Get("Idempotency-Key") != "" || req.Header.Get("X-Idempotency-Key") != "" {
			t.Fatal("replay enabled")
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{}`)), Header: http.Header{}}, nil
	})
	resp, e := r.request(context.Background(), "POST", "/channels/2/messages", []byte(`{}`))
	if e != nil {
		t.Fatal(e)
	}
	resp.Body.Close()
}

func TestSendSerialized(t *testing.T) {
	var active, max atomic.Int32
	r := localREST(t, func(w http.ResponseWriter, r *http.Request) {
		if preflightHandler(w, r) {
			return
		}
		n := active.Add(1)
		for {
			old := max.Load()
			if n <= old || max.CompareAndSwap(old, n) {
				break
			}
		}
		time.Sleep(15 * time.Millisecond)
		json.NewEncoder(w).Encode(ackFor(testChunk()))
		active.Add(-1)
	})
	var wg sync.WaitGroup
	for range 5 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if got := r.Send(context.Background(), testChunk()); got.State != "sent" {
				t.Errorf("%+v", got)
			}
		}()
	}
	wg.Wait()
	if max.Load() != 1 {
		t.Fatal("concurrent posts", max.Load())
	}
}

func TestRateLimitPacesNextUnsentChunk(t *testing.T) {
	var posts atomic.Int32
	var first time.Time
	var gap time.Duration
	r := localREST(t, func(w http.ResponseWriter, r *http.Request) {
		if preflightHandler(w, r) {
			return
		}
		n := posts.Add(1)
		if n == 1 {
			first = time.Now()
			w.Header().Set("X-RateLimit-Remaining", "0")
			w.Header().Set("X-RateLimit-Reset-After", "0.12")
			w.Header().Set("X-RateLimit-Bucket", "messages")
		} else {
			gap = time.Since(first)
		}
		json.NewEncoder(w).Encode(ackFor(testChunk()))
	})
	for range 2 {
		if got := r.Send(context.Background(), testChunk()); got.State != "sent" {
			t.Fatal(got)
		}
	}
	if posts.Load() != 2 || gap < 100*time.Millisecond {
		t.Fatalf("posts=%d gap=%s", posts.Load(), gap)
	}
}
func Test429NeverRetriesButPacesLaterIntent(t *testing.T) {
	var posts atomic.Int32
	var first time.Time
	r := localREST(t, func(w http.ResponseWriter, r *http.Request) {
		if preflightHandler(w, r) {
			return
		}
		if posts.Add(1) == 1 {
			first = time.Now()
			w.Header().Set("Retry-After", "0.12")
			w.WriteHeader(429)
			io.WriteString(w, `{"retry_after":0.12,"global":false}`)
			return
		}
		json.NewEncoder(w).Encode(ackFor(testChunk()))
	})
	if got := r.Send(context.Background(), testChunk()); got.State != "failed" || got.Code != "http_429" || posts.Load() != 1 {
		t.Fatal(got)
	}
	if got := r.Send(context.Background(), testChunk()); got.State != "sent" {
		t.Fatal(got)
	}
	if time.Since(first) < 100*time.Millisecond || posts.Load() != 2 {
		t.Fatal("budget not enforced")
	}
}
func TestGlobalFeedbackBudgetAndCancelledWait(t *testing.T) {
	var posts atomic.Int32
	r := localREST(t, func(w http.ResponseWriter, r *http.Request) {
		if preflightHandler(w, r) {
			return
		}
		if r.URL.Path == "/channels/2/typing" {
			w.WriteHeader(429)
			io.WriteString(w, `{"retry_after":0.15,"global":true}`)
			return
		}
		posts.Add(1)
		json.NewEncoder(w).Encode(ackFor(testChunk()))
	})
	if e := r.Typing(context.Background(), testEnvelope()); e == nil {
		t.Fatal("expected rate limit")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if got := r.Send(ctx, testChunk()); got.State != "failed" || posts.Load() != 0 {
		t.Fatal(got)
	}
	started := time.Now()
	if got := r.Send(context.Background(), testChunk()); got.State != "sent" {
		t.Fatal(got)
	}
	if time.Since(started) < 100*time.Millisecond || posts.Load() != 1 {
		t.Fatal("global budget ignored")
	}
}

func TestGuardRecheckedAfterBudgetWait(t *testing.T) {
	var posts atomic.Int32
	var ready atomic.Bool
	ready.Store(true)
	r := localREST(t, func(w http.ResponseWriter, r *http.Request) {
		if preflightHandler(w, r) {
			return
		}
		posts.Add(1)
		json.NewEncoder(w).Encode(ackFor(testChunk()))
	})
	route, _ := limitRoute("POST", "/channels/2/messages")
	r.limits = map[string]time.Time{route: time.Now().Add(100 * time.Millisecond)}
	time.AfterFunc(25*time.Millisecond, func() { ready.Store(false) })
	got := r.SendGuarded(context.Background(), testChunk(), ready.Load)
	if got.Code != "connection_changed_before_send" || posts.Load() != 0 {
		t.Fatal(got)
	}
}
