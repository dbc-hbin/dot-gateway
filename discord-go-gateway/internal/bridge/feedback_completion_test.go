package bridge

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestFeedbackSuccessfulCompletionOnlyRemovesOwnReactions(t *testing.T) {
	for _, old := range []string{"", "received", "failed", "uncertain"} {
		t.Run(old, func(t *testing.T) {
			var routes []string
			r := localREST(t, func(w http.ResponseWriter, q *http.Request) {
				if preflightHandler(w, q) {
					return
				}
				routes = append(routes, q.Method+" "+q.URL.Path)
				w.WriteHeader(204)
			})
			if err := updateFeedback(context.Background(), r, testEnvelope(), old, "sent"); err != nil {
				t.Fatal(err)
			}
			want := 1
			if old == "failed" || old == "uncertain" {
				want = 2
			}
			if len(routes) != want {
				t.Fatal(routes)
			}
			for _, route := range routes {
				if !strings.HasPrefix(route, "DELETE ") || !strings.HasSuffix(route, "/@me") || strings.Contains(route, "✅") {
					t.Fatal("success added or removed another user's reaction", routes)
				}
			}
			if !strings.Contains(routes[len(routes)-1], "👀") {
				t.Fatal("receipt was not cleared", routes)
			}
			if want == 2 && !strings.Contains(routes[0], "❌") {
				t.Fatal("earlier failure was not cleared", routes)
			}
		})
	}
}
func TestFeedbackFailureAndUncertainRemainVisible(t *testing.T) {
	for _, target := range []string{"failed", "uncertain"} {
		t.Run(target, func(t *testing.T) {
			var routes []string
			r := localREST(t, func(w http.ResponseWriter, q *http.Request) {
				if preflightHandler(w, q) {
					return
				}
				routes = append(routes, q.Method+" "+q.URL.Path)
				w.WriteHeader(204)
			})
			if err := updateFeedback(context.Background(), r, testEnvelope(), "received", target); err != nil {
				t.Fatal(err)
			}
			if len(routes) != 2 || !strings.HasPrefix(routes[0], "DELETE ") || !strings.Contains(routes[0], "👀") || !strings.HasPrefix(routes[1], "PUT ") || !strings.Contains(routes[1], "❌") {
				t.Fatal(routes)
			}
		})
	}
}
func TestFeedbackSentCleanupPersistsAndDoesNotRepeat(t *testing.T) {
	s, claim := typingFixture(t)
	id, err := s.QueueReply(claim.InboundID, claim.Claim, "offline answer")
	if err != nil {
		t.Fatal(err)
	}
	chunk, err := s.NextChunk()
	if err != nil || chunk == nil {
		t.Fatal(err)
	}
	if err = s.RecordResult(*chunk, SendResult{State: "sent", MessageID: "5"}); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var routes []string
	r := localREST(t, func(w http.ResponseWriter, q *http.Request) {
		if preflightHandler(w, q) {
			return
		}
		mu.Lock()
		routes = append(routes, q.Method+" "+q.URL.Path)
		mu.Unlock()
		w.WriteHeader(204)
	})
	g := &gatewayState{}
	g.ready.Store(true)
	for i := 0; i < 2; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		err = feedbackLoop(ctx, s, r, NewWakeHub(), g)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(routes) != 1 || !strings.HasPrefix(routes[0], "DELETE ") || !strings.Contains(routes[0], "👀") {
		t.Fatal(routes)
	}
	v, err := s.call(func(db *storeConn) (any, error) {
		var stage string
		err := db.QueryRow("SELECT stage FROM feedback WHERE inbound_id=?", claim.InboundID).Scan(&stage)
		return stage, err
	})
	if err != nil || v != "sent" {
		t.Fatal(v, err)
	}
	d, err := s.Delivery(id)
	if err != nil || d.State != "sent" || d.Chunks[0].Attempts != 1 {
		t.Fatal(d, err)
	}
}
func TestFeedbackSentCleanupFailureCannotMarkComplete(t *testing.T) {
	r := localREST(t, func(w http.ResponseWriter, q *http.Request) {
		if preflightHandler(w, q) {
			return
		}
		if q.Method != http.MethodDelete || !strings.Contains(q.URL.Path, "👀") {
			t.Error("unexpected success mutation")
		}
		w.WriteHeader(503)
	})
	if err := updateFeedback(context.Background(), r, testEnvelope(), "received", "sent"); err == nil {
		t.Fatal("cleanup failure swallowed")
	}
}
