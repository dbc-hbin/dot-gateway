package main

import (
	"dot-gateway/internal/bridge"
	"testing"
	"time"
)

func TestCLIPollHealthWaitingAndTimeoutCleanup(t *testing.T) {
	s, env := cliFixture(t)
	done := make(chan map[string]any, 1)
	go func() {
		r, e := cli(t, env, "", "next", "--wait", "0.7", "--consumer-id", "offline")
		if e != nil {
			r["test_error"] = true
		}
		done <- r
	}()
	observed := false
	for deadline := time.Now().Add(500 * time.Millisecond); time.Now().Before(deadline); {
		out, e := s.Status()
		if e != nil {
			t.Fatal(e)
		}
		if out["consumer"].(map[string]any)["waiting_cli_fresh"] == true {
			observed = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !observed {
		t.Fatal("waiting CLI was not observed")
	}
	select {
	case r := <-done:
		if r["message"] != nil || r["test_error"] != nil {
			t.Fatal(r)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout did not return")
	}
	out, e := s.Status()
	if e != nil {
		t.Fatal(e)
	}
	h := out["consumer"].(map[string]any)
	if h["waiting_cli_fresh"] != false || h["state"] != "absent" {
		t.Fatal("terminal poll remained healthy", h)
	}
}
func TestCLIPollHealthClaimAndRecoveredFastPath(t *testing.T) {
	s, env := cliFixture(t)
	if _, e := s.Ingest(bridge.Envelope{Platform: "discord", EventID: "3", ConversationID: "2", SenderID: "1", Text: "offline", ReceivedAt: float64(time.Now().Unix()), RouteKind: "dm"}); e != nil {
		t.Fatal(e)
	}
	var token any
	for i := 0; i < 2; i++ {
		r, e := cli(t, env, "", "next", "--wait", "3", "--begin", "--consumer-id", "offline")
		if e != nil {
			t.Fatal(r, e)
		}
		claim := r["message"].(map[string]any)["claim"]
		if i == 1 && claim != token {
			t.Fatal("recovery changed claim")
		}
		token = claim
		out, e := s.Status()
		if e != nil {
			t.Fatal(e)
		}
		h := out["consumer"].(map[string]any)
		if h["state"] != "claim_committed" || h["waiting_cli_fresh"] != false || h["active_processing_leases"] != 1 {
			t.Fatal(h)
		}
	}
}

func TestCLIPollHealthOverlappingTimeoutsDoNotClobber(t *testing.T) {
	s, env := cliFixture(t)
	first := make(chan map[string]any, 1)
	second := make(chan map[string]any, 1)
	for _, p := range []struct {
		wait string
		done chan map[string]any
	}{{"0.5", first}, {"1.0", second}} {
		go func(wait string, done chan map[string]any) {
			r, e := cli(t, env, "", "next", "--wait", wait, "--consumer-id", "same")
			if e != nil {
				r["test_error"] = true
			}
			done <- r
		}(p.wait, p.done)
	}
	observed := false
	for deadline := time.Now().Add(400 * time.Millisecond); time.Now().Before(deadline); {
		out, e := s.Status()
		if e != nil {
			t.Fatal(e)
		}
		if out["consumer"].(map[string]any)["fresh_waiting_calls"] == 2 {
			observed = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !observed {
		t.Fatal("overlapping CLI instances not both observed")
	}
	select {
	case r := <-first:
		if r["message"] != nil || r["test_error"] != nil {
			t.Fatal(r)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("first timeout did not return")
	}
	out, e := s.Status()
	if e != nil {
		t.Fatal(e)
	}
	if out["consumer"].(map[string]any)["fresh_waiting_calls"] != 1 {
		t.Fatal("old CLI cleanup clobbered active call", out)
	}
	select {
	case r := <-second:
		if r["message"] != nil || r["test_error"] != nil {
			t.Fatal(r)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("second timeout did not return")
	}
	out, e = s.Status()
	if e != nil {
		t.Fatal(e)
	}
	if out["consumer"].(map[string]any)["fresh_waiting_calls"] != 0 {
		t.Fatal("terminal CLI calls remain fresh", out)
	}
}
func TestCLIPollHealthWaitingClaimCleanup(t *testing.T) {
	s, env := cliFixture(t)
	done := make(chan map[string]any, 1)
	go func() {
		r, e := cli(t, env, "", "next", "--wait", "3", "--begin", "--consumer-id", "offline")
		if e != nil {
			r["test_error"] = true
		}
		done <- r
	}()
	observed := false
	for deadline := time.Now().Add(time.Second); time.Now().Before(deadline); {
		out, e := s.Status()
		if e != nil {
			t.Fatal(e)
		}
		if out["consumer"].(map[string]any)["fresh_waiting_calls"] == 1 {
			observed = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !observed {
		t.Fatal("waiting call not observed")
	}
	if _, e := s.Ingest(bridge.Envelope{Platform: "discord", EventID: "3", ConversationID: "2", SenderID: "1", Text: "offline", ReceivedAt: float64(time.Now().Unix()), RouteKind: "dm"}); e != nil {
		t.Fatal(e)
	}
	select {
	case r := <-done:
		if r["message"] == nil || r["test_error"] != nil {
			t.Fatal(r)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("claim did not return")
	}
	out, e := s.Status()
	if e != nil {
		t.Fatal(e)
	}
	h := out["consumer"].(map[string]any)
	if h["state"] != "claim_committed" || h["fresh_waiting_calls"] != 0 || h["active_processing_leases"] != 1 {
		t.Fatal("claimed CLI still reported waiting", h)
	}
}
