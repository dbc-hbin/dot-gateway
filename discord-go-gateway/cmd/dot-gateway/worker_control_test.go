package main

import (
	"dot-gateway/internal/bridge"
	"testing"
	"time"
)

func TestCLIWorkerControlLifecycle(t *testing.T) {
	s, env := cliFixture(t)
	args := []string{"worker-register", "/root/worker", "--incarnation", "turn-1", "--controller", "/root", "--evidence-ref", "native:start-1"}
	r, err := cli(t, env, "", args...)
	if err != nil || r["state"] != "running" || r["observation_source"] != "controller_attestation" {
		t.Fatal(r, err)
	}
	until := r["lease_until"]
	r, err = cli(t, env, "", args...)
	if err != nil || r["lease_until"] != until {
		t.Fatal("register retry changed lease", r, err)
	}
	e := bridge.Envelope{Platform: "discord", EventID: "3", ConversationID: "2", SenderID: "1", Text: "hello", ReceivedAt: float64(time.Now().Unix()), RouteKind: "dm"}
	if _, err = s.Ingest(e); err != nil {
		t.Fatal(err)
	}
	c, err := s.ClaimNext(60, 0)
	if err != nil || c == nil {
		t.Fatal(c, err)
	}
	r, err = cli(t, env, "", "worker-bind", c.InboundID, "--claim", c.Claim, "--worker-id", "/root/worker", "--incarnation", "turn-1", "--controller", "/root", "--evidence-ref", "native:claimed-1")
	if err != nil {
		t.Fatal(r, err)
	}
	status, err := s.ControlRequests(e, c.InboundID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.CancelControlRequest(e, c.InboundID, status[0].Revision); err != nil {
		t.Fatal(err)
	}
	r, err = cli(t, env, "", "worker-cancellations", "/root/worker", "--incarnation", "turn-1", "--controller", "/root")
	if err != nil || len(r["cancellations"].([]any)) != 1 {
		t.Fatal(r, err)
	}
	r, err = cli(t, env, "", "worker-cancel-ack", c.InboundID, "--worker-id", "/root/worker", "--incarnation", "turn-1", "--controller", "/root", "--evidence-ref", "native:interrupt-1")
	if err != nil || r["state"] != "acknowledged" {
		t.Fatal(r, err)
	}
	r, err = cli(t, env, "", "worker-status")
	if err != nil || r["pending_cancellations"] != float64(0) || r["native_control_available"] != false {
		t.Fatal(r, err)
	}
	r, err = cli(t, env, "", "worker-register", "/root/worker", "--incarnation", "turn-2", "--controller", "/root", "--previous-incarnation", "turn-1", "--evidence-ref", "native:resume-2")
	if err != nil || r["incarnation"] != "turn-2" {
		t.Fatal(r, err)
	}
	r, err = cli(t, env, "", "worker-observe", "/root/worker", "--incarnation", "turn-1", "--controller", "/root", "--state", "running", "--evidence-ref", "native:old")
	if err == nil || r["error"] != "worker_incarnation_fenced" {
		t.Fatal(r, err)
	}
}
func TestCLIWorkerControlValidation(t *testing.T) {
	_, env := cliFixture(t)
	for _, args := range [][]string{{"worker-register"}, {"worker-status", "unexpected"}, {"worker-register", "w", "--extra", "x"}, {"worker-register", "w", "--incarnation", "i", "--controller", "c"}, {"worker-observe", "w", "--lease-seconds", "NaN"}} {
		r, err := cli(t, env, "", args...)
		if err == nil || r["error"] == nil {
			t.Fatal(args, r, err)
		}
	}
}
