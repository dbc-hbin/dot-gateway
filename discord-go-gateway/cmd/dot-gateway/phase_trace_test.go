package main

import (
	"database/sql"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"dot-gateway/internal/bridge"
)

func traceDB(env []string) string {
	for _, e := range env {
		if strings.HasPrefix(e, "BRIDGE_DB=") {
			return strings.TrimPrefix(e, "BRIDGE_DB=")
		}
	}
	return ""
}
func traceCommand(t *testing.T, env []string, args ...string) *exec.Cmd {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(exe, append([]string{"-test.run=^TestCLIProcessHelper$", "--"}, args...)...)
	cmd.Env = append(env, "DOT_CLI_TEST_HELPER=1", "GORACE=atexit_sleep_ms=0", "BRIDGE_PHASE_TRACE=1")
	return cmd
}
func readTrace(t *testing.T, path string) []phaseTrace {
	t.Helper()
	b, err := os.ReadFile(path + ".phase-trace")
	if err != nil {
		t.Fatal(err)
	}
	var out []phaseTrace
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		var r phaseTrace
		if err := json.Unmarshal([]byte(line), &r); err != nil {
			t.Fatal(err)
		}
		out = append(out, r)
	}
	return out
}
func requirePhases(t *testing.T, tr phaseTrace, phases ...string) {
	t.Helper()
	pos := 0
	last := int64(-1)
	if len(tr.InvocationID) != 32 {
		t.Fatal("missing opaque invocation ID")
	}
	for _, p := range tr.Phases {
		if p.ElapsedNS < last {
			t.Fatal("nonmonotonic phases")
		}
		last = p.ElapsedNS
		if _, err := time.Parse(time.RFC3339Nano, p.At); err != nil {
			t.Fatal(err)
		}
		if pos < len(phases) && p.Phase == phases[pos] {
			pos++
		}
	}
	if pos != len(phases) {
		t.Fatalf("missing ordered phases: %#v wanted %v", tr.Phases, phases)
	}
}
func TestPhaseTraceEmptyStdoutExactAndDisabledByDefault(t *testing.T) {
	_, env := cliFixture(t)
	cmd := traceCommand(t, env, "next")
	out, err := cmd.Output()
	if err != nil || string(out) != "{\"message\":null}\n" {
		t.Fatalf("stdout %q: %v", out, err)
	}
	tr := readTrace(t, traceDB(env))
	if len(tr) != 1 || tr[0].InboundID != "" {
		t.Fatal(tr)
	}
	requirePhases(t, tr[0], "cli_enter", "store_open_started", "store_open_finished", "claim_call_started", "claim_returned", "stdout_ready", "stdout_write_started", "stdout_write_finished")
	before, _ := os.ReadFile(traceDB(env) + ".phase-trace")
	if _, err := cli(t, env, "", "next"); err != nil {
		t.Fatal(err)
	}
	after, _ := os.ReadFile(traceDB(env) + ".phase-trace")
	if string(before) != string(after) {
		t.Fatal("tracing unexpectedly enabled")
	}
}
func TestPhaseTraceReplyInputAndCommittedQueue(t *testing.T) {
	s, env := cliFixture(t)
	_, err := s.Ingest(bridge.Envelope{Platform: "discord", EventID: "3", ConversationID: "2", SenderID: "1", Text: "secret-inbound-marker", ReceivedAt: 1, RouteKind: "dm"})
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := cli(t, env, "", "next")
	if err != nil {
		t.Fatal(err)
	}
	m := claimed["message"].(map[string]any)
	id, claim := m["inbound_id"].(string), m["claim"].(string)
	cmd := traceCommand(t, env, "reply", id, "--claim", claim)
	input, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		time.Sleep(90 * time.Millisecond)
		_, _ = input.Write([]byte("secret-reply-marker"))
		_ = input.Close()
	}()
	out, err := cmd.Output()
	if err != nil {
		t.Fatal(err, string(out))
	}
	tr := readTrace(t, traceDB(env))
	requirePhases(t, tr[0], "reply_input_read_started", "reply_input_read_finished", "queue_call_started", "queue_returned", "notify_started", "notify_finished", "stdout_write_finished")
	var start, end int64
	for _, p := range tr[0].Phases {
		if p.Phase == "reply_input_read_started" {
			start = p.ElapsedNS
		}
		if p.Phase == "reply_input_read_finished" {
			end = p.ElapsedNS
		}
	}
	if end-start < int64(30*time.Millisecond) {
		t.Fatal("input delay not measured")
	}
	if tr[0].InboundID != id {
		t.Fatal("missing inbound correlation")
	}
	data, _ := os.ReadFile(traceDB(env) + ".phase-trace")
	for _, secret := range []string{"secret-inbound-marker", "secret-reply-marker", claim, "conversation_id", "sender_id", "claim\"", "http"} {
		if strings.Contains(string(data), secret) {
			t.Fatalf("trace leaks %q", secret)
		}
	}
	// Replaying the same reply remains idempotent with trace enabled.
	replay := traceCommand(t, env, "reply", id, "--claim", claim)
	replay.Stdin = strings.NewReader("secret-reply-marker")
	out2, err := replay.Output()
	if err != nil || string(out2) != string(out) {
		t.Fatal("reply retry changed delivery", err)
	}
}
func TestPhaseTraceStoreOpenLockAndFailedStdout(t *testing.T) {
	s, env := cliFixture(t)
	_, err := s.Ingest(bridge.Envelope{Platform: "discord", EventID: "3", ConversationID: "2", SenderID: "1", Text: "offline", ReceivedAt: 1, RouteKind: "dm"})
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", traceDB(env))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if _, err = db.Exec("BEGIN IMMEDIATE"); err != nil {
		t.Fatal(err)
	}
	cmd := traceCommand(t, env, "next", "--consumer-id", "trace-recovery")
	w, err := os.OpenFile("/dev/full", os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	cmd.Stdout = w
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	if _, err = db.Exec("ROLLBACK"); err != nil {
		t.Fatal(err)
	}
	if err = cmd.Wait(); err == nil {
		t.Fatal("expected stdout failure")
	}
	tr := readTrace(t, traceDB(env))
	requirePhases(t, tr[0], "store_open_started", "store_open_finished", "claim_returned", "stdout_write_started", "stdout_write_failed")
	for _, p := range tr[0].Phases {
		if p.Phase == "stdout_write_finished" {
			t.Fatal("failed write claimed completion")
		}
	}
	got, err := cli(t, env, "", "next", "--consumer-id", "trace-recovery")
	if err != nil || got["message"] == nil {
		t.Fatal("claim not recoverable", err)
	}
	if got["message"].(map[string]any)["inbound_id"] != tr[0].InboundID {
		t.Fatal("claim changed")
	}
}
func TestPhaseTraceRejectsUnsafeSinkAndBoundsRetention(t *testing.T) {
	for _, kind := range []string{"symlink", "fifo", "shared", "locked"} {
		t.Run(kind, func(t *testing.T) {
			dir := t.TempDir()
			os.Chmod(dir, 0700)
			path := filepath.Join(dir, "queue.db.phase-trace")
			switch kind {
			case "symlink":
				os.Symlink(filepath.Join(dir, "target"), path)
			case "fifo":
				if err := syscall.Mkfifo(path, 0600); err != nil {
					t.Fatal(err)
				}
			case "shared":
				os.WriteFile(path, []byte("unchanged"), 0644)
			case "locked":
				f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0600)
				if err != nil {
					t.Fatal(err)
				}
				defer f.Close()
				if err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
					t.Fatal(err)
				}
			}
			tr := newPhaseTrace(true)
			tr.setPath(filepath.Join(dir, "queue.db"))
			start := time.Now()
			tr.flush()
			if time.Since(start) > 250*time.Millisecond {
				t.Fatal("trace blocked command")
			}
			if kind == "symlink" {
				if _, err := os.Lstat(filepath.Join(dir, "target")); !os.IsNotExist(err) {
					t.Fatal("followed symlink")
				}
			}
			if kind == "shared" {
				b, _ := os.ReadFile(path)
				if string(b) != "unchanged" {
					t.Fatal("modified shared file")
				}
			}
		})
	}
	dir := t.TempDir()
	os.Chmod(dir, 0700)
	path := filepath.Join(dir, "queue.db")
	os.WriteFile(path+".phase-trace", make([]byte, phaseTraceLimit), 0600)
	tr := newPhaseTrace(true)
	tr.setPath(path)
	for i := 0; i < 100; i++ {
		tr.mark("stdout_ready")
	}
	tr.mark("secret-token-value")
	tr.setInbound("not-an-inbound-secret")
	tr.flush()
	records := readTrace(t, path)
	if len(records) != 1 || len(records[0].Phases) != phaseTraceMaxPhases || records[0].InboundID != "" {
		t.Fatal("trace not bounded")
	}
	st, _ := os.Stat(path + ".phase-trace")
	if st.Size() > phaseTraceLimit {
		t.Fatal("oversize trace")
	}
}

func TestPhaseTraceFailureDoesNotFailCommittedReply(t *testing.T) {
	s, env := cliFixture(t)
	if _, err := s.Ingest(bridge.Envelope{Platform: "discord", EventID: "3", ConversationID: "2", SenderID: "1", Text: "offline", ReceivedAt: 1, RouteKind: "dm"}); err != nil {
		t.Fatal(err)
	}
	claim, err := s.ClaimNext(300, 0)
	if err != nil || claim == nil {
		t.Fatal(err)
	}
	// An unsafe preexisting sink must be ignored, not alter delivery or trigger
	// an application retry after the durable queue transaction succeeds.
	if err = os.WriteFile(traceDB(env)+".phase-trace", []byte("unchanged"), 0644); err != nil {
		t.Fatal(err)
	}
	cmd := traceCommand(t, env, "reply", claim.InboundID, "--claim", claim.Claim)
	cmd.Stdin = strings.NewReader("offline answer")
	out, err := cmd.Output()
	if err != nil {
		t.Fatal(err)
	}
	var delivery bridge.Delivery
	if err = json.Unmarshal(out, &delivery); err != nil || delivery.State != "queued" {
		t.Fatal(delivery, err)
	}
	b, _ := os.ReadFile(traceDB(env) + ".phase-trace")
	if string(b) != "unchanged" {
		t.Fatal("unsafe trace sink modified")
	}
	chunk, err := s.NextChunk()
	if err != nil || chunk == nil || chunk.ReplyID != delivery.ReplyID {
		t.Fatal(chunk, err)
	}
	if err = s.RecordResult(*chunk, bridge.SendResult{State: "sent", MessageID: "5"}); err != nil {
		t.Fatal(err)
	}
	if next, err := s.NextChunk(); err != nil || next != nil {
		t.Fatal("unexpected duplicate send", next, err)
	}
}
