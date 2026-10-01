package headed

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func testQueue(t *testing.T) string {
	t.Helper()
	q := t.TempDir()
	if err := os.Chmod(q, 0700); err != nil {
		t.Fatal(err)
	}
	s := Status{PID: 123, Identity: Identity{PID: 123, BootID: "test", StartTicks: "1"}, Ready: true, UpdatedAt: time.Now().UnixMilli(), Backend: Backend, ChromiumSandbox: true}
	if err := writePrivateJSON(filepath.Join(q, "status.json"), s); err != nil {
		t.Fatal(err)
	}
	return q
}
func waitRequest(t *testing.T, q string) packet {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		files, _ := filepath.Glob(filepath.Join(q, "*.request.json"))
		if len(files) > 0 {
			var p packet
			if err := readJSON(files[0], MaxRequestBytes, &p); err != nil {
				t.Fatal(err)
			}
			return p
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("request did not arrive")
	return packet{}
}
func TestQueueRoundtripAndExactCleanup(t *testing.T) {
	q := testQueue(t)
	other := filepath.Join(q, "unrelated.txt")
	_ = os.WriteFile(other, []byte("keep"), 0600)
	done := make(chan error, 1)
	go func() {
		_, err := (Client{q}).Capture(context.Background(), Request{URL: "https://example.com", TimeoutMS: 1000})
		done <- err
	}()
	p := waitRequest(t, q)
	if p.ExpiresAt <= time.Now().UnixMilli() {
		t.Fatal("expired request")
	}
	if _, err := os.Stat(filepath.Join(q, p.ID+".lease")); err != nil {
		t.Fatal(err)
	}
	env := Envelope{HTML: "public", FinalURL: p.Args.URL, Status: 200, Automation: Backend, ChromiumSandbox: true, Observations: []json.RawMessage{json.RawMessage(`{"type":"fixture"}`)}}
	raw, _ := json.Marshal(env)
	if err := writePrivateJSON(filepath.Join(q, p.ID+".response.json"), response{ID: p.ID, OK: true, Envelope: raw}); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	for _, suffix := range []string{".lease", ".request.json", ".response.json"} {
		if _, err := os.Stat(filepath.Join(q, p.ID+suffix)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("own file left: %s", suffix)
		}
	}
	if data, err := os.ReadFile(other); err != nil || string(data) != "keep" {
		t.Fatal("unrelated file changed")
	}
}
func TestCancelledClientDropsLeaseAndLeavesWorkingFile(t *testing.T) {
	q := testQueue(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := (Client{q}).Capture(ctx, Request{URL: "https://example.com"}); done <- err }()
	p := waitRequest(t, q)
	working := filepath.Join(q, p.ID+".working.json")
	if err := os.Rename(filepath.Join(q, p.ID+".request.json"), working); err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(q, p.ID+".lease")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("lease not cancelled")
	}
	if _, err := os.Stat(working); err != nil {
		t.Fatal("helper working file removed")
	}
}
func TestStaleServiceFailsClosed(t *testing.T) {
	q := testQueue(t)
	s, _ := ReadStatus(q)
	s.UpdatedAt = 1
	_ = writePrivateJSON(filepath.Join(q, "status.json"), s)
	_, err := (Client{q}).Capture(context.Background(), Request{URL: "https://example.com"})
	if err == nil || !strings.Contains(err.Error(), "unavailable") {
		t.Fatal(err)
	}
}
func TestInsecureQueueRejected(t *testing.T) {
	q := testQueue(t)
	_ = os.Chmod(q, 0755)
	if _, err := ReadStatus(q); err == nil {
		t.Fatal("public queue accepted")
	}
}
func TestMissingHeadedProofRejected(t *testing.T) {
	q := testQueue(t)
	done := make(chan error, 1)
	go func() {
		_, err := (Client{q}).Capture(context.Background(), Request{URL: "https://example.com"})
		done <- err
	}()
	p := waitRequest(t, q)
	_ = writePrivateJSON(filepath.Join(q, p.ID+".response.json"), response{ID: p.ID, OK: true, Envelope: json.RawMessage(`{"html":"x","automation":"playwright_headed_chromium","chromiumSandbox":true}`)})
	if err := <-done; err == nil || !strings.Contains(err.Error(), "proof") {
		t.Fatal(err)
	}
}
func TestWrongResponseIdentityRejected(t *testing.T) {
	q := testQueue(t)
	done := make(chan error, 1)
	go func() {
		_, err := (Client{q}).Capture(context.Background(), Request{URL: "https://example.com"})
		done <- err
	}()
	p := waitRequest(t, q)
	_ = writePrivateJSON(filepath.Join(q, p.ID+".response.json"), response{ID: "wrong", OK: true, Envelope: json.RawMessage(`{}`)})
	if err := <-done; err == nil || !strings.Contains(err.Error(), "identity") {
		t.Fatal(err)
	}
}
func TestServerErrorsAreVisible(t *testing.T) {
	q := testQueue(t)
	done := make(chan error, 1)
	go func() {
		_, err := (Client{q}).Capture(context.Background(), Request{URL: "https://example.com"})
		done <- err
	}()
	p := waitRequest(t, q)
	_ = writePrivateJSON(filepath.Join(q, p.ID+".response.json"), response{ID: p.ID, Error: "EACCES fixture"})
	if err := <-done; err == nil || !strings.Contains(err.Error(), "EACCES") {
		t.Fatal(err)
	}
}
func TestUnsafeURLsNeverCreateQueueFiles(t *testing.T) {
	for _, raw := range []string{"file:///etc/passwd", "javascript:alert(1)", "https:///missing-host", "https://user:fixture@example.com/", "http://@example.com/"} {
		q := testQueue(t)
		_, err := (Client{q}).Capture(context.Background(), Request{URL: raw})
		if err == nil {
			t.Fatalf("accepted %s", raw)
		}
		files, _ := os.ReadDir(q)
		if len(files) != 1 || files[0].Name() != "status.json" {
			t.Fatalf("unsafe URL created files: %v", files)
		}
	}
}
func TestUnsafeStatusIsNotHealthy(t *testing.T) {
	q := testQueue(t)
	s, _ := ReadStatus(q)
	if !s.Healthy() {
		t.Fatal("baseline unhealthy")
	}
	for _, edit := range []func(*Status){func(s *Status) { s.Headless = true }, func(s *Status) { s.ChromiumSandbox = false }, func(s *Status) { s.Backend = "unexpected" }} {
		v := s
		edit(&v)
		if v.Healthy() {
			t.Fatal("unsafe status advertised healthy")
		}
	}
}
