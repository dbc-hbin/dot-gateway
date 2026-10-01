package headed

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestProcessIdentityMatchesSelf(t *testing.T) {
	value, err := ProcessIdentity(os.Getpid())
	if err != nil || value.PID != os.Getpid() || value.BootID == "" || value.StartTicks == "" {
		t.Fatalf("%+v %v", value, err)
	}
}
func TestLauncherLockSerializesConcurrentRecovery(t *testing.T) {
	q := testQueue(t)
	release, err := acquireLauncher(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	_, err = acquireLauncher(ctx, q)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("lock did not exclude second launcher: %v", err)
	}
	release()
	next, err := acquireLauncher(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	next()
}
func TestStartWithoutDisplayRefuses(t *testing.T) {
	t.Setenv("DISPLAY", "")
	t.Setenv("WAYLAND_DISPLAY", "")
	_, err := StartDetached(context.Background(), StartOptions{Root: t.TempDir()})
	if err == nil {
		t.Fatal("displayless startup accepted")
	}
}
func TestStopUsesOwnQueueAndIdentity(t *testing.T) {
	q := testQueue(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := Stop(ctx, q); done <- err }()
	for i := 0; i < 100; i++ {
		if _, err := os.Stat(filepath.Join(q, "stop")); err == nil {
			break
		}
		time.Sleep(time.Millisecond)
	}
	s, _ := ReadStatus(q)
	s.Ready = false
	_ = writePrivateJSON(filepath.Join(q, "status.json"), s)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
func TestRestartWithoutDisplayDoesNotStopWorkingHelper(t *testing.T) {
	q := testQueue(t)
	t.Setenv("DISPLAY", "")
	t.Setenv("WAYLAND_DISPLAY", "")
	if _, err := Restart(context.Background(), StartOptions{Queue: q}); err == nil {
		t.Fatal("restart accepted")
	}
	if _, err := os.Stat(filepath.Join(q, "stop")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("working helper was stopped")
	}
}
func TestKnownDeadRestartRecoversStaleReadyAndLock(t *testing.T) {
	q := testQueue(t)
	s, _ := ReadStatus(q)
	_ = writePrivateJSON(filepath.Join(q, "service.lock"), s.Identity)
	inspect := func(int) (Identity, error) { return Identity{}, os.ErrNotExist }
	if err := reconcileDeadForRestart(q, inspect); err != nil {
		t.Fatal(err)
	}
	current, _ := ReadStatus(q)
	if current.Ready {
		t.Fatal("dead helper remains ready")
	}
	if _, err := os.Stat(filepath.Join(q, "service.lock")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("dead lock retained")
	}
}
func TestRestartNeverTouchesReusedPID(t *testing.T) {
	q := testQueue(t)
	before, _ := ReadStatus(q)
	inspect := func(pid int) (Identity, error) { return Identity{PID: pid, BootID: "another", StartTicks: "999"}, nil }
	if err := reconcileDeadForRestart(q, inspect); err == nil {
		t.Fatal("reused PID accepted")
	}
	after, _ := ReadStatus(q)
	if after != before {
		t.Fatal("state changed")
	}
}
