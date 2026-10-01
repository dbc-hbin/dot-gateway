package bridge

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"
)

func wakeTestPath(t *testing.T) string {
	t.Helper()
	// Keep the Unix socket path short even with descriptive test names.
	dir, err := os.MkdirTemp("", "dot-wake-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return filepath.Join(dir, "wake.sock")
}

type wakeTestTransport struct {
	dial   ipcDialFunc
	accept func() (net.Conn, error)
}

func wakeTestListener(t *testing.T, path string) *wakeTestTransport {
	t.Helper()
	l, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if errors.Is(err, syscall.EPERM) || errors.Is(err, syscall.EACCES) {
		t.Log("Unix socket creation unavailable; exercising IPC with a local net.Pipe transport")
		client, server := net.Pipe()
		t.Cleanup(func() { client.Close(); server.Close() })
		return &wakeTestTransport{
			dial:   func(context.Context, string, string) (net.Conn, error) { return client, nil },
			accept: func() (net.Conn, error) { return server, nil },
		}
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	if err := l.SetDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	return &wakeTestTransport{
		dial:   (&net.Dialer{}).DialContext,
		accept: func() (net.Conn, error) { return l.AcceptUnix() },
	}
}

func acceptWakeTestPeer(t *testing.T, l *wakeTestTransport) net.Conn {
	t.Helper()
	c, err := l.accept()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	if err := c.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	line, err := bufio.NewReader(c).ReadString('\n')
	if err != nil || line != "watch\n" {
		t.Fatalf("watch request = %q, %v", line, err)
	}
	return c
}

func receiveWake(t *testing.T, watch *WakeWatch, within time.Duration) {
	t.Helper()
	timer := time.NewTimer(within)
	defer timer.Stop()
	select {
	case <-watch.Events():
	case <-timer.C:
		t.Fatalf("no wake within %v", within)
	}
}

func TestWatchWakeFileWithHealthyIdleSocket(t *testing.T) {
	path := wakeTestPath(t)
	l := wakeTestListener(t, path)
	watch := watchWake(path, time.Now().Add(5*time.Second), l.dial)
	defer watch.Close()
	c := acceptWakeTestPeer(t, l)
	if _, err := c.Write([]byte("ready\n")); err != nil {
		t.Fatal(err)
	}
	receiveWake(t, watch, time.Second) // asynchronous IPC readiness recheck
	start := time.Now()
	if err := NotifyFile(path + ".wake"); err != nil {
		t.Fatal(err)
	}
	receiveWake(t, watch, 250*time.Millisecond)
	t.Logf("file-only wake with healthy idle IPC: %v", time.Since(start))
}

func TestWatchWakeSocketNotification(t *testing.T) {
	path := wakeTestPath(t)
	l := wakeTestListener(t, path)
	watch := watchWake(path, time.Now().Add(5*time.Second), l.dial)
	defer watch.Close()
	c := acceptWakeTestPeer(t, l)
	if _, err := c.Write([]byte("ready\n")); err != nil {
		t.Fatal(err)
	}
	receiveWake(t, watch, time.Second)
	if _, err := c.Write([]byte("wake\n")); err != nil {
		t.Fatal(err)
	}
	receiveWake(t, watch, 250*time.Millisecond)
}

func TestWatchWakeFileDuringStalledHandshake(t *testing.T) {
	path := wakeTestPath(t)
	l := wakeTestListener(t, path)
	start := time.Now()
	watch := watchWake(path, time.Now().Add(5*time.Second), l.dial)
	defer watch.Close()
	if elapsed := time.Since(start); elapsed > 250*time.Millisecond {
		t.Fatalf("WatchWake blocked on setup for %v", elapsed)
	}
	acceptWakeTestPeer(t, l) // deliberately never sends ready
	start = time.Now()
	if err := NotifyFile(path + ".wake"); err != nil {
		t.Fatal(err)
	}
	receiveWake(t, watch, 250*time.Millisecond)
	t.Logf("file-only wake during stalled IPC handshake: %v", time.Since(start))
}

func TestWatchWakeDeadlineBoundsStalledHandshake(t *testing.T) {
	path := wakeTestPath(t)
	l := wakeTestListener(t, path)
	start := time.Now()
	watch := watchWake(path, start.Add(80*time.Millisecond), l.dial)
	defer watch.Close()
	c := acceptWakeTestPeer(t, l)
	select {
	case <-watch.done:
	case <-time.After(300 * time.Millisecond):
		t.Fatal("overall deadline did not release watcher during handshake")
	}
	if elapsed := time.Since(start); elapsed > 300*time.Millisecond {
		t.Fatalf("short wait consumed %v", elapsed)
	}
	var b [1]byte
	if _, err := c.Read(b[:]); err != io.EOF {
		t.Fatalf("deadline did not close IPC: %v", err)
	}
	select {
	case <-watch.Events():
		t.Fatal("failed handshake or deadline generated a wake or closed channel")
	default:
	}
}

func TestWatchIPCCallerDeadline(t *testing.T) {
	path := wakeTestPath(t)
	l := wakeTestListener(t, path)
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		c, _, err := watchIPCWithDialer(ctx, path, l.dial)
		if c != nil {
			c.Close()
		}
		result <- err
	}()
	acceptWakeTestPeer(t, l)
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("stalled handshake succeeded")
		}
	case <-time.After(300 * time.Millisecond):
		t.Fatal("caller deadline did not bound IPC handshake")
	}
}

func TestWatchWakeMissingSocketStillWakes(t *testing.T) {
	path := wakeTestPath(t)
	watch := WatchWake(path, time.Now().Add(time.Second))
	defer watch.Close()
	if err := NotifyFile(path + ".wake"); err != nil {
		t.Fatal(err)
	}
	receiveWake(t, watch, 250*time.Millisecond)
}

func TestWatchWakeSocketFailureLeavesFileActive(t *testing.T) {
	path := wakeTestPath(t)
	l := wakeTestListener(t, path)
	watch := watchWake(path, time.Now().Add(5*time.Second), l.dial)
	defer watch.Close()
	c := acceptWakeTestPeer(t, l)
	if watch.Connected() {
		t.Fatal("IPC connected before handshake completed")
	}
	if _, err := c.Write([]byte("ready\n")); err != nil {
		t.Fatal(err)
	}
	receiveWake(t, watch, time.Second)
	if !watch.Connected() {
		t.Fatal("ready IPC not reported connected")
	}
	c.Close()
	receiveWake(t, watch, 250*time.Millisecond)
	if watch.Connected() {
		t.Fatal("disconnected IPC still reported connected")
	}
	if err := NotifyFile(path + ".wake"); err != nil {
		t.Fatal(err)
	}
	receiveWake(t, watch, 250*time.Millisecond)
}

func TestWatchWakeMalformedSocketTriggersFallback(t *testing.T) {
	path := wakeTestPath(t)
	l := wakeTestListener(t, path)
	watch := watchWake(path, time.Now().Add(5*time.Second), l.dial)
	defer watch.Close()
	c := acceptWakeTestPeer(t, l)
	if _, err := c.Write([]byte("ready\n")); err != nil {
		t.Fatal(err)
	}
	receiveWake(t, watch, time.Second)
	if _, err := c.Write([]byte("invalid\n")); err != nil {
		t.Fatal(err)
	}
	receiveWake(t, watch, 250*time.Millisecond)
	if watch.Connected() {
		t.Fatal("invalid IPC protocol did not switch to fallback")
	}
	if err := NotifyFile(path + ".wake"); err != nil {
		t.Fatal(err)
	}
	receiveWake(t, watch, 250*time.Millisecond)
}

func TestWatchWakeConcurrentCloseCancelsHandshake(t *testing.T) {
	path := wakeTestPath(t)
	l := wakeTestListener(t, path)
	watch := watchWake(path, time.Now().Add(5*time.Second), l.dial)
	defer watch.Close()
	c := acceptWakeTestPeer(t, l)
	closed := make(chan struct{})
	go func() {
		var wg sync.WaitGroup
		for range 16 {
			wg.Add(1)
			go func() { defer wg.Done(); watch.Close() }()
		}
		wg.Wait()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(300 * time.Millisecond):
		t.Fatal("Close failed to interrupt handshake or join resources")
	}
	var b [1]byte
	if _, err := c.Read(b[:]); err != io.EOF {
		t.Fatalf("Close did not close IPC: %v", err)
	}
}

func TestWatchWakeRepeatedCloseReleasesDescriptors(t *testing.T) {
	path := wakeTestPath(t)
	before, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Skip("descriptor inventory unavailable")
	}
	for range 30 {
		watch := WatchWake(path, time.Now().Add(time.Second))
		watch.Close()
		watch.Close()
	}
	after, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	if len(after) > len(before)+1 {
		t.Fatalf("watch descriptors leaked: before=%d, after=%d", len(before), len(after))
	}
}

func TestWatchWakeRejectsInsecureFile(t *testing.T) {
	for _, kind := range []string{"symlink", "public", "oversized", "directory", "fifo", "foreign-owner"} {
		t.Run(kind, func(t *testing.T) {
			path := wakeTestPath(t)
			file := path + ".wake"
			switch kind {
			case "symlink":
				target := path + ".target"
				if err := os.WriteFile(target, []byte("safe"), 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(target, file); err != nil {
					t.Fatal(err)
				}
			case "public", "oversized", "foreign-owner":
				data := []byte("safe")
				if kind == "oversized" {
					data = bytes.Repeat([]byte{'x'}, 17)
				}
				if err := os.WriteFile(file, data, 0600); err != nil {
					t.Fatal(err)
				}
				if kind == "public" {
					if err := os.Chmod(file, 0644); err != nil {
						t.Fatal(err)
					}
				}
				if kind == "foreign-owner" {
					if os.Geteuid() != 0 {
						t.Skip("foreign-owner fixture needs root")
					}
					if err := os.Chown(file, os.Getuid()+1, -1); err != nil {
						t.Fatal(err)
					}
				}
			case "directory":
				if err := os.Mkdir(file, 0700); err != nil {
					t.Fatal(err)
				}
			case "fifo":
				if err := syscall.Mkfifo(file, 0600); err != nil {
					t.Fatal(err)
				}
			}
			if fw, err := StartFileWake(file, NewWakeHub()); err == nil {
				fw.Close()
				t.Fatal("insecure file was accepted")
			}
			watch := WatchWake(path, time.Now().Add(time.Second))
			defer watch.Close()
			if err := NotifyFile(file); err == nil {
				t.Fatal("insecure notification accepted")
			}
			select {
			case <-watch.Events():
				t.Fatal("failed wake sources generated an event")
			default:
			}
		})
	}
}

func TestWatchWakeExpiredDeadlineStartsNoWatch(t *testing.T) {
	path := wakeTestPath(t)
	watch := WatchWake(path, time.Now().Add(-time.Second))
	watch.Close()
	if _, err := os.Stat(path + ".wake"); !os.IsNotExist(err) {
		t.Fatalf("expired wait created a file watch: %v", err)
	}
}

func TestWatchWakeInstallsFileBeforeDial(t *testing.T) {
	path := wakeTestPath(t)
	result := make(chan error, 1)
	dial := func(context.Context, string, string) (net.Conn, error) {
		// A commit during the very first dial step must already be observable.
		err := NotifyFile(path + ".wake")
		result <- err
		return nil, errors.New("test socket unavailable")
	}
	watch := watchWake(path, time.Now().Add(time.Second), dial)
	defer watch.Close()
	if err := <-result; err != nil {
		t.Fatalf("file watch was not installed before dial: %v", err)
	}
	receiveWake(t, watch, 250*time.Millisecond)
}

func TestWatchIPCDialDeadlineBound(t *testing.T) {
	for _, wait := range []time.Duration{0, 80 * time.Millisecond} {
		t.Run(wait.String(), func(t *testing.T) {
			ctx := context.Background()
			if wait > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, wait)
				defer cancel()
			}
			dial := func(dialCtx context.Context, network, path string) (net.Conn, error) {
				deadline, ok := dialCtx.Deadline()
				if !ok || time.Until(deadline) > time.Second {
					t.Error("dial has no one-second cap")
				}
				if outer, ok := ctx.Deadline(); ok && deadline.After(outer) {
					t.Error("dial extends beyond overall deadline")
				}
				if network != "unix" || path != "test.sock" {
					t.Error("dial target changed")
				}
				return nil, errors.New("test dial failure")
			}
			if c, _, err := watchIPCWithDialer(ctx, "test.sock", dial); err == nil || c != nil {
				t.Fatal("failed dial succeeded")
			}
		})
	}
}

func TestWatchWakeDeadlineCancelsStalledDial(t *testing.T) {
	path := wakeTestPath(t)
	dialed := make(chan struct{})
	finished := make(chan struct{})
	dial := func(ctx context.Context, _, _ string) (net.Conn, error) {
		close(dialed)
		<-ctx.Done()
		close(finished)
		return nil, ctx.Err()
	}
	watch := watchWake(path, time.Now().Add(80*time.Millisecond), dial)
	defer watch.Close()
	<-dialed
	if err := NotifyFile(path + ".wake"); err != nil {
		t.Fatal(err)
	}
	receiveWake(t, watch, 50*time.Millisecond)
	select {
	case <-watch.done:
	case <-time.After(300 * time.Millisecond):
		t.Fatal("deadline failed to cancel stalled dial")
	}
	select {
	case <-finished:
	default:
		t.Fatal("deadline did not join dial worker")
	}
}

type deadlineRecordingConn struct {
	net.Conn
	deadlines chan time.Time
}

func (c *deadlineRecordingConn) SetDeadline(deadline time.Time) error {
	c.deadlines <- deadline
	return c.Conn.SetDeadline(deadline)
}

func TestWatchIPCHandshakeDeadlineAndReset(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	c := &deadlineRecordingConn{Conn: client, deadlines: make(chan time.Time, 2)}
	dial := func(context.Context, string, string) (net.Conn, error) { return c, nil }
	result := make(chan error, 1)
	go func() {
		conn, _, err := watchIPCWithDialer(context.Background(), "test.sock", dial)
		if conn != nil {
			conn.Close()
		}
		result <- err
	}()
	if err := server.SetDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	line, err := bufio.NewReader(server).ReadString('\n')
	if err != nil || line != "watch\n" {
		t.Fatalf("watch request = %q, %v", line, err)
	}
	remaining := time.Until(<-c.deadlines)
	if remaining <= 0 || remaining > 2*time.Second {
		t.Fatalf("invalid handshake bound: %v", remaining)
	}
	if _, err := server.Write([]byte("ready\n")); err != nil {
		t.Fatal(err)
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if deadline := <-c.deadlines; !deadline.IsZero() {
		t.Fatal("legacy indefinite watch did not clear handshake deadline")
	}
}

func TestWatchIPCRejectsOverlongHandshake(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		conn, _, err := watchIPCWithDialer(ctx, "test.sock", func(context.Context, string, string) (net.Conn, error) {
			return client, nil
		})
		if conn != nil {
			conn.Close()
		}
		result <- err
	}()
	if _, err := bufio.NewReader(server).ReadString('\n'); err != nil {
		t.Fatal(err)
	}
	if _, err := server.Write(bytes.Repeat([]byte{'x'}, 8192)); err == nil {
		t.Fatal("overlong handshake was fully consumed")
	}
	if err := <-result; err == nil {
		t.Fatal("overlong handshake accepted")
	}
}
