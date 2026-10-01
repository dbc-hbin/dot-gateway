package bridge

import (
	"bufio"
	"context"
	"net"
	"os"
	"sort"
	"testing"
	"time"
)

// TestWakeLatencySimulation is opt-in evidence, not a timing-sensitive normal
// test. It uses net.Pipe and real inotify, never a live queue or network service.
// The serialized baselines reproduce the former poller's one-second socket
// read and two-second handshake-before-file-watch ordering. They do not execute
// a historical gateway binary or measure database/Discord end-to-end latency.
func TestWakeLatencySimulation(t *testing.T) {
	if os.Getenv("DOT_GATEWAY_WAKE_TAIL_SIMULATION") != "1" {
		t.Skip("opt-in local IPC/file latency simulation")
	}
	const samples = 5
	for _, stalled := range []bool{false, true} {
		scenario := "healthy-idle-socket"
		if stalled {
			scenario = "stalled-handshake"
		}
		oldTimes := make([]time.Duration, 0, samples)
		newTimes := make([]time.Duration, 0, samples)
		for range samples {
			oldTimes = append(oldTimes, sampleSerializedWake(t, stalled))
			newTimes = append(newTimes, sampleMultiplexedWake(t, stalled))
		}
		for name, values := range map[string][]time.Duration{"serialized-baseline": oldTimes, "multiplexed": newTimes} {
			sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
			t.Logf("simulation=%s implementation=%s n=%d median=%v p95=%v max=%v samples=%v",
				scenario, name, samples, values[len(values)/2], values[len(values)-1], values[len(values)-1], values)
		}
	}
}

func sampleSerializedWake(t *testing.T, stalled bool) time.Duration {
	t.Helper()
	path := wakeTestPath(t)
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	hub := NewWakeHub()
	if !stalled {
		fw, err := StartFileWake(path+".wake", hub)
		if err != nil {
			t.Fatal(err)
		}
		defer fw.Close()
		start := time.Now()
		if err := NotifyFile(path + ".wake"); err != nil {
			t.Fatal(err)
		}
		client.SetReadDeadline(time.Now().Add(time.Second))
		if _, err := bufio.NewReader(client).ReadString('\n'); err == nil {
			t.Fatal("idle baseline unexpectedly received a socket notification")
		}
		return time.Since(start)
	}
	// The daemon's shared file already exists, but the old poller installs its
	// own file watch only after the stalled IPC handshake returns.
	if err := os.WriteFile(path+".wake", nil, 0600); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		c, _, err := watchIPCWithDialer(context.Background(), path, func(context.Context, string, string) (net.Conn, error) {
			return client, nil
		})
		if c != nil {
			c.Close()
		}
		result <- err
	}()
	if _, err := bufio.NewReader(server).ReadString('\n'); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if err := NotifyFile(path + ".wake"); err != nil {
		t.Fatal(err)
	}
	if err := <-result; err == nil {
		t.Fatal("stalled baseline handshake unexpectedly succeeded")
	}
	fw, err := StartFileWake(path+".wake", hub)
	if err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(start)
	fw.Close()
	return elapsed
}

func sampleMultiplexedWake(t *testing.T, stalled bool) time.Duration {
	t.Helper()
	path := wakeTestPath(t)
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	dial := func(context.Context, string, string) (net.Conn, error) { return client, nil }
	watch := watchWake(path, time.Now().Add(5*time.Second), dial)
	defer watch.Close()
	if _, err := bufio.NewReader(server).ReadString('\n'); err != nil {
		t.Fatal(err)
	}
	if !stalled {
		if _, err := server.Write([]byte("ready\n")); err != nil {
			t.Fatal(err)
		}
		receiveWake(t, watch, time.Second)
	}
	start := time.Now()
	if err := NotifyFile(path + ".wake"); err != nil {
		t.Fatal(err)
	}
	receiveWake(t, watch, time.Second)
	return time.Since(start)
}
