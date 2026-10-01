package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

// Phase traces are opt-in diagnostics, never part of the command's success or
// idempotency contract. One bounded batch is appended after stdout has returned.
// A successful OS write is not evidence of tool delivery or model receipt.
const phaseTraceLimit = 1 << 20
const phaseTraceMaxPhases = 32

type phasePoint struct {
	Phase     string `json:"phase"`
	At        string `json:"at"`
	ElapsedNS int64  `json:"elapsed_ns"`
}
type phaseTrace struct {
	InvocationID string       `json:"invocation_id"`
	InboundID    string       `json:"inbound_id,omitempty"`
	Phases       []phasePoint `json:"phases"`
	start        time.Time
	path         string
}

func newPhaseTrace(enabled bool) *phaseTrace {
	if !enabled {
		return nil
	}
	now := time.Now()
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return nil
	}
	t := &phaseTrace{InvocationID: hex.EncodeToString(id[:]), start: now, Phases: make([]phasePoint, 0, phaseTraceMaxPhases)}
	t.markAt("cli_enter", now)
	return t
}
func (t *phaseTrace) mark(phase string) { t.markAt(phase, time.Now()) }
func (t *phaseTrace) markAt(phase string, at time.Time) {
	if t == nil || len(t.Phases) >= phaseTraceMaxPhases {
		return
	}
	// No arbitrary labels: callers cannot accidentally log errors or input here.
	switch phase {
	case "cli_enter", "store_open_started", "store_open_finished", "claim_call_started", "claim_returned", "notify_started", "notify_finished", "stdout_ready", "stdout_write_started", "stdout_write_finished", "stdout_write_failed", "reply_input_open_started", "reply_input_open_finished", "reply_input_read_started", "reply_input_read_finished", "queue_call_started", "queue_returned":
	default:
		return
	}
	elapsed := at.Sub(t.start).Nanoseconds()
	if elapsed < 0 {
		return
	}
	t.Phases = append(t.Phases, phasePoint{phase, at.UTC().Format(time.RFC3339Nano), elapsed})
}
func (t *phaseTrace) setPath(dbPath string) {
	if t != nil {
		t.path = dbPath + ".phase-trace"
	}
}
func (t *phaseTrace) setInbound(id string) {
	if t == nil || len(id) != 32 {
		return
	}
	if _, err := hex.DecodeString(id); err == nil {
		t.InboundID = id
	}
}
func (t *phaseTrace) flush() {
	if t == nil || t.path == "" {
		return
	}
	done := make(chan struct{})
	go func() { defer close(done); t.writeBatch() }()
	// Logging cannot keep a short-lived CLI alive indefinitely, including when
	// the backing filesystem stalls. main exits without waiting for this worker.
	timer := time.NewTimer(10 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
	}
}
func (t *phaseTrace) writeBatch() {
	// Validate the parent and descriptor, refuse links/devices/shared files, and
	// never wait for another tracer. No fsync, per-phase SQL, or error propagation.
	dir, err := os.Lstat(filepath.Dir(t.path))
	if err != nil || !dir.IsDir() || dir.Mode().Perm()&0077 != 0 {
		return
	}
	owner, ok := dir.Sys().(*syscall.Stat_t)
	if !ok || owner.Uid != uint32(os.Getuid()) {
		return
	}
	data, err := json.Marshal(t)
	if err != nil || len(data) > 8192 {
		return
	}
	data = append(data, '\n')
	fd, err := syscall.Open(t.path, syscall.O_CREAT|syscall.O_WRONLY|syscall.O_APPEND|syscall.O_NOFOLLOW|syscall.O_NONBLOCK|syscall.O_CLOEXEC, 0600)
	if err != nil {
		return
	}
	f := os.NewFile(uintptr(fd), t.path)
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		return
	}
	owner, ok = info.Sys().(*syscall.Stat_t)
	if !ok || owner.Uid != uint32(os.Getuid()) || owner.Nlink != 1 {
		return
	}
	if err := syscall.Flock(fd, syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return
	}
	defer syscall.Flock(fd, syscall.LOCK_UN)
	info, err = f.Stat()
	if err != nil {
		return
	}
	// Bounded rolling batches: on overflow retain the newest whole batch. Never
	// overwrite a live trace inode via rename or follow another file's links.
	if info.Size()+int64(len(data)) > phaseTraceLimit {
		if err := f.Truncate(0); err != nil {
			return
		}
	}
	_, _ = f.Write(data)
}
