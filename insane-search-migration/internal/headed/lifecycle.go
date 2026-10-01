package headed

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

func ProcessIdentity(pid int) (Identity, error) {
	var ident Identity
	if pid <= 0 {
		return ident, errors.New("invalid native PID")
	}
	raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return ident, err
	}
	boot, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return ident, err
	}
	split := strings.LastIndex(string(raw), ") ")
	if split < 0 {
		return ident, errors.New("invalid proc process identity")
	}
	fields := strings.Fields(string(raw)[split+2:])
	if len(fields) <= 19 {
		return ident, errors.New("incomplete proc process identity")
	}
	return Identity{PID: pid, BootID: strings.TrimSpace(string(boot)), StartTicks: fields[19]}, nil
}
func ensureQueue(queue string) error {
	if err := os.MkdirAll(queue, 0700); err != nil {
		return err
	}
	return privateQueue(queue)
}
func acquireLauncher(ctx context.Context, queue string) (func(), error) {
	f, err := os.OpenFile(filepath.Join(queue, "launcher.lock"), os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}
	for {
		err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			return func() { _ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN); _ = f.Close() }, nil
		}
		if err != syscall.EWOULDBLOCK && err != syscall.EAGAIN {
			f.Close()
			return nil, err
		}
		select {
		case <-ctx.Done():
			f.Close()
			return nil, ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}
func nativeDisplay() bool { return os.Getenv("DISPLAY") != "" || os.Getenv("WAYLAND_DISPLAY") != "" }

type StartOptions struct{ Root, Queue, ProxyConfig string }

func StartDetached(ctx context.Context, options StartOptions) (Status, error) {
	var zero Status
	if !nativeDisplay() {
		return zero, errors.New("launch from a terminal in the real native desktop; no headless fallback")
	}
	root, err := filepath.Abs(options.Root)
	if err != nil {
		return zero, err
	}
	queue := options.Queue
	if queue == "" {
		queue = defaultQueue(root)
	}
	queue, err = filepath.Abs(queue)
	if err != nil {
		return zero, err
	}
	if err = ensureQueue(queue); err != nil {
		return zero, err
	}
	unlock, err := acquireLauncher(ctx, queue)
	if err != nil {
		return zero, err
	}
	defer unlock()
	if status, e := ReadStatus(queue); e == nil && fresh(status) {
		actual, e := ProcessIdentity(status.PID)
		if e == nil && actual == status.Identity {
			return status, nil
		}
	}
	lock := filepath.Join(queue, "service.lock")
	var old Identity
	if err = readJSON(lock, 32<<10, &old); err == nil {
		actual, e := ProcessIdentity(old.PID)
		if e == nil {
			if actual == old {
				return zero, errors.New("exact native helper process still exists; inspect stale status")
			}
			return zero, errors.New("stale lock PID belongs to another process; manual inspection required")
		}
		if !errors.Is(e, os.ErrNotExist) {
			return zero, e
		}
		if err = unlinkOwn(lock); err != nil {
			return zero, err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return zero, err
	}
	if _, err = os.Stat(filepath.Join(queue, "stop")); err == nil {
		return zero, errors.New("intentional stop file exists; use restart or remove it after verified stop")
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return zero, err
	}
	node, err := exec.LookPath("node")
	if err != nil {
		return zero, errors.New("verified headed browser layer requires Node.js and Playwright")
	}
	script := filepath.Join(root, "runtime", "browser", "native_service.cjs")
	if info, e := os.Stat(script); e != nil || !info.Mode().IsRegular() {
		return zero, errors.New("native browser service script missing")
	}
	log, err := os.OpenFile(filepath.Join(queue, "service.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return zero, err
	}
	defer log.Close()
	command := exec.Command(node, script)
	command.Dir = filepath.Dir(script)
	command.Env = append(os.Environ(), "INSANE_BROWSER_QUEUE="+queue)
	if options.ProxyConfig != "" {
		command.Env = append(command.Env, "INSANE_BROWSER_PROXY_CONFIG="+options.ProxyConfig)
	}
	command.Stdin = nil
	command.Stdout = log
	command.Stderr = log
	command.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err = command.Start(); err != nil {
		return zero, err
	}
	expected, err := ProcessIdentity(command.Process.Pid)
	if err != nil {
		return zero, fmt.Errorf("started helper identity unverified: %w", err)
	}
	exited := make(chan error, 1)
	go func() { exited <- command.Wait() }()
	readyctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	poll := time.NewTicker(100 * time.Millisecond)
	defer poll.Stop()
	for {
		select {
		case <-readyctx.Done():
			return zero, errors.New("helper started but readiness unconfirmed; inspect private status/log before retrying")
		case err := <-exited:
			return zero, fmt.Errorf("helper exited during startup: %v; inspect private service.log", err)
		case <-poll.C:
			status, e := ReadStatus(queue)
			if e != nil {
				if errors.Is(e, os.ErrNotExist) {
					continue
				}
				return zero, e
			}
			if fresh(status) && status.Identity == expected {
				launch := map[string]any{"state": "ready", "identity": expected, "queue": queue, "headless": false, "chromiumSandbox": true, "autostart": false, "launcher": "go"}
				if err = writePrivateJSON(filepath.Join(queue, "launch.json"), launch); err != nil {
					return zero, err
				}
				return status, nil
			}
		}
	}
}

// Stop requests only this broker's private queue stop marker; it does not send
// signals across the exec/native PID namespaces or to an unverified process.
func Stop(ctx context.Context, queue string) (Status, error) {
	var zero Status
	if err := privateQueue(queue); err != nil {
		return zero, err
	}
	unlock, err := acquireLauncher(ctx, queue)
	if err != nil {
		return zero, err
	}
	defer unlock()
	previous, err := ReadStatus(queue)
	if err != nil {
		return zero, err
	}
	if !previous.Ready {
		return previous, nil
	}
	f, err := os.OpenFile(filepath.Join(queue, "stop"), os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		return zero, err
	}
	if err = f.Close(); err != nil {
		return zero, err
	}
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return zero, ctx.Err()
		case <-ticker.C:
			current, err := ReadStatus(queue)
			if err != nil {
				return zero, err
			}
			if current.Identity != previous.Identity {
				return zero, errors.New("helper identity changed during stop")
			}
			if !current.Ready {
				return current, nil
			}
		}
	}
}
func Restart(ctx context.Context, options StartOptions) (Status, error) {
	if !nativeDisplay() {
		return Status{}, errors.New("restart must run in the real native desktop; existing helper was not stopped")
	}
	queue := options.Queue
	if queue == "" {
		queue = defaultQueue(options.Root)
	}
	if err := privateQueue(queue); err != nil {
		return Status{}, err
	}
	unlockCheck, err := acquireLauncher(ctx, queue)
	if err != nil {
		return Status{}, err
	}
	err = reconcileDeadForRestart(queue, ProcessIdentity)
	unlockCheck()
	if err != nil {
		return Status{}, err
	}
	stopped, err := Stop(ctx, queue)
	if err != nil {
		return stopped, err
	}
	unlock, err := acquireLauncher(ctx, queue)
	if err != nil {
		return stopped, err
	}
	current, err := ReadStatus(queue)
	if err == nil && (current.Ready || current.Identity != stopped.Identity) {
		err = errors.New("helper identity changed before restart")
	}
	if err == nil {
		err = unlinkOwn(filepath.Join(queue, "stop"))
	}
	unlock()
	if err != nil {
		return stopped, err
	}
	return StartDetached(ctx, options)
}
func (s Status) String() string { data, _ := json.Marshal(s); return string(data) }

// Caller holds the private launcher lock and is on the native desktop. A dead
// exact process may leave ready=true; reconcile only absence, never PID reuse.
func reconcileDeadForRestart(queue string, inspect func(int) (Identity, error)) error {
	status, err := ReadStatus(queue)
	if err != nil {
		return err
	}
	if !status.Ready {
		return nil
	}
	actual, err := inspect(status.PID)
	if err == nil {
		if actual != status.Identity {
			return errors.New("helper PID was reused; manual inspection required")
		}
		return nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	lockPath := filepath.Join(queue, "service.lock")
	var recorded Identity
	if err = readJSON(lockPath, 32<<10, &recorded); err == nil {
		if recorded != status.Identity {
			return errors.New("stale lock identity differs from status")
		}
		if err = unlinkOwn(lockPath); err != nil {
			return err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	status.Ready = false
	status.UpdatedAt = time.Now().UnixMilli()
	return writePrivateJSON(filepath.Join(queue, "status.json"), status)
}
