package bridge

import (
	"bufio"
	"context"
	"errors"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// WakeHub coalesces durable-state notifications. SQLite, not this socket, is
// authoritative, so a crashed writer cannot lose an already committed reply.
type WakeHub struct {
	mu      sync.Mutex
	clients map[chan struct{}]struct{}
}

func NewWakeHub() *WakeHub { return &WakeHub{clients: make(map[chan struct{}]struct{})} }
func (h *WakeHub) Subscribe() (<-chan struct{}, func()) {
	h.mu.Lock()
	defer h.mu.Unlock()
	c := make(chan struct{}, 1)
	h.clients[c] = struct{}{}
	return c, func() { h.mu.Lock(); delete(h.clients, c); h.mu.Unlock() }
}
func (h *WakeHub) Notify() {
	h.mu.Lock()
	defer h.mu.Unlock()
	for c := range h.clients {
		select {
		case c <- struct{}{}:
		default:
		}
	}
}

type IPCServer struct {
	listener *net.UnixListener
	done     chan struct{}
	wg       sync.WaitGroup
	mu       sync.Mutex
	conns    map[*net.UnixConn]bool
}

func StartIPC(path string, hub *WakeHub) (*IPCServer, error) {
	if len(path) > 100 {
		return nil, errors.New("ipc_path_too_long")
	}
	if st, err := os.Lstat(path); err == nil {
		if st.Mode()&os.ModeSocket == 0 {
			return nil, errors.New("ipc_path_not_socket")
		}
		if err = os.Remove(path); err != nil {
			return nil, errors.New("ipc_remove_failed")
		}
	} else if !os.IsNotExist(err) {
		return nil, errors.New("ipc_stat_failed")
	}
	l, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		return nil, errors.New("ipc_listen_failed")
	}
	if err = os.Chmod(path, 0600); err != nil {
		l.Close()
		return nil, errors.New("ipc_permissions_failed")
	}
	s := &IPCServer{listener: l, done: make(chan struct{}), conns: make(map[*net.UnixConn]bool)}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			c, e := l.AcceptUnix()
			if e != nil {
				return
			}
			s.mu.Lock()
			if len(s.conns) >= 128 {
				s.mu.Unlock()
				c.Close()
				continue
			}
			s.conns[c] = true
			s.mu.Unlock()
			s.wg.Add(1)
			go s.serve(c, hub)
		}
	}()
	return s, nil
}
func (s *IPCServer) serve(c *net.UnixConn, h *WakeHub) {
	defer s.wg.Done()
	defer c.Close()
	defer func() { s.mu.Lock(); delete(s.conns, c); s.mu.Unlock() }()
	raw, e := c.SyscallConn()
	if e != nil {
		return
	}
	allowed := false
	raw.Control(func(fd uintptr) {
		u, e := syscall.GetsockoptUcred(int(fd), syscall.SOL_SOCKET, syscall.SO_PEERCRED)
		allowed = e == nil && u.Uid == uint32(os.Getuid())
	})
	if !allowed {
		return
	}
	c.SetReadDeadline(time.Now().Add(3 * time.Second))
	line, e := bufio.NewReaderSize(c, 32).ReadString('\n')
	if e != nil {
		return
	}
	if line == "notify\n" {
		h.Notify()
		c.Write([]byte("ok\n"))
		return
	}
	if line != "watch\n" {
		return
	}
	wake, unsub := h.Subscribe()
	defer unsub()
	c.SetWriteDeadline(time.Now().Add(2 * time.Second))
	if _, e = c.Write([]byte("ready\n")); e != nil {
		return
	}
	closed := make(chan struct{})
	c.SetReadDeadline(time.Time{})
	go func() { var b [1]byte; c.Read(b[:]); close(closed) }()
	for {
		select {
		case <-s.done:
			return
		case <-closed:
			return
		case <-wake:
			c.SetWriteDeadline(time.Now().Add(2 * time.Second))
			if _, e = c.Write([]byte("wake\n")); e != nil {
				return
			}
		}
	}
}
func (s *IPCServer) Close() error {
	close(s.done)
	e := s.listener.Close()
	s.mu.Lock()
	for c := range s.conns {
		c.Close()
	}
	s.mu.Unlock()
	s.wg.Wait()
	return e
}
func NotifyIPC(path string) {
	// Always signal the shared file too: an exec caller may be prohibited from
	// creating sockets even while the native daemon owns a working Unix socket.
	NotifyFile(path + ".wake")
	c, e := net.DialTimeout("unix", path, time.Second)
	if e != nil {
		return
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(time.Second))
	c.Write([]byte("notify\n"))
}
func WatchIPC(path string) (net.Conn, *bufio.Reader, error) {
	return watchIPC(context.Background(), path)
}

type ipcDialFunc func(context.Context, string, string) (net.Conn, error)

// watchIPC bounds both setup stages by the caller's overall wait. Cancellation
// also interrupts an in-flight handshake rather than waiting for its timeout.
func watchIPC(ctx context.Context, path string) (net.Conn, *bufio.Reader, error) {
	return watchIPCWithDialer(ctx, path, (&net.Dialer{}).DialContext)
}

func watchIPCWithDialer(ctx context.Context, path string, dial ipcDialFunc) (net.Conn, *bufio.Reader, error) {
	dialCtx, cancel := context.WithTimeout(ctx, time.Second)
	c, e := dial(dialCtx, "unix", path)
	cancel()
	if e != nil {
		return nil, nil, e
	}
	stopCancel := closeIPCOnCancel(ctx, c)
	defer stopCancel()
	until := time.Now().Add(2 * time.Second)
	if deadline, ok := ctx.Deadline(); ok && deadline.Before(until) {
		until = deadline
	}
	c.SetDeadline(until)
	if _, e = c.Write([]byte("watch\n")); e != nil {
		c.Close()
		return nil, nil, e
	}
	r := bufio.NewReader(c)
	// Protocol lines are tiny. ReadSlice rejects an overlong peer response
	// without allocating an unbounded buffer.
	line, e := r.ReadSlice('\n')
	if e != nil || string(line) != "ready\n" || ctx.Err() != nil {
		c.Close()
		return nil, nil, errors.New("ipc_watch_failed")
	}
	deadline, _ := ctx.Deadline()
	c.SetDeadline(deadline)
	return c, r, nil
}

// closeIPCOnCancel returns a stop function that also joins any already-running
// close callback, so closing a wake watch leaves no cancellation work behind.
func closeIPCOnCancel(ctx context.Context, c net.Conn) func() {
	done := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		c.Close()
		close(done)
	})
	return func() {
		if !stop() {
			<-done
		}
	}
}

// WakeWatch multiplexes private socket and file notifications. Both are hints:
// callers must recheck durable state and retain a recovery polling timer.
type WakeWatch struct {
	events    <-chan struct{}
	cancel    context.CancelFunc
	done      chan struct{}
	connected atomic.Bool
}

// WatchWake installs the file watch before asynchronously setting up IPC. Thus
// an idle socket or stalled handshake can never mask a file-only notification.
// The deadline bounds IPC setup and releases all resources; zero means no limit.
// Setup failures are intentionally nonfatal because durable polling is the
// authoritative recovery path. Close is safe to call repeatedly or concurrently.
func WatchWake(path string, deadline time.Time) *WakeWatch {
	return watchWake(path, deadline, (&net.Dialer{}).DialContext)
}

func watchWake(path string, deadline time.Time, dial ipcDialFunc) *WakeWatch {
	ctx := context.Background()
	var cancel context.CancelFunc
	if deadline.IsZero() {
		ctx, cancel = context.WithCancel(ctx)
	} else {
		ctx, cancel = context.WithDeadline(ctx, deadline)
	}
	hub := NewWakeHub()
	events, unsubscribe := hub.Subscribe()
	watch := &WakeWatch{events: events, cancel: cancel, done: make(chan struct{})}
	if ctx.Err() != nil {
		cancel()
		unsubscribe()
		close(watch.done)
		return watch
	}
	fileWatch, _ := StartFileWake(path+".wake", hub)
	ipcDone := make(chan struct{})
	go func() {
		defer close(ipcDone)
		c, r, err := watchIPCWithDialer(ctx, path, dial)
		if err != nil {
			return
		}
		defer c.Close()
		stopCancel := closeIPCOnCancel(ctx, c)
		defer stopCancel()
		watch.connected.Store(true)
		defer func() {
			watch.connected.Store(false)
			if ctx.Err() == nil {
				// Reset a caller's healthy-socket recovery timer immediately
				// when the socket disappears or sends an invalid protocol line.
				hub.Notify()
			}
		}()
		// Recheck commits made between the caller's previous durable query and
		// completion of this asynchronous subscription, even if file IPC failed.
		hub.Notify()
		for {
			line, err := r.ReadSlice('\n')
			if err != nil || string(line) != "wake\n" {
				return
			}
			hub.Notify()
		}
	}()
	go func() {
		defer close(watch.done)
		<-ctx.Done()
		if fileWatch != nil {
			fileWatch.Close()
		}
		<-ipcDone
		unsubscribe()
	}()
	return watch
}

// Events carries coalesced wake hints. It is not closed at the deadline; callers
// must select their own deadline/recovery timer rather than spin on a closed hint.
func (w *WakeWatch) Events() <-chan struct{} { return w.events }

// Connected reports whether IPC has completed its subscription and remains
// healthy, allowing callers to retain their shorter disconnected recovery poll.
func (w *WakeWatch) Connected() bool { return w.connected.Load() }

func (w *WakeWatch) Close() {
	w.cancel()
	<-w.done
}

// LockDispatcher uses the exact legacy lock path, excluding the Python sender.
func LockDispatcher(dbPath string) (*os.File, error) {
	fd, e := syscall.Open(dbPath+".lock", syscall.O_CREAT|syscall.O_RDWR|syscall.O_NOFOLLOW, 0600)
	if e != nil {
		return nil, errors.New("dispatcher_lock_failed")
	}
	f := os.NewFile(uintptr(fd), dbPath+".lock")
	st, e := f.Stat()
	if e != nil || !st.Mode().IsRegular() || st.Mode().Perm()&0077 != 0 {
		f.Close()
		return nil, errors.New("dispatcher_lock_insecure")
	}
	if s, ok := st.Sys().(*syscall.Stat_t); !ok || s.Uid != uint32(os.Getuid()) {
		f.Close()
		return nil, errors.New("dispatcher_lock_insecure")
	}
	if e = syscall.Flock(fd, syscall.LOCK_EX|syscall.LOCK_NB); e != nil {
		f.Close()
		return nil, errors.New("another_dispatcher_is_running")
	}
	return f, nil
}
