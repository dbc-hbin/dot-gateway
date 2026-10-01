package bridge

import (
	"bufio"
	"errors"
	"net"
	"os"
	"sync"
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
	c, e := net.DialTimeout("unix", path, time.Second)
	if e != nil {
		return nil, nil, e
	}
	c.SetDeadline(time.Now().Add(2 * time.Second))
	if _, e = c.Write([]byte("watch\n")); e != nil {
		c.Close()
		return nil, nil, e
	}
	r := bufio.NewReader(c)
	line, e := r.ReadString('\n')
	if e != nil || line != "ready\n" {
		c.Close()
		return nil, nil, errors.New("ipc_watch_failed")
	}
	c.SetDeadline(time.Time{})
	return c, r, nil
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
