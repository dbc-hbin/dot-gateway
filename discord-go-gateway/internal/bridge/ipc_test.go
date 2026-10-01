package bridge

import (
	"errors"
	"net"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func TestPrivateIPCWakesAcrossConnections(t *testing.T) {
	probe, e := net.ListenUnix("unix", &net.UnixAddr{Name: "/tmp/dot-go-socket-test", Net: "unix"})
	if errors.Is(e, syscall.EPERM) || errors.Is(e, syscall.EACCES) {
		t.Skip("execution sandbox prohibits Unix socket creation; native-host test required")
	}
	if e == nil {
		probe.Close()
	}
	dir := t.TempDir()
	os.Chmod(dir, 0700)
	path := filepath.Join(dir, "wake.sock")
	hub := NewWakeHub()
	s, e := StartIPC(path, hub)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	c, r, e := WatchIPC(path)
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	NotifyIPC(path)
	c.SetReadDeadline(time.Now().Add(time.Second))
	line, e := r.ReadString('\n')
	if e != nil || line != "wake\n" {
		t.Fatalf("%q %v", line, e)
	}
	st, _ := os.Stat(path)
	if st.Mode().Perm() != 0600 {
		t.Fatal("public socket")
	}
}
func TestDispatcherLockExcludesAndReleases(t *testing.T) {
	path := t.TempDir() + "/queue.db"
	f, e := LockDispatcher(path)
	if e != nil {
		t.Fatal(e)
	}
	if other, e := LockDispatcher(path); e == nil {
		other.Close()
		t.Fatal("duplicate dispatcher")
	}
	f.Close()
	f, e = LockDispatcher(path)
	if e != nil {
		t.Fatal(e)
	}
	f.Close()
	os.Remove(path + ".lock")
	os.Symlink(path+".target", path+".lock")
	if f, e = LockDispatcher(path); e == nil {
		f.Close()
		t.Fatal("symlink lock accepted")
	}
}
func TestFileWakeAcrossIndependentWriters(t *testing.T) {
	path := t.TempDir() + "/signal"
	hub := NewWakeHub()
	wake, unsub := hub.Subscribe()
	defer unsub()
	watch, e := StartFileWake(path, hub)
	if e != nil {
		t.Fatal(e)
	}
	defer watch.Close()
	if e = NotifyFile(path); e != nil {
		t.Fatal(e)
	}
	select {
	case <-wake:
	case <-time.After(time.Second):
		t.Fatal("file notification missed")
	}
	st, _ := os.Stat(path)
	if st.Mode().Perm() != 0600 {
		t.Fatal("public file")
	}
}
func TestFileWakeRejectsSymlinkAndOpenPermissions(t *testing.T) {
	dir := t.TempDir()
	target := dir + "/target"
	os.WriteFile(target, []byte("x"), 0600)
	link := dir + "/link"
	os.Symlink(target, link)
	if e := NotifyFile(link); e == nil {
		t.Fatal("symlink accepted")
	}
	os.Chmod(target, 0644)
	if e := NotifyFile(target); e == nil {
		t.Fatal("public file accepted")
	}
}
