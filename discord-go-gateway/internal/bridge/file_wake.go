package bridge

import (
	"errors"
	"os"
	"sync"
	"syscall"
)

// FileWake is a private, content-free notification path for callers whose
// sandbox permits the queue filesystem but prohibits socket creation.
type FileWake struct {
	file *os.File
	done chan struct{}
	once sync.Once
}

func privateWakeFile(path string, flags int) (*os.File, error) {
	fd, e := syscall.Open(path, flags|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0600)
	if e != nil {
		return nil, errors.New("wake_file_open_failed")
	}
	f := os.NewFile(uintptr(fd), path)
	st, e := f.Stat()
	if e != nil || !st.Mode().IsRegular() || st.Mode().Perm()&0077 != 0 || st.Size() > 16 {
		f.Close()
		return nil, errors.New("wake_file_insecure")
	}
	owner, ok := st.Sys().(*syscall.Stat_t)
	if !ok || owner.Uid != uint32(os.Getuid()) {
		f.Close()
		return nil, errors.New("wake_file_insecure")
	}
	return f, nil
}
func NotifyFile(path string) error {
	f, e := privateWakeFile(path, syscall.O_WRONLY)
	if e != nil {
		return e
	}
	defer f.Close()
	_, e = f.WriteAt([]byte{1}, 0)
	return e
}
func StartFileWake(path string, hub *WakeHub) (*FileWake, error) {
	f, e := privateWakeFile(path, syscall.O_CREAT|syscall.O_RDWR)
	if e != nil {
		return nil, e
	}
	f.Close()
	fd, e := syscall.InotifyInit1(syscall.IN_CLOEXEC | syscall.IN_NONBLOCK)
	if e != nil {
		return nil, errors.New("file_watch_unavailable")
	}
	_, e = syscall.InotifyAddWatch(fd, path, syscall.IN_MODIFY|syscall.IN_CLOSE_WRITE|syscall.IN_DELETE_SELF|syscall.IN_MOVE_SELF)
	if e != nil {
		syscall.Close(fd)
		return nil, errors.New("file_watch_failed")
	}
	watch := &FileWake{file: os.NewFile(uintptr(fd), "wake-inotify"), done: make(chan struct{})}
	go func() {
		defer close(watch.done)
		buf := make([]byte, 4096)
		for {
			_, e := watch.file.Read(buf)
			if e != nil {
				return
			}
			hub.Notify()
		}
	}()
	return watch, nil
}
func (w *FileWake) Close() { w.once.Do(func() { w.file.Close(); <-w.done }) }
