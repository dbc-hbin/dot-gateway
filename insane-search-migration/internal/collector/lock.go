package collector

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// LockState uses an advisory Linux flock on a private sidecar. It is released
// by the kernel after a crash; the harmless sidecar remains to avoid inode races.
func LockState(path string) (func(), error) {
	if e := os.MkdirAll(filepath.Dir(path), 0700); e != nil {
		return nil, e
	}
	f, e := os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		return nil, e
	}
	if e = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); e != nil {
		f.Close()
		return nil, fmt.Errorf("another scan owns state lock %s: %w", path, e)
	}
	return func() { syscall.Flock(int(f.Fd()), syscall.LOCK_UN); f.Close() }, nil
}
