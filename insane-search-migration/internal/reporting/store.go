package reporting

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

type store struct {
	dir  *os.File
	lock *os.File
}

func privateRegular(f *os.File) bool {
	st, err := f.Stat()
	if err != nil {
		return false
	}
	owner, ok := st.Sys().(*syscall.Stat_t)
	return ok && st.Mode().IsRegular() && st.Mode().Perm()&0077 == 0 && int(owner.Uid) == os.Getuid() && owner.Nlink == 1
}

// openStore walks all directory components with O_NOFOLLOW and anchors every
// subsequent access to the held directory fd. Only the leaf must be private.
func openStore(path string) (*store, error) {
	abs, err := filepath.Abs(path)
	if err != nil || abs == "/" {
		return nil, errors.New("invalid_state_directory")
	}
	fd, err := syscall.Open("/", syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_CLOEXEC, 0)
	if err != nil {
		return nil, errors.New("state_directory_open_failed")
	}
	for _, part := range strings.Split(strings.TrimPrefix(abs, "/"), "/") {
		next, e := syscall.Openat(fd, part, syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
		if e == syscall.ENOENT {
			if e = syscall.Mkdirat(fd, part, 0700); e == nil {
				e = syscall.Fsync(fd)
			}
			if e == nil {
				next, e = syscall.Openat(fd, part, syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
			}
		}
		syscall.Close(fd)
		if e != nil {
			return nil, errors.New("state_directory_open_failed")
		}
		fd = next
	}
	d := os.NewFile(uintptr(fd), "state-directory")
	st, err := d.Stat()
	if err != nil {
		d.Close()
		return nil, errors.New("state_directory_stat_failed")
	}
	owner, ok := st.Sys().(*syscall.Stat_t)
	if !ok || !st.IsDir() || st.Mode().Perm()&0077 != 0 || int(owner.Uid) != os.Getuid() {
		d.Close()
		return nil, errors.New("insecure_state_directory")
	}
	lfd, err := syscall.Openat(fd, "publisher.lock", syscall.O_CREAT|syscall.O_RDWR|syscall.O_NOFOLLOW|syscall.O_CLOEXEC|syscall.O_NONBLOCK, 0600)
	if err != nil {
		d.Close()
		return nil, errors.New("publisher_lock_open_failed")
	}
	lock := os.NewFile(uintptr(lfd), "publisher-lock")
	if !privateRegular(lock) {
		lock.Close()
		d.Close()
		return nil, errors.New("insecure_publisher_lock")
	}
	if syscall.Flock(lfd, syscall.LOCK_EX|syscall.LOCK_NB) != nil {
		lock.Close()
		d.Close()
		return nil, errors.New("another_publisher_running")
	}
	return &store{dir: d, lock: lock}, nil
}
func (s *store) close() {
	syscall.Flock(int(s.lock.Fd()), syscall.LOCK_UN)
	s.lock.Close()
	s.dir.Close()
}
func (s *store) read(name string) ([]byte, error) {
	fd, err := syscall.Openat(int(s.dir.Fd()), name, syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC|syscall.O_NONBLOCK, 0)
	if err == syscall.ENOENT {
		return nil, os.ErrNotExist
	}
	if err != nil {
		return nil, errors.New("ledger_open_failed")
	}
	f := os.NewFile(uintptr(fd), "ledger")
	defer f.Close()
	if !privateRegular(f) {
		return nil, errors.New("insecure_ledger")
	}
	b, err := io.ReadAll(io.LimitReader(f, 256*1024+1))
	if err != nil || len(b) > 256*1024 {
		return nil, errors.New("ledger_read_failed")
	}
	return b, nil
}
func (s *store) write(name string, value any) error {
	// Check an existing destination before replacing it; never follow links.
	if _, err := s.read(name); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	b, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return errors.New("ledger_encoding_failed")
	}
	b = append(b, '\n')
	var random [16]byte
	if _, err = rand.Read(random[:]); err != nil {
		return errors.New("ledger_temp_failed")
	}
	temp := ".pending-" + hex.EncodeToString(random[:])
	fd, err := syscall.Openat(int(s.dir.Fd()), temp, syscall.O_WRONLY|syscall.O_CREAT|syscall.O_EXCL|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0600)
	if err != nil {
		return errors.New("ledger_temp_failed")
	}
	f := os.NewFile(uintptr(fd), "ledger-temp")
	defer func() { f.Close(); syscall.Unlinkat(int(s.dir.Fd()), temp) }()
	if _, err = f.Write(b); err != nil {
		return errors.New("ledger_write_failed")
	}
	if f.Sync() != nil {
		return errors.New("ledger_sync_failed")
	}
	if f.Close() != nil {
		return errors.New("ledger_close_failed")
	}
	if syscall.Renameat(int(s.dir.Fd()), temp, int(s.dir.Fd()), name) != nil {
		return errors.New("ledger_replace_failed")
	}
	if s.dir.Sync() != nil {
		return errors.New("ledger_directory_sync_failed")
	}
	return nil
}
func (s *store) pin(t Target) error {
	b, err := s.read("target.json")
	if errors.Is(err, os.ErrNotExist) {
		return s.write("target.json", t)
	}
	if err != nil {
		return err
	}
	var pinned Target
	if strictJSON(b, &pinned) != nil || pinned != t {
		return errors.New("state_target_pin_mismatch")
	}
	return nil
}
