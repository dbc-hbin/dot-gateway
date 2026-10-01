//go:build linux

package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
)

const maxFile = 64 << 20

func digest(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }
func strict(b []byte, v any) error {
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if e := d.Decode(v); e != nil {
		return errors.New("invalid JSON schema")
	}
	if d.Decode(new(any)) != io.EOF {
		return errors.New("trailing JSON")
	}
	return nil
}
func validPath(p string) bool {
	return p != "" && p != "." && !strings.ContainsAny(p, "\\\x00\r\n:") && !strings.HasPrefix(p, "/") && path.Clean(p) == p && p != ".." && !strings.HasPrefix(p, "../")
}
func owner(st os.FileInfo) bool {
	u, ok := st.Sys().(*syscall.Stat_t)
	return ok && int(u.Uid) == os.Getuid()
}

// Every parent component is opened without following links; writes use the held FD.
func openDir(p string, private bool) (*os.File, error) {
	p, e := filepath.Abs(p)
	if e != nil {
		return nil, e
	}
	fd, e := unix.Open("/", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if e != nil {
		return nil, e
	}
	for _, part := range strings.Split(strings.TrimPrefix(p, "/"), "/") {
		if part == "" {
			continue
		}
		n, e := unix.Openat(fd, part, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		unix.Close(fd)
		if e != nil {
			return nil, errors.New("directory missing or symlink")
		}
		fd = n
	}
	f := os.NewFile(uintptr(fd), p)
	st, e := f.Stat()
	if e != nil || private && (!owner(st) || st.Mode().Perm()&0077 != 0) {
		f.Close()
		return nil, errors.New("destination parent must be owner-owned mode 0700")
	}
	return f, nil
}
func readRegular(root *os.Root, p string, private bool, limit int64) ([]byte, error) {
	if !validPath(p) {
		return nil, errors.New("unsafe relative path")
	}
	parts := strings.Split(p, "/")
	for i := range parts {
		st, e := root.Lstat(strings.Join(parts[:i+1], "/"))
		if e != nil {
			return nil, e
		}
		if st.Mode()&os.ModeSymlink != 0 {
			return nil, errors.New("symlink rejected")
		}
	}
	f, e := root.OpenFile(p, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if e != nil {
		return nil, e
	}
	defer f.Close()
	st, e := f.Stat()
	if e != nil {
		return nil, e
	}
	sys, ok := st.Sys().(*syscall.Stat_t)
	if !ok || !st.Mode().IsRegular() || sys.Nlink != 1 || st.Size() > limit || private && (!owner(st) || st.Mode().Perm()&0077 != 0) {
		return nil, errors.New("unsafe file metadata")
	}
	b, e := io.ReadAll(io.LimitReader(f, limit+1))
	if e != nil || int64(len(b)) > limit {
		return nil, errors.New("file read failed or oversized")
	}
	return b, nil
}
func readFile(p string, private bool, limit int64) ([]byte, error) {
	d, e := openDir(filepath.Dir(p), private)
	if e != nil {
		return nil, e
	}
	defer d.Close()
	r, e := os.OpenRoot("/proc/self/fd/" + itoa(int(d.Fd())))
	if e != nil {
		return nil, e
	}
	defer r.Close()
	return readRegular(r, filepath.Base(p), private, limit)
}
func writeFile(dir, name string, b []byte) error {
	p := filepath.Join(dir, filepath.FromSlash(name))
	if !validPath(name) {
		return errors.New("unsafe output path")
	}
	if e := os.MkdirAll(filepath.Dir(p), 0700); e != nil {
		return e
	}
	f, e := os.OpenFile(p, os.O_CREATE|os.O_EXCL|os.O_WRONLY|syscall.O_NOFOLLOW, 0600)
	if e != nil {
		return e
	}
	defer f.Close()
	if _, e = f.Write(b); e != nil {
		return e
	}
	return f.Sync()
}
func jsonBytes(v any) []byte { b, _ := json.MarshalIndent(v, "", "  "); return append(b, '\n') }

// A failed build leaves no visible destination. A power failure may leave an inert
// .recovery-stage-* directory. NEVER execute a staging directory.
func atomicDir(dest string, build func(string) error) error {
	if !filepath.IsAbs(dest) || filepath.Clean(dest) != dest || dest == "/" {
		return errors.New("explicit clean absolute new root required")
	}
	parent, e := openDir(filepath.Dir(dest), true)
	if e != nil {
		return e
	}
	defer parent.Close()
	anchored := "/proc/self/fd/" + itoa(int(parent.Fd()))
	if _, e := os.Lstat(filepath.Join(anchored, filepath.Base(dest))); !os.IsNotExist(e) {
		return errors.New("destination already exists or cannot be checked")
	}
	stage, e := os.MkdirTemp(anchored, ".recovery-stage-")
	if e != nil {
		return e
	}
	defer os.RemoveAll(stage)
	if e = build(stage); e != nil {
		return e
	}
	if e = syncTree(stage); e != nil {
		return e
	}
	if e = unix.Renameat2(int(parent.Fd()), filepath.Base(stage), int(parent.Fd()), filepath.Base(dest), unix.RENAME_NOREPLACE); e != nil {
		return errors.New("atomic no-overwrite install failed")
	}
	if e = parent.Sync(); e != nil {
		return errors.New("installed but parent sync failed; verify destination before retry")
	}
	return nil
}
func syncTree(p string) error {
	return filepath.WalkDir(p, func(n string, d os.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if !d.IsDir() {
			return nil
		}
		f, e := os.Open(n)
		if e != nil {
			return e
		}
		defer f.Close()
		return f.Sync()
	})
}
func atomicFile(dest string, b []byte) error {
	if !filepath.IsAbs(dest) || filepath.Clean(dest) != dest {
		return errors.New("absolute output required")
	}
	p, e := openDir(filepath.Dir(dest), true)
	if e != nil {
		return e
	}
	defer p.Close()
	anchored := "/proc/self/fd/" + itoa(int(p.Fd()))
	f, e := os.CreateTemp(anchored, ".recovery-stage-")
	if e != nil {
		return e
	}
	defer os.Remove(f.Name())
	defer f.Close()
	if e = f.Chmod(0600); e != nil {
		return e
	}
	if _, e = f.Write(b); e != nil {
		return e
	}
	if e = f.Sync(); e != nil {
		return e
	}
	if e = f.Close(); e != nil {
		return e
	}
	if e = unix.Renameat2(int(p.Fd()), filepath.Base(f.Name()), int(p.Fd()), filepath.Base(dest), unix.RENAME_NOREPLACE); e != nil {
		return errors.New("output exists or atomic install failed")
	}
	return p.Sync()
}
