package main

import (
	"archive/zip"
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

type FileEntry struct {
	Path  string `json:"path"`
	Bytes int64  `json:"bytes"`
	SHA   string `json:"sha256"`
}
type Manifest struct {
	Schema int         `json:"schema"`
	Files  []FileEntry `json:"files"`
}

var hashPattern = regexp.MustCompile(`^[a-f0-9]{64}$`)

// This is an extra denylist, not proof that arbitrary source is credential-free.
// Only a previously reviewed, sanitized source manifest may be trusted.
func allowedSource(p string) bool {
	if !validPath(p) || p == "SOURCE_MANIFEST.json" {
		return false
	}
	for _, part := range strings.Split(strings.ToLower(p), "/") {
		if part == ".git" || part == ".env" || part == ".discord-runtime" || part == ".gh-private" || part == ".discord-private" || part == "node_modules" || part == ".queue" || part == "secrets" || strings.HasSuffix(part, ".sqlite3") || strings.HasSuffix(part, ".sqlite3-wal") || part == "bot-token" {
			return false
		}
	}
	return true
}

type Source struct {
	Manifest Manifest
	Raw      []byte
	Files    map[string][]byte
}

func loadSource(dir, archive, pin string) (Source, error) {
	var s Source
	s.Files = map[string][]byte{}
	if !hashPattern.MatchString(pin) {
		return s, errors.New("trusted manifest SHA-256 required")
	}
	if (dir == "") == (archive == "") {
		return s, errors.New("exactly one source directory or archive required")
	}
	var read func(string) ([]byte, error)
	var archiveNames map[string]bool
	if dir != "" {
		d, e := openDir(dir, false)
		if e != nil {
			return s, e
		}
		defer d.Close()
		r, e := os.OpenRoot("/proc/self/fd/" + itoa(int(d.Fd())))
		if e != nil {
			return s, e
		}
		defer r.Close()
		read = func(p string) ([]byte, error) { return readRegular(r, p, false, maxFile) }
	} else {
		b, e := readFile(archive, false, 256<<20)
		if e != nil {
			return s, e
		}
		z, e := zip.NewReader(bytes.NewReader(b), int64(len(b)))
		if e != nil {
			return s, errors.New("invalid archive")
		}
		zf := map[string]*zip.File{}
		archiveNames = map[string]bool{}
		if len(z.File) > 10000 {
			return s, errors.New("too many archive entries")
		}
		for _, f := range z.File {
			if !validPath(f.Name) || !f.Mode().IsRegular() || f.UncompressedSize64 > maxFile || archiveNames[f.Name] {
				return s, errors.New("unsafe or duplicate archive entry")
			}
			archiveNames[f.Name] = true
			zf[f.Name] = f
		}
		read = func(p string) ([]byte, error) {
			f, ok := zf[p]
			if !ok {
				return nil, errors.New("missing archive entry")
			}
			r, e := f.Open()
			if e != nil {
				return nil, e
			}
			defer r.Close()
			b, e := io.ReadAll(io.LimitReader(r, maxFile+1))
			if e != nil || len(b) > maxFile {
				return nil, errors.New("archive read failed")
			}
			return b, nil
		}
	}
	raw, e := read("SOURCE_MANIFEST.json")
	if e != nil {
		return s, e
	}
	if digest(raw) != pin {
		return s, errors.New("manifest hash mismatch")
	}
	if strict(raw, &s.Manifest) != nil || s.Manifest.Schema != 1 || len(s.Manifest.Files) == 0 || len(s.Manifest.Files) > 10000 {
		return s, errors.New("invalid source manifest")
	}
	s.Raw = raw
	total := int64(0)
	for _, f := range s.Manifest.Files {
		if !allowedSource(f.Path) || !hashPattern.MatchString(f.SHA) || f.Bytes < 0 || f.Bytes > maxFile {
			return s, errors.New("unsafe manifest entry")
		}
		if _, ok := s.Files[f.Path]; ok {
			return s, errors.New("duplicate manifest path")
		}
		total += f.Bytes
		if total > 256<<20 {
			return s, errors.New("source size limit")
		}
		b, e := read(f.Path)
		if e != nil {
			return s, e
		}
		if int64(len(b)) != f.Bytes || digest(b) != f.SHA {
			return s, errors.New("source content hash mismatch")
		}
		s.Files[f.Path] = b
	}
	if archiveNames != nil && len(archiveNames) != len(s.Files)+1 {
		return s, errors.New("unexpected archive files")
	}
	return s, nil
}
func restoreSource(s Source, dest string, failAfter int) error {
	return atomicDir(dest, func(stage string) error {
		for i, f := range s.Manifest.Files {
			if failAfter >= 0 && i == failAfter {
				return errors.New("injected interrupted write")
			}
			if e := writeFile(stage, f.Path, s.Files[f.Path]); e != nil {
				return e
			} // Runnable scripts remain text; build instructions use sh or explicit chmod after review.
		}
		return writeFile(stage, "SOURCE_MANIFEST.json", s.Raw)
	})
}
func packSource(s Source) ([]byte, error) {
	var b bytes.Buffer
	w := zip.NewWriter(&b)
	for _, f := range append([]FileEntry{{Path: "SOURCE_MANIFEST.json"}}, s.Manifest.Files...) {
		h := &zip.FileHeader{Name: f.Path, Method: zip.Deflate}
		h.SetMode(0600)
		o, e := w.CreateHeader(h)
		if e != nil {
			return nil, e
		}
		data := s.Raw
		if f.Path != "SOURCE_MANIFEST.json" {
			data = s.Files[f.Path]
		}
		if _, e = o.Write(data); e != nil {
			return nil, e
		}
	}
	if e := w.Close(); e != nil {
		return nil, e
	}
	return b.Bytes(), nil
}
func rootJoin(root, rel string) string { return filepath.Join(root, filepath.FromSlash(rel)) }
