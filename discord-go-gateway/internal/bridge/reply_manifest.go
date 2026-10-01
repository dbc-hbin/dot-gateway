package bridge

// The manifest is an opt-in capability: it can select only files deliberately
// placed in the private reply-outbox. Neither JSON nor the database contains an
// arbitrary source path for the network sender to open.
import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"time"
	"unicode/utf8"

	"golang.org/x/sys/unix"
)

const (
	MaxReplyManifestBytes = 64 * 1024
	MaxReplyFileBytes     = 8 * 1024 * 1024
	MaxReplyTotalBytes    = 16 * 1024 * 1024
	MaxReplySpoolBytes    = 128 * 1024 * 1024
	maxReplySpoolFiles    = 256
)

// ReplyManifest has no route, reference, mention, flags, or authentication fields.
// Paths are simple basenames relative to ReplyOutputDir, never arbitrary paths.
type ReplyManifest struct {
	Version     int          `json:"version"`
	Text        string       `json:"text,omitempty"`
	Attachments []ReplyFile  `json:"attachments,omitempty"`
	Embeds      []ReplyEmbed `json:"embeds,omitempty"`
}
type ReplyFile struct {
	Path        string `json:"path"`
	Filename    string `json:"filename,omitempty"`
	Description string `json:"description,omitempty"`
}
type ReplyEmbed struct {
	Title       string            `json:"title,omitempty"`
	Description string            `json:"description,omitempty"`
	URL         string            `json:"url,omitempty"`
	Color       *int              `json:"color,omitempty"`
	Fields      []ReplyEmbedField `json:"fields,omitempty"`
	Footer      *ReplyEmbedFooter `json:"footer,omitempty"`
	Author      *ReplyEmbedAuthor `json:"author,omitempty"`
	Image       *ReplyEmbedImage  `json:"image,omitempty"`
	Thumbnail   *ReplyEmbedImage  `json:"thumbnail,omitempty"`
	Timestamp   string            `json:"timestamp,omitempty"`
}
type ReplyEmbedField struct {
	Name   string `json:"name"`
	Value  string `json:"value"`
	Inline bool   `json:"inline,omitempty"`
}
type ReplyEmbedFooter struct {
	Text string `json:"text"`
}
type ReplyEmbedAuthor struct {
	Name string `json:"name"`
	URL  string `json:"url,omitempty"`
}
type ReplyEmbedImage struct {
	URL string `json:"url"`
}
type StagedReplyFile struct {
	Filename    string `json:"filename"`
	Description string `json:"description,omitempty"`
	Size        int64  `json:"size"`
	SHA256      string `json:"sha256"`
	ContentType string `json:"content_type"`
}
type replyOutput struct {
	Version     int               `json:"version"`
	Attachments []StagedReplyFile `json:"attachments,omitempty"`
	Embeds      []ReplyEmbed      `json:"embeds,omitempty"`
}

// ReplyOutputSnapshot stays an immutable value in a durable chunk. It exposes
// metadata only, not source paths, credentials, readers, or mutable file handles.
type ReplyOutputSnapshot string

func (p ReplyOutputSnapshot) MarshalJSON() ([]byte, error) {
	if p == "" {
		return []byte("null"), nil
	}
	return []byte(p), nil
}
func (p *ReplyOutputSnapshot) UnmarshalJSON(b []byte) error {
	if string(b) == "null" {
		*p = ""
		return nil
	}
	var v replyOutput
	if err := strictReplyJSON(b, &v); err != nil {
		return err
	}
	raw, _ := json.Marshal(v)
	*p = ReplyOutputSnapshot(raw)
	return nil
}

func strictReplyJSON(b []byte, v any) error {
	if len(b) > MaxReplyManifestBytes || !utf8.Valid(b) {
		return errors.New("invalid_reply_manifest")
	}
	keys := json.NewDecoder(bytes.NewReader(b))
	if err := uniqueReplyJSONValue(keys, 0); err != nil {
		return errors.New("invalid_reply_manifest")
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if d.Decode(v) != nil {
		return errors.New("invalid_reply_manifest")
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return errors.New("invalid_reply_manifest")
	}
	return nil
}

// Reject duplicate keys and deeply nested JSON instead of giving ambiguous
// manifests different meanings at different stages of the consumer pipeline.
func uniqueReplyJSONValue(d *json.Decoder, depth int) error {
	if depth > 32 {
		return errors.New("reply_json_depth")
	}
	token, err := d.Token()
	if err != nil {
		return err
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		keys := map[string]bool{}
		for d.More() {
			key, err := d.Token()
			if err != nil {
				return err
			}
			name, ok := key.(string)
			if !ok || keys[strings.ToLower(name)] {
				return errors.New("reply_json_duplicate_key")
			}
			keys[strings.ToLower(name)] = true
			if err = uniqueReplyJSONValue(d, depth+1); err != nil {
				return err
			}
		}
	case '[':
		for d.More() {
			if err := uniqueReplyJSONValue(d, depth+1); err != nil {
				return err
			}
		}
	default:
		return errors.New("reply_json_delimiter")
	}
	_, err = d.Token()
	return err
}
func ParseReplyManifest(b []byte) (ReplyManifest, error) {
	var m ReplyManifest
	if err := strictReplyJSON(b, &m); err != nil {
		return m, err
	}
	if m.Version != 1 || len(m.Attachments) > 10 || len(m.Embeds) > 10 || !utf8.ValidString(m.Text) || TextUnits(m.Text) > 16000 {
		return m, errors.New("invalid_reply_manifest")
	}
	names := map[string]bool{}
	for i := range m.Attachments {
		f := &m.Attachments[i]
		if !replySafeName(f.Path) {
			return m, errors.New("reply_file_outside_outbox")
		}
		if f.Filename == "" {
			f.Filename = f.Path
		}
		if !replySafeName(f.Filename) || names[f.Filename] || !boundedReplyText(f.Description, 1024) {
			return m, errors.New("invalid_reply_attachment")
		}
		names[f.Filename] = true
	}
	if err := validateReplyEmbeds(m.Embeds, names); err != nil {
		return m, err
	}
	if trimText(m.Text) == "" && len(m.Attachments) == 0 && len(m.Embeds) == 0 {
		return m, errors.New("empty_reply_manifest")
	}
	return m, nil
}
func replySafeName(s string) bool {
	if s == "" || len(s) > 128 || strings.HasPrefix(s, ".") || strings.ContainsAny(s, "/\\\x00\r\n\"") || !utf8.ValidString(s) {
		return false
	}
	for _, r := range s {
		if r < 32 || r == 127 {
			return false
		}
	}
	return s != "." && s != ".." && filepath.Base(s) == s
}
func boundedReplyText(s string, n int) bool {
	return utf8.ValidString(s) && TextUnits(s) <= n && !strings.ContainsRune(s, 0)
}
func replyLink(s string) bool {
	if s == "" {
		return true
	}
	u, e := url.Parse(s)
	return e == nil && u.Scheme == "https" && u.Host != "" && u.User == nil && len(s) <= 2048 && !strings.ContainsAny(s, "\r\n\x00")
}
func validateReplyEmbeds(es []ReplyEmbed, names map[string]bool) error {
	if len(es) > 10 {
		return errors.New("invalid_reply_embeds")
	}
	total := 0
	for _, e := range es {
		if !boundedReplyText(e.Title, 256) || !boundedReplyText(e.Description, 4096) || !replyLink(e.URL) || len(e.Fields) > 25 || e.Color != nil && (*e.Color < 0 || *e.Color > 0xffffff) {
			return errors.New("invalid_reply_embeds")
		}
		total += TextUnits(e.Title) + TextUnits(e.Description)
		for _, f := range e.Fields {
			if trimText(f.Name) == "" || trimText(f.Value) == "" || !boundedReplyText(f.Name, 256) || !boundedReplyText(f.Value, 1024) {
				return errors.New("invalid_reply_embeds")
			}
			total += TextUnits(f.Name) + TextUnits(f.Value)
		}
		if e.Footer != nil {
			if trimText(e.Footer.Text) == "" || !boundedReplyText(e.Footer.Text, 2048) {
				return errors.New("invalid_reply_embeds")
			}
			total += TextUnits(e.Footer.Text)
		}
		if e.Author != nil {
			if trimText(e.Author.Name) == "" || !boundedReplyText(e.Author.Name, 256) || !replyLink(e.Author.URL) {
				return errors.New("invalid_reply_embeds")
			}
			total += TextUnits(e.Author.Name)
		}
		for _, im := range []*ReplyEmbedImage{e.Image, e.Thumbnail} {
			if im != nil {
				name, ok := strings.CutPrefix(im.URL, "attachment://")
				if !ok || !names[name] {
					return errors.New("reply_embed_image_requires_attachment")
				}
			}
		}
		if e.Timestamp != "" {
			if _, err := time.Parse(time.RFC3339, e.Timestamp); err != nil {
				return errors.New("invalid_reply_embeds")
			}
		}
		if e.Title == "" && e.Description == "" && len(e.Fields) == 0 && e.Image == nil && e.Thumbnail == nil && e.Author == nil && e.Footer == nil {
			return errors.New("empty_reply_embed")
		}
	}
	if total > 6000 {
		return errors.New("reply_embeds_too_large")
	}
	return nil
}

// openReplyDirectory walks every component with O_NOFOLLOW, pinning the entire
// path before inspecting a leaf. A symlinked ancestor is never followed.
func openReplyDirectory(path string) (*os.File, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, errors.New("reply_directory_unavailable")
	}
	fd, err := unix.Open("/", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	for _, part := range strings.Split(strings.TrimPrefix(abs, "/"), "/") {
		if part == "" {
			continue
		}
		next, e := unix.Openat(fd, part, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		unix.Close(fd)
		if e != nil {
			return nil, errors.New("reply_directory_unavailable")
		}
		fd = next
	}
	f := os.NewFile(uintptr(fd), abs)
	info, e := f.Stat()
	if e != nil || !replyOwned(info) || info.Mode().Perm()&0077 != 0 {
		f.Close()
		return nil, errors.New("reply_directory_not_private")
	}
	return f, nil
}
func replyOwned(info os.FileInfo) bool {
	if info == nil {
		return false
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	return ok && int(st.Uid) == os.Getuid()
}
func ensureReplyDirectory(parent, name string) (*os.File, error) {
	p, e := openReplyDirectory(parent)
	if e != nil {
		return nil, e
	}
	defer p.Close()
	if e = unix.Mkdirat(int(p.Fd()), name, 0700); e != nil && e != syscall.EEXIST {
		return nil, errors.New("reply_directory_unavailable")
	}
	if e == nil { // Persist the new directory entry before any reply can commit.
		if e = p.Sync(); e != nil {
			return nil, errors.New("reply_directory_unavailable")
		}
	}
	fd, e := unix.Openat(int(p.Fd()), name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if e != nil {
		return nil, errors.New("reply_directory_unavailable")
	}
	f := os.NewFile(uintptr(fd), filepath.Join(parent, name))
	info, e := f.Stat()
	if e != nil || !replyOwned(info) || info.Mode().Perm()&0077 != 0 {
		f.Close()
		return nil, errors.New("reply_directory_not_private")
	}
	return f, nil
}

// ReplyStateDir namespaces output by exact database path, so sibling ledgers
// cannot accidentally prune one another's queued or uncertain artifacts.
func ReplyStateDir(databasePath string) string { return filepath.Clean(databasePath) + ".reply" }

func ensureReplyStateRoot(root string) error {
	d, err := ensureReplyDirectory(filepath.Dir(root), filepath.Base(root))
	if err != nil {
		return err
	}
	return d.Close()
}

func (s *Store) ReplyOutputDir() (string, error) {
	if err := ensureReplyStateRoot(s.replyStateDir); err != nil {
		return "", err
	}
	d, e := ensureReplyDirectory(s.replyStateDir, "reply-outbox")
	if e != nil {
		return "", e
	}
	defer d.Close()
	return filepath.Join(s.replyStateDir, "reply-outbox"), nil
}
func readReplyFile(dir *os.File, name string, max int64, private bool) ([]byte, error) {
	if !replySafeName(name) {
		return nil, errors.New("invalid_reply_filename")
	}
	fd, e := unix.Openat(int(dir.Fd()), name, unix.O_RDONLY|unix.O_NONBLOCK|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if e != nil {
		return nil, errors.New("reply_file_unavailable")
	}
	f := os.NewFile(uintptr(fd), name)
	defer f.Close()
	before, e := f.Stat()
	st, ok := beforeSys(before)
	if e != nil || !before.Mode().IsRegular() || !replyOwned(before) || !ok || st.Nlink != 1 || before.Mode().Perm()&0022 != 0 || private && before.Mode().Perm()&0077 != 0 || before.Size() <= 0 || before.Size() > max {
		return nil, errors.New("unsafe_reply_file")
	}
	b, e := io.ReadAll(io.LimitReader(f, max+1))
	after, e2 := f.Stat()
	if e != nil || e2 != nil || int64(len(b)) != before.Size() || int64(len(b)) > max || after.Size() != before.Size() || !after.ModTime().Equal(before.ModTime()) {
		return nil, errors.New("reply_file_changed")
	}
	return b, nil
}
func beforeSys(i os.FileInfo) (*syscall.Stat_t, bool) {
	if i == nil {
		return nil, false
	}
	s, ok := i.Sys().(*syscall.Stat_t)
	return s, ok
}

var replyDigestPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)
var replyStagePattern = regexp.MustCompile(`^\.stage-[0-9a-f]{32}\.tmp$`)

// Normalize equivalent registered/common MIME spellings without trusting a file
// extension. This also avoids a false mismatch when Discord normalizes WAV.
func canonicalReplyMediaType(t string) string {
	switch t {
	case "audio/wave", "audio/x-wav":
		return "audio/wav"
	case "audio/x-flac":
		return "audio/flac"
	case "image/jpg":
		return "image/jpeg"
	}
	return t
}

func stageReplyOutput(stateDir string, m ReplyManifest) (ReplyOutputSnapshot, error) {
	out := replyOutput{Version: 1, Embeds: m.Embeds}
	if len(m.Attachments) == 0 {
		b, _ := json.Marshal(out)
		return ReplyOutputSnapshot(b), nil
	}
	if err := ensureReplyStateRoot(stateDir); err != nil {
		return "", err
	}
	input, e := ensureReplyDirectory(stateDir, "reply-outbox")
	if e != nil {
		return "", e
	}
	defer input.Close()
	spool, e := ensureReplyDirectory(stateDir, "reply-spool")
	if e != nil {
		return "", e
	}
	defer spool.Close()
	// The lock serializes quota checks across independent CLI processes. It is a
	// fixed private inode opened without symlink traversal and never sent anywhere.
	lockFD, e := unix.Openat(int(spool.Fd()), ".lock", unix.O_CREAT|unix.O_RDWR|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0600)
	if e != nil {
		return "", errors.New("reply_spool_unavailable")
	}
	lock := os.NewFile(uintptr(lockFD), "reply-spool-lock")
	defer lock.Close()
	li, e := lock.Stat()
	if e != nil || !replyOwned(li) || !li.Mode().IsRegular() || li.Mode().Perm()&0077 != 0 {
		return "", errors.New("reply_spool_unavailable")
	}
	if unix.Flock(lockFD, unix.LOCK_EX) != nil {
		return "", errors.New("reply_spool_unavailable")
	}
	defer unix.Flock(lockFD, unix.LOCK_UN)
	entries, e := spool.ReadDir(-1)
	if e != nil {
		return "", errors.New("reply_spool_unavailable")
	}
	var used int64
	count := 0
	for _, ent := range entries {
		if ent.Name() == ".lock" {
			continue
		}
		info, e := ent.Info()
		if e != nil || !info.Mode().IsRegular() || !replyOwned(info) {
			return "", errors.New("reply_spool_unavailable")
		}
		if replyStagePattern.MatchString(ent.Name()) {
			st, ok := beforeSys(info)
			if !ok || st.Nlink != 1 || info.Mode().Perm()&0077 != 0 {
				return "", errors.New("reply_spool_unavailable")
			}
			// No other stage owns this flock: an unpublished temp is an orphan.
			if unix.Unlinkat(int(spool.Fd()), ent.Name(), 0) != nil {
				return "", errors.New("reply_spool_cleanup_failed")
			}
			continue
		}
		used += info.Size()
		count++
	}
	var total int64
	for _, f := range m.Attachments {
		b, e := readReplyFile(input, f.Path, MaxReplyFileBytes, false)
		if e != nil {
			return "", e
		}
		total += int64(len(b))
		if total > MaxReplyTotalBytes {
			return "", errors.New("reply_attachments_too_large")
		}
		kind := http.DetectContentType(b)
		kind, _, _ = mime.ParseMediaType(kind)
		kind = canonicalReplyMediaType(kind)
		if kind == "" {
			kind = "application/octet-stream"
		}
		sum := sha256.Sum256(b)
		digest := hex.EncodeToString(sum[:])
		name := digest + ".blob"
		stored, e := readReplyFile(spool, name, MaxReplyFileBytes, true)
		if e == nil {
			if !bytes.Equal(stored, b) {
				return "", errors.New("reply_spool_integrity_failed")
			}
		} else {
			// EXCL also rejects unexpected symlinks and other existing inode types.
			if used+int64(len(b)) > MaxReplySpoolBytes || count >= maxReplySpoolFiles {
				return "", errors.New("reply_spool_quota_exceeded")
			}
			// fsync a private temp inode before atomically publishing the digest.
			// A crash cannot leave a truncated final blob that poisons future use.
			token, err := uuidHex()
			if err != nil {
				return "", errors.New("reply_spool_write_failed")
			}
			temp := ".stage-" + token + ".tmp"
			fd, err := unix.Openat(int(spool.Fd()), temp, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0400)
			if err != nil {
				return "", errors.New("reply_spool_write_failed")
			}
			target := os.NewFile(uintptr(fd), temp)
			_, err = target.Write(b)
			if err == nil {
				err = target.Sync()
			}
			ce := target.Close()
			if err != nil || ce != nil {
				unix.Unlinkat(int(spool.Fd()), temp, 0)
				return "", errors.New("reply_spool_write_failed")
			}
			if err = unix.Renameat2(int(spool.Fd()), temp, int(spool.Fd()), name, unix.RENAME_NOREPLACE); err != nil {
				unix.Unlinkat(int(spool.Fd()), temp, 0)
				return "", errors.New("reply_spool_integrity_failed")
			}
			if err = spool.Sync(); err != nil {
				return "", errors.New("reply_spool_write_failed")
			}

			used += int64(len(b))
			count++
		}
		out.Attachments = append(out.Attachments, StagedReplyFile{f.Filename, f.Description, int64(len(b)), digest, kind})
	}
	raw, e := json.Marshal(out)
	if e != nil || len(raw) > MaxReplyManifestBytes {
		return "", errors.New("invalid_reply_manifest")
	}
	snapshot := ReplyOutputSnapshot(raw)
	if _, err := decodeReplyOutput(snapshot); err != nil {
		return "", err
	}
	return snapshot, nil
}
func decodeReplyOutput(p ReplyOutputSnapshot) (replyOutput, error) {
	var v replyOutput
	if p == "" {
		return v, nil
	}
	if err := strictReplyJSON([]byte(p), &v); err != nil {
		return v, err
	}
	if v.Version != 1 || len(v.Attachments) > 10 || len(v.Embeds) > 10 {
		return v, errors.New("invalid_reply_output")
	}
	names := map[string]bool{}
	types := map[string]string{}
	var total int64
	for _, f := range v.Attachments {
		if !replySafeName(f.Filename) || names[f.Filename] || !boundedReplyText(f.Description, 1024) || !replyDigestPattern.MatchString(f.SHA256) || f.Size <= 0 || f.Size > MaxReplyFileBytes {
			return v, errors.New("invalid_reply_output")
		}
		typ, _, e := mime.ParseMediaType(f.ContentType)
		if e != nil || typ != f.ContentType {
			return v, errors.New("invalid_reply_output")
		}
		names[f.Filename] = true
		types[f.Filename] = f.ContentType
		total += f.Size
	}
	if total > MaxReplyTotalBytes {
		return v, errors.New("invalid_reply_output")
	}
	if err := validateReplyEmbeds(v.Embeds, names); err != nil {
		return v, err
	}
	for _, embed := range v.Embeds {
		for _, image := range []*ReplyEmbedImage{embed.Image, embed.Thumbnail} {
			if image == nil {
				continue
			}
			typ := types[strings.TrimPrefix(image.URL, "attachment://")]
			if typ != "image/png" && typ != "image/jpeg" && typ != "image/gif" && typ != "image/webp" {
				return v, errors.New("reply_embed_attachment_not_image")
			}
		}
	}
	return v, nil
}

// BuildReplyPayload preserves the original immutable source/nonce and mention
// policy. Explicit rich embeds alone lift suppression; plain replies keep it.
func BuildReplyPayload(c Chunk) ([]byte, error) {
	out, e := decodeReplyOutput(c.Output)
	if e != nil {
		return nil, e
	}
	if trimText(c.Text) == "" && len(out.Attachments) == 0 && len(out.Embeds) == 0 {
		return nil, errors.New("empty_reply_payload")
	}
	var p map[string]any
	if json.Unmarshal(payload(c), &p) != nil {
		return nil, errors.New("invalid_reply_payload")
	}
	if len(out.Attachments) > 0 {
		a := make([]map[string]any, 0, len(out.Attachments))
		for i, f := range out.Attachments {
			v := map[string]any{"id": i, "filename": f.Filename}
			if f.Description != "" {
				v["description"] = f.Description
			}
			a = append(a, v)
		}
		p["attachments"] = a
	}
	if len(out.Embeds) > 0 {
		p["embeds"] = out.Embeds
		p["flags"] = 0
	}
	return json.Marshal(p)
}

// BuildReplyBody reads only content-addressed staged bytes. It verifies all
// digests and sizes and snapshots the complete bounded body before POST starts.
// The caller must not retry a failed/uncertain attempted HTTP request.
func BuildReplyBody(c Chunk, stateDir string) ([]byte, string, error) {
	p, e := BuildReplyPayload(c)
	if e != nil {
		return nil, "", e
	}
	return buildReplyBodyWithPayload(c, stateDir, p)
}
func buildReplyBodyWithPayload(c Chunk, stateDir string, p []byte) ([]byte, string, error) {
	out, e := decodeReplyOutput(c.Output)
	if e != nil {
		return nil, "", e
	}
	if len(out.Attachments) == 0 {
		return p, "application/json", nil
	}
	spool, e := openReplyDirectory(filepath.Join(stateDir, "reply-spool"))
	if e != nil {
		return nil, "", e
	}
	defer spool.Close()
	var b bytes.Buffer
	w := multipart.NewWriter(&b)
	field, e := w.CreateFormField("payload_json")
	if e != nil {
		return nil, "", e
	}
	field.Write(p)
	for i, f := range out.Attachments {
		data, e := readReplyFile(spool, f.SHA256+".blob", MaxReplyFileBytes, true)
		if e != nil {
			return nil, "", errors.New("reply_spool_integrity_failed")
		}
		sum := sha256.Sum256(data)
		if int64(len(data)) != f.Size || hex.EncodeToString(sum[:]) != f.SHA256 {
			return nil, "", errors.New("reply_spool_integrity_failed")
		}
		typ, _, _ := mime.ParseMediaType(http.DetectContentType(data))
		if canonicalReplyMediaType(typ) != f.ContentType {
			return nil, "", errors.New("reply_spool_integrity_failed")
		}
		h := textproto.MIMEHeader{}
		h.Set("Content-Disposition", mime.FormatMediaType("form-data", map[string]string{"name": fmt.Sprintf("files[%d]", i), "filename": f.Filename}))
		h.Set("Content-Type", f.ContentType)
		part, e := w.CreatePart(h)
		if e != nil {
			return nil, "", e
		}
		if _, e = part.Write(data); e != nil {
			return nil, "", e
		}
	}
	if e = w.Close(); e != nil {
		return nil, "", e
	}
	return b.Bytes(), w.FormDataContentType(), nil
}

// ValidateReplyOutputAck validates the complete requested output contract, not
// just message text. Attachment CDN URLs may be refreshed; URL equality is never
// a proof of file identity. Discord's returned snowflakes, filename, MIME, size,
// and descriptions form the receipt. File hashes are verified before upload.
func ValidateReplyOutputAck(a map[string]json.RawMessage, c Chunk) error {
	out, e := decodeReplyOutput(c.Output)
	if e != nil {
		return e
	}
	if c.Output == "" {
		return nil
	} // Keep legacy text acknowledgement semantics.
	var attached []struct {
		ID          string `json:"id"`
		Filename    string `json:"filename"`
		Size        int64  `json:"size"`
		ContentType string `json:"content_type"`
		Description string `json:"description"`
	}
	if raw, ok := a["attachments"]; ok {
		if json.Unmarshal(raw, &attached) != nil {
			return errors.New("invalid_output_ack")
		}
	}
	if len(attached) != len(out.Attachments) {
		return errors.New("invalid_output_ack")
	}
	ids := map[string]bool{}
	gotNames := map[string]bool{}
	fileIDs := map[string]string{}
	expected := map[string]StagedReplyFile{}
	for _, f := range out.Attachments {
		expected[f.Filename] = f
	}
	for _, got := range attached {
		want, ok := expected[got.Filename]
		typ, _, err := mime.ParseMediaType(got.ContentType)
		if !ok || !Snowflake(got.ID) || ids[got.ID] || gotNames[got.Filename] || got.Size != want.Size || err != nil || canonicalReplyMediaType(typ) != want.ContentType || got.Description != want.Description {
			return errors.New("invalid_output_ack")
		}
		ids[got.ID] = true
		gotNames[got.Filename] = true
		fileIDs[got.Filename] = got.ID
	}
	var embeds []json.RawMessage
	if raw, ok := a["embeds"]; ok {
		if json.Unmarshal(raw, &embeds) != nil {
			return errors.New("invalid_output_ack")
		}
	}
	if len(embeds) != len(out.Embeds) {
		return errors.New("invalid_output_ack")
	}
	for i, raw := range embeds {
		var got ReplyEmbed
		if json.Unmarshal(raw, &got) != nil {
			return errors.New("invalid_output_ack")
		}
		var typ struct {
			Type string `json:"type"`
		}
		json.Unmarshal(raw, &typ)
		if typ.Type != "" && typ.Type != "rich" {
			return errors.New("invalid_output_ack")
		}
		want := out.Embeds[i]
		for _, pair := range [][2]*ReplyEmbedImage{{want.Image, got.Image}, {want.Thumbnail, got.Thumbnail}} {
			if pair[0] == nil {
				if pair[1] != nil {
					return errors.New("invalid_output_ack")
				}
				continue
			}
			if pair[1] == nil {
				return errors.New("invalid_output_ack")
			}
			name := strings.TrimPrefix(pair[0].URL, "attachment://")
			if pair[1].URL != pair[0].URL {
				u, e := url.Parse(pair[1].URL)
				if e != nil || u.Scheme != "https" || u.User != nil || (u.Host != "cdn.discordapp.com" && u.Host != "media.discordapp.net") || !strings.HasPrefix(u.Path, "/attachments/") || filepath.Base(u.Path) != name {
					return errors.New("invalid_output_ack")
				}
				pieces := strings.Split(strings.Trim(u.Path, "/"), "/")
				if len(pieces) != 4 || pieces[1] != c.Source.ConversationID || pieces[2] != fileIDs[name] {
					return errors.New("invalid_output_ack")
				}
			}
		}
		if want.Timestamp != "" && got.Timestamp != "" {
			wt, we := time.Parse(time.RFC3339, want.Timestamp)
			gt, ge := time.Parse(time.RFC3339, got.Timestamp)
			if we == nil && ge == nil && wt.Equal(gt) {
				got.Timestamp = want.Timestamp
			}
		}
		got.Image = want.Image
		got.Thumbnail = want.Thumbnail
		wb, _ := json.Marshal(want)
		gb, _ := json.Marshal(got)
		if !bytes.Equal(wb, gb) {
			return errors.New("invalid_output_ack")
		}
	}
	return nil
}
