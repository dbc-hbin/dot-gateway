package headed

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

type Identity struct {
	PID        int    `json:"pid"`
	BootID     string `json:"boot_id"`
	StartTicks string `json:"start_ticks"`
}
type Status struct {
	PID               int      `json:"pid"`
	Identity          Identity `json:"identity"`
	Ready             bool     `json:"ready"`
	UpdatedAt         int64    `json:"updatedAt"`
	Backend           string   `json:"backend,omitempty"`
	PlaywrightVersion string   `json:"playwrightVersion,omitempty"`
	Headless          bool     `json:"headless"`
	ChromiumSandbox   bool     `json:"chromiumSandbox"`
}
type Client struct{ Queue string }
type packet struct {
	ID        string  `json:"id"`
	ExpiresAt int64   `json:"expiresAt"`
	Args      Request `json:"args"`
}
type response struct {
	ID       string          `json:"id"`
	OK       bool            `json:"ok"`
	Envelope json.RawMessage `json:"envelope"`
	Error    string          `json:"error"`
}

func defaultQueue(root string) string { return filepath.Join(root, "runtime", "browser", ".queue") }
func privateQueue(queue string) error {
	info, err := os.Lstat(queue)
	if err != nil {
		return err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0077 != 0 || !ok || int(stat.Uid) != os.Geteuid() {
		return errors.New("browser queue must be private (0700), same-owner, and not a symlink")
	}
	return nil
}
func readJSON(path string, limit int64, value any) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Size() > limit {
		return errors.New("invalid or oversized queue file")
	}
	data, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil {
		return err
	}
	if int64(len(data)) > limit {
		return errors.New("queue payload exceeds limit")
	}
	return json.Unmarshal(data, value)
}
func writePrivateJSON(path string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	temp := path + ".tmp"
	f, err := os.OpenFile(temp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	defer os.Remove(temp)
	if _, err = f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	return os.Rename(temp, path)
}
func ReadStatus(queue string) (Status, error) {
	var s Status
	if err := privateQueue(queue); err != nil {
		return s, err
	}
	if err := readJSON(filepath.Join(queue, "status.json"), 32<<10, &s); err != nil {
		return s, err
	}
	return s, nil
}
func fresh(s Status) bool {
	return s.Ready && s.Backend == Backend && !s.Headless && s.ChromiumSandbox && s.Identity.PID == s.PID && s.PID > 0 && time.Now().UnixMilli()-s.UpdatedAt < 10000 && s.UpdatedAt <= time.Now().Add(time.Second).UnixMilli()
}
func unlinkOwn(path string) error {
	err := os.Remove(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func (c Client) Capture(ctx context.Context, req Request) (Envelope, error) {
	var env Envelope
	if c.Queue == "" {
		return env, errors.New("headed queue path missing")
	}
	if req.URL == "" || len(req.URL) > 8192 || len(req.WaitSelector) > 2048 {
		return env, errors.New("invalid bounded browser request")
	}
	parsed, parseErr := url.Parse(req.URL)
	if parseErr != nil || parsed.Hostname() == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return env, errors.New("only absolute HTTP(S) browser URLs are allowed")
	}
	if parsed.User != nil {
		return env, errors.New("credential-bearing browser URLs are not allowed")
	}

	// Native service independently performs the authoritative URL/DNS validation.
	// The queue carries no script, profile, credentials, launch flags, or proxy.
	if req.TimeoutMS <= 0 {
		req.TimeoutMS = 90000
	}
	if req.TimeoutMS < 1000 {
		req.TimeoutMS = 1000
	}
	if req.TimeoutMS > 180000 {
		req.TimeoutMS = 180000
	}
	s, err := ReadStatus(c.Queue)
	if err != nil || !fresh(s) {
		return env, errors.New("headed_browser_unavailable: start the native headed helper")
	}
	timeout := time.Duration(req.TimeoutMS)*time.Millisecond + 15*time.Second
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	deadline, _ := ctx.Deadline()
	random := make([]byte, 16)
	if _, err = rand.Read(random); err != nil {
		return env, err
	}
	id := hex.EncodeToString(random)
	requestPath := filepath.Join(c.Queue, id+".request.json")
	responsePath := filepath.Join(c.Queue, id+".response.json")
	leasePath := filepath.Join(c.Queue, id+".lease")
	// These three UUID files belong exclusively to this call; never remove the
	// native helper's claimed working file or any other client's files.
	defer func() { _ = unlinkOwn(requestPath); _ = unlinkOwn(responsePath); _ = unlinkOwn(leasePath) }()
	lease, err := os.OpenFile(leasePath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return env, err
	}
	if err = lease.Close(); err != nil {
		return env, err
	}
	p := packet{ID: id, ExpiresAt: deadline.UnixMilli(), Args: req}
	encoded, err := json.Marshal(p)
	if err != nil {
		return env, err
	}
	if len(encoded) > MaxRequestBytes {
		return env, errors.New("browser request exceeds 32 KiB")
	}
	if err = writePrivateJSON(requestPath, p); err != nil {
		return env, err
	}
	poll := time.NewTicker(100 * time.Millisecond)
	defer poll.Stop()
	renew := time.NewTicker(time.Second)
	defer renew.Stop()
	for {
		select {
		case <-ctx.Done():
			return env, ctx.Err()
		case now := <-renew.C:
			if err = os.Chtimes(leasePath, now, now); err != nil {
				return env, fmt.Errorf("browser lease: %w", err)
			}
		case <-poll.C:
			var reply response
			err = readJSON(responsePath, MaxResponseBytes, &reply)
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			if err != nil {
				return env, err
			}
			if reply.ID != id {
				return env, errors.New("mismatched browser response identity")
			}
			if !reply.OK {
				message := reply.Error
				if len(message) > 3000 {
					message = message[:3000]
				}
				return env, fmt.Errorf("headed browser: %s", message)
			}
			var proof struct {
				Headless *bool `json:"headless"`
				Sandbox  *bool `json:"chromiumSandbox"`
			}
			if err = json.Unmarshal(reply.Envelope, &proof); err != nil {
				return env, err
			}
			if proof.Headless == nil || *proof.Headless || proof.Sandbox == nil || !*proof.Sandbox {
				return env, errors.New("headed sandbox proof missing")
			}
			dec := json.NewDecoder(bytes.NewReader(reply.Envelope))
			if err = dec.Decode(&env); err != nil {
				return env, err
			}
			if env.Automation != "playwright_headed_chromium" {
				return env, errors.New("unexpected browser backend")
			}
			if len(env.HTML) > MaxHTMLBytes {
				return env, errors.New("browser HTML exceeds limit")
			}
			return env, nil
		}
	}
}
func sanitizeError(err error) string {
	if err == nil {
		return ""
	}
	s := strings.TrimSpace(err.Error())
	if len(s) > 3000 {
		s = s[:3000]
	}
	return s
}

func (s Status) Healthy() bool { return fresh(s) }
