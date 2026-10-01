package collector

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

type boundedBuffer struct {
	bytes.Buffer
	limit int
}

func (b *boundedBuffer) Write(p []byte) (int, error) {
	if len(p) > b.limit-b.Len() {
		return 0, fmt.Errorf("bridge output exceeds %d-byte limit", b.limit)
	}
	return b.Buffer.Write(p)
}

type PythonBackend struct {
	Root, Python     string
	OperationTimeout time.Duration
}

func NewPythonBackend(root, python string, timeout time.Duration) *PythonBackend {
	if python == "" {
		python = filepath.Join(root, ".venv/bin/python")
		if _, e := os.Stat(python); e != nil {
			python = "python3"
		}
	}
	return &PythonBackend{root, python, timeout}
}
func (b *PythonBackend) Call(ctx context.Context, script string, input any, output any) error {
	if b.OperationTimeout <= 0 {
		return fmt.Errorf("operation timeout must be positive")
	}
	ctx, cancel := context.WithTimeout(ctx, b.OperationTimeout)
	defer cancel()
	data, e := json.Marshal(input)
	if e != nil {
		return e
	}
	if len(data) > BridgeLimit {
		return fmt.Errorf("bridge input exceeds 16 MiB")
	}
	cmd := exec.CommandContext(ctx, b.Python, filepath.Join(b.Root, "runtime", script))
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return os.ErrProcessDone
		}
		err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		if err == syscall.ESRCH {
			return os.ErrProcessDone
		}
		return err
	}
	cmd.WaitDelay = 2 * time.Second
	cmd.Dir = b.Root
	cmd.Stdin = bytes.NewReader(data)
	stdout := &boundedBuffer{limit: BridgeLimit}
	stderr := &boundedBuffer{limit: 256 * 1024}
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	cmd.Env = append(os.Environ(), "PYTHONUNBUFFERED=1", "PYTHONDONTWRITEBYTECODE=1")
	if e = cmd.Run(); e != nil {
		if ctx.Err() != nil {
			return fmt.Errorf("%s: %w", script, ctx.Err())
		}
		return fmt.Errorf("%s: %w: %s", script, e, Redact(strings.TrimSpace(stderr.String())))
	}
	if e = json.Unmarshal(stdout.Bytes(), output); e != nil {
		return fmt.Errorf("%s returned invalid JSON: %w", script, e)
	}
	return nil
}
func (b *PythonBackend) Listing(ctx context.Context, s Source) ([]Row, string, error) {
	var out struct {
		Rows  []Row  `json:"rows"`
		Error string `json:"error"`
	}
	e := b.Call(ctx, "collector_bridge.py", map[string]any{"op": "listing", "source": s.Name, "url": s.URL}, &out)
	return out.Rows, out.Error, e
}
func (b *PythonBackend) Body(ctx context.Context, r Row) (string, string, error) {
	var out struct {
		Text  string `json:"text"`
		Error string `json:"error"`
	}
	e := b.Call(ctx, "collector_bridge.py", map[string]any{"op": "body", "row": r}, &out)
	return out.Text, out.Error, e
}
func (b *PythonBackend) Fetch(ctx context.Context, request any) (json.RawMessage, error) {
	var out json.RawMessage
	e := b.Call(ctx, "fetch_bridge.py", request, &out)
	return out, e
}

func (b *PythonBackend) Search(ctx context.Context, query string, limit int) (json.RawMessage, error) {
	var out json.RawMessage
	e := b.Call(ctx, "search_bridge.py", map[string]any{"query": query, "limit": limit}, &out)
	return out, e
}
