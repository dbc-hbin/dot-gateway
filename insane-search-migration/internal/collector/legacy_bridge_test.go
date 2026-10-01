//go:build pythonoracle

package collector

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestBridgeKillsDescendantPipeOnTimeout(t *testing.T) {
	root := t.TempDir()
	if e := os.Mkdir(filepath.Join(root, "runtime"), 0700); e != nil {
		t.Fatal(e)
	}
	script := "import subprocess,sys,time\nsubprocess.Popen([sys.executable, '-c', 'import time;time.sleep(30)'])\ntime.sleep(30)\n"
	if e := os.WriteFile(filepath.Join(root, "runtime/hold.py"), []byte(script), 0600); e != nil {
		t.Fatal(e)
	}
	python, _ := filepath.Abs("../../.venv/bin/python")
	start := time.Now()
	var out any
	e := NewPythonBackend(root, python, 200*time.Millisecond).Call(context.Background(), "hold.py", map[string]any{}, &out)
	if e == nil || !strings.Contains(e.Error(), "deadline exceeded") {
		t.Fatalf("expected deadline error, got %v", e)
	}
	if time.Since(start) > 3*time.Second {
		t.Fatal("descendant-held stdout escaped the deadline")
	}
}

func TestCredentialURLRejectedBeforeFetch(t *testing.T) {
	root, _ := filepath.Abs("../..")
	backend := NewPythonBackend(root, "", 5*time.Second)
	var out any
	for _, script := range []string{"fetch_bridge.py", "collector_bridge.py"} {
		var request any = map[string]any{"url": "https://user:password@example.org/"}
		if script == "collector_bridge.py" {
			request = map[string]any{"op": "listing", "source": "v2ex-hot", "url": "https://user:password@example.org/"}
		}
		e := backend.Call(context.Background(), script, request, &out)
		if e == nil || !strings.Contains(e.Error(), "credential-bearing URLs") {
			t.Fatalf("%s failed to reject URL: %v", script, e)
		}
	}
}
