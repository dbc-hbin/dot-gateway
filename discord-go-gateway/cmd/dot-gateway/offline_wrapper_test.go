package main

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestOfflineWrapperCancellationAllowlist(t *testing.T) {
	// Copy only the shell wrapper to a disposable directory with a harmless CLI
	// stub. Never execute the installed binary or access the configured ledger.
	wrapper, err := os.ReadFile(filepath.Join("..", "..", "bin", "dot-bridge"))
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "dot-bridge")
	if err := os.WriteFile(path, wrapper, 0700); err != nil {
		t.Fatal(err)
	}
	stub := "#!/bin/sh\nprintf '{\"command\":\"%s\",\"argument\":\"%s\"}\\n' \"${1-}\" \"${2-}\"\n"
	if err := os.WriteFile(filepath.Join(dir, "dot-bridge-cli"), []byte(stub), 0700); err != nil {
		t.Fatal(err)
	}
	for _, command := range []string{"cancel-reply", "retry-failed", "delivery", "reply-output-dir", "prune-reply-spool", "verify-reply", "reconcile-reply", "gateway", "recover-thread-message", "unknown"} {
		cmd := exec.Command("/bin/sh", path, command, "reply-id")
		cmd.Env = []string{"PATH=/usr/bin:/bin"}
		out, err := cmd.Output()
		var response map[string]string
		if e := json.Unmarshal(out, &response); e != nil {
			t.Fatal("wrapper did not return expected stub output", e)
		}
		switch command {
		case "cancel-reply", "retry-failed", "delivery", "reply-output-dir", "prune-reply-spool":
			if err != nil || response["command"] != command || response["argument"] != "reply-id" {
				t.Fatal("offline command did not reach harmless stub", command, response, err)
			}
		default:
			if err == nil || response["error"] != "unsupported_offline_command" {
				t.Fatal("offline wrapper exposed network command", command, response, err)
			}
		}
	}
}
