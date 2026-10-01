package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestControlWrapperRejectsAllRegistrationModes(t *testing.T) {
	raw, err := os.ReadFile("../../bin/dot-bridge")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	wrapper := filepath.Join(dir, "dot-bridge")
	os.WriteFile(wrapper, raw, 0700)
	os.WriteFile(filepath.Join(dir, "dot-bridge-cli"), []byte("#!/bin/sh\nprintf '%s\\n' CLI_REACHED\n"), 0700)
	for _, args := range [][]string{{"register-commands", "--fetch", "--dry-run"}, {"register-commands", "--apply-reviewed", "--plan-file", "plan.json"}, {"register-commands", "--dry-run", "--snapshot-file", "snapshot.json"}} {
		cmd := exec.Command(wrapper, args...)
		raw, err := cmd.CombinedOutput()
		if err == nil || strings.Contains(string(raw), "CLI_REACHED") || !strings.Contains(string(raw), "unsupported_offline_command") {
			t.Fatal(args, string(raw), err)
		}
	}
}
