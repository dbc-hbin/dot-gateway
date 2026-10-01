package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func itoa(n int) string { return strconv.Itoa(n) }

type Identity struct {
	PID        int    `json:"pid"`
	BootID     string `json:"boot_id"`
	StartTicks string `json:"start_ticks"`
}
type BrokerStatus struct {
	PID               int      `json:"pid"`
	Identity          Identity `json:"identity"`
	Ready             bool     `json:"ready"`
	UpdatedAt         int64    `json:"updatedAt"`
	Backend           string   `json:"backend"`
	PlaywrightVersion string   `json:"playwrightVersion"`
	Headless          bool     `json:"headless"`
	Sandbox           bool     `json:"chromiumSandbox"`
}

func identity(pid int) (Identity, bool) {
	if pid <= 0 {
		return Identity{}, false
	}
	b, e := os.ReadFile("/proc/" + itoa(pid) + "/stat")
	if e != nil {
		return Identity{}, false
	}
	boot, e := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if e != nil {
		return Identity{}, false
	}
	i := strings.LastIndex(string(b), ") ")
	if i < 0 {
		return Identity{}, false
	}
	f := strings.Fields(string(b)[i+2:])
	if len(f) < 20 || f[0] == "Z" {
		return Identity{}, false
	}
	return Identity{pid, strings.TrimSpace(string(boot)), f[19]}, true
}

// Credential content is NEVER opened. Only owner, type, nonzero size and mode.
func credentialStatus(p string) string {
	if p == "" {
		return "not_configured"
	}
	d, e := openDir(filepath.Dir(p), true)
	if e != nil {
		return "missing_or_insecure_parent"
	}
	defer d.Close()
	st, e := os.Lstat(p)
	if os.IsNotExist(e) {
		return "missing"
	}
	if e != nil {
		return "unavailable"
	}
	u, ok := st.Sys().(*syscall.Stat_t)
	if !st.Mode().IsRegular() || !owner(st) || st.Mode().Perm() != 0600 || !ok || u.Nlink != 1 {
		return "insecure"
	}
	if st.Size() == 0 {
		return "empty"
	}
	return "present_metadata_only"
}
func health(dbPath, queue, token, github string) map[string]any {
	out := map[string]any{"schema": 1, "gateway": "not_checked", "broker": "not_checked", "credentials": map[string]string{"discord": credentialStatus(token), "github": credentialStatus(github)}, "consumer": "requires_separate_assistant_validation", "end_to_end_ready": false, "boot_persistence": "not_established"}
	if dbPath != "" {
		out["gateway"] = "unavailable"
		if db, e := dbRO(dbPath); e == nil {
			defer db.Close()
			var raw string
			if db.QueryRow("SELECT value FROM runtime WHERE key='gateway'").Scan(&raw) == nil {
				var v struct {
					State   string  `json:"state"`
					Updated float64 `json:"updated_at"`
				}
				if json.Unmarshal([]byte(raw), &v) == nil {
					age := float64(time.Now().Unix()) - v.Updated
					if v.State == "connected" && age >= -5 && age <= 15 {
						out["gateway"] = "connected_fresh_transport_only"
					} else {
						out["gateway"] = "not_connected_or_stale"
					}
				}
			}
			var n int
			if db.QueryRow("SELECT count(*) FROM inbound WHERE state IN ('pending','claimed')").Scan(&n) == nil {
				out["pending_events"] = n
			}
		}
	}
	if queue != "" {
		out["broker"] = "unavailable"
		if b, e := readFile(filepath.Join(queue, "status.json"), true, 32<<10); e == nil {
			var v BrokerStatus
			if json.Unmarshal(b, &v) == nil {
				actual, ok := identity(v.PID)
				age := time.Now().UnixMilli() - v.UpdatedAt
				if ok && actual == v.Identity && v.PID == v.Identity.PID && v.Ready && age >= -5000 && age <= 5000 && v.Backend == "playwright_headed_chromium" && !v.Headless && v.Sandbox {
					out["broker"] = "fresh_headed_process_identity_verified"
				} else {
					out["broker"] = "stale_or_identity_mismatch"
				}
			}
		}
	}
	return out
}
