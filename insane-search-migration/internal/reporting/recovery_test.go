package reporting

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func syntheticRecoveryFence() recoveryFence {
	yes := true
	return recoveryFence{
		Schema: 1, Target: testTarget,
		Deliveries: []recoveredDelivery{{
			RunID: "delivered-run", CanonicalURLs: []string{"https://example.invalid/article"},
			SourceURL: "https://source.invalid/original", ExpectedContentHash: hashBytes([]byte("delivered payload")),
			DiscordContentHash: hashBytes([]byte("delivered chunk")), RetainedOriginalPayloadHash: hashBytes([]byte("retained payload")),
			MessageID: "101", MessageURL: messageURL(testTarget, "101"),
			DiscordCreatedAt: "2025-01-01T00:00:00Z", DiscordContentBytes: 15, DoNotReannounce: true,
		}},
		BlockedRunIDs: []string{"blocked-run"}, LatestEmptyRun: &recoveredEmptyRun{RunID: "empty-run", Scanned: 1},
		DeliveryEnabled: &yes, ActivationAuthorized: &yes,
	}
}

func writeRecoveryJSON(t *testing.T, name string, v any) {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	writeRecoveryBytes(t, name, b)
}

func writeRecoveryBytes(t *testing.T, name string, b []byte) {
	t.Helper()
	if err := os.WriteFile(name, b, 0600); err != nil {
		t.Fatal(err)
	}
}

func recoveryFixture(t *testing.T) (string, string, recoveryFence) {
	t.Helper()
	root := privateDir(t)
	out := filepath.Join(root, "reports")
	if err := os.Mkdir(out, 0700); err != nil {
		t.Fatal(err)
	}
	f := syntheticRecoveryFence()
	writeRecoveryJSON(t, filepath.Join(root, "RECOVERED_DELIVERY_FENCES.json"), f)
	writeRecoveryJSON(t, filepath.Join(out, "recovery-no-replay.json"), f)
	return root, out, f
}

func requireRecoveryError(t *testing.T, err error, want string) {
	t.Helper()
	if err == nil || err.Error() != want {
		t.Fatalf("got %v, want %s", err, want)
	}
}

func TestRecoveryPublisherNoReplayBeforeNetwork(t *testing.T) {
	for _, tc := range []struct{ name, runID, report, want string }{
		{"delivery run", "delivered-run", "new text", "recovered_run_no_replay"},
		{"blocked run", "blocked-run", "new text", "recovered_run_no_replay"},
		{"known empty run", "empty-run", "new text", "recovered_run_no_replay"},
		{"payload", "new-run", "delivered payload", "recovered_content_no_replay"},
		{"trimmed payload", "new-run", "  delivered payload\n", "recovered_content_no_replay"},
		{"chunk", "new-run", "delivered chunk", "recovered_content_no_replay"},
		{"retained payload", "new-run", "retained payload", "recovered_content_no_replay"},
		{"canonical url", "new-run", "Updated https://example.invalid/article", "recovered_url_no_replay"},
		{"url cosmetics", "new-run", "[Article](http://EXAMPLE.invalid:80/article/?tracking=new#part).", "recovered_url_no_replay"},
		{"source url", "new-run", "Source: https://source.invalid/original", "recovered_url_no_replay"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, out, _ := recoveryFixture(t)
			api := &fakeAPI{}
			c := api.client()
			defer c.Close()
			_, err := c.Publish(context.Background(), out, tc.runID, []byte(tc.report))
			requireRecoveryError(t, err, tc.want)
			if api.posts != 0 || api.gets != 0 {
				t.Fatal("network before recovery guard")
			}
			if _, err := os.Stat(filepath.Join(out, "publisher.lock")); !os.IsNotExist(err) {
				t.Fatal("publisher state changed before recovery guard")
			}
		})
	}
}

func TestRecoveryPublisherChunkHashAndFreshReport(t *testing.T) {
	root, out, f := recoveryFixture(t)
	report := []byte(strings.Repeat("x", MaxChunkUnits) + "\nsecond chunk")
	chunks, err := Split(report)
	if err != nil || len(chunks) < 2 {
		t.Fatal("invalid multichunk fixture", err)
	}
	f.Deliveries[0].DiscordContentHash = hashBytes([]byte(chunks[1]))
	writeRecoveryJSON(t, filepath.Join(root, "RECOVERED_DELIVERY_FENCES.json"), f)
	writeRecoveryJSON(t, filepath.Join(out, "recovery-no-replay.json"), f)
	api := &fakeAPI{}
	c := api.client()
	defer c.Close()
	_, err = c.Publish(context.Background(), out, "new-run", report)
	requireRecoveryError(t, err, "recovered_content_no_replay")
	if api.posts != 0 || api.gets != 0 {
		t.Fatal("network before chunk deduplication")
	}
	receipt, err := c.Publish(context.Background(), out, "fresh-run", []byte("Fresh https://different.invalid/story"))
	if err != nil || !receipt.Complete || api.posts != 1 {
		t.Fatal("authorized unrelated report failed", err)
	}
}

func TestRecoveryPublisherFailClosed(t *testing.T) {
	for _, tc := range []struct {
		name, want string
		change     func(*testing.T, string, string, recoveryFence)
	}{
		{"block", "recovery_publish_blocked", func(t *testing.T, root, _ string, _ recoveryFence) {
			writeRecoveryBytes(t, filepath.Join(root, "RECOVERY_BLOCK.json"), []byte("not even valid JSON"))
		}},
		{"local block", "recovery_publish_blocked", func(t *testing.T, _, out string, _ recoveryFence) {
			writeRecoveryBytes(t, filepath.Join(out, "RECOVERY_BLOCK.json"), []byte("{}"))
		}},
		{"missing root fence", "recovery_fence_missing_or_unreadable", func(t *testing.T, root, _ string, _ recoveryFence) {
			if err := os.Remove(filepath.Join(root, "RECOVERED_DELIVERY_FENCES.json")); err != nil {
				t.Fatal(err)
			}
		}},
		{"missing local fence", "recovery_fence_missing_or_unreadable", func(t *testing.T, _, out string, _ recoveryFence) {
			if err := os.Remove(filepath.Join(out, "recovery-no-replay.json")); err != nil {
				t.Fatal(err)
			}
		}},
		{"mismatching copies", "recovery_fence_copies_mismatch", func(t *testing.T, _, out string, f recoveryFence) {
			f.BlockedRunIDs = append(f.BlockedRunIDs, "different")
			writeRecoveryJSON(t, filepath.Join(out, "recovery-no-replay.json"), f)
		}},
		{"target mismatch", "invalid_recovery_fence", func(t *testing.T, root, _ string, f recoveryFence) {
			f.Target.ChannelID = "44"
			writeRecoveryJSON(t, filepath.Join(root, "RECOVERED_DELIVERY_FENCES.json"), f)
		}},
		{"malformed fence", "invalid_recovery_fence", func(t *testing.T, root, _ string, _ recoveryFence) {
			writeRecoveryBytes(t, filepath.Join(root, "RECOVERED_DELIVERY_FENCES.json"), []byte("{}"))
		}},
		{"unsafe file mode", "recovery_fence_read_failed", func(t *testing.T, _, out string, _ recoveryFence) {
			if err := os.Chmod(filepath.Join(out, "recovery-no-replay.json"), 0644); err != nil {
				t.Fatal(err)
			}
		}},
		{"symlink fence", "recovery_fence_read_failed", func(t *testing.T, root, out string, _ recoveryFence) {
			p := filepath.Join(out, "recovery-no-replay.json")
			if err := os.Remove(p); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(filepath.Join(root, "RECOVERED_DELIVERY_FENCES.json"), p); err != nil {
				t.Fatal(err)
			}
		}},
		{"oversized fence", "recovery_fence_read_failed", func(t *testing.T, _, out string, _ recoveryFence) {
			writeRecoveryBytes(t, filepath.Join(out, "recovery-no-replay.json"), []byte(strings.Repeat(" ", 256*1024+1)))
		}},
		{"delivery disabled", "recovery_delivery_not_authorized", func(t *testing.T, root, out string, f recoveryFence) {
			no := false
			f.DeliveryEnabled = &no
			writeRecoveryJSON(t, filepath.Join(root, "RECOVERED_DELIVERY_FENCES.json"), f)
			writeRecoveryJSON(t, filepath.Join(out, "recovery-no-replay.json"), f)
		}},
		{"activation unauthorized", "recovery_delivery_not_authorized", func(t *testing.T, root, out string, f recoveryFence) {
			no := false
			f.ActivationAuthorized = &no
			writeRecoveryJSON(t, filepath.Join(root, "RECOVERED_DELIVERY_FENCES.json"), f)
			writeRecoveryJSON(t, filepath.Join(out, "recovery-no-replay.json"), f)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root, out, f := recoveryFixture(t)
			tc.change(t, root, out, f)
			api := &fakeAPI{}
			c := api.client()
			defer c.Close()
			_, err := c.Publish(context.Background(), out, "new-run", []byte("new text"))
			requireRecoveryError(t, err, tc.want)
			if api.posts != 0 || api.gets != 0 {
				t.Fatal("network despite failed recovery gate")
			}
		})
	}
}

func TestRecoveryFenceStrictJSON(t *testing.T) {
	f := syntheticRecoveryFence()
	b, err := json.Marshal(f)
	if err != nil {
		t.Fatal(err)
	}
	valid := string(b)
	for _, invalid := range []string{
		strings.Replace(valid, `"schema":1`, `"schema":1,"schema":1`, 1),
		strings.Replace(valid, `"schema":1`, `"schema":1,"SCHEMA":1`, 1),
		strings.Replace(valid, `"schema":1`, `"schema":1,"unknown":true`, 1),
		valid + `{}`,
		strings.Replace(valid, `"blocked_run_ids":["blocked-run"]`, `"blocked_run_ids":null`, 1),
		strings.Replace(valid, `"activation_authorized":true`, `"activation_authorized":null`, 1),
		strings.Replace(valid, `"do_not_reannounce":true`, `"do_not_reannounce":false`, 1),
		strings.Replace(valid, `https://example.invalid/article`, `file:///tmp/article`, 1),
	} {
		if _, err := decodeRecoveryFence([]byte(invalid), testTarget); err == nil {
			t.Fatal("invalid recovery JSON accepted")
		}
	}
}

func TestRecoveryConfiguredRootCannotBeBypassed(t *testing.T) {
	root, out, _ := recoveryFixture(t)
	for _, arbitrary := range []string{privateDir(t), filepath.Join(root, "different-outbox")} {
		g, err := newRecoveryGuard(arbitrary, recoveredLiveTarget())
		if err != nil {
			t.Fatal(err)
		}
		if len(g.roots) != 1 || g.roots[restoredGatewayRoot] != filepath.Join(restoredGatewayRoot, "reports") {
			t.Fatal("caller state directory bypassed compiled recovery root")
		}
		// Synthetic filesystem stand-in for a configure-source deployment. Never
		// read a real configured installation or require privileged test paths.
		g.target = testTarget
		g.roots = map[string]string{root: out}
		err = g.check("delivered-run", []byte("new text"), []string{"new text"})
		requireRecoveryError(t, err, "recovered_run_no_replay")
		if g.roots[root] != out {
			t.Fatal("discovery replaced compiled reports path")
		}
	}
	other, err := newRecoveryGuard(privateDir(t), testTarget)
	if err != nil || len(other.roots) != 0 {
		t.Fatal("public synthetic target acquired live configuration", err)
	}
}

func TestRecoveryRequirementsSurviveRemoval(t *testing.T) {
	root, out, _ := recoveryFixture(t)
	g, err := newRecoveryGuard(out, testTarget)
	if err != nil {
		t.Fatal(err)
	}
	if err = g.check("fresh-run", []byte("new text"), []string{"new text"}); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{filepath.Join(root, "RECOVERED_DELIVERY_FENCES.json"), filepath.Join(out, "recovery-no-replay.json")} {
		if err := os.Remove(name); err != nil {
			t.Fatal(err)
		}
	}
	requireRecoveryError(t, g.check("fresh-run", []byte("new text"), []string{"new text"}), "recovery_fence_missing_or_unreadable")
}

func TestRecoveryMarkersAndSymlinkDirectoriesFailClosed(t *testing.T) {
	for _, marker := range []string{"RECOVERY_STATE.json", "RESTORED_SNAPSHOT.json", "POST_SNAPSHOT_RECONCILIATION_REQUIRED.json"} {
		t.Run(marker, func(t *testing.T) {
			root := privateDir(t)
			out := filepath.Join(root, "reports")
			writeRecoveryBytes(t, filepath.Join(root, marker), []byte("{}"))
			g, err := newRecoveryGuard(out, testTarget)
			if err != nil {
				t.Fatal(err)
			}
			requireRecoveryError(t, g.check("new-run", []byte("new"), []string{"new"}), "recovery_fence_missing_or_unreadable")
			if _, err := os.Stat(out); !os.IsNotExist(err) {
				t.Fatal("guard created state directory")
			}
		})
	}
	root, _, _ := recoveryFixture(t)
	link := filepath.Join(privateDir(t), "link")
	if err := os.Symlink(root, link); err != nil {
		t.Fatal(err)
	}
	g, err := newRecoveryGuard(filepath.Join(link, "reports"), testTarget)
	if err != nil {
		t.Fatal(err)
	}
	requireRecoveryError(t, g.check("new-run", []byte("new"), []string{"new"}), "recovery_directory_open_failed")
}

func TestRecoveryRecheckedAfterPreflightBeforePOST(t *testing.T) {
	for _, mutation := range []string{"add block", "remove fence"} {
		t.Run(mutation, func(t *testing.T) {
			root, out, _ := recoveryFixture(t)
			api := &fakeAPI{}
			c := api.client()
			defer c.Close()
			changed := false
			c.http.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
				if !changed {
					changed = true
					if mutation == "add block" {
						writeRecoveryBytes(t, filepath.Join(root, "RECOVERY_BLOCK.json"), []byte("{}"))
					} else if err := os.Remove(filepath.Join(out, "recovery-no-replay.json")); err != nil {
						t.Fatal(err)
					}
				}
				return api.roundTrip(r)
			})
			_, err := c.Publish(context.Background(), out, "fresh-run", []byte("new text"))
			if err == nil || api.posts != 0 || api.gets == 0 {
				t.Fatal("changed recovery gate allowed POST", err)
			}
		})
	}
}

func TestRecoveryLargeSnapshotMarkerUsesPresenceOnly(t *testing.T) {
	root, out, _ := recoveryFixture(t)
	writeRecoveryBytes(t, filepath.Join(root, "RESTORED_SNAPSHOT.json"), []byte(strings.Repeat(" ", 256*1024+1)))
	g, err := newRecoveryGuard(out, testTarget)
	if err != nil {
		t.Fatal(err)
	}
	if err = g.check("fresh-run", []byte("new text"), []string{"new text"}); err != nil {
		t.Fatal("large marker blocked valid fences", err)
	}
	if err = os.Chmod(filepath.Join(root, "RESTORED_SNAPSHOT.json"), 0644); err != nil {
		t.Fatal(err)
	}
	requireRecoveryError(t, g.check("fresh-run", []byte("new text"), []string{"new text"}), "recovery_fence_read_failed")
}
