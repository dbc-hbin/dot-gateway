package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"dot-gateway/internal/bridge"
)

func TestCLIReplyManifestFileRoundTrip(t *testing.T) {
	s, env := cliFixture(t)
	if _, e := s.Ingest(bridge.Envelope{Platform: "discord", EventID: "3", ConversationID: "2", SenderID: "1", Text: "send the report", ReceivedAt: float64(time.Now().Unix()), RouteKind: "dm"}); e != nil {
		t.Fatal(e)
	}
	r, e := cli(t, env, "", "reply-output-dir")
	if e != nil {
		t.Fatal(r, e)
	}
	dir, ok := r["directory"].(string)
	if !ok || dir == "" {
		t.Fatal(r)
	}
	info, e := os.Stat(dir)
	if e != nil || info.Mode().Perm() != 0700 {
		t.Fatal("not private", info, e)
	}
	if e = os.WriteFile(filepath.Join(dir, "report.txt"), []byte("authored report\n"), 0600); e != nil {
		t.Fatal(e)
	}
	r, e = cli(t, env, "", "next", "--begin")
	if e != nil {
		t.Fatal(r, e)
	}
	m := r["message"].(map[string]any)
	id, claim := m["inbound_id"].(string), m["claim"].(string)
	manifest := filepath.Join(t.TempDir(), "manifest.json")
	raw := []byte(`{"version":1,"text":"The report is attached","attachments":[{"path":"report.txt","description":"Requested report"}],"embeds":[{"title":"Report","description":"Written by the current assistant"}]}`)
	os.WriteFile(manifest, raw, 0600)
	r, e = cli(t, env, "", "reply", id, "--claim", claim, "--manifest-file", manifest)
	if e != nil || r["state"] != "queued" {
		t.Fatal(r, e)
	}
	c, e := s.NextChunk()
	if e != nil || c == nil || c.Output == "" {
		t.Fatal(c, e)
	}
	body, typ, e := bridge.BuildReplyBody(*c, filepath.Dir(dir))
	if e != nil || !strings.HasPrefix(typ, "multipart/form-data;") || !strings.Contains(string(body), "authored report") {
		t.Fatal(typ, e)
	}
	b, _ := json.Marshal(c)
	if strings.Contains(string(b), dir) || strings.Contains(string(b), manifest) || strings.Contains(string(b), "authored report") {
		t.Fatal("local paths/content leaked into chunk metadata")
	}
}
func TestCLIManifestInputConflictAndUnknownFields(t *testing.T) {
	s, env := cliFixture(t)
	s.Ingest(bridge.Envelope{Platform: "discord", EventID: "3", ConversationID: "2", SenderID: "1", Text: "hello", RouteKind: "dm"})
	r, e := cli(t, env, "", "next")
	if e != nil {
		t.Fatal(r, e)
	}
	m := r["message"].(map[string]any)
	id, claim := m["inbound_id"].(string), m["claim"].(string)
	r, e = cli(t, env, `{"version":1,"text":"x"}`, "reply", id, "--claim", claim, "--manifest-file", "-", "--text-file", "-")
	if e == nil || r["error"] != "reply_input_conflict" {
		t.Fatal(r, e)
	}
	r, e = cli(t, env, `{"version":1,"text":"x","channel_id":"99"}`, "reply", id, "--claim", claim, "--manifest-file", "-")
	if e == nil || r["error"] != "invalid_reply_manifest" {
		t.Fatal(r, e)
	}
	r, e = cli(t, env, `{"version":1,"text":"embed only","embeds":[{"title":"Explicit"}]}`, "reply", id, "--claim", claim, "--manifest-file", "-")
	if e != nil || r["state"] != "queued" {
		t.Fatal(r, e)
	}
}
