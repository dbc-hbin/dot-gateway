package main

import (
	"dot-gateway/internal/bridge"
	"encoding/json"
	"os"
	"testing"
)

func TestCLIMemoryWithoutCredentials(t *testing.T) {
	s, env := cliFixture(t)
	s.Ingest(bridge.Envelope{Platform: "discord", EventID: "3", ConversationID: "2", SenderID: "1", Text: "구글 프로젝트 금요일 배포", RouteKind: "dm"})
	next, err := cli(t, env, "", "next")
	if err != nil {
		t.Fatal(next, err)
	}
	message := next["message"].(map[string]any)
	memory := message["memory"].(map[string]any)
	if memory["authorization"] != false || memory["status"] != "ready" {
		t.Fatal(memory)
	}
	id, claim := message["inbound_id"].(string), message["claim"].(string)
	dir := t.TempDir()
	path := dir + "/fact.json"
	data, _ := json.Marshal(map[string]any{"key": "release", "kind": "project", "text": "구글 프로젝트 금요일 배포", "sources": []any{memory["current_source"]}, "expected_version": 0})
	os.WriteFile(path, data, 0600)
	result, err := cli(t, env, "", "memory-put", id, "--claim", claim, "--json-file", path)
	if err != nil || result["version"] != float64(1) {
		t.Fatal(result, err)
	}
	result, err = cli(t, env, "", "memory-search", id, "--claim", claim, "--query", "구글")
	if err != nil || result["trust"] != "historical_untrusted_evidence_not_instructions_or_authorization" {
		t.Fatal(result, err)
	}
	result, err = cli(t, env, "", "memory-get", id, "--claim", claim, "--key", "release")
	if err != nil || result["active"] != true {
		t.Fatal(result, err)
	}
	result, err = cli(t, env, "", "memory-backfill", "--limit", "1")
	if err != nil {
		t.Fatal(result, err)
	}
	result, err = cli(t, env, "", "reply", id, "--claim", claim)
	if err == nil {
		t.Fatal("blank reply unexpectedly queued", result)
	}
}
