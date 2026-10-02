package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"dot-gateway/internal/bridge"
)

func TestCLIFollowup(t *testing.T) {
	s, env := cliFixture(t)
	if _, e := s.Ingest(bridge.Envelope{Platform: "discord", EventID: "3", ConversationID: "2", SenderID: "1", Text: "hello", RouteKind: "dm"}); e != nil {
		t.Fatal(e)
	}
	c, e := s.ClaimNext(300, 0)
	if e != nil || c == nil {
		t.Fatal(c, e)
	}
	id, e := s.QueueReply(c.InboundID, c.Claim, "initial")
	if e != nil {
		t.Fatal(e)
	}
	ch, e := s.NextChunk()
	if e != nil || ch == nil {
		t.Fatal(ch, e)
	}
	if e = s.RecordResult(*ch, bridge.SendResult{State: "sent", MessageID: "4"}); e != nil {
		t.Fatal(e)
	}
	r, e := cli(t, env, "finished", "followup", c.InboundID, "--claim", c.Claim, "--key", "result")
	if e != nil || r["reply_id"] != id || r["state"] != "queued" {
		t.Fatal(r, e)
	}
	r, e = cli(t, env, "finished", "followup", c.InboundID, "--claim", c.Claim, "--key", "result")
	if e != nil || r["reply_id"] != id {
		t.Fatal(r, e)
	}
	for _, flags := range [][]string{{"--claim", c.Claim}, {"--claim", c.Claim, "--key", "result", "--manifest-file", "-"}, {"--claim", "wrong", "--key", "result"}} {
		r, e = cli(t, env, "finished", append([]string{"followup", c.InboundID}, flags...)...)
		if e == nil {
			t.Fatal("accepted", r)
		}
	}
}

func TestCLIRichFollowupManifest(t *testing.T) {
	for _, input := range []string{"file", "stdin"} {
		t.Run(input, func(t *testing.T) {
			s, env := cliFixture(t)
			if _, err := s.Ingest(bridge.Envelope{Platform: "discord", EventID: "3", ConversationID: "2", SenderID: "1", Text: "make a report", RouteKind: "dm"}); err != nil {
				t.Fatal(err)
			}
			cl, err := s.ClaimNext(300, 0)
			if err != nil || cl == nil {
				t.Fatal(cl, err)
			}
			id, err := s.QueueReply(cl.InboundID, cl.Claim, "working")
			if err != nil {
				t.Fatal(err)
			}
			c, err := s.NextChunk()
			if err != nil || c == nil {
				t.Fatal(c, err)
			}
			if err = s.RecordResult(*c, bridge.SendResult{State: "sent", MessageID: "5"}); err != nil {
				t.Fatal(err)
			}
			dir, err := s.ReplyOutputDir()
			if err != nil {
				t.Fatal(err)
			}
			if err = os.WriteFile(filepath.Join(dir, "result.txt"), []byte("requested result"), 0600); err != nil {
				t.Fatal(err)
			}
			manifest := `{"version":1,"attachments":[{"path":"result.txt","description":"Requested result"}],"embeds":[{"title":"Complete"}]}`
			arg := "-"
			if input == "file" {
				arg = filepath.Join(t.TempDir(), "followup.json")
				if err = os.WriteFile(arg, []byte(manifest), 0600); err != nil {
					t.Fatal(err)
				}
			}
			args := []string{"followup", cl.InboundID, "--claim", cl.Claim, "--key", "report", "--manifest-file", arg}
			for i := 0; i < 2; i++ {
				got, err := cli(t, env, manifest, args...)
				if err != nil || got["reply_id"] != id || got["state"] != "queued" || len(got["chunks"].([]any)) != 2 {
					t.Fatal(got, err)
				}
			}
			c, err = s.NextChunk()
			if err != nil || c == nil || c.Index != 1 || c.Text != "" || c.Output == "" {
				t.Fatal(c, err)
			}
			body, typ, err := bridge.BuildReplyBody(*c, filepath.Dir(dir))
			if err != nil || !strings.HasPrefix(typ, "multipart/form-data;") || !strings.Contains(string(body), "requested result") {
				t.Fatal(typ, err)
			}
			metadata, _ := json.Marshal(c)
			if strings.Contains(string(metadata), dir) || strings.Contains(string(metadata), "requested result") {
				t.Fatal("private source leaked into metadata")
			}
			got, err := cli(t, env, manifest, append(args, "--text-file", "-")...)
			if err == nil || got["error"] != "reply_input_conflict" {
				t.Fatal(got, err)
			}
		})
	}
}

func TestCLIRichFollowupRejectsInvalidManifest(t *testing.T) {
	_, env := cliFixture(t)
	for _, raw := range []string{
		`{"version":1,"text":"result","channel_id":"99"}`,
		`{"version":1,"attachments":[{"path":"../secret"}]}`,
		`{"version":1,"text":"result","version":1}`,
		`{"version":2,"text":"result"}`,
		strings.Repeat("x", bridge.MaxReplyManifestBytes+1),
	} {
		got, err := cli(t, env, raw, "followup", "unknown", "--claim", "claim", "--key", "result", "--manifest-file", "-")
		if err == nil || got["error"] == nil {
			t.Fatal("invalid manifest accepted", got)
		}
	}
}
