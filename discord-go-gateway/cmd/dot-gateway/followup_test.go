package main

import (
	"dot-gateway/internal/bridge"
	"testing"
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
