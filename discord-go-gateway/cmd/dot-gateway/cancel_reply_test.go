package main

import (
	"dot-gateway/internal/bridge"
	"testing"
)

func TestCLICancelReplyAdvancesFailedQueue(t *testing.T) {
	s, env := cliFixture(t)
	queue := func(eventID, text string) string {
		_, err := s.Ingest(bridge.Envelope{Platform: "discord", EventID: eventID, ConversationID: "2", SenderID: "1", Text: "question", RouteKind: "dm"})
		if err != nil {
			t.Fatal(err)
		}
		claim, err := s.ClaimNext(300, 0)
		if err != nil || claim == nil {
			t.Fatal(claim, err)
		}
		id, err := s.QueueReply(claim.InboundID, claim.Claim, text)
		if err != nil {
			t.Fatal(err)
		}
		return id
	}
	first := queue("3", "first answer")
	second := queue("4", "second answer")
	chunk, _ := s.NextChunk()
	if err := s.RecordResult(*chunk, bridge.SendResult{State: "failed", Code: "http_400"}); err != nil {
		t.Fatal(err)
	}
	r, err := cli(t, env, "", "cancel-reply", first)
	if err != nil || r["state"] != "cancelled" || r["reply_id"] != first {
		t.Fatal(r, err)
	}
	if r, err := cli(t, env, "", "retry-failed", first); err == nil || r["error"] == nil {
		t.Fatal("CLI revived cancelled failure", r, err)
	}
	next, err := s.NextChunk()
	if err != nil || next == nil || next.ReplyID != second {
		t.Fatal(next, err)
	}
	if r, err := cli(t, env, "", "cancel-reply", second); err == nil || r["error"] == nil {
		t.Fatal("CLI cancelled in-flight send", r, err)
	}
}
