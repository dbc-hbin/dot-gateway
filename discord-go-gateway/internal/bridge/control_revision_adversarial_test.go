package bridge

import (
	"errors"
	"testing"
)

func TestReviewReactionCannotOutliveUnderlyingSource(t *testing.T) {
	for _, mutation := range []string{"edit", "delete"} {
		for _, phase := range []string{"pending", "claimed", "queued", "sending"} {
			t.Run(mutation+"/"+phase, func(t *testing.T) {
				s, _, m, original := sourceFixture(t)
				if _, e := s.QueueReply(original.InboundID, original.Claim, "precise question"); e != nil {
					t.Fatal(e)
				}
				c, e := s.NextChunk()
				if e != nil || c == nil {
					t.Fatal(e)
				}
				if e = s.RecordResult(*c, SendResult{State: "sent", MessageID: "9"}); e != nil {
					t.Fatal(e)
				}
				target, e := s.sentControlTarget(m.ChannelID, "9")
				if e != nil {
					t.Fatal(e)
				}
				route := testEnvelope()
				route.EventID = "9"
				route.Text = ""
				route.ReplyKind = "message"
				route.Control = encodeControl(ControlEvent{Version: 1, Kind: "reaction", ID: "review", ActorID: "1", TargetMessageID: "9", TargetRequestID: target.Request, TargetRevision: target.Revision, TargetText: target.Text, Emoji: "👍", Added: true})
				if result, e := s.ingestReaction(route); e != nil || result != "accepted" {
					t.Fatal(result, e)
				}
				var cl *Claim
				if phase != "pending" {
					cl, e = s.ClaimNext(60, 0)
					if e != nil || cl == nil {
						t.Fatal(e)
					}
				}
				if phase == "queued" || phase == "sending" {
					if _, e = s.QueueReply(cl.InboundID, cl.Claim, "reaction answer"); e != nil {
						t.Fatal(e)
					}
				}
				if phase == "sending" {
					c, e = s.NextChunk()
					if e != nil || c == nil {
						t.Fatal(e)
					}
				}
				if mutation == "delete" {
					_, e = s.DeleteSource(m.ChannelID, m.GuildID, m.ID)
				} else {
					_, e = s.InvalidateSourceUpdate(m.ChannelID, m.GuildID, m.ID)
				}
				if e != nil {
					t.Fatal(e)
				}
				if phase == "pending" {
					cl, e = s.ClaimNext(60, 0)
					if e != nil || cl != nil {
						t.Fatal("stale reaction claimed", cl, e)
					}
				} else {
					if current, e := s.SourceCurrent(cl.Envelope); e != nil || current {
						t.Fatal("stale control current", current, e)
					}
					if e = s.Renew(cl.InboundID, cl.Claim, 60); !errors.Is(e, ErrClaim) {
						t.Fatal("stale renewal accepted", e)
					}
					if _, e = s.QueueReply(cl.InboundID, cl.Claim, "reaction answer"); !errors.Is(e, ErrClaim) {
						t.Fatal("stale queue/idempotency accepted", e)
					}
				}
				if phase == "queued" {
					next, e := s.NextChunk()
					if e != nil || next != nil {
						t.Fatal("stale reaction dispatched", next, e)
					}
				}
			})
		}
	}
}
