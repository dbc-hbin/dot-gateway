package bridge

import "testing"

func TestFollowupBoundIdempotentAndUncertain(t *testing.T) {
	s, path := testStore(t)
	ingestLedger(t, s, "one", "c")
	c := claimLedger(t, s)
	rid := replyLedger(t, s, c, "initial")
	if _, e := s.QueueFollowup(c.InboundID, c.Claim, "done", "result"); e == nil {
		t.Fatal("accepted before initial delivery")
	}
	ch, e := s.NextChunk()
	if e != nil || ch == nil {
		t.Fatal(ch, e)
	}
	if e = s.RecordResult(*ch, SendResult{State: "sent", MessageID: "remote-1"}); e != nil {
		t.Fatal(e)
	}
	if _, e = s.QueueFollowup(c.InboundID, "wrong", "done", "result"); e == nil {
		t.Fatal("wrong claim")
	}
	if _, e = s.QueueFollowup(c.InboundID, c.Claim, "", "result"); e == nil {
		t.Fatal("missing key")
	}
	id, e := s.QueueFollowup(c.InboundID, c.Claim, "done", "result")
	if e != nil || id != rid {
		t.Fatal(id, e)
	}
	if _, e = s.QueueFollowup(c.InboundID, c.Claim, "done", "changed"); e == nil {
		t.Fatal("mutable key")
	}
	// A separately opened CLI shares the unchanged running dispatcher's queue.
	other, e := OpenStore(path, ledgerPolicy{})
	if e != nil {
		t.Fatal(e)
	}
	defer other.Close()
	if _, e = other.QueueFollowup(c.InboundID, c.Claim, "done", "result"); e != nil {
		t.Fatal(e)
	}
	ch, e = s.NextChunk()
	if e != nil || ch == nil || ch.Index != 1 || ch.Text != "result" || ch.Source.ConversationID != "c" || ch.Source.EventID != "one" {
		t.Fatal(ch, e)
	}
	if e = s.RecordResult(*ch, SendResult{State: "uncertain", Code: "lost_ack"}); e != nil {
		t.Fatal(e)
	}
	if _, e = other.QueueFollowup(c.InboundID, c.Claim, "done", "result"); e != nil {
		t.Fatal(e)
	}
	if _, e = other.QueueFollowup(c.InboundID, c.Claim, "second", "more"); e == nil {
		t.Fatal("bypassed uncertainty")
	}
	if ch, e = s.NextChunk(); e != nil || ch != nil {
		t.Fatal("replayed", ch, e)
	}
	d, e := s.Delivery(rid)
	if e != nil || len(d.Chunks) != 2 || d.Chunks[0].MessageID == nil || *d.Chunks[0].MessageID != "remote-1" || d.Chunks[1].Attempts != 1 {
		t.Fatal(d, e)
	}
	if _, e = s.QueueReply(c.InboundID, c.Claim, "initial"); e != nil {
		t.Fatal("original changed", e)
	}
}

func TestFollowupRejectsRevokedSourceAndCancellation(t *testing.T) {
	for _, mode := range []string{"cancelled", "revoked", "interaction"} {
		t.Run(mode, func(t *testing.T) {
			s, _ := testStore(t)
			ingestLedger(t, s, "one", "c")
			c := claimLedger(t, s)
			rid := replyLedger(t, s, c, "initial")
			ch, e := s.NextChunk()
			if e != nil || ch == nil {
				t.Fatal(ch, e)
			}
			if e = s.RecordResult(*ch, SendResult{State: "sent", MessageID: "remote"}); e != nil {
				t.Fatal(e)
			}
			_, e = s.call(func(db *storeConn) (any, error) {
				switch mode {
				case "cancelled":
					_, e := db.Exec("INSERT INTO reply_cancellations VALUES(?,?,?)", rid, epoch(), "test")
					return nil, e
				case "revoked":
					_, e := db.Exec("UPDATE inbound SET claim=NULL WHERE id=?", c.InboundID)
					return nil, e
				default:
					_, e := db.Exec(`UPDATE inbound SET envelope=json_set(envelope,'$.reply_kind','interaction') WHERE id=?`, c.InboundID)
					return nil, e
				}
			})
			if e != nil {
				t.Fatal(e)
			}
			if _, e = s.QueueFollowup(c.InboundID, c.Claim, "done", "result"); e == nil {
				t.Fatal("accepted", mode)
			}
		})
	}
}
