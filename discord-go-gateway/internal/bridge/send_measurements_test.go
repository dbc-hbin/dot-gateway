package bridge

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"strings"
	"testing"
)

func measurementRows(t *testing.T, s *Store, reply string) []SendMeasurement {
	t.Helper()
	v, err := s.call(func(db *storeConn) (any, error) {
		rows, err := db.Query("SELECT measurement FROM send_measurements WHERE reply_id=? ORDER BY chunk_index,attempt", reply)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		out := []SendMeasurement{}
		for rows.Next() {
			var raw string
			if err = rows.Scan(&raw); err != nil {
				return nil, err
			}
			var m SendMeasurement
			if err = json.Unmarshal([]byte(raw), &m); err != nil {
				return nil, err
			}
			out = append(out, m)
		}
		return out, rows.Err()
	})
	if err != nil {
		t.Fatal(err)
	}
	return v.([]SendMeasurement)
}
func TestSendMeasurementsImmutablePerChunkAndAttempt(t *testing.T) {
	s, _ := testStore(t)
	ingestLedger(t, s, "one", "same")
	id := replyLedger(t, s, claimLedger(t, s), strings.Repeat("x", 2100))
	c, err := s.NextChunk()
	if err != nil || c == nil {
		t.Fatal(err)
	}
	d := Diagnostics{Operation: "secret-url", State: "secret-text", Code: "secret-token", Seconds: 0.1, SendLockWaitSeconds: 0.02, IdentityGET: RequestDiagnostics{Seconds: 0.03, ConnectionObserved: true}, Post: RequestDiagnostics{Seconds: 0.07, Attempted: true, Reused: true}}
	if err = s.RecordResultMeasured(*c, SendResult{State: "failed", Code: "http_429"}, d); err != nil {
		t.Fatal(err)
	}
	d.Seconds = 999 // Caller mutation must not change the stored value.
	if n, err := s.RetryFailed(id); err != nil || n != 1 {
		t.Fatal(n, err)
	}
	c, err = s.NextChunk()
	if err != nil || c == nil {
		t.Fatal(err)
	}
	d.Seconds = 0.2
	if err = s.RecordResultMeasured(*c, SendResult{State: "sent", MessageID: "123"}, d); err != nil {
		t.Fatal(err)
	}
	c, err = s.NextChunk()
	if err != nil || c == nil || c.Index != 1 {
		t.Fatal(c, err)
	}
	d.Seconds = 0.3
	if err = s.RecordResultMeasured(*c, SendResult{State: "sent", MessageID: "124"}, d); err != nil {
		t.Fatal(err)
	}
	rows := measurementRows(t, s, id)
	if len(rows) != 3 || rows[0].Seconds != 0.1 || rows[1].Seconds != 0.2 || rows[2].Seconds != 0.3 {
		t.Fatal(rows)
	}
	if !rows[0].IdentityGET.ConnectionObserved || !rows[0].Post.Reused {
		t.Fatal(rows[0])
	}
	v, err := s.call(func(db *storeConn) (any, error) {
		var raw string
		err := db.QueryRow("SELECT group_concat(measurement) FROM send_measurements").Scan(&raw)
		return raw, err
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"secret-", "untrusted", id, c.Source.ConversationID} {
		if strings.Contains(v.(string), secret) {
			t.Fatalf("metrics leaked %s", secret)
		}
	}
}
func TestSendMeasurementFailureCannotUndoDelivery(t *testing.T) {
	for _, failure := range []string{"trigger", "missing_table", "invalid_numeric"} {
		t.Run(failure, func(t *testing.T) {
			s, _ := testStore(t)
			ingestLedger(t, s, "one", "same")
			id := replyLedger(t, s, claimLedger(t, s), "response")
			c, err := s.NextChunk()
			if err != nil {
				t.Fatal(err)
			}
			d := Diagnostics{Seconds: 0.3}
			_, err = s.call(func(db *storeConn) (any, error) {
				switch failure {
				case "trigger":
					_, err := db.Exec("CREATE TRIGGER reject_measurement BEFORE INSERT ON send_measurements BEGIN SELECT RAISE(ABORT,'offline failure'); END")
					return nil, err
				case "missing_table":
					_, err := db.Exec("DROP TABLE send_measurements")
					return nil, err
				}
				return nil, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if failure == "invalid_numeric" {
				d.Seconds = math.NaN()
			}
			if err = s.RecordResultMeasured(*c, SendResult{State: "sent", MessageID: "123"}, d); err != nil {
				t.Fatal("diagnostic failure affected delivery", err)
			}
			delivery, err := s.Delivery(id)
			if err != nil || delivery.State != "sent" || delivery.Chunks[0].Attempts != 1 {
				t.Fatal(delivery, err)
			}
			if next, err := s.NextChunk(); err != nil || next != nil {
				t.Fatal("measurement failure caused retry", next, err)
			}
		})
	}
}
func TestSendMeasurementsBoundedAndDispatchCorrelated(t *testing.T) {
	s, _ := testStore(t)
	_, err := s.call(func(db *storeConn) (any, error) {
		return transact(db, func(db *storeConn) (any, error) {
			for i := 0; i < 2048; i++ {
				if _, err := db.Exec("INSERT INTO send_measurements(reply_id,chunk_index,attempt,at,measurement) VALUES(?,0,1,0,'{}')", fmt.Sprint(i)); err != nil {
					return nil, err
				}
			}
			return nil, nil
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	ids := []string{}
	for i := 0; i < 2; i++ {
		ingestLedger(t, s, fmt.Sprint(i), fmt.Sprint(i))
		ids = append(ids, replyLedger(t, s, claimLedger(t, s), "response"))
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	g := &gatewayState{}
	g.ready.Store(true)
	n := 0
	err = dispatchLoopMeasured(ctx, s, NewWakeHub(), g, func(_ context.Context, c Chunk) (SendResult, Diagnostics) {
		n++
		if n == 2 {
			cancel()
		}
		return SendResult{State: "sent", MessageID: "123"}, Diagnostics{Seconds: float64(n), Post: RequestDiagnostics{Attempted: true}}
	})
	if err != nil {
		t.Fatal(err)
	}
	for i, id := range ids {
		m := measurementRows(t, s, id)
		if len(m) != 1 || m[0].Seconds != float64(i+1) {
			t.Fatalf("wrong chunk association %s: %+v", id, m)
		}
	}
	v, err := s.call(func(db *storeConn) (any, error) {
		var n int
		err := db.QueryRow("SELECT count(*) FROM send_measurements").Scan(&n)
		return n, err
	})
	if err != nil || v.(int) != 2048 {
		t.Fatal(v, err)
	}
}

func BenchmarkRecordResultMeasurement(b *testing.B) {
	for _, measured := range []bool{false, true} {
		b.Run(fmt.Sprint(measured), func(b *testing.B) {
			dir := b.TempDir()
			if err := os.Chmod(dir, 0700); err != nil {
				b.Fatal(err)
			}
			s, err := OpenStore(dir+"/queue.db", ledgerPolicy{})
			if err != nil {
				b.Fatal(err)
			}
			defer s.Close()
			if _, err = s.Ingest(ledgerEvent("one", "same")); err != nil {
				b.Fatal(err)
			}
			c, err := s.ClaimNext(300, 0)
			if err != nil {
				b.Fatal(err)
			}
			if _, err = s.QueueReply(c.InboundID, c.Claim, "offline"); err != nil {
				b.Fatal(err)
			}
			chunk, err := s.NextChunk()
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var err error
				if measured {
					err = s.RecordResultMeasured(*chunk, SendResult{State: "sent", MessageID: "123"}, Diagnostics{Seconds: 0.03})
				} else {
					err = s.RecordResult(*chunk, SendResult{State: "sent", MessageID: "123"})
				}
				if err != nil {
					b.Fatal(err)
				}
				b.StopTimer()
				_, err = s.call(func(db *storeConn) (any, error) {
					_, err := db.Exec("UPDATE chunks SET state='sending',attempts=attempts+1 WHERE reply_id=?", chunk.ReplyID)
					return nil, err
				})
				if err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
			}
		})
	}
}

func TestSendMeasurementTransactionFailureRetainsUncertainRecovery(t *testing.T) {
	s, _ := testStore(t)
	ingestLedger(t, s, "one", "same")
	id := replyLedger(t, s, claimLedger(t, s), "response")
	c, err := s.NextChunk()
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.call(func(db *storeConn) (any, error) {
		_, err := db.Exec("CREATE TRIGGER rollback_measurement BEFORE INSERT ON send_measurements BEGIN SELECT RAISE(ROLLBACK,'offline transaction failure'); END")
		return nil, err
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = s.RecordResultMeasured(*c, SendResult{State: "sent", MessageID: "123"}, Diagnostics{Seconds: 0.3}); err == nil {
		t.Fatal("transaction-wide failure must stop dispatch")
	}
	d, err := s.Delivery(id)
	if err != nil || d.State != "sending" {
		t.Fatal(d, err)
	}
	if n, err := s.RecoverInterrupted(); err != nil || n != 1 {
		t.Fatal(n, err)
	}
	d, err = s.Delivery(id)
	if err != nil || d.State != "uncertain" || d.Chunks[0].Attempts != 1 {
		t.Fatal(d, err)
	}
	if next, err := s.NextChunk(); err != nil || next != nil {
		t.Fatal("transaction failure retried delivery", next, err)
	}
}
