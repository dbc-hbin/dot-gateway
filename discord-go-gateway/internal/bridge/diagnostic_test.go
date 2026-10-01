package bridge

import (
	"fmt"
	"math"
	"reflect"
	"sync"
	"testing"
)

func diagnosticLedgerSettings() Settings {
	return Settings{ExpectedBotID: "222222222222222222", Policy: Policy{OwnerID: "111111111111111111", AllowedDMIDs: []string{"111111111111111111"}, Platform: "discord", GuildID: "333333333333333333", GuildChannelID: "444444444444444444", GuildMode: "mention"}}
}
func queueDiagnosticLedger(t *testing.T, s *Store, index int) map[string]any {
	t.Helper()
	st, err := s.QueueDiagnostic(diagnosticLedgerSettings(), index)
	if err != nil {
		t.Fatal(err)
	}
	return st
}
func nextDiagnosticLedger(t *testing.T, s *Store) *Diagnostic {
	t.Helper()
	d, err := s.NextDiagnostic()
	if err != nil || d == nil {
		t.Fatal(d, err)
	}
	return d
}
func TestDiagnosticLedgerThreeImmutableSlots(t *testing.T) {
	s, _ := testStore(t)
	cfg := diagnosticLedgerSettings()
	for _, i := range []int{-1, 3, 100} {
		if _, err := s.QueueDiagnostic(cfg, i); err == nil {
			t.Errorf("index %d allowed", i)
		}
	}
	for i := 0; i < 3; i++ {
		st := queueDiagnosticLedger(t, s, i)
		if st["index"] != i+1 || st["state"] != "pending" || st["attempts"] != 0 {
			t.Fatal(st)
		}
	}
	for i := 0; i < 3; i++ {
		d := nextDiagnosticLedger(t, s)
		if d.Index != i || d.Text != fmt.Sprintf("Go transport test %d/3. This measures gateway delivery only, not model response time.", i+1) || d.BotID != cfg.ExpectedBotID || d.OwnerID != cfg.Policy.OwnerID || d.GuildID != cfg.Policy.GuildID || d.ChannelID != cfg.Policy.GuildChannelID || d.Nonce != Nonce(Chunk{ReplyID: d.ID, Index: i}) {
			t.Fatal(d)
		}
		st, err := s.DiagnosticStatus(i)
		if err != nil || st["state"] != "sending" || st["attempts"] != 1 {
			t.Fatal(st, err)
		}
	}
	if d, err := s.NextDiagnostic(); err != nil || d != nil {
		t.Fatal("more than three attempts", d, err)
	}
}
func TestDiagnosticLedgerDuplicatesNeverReset(t *testing.T) {
	for _, state := range []string{"pending", "sending", "sent", "failed", "uncertain"} {
		t.Run(state, func(t *testing.T) {
			s, _ := testStore(t)
			before := queueDiagnosticLedger(t, s, 0)
			if state != "pending" {
				d := nextDiagnosticLedger(t, s)
				if state != "sending" {
					result := SendResult{State: state, Code: "test_result"}
					if state == "sent" {
						result.MessageID = "555555555555555555"
					}
					if err := s.RecordDiagnosticResult(*d, result, Diagnostics{Operation: "diagnostic", State: state}); err != nil {
						t.Fatal(err)
					}
				}
				var err error
				before, err = s.DiagnosticStatus(0)
				if err != nil {
					t.Fatal(err)
				}
			}
			after := queueDiagnosticLedger(t, s, 0)
			if !reflect.DeepEqual(before, after) {
				t.Fatalf("duplicate changed ledger\nbefore=%#v\nafter=%#v", before, after)
			}
			if state != "pending" {
				if d, err := s.NextDiagnostic(); err != nil || d != nil {
					t.Fatal("duplicate reenabled attempt", d, err)
				}
			}
		})
	}
}
func TestDiagnosticLedgerChangedTargetConflict(t *testing.T) {
	s, _ := testStore(t)
	before := queueDiagnosticLedger(t, s, 0)
	base := diagnosticLedgerSettings()
	for _, field := range []string{"bot", "owner", "guild", "channel"} {
		cfg := base
		switch field {
		case "bot":
			cfg.ExpectedBotID = "666666666666666666"
		case "owner":
			cfg.Policy.OwnerID = "666666666666666666"
			cfg.Policy.AllowedDMIDs = []string{cfg.Policy.OwnerID}
		case "guild":
			cfg.Policy.GuildID = "666666666666666666"
		case "channel":
			cfg.Policy.GuildChannelID = "666666666666666666"
		}
		if _, err := s.QueueDiagnostic(cfg, 0); err == nil {
			t.Errorf("changed %s accepted", field)
		}
	}
	after, err := s.DiagnosticStatus(0)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal(after, err)
	}
}
func TestDiagnosticLedgerConcurrentAttemptExactlyOnce(t *testing.T) {
	s, path := testStore(t)
	other, err := OpenStore(path, ledgerPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	queueDiagnosticLedger(t, s, 0)
	var wg sync.WaitGroup
	results := make(chan *Diagnostic, 16)
	for i := 0; i < 16; i++ {
		which := s
		if i%2 == 1 {
			which = other
		}
		wg.Add(1)
		go func(st *Store) {
			defer wg.Done()
			d, err := st.NextDiagnostic()
			if err != nil {
				t.Error(err)
			}
			if d != nil {
				results <- d
			}
		}(which)
	}
	wg.Wait()
	close(results)
	n := 0
	for range results {
		n++
	}
	if n != 1 {
		t.Fatalf("attempts %d", n)
	}
	st, err := s.DiagnosticStatus(0)
	if err != nil || st["attempts"] != 1 {
		t.Fatal(st, err)
	}
}
func TestDiagnosticLedgerRestartUncertainNoResend(t *testing.T) {
	s, path := testStore(t)
	queueDiagnosticLedger(t, s, 0)
	d := nextDiagnosticLedger(t, s)
	s.Close()
	s, err := OpenStore(path, ledgerPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for i := 0; i < 2; i++ {
		if err = s.RecoverDiagnostics(); err != nil {
			t.Fatal(err)
		}
	}
	st, err := s.DiagnosticStatus(0)
	if err != nil || st["state"] != "uncertain" || st["attempts"] != 1 || st["code"] != "interrupted_before_ack" {
		t.Fatal(st, err)
	}
	queueDiagnosticLedger(t, s, 0)
	if next, err := s.NextDiagnostic(); err != nil || next != nil {
		t.Fatal(next, err)
	}
	if err = s.RecordDiagnosticResult(*d, SendResult{State: "sent", MessageID: "555555555555555555"}, Diagnostics{}); err == nil {
		t.Fatal("late ack overwrote recovery")
	}
}
func TestDiagnosticLedgerDoesNotMutateOtherQueues(t *testing.T) {
	s, _ := testStore(t)
	ingestLedger(t, s, "one", "c")
	_, err := s.call(func(db *storeConn) (any, error) {
		_, err := db.Exec("CREATE TABLE test_sends(marker TEXT); INSERT INTO test_sends VALUES('unchanged')")
		return nil, err
	})
	if err != nil {
		t.Fatal(err)
	}
	queueDiagnosticLedger(t, s, 0)
	d := nextDiagnosticLedger(t, s)
	if err = s.RecordDiagnosticResult(*d, SendResult{State: "sent", MessageID: "555555555555555555"}, Diagnostics{}); err != nil {
		t.Fatal(err)
	}
	_, err = s.call(func(db *storeConn) (any, error) {
		var n int
		var state, marker string
		if err := db.QueryRow("SELECT count(*),state FROM inbound").Scan(&n, &state); err != nil {
			return nil, err
		}
		if n != 1 || state != "pending" {
			t.Error("inbound mutated")
		}
		if err := db.QueryRow("SELECT marker FROM test_sends").Scan(&marker); err != nil {
			return nil, err
		}
		if marker != "unchanged" {
			t.Error("test_sends mutated")
		}
		for _, table := range []string{"replies", "chunks"} {
			if err := db.QueryRow("SELECT count(*) FROM " + table).Scan(&n); err != nil {
				return nil, err
			}
			if n != 0 {
				t.Errorf("%s mutated", table)
			}
		}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
func TestDiagnosticLedgerTimestampAccuracy(t *testing.T) {
	for _, state := range []string{"sent", "failed", "uncertain"} {
		t.Run(state, func(t *testing.T) {
			s, _ := testStore(t)
			before := epoch()
			queued := queueDiagnosticLedger(t, s, 0)
			d := nextDiagnosticLedger(t, s)
			result := SendResult{State: state, Code: "test_code"}
			remoteMS := int64(1750000000123)
			if state == "sent" {
				result.MessageID = fmt.Sprint(uint64(remoteMS-1420070400000) << 22)
			}
			timings := Diagnostics{Operation: "diagnostic", State: state, Seconds: 0.012, PreflightSeconds: 0.008, PostSeconds: 0.004}
			if err := s.RecordDiagnosticResult(*d, result, timings); err != nil {
				t.Fatal(err)
			}
			after := epoch()
			st, err := s.DiagnosticStatus(0)
			if err != nil {
				t.Fatal(err)
			}
			created := st["created_at"].(float64)
			started := st["send_started_at"].(float64)
			if created < before || created > started || started > after || queued["created_at"] != created {
				t.Fatal("invalid chronology", st)
			}
			if math.Abs(st["queue_seconds"].(float64)-(started-created)) > 1e-9 {
				t.Fatal("queue duration mismatch", st)
			}
			if state == "sent" {
				ack, ok := st["acknowledged_at"].(float64)
				if !ok || ack < started || ack > after {
					t.Fatal("ack chronology", st)
				}
				if math.Abs(st["remote_created_at"].(float64)-float64(remoteMS)/1000) > 0.000001 {
					t.Fatal("snowflake timestamp", st)
				}
			} else if st["acknowledged_at"] != nil || st["remote_created_at"] != nil {
				t.Fatal("unacknowledged result claimed an acknowledgement", st)
			}
			if err = s.RecordDiagnosticResult(*d, result, timings); err == nil {
				t.Fatal("duplicate result accepted")
			}
		})
	}
}
func TestDiagnosticLedgerSymbolicBoundedCodes(t *testing.T) {
	s, _ := testStore(t)
	queueDiagnosticLedger(t, s, 0)
	d := nextDiagnosticLedger(t, s)
	if err := s.RecordDiagnosticResult(*d, SendResult{State: "failed", Code: "private response body or token"}, Diagnostics{Operation: "diagnostic", State: "failed", Code: "raw response body"}); err != nil {
		t.Fatal(err)
	}
	st, err := s.DiagnosticStatus(0)
	if err != nil {
		t.Fatal(err)
	}
	if code, ok := st["code"].(string); ok && code != "" {
		t.Fatal("raw result code retained", code)
	}
	if timing, ok := st["timings"].(Diagnostics); ok && timing.Code != "" {
		t.Fatal("raw timing code retained", timing.Code)
	}
}
func TestDiagnosticLedgerRejectsInvalidPinnedTargets(t *testing.T) {
	for _, field := range []string{"owner", "guild", "channel", "bot_owner"} {
		t.Run(field, func(t *testing.T) {
			s, _ := testStore(t)
			cfg := diagnosticLedgerSettings()
			switch field {
			case "owner":
				cfg.Policy.OwnerID = "not_an_id"
			case "guild":
				cfg.Policy.GuildID = "not_an_id"
			case "channel":
				cfg.Policy.GuildChannelID = "not_an_id"
			case "bot_owner":
				cfg.ExpectedBotID = cfg.Policy.OwnerID
			}
			if _, err := s.QueueDiagnostic(cfg, 0); err == nil {
				t.Fatal("invalid diagnostic target reserved")
			}
		})
	}
}
