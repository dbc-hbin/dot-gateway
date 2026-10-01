package bridge

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
)

type pagingFixtureRow struct {
	id      string
	created float64
	event   Envelope
}

// Fixture-only insertion keeps ordering explicit and permits historical envelopes
// whose admission policy has subsequently been revoked. Every database is temporary.
func seedPagingRows(t testing.TB, s *Store, rows []pagingFixtureRow) {
	t.Helper()
	_, err := s.call(func(db *storeConn) (any, error) {
		return transact(db, func(db *storeConn) (any, error) {
			for _, row := range rows {
				raw, err := json.Marshal(row.event)
				if err != nil {
					return nil, err
				}
				if _, err := db.Exec("INSERT INTO inbound(id,platform,event_id,envelope,created) VALUES(?,?,?,?,?)", row.id, row.event.Platform, row.event.EventID, string(raw), row.created); err != nil {
					return nil, err
				}
			}
			return nil, nil
		})
	})
	if err != nil {
		t.Fatal(err)
	}
}

func pagingRows(count int, conversation string, revoked bool) []pagingFixtureRow {
	rows := make([]pagingFixtureRow, count)
	for i := range rows {
		id := fmt.Sprintf("row-%04d", i)
		e := ledgerEvent(id, conversation)
		if revoked {
			e.SenderID = "previous-owner"
		}
		rows[i] = pagingFixtureRow{id: id, created: 1234, event: e}
	}
	return rows
}

func pagingStateCounts(t *testing.T, s *Store) map[string]int {
	t.Helper()
	v, err := s.call(func(db *storeConn) (any, error) {
		rows, err := db.Query("SELECT state,count(*) FROM inbound GROUP BY state")
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		counts := make(map[string]int)
		for rows.Next() {
			var state string
			var count int
			if err := rows.Scan(&state, &count); err != nil {
				return nil, err
			}
			counts[state] = count
		}
		return counts, rows.Err()
	})
	if err != nil {
		t.Fatal(err)
	}
	return v.(map[string]int)
}

func TestClaimPagingRevokedAcrossPages(t *testing.T) {
	for _, count := range []int{pendingClaimPageSize, 2*pendingClaimPageSize + 3} {
		for _, eligibleTail := range []bool{false, true} {
			t.Run(fmt.Sprintf("revoked_%d_tail_%t", count, eligibleTail), func(t *testing.T) {
				s, _ := testStore(t)
				rows := pagingRows(count, "revoked-conversation", true)
				if eligibleTail {
					rows = append(rows, pagingFixtureRow{"tail", 1234, ledgerEvent("tail", "eligible")})
				}
				seedPagingRows(t, s, rows)
				claim, err := s.ClaimNextForConsumer(300, 60, "paging-worker")
				if err != nil {
					t.Fatal(err)
				}
				if eligibleTail {
					if claim == nil || claim.InboundID != "tail" {
						t.Fatalf("eligible row beyond revoked pages was skipped: %#v", claim)
					}
				} else if claim != nil {
					t.Fatalf("revoked queue returned a claim: %#v", claim)
				}
				counts := pagingStateCounts(t, s)
				if counts["blocked"] != count || counts["pending"] != 0 {
					t.Fatalf("revoked candidates not all blocked: %v", counts)
				}
				// Blocked IDs remain deduplication tombstones, including on later pages.
				duplicate := ledgerEvent(rows[count-1].id, "now-authorized")
				if result, err := s.Ingest(duplicate); err != nil || result != "duplicate" {
					t.Fatalf("blocked tombstone lost: %q, %v", result, err)
				}
			})
		}
	}
}

func TestClaimPagingBusyConversationsAcrossPages(t *testing.T) {
	s, _ := testStore(t)
	ingestLedger(t, s, "live-blocker", "busy")
	blocker := claimForRecovery(t, s, "blocker")
	count := 2*pendingClaimPageSize + 3
	seedPagingRows(t, s, pagingRows(count, "busy", false))
	// A full multi-page busy queue terminates without consuming or blocking rows.
	if claim, err := s.ClaimNext(300, 0); err != nil || claim != nil {
		t.Fatalf("all-busy queue: claim=%#v error=%v", claim, err)
	}
	seedPagingRows(t, s, []pagingFixtureRow{{"tail", 1234, ledgerEvent("tail", "independent")}})
	claim := claimForRecovery(t, s, "independent-worker")
	if claim.InboundID != "tail" {
		t.Fatalf("independent work beyond busy pages was skipped: %#v", claim)
	}
	counts := pagingStateCounts(t, s)
	if counts["pending"] != count || counts["claimed"] != 2 || counts["blocked"] != 0 {
		t.Fatalf("busy candidates changed state: %v", counts)
	}
	if err := s.Ignore(blocker.InboundID, blocker.Claim); err != nil {
		t.Fatal(err)
	}
	if next := claimLedger(t, s); next.InboundID != "row-0000" {
		t.Fatalf("busy queue lost FIFO: %#v", next)
	}
}

func TestClaimPagingOrderUsesCreatedThenID(t *testing.T) {
	s, _ := testStore(t)
	rows := pagingRows(2*pendingClaimPageSize+3, "same", false)
	// All timestamps tie except the last ID, which must precede every other row.
	rows[len(rows)-1].created--
	// Reverse physical insertion order to expose accidental rowid ordering.
	for i, j := 0, len(rows)-1; i < j; i, j = i+1, j-1 {
		rows[i], rows[j] = rows[j], rows[i]
	}
	seedPagingRows(t, s, rows)
	for i := range rows {
		want := fmt.Sprintf("row-%04d", i-1)
		if i == 0 {
			want = fmt.Sprintf("row-%04d", len(rows)-1)
		}
		claim := claimLedger(t, s)
		if claim.InboundID != want {
			t.Fatalf("claim %d = %s, want %s", i, claim.InboundID, want)
		}
		if err := s.Ignore(claim.InboundID, claim.Claim); err != nil {
			t.Fatal(err)
		}
	}
	if claim, err := s.ClaimNext(300, 0); err != nil || claim != nil {
		t.Fatalf("drained queue: %#v, %v", claim, err)
	}
}

func TestClaimPagingBoundsEnvelopeDecoding(t *testing.T) {
	s, _ := testStore(t)
	seedPagingRows(t, s, pagingRows(pendingClaimPageSize+1, "same", false))
	_, err := s.call(func(db *storeConn) (any, error) {
		// An invalid payload outside this page must not be read by its query.
		_, err := db.Exec("UPDATE inbound SET envelope='invalid-json' WHERE id=?", fmt.Sprintf("row-%04d", pendingClaimPageSize))
		return nil, err
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.call(func(db *storeConn) (any, error) {
		rows, err := readPendingClaimPage(db, epoch(), nil)
		if err != nil {
			return nil, err
		}
		if len(rows) != pendingClaimPageSize || rows[0].id != "row-0000" {
			return nil, fmt.Errorf("page not bounded or ordered: %d rows", len(rows))
		}
		if _, err := readPendingClaimPage(db, epoch(), &rows[len(rows)-1]); err == nil || err.Error() != "invalid stored envelope" {
			return nil, fmt.Errorf("invalid envelope within next page accepted: %v", err)
		}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if claim := claimLedger(t, s); claim.InboundID != "row-0000" {
		t.Fatal(claim)
	}
}

func TestClaimPagingConcurrentRecoveryAcrossPages(t *testing.T) {
	s, path := testStore(t)
	seedPagingRows(t, s, pagingRows(2*pendingClaimPageSize+3, "revoked", true))
	seedPagingRows(t, s, []pagingFixtureRow{
		{"target-a", 1234, ledgerEvent("target-a", "conversation-a")},
		{"target-b", 1234, ledgerEvent("target-b", "conversation-b")},
	})
	other, err := OpenStore(path, ledgerPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	type outcome struct {
		claim *Claim
		err   error
	}
	results := make(chan outcome, 2)
	start := make(chan struct{})
	for _, store := range []*Store{s, other} {
		go func(store *Store) {
			<-start
			claim, err := store.ClaimNextForConsumer(300, 60, "shared-paging-worker")
			results <- outcome{claim, err}
		}(store)
	}
	close(start)
	first, second := <-results, <-results
	if first.err != nil || second.err != nil || first.claim == nil || second.claim == nil {
		t.Fatalf("concurrent acquisition failed: %#v %#v", first, second)
	}
	assertSameRecoveryClaim(t, second.claim, first.claim)
	if first.claim.InboundID != "target-a" {
		t.Fatalf("queue head skipped: %#v", first.claim)
	}
	if next := claimForRecovery(t, other, "independent-worker"); next.InboundID != "target-b" {
		t.Fatalf("independent identity claim: %#v", next)
	}
	if legacy, err := s.ClaimNext(300, 60); err != nil || legacy != nil {
		t.Fatalf("legacy caller replayed named claim: %#v, %v", legacy, err)
	}
	expireRecoveryClaim(t, s, first.claim)
	reclaimed := claimForRecovery(t, other, "replacement-worker")
	if reclaimed.InboundID != first.claim.InboundID || reclaimed.Claim == first.claim.Claim {
		t.Fatalf("expired ownership not replaced: %#v", reclaimed)
	}
	if err := s.Ignore(first.claim.InboundID, first.claim.Claim); !errors.Is(err, ErrClaim) {
		t.Fatalf("stale claim remains valid: %v", err)
	}
	if old, err := s.ClaimNextForConsumer(300, 60, "shared-paging-worker"); err != nil || old != nil {
		t.Fatalf("old identity recovered replacement ownership: %#v, %v", old, err)
	}
	assertSameRecoveryClaim(t, claimForRecovery(t, s, "replacement-worker"), reclaimed)
}

func TestClaimPagingPreservesQueueCapacityAndTombstones(t *testing.T) {
	s, _ := testStore(t)
	rows := pagingRows(1000, "capacity", false)
	for i := range rows {
		rows[i].event.Text = strings.Repeat("x", 8000)
	}
	seedPagingRows(t, s, rows)
	if result, err := s.Ingest(ledgerEvent("overflow", "capacity")); err != nil || result != "queue_full" {
		t.Fatalf("queue cap changed: %q, %v", result, err)
	}
	first := claimLedger(t, s)
	if first.InboundID != "row-0000" {
		t.Fatalf("full queue claim skipped head: %#v", first)
	}
	if result, err := s.Ingest(ledgerEvent("overflow", "capacity")); err != nil || result != "queue_full" {
		t.Fatalf("claim freed active capacity: %q, %v", result, err)
	}
	if err := s.Ignore(first.InboundID, first.Claim); err != nil {
		t.Fatal(err)
	}
	if result, err := s.Ingest(ledgerEvent("row-0000", "capacity")); err != nil || result != "duplicate" {
		t.Fatalf("ignored tombstone lost: %q, %v", result, err)
	}
	if result, err := s.Ingest(ledgerEvent("overflow", "capacity")); err != nil || result != "accepted" {
		t.Fatalf("released capacity unavailable: %q, %v", result, err)
	}
}

func TestClaimPagingRollsBackEarlierPagesOnClaimFailure(t *testing.T) {
	s, _ := testStore(t)
	count := 2*pendingClaimPageSize + 3
	seedPagingRows(t, s, pagingRows(count, "revoked", true))
	seedPagingRows(t, s, []pagingFixtureRow{{"tail", 1234, ledgerEvent("tail", "eligible")}})
	_, err := s.call(func(db *storeConn) (any, error) {
		_, err := db.Exec(`CREATE TRIGGER fail_claim BEFORE UPDATE OF state ON inbound
			WHEN NEW.state='claimed' BEGIN SELECT RAISE(ABORT,'injected_claim_failure'); END`)
		return nil, err
	})
	if err != nil {
		t.Fatal(err)
	}
	if claim, err := s.ClaimNextForConsumer(300, 60, "rollback-worker"); err == nil || claim != nil {
		t.Fatalf("injected claim failure ignored: %#v, %v", claim, err)
	}
	counts := pagingStateCounts(t, s)
	if counts["pending"] != count+1 || counts["blocked"] != 0 || counts["claimed"] != 0 {
		t.Fatalf("partial page mutation committed: %v", counts)
	}
	_, err = s.call(func(db *storeConn) (any, error) {
		_, err := db.Exec("DROP TRIGGER fail_claim")
		return nil, err
	})
	if err != nil {
		t.Fatal(err)
	}
	if claim := claimForRecovery(t, s, "rollback-worker"); claim.InboundID != "tail" {
		t.Fatalf("rollback changed next candidate: %#v", claim)
	}
}

func TestClaimPagingExpiredCandidateBeyondBusyPages(t *testing.T) {
	s, _ := testStore(t)
	ingestLedger(t, s, "live-blocker", "busy")
	claimForRecovery(t, s, "blocker")
	seedPagingRows(t, s, pagingRows(2*pendingClaimPageSize+3, "busy", false))
	seedPagingRows(t, s, []pagingFixtureRow{
		{"target-a-expired", 1234, ledgerEvent("expired", "independent")},
		{"target-b-pending", 1234, ledgerEvent("pending", "independent")},
	})
	_, err := s.call(func(db *storeConn) (any, error) {
		return transact(db, func(db *storeConn) (any, error) {
			if _, err := db.Exec("UPDATE inbound SET state='claimed',claim='old-token',lease_until=0 WHERE id='target-a-expired'"); err != nil {
				return nil, err
			}
			_, err := db.Exec("INSERT INTO consumer_claims(consumer,inbound_id,claim) VALUES('expired-worker','target-a-expired','old-token')")
			return nil, err
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	claim := claimForRecovery(t, s, "replacement")
	if claim.InboundID != "target-a-expired" || claim.Claim == "old-token" {
		t.Fatalf("expired candidate lost FIFO or fresh ownership: %#v", claim)
	}
	if err := s.Ignore(claim.InboundID, "old-token"); !errors.Is(err, ErrClaim) {
		t.Fatalf("expired token remains valid: %v", err)
	}
	if got, err := s.ClaimNextForConsumer(300, 60, "expired-worker"); err != nil || got != nil {
		t.Fatalf("old identity recovered expired ownership: %#v, %v", got, err)
	}
	if err := s.Ignore(claim.InboundID, claim.Claim); err != nil {
		t.Fatal(err)
	}
	if next := claimForRecovery(t, s, "replacement"); next.InboundID != "target-b-pending" {
		t.Fatalf("pending candidate after expiry skipped: %#v", next)
	}
}
