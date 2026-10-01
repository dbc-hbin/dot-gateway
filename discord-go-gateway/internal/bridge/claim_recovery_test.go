package bridge

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

func claimForRecovery(t *testing.T, s *Store, consumer string) *Claim {
	t.Helper()
	c, err := s.ClaimNextForConsumer(300, 60, consumer)
	if err != nil || c == nil {
		t.Fatalf("claim for %q: %#v, %v", consumer, c, err)
	}
	return c
}

func expireRecoveryClaim(t *testing.T, s *Store, c *Claim) {
	t.Helper()
	_, err := s.call(func(db *storeConn) (any, error) {
		_, err := db.Exec("UPDATE inbound SET lease_until=0 WHERE id=?", c.InboundID)
		return nil, err
	})
	if err != nil {
		t.Fatal(err)
	}
}

func assertSameRecoveryClaim(t *testing.T, got, want *Claim) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("claim recovery changed the claim:\n got %#v\nwant %#v", got, want)
	}
}

func TestClaimRecoveryReplaysWithoutRenewingOrDequeuing(t *testing.T) {
	s, _ := testStore(t)
	ingestLedger(t, s, "first", "conversation-one")
	ingestLedger(t, s, "second", "conversation-two")
	first := claimForRecovery(t, s, "worker.alpha:1")
	// Fixed, distinct stored deadlines make accidental renewals detectable without
	// sleeps. Expired processing must not be restarted by replaying a live claim.
	first.LeaseUntil = epoch() + 100
	processingUntil := epoch() - 1
	_, err := s.call(func(db *storeConn) (any, error) {
		if _, err := db.Exec("UPDATE inbound SET lease_until=? WHERE id=?", first.LeaseUntil, first.InboundID); err != nil {
			return nil, err
		}
		_, err := db.Exec("UPDATE processing SET until=? WHERE inbound_id=?", processingUntil, first.InboundID)
		return nil, err
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		replayed, err := s.ClaimNextForConsumer(3600, 300, "worker.alpha:1")
		if err != nil {
			t.Fatal(err)
		}
		assertSameRecoveryClaim(t, replayed, first)
	}
	_, err = s.call(func(db *storeConn) (any, error) {
		var claim string
		var until float64
		if err := db.QueryRow("SELECT claim,until FROM processing WHERE inbound_id=?", first.InboundID).Scan(&claim, &until); err != nil {
			return nil, err
		}
		if claim != first.Claim || until != processingUntil {
			t.Errorf("replay restarted processing: claim=%q until=%v", claim, until)
		}
		var claimed, pending, timings int
		if err := db.QueryRow("SELECT count(*) FROM inbound WHERE state='claimed'").Scan(&claimed); err != nil {
			return nil, err
		}
		if err := db.QueryRow("SELECT count(*) FROM inbound WHERE state='pending'").Scan(&pending); err != nil {
			return nil, err
		}
		if err := db.QueryRow("SELECT count(*) FROM timings WHERE stage='claimed'").Scan(&timings); err != nil {
			return nil, err
		}
		if claimed != 1 || pending != 1 || timings != 1 {
			t.Errorf("replay changed claim lifecycle: claimed=%d pending=%d timings=%d", claimed, pending, timings)
		}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestClaimRecoverySurvivesStoreRestart(t *testing.T) {
	s, path := testStore(t)
	ingestLedger(t, s, "first", "conversation")
	first := claimForRecovery(t, s, "durable-worker")
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenStore(path, ledgerPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	assertSameRecoveryClaim(t, claimForRecovery(t, reopened, "durable-worker"), first)
	for _, consumer := range []string{"different-worker", ""} {
		got, err := reopened.ClaimNextForConsumer(300, 60, consumer)
		if err != nil || got != nil {
			t.Fatalf("consumer %q obtained another consumer's claim: %#v, %v", consumer, got, err)
		}
	}
	if got, err := reopened.ClaimNext(300, 60); err != nil || got != nil {
		t.Fatalf("legacy caller obtained owned claim: %#v, %v", got, err)
	}
}

func TestClaimRecoveryConcurrentSeparateStoresReturnSameClaim(t *testing.T) {
	s, path := testStore(t)
	other, err := OpenStore(path, ledgerPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	ingestLedger(t, s, "first", "conversation-one")
	ingestLedger(t, s, "second", "conversation-two")
	type result struct {
		claim *Claim
		err   error
	}
	start := make(chan struct{})
	results := make(chan result, 2)
	for _, store := range []*Store{s, other} {
		go func(store *Store) {
			<-start
			claim, err := store.ClaimNextForConsumer(300, 60, "shared-worker")
			results <- result{claim, err}
		}(store)
	}
	close(start)
	first, second := <-results, <-results
	if first.err != nil || second.err != nil || first.claim == nil || second.claim == nil {
		t.Fatalf("concurrent recovery failed: %#v, %#v", first, second)
	}
	assertSameRecoveryClaim(t, second.claim, first.claim)
	if first.claim.Envelope.EventID != "first" {
		t.Fatalf("claim skipped queue head: %#v", first.claim)
	}
	// A different worker can still take independent work, but cannot steal the
	// shared worker's live claim.
	independent := claimForRecovery(t, other, "independent-worker")
	if independent.InboundID == first.claim.InboundID || independent.Envelope.EventID != "second" {
		t.Fatalf("independent claim was not isolated: %#v", independent)
	}
}

func TestClaimRecoveryLegacyCallsDoNotRecoverActiveClaims(t *testing.T) {
	s, _ := testStore(t)
	ingestLedger(t, s, "legacy-first", "conversation")
	ingestLedger(t, s, "legacy-next", "conversation")
	legacy := claimLedger(t, s)
	for _, consumer := range []string{"", "named-worker"} {
		got, err := s.ClaimNextForConsumer(300, 60, consumer)
		if err != nil || got != nil {
			t.Fatalf("consumer %q recovered anonymous claim: %#v, %v", consumer, got, err)
		}
	}
	if got, err := s.ClaimNext(300, 60); err != nil || got != nil {
		t.Fatalf("legacy call replayed active claim: %#v, %v", got, err)
	}
	expireRecoveryClaim(t, s, legacy)
	reclaimed := claimForRecovery(t, s, "named-worker")
	if reclaimed.InboundID != legacy.InboundID || reclaimed.Claim == legacy.Claim {
		t.Fatalf("anonymous expired claim was not reclaimed with a fresh token: %#v", reclaimed)
	}
}

func TestClaimRecoveryExpiryReclaimsAndRejectsStaleOwnership(t *testing.T) {
	for _, nextConsumer := range []string{"original-worker", "replacement-worker"} {
		t.Run(nextConsumer, func(t *testing.T) {
			s, _ := testStore(t)
			ingestLedger(t, s, "first", "conversation")
			old := claimForRecovery(t, s, "original-worker")
			expireRecoveryClaim(t, s, old)
			current := claimForRecovery(t, s, nextConsumer)
			if current.InboundID != old.InboundID || current.Claim == old.Claim || current.LeaseUntil <= epoch() {
				t.Fatalf("expired claim did not get a fresh live token: old=%#v current=%#v", old, current)
			}
			if err := s.Renew(old.InboundID, old.Claim, 300); !errors.Is(err, ErrClaim) {
				t.Fatalf("stale renewal: %v", err)
			}
			if err := s.BeginProcessing(old.InboundID, old.Claim, 60); !errors.Is(err, ErrClaim) {
				t.Fatalf("stale processing: %v", err)
			}
			if _, err := s.QueueReply(old.InboundID, old.Claim, "stale response"); !errors.Is(err, ErrClaim) {
				t.Fatalf("stale reply: %v", err)
			}
			if err := s.Ignore(old.InboundID, old.Claim); !errors.Is(err, ErrClaim) {
				t.Fatalf("stale ignore: %v", err)
			}
			if nextConsumer != "original-worker" {
				got, err := s.ClaimNextForConsumer(300, 60, "original-worker")
				if err != nil || got != nil {
					t.Fatalf("stale consumer recovered replacement's claim: %#v, %v", got, err)
				}
			}
			assertSameRecoveryClaim(t, claimForRecovery(t, s, nextConsumer), current)
			if err := s.Ignore(current.InboundID, current.Claim); err != nil {
				t.Fatal(err)
			}
		})
	}
}

type deniedRecoveryPolicy struct{}

func (deniedRecoveryPolicy) Allows(Envelope) bool  { return false }
func (deniedRecoveryPolicy) Accepts(Envelope) bool { return false }

func TestClaimRecoveryRechecksRevokedPolicy(t *testing.T) {
	s, path := testStore(t)
	ingestLedger(t, s, "first", "conversation")
	first := claimForRecovery(t, s, "worker")
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	revoked, err := OpenStore(path, deniedRecoveryPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	defer revoked.Close()
	if got, err := revoked.ClaimNextForConsumer(300, 60, "worker"); err != nil || got != nil {
		t.Fatalf("replayed a no-longer-authorized message: %#v, %v", got, err)
	}
	if _, err := revoked.QueueReply(first.InboundID, first.Claim, "unauthorized response"); err == nil {
		t.Fatal("revoked claim accepted a reply")
	}
	if err := revoked.Renew(first.InboundID, first.Claim, 300); !errors.Is(err, ErrClaim) {
		t.Fatalf("revoked replay left token renewable: %v", err)
	}
	if err := revoked.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenStore(path, ledgerPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got, err := reopened.ClaimNextForConsumer(300, 60, "worker"); err != nil || got != nil {
		t.Fatalf("blocked claim resurrected after restart: %#v, %v", got, err)
	}
}

func TestClaimRecoveryAdvancesAfterReplyAndIgnore(t *testing.T) {
	s, _ := testStore(t)
	for _, id := range []string{"first", "second", "third"} {
		ingestLedger(t, s, id, "conversation")
	}
	first := claimForRecovery(t, s, "worker")
	replyLedger(t, s, first, "first response")
	second := claimForRecovery(t, s, "worker")
	if second.InboundID == first.InboundID || second.Envelope.EventID != "second" {
		t.Fatalf("reply did not advance consumer: %#v", second)
	}
	// A delayed request can acquire the next event after the first reply. A
	// fresh poll must recover that new claim if the delayed result was lost.
	assertSameRecoveryClaim(t, claimForRecovery(t, s, "worker"), second)
	if err := s.Ignore(second.InboundID, second.Claim); err != nil {
		t.Fatal(err)
	}
	third := claimForRecovery(t, s, "worker")
	if third.Envelope.EventID != "third" || third.Claim == second.Claim {
		t.Fatalf("ignore did not advance consumer: %#v", third)
	}
	assertSameRecoveryClaim(t, claimForRecovery(t, s, "worker"), third)
}

func TestClaimRecoveryDoesNotResendSentOrUncertainReplies(t *testing.T) {
	for _, finalState := range []string{"sent", "uncertain"} {
		t.Run(finalState, func(t *testing.T) {
			s, path := testStore(t)
			ingestLedger(t, s, "first", "conversation")
			first := claimForRecovery(t, s, "worker")
			assertSameRecoveryClaim(t, claimForRecovery(t, s, "worker"), first)
			replyID := replyLedger(t, s, first, "one response")
			chunk, err := s.NextChunk()
			if err != nil || chunk == nil {
				t.Fatalf("initial dispatch: %#v, %v", chunk, err)
			}
			if finalState == "sent" {
				if err := s.RecordResult(*chunk, SendResult{State: "sent", MessageID: "offline-ack"}); err != nil {
					t.Fatal(err)
				}
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := OpenStore(path, ledgerPolicy{})
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			lock, err := LockDispatcher(path)
			if err != nil {
				t.Fatal(err)
			}
			defer lock.Close()
			wantRecovered := 0
			if finalState == "uncertain" {
				wantRecovered = 1
			}
			if n, err := reopened.RecoverInterrupted(); err != nil || n != wantRecovered {
				t.Fatalf("interrupted-send recovery: n=%d, %v", n, err)
			}
			if got, err := reopened.ClaimNextForConsumer(300, 60, "worker"); err != nil || got != nil {
				t.Fatalf("recovered an already-replied inbound: %#v, %v", got, err)
			}
			if id, err := reopened.QueueReply(first.InboundID, first.Claim, "one response"); err != nil || id != replyID {
				t.Fatalf("idempotent reply changed binding: id=%q, %v", id, err)
			}
			if n, err := reopened.RetryFailed(replyID); err != nil || n != 0 {
				t.Fatalf("terminal delivery became retryable: n=%d, %v", n, err)
			}
			if next, err := reopened.NextChunk(); err != nil || next != nil {
				t.Fatalf("terminal reply resent: %#v, %v", next, err)
			}
			delivery, err := reopened.Delivery(replyID)
			if err != nil || delivery.State != finalState || len(delivery.Chunks) != 1 || delivery.Chunks[0].Attempts != 1 {
				t.Fatalf("delivery history changed: %#v, %v", delivery, err)
			}
		})
	}
}

func TestClaimRecoveryConsumerValidation(t *testing.T) {
	s, _ := testStore(t)
	ingestLedger(t, s, "first", "conversation")
	for _, consumer := range []string{" ", "worker name", "worker\n", "worker/one", "worker@one", "worker\x00one", "작업자", strings.Repeat("a", 129)} {
		if got, err := s.ClaimNextForConsumer(300, 60, consumer); err == nil || got != nil {
			t.Errorf("accepted invalid consumer %q: %#v, %v", consumer, got, err)
		}
	}
	valid := "AZaz09_.:-" + strings.Repeat("x", 118)
	first := claimForRecovery(t, s, valid)
	if first.Envelope.EventID != "first" {
		t.Fatalf("invalid requests changed queue: %#v", first)
	}
	assertSameRecoveryClaim(t, claimForRecovery(t, s, valid), first)
}
