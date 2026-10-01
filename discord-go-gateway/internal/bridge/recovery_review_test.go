package bridge

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"
)

// Independent review regressions use only disposable ledgers. They exercise
// boundaries between durable quarantine, existing consumer APIs, and readiness.
func TestReviewQuarantineRestartAndExactPromotion(t *testing.T) {
	s, path := testStore(t)
	event := ledgerEvent("quarantined", "one")
	if outcome, err := s.StageIngress(event); err != nil || outcome != "validation_staged" {
		t.Fatal(outcome, err)
	}
	if outcome, err := s.Ingest(event); err != nil || outcome != "duplicate" {
		t.Fatal("ordinary ingest bypassed quarantine", outcome, err)
	}
	if claim, err := s.ClaimNextForConsumer(300, 60, "review"); err != nil || claim != nil {
		t.Fatal("quarantine exposed to consumer", claim, err)
	}
	if rows, err := s.FeedbackRows(); err != nil || len(rows) != 0 {
		t.Fatal("quarantine exposed to feedback", rows, err)
	}
	in, err := s.NextValidation(epoch())
	if err != nil || in == nil {
		t.Fatal(in, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenStore(path, ledgerPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	recovered, err := reopened.NextValidation(epoch())
	if err != nil || recovered == nil || *recovered != *in {
		t.Fatal("restart changed quarantined work", recovered, in, err)
	}
	forged := *recovered
	forged.Event.ConversationID = "different"
	if _, err := reopened.PromoteValidation(forged); err == nil {
		t.Fatal("promotion accepted a changed route")
	}
	forged = *recovered
	forged.Event.Text = "changed"
	if _, err := reopened.PromoteValidation(forged); err == nil {
		t.Fatal("promotion accepted changed content")
	}
	if outcome, err := reopened.PromoteValidation(*recovered); err != nil || outcome != "accepted" {
		t.Fatal(outcome, err)
	}
	claim, err := reopened.ClaimNextForConsumer(300, 60, "review")
	if err != nil || claim == nil || claim.InboundID != recovered.ID || claim.Envelope != event {
		t.Fatal("promotion changed immutable input", claim, err)
	}
	var created float64
	_, err = reopened.call(func(db *storeConn) (any, error) {
		return nil, db.QueryRow("SELECT created FROM inbound WHERE id=?", recovered.ID).Scan(&created)
	})
	if err != nil || created != recovered.Created {
		t.Fatal("promotion erased validation delay", created, recovered.Created, err)
	}
}

func TestReviewBlockedQuarantineOrdersOnlyItsConversation(t *testing.T) {
	s, _ := testStore(t)
	for _, event := range []Envelope{ledgerEvent("first", "a"), ledgerEvent("second", "a"), ledgerEvent("independent", "b")} {
		if outcome, err := s.StageIngress(event); err != nil || outcome != "validation_staged" {
			t.Fatal(outcome, err)
		}
	}
	first, err := s.NextValidation(epoch())
	if err != nil || first == nil || first.Event.EventID != "first" {
		t.Fatal(first, err)
	}
	if err := s.DeferValidation(first.ID, "preflight_http_403", epoch()+60, true); err != nil {
		t.Fatal(err)
	}
	independent, err := s.NextValidation(epoch())
	if err != nil || independent == nil || independent.Event.EventID != "independent" {
		t.Fatal("blocked route prevented independent work or was overtaken", independent, err)
	}
	if outcome, err := s.PromoteValidation(*independent); err != nil || outcome != "accepted" {
		t.Fatal(outcome, err)
	}
	if next, err := s.NextValidation(epoch()); err != nil || next != nil {
		t.Fatal("later conversation input overtook blocked head", next, err)
	}
	if err := s.ResumeValidation(); err != nil {
		t.Fatal(err)
	}
	resumed, err := s.NextValidation(epoch())
	if err != nil || resumed == nil || resumed.ID != first.ID {
		t.Fatal("validated reconnect did not resume exact head", resumed, err)
	}
}

func TestReviewExpiredOrFatalReadinessCannotReportReady(t *testing.T) {
	g := &gatewayState{}
	g.publishReady(g.epoch.Load())
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	fatal := make(chan error, 1)
	if err := waitForGatewayReady(ctx, g, g.epoch.Load(), fatal); err == nil || err.Error() != "gateway_startup_timeout" {
		t.Fatal("expired attempt appeared ready", err)
	}
	fatal <- errors.New("configured_channel_mismatch")
	if err := waitForGatewayReady(context.Background(), g, g.epoch.Load(), fatal); err == nil || GatewayExitCode(ClassifyGatewayError(err)) != 2 {
		t.Fatal("permanent validation failure appeared ready/transient", err)
	}
}

func TestReviewEpochChangeDuringLookupRetainsQuarantine(t *testing.T) {
	s, cfg, message := ingressFixture(t)
	g := &gatewayState{}
	g.publishReady(g.epoch.Load())
	initialEpoch := g.epoch.Load()
	r := localREST(t, func(w http.ResponseWriter, q *http.Request) {
		g.disconnect()
		preflightHandler(w, q)
	})
	if outcome, err := receiveMessage(context.Background(), r, s, cfg, message); err != nil || outcome != "validation_staged" {
		t.Fatal(outcome, err)
	}
	in, err := s.NextValidation(epoch())
	if err != nil || in == nil {
		t.Fatal(in, err)
	}
	if err := processValidation(context.Background(), s, r, g, *in, initialEpoch); err != nil {
		t.Fatal(err)
	}
	if claim, err := s.ClaimNext(60, 0); err != nil || claim != nil {
		t.Fatal("stale-epoch lookup promoted work", claim, err)
	}
	retained, err := s.NextValidation(epoch() + 60)
	if err != nil || retained == nil || retained.ID != in.ID || retained.Event != in.Event || retained.Attempts != in.Attempts+1 {
		t.Fatal("stale epoch lost exact quarantined work", retained, err)
	}
}
