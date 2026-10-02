package bridge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

func registeredWorker(t *testing.T, s *Store) WorkerStatus {
	t.Helper()
	w, err := s.RegisterWorker("/root/worker", "turn-1", "/root", "running", 60, "", "native:start-1")
	if err != nil {
		t.Fatal(err)
	}
	return w
}
func boundWorker(t *testing.T, s *Store, c *Claim) WorkerStatus {
	t.Helper()
	w := registeredWorker(t, s)
	if _, err := s.BindWorkerClaim(c.InboundID, c.Claim, w.Worker, w.Incarnation, w.Controller, "native:claim-1"); err != nil {
		t.Fatal(err)
	}
	return w
}
func workerSQL(t *testing.T, s *Store, q string, args ...any) {
	t.Helper()
	_, err := s.call(func(db *storeConn) (any, error) { _, e := db.Exec(q, args...); return nil, e })
	if err != nil {
		t.Fatal(err)
	}
}

func TestWorkerRegistrationAttestationAndStaleness(t *testing.T) {
	s, _ := testStore(t)
	if _, err := s.RegisterWorker("w", "i", "c", "running", 60, "", ""); err == nil {
		t.Fatal("missing evidence accepted")
	}
	w := registeredWorker(t, s)
	again, err := s.RegisterWorker(w.Worker, w.Incarnation, w.Controller, "running", 300, "", "native:start-1")
	if err != nil || again.LeaseUntil != w.LeaseUntil {
		t.Fatal("retry refreshed lease", again, err)
	}
	if _, err = s.RegisterWorker(w.Worker, "turn-2", w.Controller, "running", 60, w.Incarnation, "native:new"); err == nil {
		t.Fatal("uninterrupted replacement accepted")
	}
	workerSQL(t, s, `UPDATE worker_runtime SET lease_until=?`, epoch()-1)
	status, err := s.WorkerStatus()
	if err != nil {
		t.Fatal(err)
	}
	observed := status["workers"].([]WorkerStatus)[0]
	if observed.Fresh || observed.State != "unknown" || observed.AttestedState != "running" || status["native_control_available"] != false {
		t.Fatal(status)
	}
	// Starting a CLI poll cannot refresh controller observation.
	poll, err := s.StartConsumerPoll("worker", time.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	defer poll.Close()
	status, err = s.Status()
	if err != nil {
		t.Fatal(err)
	}
	if status["worker_control"].(map[string]any)["workers"].([]WorkerStatus)[0].State != "unknown" {
		t.Fatal(status)
	}
	if _, err = s.ObserveWorker(w.Worker, "wrong", w.Controller, "running", 60, "native:observed"); err == nil {
		t.Fatal("stale incarnation observed")
	}
	if _, err = s.ObserveWorker(w.Worker, w.Incarnation, "other", "running", 60, "native:observed"); err == nil {
		t.Fatal("wrong controller observed")
	}
}

func TestWorkerExplicitRecoveryFencesOldClaimAndPersists(t *testing.T) {
	s, path := testStore(t)
	ingestLedger(t, s, "one", "channel")
	c := claimLedger(t, s)
	w := boundWorker(t, s, c)
	workerSQL(t, s, `UPDATE inbound SET lease_until=? WHERE id=?`, epoch()-1, c.InboundID)
	next, err := s.ClaimNext(60, 0)
	if err != nil || next != nil {
		t.Fatal("expired bound claim silently reclaimed", next, err)
	}
	workerSQL(t, s, `UPDATE worker_runtime SET lease_until=?`, epoch()-1)
	next, err = s.ClaimNext(60, 0)
	if err != nil || next != nil {
		t.Fatal("staleness restarted reasoning", next, err)
	}
	if _, err = s.ObserveWorker(w.Worker, w.Incarnation, w.Controller, "interrupted", 60, "native:interrupt-1"); err != nil {
		t.Fatal(err)
	}
	requests, err := s.ControlRequests(c.Envelope, c.InboundID)
	if err != nil || requests[0].State != "recovery_required" {
		t.Fatal(requests, err)
	}
	if err = s.Renew(c.InboundID, c.Claim, 60); !errors.Is(err, ErrClaim) {
		t.Fatal("old claim renewed", err)
	}
	if _, err = s.QueueReply(c.InboundID, c.Claim, "late"); !errors.Is(err, ErrClaim) {
		t.Fatal("old reply admitted", err)
	}
	ingestLedger(t, s, "later", "channel")
	next, err = s.ClaimNext(60, 0)
	if err != nil || next != nil {
		t.Fatal("later same-route work escaped recovery fence", next, err)
	}
	v, err := s.call(func(db *storeConn) (any, error) { return activeInboundCount(db) })
	if err != nil || v.(int) != 2 {
		t.Fatal("recovery vanished from capacity", v, err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = OpenStore(path, ledgerPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err = s.RegisterWorker(w.Worker, "turn-2", w.Controller, "running", 60, "wrong", "native:resume-2"); err == nil {
		t.Fatal("wrong predecessor accepted")
	}
	if _, err = s.RegisterWorker(w.Worker, "turn-2", w.Controller, "running", 60, w.Incarnation, "native:resume-2"); err != nil {
		t.Fatal(err)
	}
	next, err = s.ClaimNext(60, 0)
	if err != nil || next == nil || next.InboundID != c.InboundID || next.Claim == c.Claim {
		t.Fatal(next, err)
	}
	if _, err = s.BindWorkerClaim(next.InboundID, next.Claim, w.Worker, w.Incarnation, w.Controller, "native:old"); err == nil {
		t.Fatal("old incarnation rebound")
	}
	if _, err = s.ObserveWorker(w.Worker, w.Incarnation, w.Controller, "running", 60, "native:old"); err == nil {
		t.Fatal("retired worker revived")
	}
	if _, err = s.RegisterWorker(w.Worker, w.Incarnation, w.Controller, "running", 60, "turn-2", "native:reuse"); err == nil {
		t.Fatal("retired incarnation reused")
	}
}

func TestWorkerCancellationRequiresExactInterruptAcknowledgement(t *testing.T) {
	s, path := testStore(t)
	ingestLedger(t, s, "one", "channel")
	c := claimLedger(t, s)
	w := boundWorker(t, s, c)
	wrong := c.Envelope
	wrong.ConversationID = "other"
	if _, err := s.CancelControlRequest(wrong, c.InboundID, controlRevision(c.Envelope)); err == nil {
		t.Fatal("cross-route cancel accepted")
	}
	if _, err := s.CancelControlRequest(c.Envelope, c.InboundID, "old"); err == nil {
		t.Fatal("wrong revision accepted")
	}
	r, err := s.CancelControlRequest(c.Envelope, c.InboundID, controlRevision(c.Envelope))
	if err != nil || r.State != "cancel_requested" || r.Cancellation == nil || r.Cancellation.Worker != w.Worker {
		t.Fatal(r, err)
	}
	first := r.Cancellation.RequestedAt
	r, err = s.CancelControlRequest(c.Envelope, c.InboundID, controlRevision(c.Envelope))
	if err != nil || r.Cancellation.RequestedAt != first {
		t.Fatal("repeat changed cancellation", r, err)
	}
	if _, err = s.QueueReply(c.InboundID, c.Claim, "late"); err == nil {
		t.Fatal("cancel did not suppress reply")
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = OpenStore(path, ledgerPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	pending, err := s.WorkerCancellations(w.Worker, w.Incarnation, w.Controller)
	if err != nil || len(pending) != 1 {
		t.Fatal(pending, err)
	}
	for _, x := range []struct{ worker, inc, controller, evidence string }{{w.Worker, "wrong", w.Controller, "native:stop"}, {w.Worker, w.Incarnation, "wrong", "native:stop"}, {w.Worker, w.Incarnation, w.Controller, ""}} {
		if _, err = s.AcknowledgeWorkerCancellation(c.InboundID, x.worker, x.inc, x.controller, x.evidence); err == nil {
			t.Fatal("invalid ack accepted", x)
		}
	}
	if _, err = s.ObserveWorker(w.Worker, w.Incarnation, w.Controller, "interrupted", 60, "native:observed-stop"); err != nil {
		t.Fatal(err)
	}
	if _, err = s.RegisterWorker(w.Worker, "turn-2", w.Controller, "running", 60, w.Incarnation, "native:next"); err == nil {
		t.Fatal("recovery discarded pending cancel")
	}
	ack, err := s.AcknowledgeWorkerCancellation(c.InboundID, w.Worker, w.Incarnation, w.Controller, "native:interrupt-result-1")
	if err != nil || ack.State != "acknowledged" || ack.ExecutionState != "interrupted_controller_attested" {
		t.Fatal(ack, err)
	}
	dup, err := s.AcknowledgeWorkerCancellation(c.InboundID, w.Worker, w.Incarnation, w.Controller, "native:interrupt-result-1")
	if err != nil || dup.AcknowledgedAt != ack.AcknowledgedAt {
		t.Fatal(dup, err)
	}
	if _, err = s.AcknowledgeWorkerCancellation(c.InboundID, w.Worker, w.Incarnation, w.Controller, "native:other"); err == nil {
		t.Fatal("conflicting ack accepted")
	}
	requests, err := s.ControlRequests(c.Envelope, c.InboundID)
	if err != nil || requests[0].State != "cancelled" {
		t.Fatal(requests, err)
	}
	if _, err = s.RegisterWorker(w.Worker, "turn-2", w.Controller, "running", 60, w.Incarnation, "native:next"); err != nil {
		t.Fatal(err)
	}
	if _, err = s.AcknowledgeWorkerCancellation(c.InboundID, w.Worker, w.Incarnation, w.Controller, "native:interrupt-result-1"); err == nil {
		t.Fatal("retired incarnation ACK accepted")
	}
}

func TestWorkerUnboundCancellationCanOnlyBindExactSavedClaim(t *testing.T) {
	s, _ := testStore(t)
	ingestLedger(t, s, "one", "channel")
	c := claimLedger(t, s)
	r, err := s.CancelControlRequest(c.Envelope, c.InboundID, controlRevision(c.Envelope))
	if err != nil || r.State != "cancel_requested" || r.Cancellation.ExecutionState != "worker_unbound" {
		t.Fatal(r, err)
	}
	w := registeredWorker(t, s)
	if _, err = s.AcknowledgeWorkerCancellation(c.InboundID, w.Worker, w.Incarnation, w.Controller, "native:stop"); err == nil {
		t.Fatal("unbound ACK accepted")
	}
	if _, err = s.BindWorkerClaim(c.InboundID, "wrong", w.Worker, w.Incarnation, w.Controller, "native:ownership"); err == nil {
		t.Fatal("wrong historical claim accepted")
	}
	if _, err = s.BindWorkerClaim(c.InboundID, c.Claim, w.Worker, w.Incarnation, w.Controller, "native:ownership"); err != nil {
		t.Fatal(err)
	}
	if err = s.Renew(c.InboundID, c.Claim, 60); err == nil {
		t.Fatal("reconciliation resurrected cancelled claim")
	}
	if _, err = s.AcknowledgeWorkerCancellation(c.InboundID, w.Worker, w.Incarnation, w.Controller, "native:stop"); err != nil {
		t.Fatal(err)
	}
}

func TestWorkerIncarnationCannotBindUnrelatedTurnAfterReply(t *testing.T) {
	s, _ := testStore(t)
	ingestLedger(t, s, "one", "channel")
	c := claimLedger(t, s)
	w := boundWorker(t, s, c)
	replyLedger(t, s, c, "early answer")
	ingestLedger(t, s, "two", "other")
	next := claimLedger(t, s)
	if _, err := s.BindWorkerClaim(next.InboundID, next.Claim, w.Worker, w.Incarnation, w.Controller, "native:next"); err == nil {
		t.Fatal("same incarnation rebound after early reply")
	}
	if _, err := s.ObserveWorker(w.Worker, w.Incarnation, w.Controller, "completed", 60, "native:completed-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RegisterWorker(w.Worker, "turn-2", w.Controller, "running", 60, w.Incarnation, "native:started-2"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.BindWorkerClaim(next.InboundID, next.Claim, w.Worker, "turn-2", w.Controller, "native:next"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.BindWorkerClaim(c.InboundID, c.Claim, w.Worker, "turn-2", w.Controller, "native:wrong"); err == nil {
		t.Fatal("old request rebound to new turn")
	}
}

func TestWorkerBoundDeliveredAndUncertainRepliesStillCancelExecution(t *testing.T) {
	for _, outcome := range []string{"sent", "uncertain"} {
		t.Run(outcome, func(t *testing.T) {
			s, _ := testStore(t)
			ingestLedger(t, s, "one", "channel")
			c := claimLedger(t, s)
			w := boundWorker(t, s, c)
			rid := replyLedger(t, s, c, "early answer")
			chunk, err := s.NextChunk()
			if err != nil || chunk == nil {
				t.Fatal(chunk, err)
			}
			if err = s.RecordResult(*chunk, SendResult{State: outcome, MessageID: "remote"}); err != nil {
				t.Fatal(err)
			}
			requests, err := s.activeControlRequests(c.Envelope)
			if err != nil || len(requests) != 1 || requests[0].Worker == nil {
				t.Fatal(requests, err)
			}
			r, err := s.CancelControlRequest(c.Envelope, c.InboundID, controlRevision(c.Envelope))
			if err != nil || r.State != "cancel_requested" || r.DeliveryState != outcome {
				t.Fatal(r, err)
			}
			if _, err = s.AcknowledgeWorkerCancellation(c.InboundID, w.Worker, w.Incarnation, w.Controller, "native:stop"); err != nil {
				t.Fatal(err)
			}
			d, err := s.Delivery(rid)
			if err != nil || d.State != outcome {
				t.Fatal("cancel rewrote delivery evidence", d, err)
			}
		})
	}
}

func TestWorkerRetiredSourceCannotResumeFromRecovery(t *testing.T) {
	s, _ := testStore(t)
	ingestLedger(t, s, "one", "channel")
	c := claimLedger(t, s)
	w := boundWorker(t, s, c)
	if _, err := s.ObserveWorker(w.Worker, w.Incarnation, w.Controller, "interrupted", 60, "native:stop"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DeleteSource("channel", "", "one"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RegisterWorker(w.Worker, "turn-2", w.Controller, "running", 60, w.Incarnation, "native:resume"); err != nil {
		t.Fatal(err)
	}
	next, err := s.ClaimNext(60, 0)
	if err != nil || next != nil {
		t.Fatal("deleted source recovered", next, err)
	}
}

func TestWorkerCompletionRejectsOutstandingClaim(t *testing.T) {
	s, _ := testStore(t)
	ingestLedger(t, s, "one", "channel")
	c := claimLedger(t, s)
	w := boundWorker(t, s, c)
	if _, err := s.ObserveWorker(w.Worker, w.Incarnation, w.Controller, "completed", 60, "native:wrong-complete"); err == nil {
		t.Fatal("unfinished claim marked complete")
	}
	if _, err := s.CancelControlRequest(c.Envelope, c.InboundID, controlRevision(c.Envelope)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ObserveWorker(w.Worker, w.Incarnation, w.Controller, "completed", 60, "native:wrong-complete"); err == nil {
		t.Fatal("cancel request bypassed interruption ACK")
	}
}

func TestWorkerSlashCancelDeliveredEarlyReplyAndRepeatedRequest(t *testing.T) {
	var mu sync.Mutex
	var messages []string
	s, svc, _ := interactionFixture(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/callback") {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
			return
		}
		content, _ := body["content"].(string)
		mu.Lock()
		messages = append(messages, content)
		mu.Unlock()
		json.NewEncoder(w).Encode(map[string]any{"id": "9", "channel_id": "2", "author": User{ID: "4", Bot: true}, "webhook_id": "4", "content": content, "flags": 64})
	})
	if _, err := s.Ingest(testEnvelope()); err != nil {
		t.Fatal(err)
	}
	c, err := s.ClaimNext(60, 0)
	if err != nil || c == nil {
		t.Fatal(c, err)
	}
	worker := boundWorker(t, s, c)
	replyLedger(t, s, c, "early answer")
	chunk, err := s.NextChunk()
	if err != nil || chunk == nil {
		t.Fatal(chunk, err)
	}
	if err = s.RecordResult(*chunk, SendResult{State: "sent", MessageID: "9"}); err != nil {
		t.Fatal(err)
	}
	base := mustUint(interactionID())
	for n, request := range []string{"", c.InboundID} {
		i := ownerInteraction("cancel", request)
		i.ID = fmt.Sprint(base + uint64(n))
		if got := svc.Handle(context.Background(), i, time.Now()); got != "result_sent" {
			t.Fatal("bound delivered request was not cancellable", got)
		}
	}
	r, err := s.ControlRequests(c.Envelope, c.InboundID)
	if err != nil || r[0].State != "cancel_requested" {
		t.Fatal(r, err)
	}
	if _, err = s.AcknowledgeWorkerCancellation(c.InboundID, worker.Worker, worker.Incarnation, worker.Controller, "native:interrupt-1"); err != nil {
		t.Fatal(err)
	}
	i := ownerInteraction("status", c.InboundID)
	i.ID = fmt.Sprint(base + 2)
	if got := svc.Handle(context.Background(), i, time.Now()); got != "result_sent" {
		t.Fatal(got)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(messages) != 3 || !strings.Contains(messages[0], "not yet acknowledged") || !strings.Contains(messages[2], "cancellation=acknowledged (interrupted_controller_attested)") || !strings.Contains(messages[2], "delivery=sent") {
		t.Fatal(messages)
	}
}

func TestWorkerExpiredBoundConversation(t *testing.T) {
	s, _ := testStore(t)
	ingestLedger(t, s, "one", "channel")
	c := claimLedger(t, s)
	boundWorker(t, s, c)
	workerSQL(t, s, `UPDATE inbound SET lease_until=? WHERE id=?`, epoch()-1, c.InboundID)
	workerSQL(t, s, `UPDATE worker_runtime SET lease_until=?`, epoch()-1)
	ingestLedger(t, s, "two", "channel")
	next, err := s.ClaimNext(60, 0)
	if err != nil {
		t.Fatal(err)
	}
	if next != nil {
		t.Fatalf("later same-conversation claim admitted while original bound native turn still running: old=%s next=%s", c.InboundID, next.InboundID)
	}
	ingestLedger(t, s, "other", "other-channel")
	if next = claimLedger(t, s); next.Envelope.ConversationID != "other-channel" {
		t.Fatal("execution fence blocked an unrelated conversation", next)
	}
}
func TestWorkerUnchangedSourceRefreshBound(t *testing.T) {
	s, cfg, m, c := sourceFixture(t)
	w := boundWorker(t, s, c)
	if _, err := s.InvalidateSourceUpdate(m.ChannelID, m.GuildID, m.ID); err != nil {
		t.Fatal(err)
	}
	in, err := s.SourceRefreshFor(projectGatewayMessage(cfg, m))
	if err != nil || in == nil {
		t.Fatal(in, err)
	}
	result, err := s.ApplySourceRefresh(*in, projectGatewayMessage(cfg, m))
	if err != nil || result != "unchanged" {
		t.Fatal(result, err)
	}
	next, err := s.ClaimNext(60, 0)
	if err != nil {
		t.Fatal(err)
	}
	if next != nil {
		t.Fatalf("same inbound reclaimed while original bound native turn still running: old=%s/%s next=%s/%s", c.InboundID, c.Claim, next.InboundID, next.Claim)
	}
	if _, err = s.ObserveWorker(w.Worker, w.Incarnation, w.Controller, "completed", 60, "native:completed"); err != nil {
		t.Fatal(err)
	}
	next, err = s.ClaimNext(60, 0)
	if err != nil || next == nil || next.InboundID != c.InboundID || next.Claim == c.Claim {
		t.Fatal("verified completion did not release refreshed work", next, err)
	}
}
func TestWorkerCancelAfterUnchangedSourceRefresh(t *testing.T) {
	s, cfg, m, c := sourceFixture(t)
	w := boundWorker(t, s, c)
	if _, err := s.InvalidateSourceUpdate(m.ChannelID, m.GuildID, m.ID); err != nil {
		t.Fatal(err)
	}
	in, err := s.SourceRefreshFor(projectGatewayMessage(cfg, m))
	if err != nil || in == nil {
		t.Fatal(in, err)
	}
	if _, err = s.ApplySourceRefresh(*in, projectGatewayMessage(cfg, m)); err != nil {
		t.Fatal(err)
	}
	r, err := s.CancelControlRequest(c.Envelope, c.InboundID, controlRevision(c.Envelope))
	if err != nil {
		t.Fatal(err)
	}
	if r.Cancellation == nil || r.Cancellation.State != "cancel_requested" || r.Cancellation.Worker != w.Worker || r.Cancellation.Claim != c.Claim {
		t.Fatalf("still-running bound turn lost from cancellation: %+v", r.Cancellation)
	}
	if r.Worker == nil || r.Worker.Incarnation != w.Incarnation {
		t.Fatal("status lost execution generation after claim revocation", r)
	}
	later := *m
	later.ID = "888888888888888888"
	if outcome, err := s.Ingest(projectGatewayMessage(cfg, &later)); err != nil || outcome != "accepted" {
		t.Fatal(outcome, err)
	}
	if next, err := s.ClaimNext(60, 0); err != nil || next != nil {
		t.Fatal("cancellation released conversation before interrupt", next, err)
	}
	if _, err = s.AcknowledgeWorkerCancellation(c.InboundID, w.Worker, w.Incarnation, w.Controller, "native:interrupt"); err != nil {
		t.Fatal(err)
	}
	if next, err := s.ClaimNext(60, 0); err != nil || next == nil || next.InboundID == c.InboundID {
		t.Fatal("interrupt acknowledgement did not release later work", next, err)
	}
}

func TestWorkerConversationReleasedAfterCompletion(t *testing.T) {
	s, _ := testStore(t)
	ingestLedger(t, s, "one", "channel")
	c := claimLedger(t, s)
	w := boundWorker(t, s, c)
	replyLedger(t, s, c, "early answer")
	ingestLedger(t, s, "two", "channel")
	if next, err := s.ClaimNext(60, 0); err != nil || next != nil {
		t.Fatal("early reply released still-running execution", next, err)
	}
	if _, err := s.ObserveWorker(w.Worker, w.Incarnation, w.Controller, "completed", 60, "native:completed"); err != nil {
		t.Fatal(err)
	}
	workerSQL(t, s, `UPDATE worker_runtime SET lease_until=?`, epoch()-1)
	if next, err := s.ClaimNext(60, 0); err != nil || next == nil || next.InboundID == c.InboundID {
		t.Fatal("completed execution kept conversation fenced", next, err)
	}
}
