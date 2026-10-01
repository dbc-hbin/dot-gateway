package bridge

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
)

func TestIntegratedRichReplyEditRevokesBeforeFileStaging(t *testing.T) {
	s, cfg, m, claim := sourceFixture(t)
	dir, e := s.ReplyOutputDir()
	if e != nil {
		t.Fatal(e)
	}
	os.WriteFile(filepath.Join(dir, "answer.txt"), []byte("original bytes"), 0600)
	raw := []byte(`{"version":1,"attachments":[{"path":"answer.txt"}]}`)
	id, e := s.QueueReplyManifest(claim.InboundID, claim.Claim, raw)
	if e != nil {
		t.Fatal(e)
	}
	applyEdit(t, s, cfg, m, "edited question")
	if _, e = s.QueueReplyManifest(claim.InboundID, claim.Claim, raw); !errors.Is(e, ErrClaim) {
		t.Fatal("old rich idempotence bypass", e)
	}
	d, e := s.Delivery(id)
	if e != nil || d.State != "cancelled" {
		t.Fatal(d, e)
	}
	if chunk, e := s.NextChunk(); e != nil || chunk != nil {
		t.Fatal("old rich output sent", chunk, e)
	}
	latest, e := s.ClaimNext(60, 0)
	if e != nil || latest == nil || latest.Envelope.EventID != claim.Envelope.EventID || latest.Envelope.SourceRevision <= claim.Envelope.SourceRevision {
		t.Fatal(latest, e)
	}
	if _, e = s.QueueReplyManifest(latest.InboundID, latest.Claim, raw); e != nil {
		t.Fatal(e)
	}
	chunk, e := s.NextChunk()
	if e != nil || chunk == nil || chunk.Source.SourceRevision != latest.Envelope.SourceRevision || chunk.Output == "" {
		t.Fatal(chunk, e)
	}
}
func TestIntegratedDeletedRichUncertaintyReconcilesThenDrains(t *testing.T) {
	s, cfg, m, claim := sourceFixture(t)
	dir, e := s.ReplyOutputDir()
	if e != nil {
		t.Fatal(e)
	}
	os.WriteFile(filepath.Join(dir, "answer.txt"), []byte("original bytes"), 0600)
	raw, _ := json.Marshal(ReplyManifest{Version: 1, Text: strings.Repeat("a", 2000), Attachments: []ReplyFile{{Path: "answer.txt"}}})
	first, e := s.QueueReplyManifest(claim.InboundID, claim.Claim, raw)
	if e != nil {
		t.Fatal(e)
	}
	chunk, e := s.NextChunk()
	if e != nil || chunk == nil {
		t.Fatal(e)
	}
	if changed, e := s.DeleteSource(m.ChannelID, m.GuildID, m.ID); e != nil || !changed {
		t.Fatal(changed, e)
	}
	if e = s.RecordResult(*chunk, SendResult{State: "uncertain", Code: "lost_ack"}); e != nil {
		t.Fatal(e)
	}
	newer := *m
	newer.ID = "8"
	if _, e = s.Ingest(projectGatewayMessage(cfg, &newer)); e != nil {
		t.Fatal(e)
	}
	nextClaim, e := s.ClaimNext(60, 0)
	if e != nil || nextClaim == nil {
		t.Fatal(e)
	}
	second, e := s.QueueReply(nextClaim.InboundID, nextClaim.Claim, "next answer")
	if e != nil {
		t.Fatal(e)
	}
	if next, e := s.NextChunk(); e != nil || next != nil {
		t.Fatal("uncertainty bypass", next, e)
	}
	var writes atomic.Int32
	r := localREST(t, func(w http.ResponseWriter, req *http.Request) {
		if req.Method != "GET" {
			writes.Add(1)
		}
		if preflightHandler(w, req) {
			return
		}
		json.NewEncoder(w).Encode(outputACK(*chunk))
	})
	// An old already-posted message can be verified after source deletion. This
	// is not authorization to send anything new to the deleted source.
	if result, e := r.ReconcileReply(context.Background(), s, first, 0, "9"); e != nil || result.State != "sent" || writes.Load() != 0 {
		t.Fatal(result, e, writes.Load())
	}
	d, e := s.Delivery(first)
	if e != nil || d.State != "cancelled" || d.Chunks[0].State != "sent" || d.Chunks[0].OutputReceipt == "" || d.Chunks[1].State != "cancelled" {
		t.Fatal(d, e)
	}
	if next, e := s.NextChunk(); e != nil || next == nil || next.ReplyID != second {
		t.Fatal("safe queue did not drain", next, e)
	}
}
func TestIntegratedInteractionAttachmentOnlyRoundTrip(t *testing.T) {
	var chunk *Chunk
	var uploads atomic.Int32
	s, svc, _ := interactionFixture(t, func(w http.ResponseWriter, req *http.Request) {
		if strings.Contains(req.URL.Path, "/callback") {
			w.WriteHeader(204)
			return
		}
		uploads.Add(1)
		if req.Method != http.MethodPatch || chunk == nil {
			t.Error("unexpected upload")
			w.WriteHeader(400)
			return
		}
		typ, params, e := mime.ParseMediaType(req.Header.Get("Content-Type"))
		if e != nil || typ != "multipart/form-data" {
			t.Error("not multipart")
			w.WriteHeader(400)
			return
		}
		reader := multipart.NewReader(req.Body, params["boundary"])
		part, e := reader.NextPart()
		if e != nil {
			t.Error(e)
			return
		}
		var body map[string]any
		json.NewDecoder(part).Decode(&body)
		if body["flags"] != float64(64) || body["content"] != "" {
			t.Error("incorrect rich ephemeral payload", body)
		}
		for _, key := range []string{"message_reference", "nonce", "enforce_nonce"} {
			if _, ok := body[key]; ok {
				t.Error("message-only field", key)
			}
		}
		part, e = reader.NextPart()
		if e != nil {
			t.Error(e)
			return
		}
		b, _ := io.ReadAll(part)
		if string(b) != "model-authored report" {
			t.Error("output lost")
		}
		ack := outputACK(*chunk)
		delete(ack, "message_reference")
		delete(ack, "nonce")
		ack["webhook_id"] = "4"
		ack["flags"] = 64
		json.NewEncoder(w).Encode(ack)
	})
	svc.transport.outputStateDir = s.replyStateDir
	if out := svc.Handle(context.Background(), ownerInteraction("ask", "send a report"), time.Now()); out != "queued" {
		t.Fatal(out)
	}
	claim, e := s.ClaimNext(60, 0)
	if e != nil || claim == nil {
		t.Fatal(e)
	}
	dir, e := s.ReplyOutputDir()
	if e != nil {
		t.Fatal(e)
	}
	os.WriteFile(filepath.Join(dir, "report.txt"), []byte("model-authored report"), 0600)
	id, e := s.QueueReplyManifest(claim.InboundID, claim.Claim, []byte(`{"version":1,"attachments":[{"path":"report.txt"}],"embeds":[{"title":"Report"}]}`))
	if e != nil {
		t.Fatal(e)
	}
	chunk, e = s.NextChunk()
	if e != nil || chunk == nil {
		t.Fatal(e)
	}
	if ordinary := svc.rest.Send(context.Background(), *chunk); ordinary.State != "failed" || uploads.Load() != 0 {
		t.Fatal("public fallback", ordinary)
	}
	result, _ := svc.Send(context.Background(), *chunk, func() bool { return true })
	if result.State != "sent" || result.OutputReceipt == "" || uploads.Load() != 1 {
		t.Fatal(result, uploads.Load())
	}
	if e = s.RecordResult(*chunk, result); e != nil {
		t.Fatal(e)
	}
	d, e := s.Delivery(id)
	if e != nil || d.State != "sent" || d.Chunks[0].OutputReceipt == "" {
		t.Fatal(d, e)
	}
}
func TestIntegratedRichReactionReceiptAndSourceCascade(t *testing.T) {
	var ack map[string]any
	s, svc, _ := interactionFixture(t, func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path == "/channels/2/messages/9" {
			json.NewEncoder(w).Encode(ack)
			return
		}
		w.WriteHeader(404)
	})
	if _, e := s.Ingest(testEnvelope()); e != nil {
		t.Fatal(e)
	}
	claim, e := s.ClaimNext(60, 0)
	if e != nil || claim == nil {
		t.Fatal(e)
	}
	dir, e := s.ReplyOutputDir()
	if e != nil {
		t.Fatal(e)
	}
	os.WriteFile(filepath.Join(dir, "proposal.txt"), []byte("proposal"), 0600)
	id, c := queueOutput(t, s, claim, ReplyManifest{Version: 1, Text: "Should I use this proposal?", Attachments: []ReplyFile{{Path: "proposal.txt"}}})
	ack = outputACK(*c)
	b, _ := json.Marshal(ack)
	var raw map[string]json.RawMessage
	json.Unmarshal(b, &raw)
	receipt, e := ReplyOutputAckReceipt(raw, *c)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.RecordResult(*c, SendResult{State: "sent", MessageID: "9", OutputReceipt: receipt}); e != nil {
		t.Fatal(e)
	}
	if _, e = s.BindPendingResponse(claim.InboundID, claim.Claim, "9"); e != nil {
		t.Fatal(e)
	}
	reaction := &discordgo.MessageReaction{UserID: "1", MessageID: "9", ChannelID: "2", Emoji: discordgo.Emoji{Name: "👍"}}
	if out := svc.HandleReaction(context.Background(), reaction, true); out != "accepted" {
		t.Fatal(out, id)
	}
	next, e := s.ClaimNext(60, 0)
	if e != nil || next == nil {
		t.Fatal(e)
	}
	control, e := next.Envelope.ControlEvent()
	if e != nil || control.ApprovalGranted || len(control.TargetOutput) == 0 || len(control.TargetReceipt) == 0 || control.PendingResponseID == "" {
		t.Fatal(control, e)
	}
	if changed, e := s.DeleteSource("2", "", "3"); e != nil || !changed {
		t.Fatal(changed, e)
	}
	if _, e = s.QueueReplyManifest(next.InboundID, next.Claim, []byte(`{"version":1,"text":"reaction answer","embeds":[{"title":"Reply"}]}`)); !errors.Is(e, ErrClaim) {
		t.Fatal("source-deleted reaction queued rich answer", e)
	}
}
