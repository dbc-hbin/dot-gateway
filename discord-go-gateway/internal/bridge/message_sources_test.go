package bridge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
)

func sourceFixture(t *testing.T) (*Store, Settings, *discordgo.Message, *Claim) {
	s, cfg, m := ingressFixture(t)
	m.Content = "original question"
	e := projectGatewayMessage(cfg, m)
	if outcome, err := s.Ingest(e); err != nil || outcome != "accepted" {
		t.Fatal(outcome, err)
	}
	c, err := s.ClaimNextForConsumer(300, 60, "source-test")
	if err != nil || c == nil {
		t.Fatal(c, err)
	}
	return s, cfg, m, c
}
func applyEdit(t *testing.T, s *Store, cfg Settings, m *discordgo.Message, text string) Envelope {
	t.Helper()
	if changed, err := s.InvalidateSourceUpdate(m.ChannelID, m.GuildID, m.ID); err != nil || !changed {
		t.Fatal(changed, err)
	}
	in, err := s.SourceRefreshFor(projectGatewayMessage(cfg, m))
	if err != nil || in == nil {
		t.Fatal(in, err)
	}
	edited := *m
	edited.Content = text
	if outcome, err := s.ApplySourceRefresh(*in, projectGatewayMessage(cfg, &edited)); err != nil || outcome != "revised" {
		t.Fatal(outcome, err)
	}
	validation, err := s.NextValidation(epoch())
	if err != nil || validation == nil {
		t.Fatal(validation, err)
	}
	if outcome, err := s.PromoteValidation(*validation); err != nil || outcome != "accepted" {
		t.Fatal(outcome, err)
	}
	return validation.Event
}
func TestSourceEditRevokesClaimAndPreservesOldReplySnapshot(t *testing.T) {
	s, cfg, m, old := sourceFixture(t)
	first, err := s.QueueReply(old.InboundID, old.Claim, "old answer")
	if err != nil {
		t.Fatal(err)
	}
	latest := applyEdit(t, s, cfg, m, "updated question")
	if latest.SourceRevision != 1 || latest.EventID != m.ID {
		t.Fatal(latest)
	}
	d, err := s.Delivery(first)
	if err != nil || d.State != "cancelled" {
		t.Fatal(d, err)
	}
	if _, err = s.QueueReply(old.InboundID, old.Claim, "old answer"); !errors.Is(err, ErrClaim) {
		t.Fatalf("old idempotence bypass: %v", err)
	}
	if outcome, err := receiveMessage(context.Background(), nil, s, cfg, m); err != nil || outcome != "duplicate" {
		t.Fatal(outcome, err)
	}
	c, err := s.ClaimNextForConsumer(60, 0, "source-test")
	if err != nil || c == nil || c.InboundID == old.InboundID || c.Envelope.Text != "updated question" {
		t.Fatal(c, err)
	}
	var raw string
	s.call(func(db *storeConn) (any, error) {
		return nil, db.QueryRow(`SELECT envelope FROM inbound WHERE id=?`, old.InboundID).Scan(&raw)
	})
	var saved Envelope
	json.Unmarshal([]byte(raw), &saved)
	if saved.Text != m.Content || saved.SourceRevision != 0 {
		t.Fatal(saved)
	}
	rid, err := s.QueueReply(c.InboundID, c.Claim, "updated answer")
	if err != nil {
		t.Fatal(err)
	}
	chunk, err := s.NextChunk()
	if err != nil || chunk == nil || chunk.ReplyID != rid || chunk.Source.EventID != m.ID || chunk.Source.SourceRevision != 1 {
		t.Fatal(chunk, err)
	}
}
func TestSourcePartialUpdateRefreshesExactKnownOwnerMessage(t *testing.T) {
	s, cfg, m, claim := sourceFixture(t)
	outcome, err := receiveSourceEvent(context.Background(), nil, s, cfg, sourceGatewayEvent{Update: &discordgo.Message{ID: m.ID, ChannelID: m.ChannelID}})
	if err != nil || outcome != "source_update" {
		t.Fatal(outcome, err)
	}
	if _, err = s.QueueReply(claim.InboundID, claim.Claim, "stale"); !errors.Is(err, ErrClaim) {
		t.Fatal(err)
	}
	var reads atomic.Int32
	r := localREST(t, func(w http.ResponseWriter, q *http.Request) {
		if preflightHandler(w, q) {
			return
		}
		if q.Method != "GET" || q.URL.Path != "/channels/2/messages/3" {
			t.Errorf("scope %s %s", q.Method, q.URL.Path)
		}
		reads.Add(1)
		edited := *m
		edited.Content = "fresh exact content"
		json.NewEncoder(w).Encode(edited)
	})
	in, _ := s.NextSourceRefresh(epoch())
	if err = refreshMessageSource(context.Background(), s, r, *in); err != nil {
		t.Fatal(err)
	}
	v, _ := s.NextValidation(epoch())
	if v == nil || v.Event.Text != "fresh exact content" || reads.Load() != 1 {
		t.Fatal(v, reads.Load())
	}
}
func TestSourceDeleteTombstoneRestartAndAutomaticQueueDrain(t *testing.T) {
	dir := t.TempDir()
	os.Chmod(dir, 0700)
	path := dir + "/source.db"
	s, err := OpenStore(path, testPolicy())
	if err != nil {
		t.Fatal(err)
	}
	e := testEnvelope()
	e.ContentHash = strings.Repeat("a", 64)
	s.Ingest(e)
	c, _ := s.ClaimNext(60, 0)
	first, _ := s.QueueReply(c.InboundID, c.Claim, "obsolete")
	if changed, err := s.DeleteSource(e.ConversationID, e.GuildID, e.EventID); err != nil || !changed {
		t.Fatal(changed, err)
	}
	d, _ := s.Delivery(first)
	if d.State != "cancelled" {
		t.Fatal(d)
	}
	s.Close()
	s, err = OpenStore(path, testPolicy())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if outcome, err := s.StageIngress(e); err != nil || outcome != "duplicate" {
		t.Fatal(outcome, err)
	}
	if current, err := s.SourceCurrent(e); err != nil || current {
		t.Fatal(current, err)
	}
	newer := e
	newer.EventID = "8"
	newer.Text = "another question"
	s.Ingest(newer)
	c, _ = s.ClaimNext(60, 0)
	if c == nil {
		t.Fatal("new question not claimable")
	}
	second, _ := s.QueueReply(c.InboundID, c.Claim, "new answer")
	chunk, _ := s.NextChunk()
	if chunk == nil || chunk.ReplyID != second {
		t.Fatal(chunk)
	}
}
func TestSourceDeletePreservesAmbiguousPOSTUntilResolved(t *testing.T) {
	s, cfg, m, c := sourceFixture(t)
	first, _ := s.QueueReply(c.InboundID, c.Claim, strings.Repeat("a", 2000))
	chunk, _ := s.NextChunk()
	s.DeleteSource(m.ChannelID, m.GuildID, m.ID)
	d, _ := s.Delivery(first)
	if d.State != "sending" {
		t.Fatal(d)
	}
	if err := s.RecordResult(*chunk, SendResult{State: "uncertain", Code: "request_or_ack_failed"}); err != nil {
		t.Fatal(err)
	}
	newer := *m
	newer.ID = "9"
	s.Ingest(projectGatewayMessage(cfg, &newer))
	c2, _ := s.ClaimNext(60, 0)
	second, _ := s.QueueReply(c2.InboundID, c2.Claim, "next answer")
	if next, _ := s.NextChunk(); next != nil {
		t.Fatal("ambiguous post released later reply")
	}
	if err := s.ResolveSent(first, 0, "99"); err != nil {
		t.Fatal(err)
	}
	d, _ = s.Delivery(first)
	if d.State != "cancelled" || d.Chunks[0].State != "sent" || d.Chunks[1].State != "cancelled" {
		t.Fatal(d)
	}
	next, _ := s.NextChunk()
	if next == nil || next.ReplyID != second {
		t.Fatal(next)
	}
}
func TestSourceDeleteDuringSendingKnownUnsentResultCancels(t *testing.T) {
	s, _, m, c := sourceFixture(t)
	rid, _ := s.QueueReply(c.InboundID, c.Claim, strings.Repeat("a", 2000))
	chunk, _ := s.NextChunk()
	s.DeleteSource(m.ChannelID, m.GuildID, m.ID)
	if err := s.RecordResult(*chunk, SendResult{State: "failed", Code: "source_message_deleted"}); err != nil {
		t.Fatal(err)
	}
	d, _ := s.Delivery(rid)
	if d.State != "cancelled" || d.Chunks[0].State != "failed" || d.Chunks[1].State != "cancelled" {
		t.Fatal(d)
	}
}
func TestSourceRefreshGenerationCASAndDeleteWin(t *testing.T) {
	s, cfg, m, _ := sourceFixture(t)
	s.InvalidateSourceUpdate(m.ChannelID, m.GuildID, m.ID)
	old, _ := s.NextSourceRefresh(epoch())
	s.InvalidateSourceUpdate(m.ChannelID, m.GuildID, m.ID)
	latest, _ := s.NextSourceRefresh(epoch())
	changed := *m
	changed.Content = "stale fetched text"
	if outcome, err := s.ApplySourceRefresh(*old, projectGatewayMessage(cfg, &changed)); err != nil || outcome != "superseded" {
		t.Fatal(outcome, err)
	}
	s.DeleteSource(m.ChannelID, m.GuildID, m.ID)
	if outcome, err := s.ApplySourceRefresh(*latest, projectGatewayMessage(cfg, &changed)); err != nil || outcome != "superseded" {
		t.Fatal(outcome, err)
	}
	if c, _ := s.ClaimNext(60, 0); c != nil {
		t.Fatal(c)
	}
}
func TestSourceIdenticalRefreshDoesNotDuplicateCompletedAnswer(t *testing.T) {
	s, cfg, m, c := sourceFixture(t)
	rid, _ := s.QueueReply(c.InboundID, c.Claim, "answer")
	chunk, _ := s.NextChunk()
	s.RecordResult(*chunk, SendResult{State: "sent", MessageID: "77"})
	s.InvalidateSourceUpdate(m.ChannelID, m.GuildID, m.ID)
	in, _ := s.NextSourceRefresh(epoch())
	if outcome, err := s.ApplySourceRefresh(*in, projectGatewayMessage(cfg, m)); err != nil || outcome != "unchanged" {
		t.Fatal(outcome, err)
	}
	if c, _ := s.ClaimNext(60, 0); c != nil {
		t.Fatal("duplicate author task")
	}
	d, _ := s.Delivery(rid)
	if d.State != "sent" {
		t.Fatal(d)
	}
}
func TestSourceGatewayFIFOAndQuarantineDeletion(t *testing.T) {
	s, cfg, m := ingressFixture(t)
	for _, event := range []sourceGatewayEvent{{Create: m}, {Delete: &discordgo.Message{ID: m.ID, ChannelID: m.ChannelID}}} {
		if _, err := receiveSourceEvent(context.Background(), nil, s, cfg, event); err != nil {
			t.Fatal(err)
		}
	}
	if v, _ := s.NextValidation(epoch()); v != nil {
		t.Fatal(v)
	}
	if c, _ := s.ClaimNext(60, 0); c != nil {
		t.Fatal(c)
	}
	if changed, err := s.DeleteSource("999", "", m.ID); err != nil || changed {
		t.Fatal("cross-route delete", changed, err)
	}
}
func TestSourceFinalGuardAfterRateWaitPreventsPOST(t *testing.T) {
	s, _, _, c := sourceFixture(t)
	var posts atomic.Int32
	r := localREST(t, func(w http.ResponseWriter, q *http.Request) { posts.Add(1); w.WriteHeader(204) })
	r.limitMu.Lock()
	r.globalUntil = time.Now().Add(100 * time.Millisecond)
	r.limitMu.Unlock()
	ctx := context.WithValue(context.Background(), sendGuardContextKey{}, func() bool { current, err := s.SourceCurrent(c.Envelope); return err == nil && current })
	done := make(chan error, 1)
	go func() { _, err := r.request(ctx, http.MethodPost, "/channels/2/messages", []byte(`{}`)); done <- err }()
	time.Sleep(10 * time.Millisecond)
	s.DeleteSource(c.Envelope.ConversationID, c.Envelope.GuildID, c.Envelope.EventID)
	if err := <-done; !errors.Is(err, errSendGuardChanged) {
		t.Fatal(err)
	}
	if posts.Load() != 0 {
		t.Fatal(posts.Load())
	}
}
func TestSourceFresh404IsDeletionBut403IsNot(t *testing.T) {
	for _, status := range []int{404, 403} {
		t.Run(fmtStatus(status), func(t *testing.T) {
			s, _, _, c := sourceFixture(t)
			r := localREST(t, func(w http.ResponseWriter, q *http.Request) {
				if preflightHandler(w, q) {
					return
				}
				w.WriteHeader(status)
			})
			err := r.VerifySourceBeforeSend(context.Background(), s, c.Envelope)
			if err == nil {
				t.Fatal("expected source failure")
			}
			current, _ := s.SourceCurrent(c.Envelope)
			if current != (status == 403) {
				t.Fatal(status, current)
			}
		})
	}
}
func fmtStatus(n int) string {
	if n == 404 {
		return "deleted"
	}
	return "permission"
}

func TestSourceMigrationBackfillsLegacyOnceWithoutHistoryRescan(t *testing.T) {
	dir := t.TempDir()
	os.Chmod(dir, 0700)
	path := dir + "/legacy.db"
	s, err := OpenStore(path, testPolicy())
	if err != nil {
		t.Fatal(err)
	}
	e := testEnvelope()
	s.Ingest(e)
	_, err = s.call(func(db *storeConn) (any, error) {
		return transact(db, func(db *storeConn) (any, error) {
			if _, err := db.Exec(`DELETE FROM message_sources`); err != nil {
				return nil, err
			}
			if _, err := db.Exec(`DELETE FROM runtime WHERE key='message_sources_migration_v1'`); err != nil {
				return nil, err
			}
			for n := 0; n < 2000; n++ {
				legacy := e
				legacy.EventID = fmt.Sprint(100 + n)
				raw, _ := json.Marshal(legacy)
				if _, err := db.Exec(`INSERT INTO inbound(id,platform,event_id,envelope,state,created) VALUES(?,?,?,?,?,?)`, fmt.Sprint("legacy-", n), legacy.Platform, legacy.EventID, string(raw), "ignored", epoch()); err != nil {
					return nil, err
				}
			}
			return nil, nil
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
	s, err = OpenStore(path, testPolicy())
	if err != nil {
		t.Fatal(err)
	}
	var sources, markers int
	s.call(func(db *storeConn) (any, error) {
		db.QueryRow(`SELECT count(*) FROM message_sources`).Scan(&sources)
		return nil, db.QueryRow(`SELECT count(*) FROM runtime WHERE key='message_sources_migration_v1'`).Scan(&markers)
	})
	if sources != 2001 || markers != 1 {
		t.Fatal(sources, markers)
	}
	// A historical row that cannot be decoded proves the subsequent opener does
	// not scan/unmarshal history after the durable migration marker exists.
	s.call(func(db *storeConn) (any, error) {
		_, err := db.Exec(`INSERT INTO inbound(id,platform,event_id,envelope,state,created) VALUES('poison','discord','999999','invalid-json','ignored',0)`)
		return nil, err
	})
	s.Close()
	s, err = OpenStore(path, testPolicy())
	if err != nil {
		t.Fatalf("history rescanned: %v", err)
	}
	defer s.Close()
	current, err := s.SourceCurrent(e)
	if err != nil || !current {
		t.Fatal(current, err)
	}
}

func TestSourceMigrationRollbackDoesNotPublishMarker(t *testing.T) {
	s, _, _, _ := sourceFixture(t)
	_, err := s.call(func(db *storeConn) (any, error) {
		return transact(db, func(db *storeConn) (any, error) {
			db.Exec(`DELETE FROM runtime WHERE key='message_sources_migration_v1'`)
			db.Exec(`DELETE FROM message_sources`)
			db.Exec(`INSERT INTO inbound(id,platform,event_id,envelope,state,created) VALUES('bad','discord','9999','broken','ignored',0)`)
			return nil, nil
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.call(func(db *storeConn) (any, error) {
		return transact(db, func(db *storeConn) (any, error) { return nil, initMessageSources(db) })
	})
	if err == nil {
		t.Fatal("corrupt legacy envelope accepted")
	}
	var n int
	s.call(func(db *storeConn) (any, error) {
		return nil, db.QueryRow(`SELECT count(*) FROM runtime WHERE key='message_sources_migration_v1'`).Scan(&n)
	})
	if n != 0 {
		t.Fatal("partial migration published marker")
	}
}

func TestSourceDecorativeUpdateAndSignedURLDoNotReissueTask(t *testing.T) {
	s, cfg, m := ingressFixture(t)
	m.Content = "see https://example.com"
	m.Attachments = []*discordgo.MessageAttachment{{ID: "77", Filename: "a.png", ContentType: "image/png", Size: 12, URL: "https://cdn.discordapp.com/attachments/2/77/a.png?ex=first"}}
	s.Ingest(projectGatewayMessage(cfg, m))
	c, _ := s.ClaimNext(60, 0)
	rid, _ := s.QueueReply(c.InboundID, c.Claim, "answer")
	chunk, _ := s.NextChunk()
	s.RecordResult(*chunk, SendResult{State: "sent", MessageID: "88"})
	updated := *m
	attachment := *m.Attachments[0]
	attachment.URL = "https://cdn.discordapp.com/attachments/2/77/a.png?ex=second&hm=new"
	updated.Attachments = []*discordgo.MessageAttachment{&attachment}
	updated.Embeds = []*discordgo.MessageEmbed{{Type: discordgo.EmbedTypeLink, Title: "automatic preview", Description: "decoration"}}
	now := time.Now()
	updated.EditedTimestamp = &now
	if MessageContentFingerprint(m) != MessageContentFingerprint(&updated) {
		t.Fatal("nonsemantic update changes fingerprint")
	}
	s.InvalidateSourceUpdate(m.ChannelID, m.GuildID, m.ID)
	in, _ := s.NextSourceRefresh(epoch())
	if outcome, err := s.ApplySourceRefresh(*in, projectGatewayMessage(cfg, &updated)); err != nil || outcome != "unchanged" {
		t.Fatal(outcome, err)
	}
	if claim, _ := s.ClaimNext(60, 0); claim != nil {
		t.Fatal("decorative update created new owner instruction")
	}
	d, _ := s.Delivery(rid)
	if d.State != "sent" {
		t.Fatal(d)
	}
}

func TestSourceObservedGETCannotOverwriteQueuedNewerUpdate(t *testing.T) {
	s, _, m, c := sourceFixture(t)
	s.InvalidateSourceUpdate(m.ChannelID, m.GuildID, m.ID)
	observed, err := s.InvalidateObservedSource(c.Envelope)
	if err != nil || observed != nil {
		t.Fatal("stale send read stole newer update", observed, err)
	}
}

func TestSourceLegacyTextDecorationDoesNotReplay(t *testing.T) {
	s, cfg, m := ingressFixture(t)
	e := testEnvelope()
	e.Text = m.Content
	s.Ingest(e)
	c, _ := s.ClaimNext(60, 0)
	rid, _ := s.QueueReply(c.InboundID, c.Claim, "legacy answer")
	chunk, _ := s.NextChunk()
	s.RecordResult(*chunk, SendResult{State: "sent", MessageID: "77"})
	m.Embeds = []*discordgo.MessageEmbed{{Type: discordgo.EmbedTypeLink, Title: "late preview"}}
	s.InvalidateSourceUpdate(m.ChannelID, m.GuildID, m.ID)
	in, _ := s.NextSourceRefresh(epoch())
	if out, err := s.ApplySourceRefresh(*in, projectGatewayMessage(cfg, m)); err != nil || out != "unchanged" {
		t.Fatal(out, err)
	}
	if c, _ := s.ClaimNext(60, 0); c != nil {
		t.Fatal(c)
	}
	d, _ := s.Delivery(rid)
	if d.State != "sent" {
		t.Fatal(d)
	}
}
func TestSourceIdenticalRefreshRequeuesOnlyKnownUnattemptedFailure(t *testing.T) {
	s, cfg, m, c := sourceFixture(t)
	rid, _ := s.QueueReply(c.InboundID, c.Claim, "answer")
	chunk, _ := s.NextChunk()
	s.InvalidateSourceUpdate(m.ChannelID, m.GuildID, m.ID)
	s.RecordResult(*chunk, SendResult{State: "failed", Code: "source_not_current"})
	in, _ := s.NextSourceRefresh(epoch())
	s.ApplySourceRefresh(*in, projectGatewayMessage(cfg, m))
	next, _ := s.NextChunk()
	if next == nil || next.ReplyID != rid {
		t.Fatal(next)
	}
	s.RecordResult(*next, SendResult{State: "uncertain", Code: "request_or_ack_failed"})
	s.InvalidateSourceUpdate(m.ChannelID, m.GuildID, m.ID)
	in, _ = s.NextSourceRefresh(epoch())
	s.ApplySourceRefresh(*in, projectGatewayMessage(cfg, m))
	if next, _ := s.NextChunk(); next != nil {
		t.Fatal("uncertain retry", next)
	}
}
func TestSourceConcurrentOpenersPublishSingleCompleteMigration(t *testing.T) {
	dir := t.TempDir()
	os.Chmod(dir, 0700)
	path := dir + "/concurrent.db"
	s, err := OpenStore(path, testPolicy())
	if err != nil {
		t.Fatal(err)
	}
	s.Ingest(testEnvelope())
	s.call(func(db *storeConn) (any, error) {
		db.Exec(`DELETE FROM message_sources`)
		_, err := db.Exec(`DELETE FROM runtime WHERE key='message_sources_migration_v1'`)
		return nil, err
	})
	s.Close()
	results := make(chan *Store, 2)
	errs := make(chan error, 2)
	for n := 0; n < 2; n++ {
		go func() {
			store, err := OpenStore(path, testPolicy())
			if err != nil {
				errs <- err
				return
			}
			results <- store
		}()
	}
	for n := 0; n < 2; n++ {
		select {
		case err := <-errs:
			t.Fatal(err)
		case store := <-results:
			current, err := store.SourceCurrent(testEnvelope())
			if err != nil || !current {
				t.Fatal(current, err)
			}
			store.Close()
		case <-time.After(5 * time.Second):
			t.Fatal("concurrent migration stalled")
		}
	}
}
func TestSourceMaterializationOwnershipRejectsRefreshingHead(t *testing.T) {
	s, _, m, c := sourceFixture(t)
	if _, err := s.ClaimedEnvelope(c.InboundID, c.Claim); err != nil {
		t.Fatal(err)
	}
	// Alter only the head to prove source-current gating even without the usual
	// invalidation's claim revocation. This is a temporary fixture, not production.
	s.call(func(db *storeConn) (any, error) {
		_, err := db.Exec(`UPDATE message_sources SET state='refresh' WHERE event_id=?`, m.ID)
		return nil, err
	})
	if _, err := s.ClaimedEnvelope(c.InboundID, c.Claim); !errors.Is(err, ErrClaim) {
		t.Fatal(err)
	}
}

func TestSourceEditedForeignCandidateRejectsWithoutContentLeakOrRetry(t *testing.T) {
	s, _ := threadStore(t)
	cfg := Settings{Policy: threadPolicy(), ExpectedBotID: "4"}
	m := &discordgo.Message{ID: "3", ChannelID: "999", GuildID: "5", Content: "FOREIGN_CONTENT_CANARY", Author: &discordgo.User{ID: "1"}, Mentions: []*discordgo.User{{ID: "4"}}}
	receiveMessage(context.Background(), nil, s, cfg, m)
	s.InvalidateSourceUpdate(m.ChannelID, m.GuildID, m.ID)
	in, _ := s.NextSourceRefresh(epoch())
	r := localREST(t, func(w http.ResponseWriter, q *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{"id": "999", "guild_id": "5", "type": 0})
	})
	r.settings.Policy = threadPolicy()
	if err := refreshMessageSource(context.Background(), s, r, *in); err != nil {
		t.Fatal(err)
	}
	if next, _ := s.NextSourceRefresh(epoch() + 1000); next != nil {
		t.Fatal("foreign source still retries", next)
	}
	var sources, active int
	s.call(func(db *storeConn) (any, error) {
		db.QueryRow(`SELECT count(*) FROM message_sources WHERE event_id='3'`).Scan(&sources)
		n, err := activeInboundCount(db)
		active = n
		return nil, err
	})
	if sources != 0 || active != 0 {
		t.Fatal(sources, active)
	}
	if outcome, err := receiveMessage(context.Background(), nil, s, cfg, m); err != nil || outcome != "duplicate" {
		t.Fatal("lost bounded rejection dedup", outcome, err)
	}
}
func TestSourcePromotionStoresVerifiedThreadHeadForRefresh(t *testing.T) {
	s, _ := threadStore(t)
	e := threadCandidate()
	s.StageIngress(e)
	in, _ := s.NextValidation(epoch())
	var writes atomic.Int32
	r := threadREST(t, nil, &writes)
	if err := processValidation(context.Background(), s, r, readyThreadGateway(), *in, 0); err != nil {
		t.Fatal(err)
	}
	s.InvalidateSourceUpdate(e.ConversationID, e.GuildID, e.EventID)
	refresh, _ := s.NextSourceRefresh(epoch())
	if refresh == nil || refresh.Event.RouteKind != "guild_thread" || refresh.Event.ParentChannelID != "2" {
		t.Fatal(refresh)
	}
}

func TestSourceIdenticalRefreshBeforeFailedResultStillRequeues(t *testing.T) {
	s, cfg, m, c := sourceFixture(t)
	rid, _ := s.QueueReply(c.InboundID, c.Claim, "answer")
	chunk, _ := s.NextChunk()
	s.InvalidateSourceUpdate(m.ChannelID, m.GuildID, m.ID)
	in, _ := s.NextSourceRefresh(epoch())
	if out, err := s.ApplySourceRefresh(*in, projectGatewayMessage(cfg, m)); err != nil || out != "unchanged" {
		t.Fatal(out, err)
	}
	if err := s.RecordResult(*chunk, SendResult{State: "failed", Code: "source_not_current"}); err != nil {
		t.Fatal(err)
	}
	next, _ := s.NextChunk()
	if next == nil || next.ReplyID != rid {
		t.Fatal("same revision deadlocked", next)
	}
}
func TestSourceGenericConnectionFailureDoesNotAutoRequeue(t *testing.T) {
	s, _, _, c := sourceFixture(t)
	rid, _ := s.QueueReply(c.InboundID, c.Claim, "answer")
	chunk, _ := s.NextChunk()
	s.RecordResult(*chunk, SendResult{State: "failed", Code: "connection_changed_before_send"})
	if next, _ := s.NextChunk(); next != nil {
		t.Fatal("generic failure auto retried", next)
	}
	d, _ := s.Delivery(rid)
	if d.State != "failed" {
		t.Fatal(d)
	}
}

func TestSourcePartialGuildEventsUseExactStoredBinding(t *testing.T) {
	s, _ := threadStore(t)
	e := threadCandidate()
	s.StageIngress(e)
	if changed, err := s.InvalidateSourceUpdate(e.ConversationID, "", e.EventID); err != nil || !changed {
		t.Fatal(changed, err)
	}
	in, _ := s.NextSourceRefresh(epoch())
	if in == nil || in.Event.GuildID != "5" {
		t.Fatal("stored guild lost", in)
	}
	if changed, err := s.DeleteSource(e.ConversationID, "999", e.EventID); err != nil || changed {
		t.Fatal("wrong explicit guild accepted", changed, err)
	}
	if changed, err := s.DeleteSource(e.ConversationID, "", e.EventID); err != nil || !changed {
		t.Fatal(changed, err)
	}
}

func seedSourceCapacity(t *testing.T, s *Store, n int) {
	t.Helper()
	_, err := s.call(func(db *storeConn) (any, error) {
		return transact(db, func(db *storeConn) (any, error) {
			for i := 0; i < n; i++ {
				e := testEnvelope()
				e.EventID = fmt.Sprint(100 + i)
				raw, _ := json.Marshal(e)
				if _, err := db.Exec(`INSERT INTO inbound(id,platform,event_id,envelope,created) VALUES(?,?,?,?,?)`, fmt.Sprint("capacity-", i), e.Platform, e.EventID, string(raw), epoch()); err != nil {
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
func TestSourceHistoricalEditRespectsCapacityAndRetries(t *testing.T) {
	s, cfg, m, c := sourceFixture(t)
	if err := s.Ignore(c.InboundID, c.Claim); err != nil {
		t.Fatal(err)
	}
	seedSourceCapacity(t, s, 1000)
	s.InvalidateSourceUpdate(m.ChannelID, m.GuildID, m.ID)
	in, _ := s.NextSourceRefresh(epoch())
	edited := *m
	edited.Content = "changed historical question"
	if outcome, err := s.ApplySourceRefresh(*in, projectGatewayMessage(cfg, &edited)); err != nil || outcome != "queue_full" {
		t.Fatal(outcome, err)
	}
	var n int
	s.call(func(db *storeConn) (any, error) { v, err := activeInboundCount(db); n = v; return nil, err })
	if n != 1000 {
		t.Fatal(n)
	}
	if due, _ := s.NextSourceRefresh(epoch()); due != nil {
		t.Fatal("queue-full refresh busy retries", due)
	}
	s.call(func(db *storeConn) (any, error) {
		_, err := db.Exec(`UPDATE inbound SET state='ignored' WHERE id='capacity-0'`)
		return nil, err
	})
	in, _ = s.NextSourceRefresh(epoch() + 3)
	if in == nil {
		t.Fatal("lost capacity-blocked refresh")
	}
	if outcome, err := s.ApplySourceRefresh(*in, projectGatewayMessage(cfg, &edited)); err != nil || outcome != "revised" {
		t.Fatal(outcome, err)
	}
	s.call(func(db *storeConn) (any, error) { v, err := activeInboundCount(db); n = v; return nil, err })
	if n != 1000 {
		t.Fatal(n)
	}
}
func TestSourceActiveEditReusesReservedCapacity(t *testing.T) {
	s, cfg, m, _ := sourceFixture(t)
	seedSourceCapacity(t, s, 999)
	s.InvalidateSourceUpdate(m.ChannelID, m.GuildID, m.ID)
	in, _ := s.NextSourceRefresh(epoch())
	edited := *m
	edited.Content = "replace active slot"
	if out, err := s.ApplySourceRefresh(*in, projectGatewayMessage(cfg, &edited)); err != nil || out != "revised" {
		t.Fatal(out, err)
	}
	var n int
	s.call(func(db *storeConn) (any, error) { v, err := activeInboundCount(db); n = v; return nil, err })
	if n != 1000 {
		t.Fatal(n)
	}
}
