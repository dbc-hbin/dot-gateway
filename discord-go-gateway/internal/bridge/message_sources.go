package bridge

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/bwmarrin/discordgo"
)

// SQL event_id is an internal dedup key for revised rows only. The exact Discord
// ID always remains Envelope.EventID. Each revision owns a separate immutable
// inbound row, claim, reply and nonce; old reply sources are never overwritten.
func sourceLedgerKey(e Envelope) string {
	if e.SourceRevision == 0 {
		return e.EventID
	}
	return fmt.Sprintf("%s:revision:%d", e.EventID, e.SourceRevision)
}

func initMessageSources(db *storeConn) error {
	_, err := db.Exec(`CREATE TABLE IF NOT EXISTS message_sources(
 platform TEXT NOT NULL,event_id TEXT NOT NULL,channel_id TEXT NOT NULL,guild_id TEXT NOT NULL,
 revision INTEGER NOT NULL DEFAULT 0,generation INTEGER NOT NULL DEFAULT 0,
 state TEXT NOT NULL DEFAULT 'current',envelope TEXT NOT NULL,
 attempts INTEGER NOT NULL DEFAULT 0,next_attempt REAL NOT NULL DEFAULT 0,code TEXT,
 PRIMARY KEY(platform,event_id));
 CREATE INDEX IF NOT EXISTS source_refresh_due ON message_sources(state,next_attempt);`)
	if err != nil {
		return err
	}
	// initializeStore holds BEGIN IMMEDIATE: concurrent openers observe either
	// the complete backfill and marker, or neither after rollback/crash.
	var migrated int
	if err = db.QueryRow(`SELECT count(*) FROM runtime WHERE key='message_sources_migration_v1'`).Scan(&migrated); err != nil {
		return err
	}
	if migrated != 0 {
		return nil
	}
	// Additive migration. Old rows retain their exact IDs, source snapshots and
	// nonce ledger. An already migrated tombstone/head is never overwritten.
	rows, err := db.Query(`SELECT envelope FROM inbound UNION ALL SELECT envelope FROM ingress_validation WHERE state!='rejected'`)
	if err != nil {
		return err
	}
	var list []Envelope
	for rows.Next() {
		var raw string
		if err = rows.Scan(&raw); err != nil {
			rows.Close()
			return err
		}
		var e Envelope
		if json.Unmarshal([]byte(raw), &e) != nil {
			rows.Close()
			return errors.New("invalid legacy source envelope")
		}
		if e.EventID != "" {
			list = append(list, e)
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, e := range list {
		if err = registerSourceDB(db, e); err != nil {
			return err
		}
	}
	_, err = db.Exec(`INSERT INTO runtime(key,value) VALUES('message_sources_migration_v1','1')`)
	return err
}
func registerSourceDB(db *storeConn, e Envelope) error {
	if !isMessageSource(e) {
		return nil
	}
	raw, err := json.Marshal(e)
	if err != nil {
		return err
	}
	_, err = db.Exec(`INSERT OR IGNORE INTO message_sources(platform,event_id,channel_id,guild_id,revision,envelope) VALUES(?,?,?,?,?,?)`, e.Platform, e.EventID, e.ConversationID, e.GuildID, e.SourceRevision, string(raw))
	return err
}
func sourceKnownDB(db *storeConn, e Envelope) (bool, error) {
	if !isMessageSource(e) {
		return false, nil
	}
	var n int
	err := db.QueryRow(`SELECT count(*) FROM message_sources WHERE platform=? AND event_id=?`, e.Platform, e.EventID).Scan(&n)
	return n > 0, err
}
func sourceCurrentDB(db *storeConn, e Envelope) (bool, error) { return sourceCurrentDBDepth(db, e, 0) }
func sourceCurrentDBDepth(db *storeConn, e Envelope, depth int) (bool, error) {
	if e.Control != "" {
		return controlSourceCurrentDB(db, e, depth)
	}
	if !isMessageSource(e) {
		return true, nil
	}
	var channel, guild, state string
	var revision int64
	err := db.QueryRow(`SELECT channel_id,guild_id,revision,state FROM message_sources WHERE platform=? AND event_id=?`, e.Platform, e.EventID).Scan(&channel, &guild, &revision, &state)
	// Compatibility for directly seeded offline fixtures and historical rows.
	if err == sql.ErrNoRows {
		return e.SourceRevision == 0, nil
	}
	if err != nil {
		return false, err
	}
	return state == "current" && channel == e.ConversationID && guild == e.GuildID && revision == e.SourceRevision, nil
}
func (s *Store) SourceCurrent(e Envelope) (bool, error) {
	v, err := s.call(func(db *storeConn) (any, error) { return sourceCurrentDB(db, e) })
	if err != nil {
		return false, err
	}
	return v.(bool), nil
}

// MessageSource is a bounded refresh job, never supplied by an untrusted author.
type MessageSource struct {
	Event      Envelope
	Generation int64
	Attempts   int
}

// InvalidateSourceUpdate immediately revokes model ownership and send eligibility
// before any REST refresh. Partial updates never supply authoritative content.
func (s *Store) InvalidateSourceUpdate(channel, guild, id string) (bool, error) {
	v, err := s.call(func(db *storeConn) (any, error) {
		return transact(db, func(db *storeConn) (any, error) {
			var raw, state string
			err := db.QueryRow(`SELECT envelope,state FROM message_sources WHERE platform='discord' AND event_id=? AND channel_id=? AND (?='' OR guild_id=?)`, id, channel, guild, guild).Scan(&raw, &state)
			if err == sql.ErrNoRows {
				return false, nil
			}
			if err != nil {
				return nil, err
			}
			if state == "deleted" {
				return false, nil
			}
			var e Envelope
			if json.Unmarshal([]byte(raw), &e) != nil {
				return nil, errors.New("invalid source envelope")
			}
			if _, err = db.Exec(`UPDATE message_sources SET state='refresh',generation=generation+1,attempts=0,next_attempt=0,code=NULL WHERE platform=? AND event_id=?`, e.Platform, id); err != nil {
				return nil, err
			}
			if err = revokeSourceClaimsDB(db, e); err != nil {
				return nil, err
			}
			return true, nil
		})
	})
	if err != nil {
		return false, err
	}
	return v.(bool), nil
}
func revokeSourceClaimsDB(db *storeConn, e Envelope) error {
	invalidateMemorySourceDB(db, e)
	key := sourceLedgerKey(e)
	for _, q := range []string{
		`DELETE FROM consumer_claims WHERE inbound_id IN(SELECT id FROM inbound WHERE platform=? AND event_id=?)`,
		`DELETE FROM processing WHERE inbound_id IN(SELECT id FROM inbound WHERE platform=? AND event_id=?)`,
		`UPDATE inbound SET state='pending',claim=NULL,lease_until=NULL WHERE platform=? AND event_id=? AND state='claimed'`,
	} {
		if _, err := db.Exec(q, e.Platform, key); err != nil {
			return err
		}
	}
	return revokeDependentControlClaimsDB(db, e, false)
}

func (s *Store) DeleteSource(channel, guild, id string) (bool, error) {
	v, err := s.call(func(db *storeConn) (any, error) {
		return transact(db, func(db *storeConn) (any, error) {
			var raw string
			err := db.QueryRow(`SELECT envelope FROM message_sources WHERE platform='discord' AND event_id=? AND channel_id=? AND (?='' OR guild_id=?)`, id, channel, guild, guild).Scan(&raw)
			if err == sql.ErrNoRows {
				return false, nil
			}
			if err != nil {
				return nil, err
			}
			var e Envelope
			if json.Unmarshal([]byte(raw), &e) != nil {
				return nil, errors.New("invalid source envelope")
			}
			if _, err = db.Exec(`UPDATE message_sources SET state='deleted',generation=generation+1,code='source_deleted' WHERE platform=? AND event_id=?`, e.Platform, id); err != nil {
				return nil, err
			}
			if err = retireSourceDB(db, e, "source_deleted"); err != nil {
				return nil, err
			}
			return true, nil
		})
	})
	if err != nil {
		return false, err
	}
	return v.(bool), nil
}
func retireSourceDB(db *storeConn, e Envelope, code string) error {
	if err := revokeSourceClaimsDB(db, e); err != nil {
		return err
	}
	if err := revokeDependentControlClaimsDB(db, e, true); err != nil {
		return err
	}
	key := sourceLedgerKey(e)
	if _, err := db.Exec(`UPDATE inbound SET state='superseded',claim=NULL,lease_until=NULL WHERE platform=? AND event_id=?`, e.Platform, key); err != nil {
		return err
	}
	if _, err := db.Exec(`UPDATE ingress_validation SET state='superseded',code=? WHERE platform=? AND event_id=?`, code, e.Platform, key); err != nil {
		return err
	}
	rows, err := db.Query(`SELECT r.id FROM replies r JOIN inbound i ON i.id=r.inbound_id WHERE i.platform=? AND i.event_id=?`, e.Platform, key)
	if err != nil {
		return err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, id := range ids {
		if _, err = cancelReplyIfUnsent(db, id, code); err != nil {
			return err
		}
	}
	return nil
}
func (s *Store) NextSourceRefresh(now float64) (*MessageSource, error) {
	v, err := s.call(func(db *storeConn) (any, error) {
		var in MessageSource
		var raw string
		err := db.QueryRow(`SELECT envelope,generation,attempts FROM message_sources WHERE state='refresh' AND next_attempt<=? ORDER BY next_attempt,event_id LIMIT 1`, now).Scan(&raw, &in.Generation, &in.Attempts)
		if err == sql.ErrNoRows {
			return (*MessageSource)(nil), nil
		}
		if err != nil {
			return nil, err
		}
		if json.Unmarshal([]byte(raw), &in.Event) != nil {
			return nil, errors.New("invalid source envelope")
		}
		return &in, nil
	})
	if err != nil {
		return nil, err
	}
	return v.(*MessageSource), nil
}
func (s *Store) DeferSourceRefresh(in MessageSource, code string) error {
	attempt := in.Attempts + 1
	if attempt > 5 {
		attempt = 5
	}
	delay := float64(int64(1) << attempt)
	if delay > 30 {
		delay = 30
	}
	_, err := s.call(func(db *storeConn) (any, error) {
		_, err := db.Exec(`UPDATE message_sources SET attempts=attempts+1,next_attempt=?,code=? WHERE platform=? AND event_id=? AND state='refresh' AND generation=?`, epoch()+delay, symbolicCode(code), in.Event.Platform, in.Event.EventID, in.Generation)
		return nil, err
	})
	return err
}

// ApplySourceRefresh requires exact message/route validation by the caller. A
// generation CAS discards late/out-of-order reads; tombstones cannot resurrect.
func (s *Store) ApplySourceRefresh(in MessageSource, e Envelope) (string, error) {
	if e.Platform != in.Event.Platform || e.EventID != in.Event.EventID || e.ConversationID != in.Event.ConversationID || e.GuildID != in.Event.GuildID || e.SenderID != in.Event.SenderID {
		return "", errors.New("source_identity_changed")
	}
	if e.RouteKind == "guild_thread" {
		e.RouteKind, e.ParentChannelID, e.ThreadType, e.ThreadName = "guild_thread_candidate", "", 0, ""
	}
	v, err := s.call(func(db *storeConn) (any, error) {
		return transact(db, func(db *storeConn) (any, error) {
			var state, raw string
			var generation int64
			if err := db.QueryRow(`SELECT state,generation,envelope FROM message_sources WHERE platform=? AND event_id=?`, e.Platform, e.EventID).Scan(&state, &generation, &raw); err != nil {
				return nil, err
			}
			if state != "refresh" || generation != in.Generation {
				return "superseded", nil
			}
			var old Envelope
			if json.Unmarshal([]byte(raw), &old) != nil {
				return nil, errors.New("invalid source envelope")
			}
			// Legacy snapshots had no fingerprint. Equality of text remains useful but
			// cannot prove media equality, so first meaningful fetch creates a revision.
			same := old.ContentHash != "" && old.ContentHash == e.ContentHash || legacyEnvelopeUnchanged(old, e)
			if same {
				if _, err := db.Exec(`UPDATE chunks SET state='pending',code=NULL WHERE state='failed' AND code='source_not_current' AND reply_id IN(SELECT r.id FROM replies r JOIN inbound i ON i.id=r.inbound_id WHERE i.platform=? AND i.event_id=?)`, old.Platform, sourceLedgerKey(old)); err != nil {
					return nil, err
				}
				_, err := db.Exec(`UPDATE message_sources SET state='current',attempts=0,next_attempt=0,code=NULL WHERE platform=? AND event_id=?`, e.Platform, e.EventID)
				if err == nil {
					restoreMemorySourceDB(db, old)
				}
				return "unchanged", err
			}
			if s.stages(e) {
				active, err := activeInboundCount(db)
				if err != nil {
					return nil, err
				}
				var own int
				if err = db.QueryRow(`SELECT (SELECT count(*) FROM inbound WHERE platform=? AND event_id=? AND state IN('pending','claimed')) + (SELECT count(*) FROM ingress_validation WHERE platform=? AND event_id=? AND state IN('pending','blocked'))`, old.Platform, sourceLedgerKey(old), old.Platform, sourceLedgerKey(old)).Scan(&own); err != nil {
					return nil, err
				}
				if active-own >= 1000 {
					_, err = db.Exec(`UPDATE message_sources SET attempts=attempts+1,next_attempt=?,code='source_queue_full' WHERE platform=? AND event_id=? AND generation=?`, epoch()+2, e.Platform, e.EventID, in.Generation)
					return "queue_full", err
				}
			}
			if err := retireSourceDB(db, old, "source_superseded"); err != nil {
				return nil, err
			}
			e.SourceRevision = old.SourceRevision + 1
			encoded, err := json.Marshal(e)
			if err != nil {
				return nil, err
			}
			nextState := "current"
			if !s.stages(e) {
				nextState = "rejected"
			}
			if _, err = db.Exec(`UPDATE message_sources SET revision=?,state=?,envelope=?,attempts=0,next_attempt=0,code=NULL WHERE platform=? AND event_id=?`, e.SourceRevision, nextState, string(encoded), e.Platform, e.EventID); err != nil {
				return nil, err
			}
			if nextState != "current" {
				return "rejected", nil
			}
			id, err := uuidHex()
			if err != nil {
				return nil, err
			}
			_, err = db.Exec(`INSERT INTO ingress_validation(id,platform,event_id,envelope,created) VALUES(?,?,?,?,?)`, id, e.Platform, sourceLedgerKey(e), string(encoded), epoch())
			return "revised", err
		})
	})
	if err != nil {
		return "", err
	}
	return v.(string), nil
}

func (r *RESTClient) readSourceMessage(ctx context.Context, e Envelope) (*discordgo.Message, error) {
	c, err := r.Channel(ctx, e.ConversationID)
	if err != nil {
		return nil, err
	}
	if _, err = r.validateIngressRoute(ctx, c, e); err != nil {
		return nil, err
	}
	var m discordgo.Message
	if err = r.get(ctx, "/channels/"+e.ConversationID+"/messages/"+e.EventID, &m); err != nil {
		if err.Error() == "preflight_http_404" {
			return nil, errors.New("source_message_deleted")
		}
		return nil, err
	}
	if m.ID != e.EventID || m.ChannelID != e.ConversationID || (m.GuildID != "" && m.GuildID != e.GuildID) || m.Author == nil || m.Author.ID != e.SenderID || m.Author.Bot || m.WebhookID != "" || (m.Type != discordgo.MessageTypeDefault && m.Type != discordgo.MessageTypeReply) {
		return nil, errors.New("source_identity_changed")
	}
	m.GuildID = e.GuildID
	return &m, nil
}
func refreshMessageSource(ctx context.Context, store *Store, rest *RESTClient, in MessageSource) error {
	check, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	m, err := rest.readSourceMessage(check, in.Event)
	if err != nil {
		if err.Error() == "source_message_deleted" {
			_, err = store.DeleteSource(in.Event.ConversationID, in.Event.GuildID, in.Event.EventID)
			return err
		}
		if err.Error() == "preflight_channel_mismatch" || err.Error() == "preflight_recipient_mismatch" || err.Error() == "source_identity_changed" {
			return store.RejectSourceRefresh(in)
		}
		return store.DeferSourceRefresh(in, err.Error())
	}
	e := projectGatewayMessage(rest.settings, m)
	_, err = store.ApplySourceRefresh(in, e)
	return err
}
func messageSourceLoop(ctx context.Context, store *Store, rest *RESTClient, hub *WakeHub, g *gatewayState) error {
	wake, unsub := hub.Subscribe()
	defer unsub()
	tick := time.NewTicker(250 * time.Millisecond)
	defer tick.Stop()
	for {
		for ctx.Err() == nil && g.ready.Load() {
			in, err := store.NextSourceRefresh(epoch())
			if err != nil {
				return err
			}
			if in == nil {
				break
			}
			if err = refreshMessageSource(ctx, store, rest, *in); err != nil {
				return err
			}
			hub.Notify()
		}
		select {
		case <-ctx.Done():
			return nil
		case <-wake:
		case <-tick.C:
		}
	}
}

// VerifySourceBeforeSend catches missed update/delete events. It never posts.
// It is installed by the real gateway, not by standalone offline REST fixtures.
func (r *RESTClient) VerifySourceBeforeSend(ctx context.Context, s *Store, e Envelope) error {
	if e.Control != "" {
		current, err := s.SourceCurrent(e)
		if err != nil {
			return err
		}
		if !current {
			return errors.New("source_not_current")
		}
		ce, err := e.ControlEvent()
		if err != nil {
			return err
		}
		if ce.Kind == "reaction" {
			return r.verifyReactionTarget(ctx, e)
		}
		return nil
	}
	if !isMessageSource(e) {
		return nil
	}
	current, err := s.SourceCurrent(e)
	if err != nil {
		return err
	}
	if !current {
		return errors.New("source_not_current")
	}
	m, err := r.readSourceMessage(ctx, e)
	if err != nil {
		if err.Error() == "source_message_deleted" {
			_, de := s.DeleteSource(e.ConversationID, e.GuildID, e.EventID)
			if de != nil {
				return de
			}
		}
		return err
	}
	if e.ContentHash != "" && e.ContentHash == MessageContentFingerprint(m) {
		return nil
	}
	// Backward-compatible legacy text-only snapshots may proceed when equal;
	// new snapshots always carry the full content/media fingerprint.
	if legacyEnvelopeUnchanged(e, projectGatewayMessage(r.settings, m)) {
		return nil
	}
	in, err := s.InvalidateObservedSource(e)
	if err != nil {
		return err
	}
	if in != nil {
		if _, err = s.ApplySourceRefresh(*in, projectGatewayMessage(r.settings, m)); err != nil {
			return err
		}
	}
	return errors.New("source_changed_before_send")
}
func (s *Store) SourceRefreshFor(e Envelope) (*MessageSource, error) {
	v, err := s.call(func(db *storeConn) (any, error) {
		var in MessageSource
		var raw string
		err := db.QueryRow(`SELECT envelope,generation,attempts FROM message_sources WHERE platform=? AND event_id=? AND state='refresh'`, e.Platform, e.EventID).Scan(&raw, &in.Generation, &in.Attempts)
		if err == sql.ErrNoRows {
			return (*MessageSource)(nil), nil
		}
		if err != nil {
			return nil, err
		}
		if json.Unmarshal([]byte(raw), &in.Event) != nil {
			return nil, errors.New("invalid source envelope")
		}
		return &in, nil
	})
	if err != nil {
		return nil, err
	}
	return v.(*MessageSource), nil
}

func cancelStaleReplyAfterResultDB(db *storeConn, c Chunk) error {
	current, err := sourceCurrentDB(db, c.Source)
	if err != nil || current {
		return err
	}
	if c.Source.Control != "" {
		_, err = cancelReplyIfUnsent(db, c.ReplyID, "control_source_not_current")
		return err
	}
	var state string
	if err = db.QueryRow(`SELECT state FROM message_sources WHERE platform=? AND event_id=?`, c.Source.Platform, c.Source.EventID).Scan(&state); err != nil {
		return err
	}
	if state == "refresh" {
		return nil
	} // A content-identical refresh can restore it.
	code := "source_superseded"
	if state == "deleted" {
		code = "source_deleted"
	}
	_, err = cancelReplyIfUnsent(db, c.ReplyID, code)
	return err
}

// Create/update/delete share one FIFO so a delete cannot overtake an unstaged
// create. Only existing owner-bound sources are eligible for update/delete work.
type sourceGatewayEvent struct{ Create, Update, Delete *discordgo.Message }

func receiveSourceEvent(ctx context.Context, r *RESTClient, s *Store, settings Settings, event sourceGatewayEvent) (string, error) {
	if event.Create != nil {
		return receiveMessage(ctx, r, s, settings, event.Create)
	}
	if m := event.Update; m != nil {
		if err := s.catchupObserveMutation(m.ChannelID, m.GuildID, m.ID); err != nil {
			return "", err
		}
		changed, err := s.InvalidateSourceUpdate(m.ChannelID, m.GuildID, m.ID)
		if changed {
			if err == nil {
				_, err = s.InvalidateControlTarget(m.ChannelID, m.GuildID, m.ID, "target_updated")
			}
			return "source_update", err
		}
		if err == nil {
			changed, err = s.InvalidateControlTarget(m.ChannelID, m.GuildID, m.ID, "target_updated")
			if changed {
				return "control_target_updated", err
			}
		}
		return "rejected", err
	}
	if m := event.Delete; m != nil {
		if err := s.catchupObserveMutation(m.ChannelID, m.GuildID, m.ID); err != nil {
			return "", err
		}
		changed, err := s.DeleteSource(m.ChannelID, m.GuildID, m.ID)
		if changed {
			if err == nil {
				_, err = s.InvalidateControlTarget(m.ChannelID, m.GuildID, m.ID, "target_deleted")
			}
			return "source_deleted", err
		}
		if err == nil {
			changed, err = s.InvalidateControlTarget(m.ChannelID, m.GuildID, m.ID, "target_deleted")
			if changed {
				return "control_target_deleted", err
			}
		}
		return "rejected", err
	}
	return "rejected", nil
}

// The source read for a send belongs to one observed revision. A queued newer
// gateway update wins; an old GET may never overwrite that newer generation.
func (s *Store) InvalidateObservedSource(e Envelope) (*MessageSource, error) {
	v, err := s.call(func(db *storeConn) (any, error) {
		return transact(db, func(db *storeConn) (any, error) {
			var raw, state string
			var revision, generation int64
			err := db.QueryRow(`SELECT envelope,state,revision,generation FROM message_sources WHERE platform=? AND event_id=? AND channel_id=? AND guild_id=?`, e.Platform, e.EventID, e.ConversationID, e.GuildID).Scan(&raw, &state, &revision, &generation)
			if err == sql.ErrNoRows {
				return (*MessageSource)(nil), nil
			}
			if err != nil {
				return nil, err
			}
			if state != "current" || revision != e.SourceRevision {
				return (*MessageSource)(nil), nil
			}
			var original Envelope
			if json.Unmarshal([]byte(raw), &original) != nil {
				return nil, errors.New("invalid source envelope")
			}
			if _, err = db.Exec(`UPDATE message_sources SET state='refresh',generation=generation+1,attempts=0,next_attempt=0 WHERE platform=? AND event_id=?`, e.Platform, e.EventID); err != nil {
				return nil, err
			}
			if err = revokeSourceClaimsDB(db, original); err != nil {
				return nil, err
			}
			return &MessageSource{Event: original, Generation: generation + 1}, nil
		})
	})
	if err != nil {
		return nil, err
	}
	return v.(*MessageSource), nil
}

// Legacy text-only envelopes cannot prove historical media identity. Permit
// unchanged text/reference only when the fresh message still has no authored
// media; async preview decoration alone never becomes a new user instruction.
func legacyEnvelopeUnchanged(old, fresh Envelope) bool {
	if old.ContentHash != "" || old.Media != "" || old.Text != fresh.Text || old.ReplyToEventID != fresh.ReplyToEventID {
		return false
	}
	media, err := fresh.Media.Metadata()
	if err != nil {
		return false
	}
	return len(media.Attachments) == 0 && len(media.Stickers) == 0 && media.Poll == nil && len(media.Forwards) == 0
}

// An edited quarantine candidate proven out of scope is terminal, just like the
// ordinary validator. It cannot occupy refresh capacity or retain an unbounded
// side copy of rejected-channel content.
func (s *Store) RejectSourceRefresh(in MessageSource) error {
	_, err := s.call(func(db *storeConn) (any, error) {
		return transact(db, func(db *storeConn) (any, error) {
			var state string
			var generation int64
			err := db.QueryRow(`SELECT state,generation FROM message_sources WHERE platform=? AND event_id=?`, in.Event.Platform, in.Event.EventID).Scan(&state, &generation)
			if err == sql.ErrNoRows {
				return nil, nil
			}
			if err != nil {
				return nil, err
			}
			if state != "refresh" || generation != in.Generation {
				return nil, nil
			}
			if err = retireSourceDB(db, in.Event, "source_out_of_scope"); err != nil {
				return nil, err
			}
			if in.Event.RouteKind == "guild_thread_candidate" {
				if _, err = db.Exec(`UPDATE ingress_validation SET state='rejected',envelope='{}',code='out_of_scope' WHERE platform=? AND event_id=?`, in.Event.Platform, sourceLedgerKey(in.Event)); err != nil {
					return nil, err
				}
				if _, err = db.Exec(`DELETE FROM ingress_validation WHERE state='rejected' AND id NOT IN(SELECT id FROM ingress_validation WHERE state='rejected' ORDER BY created DESC,rowid DESC LIMIT 1000)`); err != nil {
					return nil, err
				}
				_, err = db.Exec(`DELETE FROM message_sources WHERE platform=? AND event_id=?`, in.Event.Platform, in.Event.EventID)
			} else {
				_, err = db.Exec(`UPDATE message_sources SET state='rejected',code='source_out_of_scope' WHERE platform=? AND event_id=?`, in.Event.Platform, in.Event.EventID)
			}
			return nil, err
		})
	})
	return err
}

// The control-plane integration extends this single hook with Control=="" and
// ReplyKind!="interaction". The standalone message-plane build has no controls.
func isMessageSource(e Envelope) bool {
	return e.Platform == "discord" && e.Control == "" && e.ReplyKind != "interaction"
}

// RecordResult and an unchanged refresh can complete in either order. Only this
// explicit source fence failure is known to have attempted zero POSTs.
func restoreCurrentSourceResultDB(db *storeConn, c Chunk, r SendResult) error {
	if r.State != "failed" || r.Code != "source_not_current" {
		return nil
	}
	current, err := sourceCurrentDB(db, c.Source)
	if err != nil || !current {
		return err
	}
	_, err = db.Exec(`UPDATE chunks SET state='pending',code=NULL WHERE reply_id=? AND idx=? AND state='failed' AND code='source_not_current'`, c.ReplyID, c.Index)
	return err
}
func (r *RESTClient) SendCurrentSourceMeasured(ctx context.Context, s *Store, c Chunk, guard func() bool) (SendResult, Diagnostics) {
	if err := r.VerifySourceBeforeSend(ctx, s, c.Source); err != nil {
		return SendResult{State: "failed", Code: err.Error()}, Diagnostics{}
	}
	var sourceInvalidated atomic.Bool
	combined := func() bool {
		current, err := s.SourceCurrent(c.Source)
		if err == nil && !current {
			sourceInvalidated.Store(true)
		}
		return err == nil && current && (guard == nil || guard())
	}
	result, measurement := r.SendGuardedMeasured(ctx, c, combined)
	if result.State == "failed" && result.Code == "connection_changed_before_send" && sourceInvalidated.Load() {
		result.Code = "source_not_current"
		measurement.Code = result.Code
	}
	return result, measurement
}
