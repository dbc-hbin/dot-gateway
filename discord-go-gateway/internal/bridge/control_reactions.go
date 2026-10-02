package bridge

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/bwmarrin/discordgo"
)

type sentControlTarget struct {
	Request, Revision, Text, Message, ReplyID, Output, Receipt string
	Index                                                      int
	Source                                                     Envelope
}

func sentControlTargetDB(db *storeConn, channel, message string) (sentControlTarget, error) {
	var t sentControlTarget
	var invalid int
	if err := db.QueryRow("SELECT count(*) FROM control_target_invalidations WHERE channel=? AND message=?", channel, message).Scan(&invalid); err != nil {
		return t, err
	}
	projection, projectionID, projectionErr := verifiedOperationControlSnapshotDB(db, channel, message)
	if projectionErr != nil && projectionErr != sql.ErrNoRows {
		return t, projectionErr
	}
	if invalid != 0 && projectionErr != nil {
		return t, errors.New("reaction_target_invalidated")
	}
	var raw, code string
	err := db.QueryRow(`SELECT r.inbound_id,c.text,i.envelope,c.message_id,COALESCE(c.code,''),c.reply_id,c.idx FROM chunks c JOIN replies r ON r.id=c.reply_id JOIN inbound i ON i.id=r.inbound_id WHERE c.state='sent' AND c.message_id=? AND json_extract(i.envelope,'$.conversation_id')=?`, message, channel).Scan(&t.Request, &t.Text, &raw, &t.Message, &code, &t.ReplyID, &t.Index)
	if err != nil {
		return t, errors.New("reaction_target_not_sent")
	}
	if json.Unmarshal([]byte(raw), &t.Source) != nil {
		return t, errors.New("invalid_reaction_target")
	}
	if code == "ack_terminal_lf_removed" {
		t.Text = strings.TrimSuffix(t.Text, "\n")
	}
	t.Revision = controlRevision(t.Source)
	var richSchema int
	if err = db.QueryRow("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='reply_outputs'").Scan(&richSchema); err != nil {
		return t, err
	}
	if richSchema != 0 {
		output, err := replyChunkOutputDB(db, t.ReplyID, t.Index)
		if err != nil {
			return t, err
		}
		t.Output = string(output)
		if t.Output != "" {
			if err = db.QueryRow("SELECT receipt FROM reply_output_receipts WHERE reply_id=? AND idx=?", t.ReplyID, t.Index).Scan(&t.Receipt); err != nil || !json.Valid([]byte(t.Output)) || !json.Valid([]byte(t.Receipt)) {
				return t, errors.New("reaction_output_receipt_missing")
			}
			hash := sha256.Sum256([]byte(t.Revision + "\x00" + t.Output + "\x00" + t.Receipt))
			t.Revision = hex.EncodeToString(hash[:])
		}
	}

	if projectionErr == nil {
		t.Text = projection.Content
		t.Revision = operationHash([]string{t.Revision, projectionID, operationJSON(projection)})
	}
	return t, nil
}
func (s *Store) sentControlTarget(channel, message string) (sentControlTarget, error) {
	v, err := s.call(func(db *storeConn) (any, error) { return sentControlTargetDB(db, channel, message) })
	if err != nil {
		return sentControlTarget{}, err
	}
	return v.(sentControlTarget), nil
}
func (r *RESTClient) verifyReactionTarget(ctx context.Context, e Envelope) error {
	ce, err := e.ControlEvent()
	if err != nil || ce.Kind != "reaction" {
		return errors.New("invalid_reaction_control")
	}
	if r.controlStore == nil {
		return errors.New("reaction_source_store_required")
	}
	target, err := r.controlStore.sentControlTarget(e.ConversationID, ce.TargetMessageID)
	if err != nil || target.Request != ce.TargetRequestID || target.Revision != ce.TargetRevision {
		return errors.New("source_not_current")
	}
	current, err := r.controlStore.SourceCurrent(target.Source)
	if err != nil || !current {
		return errors.New("source_not_current")
	}
	depth, _ := ctx.Value(controlTargetDepthKey{}).(int)
	if depth >= 32 {
		return errors.New("source_not_current")
	}
	if err = r.VerifySourceBeforeSend(context.WithValue(ctx, controlTargetDepthKey{}, depth+1), r.controlStore, target.Source); err != nil {
		return err
	}
	if projected, err := r.verifyEditedControlTarget(ctx, e, target); err != nil {
		return err
	} else if projected {
		return nil
	}
	if len(ce.TargetOutput) != 0 {
		if r.controlStore == nil {
			return errors.New("reaction_rich_verifier_unavailable")
		}
		target, err := r.controlStore.sentControlTarget(e.ConversationID, ce.TargetMessageID)
		if err != nil || target.Revision != ce.TargetRevision || target.Output != string(ce.TargetOutput) || target.Receipt != string(ce.TargetReceipt) {
			return errors.New("reaction_rich_target_changed")
		}
		reader, ok := any(r.controlStore).(interface {
			ReplyReadback(string, int) (Chunk, SendResult, error)
		})
		if !ok {
			return errors.New("reaction_rich_verifier_unavailable")
		}
		verifier, ok := any(r).(interface {
			VerifyStoredReply(context.Context, Chunk, SendResult) (SendResult, error)
		})
		if !ok {
			return errors.New("reaction_rich_verifier_unavailable")
		}
		chunk, receipt, err := reader.ReplyReadback(target.ReplyID, target.Index)
		if err != nil {
			return errors.New("reaction_rich_target_changed")
		}
		if _, err = verifier.VerifyStoredReply(ctx, chunk, receipt); err != nil {
			return errors.New("reaction_rich_target_changed")
		}
		return nil // Full immutable content/output/receipt were checked together.
	}
	var m discordgo.Message
	if err = r.get(ctx, "/channels/"+e.ConversationID+"/messages/"+ce.TargetMessageID, &m); err != nil {
		return errors.New("reaction_target_unavailable")
	}
	if m.ID != ce.TargetMessageID || m.ChannelID != e.ConversationID || m.Author == nil || m.Author.ID != r.settings.ExpectedBotID || !m.Author.Bot || m.Content != ce.TargetText {
		return errors.New("reaction_target_changed")
	}
	return nil
}

// Reaction input is owner-authored context on a verified bot message. It does
// not grant approval, dispatch commands, or synthesize a user message snowflake.
func (s *InteractionService) HandleReaction(ctx context.Context, m *discordgo.MessageReaction, added bool) string {
	if m == nil || m.UserID != s.settings.Policy.OwnerID || !Snowflake(m.MessageID) || !Snowflake(m.ChannelID) || !utf8.ValidString(m.Emoji.Name) {
		return "rejected"
	}
	emoji := m.Emoji.Name
	if m.Emoji.ID != "" {
		if !Snowflake(m.Emoji.ID) {
			return "rejected"
		}
		emoji = "id:" + m.Emoji.ID
	}
	if emoji == "" || len(emoji) > 128 {
		return "rejected"
	}
	target, err := s.store.sentControlTarget(m.ChannelID, m.MessageID)
	if err != nil {
		return "rejected"
	}
	if target.Source.SenderID != m.UserID || target.Source.GuildID != m.GuildID || !s.settings.Policy.Allows(target.Source) {
		return "rejected"
	}
	{
		current, err := s.store.SourceCurrent(target.Source)
		if err != nil || !current {
			return "rejected"
		}
	}
	route, err := s.route(ctx, m.MessageID, m.ChannelID, m.GuildID, m.UserID)
	if err != nil {
		return "rejected"
	}
	route.Text, route.ReplyKind = "", "message"
	id, err := uuidHex()
	if err != nil {
		return "store_failed"
	}
	route.Control = encodeControl(ControlEvent{Version: 1, Kind: "reaction", ID: id, ActorID: m.UserID, TargetMessageID: m.MessageID, TargetRequestID: target.Request, TargetRevision: target.Revision, TargetText: target.Text, TargetOutput: json.RawMessage(target.Output), TargetReceipt: json.RawMessage(target.Receipt), Emoji: emoji, Added: added})
	if err = s.rest.verifyReactionTarget(ctx, route); err != nil {
		return "rejected"
	}
	outcome, err := s.store.ingestReaction(route)
	if err != nil {
		return "store_failed"
	}
	if outcome == "accepted" && s.notify != nil {
		s.notify()
	}
	return outcome
}
func (s *Store) ingestReaction(e Envelope) (string, error) {
	if !s.policy.Accepts(e) {
		return "rejected", nil
	}
	ce, err := e.ControlEvent()
	if err != nil || ce.Kind != "reaction" {
		return "rejected", nil
	}
	v, err := s.call(func(db *storeConn) (any, error) {
		return transact(db, func(db *storeConn) (any, error) {
			target, err := sentControlTargetDB(db, e.ConversationID, ce.TargetMessageID)
			if err != nil || target.Request != ce.TargetRequestID || target.Revision != ce.TargetRevision || target.Text != ce.TargetText || target.Output != string(ce.TargetOutput) || target.Receipt != string(ce.TargetReceipt) {
				return "rejected", nil
			}
			current, err := sourceCurrentDB(db, target.Source)
			if err != nil {
				return nil, err
			}
			if !current {
				return "rejected", nil
			}
			var present, sequence int
			err = db.QueryRow("SELECT present,sequence FROM control_reactions WHERE channel=? AND message=? AND owner=? AND emoji=?", e.ConversationID, ce.TargetMessageID, e.SenderID, ce.Emoji).Scan(&present, &sequence)
			if err != nil && err != sql.ErrNoRows {
				return nil, err
			}
			next := 0
			if ce.Added {
				next = 1
			}
			if present == next {
				return "duplicate", nil
			}
			n, err := activeInboundCount(db)
			if err != nil {
				return nil, err
			}
			if n >= 1000 {
				return "queue_full", nil
			}
			sequence++
			var pending string
			err = db.QueryRow("SELECT id FROM control_pending_responses WHERE request=? AND revision=? AND message=? AND used=0 AND expires>? ORDER BY expires LIMIT 1", target.Request, target.Revision, ce.TargetMessageID, epoch()).Scan(&pending)
			if err != nil && err != sql.ErrNoRows {
				return nil, err
			}
			if pending != "" && ce.Added {
				ce.PendingResponseID = pending
				if err = changedOne(db.Exec("UPDATE control_pending_responses SET used=1 WHERE id=? AND used=0", pending)); err != nil {
					return nil, err
				}
			}
			// Transition identity is persistent, independent of Gateway redelivery.
			ce.ID = ce.TargetMessageID + ":" + e.SenderID + ":" + ce.Emoji + ":" + strconv.Itoa(sequence)
			e.Control = encodeControl(ce)
			id, err := uuidHex()
			if err != nil {
				return nil, err
			}
			raw, err := json.Marshal(e)
			if err != nil {
				return nil, err
			}
			if _, err = db.Exec("INSERT INTO inbound(id,platform,event_id,envelope,created) VALUES(?,?,?,?,?)", id, e.Platform, "control:"+ce.ID, string(raw), epoch()); err != nil {
				return nil, err
			}
			_, err = db.Exec("INSERT INTO control_reactions VALUES(?,?,?,?,?,?) ON CONFLICT(channel,message,owner,emoji) DO UPDATE SET present=excluded.present,sequence=excluded.sequence", e.ConversationID, ce.TargetMessageID, e.SenderID, ce.Emoji, next, sequence)
			return "accepted", err
		})
	})
	if err != nil {
		return "", err
	}
	return v.(string), nil
}

type controlTargetContextKey struct{}

type controlTargetDepthKey struct{}
