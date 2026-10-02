package bridge

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"mime"
	"sort"
)

// ReplyOutputReceipt is a canonical JSON value containing returned attachment
// identity, never expiring download URLs or authorization material.
type ReplyOutputReceipt string

func (p ReplyOutputReceipt) MarshalJSON() ([]byte, error) {
	if p == "" {
		return []byte("null"), nil
	}
	return []byte(p), nil
}

func (p *ReplyOutputReceipt) UnmarshalJSON(b []byte) error {
	if string(b) == "null" {
		*p = ""
		return nil
	}
	var files []replyReceiptFile
	if err := strictReplyJSON(b, &files); err != nil {
		return err
	}
	raw, _ := json.Marshal(files)
	*p = ReplyOutputReceipt(raw)
	return nil
}

type replyReceiptFile struct {
	ID          string `json:"id"`
	Filename    string `json:"filename"`
	Size        int64  `json:"size"`
	ContentType string `json:"content_type"`
	Description string `json:"description,omitempty"`
}

func replyAckReceipt(a map[string]json.RawMessage, c Chunk) ReplyOutputReceipt {
	if c.Output == "" {
		return ""
	}
	var files []replyReceiptFile
	_ = json.Unmarshal(a["attachments"], &files)
	if files == nil {
		files = []replyReceiptFile{}
	}
	for i := range files {
		files[i].ContentType, _, _ = mime.ParseMediaType(files[i].ContentType)
		files[i].ContentType = canonicalReplyMediaType(files[i].ContentType)
	}
	sort.Slice(files, func(i, j int) bool { return files[i].Filename < files[j].Filename })
	b, _ := json.Marshal(files)
	return ReplyOutputReceipt(b)
}
func persistReplyOutputReceipt(db *storeConn, c Chunk, r SendResult) error {
	if c.Output == "" || r.State != "sent" {
		return nil
	}
	if r.OutputReceipt == "" {
		return errors.New("missing_reply_output_receipt")
	}
	var files []replyReceiptFile
	if strictReplyJSON([]byte(r.OutputReceipt), &files) != nil {
		return errors.New("invalid_reply_output_receipt")
	}
	out, e := decodeReplyOutput(c.Output)
	if e != nil || len(files) != len(out.Attachments) {
		return errors.New("invalid_reply_output_receipt")
	}
	ids := map[string]bool{}
	names := map[string]bool{}
	for _, f := range files {
		if !Snowflake(f.ID) || ids[f.ID] || names[f.Filename] {
			return errors.New("invalid_reply_output_receipt")
		}
		ids[f.ID] = true
		names[f.Filename] = true
		found := false
		for _, want := range out.Attachments {
			if want.Filename == f.Filename && want.Size == f.Size && want.ContentType == f.ContentType && want.Description == f.Description {
				found = true
				break
			}
		}
		if !found {
			return errors.New("invalid_reply_output_receipt")
		}
	}
	_, e = db.Exec("INSERT INTO reply_output_receipts(reply_id,idx,receipt) VALUES(?,?,?)", c.ReplyID, c.Index, string(r.OutputReceipt))
	return e
}

// ReplyReadback returns the exact immutable stored chunk and receipt for a known
// sent message. This does not transition delivery state or resolve uncertainty.
func (s *Store) ReplyReadback(replyID string, index int) (Chunk, SendResult, error) {
	return s.replyVerificationChunk(replyID, index, false)
}

func (s *Store) replyVerificationChunk(replyID string, index int, uncertain bool) (Chunk, SendResult, error) {
	type result struct {
		C Chunk
		R SendResult
	}
	v, e := s.call(func(db *storeConn) (any, error) {
		var v result
		var source string
		err := db.QueryRow(`SELECT c.reply_id,c.idx,c.text,i.envelope,`+chunkReplyOutputSQL+`,c.state,COALESCE(c.message_id,''),COALESCE(receipt.receipt,'') FROM chunks c JOIN replies r ON r.id=c.reply_id JOIN inbound i ON i.id=r.inbound_id LEFT JOIN reply_output_receipts receipt ON receipt.reply_id=c.reply_id AND receipt.idx=c.idx WHERE c.reply_id=? AND c.idx=?`, replyID, index).Scan(&v.C.ReplyID, &v.C.Index, &v.C.Text, &source, &v.C.Output, &v.R.State, &v.R.MessageID, &v.R.OutputReceipt)
		if err == sql.ErrNoRows {
			return nil, errors.New("unknown_reply_chunk")
		}
		if err != nil {
			return nil, err
		}
		if json.Unmarshal([]byte(source), &v.C.Source) != nil || !s.policy.Allows(v.C.Source) || (!uncertain && (v.R.State != "sent" || !Snowflake(v.R.MessageID))) || (uncertain && v.R.State != "uncertain") {
			return nil, errors.New("reply_readback_unavailable")
		}
		return v, nil
	})
	if e != nil {
		return Chunk{}, SendResult{}, e
	}
	r := v.(result)
	return r.C, r.R, nil
}

// VerifyStoredReply is read-only, requiring both message and attachment IDs to
// match a committed receipt. An interrupted send still requires explicit review.
func (r *RESTClient) VerifyStoredReply(ctx context.Context, c Chunk, receipt SendResult) (SendResult, error) {
	if c.Source.ReplyKind == "interaction" {
		return SendResult{}, errors.New("interaction_readback_requires_token")
	}
	if !Snowflake(receipt.MessageID) || receipt.State != "sent" || !r.settings.Policy.Allows(c.Source) {
		return SendResult{}, errors.New("invalid_readback")
	}
	if err := r.preflight(ctx, c.Source); err != nil {
		return SendResult{}, err
	}
	var raw map[string]json.RawMessage
	if err := r.get(ctx, "/channels/"+c.Source.ConversationID+"/messages/"+receipt.MessageID, &raw); err != nil {
		return SendResult{}, err
	}
	id, code := validateAck(raw, c, r.settings.ExpectedBotID)
	actual := replyAckReceipt(raw, c)
	if id != receipt.MessageID || actual != receipt.OutputReceipt {
		return SendResult{}, errors.New("readback_mismatch")
	}
	return SendResult{State: "sent", MessageID: id, Code: code, OutputReceipt: actual}, nil
}

// BuildInteractionReplyBody shares verified immutable staged bytes but cannot
// apply an ordinary message reference or nonce to webhook interaction replies.
func BuildInteractionReplyBody(c Chunk, stateDir string) ([]byte, string, error) {
	raw, e := BuildReplyPayload(c)
	if e != nil {
		return nil, "", e
	}
	var v map[string]json.RawMessage
	if json.Unmarshal(raw, &v) != nil {
		return nil, "", errors.New("invalid_reply_payload")
	}
	delete(v, "message_reference")
	delete(v, "nonce")
	delete(v, "enforce_nonce")
	var flags int
	_ = json.Unmarshal(v["flags"], &flags)
	v["flags"], _ = json.Marshal(flags | 64) // Owner-scoped ephemeral interaction replies.
	raw, e = json.Marshal(v)
	if e != nil {
		return nil, "", e
	}
	return buildReplyBodyWithPayload(c, stateDir, raw)
}

// ReplyOutputAckReceipt validates then extracts the identity receipt for a
// transport with its own route/author/message validation, such as interactions.
func ReplyOutputAckReceipt(raw map[string]json.RawMessage, c Chunk) (ReplyOutputReceipt, error) {
	if e := ValidateReplyOutputAck(raw, c); e != nil {
		return "", e
	}
	return replyAckReceipt(raw, c), nil
}

// ReconcileReply reads one operator-selected Discord message and resolves only
// an uncertain chunk after full remote ACK verification. It performs GETs only,
// never posts or retries, and commits the attachment identity receipt atomically.
func (r *RESTClient) ReconcileReply(ctx context.Context, store *Store, replyID string, index int, messageID string) (SendResult, error) {
	if !Snowflake(messageID) {
		return SendResult{}, errors.New("invalid_readback")
	}
	c, _, err := store.replyVerificationChunk(replyID, index, true)
	if err != nil {
		return SendResult{}, err
	}
	if c.Source.ReplyKind == "interaction" {
		return SendResult{}, errors.New("interaction_readback_requires_token")
	}
	if err = r.preflight(ctx, c.Source); err != nil {
		return SendResult{}, err
	}
	var raw map[string]json.RawMessage
	if err = r.get(ctx, "/channels/"+c.Source.ConversationID+"/messages/"+messageID, &raw); err != nil {
		return SendResult{}, err
	}
	id, _ := validateAck(raw, c, r.settings.ExpectedBotID)
	if id != messageID {
		return SendResult{}, errors.New("readback_mismatch")
	}
	verified := SendResult{State: "sent", MessageID: id, Code: "operator_verified_output", OutputReceipt: replyAckReceipt(raw, c)}
	_, err = store.call(func(db *storeConn) (any, error) {
		return transact(db, func(db *storeConn) (any, error) {
			if err := persistReplyOutputReceipt(db, c, verified); err != nil {
				return nil, err
			}
			if err := changedOne(db.Exec("UPDATE chunks SET state='sent',message_id=?,code='operator_verified_output' WHERE reply_id=? AND idx=? AND state='uncertain'", messageID, replyID, index)); err != nil {
				return nil, err
			}
			memoryBestEffort(db, func() error { return rememberSentDB(db, replyID, index) })
			if err := cancelStaleReplyAfterResultDB(db, c); err != nil {
				return nil, err
			}
			return nil, nil
		})
	})
	if err != nil {
		return SendResult{}, err
	}
	return verified, nil
}
