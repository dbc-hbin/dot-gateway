package bridge

// Explicit, request-bound message mutations. Nothing in this module is dispatched
// from Gateway events or from a queue. A durable attempt is consumed BEFORE I/O.
import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"unicode/utf8"
)

const permissionPinMessages uint64 = 1 << 51

// MessageOperationSpec is deliberately not an HTTP passthrough. Text edits change
// only content; attachment identity, flags, embeds and components are preserved.
type MessageOperationSpec struct {
	Version   int    `json:"version"`
	Key       string `json:"key"`
	Action    string `json:"action"`
	ChannelID string `json:"channel_id"`
	MessageID string `json:"message_id"`
	Text      string `json:"text,omitempty"`
	Emoji     string `json:"emoji,omitempty"`
}
type MessageOperation struct {
	ID            string               `json:"id"`
	RequestID     string               `json:"request_id"`
	Spec          MessageOperationSpec `json:"spec"`
	State         string               `json:"state"`
	Code          string               `json:"code,omitempty"`
	Attempts      int                  `json:"attempts"`
	Revision      int                  `json:"revision,omitempty"`
	generation    int64
	route         ReadRoute
	before, after operationSnapshot
}
type operationAttachment struct {
	ID          string `json:"id"`
	Filename    string `json:"filename"`
	Size        int64  `json:"size"`
	ContentType string `json:"content_type,omitempty"`
	Description string `json:"description,omitempty"`
}
type operationSnapshot struct {
	Content        string                `json:"content"`
	Author         string                `json:"author"`
	Bot            bool                  `json:"bot"`
	Type           int                   `json:"type"`
	Flags          int64                 `json:"flags"`
	Attachments    []operationAttachment `json:"attachments"`
	EmbedsHash     string                `json:"embeds_hash"`
	ComponentsHash string                `json:"components_hash"`
	ReferenceHash  string                `json:"reference_hash"`
	Pinned         bool                  `json:"pinned"`
	OwnReaction    bool                  `json:"own_reaction"`
}

func operationJSON(v any) string { b, _ := json.Marshal(v); return string(b) }
func operationHash(v any) string {
	b := sha256.Sum256([]byte(operationJSON(v)))
	return hex.EncodeToString(b[:])
}
func validOperationSpec(v MessageOperationSpec) bool {
	if v.Version != 1 || len(v.Key) < 1 || len(v.Key) > 80 || !Snowflake(v.ChannelID) || !Snowflake(v.MessageID) {
		return false
	}
	for _, c := range v.Key {
		if c != '-' && c != '_' && (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') && (c < '0' || c > '9') {
			return false
		}
	}
	switch v.Action {
	case "edit_text":
		return v.Emoji == "" && utf8.ValidString(v.Text) && trimText(v.Text) != "" && !strings.HasSuffix(v.Text, "\n") && TextUnits(v.Text) <= 2000
	case "add_reaction", "remove_own_reaction":
		if v.Text != "" || v.Emoji == "" || len(v.Emoji) > 128 || !utf8.ValidString(v.Emoji) || strings.ContainsAny(v.Emoji, "/\\?#%\r\n\t ") {
			return false
		}
		// Custom emoji use the documented name:id wire format, never arbitrary paths.
		if strings.Contains(v.Emoji, ":") {
			parts := strings.Split(v.Emoji, ":")
			if len(parts) != 2 || !Snowflake(parts[1]) || parts[0] == "" {
				return false
			}
			for _, c := range parts[0] {
				if c != '_' && (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') && (c < '0' || c > '9') {
					return false
				}
			}
		}
		return true
	case "pin", "unpin":
		return v.Text == "" && v.Emoji == ""
	}
	return false
}
func ParseMessageOperation(raw []byte) (MessageOperationSpec, error) {
	var v MessageOperationSpec
	if len(raw) > 16384 || strictReplyJSON(raw, &v) != nil || !validOperationSpec(v) {
		return v, errors.New("invalid_message_operation")
	}
	return v, nil
}
func initMessageOperations(db *storeConn) error {
	_, err := db.Exec(`CREATE TABLE IF NOT EXISTS message_operations(
 id TEXT PRIMARY KEY,request_id TEXT NOT NULL REFERENCES inbound(id),operation_key TEXT NOT NULL,spec TEXT NOT NULL,
 channel TEXT NOT NULL,message TEXT NOT NULL,route TEXT NOT NULL,before_snapshot TEXT NOT NULL,after_snapshot TEXT NOT NULL,
 state TEXT NOT NULL,code TEXT NOT NULL DEFAULT '',attempts INTEGER NOT NULL DEFAULT 0 CHECK(attempts IN(0,1)),revision INTEGER NOT NULL DEFAULT 0,generation INTEGER NOT NULL,
 created REAL NOT NULL,updated REAL NOT NULL,UNIQUE(request_id,operation_key));
 CREATE INDEX IF NOT EXISTS inbound_read_route_lookup ON inbound(platform,json_extract(envelope,'$.conversation_id'),json_extract(envelope,'$.sender_id'),created DESC) WHERE json_valid(envelope);
 CREATE TABLE IF NOT EXISTS message_operation_recovery_fences(id TEXT PRIMARY KEY,channel TEXT NOT NULL,message TEXT NOT NULL,hold_target INTEGER NOT NULL CHECK(hold_target IN(0,1)),action TEXT NOT NULL,state TEXT NOT NULL,attempts INTEGER NOT NULL,created REAL NOT NULL);
 CREATE TABLE IF NOT EXISTS message_edit_recovery_projections(channel TEXT NOT NULL,message TEXT NOT NULL,revision INTEGER NOT NULL,PRIMARY KEY(channel,message));
 CREATE TABLE IF NOT EXISTS message_operation_targets(channel TEXT NOT NULL,message TEXT NOT NULL,guild TEXT NOT NULL,generation INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(channel,message));
 CREATE UNIQUE INDEX IF NOT EXISTS message_operation_target_hold ON message_operations(channel,message) WHERE state IN('prepared','uncertain');
 CREATE TABLE IF NOT EXISTS message_edit_revisions(channel TEXT NOT NULL,message TEXT NOT NULL,revision INTEGER NOT NULL,operation_id TEXT NOT NULL UNIQUE,snapshot TEXT NOT NULL,created REAL NOT NULL,PRIMARY KEY(channel,message,revision));
 CREATE TABLE IF NOT EXISTS message_edit_projection(channel TEXT NOT NULL,message TEXT NOT NULL,revision INTEGER NOT NULL,state TEXT NOT NULL,operation_id TEXT NOT NULL,memory_key TEXT NOT NULL,PRIMARY KEY(channel,message));
 CREATE TRIGGER IF NOT EXISTS message_operation_immutable BEFORE UPDATE OF id,request_id,operation_key,spec,channel,message,route,before_snapshot,after_snapshot,generation ON message_operations BEGIN SELECT RAISE(ABORT,'immutable_message_operation'); END;
 CREATE TRIGGER IF NOT EXISTS message_revision_immutable BEFORE UPDATE ON message_edit_revisions BEGIN SELECT RAISE(ABORT,'immutable_message_revision'); END;`)
	return err
}
func operationClaimDB(db *storeConn, p StorePolicy, id, claim string) (Envelope, error) {
	return memoryClaimDB(db, p, id, claim)
}
func loadOperationDB(db *storeConn, id string) (MessageOperation, error) {
	var o MessageOperation
	var spec, route, before, after string
	err := db.QueryRow(`SELECT id,request_id,spec,route,before_snapshot,after_snapshot,state,code,attempts,revision,generation FROM message_operations WHERE id=?`, id).Scan(&o.ID, &o.RequestID, &spec, &route, &before, &after, &o.State, &o.Code, &o.Attempts, &o.Revision, &o.generation)
	if err != nil {
		return o, err
	}
	if json.Unmarshal([]byte(spec), &o.Spec) != nil || json.Unmarshal([]byte(route), &o.route) != nil || json.Unmarshal([]byte(before), &o.before) != nil || json.Unmarshal([]byte(after), &o.after) != nil || !validOperationSpec(o.Spec) {
		return o, errors.New("invalid_operation_ledger")
	}
	return o, nil
}
func (s *Store) MessageOperation(id string) (MessageOperation, error) {
	v, e := s.call(func(db *storeConn) (any, error) { return loadOperationDB(db, id) })
	if e != nil {
		return MessageOperation{}, e
	}
	return v.(MessageOperation), nil
}
func operationID(request, key string) string { return operationHash([]string{request, key}) }

func readOperationSnapshot(raw json.RawMessage, route ReadRoute, spec MessageOperationSpec) (operationSnapshot, error) {
	var out operationSnapshot
	m, err := decodeReadMessage(raw, route)
	if err != nil {
		return out, err
	}
	if m.ID != spec.MessageID || m.WebhookID != "" || (int(m.Type) != 0 && int(m.Type) != 19) {
		return out, errors.New("operation_target_mismatch")
	}
	var fields map[string]json.RawMessage
	_ = json.Unmarshal(raw, &fields)
	if _, ok := fields["pinned"]; !ok {
		return out, errors.New("operation_target_incomplete")
	}
	out.Content, out.Author, out.Bot, out.Type = m.Content, m.Author.ID, m.Author.Bot, int(m.Type)
	if f, ok := fields["flags"]; ok {
		if string(f) == "null" || json.Unmarshal(f, &out.Flags) != nil || out.Flags < 0 {
			return out, errors.New("operation_target_flags")
		}
	}
	if string(fields["pinned"]) == "null" || json.Unmarshal(fields["pinned"], &out.Pinned) != nil {
		return out, errors.New("operation_target_incomplete")
	}
	out.Attachments = []operationAttachment{}
	if json.Unmarshal(fields["attachments"], &out.Attachments) != nil || len(out.Attachments) > 10 {
		return out, errors.New("operation_target_attachments")
	}
	seen := map[string]bool{}
	for _, a := range out.Attachments {
		if !Snowflake(a.ID) || seen[a.ID] || a.Filename == "" || a.Size < 0 {
			return out, errors.New("operation_target_attachments")
		}
		seen[a.ID] = true
	}

	for _, pair := range []struct {
		key string
		dst *string
	}{{"embeds", &out.EmbedsHash}, {"components", &out.ComponentsHash}, {"message_reference", &out.ReferenceHash}} {
		var v any
		val := fields[pair.key]
		if len(val) == 0 {
			val = []byte("null")
		}
		if json.Unmarshal(val, &v) != nil {
			return out, errors.New("operation_target_incomplete")
		}
		if pair.key == "embeds" {
			v = canonicalOperationEmbeds(v)
		}
		*pair.dst = operationHash(v)
	}
	if spec.Emoji != "" {
		var reactions []struct {
			Me    *bool `json:"me"`
			Emoji struct {
				ID   string `json:"id"`
				Name string `json:"name"`
			} `json:"emoji"`
		}
		if val, ok := fields["reactions"]; ok && (string(val) == "null" || json.Unmarshal(val, &reactions) != nil) {
			return out, errors.New("operation_reaction_invalid")
		}
		seenReactions := map[string]bool{}
		for _, reaction := range reactions {
			if reaction.Me == nil || reaction.Emoji.Name == "" || (reaction.Emoji.ID != "" && !Snowflake(reaction.Emoji.ID)) {
				return out, errors.New("operation_reaction_invalid")
			}
			emoji := reaction.Emoji.Name
			if reaction.Emoji.ID != "" {
				emoji += ":" + reaction.Emoji.ID
			}
			identity := "unicode:" + reaction.Emoji.Name
			if reaction.Emoji.ID != "" {
				identity = "id:" + reaction.Emoji.ID
			}
			if seenReactions[identity] {
				return out, errors.New("operation_reaction_invalid")
			}
			seenReactions[identity] = true
			match := emoji == spec.Emoji
			if _, id, custom := strings.Cut(spec.Emoji, ":"); custom {
				match = reaction.Emoji.ID == id
			}
			if match {
				out.OwnReaction = *reaction.Me
			}
		}
	}
	return out, nil
}
func sameOperationMeaning(a, b operationSnapshot) bool {
	a.Pinned = false
	b.Pinned = false
	a.OwnReaction = false
	b.OwnReaction = false
	return operationJSON(a) == operationJSON(b)
}
func sameOperationTarget(a, b operationSnapshot, spec MessageOperationSpec) bool {
	if !sameOperationMeaning(a, b) {
		return false
	}
	switch spec.Action {
	case "pin", "unpin":
		return a.Pinned == b.Pinned
	case "add_reaction", "remove_own_reaction":
		return a.OwnReaction == b.OwnReaction
	}
	return true
}
func desiredSnapshot(before operationSnapshot, spec MessageOperationSpec) operationSnapshot {
	after := before
	switch spec.Action {
	case "edit_text":
		after.Content = spec.Text
	case "pin":
		after.Pinned = true
	case "unpin":
		after.Pinned = false
	case "add_reaction":
		after.OwnReaction = true
	case "remove_own_reaction":
		after.OwnReaction = false
	}
	return after
}
func (r *RESTClient) readOperationTarget(ctx context.Context, route ReadRoute, spec MessageOperationSpec) (operationSnapshot, json.RawMessage, error) {
	var raw json.RawMessage
	err := r.get(ctx, "/channels/"+spec.ChannelID+"/messages/"+spec.MessageID, &raw)
	if err != nil {
		return operationSnapshot{}, nil, err
	}
	v, err := readOperationSnapshot(raw, route, spec)
	return v, raw, err
}
func (r *RESTClient) operationRoute(ctx context.Context, route ReadRoute, spec MessageOperationSpec) error {
	if err := r.validateReadRoute(ctx, route); err != nil {
		return err
	}
	if route.Kind == "dm" {
		return nil
	}
	c, err := r.Channel(ctx, route.ChannelID)
	if err != nil {
		return err
	}
	extra := uint64(0)
	switch spec.Action {
	case "pin", "unpin":
		extra = permissionPinMessages
	case "add_reaction":
		extra = permissionAddReactions
	}
	if route.Kind == "guild_thread" {
		return r.validateThreadRoute(ctx, c, route.envelope(), extra)
	}
	if c.PermissionOverwrites == nil {
		return errors.New("operation_permissions_incomplete")
	}
	bits, err := r.freshThreadPermissions(ctx, c)
	if err != nil {
		return err
	}
	need := permissionViewChannel | permissionReadHistory | extra
	if bits&need != need {
		return errors.New("operation_permissions_missing")
	}
	// Refresh channel overwrites after the member/roles reads and compute again.
	c, err = r.Channel(ctx, route.ChannelID)
	if err != nil {
		return err
	}
	if err = r.ValidateChannel(c, route.envelope()); err != nil {
		return err
	}
	if c.PermissionOverwrites == nil {
		return errors.New("operation_permissions_incomplete")
	}
	bits, err = r.freshThreadPermissions(ctx, c)
	if err != nil {
		return err
	}
	if bits&need != need {
		return errors.New("operation_permissions_missing")
	}
	return nil
}
func operationOwnedChunkDB(db *storeConn, channel, message string) (Chunk, SendResult, error) {
	var c Chunk
	var receipt SendResult
	var raw string
	err := db.QueryRow(`SELECT c.reply_id,c.idx,c.text,i.envelope,`+chunkReplyOutputSQL+`,c.state,c.message_id,COALESCE(rc.receipt,''),COALESCE(c.code,'') FROM chunks c JOIN replies r ON r.id=c.reply_id JOIN inbound i ON i.id=r.inbound_id LEFT JOIN reply_output_receipts rc ON rc.reply_id=c.reply_id AND rc.idx=c.idx WHERE c.state='sent' AND c.message_id=? AND json_extract(i.envelope,'$.conversation_id')=?`, message, channel).Scan(&c.ReplyID, &c.Index, &c.Text, &raw, &c.Output, &receipt.State, &receipt.MessageID, &receipt.OutputReceipt, &receipt.Code)
	if err != nil {
		return c, receipt, errors.New("operation_target_not_ledger_owned")
	}
	if json.Unmarshal([]byte(raw), &c.Source) != nil || c.Source.ReplyKind == "interaction" {
		return c, receipt, errors.New("operation_target_not_ledger_owned")
	}
	return c, receipt, nil
}
func (s *Store) verifyOperationEditSnapshot(spec MessageOperationSpec, snapshot operationSnapshot, raw json.RawMessage, bot string) error {
	if snapshot.Author != bot || !snapshot.Bot || snapshot.Flags&(64|128|8192|32768) != 0 {
		return errors.New("operation_edit_target_invalid")
	}
	_, err := s.call(func(db *storeConn) (any, error) {
		c, receipt, err := operationOwnedChunkDB(db, spec.ChannelID, spec.MessageID)
		if err != nil {
			return nil, err
		}
		var state, previous string
		var revision int
		err = db.QueryRow(`SELECT p.state,p.revision,r.snapshot FROM message_edit_projection p LEFT JOIN message_edit_revisions r ON r.channel=p.channel AND r.message=p.message AND r.revision=p.revision WHERE p.channel=? AND p.message=?`, spec.ChannelID, spec.MessageID).Scan(&state, &revision, &previous)
		if err == nil {
			var old operationSnapshot
			if state != "verified" || json.Unmarshal([]byte(previous), &old) != nil || !sameOperationMeaning(old, snapshot) {
				return nil, errors.New("operation_edit_projection_unknown")
			}
			return nil, nil
		}
		if err != sql.ErrNoRows {
			return nil, err
		}
		var fields map[string]json.RawMessage
		_ = json.Unmarshal(raw, &fields)
		id, _ := validateAck(fields, c, bot)
		if id != spec.MessageID || replyAckReceipt(fields, c) != receipt.OutputReceipt {
			return nil, errors.New("operation_original_target_changed")
		}
		return nil, nil
	})
	return err
}

// ExecuteMessageOperation is synchronous and explicit. Repeating the same key
// returns evidence; it NEVER repeats an HTTP mutation, including after a crash.
func (r *RESTClient) ExecuteMessageOperation(ctx context.Context, s *Store, request, claim string, spec MessageOperationSpec) (MessageOperation, error) {
	if !validOperationSpec(spec) {
		return MessageOperation{}, errors.New("invalid_message_operation")
	}
	id := operationID(request, spec.Key)
	if err := s.operationRecoveryAllowed(id, spec.ChannelID, spec.MessageID); err != nil {
		return MessageOperation{}, err
	}
	source, err := s.ClaimedEnvelope(request, claim)
	if err != nil || !isMessageSource(source) {
		return MessageOperation{}, ErrClaim
	}
	if old, e := s.MessageOperation(id); e == nil {
		if operationJSON(old.Spec) != operationJSON(spec) {
			return old, errors.New("operation_key_conflict")
		}
		if old.State == "prepared" {
			return r.dispatchMessageOperation(ctx, s, id, claim)
		}
		return old, nil
	} else if e != sql.ErrNoRows {
		return MessageOperation{}, e
	}
	route, err := r.ResolveReadRoute(ctx, s, spec.ChannelID)
	if err != nil {
		return MessageOperation{}, err
	}
	if err = r.VerifySourceBeforeSend(ctx, s, source); err != nil {
		return MessageOperation{}, err
	}
	if err = r.operationRoute(ctx, route, spec); err != nil {
		return MessageOperation{}, err
	}
	generation, err := s.bindOperationTarget(route, spec.MessageID)
	if err != nil {
		return MessageOperation{}, err
	}
	before, raw, err := r.readOperationTarget(ctx, route, spec)
	if err != nil {
		return MessageOperation{}, err
	}
	if spec.Action == "edit_text" {
		if err = s.verifyOperationEditSnapshot(spec, before, raw, r.settings.ExpectedBotID); err != nil {
			return MessageOperation{}, err
		}
	}
	after := desiredSnapshot(before, spec)
	v, err := s.call(func(db *storeConn) (any, error) {
		return transact(db, func(db *storeConn) (any, error) {
			if err := operationRecoveryAllowedDB(db, id, spec.ChannelID, spec.MessageID); err != nil {
				return nil, err
			}
			if _, err := operationClaimDB(db, s.policy, request, claim); err != nil {
				return nil, err
			}
			if old, err := loadOperationDB(db, id); err == nil {
				if operationJSON(old.Spec) != operationJSON(spec) {
					return nil, errors.New("operation_key_conflict")
				}
				return false, nil
			} else if err != sql.ErrNoRows {
				return nil, err
			}
			if err := requireOperationGenerationDB(db, spec.ChannelID, spec.MessageID, generation); err != nil {
				return nil, err
			}
			var held int
			if err := db.QueryRow(`SELECT count(*) FROM message_operations WHERE channel=? AND message=? AND state IN('prepared','uncertain')`, spec.ChannelID, spec.MessageID).Scan(&held); err != nil {
				return nil, err
			}
			if held != 0 {
				return nil, errors.New("operation_target_held")
			}
			// Even a confirmed earlier operation cannot authorize assumptions while its
			// edit projection has become unknown after an external update.
			var unknown int
			if err := db.QueryRow(`SELECT count(*) FROM message_edit_projection WHERE channel=? AND message=? AND state!='verified'`, spec.ChannelID, spec.MessageID).Scan(&unknown); err != nil {
				return nil, err
			}
			if unknown != 0 {
				return nil, errors.New("operation_edit_projection_unknown")
			}
			_, err := db.Exec(`INSERT INTO message_operations(id,request_id,operation_key,spec,channel,message,route,before_snapshot,after_snapshot,state,generation,created,updated) VALUES(?,?,?,?,?,?,?,?,?,'prepared',?,?,?)`, id, request, spec.Key, operationJSON(spec), spec.ChannelID, spec.MessageID, operationJSON(route), operationJSON(before), operationJSON(after), generation, epoch(), epoch())
			return true, err
		})
	})
	if err != nil {
		return MessageOperation{}, err
	}
	if !v.(bool) {
		return s.MessageOperation(id)
	}
	return r.dispatchMessageOperation(ctx, s, id, claim)
}
func (r *RESTClient) dispatchMessageOperation(ctx context.Context, s *Store, id, claim string) (MessageOperation, error) {
	r.sendMu.Lock()
	defer r.sendMu.Unlock()
	o, err := s.MessageOperation(id)
	if err != nil {
		return o, err
	}
	if o.State != "prepared" {
		return o, nil
	}
	if err := s.operationRecoveryAllowed(id, o.Spec.ChannelID, o.Spec.MessageID); err != nil {
		return o, err
	}
	source, err := s.ClaimedEnvelope(o.RequestID, claim)
	if err != nil {
		return s.failPreparedMessageOperation(id, "claim_expired_before_attempt")
	}
	check := func(checkCtx context.Context) error {
		if _, err := s.ClaimedEnvelope(o.RequestID, claim); err != nil {
			return err
		}
		if err := r.VerifySourceBeforeSend(checkCtx, s, source); err != nil {
			return err
		}
		if err := r.operationRoute(checkCtx, o.route, o.Spec); err != nil {
			return err
		}
		current, _, err := r.readOperationTarget(checkCtx, o.route, o.Spec)
		if err != nil {
			return err
		}
		if err := r.operationRoute(checkCtx, o.route, o.Spec); err != nil {
			return err
		}
		if !s.operationGenerationCurrent(o) {
			return errors.New("operation_target_invalidated")
		}
		if !sameOperationTarget(current, o.before, o.Spec) {
			return errors.New("operation_target_changed")
		}
		return nil
	}
	if err = check(ctx); err != nil {
		return s.failPreparedMessageOperation(id, "preflight_rejected")
	}
	// Transactionally invalidate approval bindings before an edit might land. The
	// uncertain state and attempt fence survive failures at any subsequent line.
	_, err = s.call(func(db *storeConn) (any, error) {
		return transact(db, func(db *storeConn) (any, error) {
			if _, err := operationClaimDB(db, s.policy, o.RequestID, claim); err != nil {
				return nil, err
			}
			if err := requireOperationGenerationDB(db, o.Spec.ChannelID, o.Spec.MessageID, o.generation); err != nil {
				return nil, err
			}
			res, err := db.Exec(`UPDATE message_operations SET state='uncertain',code='attempt_reserved',attempts=1,updated=? WHERE id=? AND state='prepared' AND attempts=0`, epoch(), id)
			if err != nil {
				return nil, err
			}
			n, _ := res.RowsAffected()
			if n != 1 {
				return nil, errors.New("operation_already_attempted")
			}
			if o.Spec.Action == "edit_text" {
				if err = invalidateOperationBindingsDB(db, o.Spec.ChannelID, o.Spec.MessageID); err != nil {
					return nil, err
				}
				if err = baselineEditProjectionDB(db, o); err != nil {
					return nil, err
				}
			}
			return nil, nil
		})
	})
	if err != nil {
		return s.failPreparedMessageOperation(id, "reservation_rejected")
	}
	readbackCtx := ctx
	method, path, body := operationRequest(o)
	// The shared write-budget loop refreshes this proof after EVERY rate wait.
	ctx = context.WithValue(ctx, threadWriteContextKey{}, check)
	// Recheck active claim immediately before Do as well, after the final GET.
	ctx = context.WithValue(ctx, sendGuardContextKey{}, func() bool {
		_, err := s.ClaimedEnvelope(o.RequestID, claim)
		return err == nil && s.operationGenerationCurrent(o)
	})
	measured, measurement := newRequestMeasurement(ctx)
	resp, requestErr := r.request(measured, method, path, body)
	diag := measurement.finish()
	if requestErr != nil {
		if !diag.Attempted {
			return s.finishMessageOperation(id, "failed", "not_attempted", false)
		}
		return s.finishMessageOperation(id, "uncertain", "transport_uncertain", false)
	}
	if resp == nil {
		return s.finishMessageOperation(id, "uncertain", "missing_response", false)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		io.Copy(io.Discard, io.LimitReader(resp.Body, 1048577))
		if resp.StatusCode == 400 || resp.StatusCode == 401 || resp.StatusCode == 403 || resp.StatusCode == 404 || resp.StatusCode == 405 || resp.StatusCode == 429 {
			return s.finishMessageOperation(id, "failed", fmt.Sprintf("http_%d", resp.StatusCode), false)
		}
		return s.finishMessageOperation(id, "uncertain", "http_uncertain", false)
	}
	// An ACK alone does not establish current content/attachment/pin/reaction state.
	io.Copy(io.Discard, io.LimitReader(resp.Body, 1048577))
	resp.Body.Close()
	verifiedGeneration, err := r.verifyOperationAfter(readbackCtx, s, o)
	if err != nil {
		return s.finishMessageOperation(id, "uncertain", "readback_unverified", false)
	}
	return s.finishMessageOperation(id, "verified", "readback_verified", true, verifiedGeneration)
}
func operationRequest(o MessageOperation) (string, string, []byte) {
	spec := o.Spec
	path := "/channels/" + spec.ChannelID + "/messages/" + spec.MessageID
	switch spec.Action {
	case "edit_text":
		attachments := []map[string]string{}
		for _, a := range o.before.Attachments {
			attachments = append(attachments, map[string]string{"id": a.ID})
		}
		body, _ := json.Marshal(map[string]any{"content": spec.Text, "allowed_mentions": map[string]any{"parse": []string{}, "users": []string{}, "roles": []string{}, "replied_user": false}, "attachments": attachments, "flags": o.before.Flags})
		return http.MethodPatch, path, body
	case "pin":
		return http.MethodPut, "/channels/" + spec.ChannelID + "/messages/pins/" + spec.MessageID, nil
	case "unpin":
		return http.MethodDelete, "/channels/" + spec.ChannelID + "/messages/pins/" + spec.MessageID, nil
	case "add_reaction":
		return http.MethodPut, path + "/reactions/" + url.PathEscape(spec.Emoji) + "/@me", nil
	default:
		return http.MethodDelete, path + "/reactions/" + url.PathEscape(spec.Emoji) + "/@me", nil
	}
}
func (r *RESTClient) verifyOperationAfter(ctx context.Context, s *Store, o MessageOperation) (int64, error) {
	generation, err := s.operationTargetGeneration(o.Spec.ChannelID, o.Spec.MessageID)
	if err != nil {
		return 0, err
	}
	if err := r.operationRoute(ctx, o.route, o.Spec); err != nil {
		return 0, err
	}
	actual, _, err := r.readOperationTarget(ctx, o.route, o.Spec)
	if err != nil {
		return 0, err
	}
	if !sameOperationTarget(actual, o.after, o.Spec) {
		return 0, errors.New("operation_readback_mismatch")
	}
	return generation, nil
}

// Reconciliation is a separate explicit read-only network action. It can prove
// the exact desired result, never infer non-application or reset an attempt.
func (r *RESTClient) ReconcileMessageOperation(ctx context.Context, s *Store, request, claim, id string, explicit bool) (MessageOperation, error) {
	if !explicit {
		return MessageOperation{}, errors.New("explicit_operation_reconciliation_required")
	}
	e, err := s.ClaimedEnvelope(request, claim)
	if err != nil || !isMessageSource(e) {
		return MessageOperation{}, ErrClaim
	}
	o, err := s.MessageOperation(id)
	if err != nil {
		return o, err
	}
	if err := s.operationRecoveryAllowed(id, o.Spec.ChannelID, o.Spec.MessageID); err != nil {
		return o, err
	}
	if o.State != "uncertain" && o.State != "verified" {
		return o, errors.New("operation_not_reconcilable")
	}
	if err = r.VerifySourceBeforeSend(ctx, s, e); err != nil {
		return o, err
	}
	route, err := r.ResolveReadRoute(ctx, s, o.Spec.ChannelID)
	if err != nil || route != o.route {
		return o, errors.New("operation_route_changed")
	}
	verifiedGeneration, err := r.verifyOperationAfter(ctx, s, o)
	if err != nil {
		return o, err
	}
	if _, err = s.ClaimedEnvelope(request, claim); err != nil {
		return o, err
	}
	return s.finishMessageOperation(id, "verified", "explicit_readback_verified", true, verifiedGeneration)
}

// New reaction/question bindings can refer to a verified edit revision. Old
// bindings carry a different revision digest and remain unusable permanently.
func (r *RESTClient) verifyEditedControlTarget(ctx context.Context, e Envelope, target sentControlTarget) (bool, error) {
	v, err := r.controlStore.call(func(db *storeConn) (any, error) {
		snapshot, _, err := verifiedOperationControlSnapshotDB(db, e.ConversationID, target.Message)
		return snapshot, err
	})
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	route, err := r.ResolveReadRoute(ctx, r.controlStore, e.ConversationID)
	if err != nil {
		return false, err
	}
	spec := MessageOperationSpec{Version: 1, Key: "readback", Action: "edit_text", ChannelID: e.ConversationID, MessageID: target.Message, Text: target.Text}
	current, _, err := r.readOperationTarget(ctx, route, spec)
	if err != nil {
		return false, err
	}
	if !sameOperationMeaning(current, v.(operationSnapshot)) {
		return false, errors.New("operation_edit_projection_unknown")
	}
	fresh, err := r.controlStore.sentControlTarget(e.ConversationID, target.Message)
	if err != nil || fresh.Revision != target.Revision {
		return false, errors.New("operation_edit_projection_unknown")
	}
	return true, nil
}

// Discord regenerates proxy URLs and attachment signatures. Strip only those
// known transport fields; preserve every authored embed field and attachment ID.
func canonicalOperationEmbeds(value any) any {
	switch v := value.(type) {
	case []any:
		for i := range v {
			v[i] = canonicalOperationEmbeds(v[i])
		}
		return v
	case map[string]any:
		for key, child := range v {
			if key == "proxy_url" || key == "proxy_icon_url" {
				delete(v, key)
				continue
			}
			if key == "url" || key == "icon_url" {
				if raw, ok := child.(string); ok {
					u, err := url.Parse(raw)
					if err == nil && u.Scheme == "https" && u.User == nil && (u.Host == "cdn.discordapp.com" || u.Host == "media.discordapp.net") && strings.HasPrefix(u.Path, "/attachments/") {
						parts := strings.Split(strings.TrimPrefix(u.Path, "/attachments/"), "/")
						if len(parts) == 3 && Snowflake(parts[0]) && Snowflake(parts[1]) {
							q := u.Query()
							q.Del("ex")
							q.Del("is")
							q.Del("hm")
							u.RawQuery = q.Encode()
							v[key] = u.String()
							continue
						}
					}
				}
			}
			v[key] = canonicalOperationEmbeds(child)
		}
		return v
	default:
		return value
	}
}
