package main

// Allowlisted, tokenless control metadata. This never snapshots command bodies,
// interaction tokens, question/modal bindings, output payloads, or file bytes.
import (
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

const phase3RowLimit = 100000
const registrationAttemptLimit = 4096

type OwnedCommand struct {
	Name string `json:"name"`
	ID   string `json:"id"`
}
type CommandAttempt struct {
	Key       string  `json:"key"`
	Name      string  `json:"name"`
	Action    string  `json:"action"`
	State     string  `json:"state"`
	CommandID string  `json:"command_id,omitempty"`
	Created   float64 `json:"created"`
}
type InteractionFence struct {
	ID        string  `json:"id"`
	Owner     string  `json:"owner"`
	Channel   string  `json:"channel"`
	Action    string  `json:"action"`
	State     string  `json:"state"`
	Created   float64 `json:"created"`
	InboundID string  `json:"inbound_id,omitempty"`
}
type ReactionFence struct {
	Channel  string `json:"channel"`
	Message  string `json:"message"`
	Owner    string `json:"owner"`
	Emoji    string `json:"emoji"`
	Present  bool   `json:"present"`
	Sequence int64  `json:"sequence"`
}
type TargetFence struct {
	Channel string `json:"channel"`
	Message string `json:"message"`
	Code    string `json:"code"`
}
type CancellationFence struct {
	ReplyID string  `json:"reply_id"`
	At      float64 `json:"at"`
	Code    string  `json:"code"`
}
type AttachmentMetadata struct {
	ID   string `json:"id"`
	Size int64  `json:"size"`
}
type RichReceiptMetadata struct {
	ReplyID string               `json:"reply_id"`
	Index   int                  `json:"index"`
	Files   []AttachmentMetadata `json:"files"`
}
type Phase3State struct {
	Version              int                   `json:"version"`
	ApplicationID        string                `json:"application_id"`
	GuildID              string                `json:"guild_id"`
	OwnerID              string                `json:"owner_id"`
	Commands             []OwnedCommand        `json:"commands,omitempty"`
	RegistrationAttempts []CommandAttempt      `json:"registration_attempts,omitempty"`
	Interactions         []InteractionFence    `json:"interactions,omitempty"`
	Reactions            []ReactionFence       `json:"reactions,omitempty"`
	TargetInvalidations  []TargetFence         `json:"target_invalidations,omitempty"`
	ReplyCancellations   []CancellationFence   `json:"reply_cancellations,omitempty"`
	CancelledRequests    []string              `json:"cancelled_requests,omitempty"`
	RichReceipts         []RichReceiptMetadata `json:"rich_receipts,omitempty"`
}

func commandName(name string) bool { return name == "ask" || name == "status" || name == "cancel" }
func safeReactionEmoji(emoji string) bool {
	if id, ok := strings.CutPrefix(emoji, "id:"); ok {
		return snowflakeID(id)
	}
	if emoji == "" || !utf8.ValidString(emoji) || len(emoji) > 128 || utf8.RuneCountInString(emoji) > 32 {
		return false
	}
	runes := []rune(emoji)
	// Subdivision flags: black flag, bounded lowercase tag letters, cancel tag.
	if len(runes) > 1 && runes[0] == '\U0001f3f4' && runes[1] >= 0xe0020 && runes[1] <= 0xe007f {
		if len(runes) < 4 || len(runes) > 18 || runes[len(runes)-1] != 0xe007f {
			return false
		}
		for _, r := range runes[1 : len(runes)-1] {
			if r < 0xe0061 || r > 0xe007a {
				return false
			}
		}
		return true
	}
	// The only permitted ASCII form is one complete Unicode keycap sequence.
	if len(runes) >= 2 && (runes[0] == '#' || runes[0] == '*' || runes[0] >= '0' && runes[0] <= '9') {
		return len(runes) == 2 && runes[1] == '\u20e3' || len(runes) == 3 && runes[1] == '\ufe0f' && runes[2] == '\u20e3'
	}
	hasSymbol := false
	for _, r := range runes {
		if unicode.IsSymbol(r) || r == 0x203c || r == 0x2049 || r == 0x3030 || r == 0x303d {
			hasSymbol = true
			continue
		}
		if unicode.IsMark(r) || r == '\u200d' {
			continue
		}
		return false
	}
	return hasSymbol
}
func splitReactionTransition(id string) (string, string, string, int64, bool) {
	pieces := strings.SplitN(id, ":", 3)
	if len(pieces) != 3 || !snowflakeID(pieces[0]) || !snowflakeID(pieces[1]) {
		return "", "", "", 0, false
	}
	at := strings.LastIndexByte(pieces[2], ':')
	if at < 1 {
		return "", "", "", 0, false
	}
	emoji, seq := pieces[2][:at], pieces[2][at+1:]
	n, err := strconv.ParseInt(seq, 10, 64)
	if err != nil || n < 1 || strconv.FormatInt(n, 10) != seq || !safeReactionEmoji(emoji) {
		return "", "", "", 0, false
	}
	return pieces[0], pieces[1], emoji, n, true
}
func validReactionTransition(id string) bool {
	_, _, _, _, ok := splitReactionTransition(id)
	return ok
}
func reactionTransitionOwner(key string) string {
	id, ok := strings.CutPrefix(key, "control:")
	if !ok {
		return ""
	}
	_, owner, _, _, valid := splitReactionTransition(id)
	if !valid {
		return ""
	}
	return owner
}
func phase3Table(tx *sql.Tx, table string) (bool, error) {
	var n int
	err := tx.QueryRow(`SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?`, table).Scan(&n)
	return n == 1, err
}
func phase3Read(tx *sql.Tx, table, query string, limit int, scan func(*sql.Rows) error) error {
	exists, err := phase3Table(tx, table)
	if err != nil || !exists {
		return err
	}
	rows, err := tx.Query(query + fmt.Sprintf(" LIMIT %d", limit+1))
	if err != nil {
		return err
	}
	defer rows.Close()
	n := 0
	for rows.Next() {
		n++
		if n > limit {
			return errors.New("phase3 metadata limit exceeded")
		}
		if err = scan(rows); err != nil {
			return errors.New("invalid phase3 source metadata")
		}
	}
	return rows.Err()
}
func snapshotPhase3(tx *sql.Tx, s *Snapshot) error {
	p := &Phase3State{Version: 1, ApplicationID: s.Operation.BotID, GuildID: s.Operation.GuildID, OwnerID: s.Operation.OwnerID}
	read := func(table, query string, limit int, scan func(*sql.Rows) error) error {
		return phase3Read(tx, table, query, limit, scan)
	}
	if err := read("control_command_ids", `SELECT guild,name,id FROM control_command_ids ORDER BY guild,name`, 3, func(r *sql.Rows) error {
		var guild string
		var v OwnedCommand
		if err := r.Scan(&guild, &v.Name, &v.ID); err != nil {
			return err
		}
		if guild != p.GuildID {
			return errors.New("control command guild mismatch")
		}
		p.Commands = append(p.Commands, v)
		return nil
	}); err != nil {
		return err
	}
	if err := read("control_registration_attempts", `SELECT key,name,action,state,command_id,created FROM control_registration_attempts ORDER BY key`, registrationAttemptLimit, func(r *sql.Rows) error {
		var v CommandAttempt
		if err := r.Scan(&v.Key, &v.Name, &v.Action, &v.State, &v.CommandID, &v.Created); err != nil {
			return err
		}
		p.RegistrationAttempts = append(p.RegistrationAttempts, v)
		return nil
	}); err != nil {
		return err
	}
	if err := read("control_interactions", `SELECT id,owner,channel,action,state,created,COALESCE(inbound_id,'') FROM control_interactions ORDER BY id`, phase3RowLimit, func(r *sql.Rows) error {
		var v InteractionFence
		if err := r.Scan(&v.ID, &v.Owner, &v.Channel, &v.Action, &v.State, &v.Created, &v.InboundID); err != nil {
			return err
		}
		p.Interactions = append(p.Interactions, v)
		return nil
	}); err != nil {
		return err
	}
	if err := read("control_reactions", `SELECT channel,message,owner,emoji,present,sequence FROM control_reactions ORDER BY channel,message,owner,emoji`, phase3RowLimit, func(r *sql.Rows) error {
		var v ReactionFence
		if err := r.Scan(&v.Channel, &v.Message, &v.Owner, &v.Emoji, &v.Present, &v.Sequence); err != nil {
			return err
		}
		p.Reactions = append(p.Reactions, v)
		return nil
	}); err != nil {
		return err
	}
	if err := read("control_target_invalidations", `SELECT channel,message,code FROM control_target_invalidations ORDER BY channel,message`, phase3RowLimit, func(r *sql.Rows) error {
		var v TargetFence
		if err := r.Scan(&v.Channel, &v.Message, &v.Code); err != nil {
			return err
		}
		p.TargetInvalidations = append(p.TargetInvalidations, v)
		return nil
	}); err != nil {
		return err
	}
	if err := read("reply_cancellations", `SELECT reply_id,cancelled_at,code FROM reply_cancellations ORDER BY reply_id`, phase3RowLimit, func(r *sql.Rows) error {
		var v CancellationFence
		if err := r.Scan(&v.ReplyID, &v.At, &v.Code); err != nil {
			return err
		}
		p.ReplyCancellations = append(p.ReplyCancellations, v)
		return nil
	}); err != nil {
		return err
	}
	cancelled := map[string]bool{}
	if err := read("inbound", `SELECT id FROM inbound WHERE state='cancelled' ORDER BY id`, phase3RowLimit, func(r *sql.Rows) error {
		var id string
		if err := r.Scan(&id); err != nil {
			return err
		}
		cancelled[id] = true
		return nil
	}); err != nil {
		return err
	}
	if err := read("recovery_cancelled_requests", `SELECT inbound_id FROM recovery_cancelled_requests ORDER BY inbound_id`, phase3RowLimit, func(r *sql.Rows) error {
		var id string
		if err := r.Scan(&id); err != nil {
			return err
		}
		cancelled[id] = true
		return nil
	}); err != nil {
		return err
	}
	for id := range cancelled {
		p.CancelledRequests = append(p.CancelledRequests, id)
	}
	sort.Strings(p.CancelledRequests)
	// Retain returned attachment identity/size only. Filename, description, MIME,
	// receipt JSON, embed payload and URLs are never exported or used as proof.
	receipts := map[string]RichReceiptMetadata{}
	if err := read("reply_output_receipts", `SELECT reply_id,idx,json_valid(receipt),CASE WHEN json_valid(receipt) THEN json_type(receipt) ELSE '' END,CASE WHEN json_valid(receipt) THEN json_array_length(receipt) ELSE -1 END FROM reply_output_receipts ORDER BY reply_id,idx`, phase3RowLimit, func(r *sql.Rows) error {
		var v RichReceiptMetadata
		var valid int
		var kind string
		var count int
		if err := r.Scan(&v.ReplyID, &v.Index, &valid, &kind, &count); err != nil {
			return err
		}
		if valid != 1 || kind != "array" || count < 0 || count > 10 {
			return errors.New("invalid rich receipt metadata")
		}
		v.Files = []AttachmentMetadata{}
		receipts[fmt.Sprint(v.ReplyID, "/", v.Index)] = v
		return nil
	}); err != nil {
		return err
	}
	keys := []string{}
	for key := range receipts {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		v := receipts[key]
		rows, err := tx.Query(`SELECT j.type,json_type(j.value,'$.id'),json_extract(j.value,'$.id'),json_type(j.value,'$.size'),json_extract(j.value,'$.size') FROM reply_output_receipts r,json_each(r.receipt) j WHERE r.reply_id=? AND r.idx=? ORDER BY j.key`, v.ReplyID, v.Index)
		if err != nil {
			return err
		}
		for rows.Next() {
			var object, idType, sizeType string
			var f AttachmentMetadata
			if err = rows.Scan(&object, &idType, &f.ID, &sizeType, &f.Size); err != nil {
				rows.Close()
				return errors.New("invalid rich receipt metadata")
			}
			if object != "object" || idType != "text" || sizeType != "integer" {
				rows.Close()
				return errors.New("invalid rich receipt metadata")
			}
			v.Files = append(v.Files, f)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		receipts[key] = v
	}
	if err := read("recovery_rich_receipts", `SELECT reply_id,idx,metadata FROM recovery_rich_receipts ORDER BY reply_id,idx`, phase3RowLimit, func(r *sql.Rows) error {
		var v RichReceiptMetadata
		var raw string
		if err := r.Scan(&v.ReplyID, &v.Index, &raw); err != nil {
			return err
		}
		if strict([]byte(raw), &v.Files) != nil {
			return errors.New("invalid inert rich receipt metadata")
		}
		key := fmt.Sprint(v.ReplyID, "/", v.Index)
		if _, ok := receipts[key]; !ok {
			receipts[key] = v
		}
		return nil
	}); err != nil {
		return err
	}
	keys = nil
	for key := range receipts {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		p.RichReceipts = append(p.RichReceipts, receipts[key])
	}
	if len(p.Commands)+len(p.RegistrationAttempts)+len(p.Interactions)+len(p.Reactions)+len(p.TargetInvalidations)+len(p.ReplyCancellations)+len(p.CancelledRequests)+len(p.RichReceipts) > 0 {
		s.Phase3 = p
	}
	return validatePhase3(*s)
}
func validatePhase3(s Snapshot) error {
	p := s.Phase3
	if p == nil {
		return nil
	}
	if p.Version != 1 || p.ApplicationID != s.Operation.BotID || p.GuildID != s.Operation.GuildID || p.OwnerID != s.Operation.OwnerID || !snowflakeID(p.ApplicationID) || !snowflakeID(p.GuildID) || !snowflakeID(p.OwnerID) {
		return errors.New("phase3 metadata scope mismatch")
	}
	if len(p.Commands) > 3 || len(p.RegistrationAttempts) > registrationAttemptLimit || len(p.Interactions) > phase3RowLimit || len(p.Reactions) > phase3RowLimit || len(p.TargetInvalidations) > phase3RowLimit || len(p.ReplyCancellations) > phase3RowLimit || len(p.CancelledRequests) > phase3RowLimit || len(p.RichReceipts) > phase3RowLimit {
		return errors.New("phase3 metadata limit exceeded")
	}
	inbound := map[string]bool{}
	eventKeys := map[string]string{}
	for _, e := range s.Events {
		inbound[e.ID] = true
		eventKeys[e.ID] = e.EventID
	}
	for _, e := range s.Ingress {
		inbound[e.ID] = true
		eventKeys[e.ID] = e.EventID
	}
	replies := map[string]bool{}
	for _, r := range s.Replies {
		replies[r.ID] = true
	}
	chunks := map[string]Chunk{}
	for _, c := range s.Chunks {
		chunks[fmt.Sprint(c.ReplyID, "/", c.Index)] = c
	}
	seen, names := map[string]bool{}, map[string]bool{}
	for _, v := range p.Commands {
		if !commandName(v.Name) || !snowflakeID(v.ID) || seen[v.ID] || names[v.Name] {
			return errors.New("invalid owned command metadata")
		}
		seen[v.ID] = true
		names[v.Name] = true
	}
	seen = map[string]bool{}
	for _, v := range p.RegistrationAttempts {
		parts := strings.Split(v.Key, ":")
		if len(parts) != 3 || !hashPattern.MatchString(parts[0]) || parts[1] != v.Name || parts[2] != v.Action || !commandName(v.Name) || (v.Action != "create" && v.Action != "update") || seen[v.Key] || !finitePositive(v.Created) {
			return errors.New("invalid registration attempt metadata")
		}
		if v.State != "attempted" && v.State != "acknowledged" && v.State != "uncertain" && v.State != "failed" {
			return errors.New("invalid registration attempt state")
		}
		if v.State == "acknowledged" && !snowflakeID(v.CommandID) || v.State != "acknowledged" && v.CommandID != "" {
			return errors.New("invalid registration acknowledgement identity")
		}
		seen[v.Key] = true
	}
	states := map[string]bool{}
	for _, state := range []string{"received", "acknowledged", "queued", "token_unavailable", "sent", "failed", "uncertain", "rejected", "expired", "store_failed", "queue_failed", "cancel_failed", "ambiguous_request", "no_active_request", "ack_sent", "ack_failed", "ack_uncertain", "ack_expired", "result_sent", "result_failed", "result_uncertain", "result_expired"} {
		states[state] = true
	}
	seen = map[string]bool{}
	for _, v := range p.Interactions {
		if v.InboundID != "" {
			key := eventKeys[v.InboundID]
			tail, ok := strings.CutPrefix(key, "control:")
			simple := ok && (snowflakeID(tail) || len(tail) == 32 && strings.Trim(tail, "0123456789abcdef") == "")
			if v.Action != "ask" || !simple {
				return errors.New("invalid interaction inbound binding")
			}
		}
		if !snowflakeID(v.ID) || v.Owner != p.OwnerID || !snowflakeID(v.Channel) || (!commandName(v.Action) && v.Action != "open_ask") || !states[v.State] || !finitePositive(v.Created) || seen[v.ID] || v.InboundID != "" && !inbound[v.InboundID] {
			return errors.New("invalid interaction fence")
		}
		seen[v.ID] = true
	}
	seen = map[string]bool{}
	for _, v := range p.Reactions {
		key := v.Channel + ":" + v.Message + ":" + v.Owner + ":" + v.Emoji
		if !snowflakeID(v.Channel) || !snowflakeID(v.Message) || v.Owner != p.OwnerID || !safeReactionEmoji(v.Emoji) || v.Sequence < 1 || seen[key] {
			return errors.New("invalid reaction fence")
		}
		seen[key] = true
	}
	seen = map[string]bool{}
	for _, v := range p.TargetInvalidations {
		key := v.Channel + ":" + v.Message
		if !snowflakeID(v.Channel) || !snowflakeID(v.Message) || (v.Code != "target_updated" && v.Code != "target_deleted") || seen[key] {
			return errors.New("invalid target invalidation fence")
		}
		seen[key] = true
	}
	seen = map[string]bool{}
	codes := map[string]bool{"operator_cancelled": true, "source_deleted": true, "source_superseded": true, "source_out_of_scope": true, "control_source_not_current": true}
	for _, v := range p.ReplyCancellations {
		if !replies[v.ReplyID] || seen[v.ReplyID] || !finitePositive(v.At) || !codes[v.Code] {
			return errors.New("invalid cancellation fence")
		}
		seen[v.ReplyID] = true
	}
	seen = map[string]bool{}
	for _, id := range p.CancelledRequests {
		if !inbound[id] || seen[id] {
			return errors.New("invalid cancelled request fence")
		}
		seen[id] = true
	}
	seen = map[string]bool{}
	for _, v := range p.RichReceipts {
		key := fmt.Sprint(v.ReplyID, "/", v.Index)
		c, ok := chunks[key]
		if !ok || c.State != "sent" || v.Index < 0 || v.Index > 1000 || seen[key] || len(v.Files) > 10 {
			return errors.New("invalid rich receipt binding")
		}
		seen[key] = true
		ids := map[string]bool{}
		for _, f := range v.Files {
			if !snowflakeID(f.ID) || ids[f.ID] || f.Size < 0 || f.Size > 128<<20 {
				return errors.New("invalid attachment metadata")
			}
			ids[f.ID] = true
		}
	}
	return nil
}
func restorePhase3(tx *sql.Tx, s Snapshot) error {
	p := s.Phase3
	if p == nil {
		return nil
	}
	if err := validatePhase3(s); err != nil {
		return err
	}
	_, err := tx.Exec(`CREATE TABLE IF NOT EXISTS control_command_ids(guild TEXT NOT NULL,name TEXT NOT NULL,id TEXT NOT NULL,PRIMARY KEY(guild,name));
 CREATE TABLE IF NOT EXISTS control_registration_attempts(key TEXT PRIMARY KEY,name TEXT NOT NULL,action TEXT NOT NULL,state TEXT NOT NULL,command_id TEXT NOT NULL,created REAL NOT NULL);
 CREATE TABLE IF NOT EXISTS control_interactions(id TEXT PRIMARY KEY,owner TEXT NOT NULL,channel TEXT NOT NULL,action TEXT NOT NULL,state TEXT NOT NULL,created REAL NOT NULL,inbound_id TEXT);
 CREATE TABLE IF NOT EXISTS control_reactions(channel TEXT NOT NULL,message TEXT NOT NULL,owner TEXT NOT NULL,emoji TEXT NOT NULL,present INTEGER NOT NULL,sequence INTEGER NOT NULL,PRIMARY KEY(channel,message,owner,emoji));
 CREATE TABLE IF NOT EXISTS control_target_invalidations(channel TEXT NOT NULL,message TEXT NOT NULL,code TEXT NOT NULL,PRIMARY KEY(channel,message));
 CREATE TABLE IF NOT EXISTS reply_cancellations(reply_id TEXT PRIMARY KEY REFERENCES replies(id),cancelled_at REAL NOT NULL,code TEXT NOT NULL);
 CREATE TABLE IF NOT EXISTS recovery_cancelled_requests(inbound_id TEXT PRIMARY KEY REFERENCES inbound(id));
 CREATE TABLE IF NOT EXISTS recovery_rich_receipts(reply_id TEXT NOT NULL REFERENCES replies(id),idx INTEGER NOT NULL,metadata TEXT NOT NULL,PRIMARY KEY(reply_id,idx));`)
	if err != nil {
		return err
	}
	for _, v := range p.Commands {
		if _, err = tx.Exec(`INSERT INTO control_command_ids VALUES(?,?,?)`, p.GuildID, v.Name, v.ID); err != nil {
			return err
		}
	}
	for _, v := range p.RegistrationAttempts {
		state := v.State
		if state == "attempted" {
			state = "uncertain"
		}
		if _, err = tx.Exec(`INSERT INTO control_registration_attempts VALUES(?,?,?,?,?,?)`, v.Key, v.Name, v.Action, state, v.CommandID, v.Created); err != nil {
			return err
		}
	}
	for _, v := range p.Interactions {
		var in any
		if v.InboundID != "" {
			in = v.InboundID
		}
		if _, err = tx.Exec(`INSERT INTO control_interactions VALUES(?,?,?,?,'token_unavailable',?,?)`, v.ID, v.Owner, v.Channel, v.Action, v.Created, in); err != nil {
			return err
		}
	}
	for _, v := range p.Reactions {
		if _, err = tx.Exec(`INSERT INTO control_reactions VALUES(?,?,?,?,?,?)`, v.Channel, v.Message, v.Owner, v.Emoji, v.Present, v.Sequence); err != nil {
			return err
		}
	}
	for _, v := range p.TargetInvalidations {
		if _, err = tx.Exec(`INSERT INTO control_target_invalidations VALUES(?,?,?)`, v.Channel, v.Message, v.Code); err != nil {
			return err
		}
	}
	for _, v := range p.ReplyCancellations {
		if _, err = tx.Exec(`INSERT INTO reply_cancellations VALUES(?,?,?)`, v.ReplyID, v.At, v.Code); err != nil {
			return err
		}
	}
	for _, id := range p.CancelledRequests {
		if _, err = tx.Exec(`INSERT INTO recovery_cancelled_requests VALUES(?)`, id); err != nil {
			return err
		}
	}
	for _, v := range p.RichReceipts {
		if _, err = tx.Exec(`INSERT INTO recovery_rich_receipts VALUES(?,?,?)`, v.ReplyID, v.Index, string(jsonBytes(v.Files))); err != nil {
			return err
		}
	}
	return nil
}

// Content-free diagnostics for a verified snapshot; no names, IDs or text.
func recoveryMetadataCounts(s Snapshot) map[string]int {
	out := map[string]int{"owned_commands": 0, "registration_attempt_fences": 0, "unresolved_registration_attempts": 0, "interaction_fences": 0, "reaction_fences": 0, "target_invalidations": 0, "reply_cancellations": 0, "cancelled_requests": 0, "inert_rich_receipts": 0, "operation_fences": 0, "held_operation_targets": 0, "unknown_edit_projections": 0, "memory_source_fences": len(s.MemorySources), "memory_document_fences": len(s.MemoryFences)}
	out["unresolved_worker_requests"] = 0
	out["worker_recovery_pending"] = 0
	out["worker_cancellation_fences"] = 0
	out["pending_worker_cancellations"] = 0
	if p := s.WorkerControl; p != nil {
		out["unresolved_worker_requests"] = len(p.UnresolvedRequests)
		out["worker_recovery_pending"] = len(p.RecoveryPending)
		out["worker_cancellation_fences"] = len(p.Cancellations)
		for _, v := range p.Cancellations {
			if v.State == "cancel_requested" {
				out["pending_worker_cancellations"]++
			}
		}
	}
	if p := s.Phase3; p != nil {
		out["owned_commands"] = len(p.Commands)
		out["registration_attempt_fences"] = len(p.RegistrationAttempts)
		for _, a := range p.RegistrationAttempts {
			if a.State == "uncertain" || a.State == "attempted" {
				out["unresolved_registration_attempts"]++
			}
		}
		out["interaction_fences"] = len(p.Interactions)
		out["reaction_fences"] = len(p.Reactions)
		out["target_invalidations"] = len(p.TargetInvalidations)
		out["reply_cancellations"] = len(p.ReplyCancellations)
		out["cancelled_requests"] = len(p.CancelledRequests)
		out["inert_rich_receipts"] = len(p.RichReceipts)
	}
	if p := s.MessageOperations; p != nil {
		out["operation_fences"] = len(p.Operations)
		targets := map[string]bool{}
		for _, o := range p.Operations {
			if o.HoldTarget {
				targets[o.Channel+":"+o.Message] = true
			}
		}
		out["held_operation_targets"] = len(targets)
		out["unknown_edit_projections"] = len(p.Projections)
	}
	return out
}
