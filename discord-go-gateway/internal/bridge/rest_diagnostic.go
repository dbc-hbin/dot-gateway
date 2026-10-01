package bridge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"
	"unicode/utf8"
)

// SendDiagnostic sends an explicitly authorized durable standalone diagnostic.
// It does not create an inbound event and never retries an attempted message.
func (r *RESTClient) sendDiagnosticLocked(ctx context.Context, d Diagnostic, guard func() bool, diag *Diagnostics) (result SendResult) {
	started := time.Now()
	defer func() { r.publishDiagnostics(started, result, diag) }()
	p := r.settings.Policy
	if d.ID == "" || d.Index < 0 || d.Index >= 3 || d.Nonce != Nonce(Chunk{ReplyID: d.ID, Index: d.Index}) || d.GuildID == "" || d.OwnerID != p.OwnerID || d.BotID != r.settings.ExpectedBotID || !Snowflake(d.ChannelID) || !utf8.ValidString(d.Text) || trimText(d.Text) == "" || TextUnits(d.Text) > 1900 {
		return SendResult{State: "failed", Code: "invalid_diagnostic"}
	}
	source := Envelope{Platform: "discord", ConversationID: d.ChannelID, SenderID: d.OwnerID, RouteKind: "dm"}
	if d.GuildID != "" {
		if d.GuildID != p.GuildID || d.ChannelID != p.GuildChannelID {
			return SendResult{State: "failed", Code: "invalid_diagnostic_route"}
		}
		source.RouteKind = "guild_text"
		source.GuildID = d.GuildID
	}
	pre := time.Now()
	err := r.preflightMeasured(ctx, source, diag)
	diag.PreflightSeconds = time.Since(pre).Seconds()
	if err != nil {
		return SendResult{State: "failed", Code: err.Error()}
	}
	if ctx.Err() != nil {
		return SendResult{State: "failed", Code: "cancelled_before_send"}
	}
	if guard != nil && !guard() {
		return SendResult{State: "failed", Code: "connection_changed_before_send"}
	}
	body, _ := json.Marshal(map[string]any{"content": d.Text, "nonce": d.Nonce, "enforce_nonce": true, "allowed_mentions": map[string]any{"parse": []string{}, "users": []string{}, "roles": []string{}, "replied_user": false}, "flags": 4})
	if guard != nil {
		ctx = context.WithValue(ctx, sendGuardContextKey{}, guard)
	}
	post := time.Now()
	ctx, measured := newRequestMeasurement(ctx)
	defer func() {
		diag.Post = measured.finish()
		diag.PostSeconds = time.Since(post).Seconds()
		diag.Reused = diag.Post.Reused
	}()
	resp, err := r.request(ctx, http.MethodPost, "/channels/"+d.ChannelID+"/messages", body)
	if err != nil {
		if errors.Is(err, errSendGuardChanged) {
			return SendResult{State: "failed", Code: "connection_changed_before_send"}
		}
		if measured.failedBeforeConnection() {
			return SendResult{State: "failed", Code: "connect_failed"}
		}
		return SendResult{State: "uncertain", Code: "request_or_ack_failed"}
	}
	if resp.StatusCode != 200 && resp.StatusCode != 201 {
		resp.Body.Close()
		state := "uncertain"
		switch resp.StatusCode {
		case 400, 401, 403, 404, 405, 413, 429:
			state = "failed"
		}
		return SendResult{State: state, Code: fmt.Sprintf("http_%d", resp.StatusCode)}
	}
	var ack map[string]json.RawMessage
	if err = readJSON(resp, &ack); err != nil {
		return SendResult{State: "uncertain", Code: err.Error()}
	}
	id := jsonString(ack["id"])
	var author User
	if !Snowflake(id) || jsonString(ack["channel_id"]) != d.ChannelID || json.Unmarshal(ack["author"], &author) != nil || author.ID != d.BotID || !author.Bot || jsonString(ack["content"]) != d.Text || jsonString(ack["nonce"]) != d.Nonce {
		return SendResult{State: "uncertain", Code: "invalid_ack"}
	}
	if _, ok := ack["webhook_id"]; ok {
		return SendResult{State: "uncertain", Code: "invalid_ack"}
	}
	if ref, ok := ack["message_reference"]; ok && string(ref) != "null" {
		return SendResult{State: "uncertain", Code: "invalid_ack"}
	}
	if guild, ok := ack["guild_id"]; ok && jsonString(guild) != d.GuildID {
		return SendResult{State: "uncertain", Code: "invalid_ack"}
	}
	return SendResult{State: "sent", MessageID: id}
}

// SendDiagnosticMeasured snapshots timings while still holding the send lease.
func (r *RESTClient) SendDiagnosticMeasured(ctx context.Context, d Diagnostic, guard func() bool) (SendResult, Diagnostics) {
	queued := time.Now()
	r.sendMu.Lock()
	defer r.sendMu.Unlock()
	diag := Diagnostics{Operation: "diagnostic", SendLockWaitSeconds: time.Since(queued).Seconds()}
	result := r.sendDiagnosticLocked(ctx, d, guard, &diag)
	return result, diag
}
func (r *RESTClient) SendDiagnostic(ctx context.Context, d Diagnostic, guard func() bool) SendResult {
	result, _ := r.SendDiagnosticMeasured(ctx, d, guard)
	return result
}
