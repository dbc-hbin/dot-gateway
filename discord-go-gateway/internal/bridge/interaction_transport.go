package bridge

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

type interactionToken struct {
	value   string
	expires time.Time
	timer   *time.Timer
}

// Tokens live only here, never in envelopes/claims/SQLite/diagnostics/rate keys.
// A restart intentionally loses them; outstanding requests must be reissued.
type interactionTransport struct {
	client         *http.Client
	receiveOnly    bool
	outputStateDir string
	base           string
	app            string
	mu             sync.Mutex
	tokens         map[string]interactionToken
	onExpire       func(string)
}

func newInteractionTransport(s Settings) (*interactionTransport, error) {
	r, err := NewRESTClient(s)
	if err != nil {
		return nil, err
	}
	r.client.Timeout = 2500 * time.Millisecond
	return &interactionTransport{receiveOnly: s.ReceiveOnly, outputStateDir: ReplyStateDir(s.DBPath), client: r.client, base: r.baseURL, app: s.ExpectedBotID, tokens: map[string]interactionToken{}}, nil
}
func (t *interactionTransport) close() {
	t.mu.Lock()
	for _, entry := range t.tokens {
		if entry.timer != nil {
			entry.timer.Stop()
		}
	}
	t.tokens = map[string]interactionToken{}
	t.mu.Unlock()
	t.client.CloseIdleConnections()
}
func (t *interactionTransport) remember(id, token string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := time.Now()
	for id, v := range t.tokens {
		if !now.Before(v.expires) {
			delete(t.tokens, id)
		}
	}
	if len(t.tokens) < 1000 {
		entry := interactionToken{value: token, expires: now.Add(14 * time.Minute)}
		entry.timer = time.AfterFunc(14*time.Minute, func() { t.expire(id) })
		t.tokens[id] = entry
	}
}
func (t *interactionTransport) forget(id string) {
	t.mu.Lock()
	if entry, ok := t.tokens[id]; ok && entry.timer != nil {
		entry.timer.Stop()
	}
	delete(t.tokens, id)
	t.mu.Unlock()
}
func (t *interactionTransport) expire(id string) {
	t.forget(id)
	if t.onExpire != nil {
		t.onExpire(id)
	}
}
func (t *interactionTransport) token(id string) (string, bool) {
	t.mu.Lock()
	v, ok := t.tokens[id]
	expired := ok && !time.Now().Before(v.expires)
	t.mu.Unlock()
	if expired {
		t.expire(id)
		return "", false
	}
	if !ok {
		return "", false
	}
	return v.value, true
}
func safeInteractionToken(v string) bool {
	if len(v) < 1 || len(v) > 4096 {
		return false
	}
	for _, r := range v {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' || r == '-' || r == '.') {
			return false
		}
	}
	return true
}

// Exactly one HTTP attempt, no automatic retries, redirects or SDK rate queues.
// A returned transport error is deliberately content-free and uncertain once Do
// begins. Even errors that contain a URL are discarded without formatting them.
func (t *interactionTransport) request(ctx context.Context, method, path string, body any) (map[string]json.RawMessage, string) {
	raw, err := json.Marshal(body)
	if err != nil {
		return nil, "failed"
	}
	return t.requestBytes(ctx, method, path, raw, "application/json", false, nil)
}

func (t *interactionTransport) requestBytes(ctx context.Context, method, path string, raw []byte, contentType string, upload bool, guard func() bool) (map[string]json.RawMessage, string) {
	if t.receiveOnly && !readOnlyHTTPMethod(method) {
		return nil, "failed"
	}
	req, err := http.NewRequestWithContext(ctx, method, t.base+path, io.NopCloser(bytes.NewReader(raw)))
	if err != nil || ctx.Err() != nil {
		return nil, "failed"
	}
	req.GetBody = nil
	req.ContentLength = int64(len(raw))
	req.Header.Set("Content-Type", contentType)
	client := t.client
	if upload {
		copy := *t.client
		copy.Timeout = 20 * time.Second
		client = &copy
	}
	if guard != nil && !guard() {
		return nil, "failed"
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, "uncertain"
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		io.Copy(io.Discard, io.LimitReader(resp.Body, 1048576))
		switch resp.StatusCode {
		case 400, 401, 403, 404, 405, 413, 429:
			return nil, "failed"
		default:
			return nil, "uncertain"
		}
	}
	if resp.StatusCode == 204 {
		return nil, "sent"
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, 1048577))
	if err != nil || len(b) > 1048576 {
		return nil, "uncertain"
	}
	var ack map[string]json.RawMessage
	if len(b) > 0 && json.Unmarshal(b, &ack) != nil {
		return nil, "uncertain"
	}
	return ack, "sent"
}
func (t *interactionTransport) callback(ctx context.Context, id, token string, body any) string {
	if !Snowflake(id) || !safeInteractionToken(token) {
		return "failed"
	}
	_, state := t.request(ctx, http.MethodPost, "/interactions/"+id+"/"+token+"/callback", body)
	return state
}
func noMentions() map[string]any {
	return map[string]any{"parse": []string{}, "users": []string{}, "roles": []string{}, "replied_user": false}
}
func interactionBody(text string, components any) map[string]any {
	body := map[string]any{"content": text, "allowed_mentions": noMentions(), "flags": 68}
	if components != nil {
		body["components"] = components
	}
	return body
}
func (t *interactionTransport) edit(ctx context.Context, id, channel, text string, components any) (string, string) {
	token, ok := t.token(id)
	if !ok {
		return "", "expired"
	}
	ack, state := t.request(ctx, http.MethodPatch, "/webhooks/"+t.app+"/"+token+"/messages/@original", interactionBody(text, components))
	if state != "sent" {
		return "", state
	}
	mid := validateInteractionAck(ack, t.app, channel, text)
	if mid == "" {
		return "", "uncertain"
	}
	return mid, "sent"
}
func (t *interactionTransport) send(ctx context.Context, c Chunk, guard func() bool) SendResult {
	ce, err := c.Source.ControlEvent()
	if err != nil || ce.Kind != "ask" || c.Source.ReplyKind != "interaction" || c.ReplyID == "" || c.Index < 0 || TextUnits(c.Text) > 1900 || trimText(c.Text) == "" && c.Output == "" {
		return SendResult{State: "failed", Code: "invalid_interaction_reply"}
	}
	body, contentType, err := BuildInteractionReplyBody(c, t.outputStateDir)
	if err != nil {
		return SendResult{State: "failed", Code: symbolicCode(err.Error())}
	}
	token, ok := t.token(ce.InteractionID)
	if !ok {
		return SendResult{State: "failed", Code: "interaction_token_unavailable_reissue_required"}
	}
	if guard != nil && !guard() {
		return SendResult{State: "failed", Code: "interaction_request_cancelled"}
	}
	method, path := http.MethodPatch, "/webhooks/"+t.app+"/"+token+"/messages/@original"
	if c.Index > 0 {
		method, path = http.MethodPost, "/webhooks/"+t.app+"/"+token+"?wait=true"
	}
	ack, state := t.requestBytes(ctx, method, path, body, contentType, true, guard)
	if state != "sent" {
		return SendResult{State: state, Code: "interaction_request_or_ack_failed"}
	}
	id := validateInteractionAck(ack, t.app, c.Source.ConversationID, c.Text)
	if id == "" {
		return SendResult{State: "uncertain", Code: "invalid_interaction_ack"}
	}
	receipt, err := ReplyOutputAckReceipt(ack, c)
	if err != nil {
		return SendResult{State: "uncertain", Code: "invalid_interaction_output_ack"}
	}
	code, _ := validateReplyContent(jsonString(ack["content"]), c.Text)
	return SendResult{State: "sent", MessageID: id, Code: code, OutputReceipt: receipt}
}

var errInteractionRejected = errors.New("interaction_rejected")

func validateInteractionAck(ack map[string]json.RawMessage, app, channel, text string) string {
	id := jsonString(ack["id"])
	var user User
	var flags int
	actual, present := requiredJSONString(ack["content"])
	_, contentValid := validateReplyContent(actual, text)
	if !present || !contentValid || json.Unmarshal(ack["author"], &user) != nil || json.Unmarshal(ack["flags"], &flags) != nil || flags&64 == 0 || !Snowflake(id) || jsonString(ack["channel_id"]) != channel || user.ID != app || !user.Bot || jsonString(ack["webhook_id"]) != app {
		return ""
	}
	if raw, ok := ack["message_reference"]; ok && string(raw) != "null" && strings.TrimSpace(string(raw)) != "{}" {
		return ""
	}
	return id
}
