package bridge

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"
)

type User struct {
	ID  string `json:"id"`
	Bot bool   `json:"bot"`
}
type Channel struct {
	ID         string `json:"id"`
	Type       int    `json:"type"`
	GuildID    string `json:"guild_id"`
	Recipients []User `json:"recipients"`
}
type Diagnostics struct {
	Operation        string  `json:"operation"`
	State            string  `json:"state"`
	Code             string  `json:"code,omitempty"`
	Seconds          float64 `json:"seconds"`
	PreflightSeconds float64 `json:"preflight_seconds"`
	PostSeconds      float64 `json:"post_seconds"`
	Reused           bool    `json:"reused"`
}
type RESTClient struct {
	settings    Settings
	client      *http.Client
	baseURL     string
	closed      atomic.Bool
	sendMu      sync.Mutex
	limitMu     sync.Mutex
	limits      map[string]time.Time
	buckets     map[string]string
	globalUntil time.Time
	diagMu      sync.RWMutex
	diagnostics Diagnostics
}

func NewRESTClient(s Settings) (*RESTClient, error) {
	if e := s.Policy.Validate(); e != nil {
		return nil, e
	}
	if !Snowflake(s.ExpectedBotID) || s.ExpectedBotID == s.Policy.OwnerID {
		return nil, errors.New("invalid_expected_bot_id")
	}
	p := s.ProxyConfig
	if p == nil {
		p = &ProxyConfig{}
	}
	p, proxyErr := NewProxyConfig(p.URL, p.NoProxy)
	if proxyErr != nil {
		return nil, proxyErr
	}
	s.ProxyConfig = p
	route, e := p.DiscordRoute()
	if e != nil {
		return nil, e
	}
	var proxy *url.URL
	if route != "" {
		proxy, _ = url.Parse(route)
	}
	keep := s.HTTPKeepaliveSeconds
	if keep == 0 {
		keep = 120
	}
	tr := &http.Transport{Proxy: func(r *http.Request) (*url.URL, error) {
		if r.URL.Scheme != "https" || r.URL.Host != "discord.com" {
			return nil, errors.New("unexpected_rest_endpoint")
		}
		return proxy, nil
	}, DialContext: (&net.Dialer{Timeout: 15 * time.Second, KeepAlive: 30 * time.Second}).DialContext, TLSHandshakeTimeout: 15 * time.Second, ResponseHeaderTimeout: 15 * time.Second, IdleConnTimeout: time.Duration(keep * float64(time.Second)), MaxIdleConns: 16, MaxIdleConnsPerHost: 8, MaxConnsPerHost: 16, ForceAttemptHTTP2: false, TLSNextProto: map[string]func(string, *tls.Conn) http.RoundTripper{}}
	s.Policy.AllowedDMIDs = append([]string(nil), s.Policy.AllowedDMIDs...)
	return &RESTClient{settings: s, client: &http.Client{Transport: tr, Timeout: 20 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, baseURL: "https://discord.com/api/v10"}, nil
}

// Client is for read-only Gateway discovery; message POSTs must use Send.
func (r *RESTClient) Client() *http.Client { return r.client }
func (r *RESTClient) Close()               { r.closed.Store(true); r.client.CloseIdleConnections() }
func (r *RESTClient) Diagnostics() Diagnostics {
	r.diagMu.RLock()
	defer r.diagMu.RUnlock()
	return r.diagnostics
}
func (r *RESTClient) request(ctx context.Context, method, path string, body []byte) (*http.Response, error) {
	if r.closed.Load() {
		return nil, errors.New("sender_closed")
	}
	if err := r.waitLimit(ctx, method, path); err != nil {
		return nil, err
	}
	if guard, ok := ctx.Value(sendGuardContextKey{}).(func() bool); ok && !guard() {
		return nil, errSendGuardChanged
	}
	var reader io.Reader
	if body != nil {
		reader = io.NopCloser(bytes.NewReader(body))
	}
	req, e := http.NewRequestWithContext(ctx, method, r.baseURL+path, reader)
	if e != nil {
		return nil, errors.New("request_build_failed")
	}
	req.GetBody = nil
	if body != nil {
		req.ContentLength = int64(len(body))
	}
	req.Header.Set("Authorization", "Bot "+r.settings.Token)
	req.Header.Set("User-Agent", "DotTextBridge/1.0 (Go)")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := r.client.Do(req)
	if resp != nil {
		r.observeLimits(resp, method, path)
	}
	return resp, err
}
func readJSON(resp *http.Response, out any) error {
	defer resp.Body.Close()
	b, e := io.ReadAll(io.LimitReader(resp.Body, 1048577))
	if e != nil {
		return errors.New("response_read_failed")
	}
	if len(b) > 1048576 {
		return errors.New("oversize_ack")
	}
	if !utf8.Valid(b) || json.Unmarshal(b, out) != nil {
		return errors.New("invalid_ack")
	}
	return nil
}
func (r *RESTClient) get(ctx context.Context, path string, out any) error {
	resp, e := r.request(ctx, http.MethodGet, path, nil)
	if e != nil {
		return errors.New("preflight_transport_failed")
	}
	if resp.StatusCode != 200 {
		resp.Body.Close()
		return fmt.Errorf("preflight_http_%d", resp.StatusCode)
	}
	if e = readJSON(resp, out); e != nil {
		return errors.New("preflight_invalid_ack")
	}
	return nil
}
func (r *RESTClient) Identity(ctx context.Context) (User, error) {
	var u User
	if e := r.get(ctx, "/users/@me", &u); e != nil {
		return u, e
	}
	if u.ID != r.settings.ExpectedBotID || !u.Bot {
		return u, errors.New("preflight_identity_mismatch")
	}
	return u, nil
}
func (r *RESTClient) Channel(ctx context.Context, id string) (Channel, error) {
	var c Channel
	if !Snowflake(id) {
		return c, errors.New("invalid_channel_id")
	}
	var raw map[string]json.RawMessage
	e := r.get(ctx, "/channels/"+id, &raw)
	if e == nil {
		if _, ok := raw["type"]; !ok {
			return c, errors.New("preflight_channel_mismatch")
		}
		b, _ := json.Marshal(raw)
		if json.Unmarshal(b, &c) != nil {
			return c, errors.New("preflight_channel_mismatch")
		}
	}
	if e == nil && c.ID != id {
		e = errors.New("preflight_channel_mismatch")
	}
	return c, e
}
func (r *RESTClient) ValidateChannel(c Channel, s Envelope) error {
	if c.ID != s.ConversationID {
		return errors.New("preflight_channel_mismatch")
	}
	p := r.settings.Policy
	if s.RouteKind == "guild_text" {
		if c.Type != 0 || c.GuildID != p.GuildID || c.ID != p.GuildChannelID || s.GuildID != p.GuildID {
			return errors.New("preflight_channel_mismatch")
		}
	} else if s.RouteKind != "dm" || s.GuildID != "" || c.Type != 1 || c.GuildID != "" || len(c.Recipients) != 1 || c.Recipients[0].ID != p.OwnerID || c.Recipients[0].Bot {
		return errors.New("preflight_recipient_mismatch")
	}
	return nil
}
func (r *RESTClient) GatewayURL(ctx context.Context) (string, error) {
	var v struct {
		URL string `json:"url"`
	}
	if e := r.get(ctx, "/gateway/bot", &v); e != nil {
		return "", e
	}
	route, e := r.settings.ProxyConfig.DiscordRoute()
	if e != nil {
		return "", e
	}
	if e = r.settings.ProxyConfig.ValidateGateway(v.URL, route); e != nil {
		return "", e
	}
	return v.URL, nil
}
func (r *RESTClient) preflight(ctx context.Context, s Envelope) error {
	var wg sync.WaitGroup
	wg.Add(2)
	var identityErr, channelErr error
	go func() { defer wg.Done(); _, identityErr = r.Identity(ctx) }()
	go func() {
		defer wg.Done()
		c, e := r.Channel(ctx, s.ConversationID)
		if e == nil {
			e = r.ValidateChannel(c, s)
		}
		channelErr = e
	}()
	wg.Wait()
	if identityErr != nil {
		return identityErr
	}
	return channelErr
}
func Nonce(c Chunk) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s:%d", c.ReplyID, c.Index)))
	return hex.EncodeToString(sum[:])[:24]
}
func payload(c Chunk) []byte {
	b, _ := json.Marshal(map[string]any{"content": c.Text, "nonce": Nonce(c), "enforce_nonce": true, "allowed_mentions": map[string]any{"parse": []string{}, "users": []string{}, "roles": []string{}, "replied_user": false}, "message_reference": map[string]any{"message_id": c.Source.EventID, "channel_id": c.Source.ConversationID, "fail_if_not_exists": true}, "flags": 4})
	return b
}
func (r *RESTClient) Send(ctx context.Context, c Chunk) SendResult { return r.SendGuarded(ctx, c, nil) }

// SendGuarded attempts at most one message POST. No caller may retry uncertain results.
func (r *RESTClient) SendGuarded(ctx context.Context, c Chunk, guard func() bool) (result SendResult) {
	r.sendMu.Lock()
	defer r.sendMu.Unlock()
	started := time.Now()
	d := Diagnostics{Operation: "send"}
	defer func() {
		d.Seconds = time.Since(started).Seconds()
		d.State = result.State
		d.Code = result.Code
		r.diagMu.Lock()
		r.diagnostics = d
		r.diagMu.Unlock()
	}()
	if !r.settings.Policy.Allows(c.Source) || c.ReplyID == "" || c.Index < 0 || !utf8.ValidString(c.Text) || trimText(c.Text) == "" || TextUnits(c.Text) > 1900 {
		return SendResult{State: "failed", Code: "invalid_route_or_chunk"}
	}
	pre := time.Now()
	e := r.preflight(ctx, c.Source)
	d.PreflightSeconds = time.Since(pre).Seconds()
	if e != nil {
		return SendResult{State: "failed", Code: e.Error()}
	}
	if ctx.Err() != nil {
		return SendResult{State: "failed", Code: "cancelled_before_send"}
	}
	if guard != nil && !guard() {
		return SendResult{State: "failed", Code: "connection_changed_before_send"}
	}
	var gotConn atomic.Bool
	var reused atomic.Bool
	trace := &httptrace.ClientTrace{GotConn: func(i httptrace.GotConnInfo) { gotConn.Store(true); reused.Store(i.Reused) }}
	if guard != nil {
		ctx = context.WithValue(ctx, sendGuardContextKey{}, guard)
	}
	post := time.Now()
	defer func() { d.PostSeconds = time.Since(post).Seconds() }()
	resp, e := r.request(httptrace.WithClientTrace(ctx, trace), http.MethodPost, "/channels/"+c.Source.ConversationID+"/messages", payload(c))
	d.PostSeconds = time.Since(post).Seconds()
	d.Reused = reused.Load()
	if e != nil {
		if errors.Is(e, errSendGuardChanged) {
			return SendResult{State: "failed", Code: "connection_changed_before_send"}
		}
		if !gotConn.Load() {
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
	if e = readJSON(resp, &ack); e != nil {
		return SendResult{State: "uncertain", Code: e.Error()}
	}
	id, code := validateAck(ack, c, r.settings.ExpectedBotID)
	if id == "" {
		return SendResult{State: "uncertain", Code: "invalid_ack"}
	}
	return SendResult{State: "sent", MessageID: id, Code: code}
}
func jsonString(v json.RawMessage) string {
	var s string
	if json.Unmarshal(v, &s) == nil {
		return s
	}
	return ""
}
func validateAck(a map[string]json.RawMessage, c Chunk, botID string) (string, string) {
	id := jsonString(a["id"])
	if !Snowflake(id) || jsonString(a["channel_id"]) != c.Source.ConversationID {
		return "", ""
	}
	var author User
	if json.Unmarshal(a["author"], &author) != nil || author.ID != botID || !author.Bot {
		return "", ""
	}
	if _, ok := a["webhook_id"]; ok {
		return "", ""
	}
	if v, ok := a["guild_id"]; ok && jsonString(v) != c.Source.GuildID {
		return "", ""
	}
	if v, ok := a["nonce"]; ok && jsonString(v) != Nonce(c) {
		return "", ""
	}
	actual := jsonString(a["content"])
	code := ""
	if actual != c.Text {
		if !strings.HasSuffix(c.Text, "\n") || len(c.Text) <= 1 || actual != c.Text[:len(c.Text)-1] {
			return "", ""
		}
		prev, _ := utf8.DecodeLastRuneInString(actual)
		if isTextSpace(prev) {
			return "", ""
		}
		code = "ack_terminal_lf_removed"
	}
	// Require full immutable reference even when content is unchanged.
	var ref struct {
		MessageID string `json:"message_id"`
		ChannelID string `json:"channel_id"`
		GuildID   string `json:"guild_id"`
	}
	if json.Unmarshal(a["message_reference"], &ref) != nil || ref.MessageID != c.Source.EventID || ref.ChannelID != c.Source.ConversationID || ref.GuildID != "" && ref.GuildID != c.Source.GuildID {
		return "", ""
	}
	return id, code
}
func (r *RESTClient) feedback(ctx context.Context, s Envelope, method, suffix string) error {
	if !r.settings.Policy.Allows(s) {
		return errors.New("invalid_feedback_route")
	}
	c, e := r.Channel(ctx, s.ConversationID)
	if e != nil {
		return e
	}
	if e = r.ValidateChannel(c, s); e != nil {
		return e
	}
	var b []byte
	if method == http.MethodPost {
		b = []byte(`{}`)
	}
	resp, e := r.request(ctx, method, "/channels/"+s.ConversationID+suffix, b)
	if e != nil {
		return errors.New("feedback_transport_failed")
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, io.LimitReader(resp.Body, 1048577))
	if resp.StatusCode != 204 && resp.StatusCode != 200 {
		return fmt.Errorf("feedback_http_%d", resp.StatusCode)
	}
	return nil
}
func (r *RESTClient) Typing(ctx context.Context, s Envelope) error {
	return r.feedback(ctx, s, http.MethodPost, "/typing")
}
func (r *RESTClient) Reaction(ctx context.Context, s Envelope, emoji string) error {
	return r.feedback(ctx, s, http.MethodPut, "/messages/"+s.EventID+"/reactions/"+url.PathEscape(emoji)+"/@me")
}
func (r *RESTClient) RemoveReaction(ctx context.Context, s Envelope, emoji string) error {
	return r.feedback(ctx, s, http.MethodDelete, "/messages/"+s.EventID+"/reactions/"+url.PathEscape(emoji)+"/@me")
}
func (p *ProxyConfig) ProxyRequest(req *http.Request) (*url.URL, error) {
	route, e := p.DiscordRoute()
	if e != nil {
		return nil, e
	}
	target := *req.URL
	if target.Scheme == "https" {
		target.Scheme = "wss"
	}
	if e = p.ValidateGateway(target.String(), route); e != nil {
		return nil, e
	}
	if route == "" {
		return nil, nil
	}
	return url.Parse(route)
}

// Route budgets delay only a future request. They never replay an attempted POST.
func limitRoute(method, path string) (string, string) {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) >= 2 && parts[0] == "channels" {
		major := parts[1]
		route := method + ":channels/" + major
		if len(parts) > 2 {
			route += "/" + parts[2]
		}
		if len(parts) > 4 && parts[4] == "reactions" {
			route += "/reactions"
		}
		return route, major
	}
	return method + ":" + path, ""
}
func durationSeconds(raw string) time.Duration {
	v, e := strconv.ParseFloat(strings.TrimSpace(raw), 64)
	if e != nil || math.IsNaN(v) || math.IsInf(v, 0) || v <= 0 {
		return 0
	}
	if v > 86400 {
		v = 86400
	}
	return time.Duration(v * float64(time.Second))
}
func (r *RESTClient) waitLimit(ctx context.Context, method, path string) error {
	route, _ := limitRoute(method, path)
	for {
		r.limitMu.Lock()
		until := r.globalUntil
		if d := r.limits[route]; d.After(until) {
			until = d
		}
		if key := r.buckets[route]; key != "" {
			if d := r.limits[key]; d.After(until) {
				until = d
			}
		}
		r.limitMu.Unlock()
		wait := time.Until(until)
		if wait <= 0 {
			return nil
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return errors.New("rate_limit_wait_cancelled")
		case <-timer.C:
		}
	}
}
func (r *RESTClient) observeLimits(resp *http.Response, method, path string) {
	route, major := limitRoute(method, path)
	now := time.Now()
	reset := time.Duration(0)
	if strings.TrimSpace(resp.Header.Get("X-RateLimit-Remaining")) == "0" {
		reset = durationSeconds(resp.Header.Get("X-RateLimit-Reset-After"))
	}
	global := strings.EqualFold(resp.Header.Get("X-RateLimit-Global"), "true") || strings.EqualFold(resp.Header.Get("X-RateLimit-Scope"), "global")
	if resp.StatusCode == 429 {
		retry := durationSeconds(resp.Header.Get("Retry-After"))
		if retry > reset {
			reset = retry
		}
		// Discord's JSON global marker is authoritative too. The bounded error body
		// is consumed here and never logged; the caller only receives HTTP status.
		b, err := io.ReadAll(io.LimitReader(resp.Body, 1048577))
		resp.Body.Close()
		resp.Body = io.NopCloser(bytes.NewReader(nil))
		if err == nil && len(b) <= 1048576 {
			var v struct {
				Global     bool    `json:"global"`
				RetryAfter float64 `json:"retry_after"`
			}
			if json.Unmarshal(b, &v) == nil {
				global = global || v.Global
				retry = durationSeconds(strconv.FormatFloat(v.RetryAfter, 'f', -1, 64))
				if retry > reset {
					reset = retry
				}
			}
		}
	}
	r.limitMu.Lock()
	defer r.limitMu.Unlock()
	if r.limits == nil {
		r.limits = map[string]time.Time{}
		r.buckets = map[string]string{}
	}
	bucket := resp.Header.Get("X-RateLimit-Bucket")
	if bucket != "" && len(bucket) <= 256 {
		r.buckets[route] = "bucket:" + major + ":" + bucket
	}
	if reset <= 0 {
		return
	}
	until := now.Add(reset)
	if until.After(r.limits[route]) {
		r.limits[route] = until
	}
	if key := r.buckets[route]; key != "" && until.After(r.limits[key]) {
		r.limits[key] = until
	}
	if global && until.After(r.globalUntil) {
		r.globalUntil = until
	}
}

type sendGuardContextKey struct{}

var errSendGuardChanged = errors.New("connection_changed_before_send")
