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
	Name                 string                `json:"name"`
	ID                   string                `json:"id"`
	Type                 int                   `json:"type"`
	GuildID              string                `json:"guild_id"`
	Recipients           []User                `json:"recipients"`
	ParentID             string                `json:"parent_id"`
	ThreadMetadata       *ThreadMetadata       `json:"thread_metadata"`
	Member               *ThreadMember         `json:"member"`
	PermissionOverwrites []PermissionOverwrite `json:"permission_overwrites"`
}

// RequestDiagnostics is a bounded, content-free value snapshot for one HTTP request.
// Seconds includes rate-limit waiting and response validation. HeadersSeconds and
// BodySeconds run from the start of the phase (also including rate-limit waiting)
// to received headers and body EOF/error/close, respectively. BodyComplete means
// EOF was observed; a closed or failed body is not assumed to have been consumed.
// Attempted means http.Client.Do was invoked, not that any bytes were written.
// Connection/DNS/connect/TLS observations are optional: false means unavailable,
// not a measured zero. ConnectSeconds spans the first start through the last
// completed dial, so overlapping connection attempts are not double-counted.
type RequestDiagnostics struct {
	Seconds              float64 `json:"seconds"`
	RateLimitWaitSeconds float64 `json:"rate_limit_wait_seconds"`
	Attempted            bool    `json:"attempted"`
	HeadersReceived      bool    `json:"headers_received"`
	HeadersSeconds       float64 `json:"headers_seconds"`
	BodyFinished         bool    `json:"body_finished"`
	BodyComplete         bool    `json:"body_complete"`
	BodySeconds          float64 `json:"body_seconds"`
	ConnectionObserved   bool    `json:"connection_observed"`
	Reused               bool    `json:"reused"`
	DNSObserved          bool    `json:"dns_observed"`
	DNSSeconds           float64 `json:"dns_seconds"`
	ConnectObserved      bool    `json:"connect_observed"`
	ConnectSeconds       float64 `json:"connect_seconds"`
	TLSObserved          bool    `json:"tls_observed"`
	TLSSeconds           float64 `json:"tls_seconds"`
}

// Diagnostics retains legacy meanings: Seconds excludes send-lock waiting,
// PreflightSeconds is parallel preflight wall time, PostSeconds includes POST
// rate waiting and acknowledgement validation, and Reused refers only to POST.
// Per-request values distinguish a warmed POST from cold preflight connections.
type Diagnostics struct {
	Operation           string             `json:"operation"`
	State               string             `json:"state"`
	Code                string             `json:"code,omitempty"`
	Seconds             float64            `json:"seconds"`
	PreflightSeconds    float64            `json:"preflight_seconds"`
	PostSeconds         float64            `json:"post_seconds"`
	Reused              bool               `json:"reused"`
	SendLockWaitSeconds float64            `json:"send_lock_wait_seconds"`
	IdentityGET         RequestDiagnostics `json:"identity_get"`
	ChannelGET          RequestDiagnostics `json:"channel_get"`
	Post                RequestDiagnostics `json:"post"`
}

type requestMeasurementKey struct{}

// Trace callbacks may race with response processing or finish after a request.
// Freeze under the mutex before returning a value; no callbacks can mutate it.
type requestMeasurement struct {
	mu                                        sync.Mutex
	started, dnsStart, connectStart, tlsStart time.Time
	metrics                                   RequestDiagnostics
	connectionRequested, frozen               bool
}

func newRequestMeasurement(ctx context.Context) (context.Context, *requestMeasurement) {
	m := &requestMeasurement{started: time.Now()}
	trace := &httptrace.ClientTrace{
		GetConn: func(string) { m.update(func() { m.connectionRequested = true }) },
		GotConn: func(i httptrace.GotConnInfo) {
			m.update(func() { m.metrics.ConnectionObserved = true; m.metrics.Reused = i.Reused })
		},
		DNSStart: func(httptrace.DNSStartInfo) {
			m.update(func() {
				if m.dnsStart.IsZero() {
					m.dnsStart = time.Now()
				}
			})
		},
		DNSDone: func(httptrace.DNSDoneInfo) {
			m.update(func() {
				if !m.dnsStart.IsZero() {
					m.metrics.DNSObserved = true
					m.metrics.DNSSeconds = time.Since(m.dnsStart).Seconds()
				}
			})
		},
		ConnectStart: func(string, string) {
			m.update(func() {
				if m.connectStart.IsZero() {
					m.connectStart = time.Now()
				}
			})
		},
		ConnectDone: func(string, string, error) {
			m.update(func() {
				if !m.connectStart.IsZero() {
					m.metrics.ConnectObserved = true
					m.metrics.ConnectSeconds = time.Since(m.connectStart).Seconds()
				}
			})
		},
		TLSHandshakeStart: func() {
			m.update(func() {
				if m.tlsStart.IsZero() {
					m.tlsStart = time.Now()
				}
			})
		},
		TLSHandshakeDone: func(tls.ConnectionState, error) {
			m.update(func() {
				if !m.tlsStart.IsZero() {
					m.metrics.TLSObserved = true
					m.metrics.TLSSeconds = time.Since(m.tlsStart).Seconds()
				}
			})
		},
	}
	ctx = context.WithValue(ctx, requestMeasurementKey{}, m)
	return httptrace.WithClientTrace(ctx, trace), m
}
func (m *requestMeasurement) update(f func()) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.frozen {
		f()
	}
}
func (m *requestMeasurement) finish() RequestDiagnostics {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.frozen {
		m.metrics.Seconds = time.Since(m.started).Seconds()
		m.frozen = true
	}
	return m.metrics
}
func (m *requestMeasurement) failedBeforeConnection() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	// Missing trace support after Do is not proof that a POST was unsent.
	return !m.metrics.Attempted || m.connectionRequested && !m.metrics.ConnectionObserved
}
func (m *requestMeasurement) bodyFinished(complete bool) {
	m.update(func() {
		if !m.metrics.BodyFinished {
			m.metrics.BodyFinished = true
			m.metrics.BodySeconds = time.Since(m.started).Seconds()
		}
		m.metrics.BodyComplete = m.metrics.BodyComplete || complete
	})
}

type measuredResponseBody struct {
	io.ReadCloser
	measurement *requestMeasurement
}

func (b *measuredResponseBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err != nil {
		b.measurement.bodyFinished(err == io.EOF)
	}
	return n, err
}
func (b *measuredResponseBody) Close() error {
	err := b.ReadCloser.Close()
	b.measurement.bodyFinished(false)
	return err
}

type RESTClient struct {
	contextEnabled  bool
	settings        Settings
	client          *http.Client
	baseURL         string
	closed          atomic.Bool
	sendMu          sync.Mutex
	limitMu         sync.Mutex
	limits          map[string]time.Time
	buckets         map[string]string
	globalUntil     time.Time
	diagMu          sync.RWMutex
	diagnostics     Diagnostics
	routePermission func(Envelope) bool
	controlStore    *Store
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
	var transport http.RoundTripper = tr
	if s.ReceiveOnly {
		transport = &receiveOnlyTransport{next: tr}
	}
	s.Policy.AllowedDMIDs = append([]string(nil), s.Policy.AllowedDMIDs...)
	return &RESTClient{settings: s, client: &http.Client{Transport: transport, Timeout: 20 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, baseURL: "https://discord.com/api/v10"}, nil
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
	return r.requestContent(ctx, method, path, body, "application/json")
}
func (r *RESTClient) requestContent(ctx context.Context, method, path string, body []byte, contentType string) (*http.Response, error) {
	if r.settings.ReceiveOnly && !readOnlyHTTPMethod(method) {
		return nil, errReceiveOnly
	}
	if r.closed.Load() {
		return nil, errors.New("sender_closed")
	}
	measurement, _ := ctx.Value(requestMeasurementKey{}).(*requestMeasurement)
	if err := r.waitWriteBudget(ctx, method, path, measurement); err != nil {
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
		req.Header.Set("Content-Type", contentType)
	}
	if guard, ok := ctx.Value(sendGuardContextKey{}).(func() bool); ok && !guard() {
		return nil, errSendGuardChanged
	}
	measurement.update(func() { measurement.metrics.Attempted = true })
	resp, err := r.client.Do(req)
	if resp != nil {
		measurement.update(func() {
			measurement.metrics.HeadersReceived = true
			measurement.metrics.HeadersSeconds = time.Since(measurement.started).Seconds()
		})
		if measurement != nil && resp.Body != nil {
			resp.Body = &measuredResponseBody{ReadCloser: resp.Body, measurement: measurement}
		}
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
		if value, ok := raw["type"]; !ok || string(value) == "null" {
			return c, errors.New("preflight_channel_invalid")
		}
		b, _ := json.Marshal(raw)
		if json.Unmarshal(b, &c) != nil {
			return c, errors.New("preflight_channel_invalid")
		}
	}
	if e == nil && c.ID != id {
		e = errors.New("preflight_channel_invalid")
	}
	return c, e
}
func (r *RESTClient) ValidateChannel(c Channel, s Envelope) error {
	if c.ID != s.ConversationID {
		return errors.New("preflight_channel_mismatch")
	}
	p := r.settings.Policy
	if s.RouteKind == "guild_thread" {
		if (c.Type == 11 || c.Type == 12) && (!Snowflake(c.GuildID) || !Snowflake(c.ParentID)) {
			return errors.New("preflight_channel_invalid")
		}
		if c.GuildID != p.GuildID || s.GuildID != p.GuildID || c.ParentID != p.GuildChannelID || s.ParentChannelID != p.GuildChannelID || c.Type != s.ThreadType || (c.Type != 11 && c.Type != 12) || c.ID == p.GuildChannelID {
			return errors.New("preflight_channel_mismatch")
		}
	} else if s.RouteKind == "guild_text" {
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
	return r.preflightMeasured(ctx, s, nil)
}
func (r *RESTClient) preflightMeasured(ctx context.Context, s Envelope, diagnostics *Diagnostics) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(2)
	var identityErr, channelErr error
	var channel Channel
	var identityMetrics, channelMetrics RequestDiagnostics
	go func() {
		defer wg.Done()
		identityCtx, measured := newRequestMeasurement(ctx)
		_, identityErr = r.Identity(identityCtx)
		// A failed identity check definitively forbids POST. Cancel only the
		// sibling work, then join it and retain the original identity error.
		if identityErr != nil {
			cancel()
		}
		identityMetrics = measured.finish()
	}()
	go func() {
		defer wg.Done()
		channelCtx, measured := newRequestMeasurement(ctx)
		c, e := r.Channel(channelCtx, s.ConversationID)
		channel = c
		channelMetrics = measured.finish()
		if e == nil {
			if s.RouteKind == "guild_thread" {
				e = r.ValidateChannel(c, s)
			} else {
				e = r.validateRoute(ctx, c, s)
			}
		}
		channelErr = e
	}()
	wg.Wait()
	if diagnostics != nil {
		diagnostics.IdentityGET = identityMetrics
		diagnostics.ChannelGET = channelMetrics
	}
	if identityErr != nil {
		return identityErr
	}
	if channelErr == nil && s.RouteKind == "guild_thread" {
		return r.validateRoute(ctx, channel, s)
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
func (r *RESTClient) SendGuarded(ctx context.Context, c Chunk, guard func() bool) SendResult {
	result, _ := r.SendGuardedMeasured(ctx, c, guard)
	return result
}

// SendGuardedMeasured returns this send's immutable measurement while holding
// the send lease. Callers must persist this value with its result, never read
// Diagnostics later to associate the mutable latest-send snapshot with a reply.
func (r *RESTClient) SendGuardedMeasured(ctx context.Context, c Chunk, guard func() bool) (SendResult, Diagnostics) {
	queued := time.Now()
	r.sendMu.Lock()
	defer r.sendMu.Unlock()
	d := Diagnostics{Operation: "send", SendLockWaitSeconds: time.Since(queued).Seconds()}
	result := r.sendGuardedLocked(ctx, c, guard, &d)
	return result, d
}
func (r *RESTClient) publishDiagnostics(started time.Time, result SendResult, d *Diagnostics) {
	d.Seconds = time.Since(started).Seconds()
	d.State = result.State
	d.Code = result.Code
	r.diagMu.Lock()
	r.diagnostics = *d
	r.diagMu.Unlock()
}
func (r *RESTClient) sendGuardedLocked(ctx context.Context, c Chunk, guard func() bool, d *Diagnostics) (result SendResult) {
	started := time.Now()
	defer func() { r.publishDiagnostics(started, result, d) }()
	if c.Source.ReplyKind == "interaction" {
		return SendResult{State: "failed", Code: "interaction_transport_required"}
	}
	if !r.settings.Policy.Allows(c.Source) || c.ReplyID == "" || c.Index < 0 || !utf8.ValidString(c.Text) || trimText(c.Text) == "" && c.Output == "" || TextUnits(c.Text) > 1900 {
		return SendResult{State: "failed", Code: "invalid_route_or_chunk"}
	}
	body, contentType, e := BuildReplyBody(c, ReplyStateDir(r.settings.DBPath))
	if e != nil {
		return SendResult{State: "failed", Code: symbolicCode(e.Error())}
	}
	pre := time.Now()
	e = r.preflightMeasured(ctx, c.Source, d)
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
	if guard != nil {
		ctx = context.WithValue(ctx, sendGuardContextKey{}, guard)
	}
	ctx = r.threadWriteContext(ctx, c.Source, 0)
	if c.Source.Control != "" {
		ctx = context.WithValue(ctx, controlTargetContextKey{}, c.Source)
	}
	post := time.Now()
	ctx, measured := newRequestMeasurement(ctx)
	defer func() {
		d.Post = measured.finish()
		d.PostSeconds = time.Since(post).Seconds()
		d.Reused = d.Post.Reused
	}()
	resp, e := r.requestContent(ctx, http.MethodPost, "/channels/"+c.Source.ConversationID+"/messages", body, contentType)
	if e != nil {
		if strings.HasPrefix(e.Error(), "preflight_") || strings.HasPrefix(e.Error(), "reaction_") || e.Error() == "source_not_current" {
			return SendResult{State: "failed", Code: e.Error()}
		}
		if errors.Is(e, errSendGuardChanged) {
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
	if e = readJSON(resp, &ack); e != nil {
		return SendResult{State: "uncertain", Code: e.Error()}
	}
	id, code := validateAck(ack, c, r.settings.ExpectedBotID)
	if id == "" {
		return SendResult{State: "uncertain", Code: "invalid_ack"}
	}
	return SendResult{State: "sent", MessageID: id, Code: code, OutputReceipt: replyAckReceipt(ack, c)}
}
func jsonString(v json.RawMessage) string {
	var s string
	if json.Unmarshal(v, &s) == nil {
		return s
	}
	return ""
}

// validateReplyContent permits only Discord's observed single terminal-LF
// removal after a nonspace rune. All other whitespace/content changes fail.
func validateReplyContent(actual, expected string) (string, bool) {
	if actual == expected {
		return "", true
	}
	if !strings.HasSuffix(expected, "\n") || len(expected) <= 1 || actual != expected[:len(expected)-1] {
		return "", false
	}
	previous, _ := utf8.DecodeLastRuneInString(actual)
	if isTextSpace(previous) {
		return "", false
	}
	return "ack_terminal_lf_removed", true
}
func requiredJSONString(v json.RawMessage) (string, bool) {
	var value *string
	if json.Unmarshal(v, &value) != nil || value == nil {
		return "", false
	}
	return *value, true
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
	actual, present := requiredJSONString(a["content"])
	code, valid := validateReplyContent(actual, c.Text)
	if !present || !valid {
		return "", ""
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
	if err := ValidateReplyOutputAck(a, c); err != nil {
		return "", ""
	}
	return id, code
}
func (r *RESTClient) feedback(ctx context.Context, s Envelope, method, suffix string) error {
	if s.Control != "" || !r.settings.Policy.Allows(s) {
		return errors.New("invalid_feedback_route")
	}
	extra := uint64(0)
	if s.RouteKind == "guild_thread" {
		if _, err := r.Identity(ctx); err != nil {
			return err
		}
		if method == http.MethodPut {
			extra = permissionAddReactions
		}
	}
	c, err := r.Channel(ctx, s.ConversationID)
	if err != nil {
		return err
	}
	if s.RouteKind == "guild_thread" {
		if r.routePermission != nil && !r.routePermission(s) {
			return errors.New("preflight_route_permissions_missing")
		}
		if err = r.validateThreadRoute(ctx, c, s, extra); err != nil {
			return err
		}
		ctx = r.threadWriteContext(ctx, s, extra)
	} else if err = r.validateRoute(ctx, c, s); err != nil {
		return err
	}
	var b []byte
	if method == http.MethodPost {
		b = []byte(`{}`)
	}
	resp, err := r.request(ctx, method, "/channels/"+s.ConversationID+suffix, b)
	if err != nil {
		if strings.HasPrefix(err.Error(), "preflight_") {
			return err
		}
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
	return r.waitLimitMeasured(ctx, method, path, nil)
}
func (r *RESTClient) waitLimitMeasured(ctx context.Context, method, path string, measurement *requestMeasurement) error {
	_, err := r.waitLimitObserved(ctx, method, path, measurement)
	return err
}
func (r *RESTClient) waitLimitObserved(ctx context.Context, method, path string, measurement *requestMeasurement) (bool, error) {
	waited := false
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
			return waited, nil
		}
		waited = true
		started := time.Now()
		timer := time.NewTimer(wait)
		var err error
		select {
		case <-ctx.Done():
			timer.Stop()
			err = errors.New("rate_limit_wait_cancelled")
		case <-timer.C:
		}
		measurement.update(func() { measurement.metrics.RateLimitWaitSeconds += time.Since(started).Seconds() })
		if err != nil {
			return waited, err
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
