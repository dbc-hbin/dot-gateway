package reporting

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"time"
	"unicode/utf8"
)

const apiBase = "https://discord.com/api/v10"
const requiredPermissions uint64 = 1<<10 | 1<<11 | 1<<16 // view, send, read history

type Client struct {
	target Target
	token  string
	http   *http.Client
}

func (c *Client) String() string   { return "DiscordReportClient{credentials:[REDACTED]}" }
func (c *Client) GoString() string { return c.String() }

// NewLiveClient's sole credential source is TokenPath. Its API origin is fixed.
func NewLiveClient(target Target, proxyPath string) (*Client, error) {
	if err := target.Validate(); err != nil {
		return nil, err
	}
	if target != (Target{BotID: "100000000000000001", GuildID: "100000000000000002", ChannelID: "100000000000000003"}) {
		return nil, errors.New("target_not_approved_for_live_delivery")
	}
	proxy, err := loadProxy(proxyPath)
	if err != nil {
		return nil, err
	}
	token, err := loadTokenFile(TokenPath)
	if err != nil {
		return nil, err
	}
	tr := &http.Transport{
		Proxy: func(r *http.Request) (*url.URL, error) {
			if r.URL.Scheme != "https" || r.URL.Host != "discord.com" {
				return nil, errors.New("unexpected_api_origin")
			}
			return proxy, nil
		},
		DialContext:     (&net.Dialer{Timeout: 15 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12}, TLSHandshakeTimeout: 15 * time.Second, ResponseHeaderTimeout: 20 * time.Second,
		IdleConnTimeout: 30 * time.Second, MaxConnsPerHost: 2, MaxIdleConnsPerHost: 2, ForceAttemptHTTP2: false,
		TLSNextProto: map[string]func(string, *tls.Conn) http.RoundTripper{},
	}
	return &Client{target: target, token: token, http: &http.Client{Transport: tr, Timeout: 30 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}
func (c *Client) Close() { c.http.CloseIdleConnections() }
func (c *Client) request(ctx context.Context, method, path string, body []byte) (*http.Response, error) {
	var reader io.Reader
	// A non-replayable body and no idempotency header prevent Go Transport from
	// replaying POSTs, even if an idle connection becomes stale.
	if body != nil {
		reader = io.NopCloser(bytes.NewReader(body))
	}
	req, err := http.NewRequestWithContext(ctx, method, apiBase+path, reader)
	if err != nil {
		return nil, errors.New("request_build_failed")
	}
	req.GetBody = nil
	if body != nil {
		req.ContentLength = int64(len(body))
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Authorization", "Bot "+c.token)
	req.Header.Set("User-Agent", "DiscordReportPublisher/1.0 (Go)")
	return c.http.Do(req)
}
func responseJSON(resp *http.Response, out any) error {
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 1048577))
	if err != nil || len(b) > 1048576 || !utf8.Valid(b) || json.Unmarshal(b, out) != nil {
		return errors.New("invalid_api_response")
	}
	return nil
}
func (c *Client) get(ctx context.Context, path string, out any) error {
	resp, err := c.request(ctx, http.MethodGet, path, nil)
	if err != nil {
		if resp != nil {
			resp.Body.Close()
		}
		return errors.New("read_transport_failed")
	}
	if resp.StatusCode != 200 {
		resp.Body.Close()
		return fmt.Errorf("read_http_%d", resp.StatusCode)
	}
	return responseJSON(resp, out)
}

type user struct {
	ID  string `json:"id"`
	Bot bool   `json:"bot"`
}
type overwrite struct {
	ID    string `json:"id"`
	Type  *int   `json:"type"`
	Allow string `json:"allow"`
	Deny  string `json:"deny"`
}
type channel struct {
	ID         string       `json:"id"`
	GuildID    string       `json:"guild_id"`
	Type       *int         `json:"type"`
	Name       string       `json:"name"`
	Overwrites *[]overwrite `json:"permission_overwrites"`
}
type guild struct {
	ID      string `json:"id"`
	OwnerID string `json:"owner_id"`
}
type role struct {
	ID          string `json:"id"`
	Permissions string `json:"permissions"`
}
type member struct {
	User    user      `json:"user"`
	Roles   *[]string `json:"roles"`
	Pending bool      `json:"pending"`
	Timeout *string   `json:"communication_disabled_until"`
}
type Inspection struct {
	Target      Target `json:"target"`
	Verified    bool   `json:"verified"`
	ChannelName string `json:"channel_name"`
	Permissions string `json:"permissions"`
	CheckedAt   string `json:"checked_at"`
}

func permissionBits(s string) (uint64, error) {
	if s == "" {
		return 0, errors.New("invalid_permissions")
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return 0, errors.New("invalid_permissions")
		}
	}
	p, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, errors.New("invalid_permissions")
	}
	return p, nil
}
func effectivePermissions(t Target, g guild, ch channel, m member, roles []role, now time.Time) (uint64, error) {
	if g.ID != t.GuildID || !snowflake(g.OwnerID) || ch.ID != t.ChannelID || ch.GuildID != t.GuildID || ch.Type == nil || *ch.Type != 0 || ch.Overwrites == nil || m.User.ID != t.BotID || !m.User.Bot || m.Roles == nil || m.Pending {
		return 0, errors.New("preflight_scope_or_member_mismatch")
	}
	if m.Timeout != nil && *m.Timeout != "" {
		until, err := time.Parse(time.RFC3339Nano, *m.Timeout)
		if err != nil || until.After(now) {
			return 0, errors.New("preflight_member_timed_out")
		}
	}
	perms := map[string]uint64{}
	for _, r := range roles {
		p, err := permissionBits(r.Permissions)
		if err != nil || !snowflake(r.ID) {
			return 0, errors.New("preflight_invalid_role")
		}
		if _, exists := perms[r.ID]; exists {
			return 0, errors.New("preflight_duplicate_role")
		}
		perms[r.ID] = p
	}
	base, ok := perms[t.GuildID]
	if !ok {
		return 0, errors.New("preflight_missing_everyone_role")
	}
	assigned := map[string]bool{}
	for _, id := range *m.Roles {
		p, exists := perms[id]
		if !exists || assigned[id] || id == t.GuildID {
			return 0, errors.New("preflight_invalid_member_role")
		}
		assigned[id] = true
		base |= p
	}
	type parsed struct {
		id          string
		kind        int
		allow, deny uint64
	}
	var os []parsed
	seen := map[string]bool{}
	for _, o := range *ch.Overwrites {
		a, e1 := permissionBits(o.Allow)
		d, e2 := permissionBits(o.Deny)
		if !snowflake(o.ID) || o.Type == nil || (*o.Type != 0 && *o.Type != 1) || e1 != nil || e2 != nil {
			return 0, errors.New("preflight_invalid_overwrite")
		}
		key := fmt.Sprintf("%d:%s", *o.Type, o.ID)
		if seen[key] {
			return 0, errors.New("preflight_duplicate_overwrite")
		}
		seen[key] = true
		os = append(os, parsed{o.ID, *o.Type, a, d})
	}
	if g.OwnerID == t.BotID || base&(1<<3) != 0 {
		return ^uint64(0), nil
	}
	for _, o := range os {
		if o.kind == 0 && o.id == t.GuildID {
			base = (base &^ o.deny) | o.allow
		}
	}
	var allow, deny uint64
	for _, o := range os {
		if o.kind == 0 && assigned[o.id] {
			allow |= o.allow
			deny |= o.deny
		}
	}
	base = (base &^ deny) | allow
	for _, o := range os {
		if o.kind == 1 && o.id == t.BotID {
			base = (base &^ o.deny) | o.allow
		}
	}
	return base, nil
}

// Inspect performs only GETs and does not open or modify the publisher ledger.
func (c *Client) Inspect(ctx context.Context) (Inspection, error) {
	out := Inspection{Target: c.target}
	var u user
	if err := c.get(ctx, "/users/@me", &u); err != nil {
		return out, err
	}
	if u.ID != c.target.BotID || !u.Bot {
		return out, errors.New("preflight_identity_mismatch")
	}
	var ch channel
	if err := c.get(ctx, "/channels/"+c.target.ChannelID, &ch); err != nil {
		return out, err
	}
	// Verify channel immediately, before making any other guild request.
	if ch.ID != c.target.ChannelID || ch.GuildID != c.target.GuildID || ch.Type == nil || *ch.Type != 0 {
		return out, errors.New("preflight_channel_mismatch")
	}
	var g guild
	if err := c.get(ctx, "/guilds/"+c.target.GuildID, &g); err != nil {
		return out, err
	}
	var m member
	if err := c.get(ctx, "/guilds/"+c.target.GuildID+"/members/"+c.target.BotID, &m); err != nil {
		return out, err
	}
	var roles []role
	if err := c.get(ctx, "/guilds/"+c.target.GuildID+"/roles", &roles); err != nil {
		return out, err
	}
	p, err := effectivePermissions(c.target, g, ch, m, roles, time.Now())
	if err != nil {
		return out, err
	}
	if p&requiredPermissions != requiredPermissions {
		return out, errors.New("preflight_permissions_missing")
	}
	out.Verified = true
	out.ChannelName = ch.Name
	out.Permissions = strconv.FormatUint(p, 10)
	out.CheckedAt = time.Now().UTC().Format(time.RFC3339Nano)
	return out, nil
}

type message struct {
	ID        string          `json:"id"`
	ChannelID string          `json:"channel_id"`
	Author    user            `json:"author"`
	Content   string          `json:"content"`
	Nonce     json.RawMessage `json:"nonce"`
	WebhookID string          `json:"webhook_id"`
	Type      *int            `json:"type"`
}

func validateMessage(m message, t Target, text, expectedID, expectedNonce string) error {
	if !snowflake(m.ID) || (expectedID != "" && m.ID != expectedID) || m.ChannelID != t.ChannelID || m.Author.ID != t.BotID || !m.Author.Bot || m.Content != text || m.WebhookID != "" || m.Type == nil || *m.Type != 0 {
		return errors.New("message_verification_mismatch")
	}
	if expectedNonce != "" && len(m.Nonce) != 0 {
		var n string
		if json.Unmarshal(m.Nonce, &n) != nil || n != expectedNonce {
			return errors.New("message_nonce_mismatch")
		}
	}
	return nil
}
func (c *Client) readback(ctx context.Context, id, text string) error {
	if !snowflake(id) {
		return errors.New("invalid_message_id")
	}
	var m message
	if err := c.get(ctx, "/channels/"+c.target.ChannelID+"/messages/"+id, &m); err != nil {
		return err
	}
	return validateMessage(m, c.target, text, id, "")
}
func (c *Client) postOnce(ctx context.Context, text, nonce string) (id, state, code string) {
	resp, err := c.request(ctx, http.MethodPost, "/channels/"+c.target.ChannelID+"/messages", postPayload(text, nonce))
	if err != nil {
		if resp != nil {
			resp.Body.Close()
		}
		return "", "uncertain", "post_transport_failed"
	}
	if resp.StatusCode != 200 && resp.StatusCode != 201 {
		resp.Body.Close()
		state = "uncertain"
		switch resp.StatusCode {
		case 400, 401, 403, 404, 405, 413, 429:
			state = "failed"
		}
		return "", state, fmt.Sprintf("post_http_%d", resp.StatusCode)
	}
	var m message
	if responseJSON(resp, &m) != nil {
		return "", "uncertain", "post_ack_invalid"
	}
	if validateMessage(m, c.target, text, "", nonce) != nil {
		return "", "uncertain", "post_ack_mismatch"
	}
	return m.ID, "acknowledged", ""
}
