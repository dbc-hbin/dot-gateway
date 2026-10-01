package bridge

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/bwmarrin/discordgo"
)

// validAttachmentURL never authorizes a typed URL or an embed URL. Callers must
// additionally obtain this URL from the exact freshly authorized source message.
func validAttachmentURL(raw, channelID, attachmentID string) error {
	fail := errors.New("invalid_cdn_url")
	if len(raw) > 4096 || !Snowflake(channelID) || !Snowflake(attachmentID) {
		return fail
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.User != nil || u.Fragment != "" || u.Opaque != "" || u.Port() != "" || (u.Host != "cdn.discordapp.com" && u.Host != "media.discordapp.net") {
		return fail
	}
	// A path is bound to the admitted conversation and captured attachment ID.
	// Escaped slashes, dots and backslashes cannot change its segment interpretation.
	parts := strings.Split(u.EscapedPath(), "/")
	if len(parts) != 5 || parts[0] != "" || parts[1] != "attachments" || parts[2] != channelID || parts[3] != attachmentID || parts[4] == "" {
		return fail
	}
	filename, err := url.PathUnescape(parts[4])
	if err != nil || filename == "." || filename == ".." || strings.ContainsAny(filename, "/\\\x00\r\n") || !utf8.ValidString(filename) {
		return fail
	}
	q, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return fail
	}
	for k, values := range q {
		if len(values) != 1 {
			return fail
		}
		switch k {
		case "ex", "is":
			if len(values[0]) < 1 || len(values[0]) > 16 {
				return fail
			}
			if _, e := strconv.ParseUint(values[0], 16, 64); e != nil {
				return fail
			}
		case "hm":
			if len(values[0]) != 64 {
				return fail
			}
			if _, e := hex.DecodeString(values[0]); e != nil {
				return fail
			}
		default:
			return fail
		}
	}
	return nil
}

// Public-unicast-only DNS pinning prevents private/link-local targets, including
// DNS rebinding. No environment proxy, cookie jar, bot token, URL credentials,
// client certificate, redirect, HTTP downgrade, or arbitrary port is used.
func publicMediaIP(ip netip.Addr) bool {
	ip = ip.Unmap()
	if !ip.IsValid() || !ip.IsGlobalUnicast() || ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsMulticast() || (ip.Is6() && !netip.MustParsePrefix("2000::/3").Contains(ip)) {
		return false
	}
	for _, cidr := range []string{"0.0.0.0/8", "100.64.0.0/10", "192.0.0.0/24", "192.0.2.0/24", "192.88.99.0/24", "198.18.0.0/15", "198.51.100.0/24", "203.0.113.0/24", "240.0.0.0/4", "2001:db8::/32", "2001::/23", "2002::/16", "64:ff9b::/96", "64:ff9b:1::/48", "100::/64"} {
		if netip.MustParsePrefix(cidr).Contains(ip) {
			return false
		}
	}
	return true
}

// The explicitly configured credential-free proxy is a trusted relay, not an
// arbitrary attachment destination. In proxy mode it resolves the fixed CDN
// hostname; this process cannot inspect that remote DNS result. TLS still
// authenticates the exact CDN. Direct mode pins public addresses locally.
func newMediaHTTPClient(config *ProxyConfig) (*http.Client, error) {
	if config == nil {
		config = &ProxyConfig{}
	}
	p, err := NewProxyConfig(config.URL, config.NoProxy)
	if err != nil {
		return nil, err
	}
	route, err := p.DiscordRoute()
	if err != nil {
		return nil, err
	}
	for _, host := range []string{"cdn.discordapp.com", "media.discordapp.net"} {
		if p.ForHost(host, 443) != route {
			return nil, errors.New("media_proxy_route_mismatch")
		}
	}
	var relay *url.URL
	if route != "" {
		relay, err = url.Parse(route)
		if err != nil {
			return nil, errors.New("invalid_proxy_url")
		}
	}
	transport := &http.Transport{DisableCompression: true, DisableKeepAlives: true, TLSHandshakeTimeout: 5 * time.Second, ResponseHeaderTimeout: 10 * time.Second, MaxResponseHeaderBytes: 32 * 1024}
	transport.Proxy = func(req *http.Request) (*url.URL, error) {
		if !allowedMediaRequest(req) {
			return nil, errors.New("media_endpoint_denied")
		}
		return relay, nil
	}
	transport.GetProxyConnectHeader = func(_ context.Context, proxyURL *url.URL, target string) (http.Header, error) {
		if relay == nil || proxyURL.String() != relay.String() || (target != "cdn.discordapp.com:443" && target != "media.discordapp.net:443") {
			return nil, errors.New("media_proxy_endpoint_denied")
		}
		return make(http.Header), nil
	}
	transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		if relay != nil {
			if address != relay.Host {
				return nil, errors.New("media_proxy_endpoint_denied")
			}
			return (&net.Dialer{Timeout: 5 * time.Second}).DialContext(ctx, network, address)
		}
		host, port, err := net.SplitHostPort(address)
		if err != nil || port != "443" || (host != "cdn.discordapp.com" && host != "media.discordapp.net") {
			return nil, errors.New("media_endpoint_denied")
		}
		addresses, err := net.DefaultResolver.LookupNetIP(ctx, "ip", host)
		if err != nil || len(addresses) == 0 {
			return nil, errors.New("media_dns_failed")
		}
		for _, ip := range addresses {
			if !publicMediaIP(ip) {
				return nil, errors.New("media_private_destination")
			}
		}
		var last error
		for _, ip := range addresses {
			c, e := (&net.Dialer{Timeout: 5 * time.Second}).DialContext(ctx, network, net.JoinHostPort(ip.String(), "443"))
			if e == nil {
				return c, nil
			}
			last = e
		}
		return nil, last
	}
	return &http.Client{Transport: transport, Timeout: 20 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, nil
}
func allowedMediaRequest(req *http.Request) bool {
	if req.Method != http.MethodGet || req.Header.Get("Authorization") != "" || req.Header.Get("Proxy-Authorization") != "" || req.Header.Get("Cookie") != "" || req.Header.Get("Referer") != "" {
		return false
	}
	parts := strings.Split(req.URL.EscapedPath(), "/")
	return len(parts) == 5 && validAttachmentURL(req.URL.String(), parts[2], parts[3]) == nil
}

type MaterializedAttachment struct {
	AttachmentID   string `json:"attachment_id"`
	Path           string `json:"path"`
	Filename       string `json:"filename"`
	ContentType    string `json:"content_type"`
	Size           int64  `json:"size"`
	SHA256         string `json:"sha256"`
	Trust          string `json:"trust"`
	Interpretation string `json:"interpretation"`
}

// ClaimedEnvelope is read-only and never extends ownership. Materialization
// cannot retrieve arbitrary history or consume work that has not been claimed.
func (s *Store) ClaimedEnvelope(id, claim string) (Envelope, error) {
	if id == "" || claim == "" {
		return Envelope{}, ErrClaim
	}
	v, err := s.call(func(db *storeConn) (any, error) {
		rows, err := readInbound(db, "SELECT id,state,claim,lease_until,envelope FROM inbound WHERE id=?", id)
		if err != nil {
			return nil, err
		}
		if len(rows) != 1 || rows[0].state != "claimed" || rows[0].claim != claim || rows[0].lease <= epoch() || !s.policy.Accepts(rows[0].event) {
			return nil, ErrClaim
		}
		current, err := sourceCurrentDB(db, rows[0].event)
		if err != nil {
			return nil, err
		}
		if !current {
			return nil, ErrClaim
		}
		return rows[0].event, nil
	})
	if err != nil {
		return Envelope{}, err
	}
	return v.(Envelope), nil
}

// MaterializeAttachment is an explicit live/network command. It uses the
// existing secure credential loader only for Discord API identity/source GETs;
// a completely separate unauthenticated client retrieves the bounded CDN bytes.
func (r *RESTClient) MaterializeAttachment(ctx context.Context, s *Store, id, claim, attachmentID string) (*MaterializedAttachment, error) {
	client, err := newMediaHTTPClient(r.settings.ProxyConfig)
	if err != nil {
		return nil, err
	}
	defer client.CloseIdleConnections()
	return r.materializeAttachment(ctx, s, id, claim, attachmentID, client)
}
func (r *RESTClient) materializeAttachment(ctx context.Context, s *Store, id, claim, attachmentID string, client *http.Client) (*MaterializedAttachment, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	original, err := s.ClaimedEnvelope(id, claim)
	if err != nil {
		return nil, err
	}
	if !r.settings.Policy.Accepts(original) || !Snowflake(attachmentID) {
		return nil, errors.New("media_source_unauthorized")
	}
	meta, err := original.Media.Metadata()
	if err != nil {
		return nil, err
	}
	var wanted *AttachmentMetadata
	for i := range meta.Attachments {
		if meta.Attachments[i].ID == attachmentID {
			wanted = &meta.Attachments[i]
			break
		}
	}
	if wanted == nil {
		return nil, errors.New("unknown_attachment")
	}
	if wanted.Materialization != "available_via_materialize" {
		return nil, fmt.Errorf("media_unavailable_%s", wanted.UnavailableReason)
	}
	if err = r.preflight(ctx, original); err != nil {
		return nil, err
	}
	var source discordgo.Message
	if err = r.get(ctx, "/channels/"+original.ConversationID+"/messages/"+original.EventID, &source); err != nil {
		return nil, err
	}
	if source.ID != original.EventID || source.ChannelID != original.ConversationID || source.Author == nil || source.Author.ID != original.SenderID || source.Author.Bot || source.WebhookID != "" || (source.GuildID != "" && source.GuildID != original.GuildID) || (source.Type != discordgo.MessageTypeDefault && source.Type != discordgo.MessageTypeReply) {
		return nil, errors.New("media_source_mismatch")
	}
	source.GuildID = original.GuildID
	if source.Content != original.Text || (original.ContentHash != "" && MessageContentFingerprint(&source) != original.ContentHash) {
		return nil, errors.New("media_source_changed")
	}
	refreshed, err := ProjectMedia(&source).Metadata()
	if err != nil {
		return nil, errors.New("media_source_changed")
	}
	foundCaptured := false
	for _, candidate := range refreshed.Attachments {
		if candidate.ID == attachmentID {
			foundCaptured = candidate == *wanted
			break
		}
	}
	if !foundCaptured {
		return nil, errors.New("media_attachment_mismatch")
	}
	var attachment *discordgo.MessageAttachment
	for _, a := range source.Attachments {
		if a != nil && a.ID == attachmentID {
			if attachment != nil {
				return nil, errors.New("media_attachment_mismatch")
			}
			attachment = a
		}
	}
	if attachment == nil || int64(attachment.Size) != wanted.Size || attachment.Filename != wanted.Filename || mediaContentType(attachment.ContentType) != wanted.ContentType {
		return nil, errors.New("media_attachment_mismatch")
	}
	if err = validAttachmentURL(attachment.URL, source.ChannelID, attachmentID); err != nil {
		return nil, err
	}
	if r.settings.DBPath == "" {
		return nil, errors.New("media_directory_unconfigured")
	}
	destination, err := openMediaDirectory(r.settings.DBPath + ".media")
	if err != nil {
		return nil, err
	}
	defer destination.Close()
	if err = lockMediaDirectory(ctx, destination); err != nil {
		return nil, err
	}
	defer syscall.Flock(int(destination.Fd()), syscall.LOCK_UN)
	if err = checkMediaQuota(destination, wanted.Size); err != nil {
		return nil, err
	}
	// A request gets only fixed non-credential headers; download errors never echo
	// the signed URL, API response body, filename, or caller-supplied content.
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, attachment.URL, nil)
	if err != nil {
		return nil, errors.New("media_request_failed")
	}
	req.Header.Set("Accept-Encoding", "identity")
	req.Header.Set("User-Agent", "DotMediaBridge/1.0")
	response, err := client.Do(req)
	if err != nil {
		return nil, errors.New("media_download_failed")
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		return nil, fmt.Errorf("media_http_%d", response.StatusCode)
	}
	if response.Header.Get("Content-Encoding") != "" && response.Header.Get("Content-Encoding") != "identity" {
		return nil, errors.New("media_encoding_denied")
	}
	if response.ContentLength > MaxAttachmentBytes || (response.ContentLength >= 0 && response.ContentLength != wanted.Size) {
		return nil, errors.New("media_size_mismatch")
	}
	declared, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil || !compatibleMediaType(wanted.ContentType, strings.ToLower(declared)) {
		return nil, errors.New("media_content_type_mismatch")
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, MaxAttachmentBytes+1))
	if err != nil {
		return nil, errors.New("media_read_failed")
	}
	if int64(len(body)) != wanted.Size || len(body) > MaxAttachmentBytes {
		return nil, errors.New("media_size_mismatch")
	}
	detected := mediaContentType(http.DetectContentType(body))
	if !sniffedMediaMatches(wanted.ContentType, detected, body) {
		return nil, errors.New("media_content_sniff_mismatch")
	}
	current, err := s.ClaimedEnvelope(id, claim)
	if err != nil || current != original {
		return nil, ErrClaim
	}
	if err = ctx.Err(); err != nil {
		return nil, errors.New("media_deadline_exceeded")
	}
	sum := sha256.Sum256(body)
	path, err := writeMediaFile(destination, original, attachmentID, body)
	if err != nil {
		return nil, err
	}
	return &MaterializedAttachment{AttachmentID: attachmentID, Path: path, Filename: wanted.Filename, ContentType: wanted.ContentType, Size: int64(len(body)), SHA256: hex.EncodeToString(sum[:]), Trust: "untrusted_attachment_bytes", Interpretation: "not_interpreted"}, nil
}
func compatibleMediaType(want, got string) bool {
	if got == want {
		return true
	}
	if got == "application/octet-stream" {
		return true
	} // bytes still require a positive MIME sniff below
	if (want == "audio/mp4" && got == "video/mp4") || (want == "audio/webm" && got == "video/webm") || (want == "audio/x-wav" && got == "audio/wave") || (want == "audio/wav" && (got == "audio/wave" || got == "audio/x-wav")) {
		return true
	}
	return false
}
func sniffedMediaMatches(want, got string, body []byte) bool {
	if !supportedAttachmentType(want) {
		return false
	}
	if want == "text/plain" || want == "text/csv" || want == "application/json" {
		if got != "text/plain" || !utf8.Valid(body) || bytes.IndexByte(body, 0) >= 0 {
			return false
		}
		if want == "application/json" {
			return json.Valid(body)
		}
		return true
	}
	if want == "audio/flac" {
		return len(body) >= 4 && string(body[:4]) == "fLaC"
	}
	if want == "audio/ogg" {
		return got == "application/ogg" || got == "audio/ogg"
	}
	return got != "application/octet-stream" && compatibleMediaType(want, got)
}

func openMediaDirectory(path string) (*os.File, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return nil, errors.New("media_directory_invalid")
	}
	if err = os.Mkdir(absolute, 0700); err != nil && !errors.Is(err, os.ErrExist) {
		return nil, errors.New("media_directory_failed")
	}
	fd, err := syscall.Open(absolute, syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
	if err != nil {
		return nil, errors.New("media_directory_invalid")
	}
	f := os.NewFile(uintptr(fd), absolute)
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, errors.New("media_directory_invalid")
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !info.IsDir() || info.Mode().Perm()&0077 != 0 || !ok || int(st.Uid) != os.Getuid() {
		f.Close()
		return nil, errors.New("media_directory_not_private")
	}
	return f, nil
}
func lockMediaDirectory(ctx context.Context, f *os.File) error {
	for {
		err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			return nil
		}
		if err != syscall.EWOULDBLOCK && err != syscall.EAGAIN {
			return errors.New("media_directory_lock_failed")
		}
		select {
		case <-ctx.Done():
			return errors.New("media_directory_busy")
		case <-time.After(20 * time.Millisecond):
		}
	}
}
func checkMediaQuota(directory *os.File, incoming int64) error {
	if incoming < 0 || incoming > MaxAttachmentBytes {
		return errors.New("media_size_mismatch")
	}
	entries, err := directory.ReadDir(101)
	if err != nil && err != io.EOF {
		return errors.New("media_directory_read_failed")
	}
	if len(entries) >= 100 {
		return errors.New("media_directory_capacity")
	}
	total := incoming
	for _, e := range entries {
		info, err := e.Info()
		if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
			return errors.New("media_directory_invalid_entry")
		}
		st, ok := info.Sys().(*syscall.Stat_t)
		if !ok || int(st.Uid) != os.Getuid() {
			return errors.New("media_directory_invalid_entry")
		}
		if info.Size() > 100*1024*1024 {
			return errors.New("media_directory_capacity")
		}
		total += info.Size()
		if total > 100*1024*1024 {
			return errors.New("media_directory_capacity")
		}
	}
	return nil
}
func writeMediaFile(directory *os.File, source Envelope, attachmentID string, body []byte) (string, error) {
	random, err := uuidHex()
	if err != nil {
		return "", errors.New("media_file_failed")
	}
	temp := ".download-" + random
	fd, err := syscall.Openat(int(directory.Fd()), temp, syscall.O_CREAT|syscall.O_EXCL|syscall.O_NOFOLLOW|syscall.O_WRONLY|syscall.O_CLOEXEC, 0600)
	if err != nil {
		return "", errors.New("media_file_failed")
	}
	f := os.NewFile(uintptr(fd), temp)
	defer f.Close()
	defer syscall.Unlinkat(int(directory.Fd()), temp)
	if _, err = f.Write(body); err != nil {
		return "", errors.New("media_file_failed")
	}
	if err = f.Sync(); err != nil {
		return "", errors.New("media_file_failed")
	}
	binding := sha256.Sum256([]byte(source.ConversationID + ":" + source.EventID + ":" + attachmentID))
	// An inert .bin extension is deliberate. The declared original name is display
	// metadata only; neither filenames nor MIME ever select a command or parser.
	name := "attachment-" + hex.EncodeToString(binding[:]) + ".bin"
	if err = syscall.Renameat(int(directory.Fd()), temp, int(directory.Fd()), name); err != nil {
		return "", errors.New("media_file_failed")
	}
	if err = directory.Sync(); err != nil {
		return "", errors.New("media_file_failed")
	}
	return filepath.Join(directory.Name(), name), nil
}
