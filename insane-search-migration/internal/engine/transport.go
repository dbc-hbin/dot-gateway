package engine

import (
	"bufio"
	"compress/flate"
	"compress/gzip"
	"compress/zlib"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/netip"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/andybalholm/brotli"
	utls "github.com/refraction-networking/utls"
	"golang.org/x/net/http2"
	"golang.org/x/net/publicsuffix"
)

const MaxResponseBytes = 25 * 1024 * 1024

var tlsProfiles = map[string]utls.ClientHelloID{"safari": utls.HelloSafari_Auto, "safari_ios": utls.HelloIOS_Auto, "chrome": utls.HelloChrome_Auto, "chrome_android": utls.HelloAndroid_11_OkHttp, "firefox": utls.HelloFirefox_Auto, "edge": utls.HelloEdge_Auto}

func AvailableTLSProfiles() []string {
	return []string{"safari", "safari_ios", "chrome", "chrome_android", "firefox", "edge"}
}
func TLSProfileVersion(name string) string {
	v, ok := tlsProfiles[name]
	if !ok {
		return "unsupported"
	}
	return v.Str()
}

type session struct {
	warmed    bool
	jar       http.CookieJar
	cache     utls.ClientSessionCache
	userAgent string
}
type Transport struct {
	Guard      URLGuard
	Proxy      func(*http.Request) (*url.URL, error)
	mu         sync.Mutex
	sessions   map[string]*session
	Dialer     *net.Dialer
	TLSRootCAs *tls.Config
}

func NewTransport() *Transport {
	return &Transport{Guard: URLGuard{}, Proxy: http.ProxyFromEnvironment, sessions: map[string]*session{}, Dialer: &net.Dialer{Timeout: 25 * time.Second, KeepAlive: 30 * time.Second}}
}
func (t *Transport) getSession(host, profile string) *session {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.sessions == nil {
		t.sessions = map[string]*session{}
	}
	key := host + "\x00" + profile
	if ent := t.sessions[key]; ent != nil {
		return ent
	}
	jar, _ := cookiejar.New(&cookiejar.Options{PublicSuffixList: publicsuffix.List})
	ent := &session{jar: jar, cache: utls.NewLRUClientSessionCache(64)}
	if len(t.sessions) >= 256 {
		for k := range t.sessions {
			delete(t.sessions, k)
			break
		}
	}
	t.sessions[key] = ent
	return ent
}
func (t *Transport) InjectCookies(raw, profile string, cookies []Cookie, userAgent string) error {
	u, e := url.Parse(raw)
	if e != nil {
		return e
	}
	ent := t.getSession(u.Hostname(), profile)
	safe := []*http.Cookie{}
	for _, c := range cookies {
		domain := strings.TrimPrefix(strings.ToLower(c.Domain), ".")
		host := strings.ToLower(u.Hostname())
		if domain != "" && host != domain && !strings.HasSuffix(host, "."+domain) {
			continue
		}
		cookie := &http.Cookie{Name: c.Name, Value: c.Value, Domain: c.Domain, Path: c.Path, Secure: c.Secure, HttpOnly: c.HTTPOnly}
		if c.ExpiresUnix > 0 {
			cookie.Expires = time.Unix(c.ExpiresUnix, 0)
		}
		safe = append(safe, cookie)
	}
	ent.jar.SetCookies(u, safe)
	t.mu.Lock()
	ent.userAgent = userAgent
	t.mu.Unlock()
	return nil
}
func (t *Transport) Do(ctx context.Context, input HTTPRequest) (HTTPResponse, error) {
	if input.Timeout <= 0 {
		input.Timeout = 25 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, input.Timeout)
	defer cancel()
	current := input.URL
	for redirects := 0; redirects <= 10; redirects++ {
		u, ips, e := t.Guard.Resolve(ctx, current)
		if e != nil {
			return HTTPResponse{}, e
		}
		response, e := t.single(ctx, u, ips, input)
		if e != nil {
			return response, e
		}
		switch response.Status {
		case 301, 302, 303, 307, 308:
			loc := response.Headers.Get("Location")
			if loc == "" {
				return response, nil
			}
			next, e := u.Parse(loc)
			if e != nil {
				return HTTPResponse{}, fmt.Errorf("invalid redirect location")
			}
			current = next.String()
			continue
		}
		return response, nil
	}
	return HTTPResponse{}, fmt.Errorf("too_many_redirects")
}
func (t *Transport) single(ctx context.Context, u *url.URL, ips []netip.Addr, input HTTPRequest) (HTTPResponse, error) {
	profile := input.Profile
	if profile == "" {
		profile = "safari"
	}
	hello, ok := tlsProfiles[profile]
	if !ok {
		return HTTPResponse{}, fmt.Errorf("unsupported TLS profile %q", profile)
	}
	ent := t.getSession(u.Hostname(), profile)
	req, e := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if e != nil {
		return HTTPResponse{}, e
	}
	req.Header = make(http.Header)
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
	req.Header.Set("Accept-Language", "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7")
	req.Header.Set("Accept-Encoding", "gzip, deflate, br")
	req.Header.Set("User-Agent", userAgent(profile))
	if input.Referer != "" {
		req.Header.Set("Referer", input.Referer)
	}
	for k, values := range input.Headers {
		req.Header[k] = append([]string{}, values...)
	}
	t.mu.Lock()
	injectedUA := ent.userAgent
	t.mu.Unlock()
	if injectedUA != "" {
		req.Header.Set("User-Agent", injectedUA)
	}
	for _, c := range ent.jar.Cookies(u) {
		req.AddCookie(c)
	}
	port := u.Port()
	if port == "" {
		if u.Scheme == "https" {
			port = "443"
		} else {
			port = "80"
		}
	}
	dialer := t.Dialer
	if dialer == nil {
		dialer = &net.Dialer{Timeout: 25 * time.Second}
	}
	var proxy *url.URL
	if t.Proxy != nil {
		proxy, e = t.Proxy(req)
		if e != nil {
			return HTTPResponse{}, e
		}
		if proxy != nil && proxy.User != nil {
			return HTTPResponse{}, fmt.Errorf("proxy credentials not accepted")
		}
	}
	var conn net.Conn
	for _, ip := range ips {
		target := net.JoinHostPort(ip.String(), port)
		conn, e = t.dial(ctx, dialer, proxy, target)
		if e == nil {
			break
		}
	}
	if e != nil {
		return HTTPResponse{}, e
	}
	defer conn.Close()
	baseConn := conn
	cancelIO := context.AfterFunc(ctx, func() { baseConn.Close() })
	defer cancelIO()
	if deadline, ok := ctx.Deadline(); ok {
		conn.SetDeadline(deadline)
	}
	negotiated := "http/1.1"
	if u.Scheme == "https" {
		config := &utls.Config{ServerName: u.Hostname(), ClientSessionCache: ent.cache, MinVersion: tls.VersionTLS12}
		if t.TLSRootCAs != nil {
			config.RootCAs = t.TLSRootCAs.RootCAs
		}
		tlsConn := utls.UClient(conn, config, hello)
		if e = tlsConn.HandshakeContext(ctx); e != nil {
			return HTTPResponse{}, fmt.Errorf("utls %s handshake: %w", profile, e)
		}
		conn = tlsConn
		negotiated = tlsConn.ConnectionState().NegotiatedProtocol
	}
	var resp *http.Response
	if negotiated == "h2" {
		h2 := &http2.Transport{}
		cc, e := h2.NewClientConn(conn)
		if e != nil {
			return HTTPResponse{}, e
		}
		defer cc.Close()
		resp, e = cc.RoundTrip(req)
		if e != nil {
			return HTTPResponse{}, e
		}
	} else {
		if e = req.Write(conn); e != nil {
			return HTTPResponse{}, e
		}
		resp, e = http.ReadResponse(bufio.NewReader(conn), req)
		if e != nil {
			return HTTPResponse{}, e
		}
	}
	defer resp.Body.Close()
	var reader io.Reader = resp.Body
	var closeDecoder io.Closer
	switch strings.ToLower(resp.Header.Get("Content-Encoding")) {
	case "gzip":
		r, e := gzip.NewReader(resp.Body)
		if e != nil {
			return HTTPResponse{}, e
		}
		reader = r
		closeDecoder = r
	case "br":
		reader = brotli.NewReader(resp.Body)
	case "deflate":
		buffered := bufio.NewReader(resp.Body)
		header, _ := buffered.Peek(2)
		if len(header) == 2 && header[0]&15 == 8 && (int(header[0])*256+int(header[1]))%31 == 0 {
			r, e := zlib.NewReader(buffered)
			if e != nil {
				return HTTPResponse{}, e
			}
			reader = r
			closeDecoder = r
		} else {
			r := flate.NewReader(buffered)
			reader = r
			closeDecoder = r
		}
	}
	if closeDecoder != nil {
		defer closeDecoder.Close()
	}
	body, e := io.ReadAll(io.LimitReader(reader, MaxResponseBytes+1))
	if e != nil {
		return HTTPResponse{}, e
	}
	if len(body) > MaxResponseBytes {
		return HTTPResponse{}, fmt.Errorf("response exceeds 25 MiB")
	}
	ent.jar.SetCookies(u, resp.Cookies())
	return HTTPResponse{URL: u.String(), Status: resp.StatusCode, Body: body, Headers: resp.Header, Cookies: append(ent.jar.Cookies(u), resp.Cookies()...)}, nil
}
func (t *Transport) dial(ctx context.Context, dialer *net.Dialer, proxy *url.URL, target string) (net.Conn, error) {
	if proxy == nil {
		return dialer.DialContext(ctx, "tcp", target)
	}
	if proxy.Scheme != "http" && proxy.Scheme != "https" {
		return nil, fmt.Errorf("unsupported proxy scheme")
	}
	port := proxy.Port()
	if port == "" {
		if proxy.Scheme == "https" {
			port = "443"
		} else {
			port = "80"
		}
	}
	conn, e := dialer.DialContext(ctx, "tcp", net.JoinHostPort(proxy.Hostname(), port))
	if e != nil {
		return nil, e
	}
	if deadline, ok := ctx.Deadline(); ok {
		conn.SetDeadline(deadline)
	}
	if proxy.Scheme == "https" {
		tc := tls.Client(conn, &tls.Config{ServerName: proxy.Hostname(), MinVersion: tls.VersionTLS12})
		if e = tc.HandshakeContext(ctx); e != nil {
			conn.Close()
			return nil, e
		}
		conn = tc
	}
	connect := &http.Request{Method: "CONNECT", URL: &url.URL{Opaque: target}, Host: target, Header: make(http.Header)}
	if e = connect.Write(conn); e != nil {
		conn.Close()
		return nil, e
	}
	resp, e := http.ReadResponse(bufio.NewReader(conn), connect)
	if e != nil {
		conn.Close()
		return nil, e
	}
	if resp.StatusCode != 200 {
		conn.Close()
		return nil, fmt.Errorf("proxy CONNECT status %d", resp.StatusCode)
	}
	return conn, nil
}
func userAgent(profile string) string {
	switch profile {
	case "safari":
		return "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Safari/605.1.15"
	case "safari_ios":
		return "Mozilla/5.0 (iPhone; CPU iPhone OS 14_0 like Mac OS X) AppleWebKit/605.1.15 Version/14.0 Mobile/15E148 Safari/604.1"
	case "firefox":
		return "Mozilla/5.0 (X11; Linux x86_64; rv:120.0) Gecko/20100101 Firefox/120.0"
	case "chrome_android":
		return "okhttp/4.9.1"
	case "edge":
		return "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/85.0.4183.83 Safari/537.36 Edg/85.0.564.41"
	}
	return "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36"
}

// Warmup is once per host/profile and always uses the same guarded request path.
func (t *Transport) Warmup(ctx context.Context, raw, profile string, timeout time.Duration) (bool, error) {
	u, e := url.Parse(raw)
	if e != nil {
		return false, e
	}
	root := u.Scheme + "://" + u.Host + "/"
	if root == raw {
		return false, nil
	}
	ent := t.getSession(u.Hostname(), profile)
	t.mu.Lock()
	if ent.warmed {
		t.mu.Unlock()
		return false, nil
	}
	ent.warmed = true
	t.mu.Unlock()
	_, e = t.Do(ctx, HTTPRequest{URL: root, Profile: profile, Timeout: min(timeout, 15*time.Second)})
	return true, e
}
