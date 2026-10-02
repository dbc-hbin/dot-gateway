package bridge

import (
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"unicode/utf8"
)

var idPattern = regexp.MustCompile(`^[1-9][0-9]{0,19}$`)

func Snowflake(s string) bool {
	if !idPattern.MatchString(s) {
		return false
	}
	_, e := strconv.ParseUint(s, 10, 64)
	return e == nil
}

type Policy struct {
	OwnerID                string
	AllowedDMIDs           []string
	Platform               string
	GuildID                string
	GuildChannelID         string
	GuildMode              string
	MessageContentApproved bool
}

func (p Policy) Validate() error {
	if !Snowflake(p.OwnerID) || len(p.AllowedDMIDs) != 1 || p.AllowedDMIDs[0] != p.OwnerID {
		return errors.New("invalid_owner_policy")
	}
	if (p.GuildID == "") != (p.GuildChannelID == "") || p.GuildID != "" && (!Snowflake(p.GuildID) || !Snowflake(p.GuildChannelID)) {
		return errors.New("invalid_guild_scope")
	}
	if p.GuildMode != "mention" && p.GuildMode != "all" {
		return errors.New("invalid_guild_mode")
	}
	if p.GuildMode == "all" && (p.GuildID == "" || !p.MessageContentApproved) || p.MessageContentApproved && (p.GuildID == "" || p.GuildMode != "all") {
		return errors.New("invalid_message_content_intent")
	}
	return nil
}
func (p Policy) Allows(e Envelope) bool {
	platform := p.Platform
	if platform == "" {
		platform = "discord"
	}
	plain := e.ParentChannelID == "" && e.ThreadType == 0 && e.ThreadName == ""
	guild := p.GuildID != "" && e.GuildID == p.GuildID && (p.GuildMode == "all" || e.BotMentioned)
	route := plain && (e.RouteKind == "dm" && e.GuildID == "" || e.RouteKind == "guild_text" && guild && e.ConversationID == p.GuildChannelID) ||
		e.RouteKind == "guild_thread" && guild && e.ConversationID != p.GuildChannelID && e.ParentChannelID == p.GuildChannelID && (e.ThreadType == 11 || e.ThreadType == 12)
	return (e.Control == "" || validControl(e)) && p.Validate() == nil && e.Platform == platform && route && !e.SenderIsBot && e.SenderID == p.OwnerID && Snowflake(e.ConversationID) && Snowflake(e.EventID)
}
func (p Policy) Accepts(e Envelope) bool {
	if e.Control != "" {
		return p.Allows(e) && validControl(e) && utf8.ValidString(e.Text) && TextUnits(e.Text) <= 8000 && (trimText(e.Text) != "" || e.ReplyKind == "message")
	}
	if e.ReplyKind != "" && e.ReplyKind != "message" {
		return false
	}
	return p.Allows(e) && utf8.ValidString(e.Text) && (trimText(e.Text) != "" || e.Media.HasContent()) && TextUnits(e.Text) <= 8000 && validEnvelopeMedia(e)
}

// Stages is deliberately broader than Accepts only for hidden quarantine.
// A candidate channel is not a trusted route and cannot be claimed or sent to.
func (p Policy) Stages(e Envelope) bool {
	if e.RouteKind != "guild_thread_candidate" {
		return p.Accepts(e)
	}
	if e.ParentChannelID != "" || e.ThreadType != 0 || e.ThreadName != "" || p.GuildID == "" || e.GuildID != p.GuildID || e.ConversationID == p.GuildChannelID || !Snowflake(e.ConversationID) {
		return false
	}
	// Reuse every ordinary owner, guild, mention, ID and text requirement.
	e.RouteKind, e.ConversationID = "guild_text", p.GuildChannelID
	return p.Accepts(e)
}

type Settings struct {
	Policy               Policy
	DBPath               string
	ExpectedBotID        string
	Token                string       `json:"-"`
	ProxyConfig          *ProxyConfig `json:"-"`
	HTTPKeepaliveSeconds float64
	ReceiveOnly          bool
	KeepCatchupDisarmed  bool
}

func LoadSettings(live, validateProxy bool) (Settings, error) {
	env := map[string]string{}
	for _, v := range os.Environ() {
		k, v, _ := strings.Cut(v, "=")
		env[k] = v
	}
	return SettingsFromEnv(env, live, validateProxy)
}
func SettingsFromEnv(env map[string]string, live, validateProxy bool) (Settings, error) {
	s := Settings{DBPath: ".state/bridge.sqlite3", HTTPKeepaliveSeconds: 120}
	get := func(k, d string) string {
		if v, ok := env[k]; ok {
			return strings.TrimSpace(v)
		}
		return d
	}
	for key, target := range map[string]*bool{"BRIDGE_RECEIVE_ONLY": &s.ReceiveOnly, "BRIDGE_KEEP_CATCHUP_DISARMED": &s.KeepCatchupDisarmed} {
		if value, present := env[key]; present {
			switch value {
			case "true":
				*target = true
			case "false":
				*target = false
			default:
				return s, fmt.Errorf("invalid_boolean_%s", strings.ToLower(key))
			}
		}
	}
	s.Policy = Policy{OwnerID: get("DISCORD_OWNER_ID", ""), Platform: "discord", GuildID: get("DISCORD_GUILD_ID", ""), GuildChannelID: get("DISCORD_GUILD_CHANNEL_ID", ""), GuildMode: get("DISCORD_GUILD_MODE", "mention")}
	seen := map[string]bool{}
	for _, v := range strings.Split(get("DISCORD_ALLOWED_DM_IDS", ""), ",") {
		v = strings.TrimSpace(v)
		if v != "" && !seen[v] {
			s.Policy.AllowedDMIDs = append(s.Policy.AllowedDMIDs, v)
			seen[v] = true
		}
	}
	gate := strings.ToLower(get("DISCORD_ENABLE_MESSAGE_CONTENT", "false"))
	if gate != "true" && gate != "false" {
		return s, errors.New("invalid_message_content_gate")
	}
	s.Policy.MessageContentApproved = gate == "true"
	if err := s.Policy.Validate(); err != nil {
		return s, err
	}
	s.ExpectedBotID = get("DISCORD_EXPECTED_BOT_ID", "")
	if s.ExpectedBotID != "" && (!Snowflake(s.ExpectedBotID) || s.ExpectedBotID == s.Policy.OwnerID) {
		return s, errors.New("invalid_expected_bot_id")
	}
	s.DBPath = get("BRIDGE_DB", s.DBPath)
	keep, e := strconv.ParseFloat(get("BRIDGE_HTTP_KEEPALIVE_SECONDS", "120"), 64)
	if e != nil || !(keep >= 15 && keep <= 600) {
		return s, errors.New("invalid_http_keepalive")
	}
	s.HTTPKeepaliveSeconds = keep
	if live || validateProxy {
		p, e := ProxyFromEnv(env)
		if e != nil {
			return s, e
		}
		if _, e = p.DiscordRoute(); e != nil {
			return s, e
		}
		s.ProxyConfig = p
	}
	if live {
		if !Snowflake(s.ExpectedBotID) {
			return s, errors.New("expected_bot_id_required")
		}
		s.Token = get("DISCORD_BOT_TOKEN", "")
		path := get("DISCORD_BOT_TOKEN_FILE", "")
		if s.Token != "" && path != "" {
			return s, errors.New("multiple_token_sources")
		}
		if path != "" {
			s.Token, e = LoadTokenFile(path)
			if e != nil {
				return s, e
			}
		}
		if !validToken(s.Token) {
			return s, errors.New("secure_bot_token_required")
		}
	}
	return s, nil
}
func validToken(t string) bool {
	if len(t) == 0 || len(t) > 4096 {
		return false
	}
	for _, r := range t {
		if r < 33 || r > 126 {
			return false
		}
	}
	return true
}
func LoadTokenFile(path string) (string, error) {
	fd, e := syscall.Open(path, syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK|syscall.O_CLOEXEC, 0)
	if e != nil {
		return "", errors.New("token_file_open_failed")
	}
	f := os.NewFile(uintptr(fd), "token")
	defer f.Close()
	st, e := f.Stat()
	if e != nil {
		return "", errors.New("token_file_stat_failed")
	}
	sys, ok := st.Sys().(*syscall.Stat_t)
	if !ok || !st.Mode().IsRegular() || st.Mode().Perm()&0077 != 0 || int(sys.Uid) != os.Getuid() {
		return "", errors.New("insecure_token_file")
	}
	b, e := io.ReadAll(io.LimitReader(f, 4097))
	if e != nil || len(b) > 4096 {
		return "", errors.New("invalid_token_file")
	}
	t := strings.TrimSpace(string(b))
	if !validToken(t) {
		return "", errors.New("invalid_token_file")
	}
	return t, nil
}

type ProxyConfig struct {
	URL      string `json:"-"`
	NoProxy  string `json:"-"`
	networks []*net.IPNet
	hosts    []string
}

func ordinaryASCII(s string, spaces bool) bool {
	for _, r := range s {
		if r >= 127 || r < 32 || (!spaces && r == 32) {
			return false
		}
	}
	return true
}

var hostPattern = regexp.MustCompile(`^[A-Za-z0-9](?:[A-Za-z0-9.-]*[A-Za-z0-9])?$`)

func NewProxyConfig(raw, bypass string) (*ProxyConfig, error) {
	p := &ProxyConfig{NoProxy: bypass}
	if raw != "" {
		u, e := url.Parse(raw)
		if e != nil || !ordinaryASCII(raw, false) || u.Scheme != "http" || u.User != nil || u.Host == "" || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" || strings.HasSuffix(u.Host, ":") {
			return nil, errors.New("invalid_proxy_url")
		}
		host := u.Hostname()
		port := u.Port()
		if port == "" {
			port = "80"
		}
		n, e := strconv.Atoi(port)
		if e != nil || n < 1 || n > 65535 || strings.Contains(host, "%") || strings.Contains(host, "..") || !hostPattern.MatchString(host) && net.ParseIP(host) == nil {
			return nil, errors.New("invalid_proxy_host")
		}
		p.URL = "http://" + net.JoinHostPort(host, port)
	}
	if !ordinaryASCII(bypass, true) {
		return nil, errors.New("invalid_no_proxy")
	}
	for _, v := range strings.Split(bypass, ",") {
		v = strings.TrimSpace(v)
		if v == "" {
			continue
		}
		if strings.Contains(v, "/") {
			_, n, e := net.ParseCIDR(v)
			if e != nil {
				return nil, errors.New("invalid_no_proxy_network")
			}
			p.networks = append(p.networks, n)
		} else if strings.Contains(v, "*") && v != "*" {
			return nil, errors.New("invalid_no_proxy_wildcard")
		} else {
			p.hosts = append(p.hosts, strings.ToLower(v))
		}
	}
	return p, nil
}
func ProxyFromEnv(env map[string]string) (*ProxyConfig, error) {
	pick := func(a, b string) string {
		if v, ok := env[a]; ok {
			return v
		}
		return env[b]
	}
	raw, ok := env["BRIDGE_HTTPS_PROXY"]
	if ok && raw == "" {
		return nil, errors.New("empty_proxy_override")
	}
	if !ok {
		raw = pick("https_proxy", "HTTPS_PROXY")
		if raw == "" {
			raw = pick("all_proxy", "ALL_PROXY")
		}
	}
	bypass, ok := env["BRIDGE_NO_PROXY"]
	if !ok {
		bypass = pick("no_proxy", "NO_PROXY")
	}
	return NewProxyConfig(raw, bypass)
}
func (p *ProxyConfig) ForHost(host string, port int) string {
	if p == nil || p.URL == "" {
		return ""
	}
	host = strings.ToLower(host)
	authority := fmt.Sprintf("%s:%d", host, port)
	for _, pat := range p.hosts {
		if pat == "*" {
			return ""
		}
		pat = strings.TrimPrefix(pat, ".")
		if host == pat || authority == pat || strings.HasSuffix(host, "."+pat) || strings.HasSuffix(authority, "."+pat) {
			return ""
		}
	}
	ip := net.ParseIP(strings.Trim(host, "[]"))
	if ip != nil {
		for _, n := range p.networks {
			if n.Contains(ip) {
				return ""
			}
		}
	}
	return p.URL
}
func (p *ProxyConfig) DiscordRoute() (string, error) {
	r := p.ForHost("discord.com", 443)
	if r != p.ForHost("gateway.discord.gg", 443) {
		return "", errors.New("discord_proxy_route_mismatch")
	}
	return r, nil
}
func (p *ProxyConfig) ValidateGateway(raw, route string) error {
	u, e := url.Parse(raw)
	if e != nil || !ordinaryASCII(raw, false) || u.Scheme != "wss" || u.User != nil || u.Fragment != "" || strings.HasSuffix(u.Host, ":") || (u.Port() != "" && u.Port() != "443") || !hostPattern.MatchString(u.Hostname()) || strings.Contains(u.Hostname(), "..") || !strings.HasSuffix(u.Hostname(), ".discord.gg") {
		return errors.New("unexpected_gateway_endpoint")
	}
	if p.ForHost(u.Hostname(), 443) != route {
		return errors.New("gateway_proxy_route_mismatch")
	}
	return nil
}

// String prevents accidental credential disclosure through ordinary formatting.
func (s Settings) String() string {
	return fmt.Sprintf("Settings{DBPath:%q ExpectedBotID:%q Token:[REDACTED]}", s.DBPath, s.ExpectedBotID)
}
func (s Settings) GoString() string { return s.String() }
