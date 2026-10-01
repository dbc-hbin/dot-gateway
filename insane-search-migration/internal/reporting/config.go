// Package reporting publishes bounded, explicitly prepared Discord reports.
// It has no gateway, scheduler, model client, or inbound admission capability.
package reporting

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"syscall"
)

const TokenPath = "/opt/assistant-shared/.discord-private/bot-token"
const DefaultProxyPath = "/opt/assistant-project/gateway_native_proxy_config.json"
const MaxReportBytes = 64 * 1024
const MaxChunkUnits = 1900
const MaxChunks = 40

var idPattern = regexp.MustCompile(`^[1-9][0-9]{0,19}$`)
var runPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)

func snowflake(s string) bool {
	if !idPattern.MatchString(s) {
		return false
	}
	_, err := strconv.ParseUint(s, 10, 64)
	return err == nil
}

type Target struct {
	BotID     string `json:"bot_id"`
	GuildID   string `json:"guild_id"`
	ChannelID string `json:"channel_id"`
}

func (t Target) Validate() error {
	if !snowflake(t.BotID) || !snowflake(t.GuildID) || !snowflake(t.ChannelID) || t.BotID == t.GuildID || t.BotID == t.ChannelID || t.GuildID == t.ChannelID {
		return errors.New("invalid_exact_target")
	}
	return nil
}
func strictJSON(b []byte, out any) error {
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if d.Decode(out) != nil {
		return errors.New("invalid_json")
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return errors.New("invalid_json")
	}
	return nil
}
func readSmallFile(path string, max int) ([]byte, error) {
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK|syscall.O_CLOEXEC, 0)
	if err != nil {
		return nil, errors.New("configuration_open_failed")
	}
	f := os.NewFile(uintptr(fd), "configuration")
	defer f.Close()
	st, err := f.Stat()
	if err != nil || !st.Mode().IsRegular() {
		return nil, errors.New("configuration_not_regular")
	}
	b, err := io.ReadAll(io.LimitReader(f, int64(max+1)))
	if err != nil || len(b) > max {
		return nil, errors.New("configuration_read_failed")
	}
	return b, nil
}
func LoadTarget(path string) (Target, error) {
	var t Target
	b, err := readSmallFile(path, 4096)
	if err != nil {
		return t, err
	}
	if strictJSON(b, &t) != nil {
		return t, errors.New("invalid_target_configuration")
	}
	return t, t.Validate()
}

// loadTokenFile preserves the gateway's no-follow, owner and mode safeguards.
// It never returns token contents in errors and is only called by NewLiveClient.
func loadTokenFile(path string) (string, error) {
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK|syscall.O_CLOEXEC, 0)
	if err != nil {
		return "", errors.New("token_file_open_failed")
	}
	f := os.NewFile(uintptr(fd), "token")
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return "", errors.New("token_file_stat_failed")
	}
	owner, ok := st.Sys().(*syscall.Stat_t)
	if !ok || !st.Mode().IsRegular() || st.Mode().Perm()&0077 != 0 || int(owner.Uid) != os.Getuid() || owner.Nlink != 1 {
		return "", errors.New("insecure_token_file")
	}
	b, err := io.ReadAll(io.LimitReader(f, 4097))
	if err != nil || len(b) > 4096 {
		return "", errors.New("invalid_token_file")
	}
	token := strings.TrimSpace(string(b))
	if token == "" {
		return "", errors.New("invalid_token_file")
	}
	for _, r := range token {
		if r < 33 || r > 126 {
			return "", errors.New("invalid_token_file")
		}
	}
	return token, nil
}
func loadProxy(path string) (*url.URL, error) {
	var p struct {
		URL     string `json:"BRIDGE_HTTPS_PROXY"`
		NoProxy string `json:"BRIDGE_NO_PROXY"`
	}
	if path == "" {
		p.URL = os.Getenv("HTTPS_PROXY")
		if p.URL == "" {
			p.URL = os.Getenv("https_proxy")
		}
		p.NoProxy = os.Getenv("NO_PROXY")
		if p.NoProxy == "" {
			p.NoProxy = os.Getenv("no_proxy")
		}
	} else {
		b, err := readSmallFile(path, 16384)
		if err != nil {
			return nil, err
		}
		if strictJSON(b, &p) != nil {
			return nil, errors.New("invalid_proxy_configuration")
		}
	}
	u, err := url.Parse(p.URL)
	if err != nil || u.Scheme != "http" || u.User != nil || u.Host == "" || u.Opaque != "" || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" || strings.HasSuffix(u.Host, ":") {
		return nil, errors.New("invalid_proxy_url")
	}
	for _, r := range p.URL {
		if r <= 32 || r >= 127 {
			return nil, errors.New("invalid_proxy_url")
		}
	}
	host := u.Hostname()
	if strings.Contains(host, "..") || strings.Contains(host, "%") || (net.ParseIP(host) == nil && !regexp.MustCompile(`^[A-Za-z0-9](?:[A-Za-z0-9.-]*[A-Za-z0-9])?$`).MatchString(host)) {
		return nil, errors.New("invalid_proxy_host")
	}
	port := u.Port()
	if port == "" {
		port = "80"
	}
	n, err := strconv.Atoi(port)
	if err != nil || n < 1 || n > 65535 {
		return nil, errors.New("invalid_proxy_port")
	}
	// This REST-only publisher always uses the explicit proxy. A bypass applying
	// to Discord is rejected rather than silently changing the authorized route.
	for _, v := range strings.Split(p.NoProxy, ",") {
		v = strings.ToLower(strings.TrimSpace(v))
		for _, r := range v {
			if r < 32 || r >= 127 {
				return nil, errors.New("invalid_no_proxy")
			}
		}
		v = strings.TrimPrefix(v, ".")
		if v == "*" || v == "discord.com" || v == "discord.com:443" || v == "com" {
			return nil, errors.New("discord_proxy_bypass_forbidden")
		}
	}
	return u, nil
}
