package bridge

import (
	"os"
	"path/filepath"
	"testing"
)

func testPolicy() Policy {
	return Policy{OwnerID: "1", AllowedDMIDs: []string{"1"}, Platform: "discord", GuildMode: "mention"}
}
func testEnvelope() Envelope {
	return Envelope{Platform: "discord", EventID: "3", ConversationID: "2", SenderID: "1", Text: "hi", RouteKind: "dm"}
}
func TestPolicy(t *testing.T) {
	p := testPolicy()
	e := testEnvelope()
	if !p.Accepts(e) {
		t.Fatal("valid denied")
	}
	for _, mut := range []func(*Envelope){func(e *Envelope) { e.SenderID = "4" }, func(e *Envelope) { e.SenderIsBot = true }, func(e *Envelope) { e.GuildID = "5" }, func(e *Envelope) { e.ConversationID = "../../" }, func(e *Envelope) { e.Platform = "other" }} {
		v := e
		mut(&v)
		if p.Allows(v) {
			t.Fatal("allowed unsafe")
		}
	}
	p.GuildID = "5"
	p.GuildChannelID = "2"
	e.GuildID = "5"
	e.RouteKind = "guild_text"
	if p.Allows(e) {
		t.Fatal("mention missing")
	}
	e.BotMentioned = true
	if !p.Allows(e) {
		t.Fatal("mention denied")
	}
}
func TestSnowflake(t *testing.T) {
	for _, s := range []string{"0", "01", "18446744073709551616", "-1", "1\n"} {
		if Snowflake(s) {
			t.Fatal(s)
		}
	}
	if !Snowflake("18446744073709551615") {
		t.Fatal("max denied")
	}
}
func TestTokenFile(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "token")
	os.WriteFile(p, []byte("fake.offline.token\n"), 0600)
	if s, e := LoadTokenFile(p); e != nil || s != "fake.offline.token" {
		t.Fatal(e)
	}
	os.Chmod(p, 0644)
	if _, e := LoadTokenFile(p); e == nil {
		t.Fatal("insecure accepted")
	}
	os.Chmod(p, 0600)
	link := filepath.Join(dir, "link")
	os.Symlink(p, link)
	if _, e := LoadTokenFile(link); e == nil {
		t.Fatal("symlink accepted")
	}
	if _, e := LoadTokenFile(dir); e == nil {
		t.Fatal("directory accepted")
	}
}
func TestProxy(t *testing.T) {
	for _, s := range []string{"https://proxy:80", "http://a:b@proxy", "http://proxy/path", "http://proxy:", "http://proxy:65536", "http://proxy\n", "http://proxy?x=1", "http://a..b"} {
		if _, e := NewProxyConfig(s, ""); e == nil {
			t.Fatalf("accepted %q", s)
		}
	}
	p, e := NewProxyConfig("http://proxy:80", ".example.com,10.0.0.0/8")
	if e != nil {
		t.Fatal(e)
	}
	if p.ForHost("sub.example.com", 443) != "" || p.ForHost("10.2.3.4", 443) != "" || p.ForHost("discord.com", 443) == "" {
		t.Fatal("bad bypass")
	}
	p, _ = NewProxyConfig("http://proxy:80", "discord.com")
	if _, e = p.DiscordRoute(); e == nil {
		t.Fatal("mixed route accepted")
	}
	p, _ = NewProxyConfig("http://proxy:80", "")
	route, _ := p.DiscordRoute()
	for _, u := range []string{"wss://gateway.discord.gg", "wss://us-east.discord.gg/?v=10"} {
		if e = p.ValidateGateway(u, route); e != nil {
			t.Fatal(e)
		}
	}
	for _, u := range []string{"wss://discord.gg", "wss://gateway.discord.gg.evil.test", "ws://gateway.discord.gg", "wss://x@gateway.discord.gg", "wss://gateway.discord.gg:444"} {
		if p.ValidateGateway(u, route) == nil {
			t.Fatal("bad gateway")
		}
	}
}
func TestOfflineSettingsIgnoreSecret(t *testing.T) {
	env := map[string]string{"DISCORD_OWNER_ID": "1", "DISCORD_ALLOWED_DM_IDS": "1", "DISCORD_BOT_TOKEN_FILE": "/not/read", "BRIDGE_HTTPS_PROXY": "bad"}
	s, e := SettingsFromEnv(env, false, false)
	if e != nil || s.Token != "" {
		t.Fatal(e)
	}
	if _, e = SettingsFromEnv(env, false, true); e == nil {
		t.Fatal("proxy ignored")
	}
}
