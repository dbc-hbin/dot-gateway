package bridge

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
)

var mediaPNG = []byte{137, 80, 78, 71, 13, 10, 26, 10}

func materializeFixture(t *testing.T) (*Store, *RESTClient, *Claim, *discordgo.Message, *http.Client, *atomic.Int32) {
	t.Helper()
	s, settings, _ := ingressFixture(t)
	message := mediaMessage(t, attachmentJSON(mediaURL))
	event := testEnvelope()
	event.Text = message.Content
	event.Media = ProjectMedia(message)
	event.ContentHash = MessageContentFingerprint(message)
	if got, err := s.Ingest(event); err != nil || got != "accepted" {
		t.Fatal(got, err)
	}
	claim, err := s.ClaimNext(60, 0)
	if err != nil || claim == nil {
		t.Fatal(err)
	}
	rest := localREST(t, func(w http.ResponseWriter, q *http.Request) {
		if q.Method != "GET" {
			t.Error("unexpected write", q.Method)
		}
		if preflightHandler(w, q) {
			return
		}
		if q.URL.Path != "/channels/2/messages/3" {
			t.Error("escaped exact source", q.URL.Path)
			w.WriteHeader(404)
			return
		}
		json.NewEncoder(w).Encode(message)
	})
	rest.settings.DBPath = filepath.Join(t.TempDir(), "private.db")
	rest.settings.Policy = settings.Policy
	calls := &atomic.Int32{}
	client, err := newMediaHTTPClient(nil)
	if err != nil {
		t.Fatal(err)
	}
	client.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
		calls.Add(1)
		if req.Header.Get("Authorization") != "" || req.Header.Get("Cookie") != "" || req.Header.Get("Proxy-Authorization") != "" || req.Header.Get("Referer") != "" {
			t.Error("credentials leaked")
		}
		if req.URL.Host != "cdn.discordapp.com" || req.URL.Path != "/attachments/2/7/picture.png" || req.Method != "GET" {
			t.Error("escaped CDN destination", req.URL)
		}
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"image/png"}}, Body: io.NopCloser(bytes.NewReader(mediaPNG)), ContentLength: int64(len(mediaPNG)), Request: req}, nil
	})
	return s, rest, claim, message, client, calls
}
func TestMediaMaterializationRefreshesAndWritesPrivateVerifiedPath(t *testing.T) {
	s, rest, claim, message, client, calls := materializeFixture(t)
	message.Attachments[0].URL = strings.Replace(mediaURL, "ex=ffffff", "ex=eeeeee", 1)
	original := client.Transport
	client.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.Query().Get("ex") != "eeeeee" {
			t.Error("stale signed URL used")
		}
		return original.RoundTrip(req)
	})
	got, err := rest.materializeAttachment(context.Background(), s, claim.InboundID, claim.Claim, "7", client)
	if err != nil || got == nil || calls.Load() != 1 {
		t.Fatal(got, err, calls.Load())
	}
	raw, err := os.ReadFile(got.Path)
	if err != nil || !bytes.Equal(raw, mediaPNG) {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	if got.SHA256 != hex.EncodeToString(sum[:]) || got.Size != 8 || got.ContentType != "image/png" || got.Interpretation != "not_interpreted" {
		t.Fatal(got)
	}
	info, _ := os.Stat(got.Path)
	directory, _ := os.Stat(filepath.Dir(got.Path))
	if info.Mode().Perm() != 0600 || directory.Mode().Perm() != 0700 || filepath.Ext(got.Path) != ".bin" || !strings.HasPrefix(got.Path, rest.settings.DBPath+".media/") {
		t.Fatal("unsafe file", got.Path, info.Mode(), directory.Mode())
	}
	second, err := rest.materializeAttachment(context.Background(), s, claim.InboundID, claim.Claim, "7", client)
	if err != nil || second.Path != got.Path {
		t.Fatal("not bounded/idempotent path", second, err)
	}
	entries, _ := os.ReadDir(filepath.Dir(got.Path))
	if len(entries) != 1 {
		t.Fatal("unbounded duplicate files", entries)
	}
}
func TestMediaMaterializationRejectsStaleOrWrongSourceBeforeCDN(t *testing.T) {
	for _, name := range []string{"claim", "ignored", "author", "bot", "webhook", "message", "channel", "guild", "type", "caption", "metadata", "missing", "url", "hash"} {
		t.Run(name, func(t *testing.T) {
			s, rest, claim, message, client, calls := materializeFixture(t)
			claimToken := claim.Claim
			switch name {
			case "claim":
				claimToken = "wrong"
			case "ignored":
				s.Ignore(claim.InboundID, claimToken)
			case "author":
				message.Author.ID = "9"
			case "bot":
				message.Author.Bot = true
			case "webhook":
				message.WebhookID = "8"
			case "message":
				message.ID = "8"
			case "channel":
				message.ChannelID = "8"
			case "guild":
				message.GuildID = "8"
			case "type":
				message.Type = discordgo.MessageTypeThreadStarterMessage
			case "caption":
				message.Content = "changed"
			case "metadata":
				message.Attachments[0].Size = 9
			case "missing":
				message.Attachments = nil
			case "url":
				message.Attachments[0].URL = "https://127.0.0.1/a"
			case "hash":
				message.MessageReference = &discordgo.MessageReference{MessageID: "8"}
			}
			got, err := rest.materializeAttachment(context.Background(), s, claim.InboundID, claimToken, "7", client)
			if err == nil || got != nil || calls.Load() != 0 {
				t.Fatal("downloaded invalid source", got, err, calls.Load())
			}
		})
	}
}
func TestMediaMaterializationRechecksClaimAfterDownload(t *testing.T) {
	s, rest, claim, _, client, calls := materializeFixture(t)
	original := client.Transport
	client.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if err := s.Ignore(claim.InboundID, claim.Claim); err != nil {
			t.Fatal(err)
		}
		return original.RoundTrip(req)
	})
	got, err := rest.materializeAttachment(context.Background(), s, claim.InboundID, claim.Claim, "7", client)
	if err != ErrClaim || got != nil || calls.Load() != 1 {
		t.Fatal(got, err)
	}
	entries, _ := os.ReadDir(rest.settings.DBPath + ".media")
	if len(entries) != 0 {
		t.Fatal("stale claim saved bytes")
	}
}
func TestMediaMaterializationFailsClosedOnResponseProblems(t *testing.T) {
	for _, name := range []string{"redirect", "type", "sniff", "size", "oversize", "encoding", "deadline"} {
		t.Run(name, func(t *testing.T) {
			s, rest, claim, _, client, calls := materializeFixture(t)
			client.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
				calls.Add(1)
				resp := &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"image/png"}}, Body: io.NopCloser(bytes.NewReader(mediaPNG)), ContentLength: 8, Request: req}
				switch name {
				case "redirect":
					resp.StatusCode = 302
					resp.Header.Set("Location", "https://127.0.0.1/secret")
				case "type":
					resp.Header.Set("Content-Type", "text/html")
				case "sniff":
					resp.Body = io.NopCloser(strings.NewReader("<html>xx"))
				case "size":
					resp.ContentLength = 9
				case "oversize":
					resp.ContentLength = -1
					resp.Body = io.NopCloser(io.MultiReader(bytes.NewReader(mediaPNG), strings.NewReader(strings.Repeat("x", MaxAttachmentBytes))))
				case "encoding":
					resp.Header.Set("Content-Encoding", "gzip")
				case "deadline":
					<-req.Context().Done()
					return nil, req.Context().Err()
				}
				return resp, nil
			})
			ctx := context.Background()
			if name == "deadline" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, 40*time.Millisecond)
				defer cancel()
			}
			got, err := rest.materializeAttachment(ctx, s, claim.InboundID, claim.Claim, "7", client)
			if err == nil || got != nil || calls.Load() != 1 || strings.Contains(err.Error(), "hm=") {
				t.Fatal(got, err, calls.Load())
			}
			entries, _ := os.ReadDir(rest.settings.DBPath + ".media")
			if len(entries) != 0 {
				t.Fatal("invalid bytes persisted")
			}
		})
	}
}
func TestMediaURLAndPublicIPGuards(t *testing.T) {
	for _, raw := range []string{mediaURL, "https://media.discordapp.net/attachments/2/7/picture.png"} {
		if err := validAttachmentURL(raw, "2", "7"); err != nil {
			t.Fatal(raw, err)
		}
	}
	for _, raw := range []string{
		"http://cdn.discordapp.com/attachments/2/7/picture.png", "https://user:pass@cdn.discordapp.com/attachments/2/7/picture.png", "https://cdn.discordapp.com:443/attachments/2/7/picture.png", "https://cdn.discordapp.com.evil.org/attachments/2/7/picture.png", "https://127.0.0.1/attachments/2/7/picture.png", "https://cdn.discordapp.com/attachments/9/7/picture.png", "https://cdn.discordapp.com/attachments/2/8/picture.png", "https://cdn.discordapp.com/attachments/2/7/../x", "https://cdn.discordapp.com/attachments/2/7/%2Fetc", "https://cdn.discordapp.com/attachments/2/7/%5Cx", "https://cdn.discordapp.com/attachments/2/7/%00x", "https://cdn.discordapp.com/attachments/2/7/x#fragment", "https://cdn.discordapp.com/attachments/2/7/x?url=http://localhost", "https://cdn.discordapp.com/attachments/2/7/x?ex=ff&ex=ff",
	} {
		if err := validAttachmentURL(raw, "2", "7"); err == nil {
			t.Fatal("unsafe URL", raw)
		}
	}
	for _, ip := range []string{"127.0.0.1", "10.1.1.1", "169.254.169.254", "192.168.0.1", "172.16.0.1", "100.64.1.1", "::1", "fc00::1", "fe80::1", "::ffff:127.0.0.1", "2001:db8::1", "64:ff9b::a00:1", "198.18.0.1", "240.1.1.1"} {
		if publicMediaIP(netip.MustParseAddr(ip)) {
			t.Fatal("private/reserved accepted", ip)
		}
	}
	for _, ip := range []string{"104.16.1.1", "2606:4700::1"} {
		if !publicMediaIP(netip.MustParseAddr(ip)) {
			t.Fatal("public rejected", ip)
		}
	}
}
func TestMediaClientPinsConfiguredProxyAndNeverCredentials(t *testing.T) {
	var calls atomic.Int32
	relay := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, q *http.Request) {
		calls.Add(1)
		if q.Method != "CONNECT" || q.Host != "cdn.discordapp.com:443" || q.Header.Get("Proxy-Authorization") != "" || q.Header.Get("Authorization") != "" || q.Header.Get("Cookie") != "" {
			t.Error("unsafe CONNECT", q.Method, q.Host, q.Header)
		}
		w.WriteHeader(http.StatusForbidden) // no TLS connection or external request
	}))
	defer relay.Close()
	config, err := NewProxyConfig(relay.URL, "")
	if err != nil {
		t.Fatal(err)
	}
	client, err := newMediaHTTPClient(config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	if _, err = client.Get(mediaURL); err == nil || calls.Load() != 1 {
		t.Fatal("proxy not pinned", err, calls.Load())
	}
	for _, raw := range []string{"https://127.0.0.1/attachments/2/7/x", "http://cdn.discordapp.com/attachments/2/7/x", "https://cdn.discordapp.com/other"} {
		if _, err = client.Get(raw); err == nil {
			t.Fatal("unexpected target accepted", raw)
		}
	}
	req, _ := http.NewRequest("GET", mediaURL, nil)
	req.Header.Set("Authorization", "Bot OFFLINE_CANARY")
	if _, err = client.Do(req); err == nil {
		t.Fatal("credential header sent")
	}
	if calls.Load() != 1 {
		t.Fatal("blocked request reached relay", calls.Load())
	}
	bad := &ProxyConfig{URL: "http://user:pass@127.0.0.1:8080"}
	if _, err = newMediaHTTPClient(bad); err == nil {
		t.Fatal("credential proxy accepted")
	}
	mismatch := &ProxyConfig{URL: relay.URL, NoProxy: "cdn.discordapp.com"}
	if _, err = newMediaHTTPClient(mismatch); err == nil {
		t.Fatal("route mismatch accepted")
	}
	direct, err := newMediaHTTPClient(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer direct.CloseIdleConnections()
	transport := direct.Transport.(*http.Transport)
	u, _ := url.Parse(mediaURL)
	if proxy, err := transport.Proxy(&http.Request{URL: u, Method: "GET", Header: http.Header{}}); err != nil || proxy != nil {
		t.Fatal("ambient proxy used")
	}
	if direct.Jar != nil || direct.Timeout != 20*time.Second || transport.TLSClientConfig != nil {
		t.Fatal("unsafe defaults")
	}
}
func TestMediaPrivateDestinationAndQuota(t *testing.T) {
	for _, name := range []string{"symlink", "permissions", "quota", "invalid_entry"} {
		t.Run(name, func(t *testing.T) {
			s, rest, claim, _, client, calls := materializeFixture(t)
			destination := rest.settings.DBPath + ".media"
			switch name {
			case "symlink":
				os.Symlink(t.TempDir(), destination)
			case "permissions":
				os.Mkdir(destination, 0755)
			case "quota":
				os.Mkdir(destination, 0700)
				f, err := os.OpenFile(filepath.Join(destination, "full"), os.O_CREATE|os.O_WRONLY, 0600)
				if err != nil {
					t.Fatal(err)
				}
				f.Truncate(100 * 1024 * 1024)
				f.Close()
			case "invalid_entry":
				os.Mkdir(destination, 0700)
				os.Symlink("/tmp", filepath.Join(destination, "link"))
			}
			got, err := rest.materializeAttachment(context.Background(), s, claim.InboundID, claim.Claim, "7", client)
			if err == nil || got != nil || calls.Load() != 0 {
				t.Fatal("unsafe path downloaded", got, err, calls.Load())
			}
		})
	}
}
func TestMediaSniffTypes(t *testing.T) {
	for _, tc := range []struct {
		want, body string
		accepted   bool
	}{
		{"text/plain", "hello world", true}, {"text/csv", "a,b\n1,2", true}, {"application/json", `{"a":1}`, true}, {"application/json", "not-json", false}, {"image/png", "<html><script>x</script>", false}, {"text/plain", "<html><script>x</script>", false}, {"application/pdf", "%PDF-1.7\n", true}, {"audio/ogg", "OggS\x00\x02abcdefgh", true}, {"application/octet-stream", "hello", false},
	} {
		body := []byte(tc.body)
		got := sniffedMediaMatches(tc.want, mediaContentType(http.DetectContentType(body)), body)
		if got != tc.accepted {
			t.Fatal(tc.want, tc.body, got)
		}
	}
}

func TestMediaIncidentalPreviewDoesNotBlockMaterialization(t *testing.T) {
	s, rest, claim, message, client, _ := materializeFixture(t)
	message.Embeds = []*discordgo.MessageEmbed{{Title: "automatically populated", Description: strings.Repeat("x", 50000)}}
	result, err := rest.materializeAttachment(context.Background(), s, claim.InboundID, claim.Claim, "7", client)
	if err != nil || result == nil {
		t.Fatal(result, err)
	}
}
