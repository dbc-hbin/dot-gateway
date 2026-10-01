package reporting

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"
)

var testTarget = Target{BotID: "11", GuildID: "22", ChannelID: "33"}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type fakeAPI struct {
	mu             sync.Mutex
	posts, gets    int
	messages       map[string]map[string]any
	postStatus     int
	failPost       bool
	failRead       bool
	wrongAck       bool
	wrongIdentity  bool
	wrongChannel   bool
	omitType       bool
	permission     string
	stateDir       string
	requireAttempt bool
}

func (f *fakeAPI) client() *Client {
	return &Client{target: testTarget, token: "fake-test-token", http: &http.Client{Transport: roundTripFunc(f.roundTrip), CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
}
func (f *fakeAPI) roundTrip(r *http.Request) (*http.Response, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if r.URL.Scheme != "https" || r.URL.Host != "discord.com" {
		return nil, errors.New("unexpected test origin")
	}
	status := 200
	var value any
	path := strings.TrimPrefix(r.URL.Path, "/api/v10")
	if r.Method == "POST" {
		f.posts++
		if path != "/channels/33/messages" || r.GetBody != nil || r.Header.Get("Idempotency-Key") != "" {
			return nil, errors.New("invalid post route or replayable body")
		}
		var body map[string]any
		if json.NewDecoder(r.Body).Decode(&body) != nil {
			return nil, errors.New("bad payload")
		}
		if body["enforce_nonce"] != true || len(body["nonce"].(string)) != 24 || body["message_reference"] != nil || body["flags"] != float64(4) {
			return nil, errors.New("bad post fields")
		}
		mentions := body["allowed_mentions"].(map[string]any)
		if len(mentions["parse"].([]any)) != 0 || len(mentions["users"].([]any)) != 0 || len(mentions["roles"].([]any)) != 0 || mentions["replied_user"] != false {
			return nil, errors.New("unsafe mentions")
		}
		if f.requireAttempt {
			entries, _ := os.ReadDir(f.stateDir)
			found := false
			for _, e := range entries {
				if strings.HasPrefix(e.Name(), "run-") {
					b, _ := os.ReadFile(filepath.Join(f.stateDir, e.Name()))
					var v Receipt
					json.Unmarshal(b, &v)
					for _, c := range v.Chunks {
						if c.Nonce == body["nonce"] && c.State == "attempted" && c.AttemptedAt != "" {
							found = true
						}
					}
				}
			}
			if !found {
				return nil, errors.New("attempt not persisted")
			}
		}
		if f.failPost {
			return nil, errors.New("transport error must not leak fake-test-token")
		}
		if f.postStatus != 0 {
			status = f.postStatus
			value = map[string]any{"message": "private external error text"}
		} else {
			id := strconv.Itoa(100 + f.posts)
			value = map[string]any{"id": id, "channel_id": "33", "author": map[string]any{"id": "11", "bot": true}, "content": body["content"], "nonce": body["nonce"], "type": 0}
			if f.wrongAck {
				value.(map[string]any)["content"] = "changed"
			}
			if f.messages == nil {
				f.messages = map[string]map[string]any{}
			}
			f.messages[id] = value.(map[string]any)
		}
	} else if r.Method == "GET" {
		f.gets++
		switch path {
		case "/users/@me":
			id := "11"
			if f.wrongIdentity {
				id = "99"
			}
			value = map[string]any{"id": id, "bot": true}
		case "/channels/33":
			id := "33"
			if f.wrongChannel {
				id = "99"
			}
			value = map[string]any{"id": id, "guild_id": "22", "type": 0, "name": "test-report", "permission_overwrites": []any{}}
			if f.omitType {
				delete(value.(map[string]any), "type")
			}
		case "/guilds/22":
			value = map[string]any{"id": "22", "owner_id": "44"}
		case "/guilds/22/members/11":
			value = map[string]any{"user": map[string]any{"id": "11", "bot": true}, "roles": []string{}}
		case "/guilds/22/roles":
			p := f.permission
			if p == "" {
				p = strconv.FormatUint(requiredPermissions, 10)
			}
			value = []any{map[string]any{"id": "22", "permissions": p}}
		default:
			if strings.HasPrefix(path, "/channels/33/messages/") {
				if f.failRead {
					return nil, errors.New("readback connection failed")
				}
				value = f.messages[strings.TrimPrefix(path, "/channels/33/messages/")]
				if value == nil {
					status = 404
				}
			} else {
				return nil, errors.New("unexpected GET")
			}
		}
	} else {
		return nil, errors.New("unexpected method")
	}
	b, _ := json.Marshal(value)
	return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(string(b))), Header: make(http.Header), Request: r}, nil
}
func privateDir(t *testing.T) string {
	t.Helper()
	d := filepath.Join(t.TempDir(), "state")
	if err := os.Mkdir(d, 0700); err != nil {
		t.Fatal(err)
	}
	return d
}
func TestSplitRoundtrip(t *testing.T) {
	for _, s := range []string{"hello", strings.Repeat("😀한글 line\n", 900), strings.Repeat("x", 1901), strings.Repeat("한", 2200)} {
		parts, err := Split([]byte(s))
		if err != nil {
			t.Fatal(err)
		}
		if strings.Join(strings.Fields(strings.Join(parts, " ")), " ") != strings.Join(strings.Fields(s), " ") && strings.ReplaceAll(strings.Join(parts, ""), "\n", "") != strings.ReplaceAll(strings.TrimSpace(s), "\n", "") {
			t.Fatal("non-boundary text changed")
		}
		for _, p := range parts {
			if textUnits(p) > 1900 || !utf8.ValidString(p) {
				t.Fatal("invalid chunk")
			}
		}
	}
	for _, b := range [][]byte{nil, []byte("  \n"), {0xff}, []byte("a\x00b"), []byte(strings.Repeat("x", MaxReportBytes+1))} {
		if _, err := Split(b); err == nil {
			t.Fatal("invalid report accepted")
		}
	}
}
func TestPublishIdempotenceAndBinding(t *testing.T) {
	d := privateDir(t)
	api := &fakeAPI{stateDir: d, requireAttempt: true}
	c := api.client()
	report := []byte(strings.Repeat("한글😀 reports\n", 220))
	got, err := c.Publish(context.Background(), d, "test-1", report)
	if err != nil {
		t.Fatal(err)
	}
	if !got.Complete || len(got.Chunks) < 2 || api.posts != len(got.Chunks) {
		t.Fatalf("unexpected result %+v", got)
	}
	posts, gets := api.posts, api.gets
	again, err := c.Publish(context.Background(), d, "test-1", report)
	if err != nil || !again.Complete || api.posts != posts || api.gets != gets {
		t.Fatal("not idempotent")
	}
	if _, err = c.Publish(context.Background(), d, "test-1", []byte("changed report")); err == nil {
		t.Fatal("payload binding changed")
	}
	changed := *c
	changed.target.ChannelID = "55"
	if _, err = changed.Publish(context.Background(), d, "new-run", []byte("another report")); err == nil {
		t.Fatal("target pin changed")
	}
	if api.posts != posts {
		t.Fatal("unexpected POST")
	}
}
func TestFailuresNeverResend(t *testing.T) {
	for _, kind := range []string{"transport", "400", "429", "500", "ack"} {
		t.Run(kind, func(t *testing.T) {
			d := privateDir(t)
			api := &fakeAPI{}
			switch kind {
			case "transport":
				api.failPost = true
			case "ack":
				api.wrongAck = true
			default:
				api.postStatus, _ = strconv.Atoi(kind)
			}
			c := api.client()
			r, err := c.Publish(context.Background(), d, "failed", []byte("report"))
			if err == nil || api.posts != 1 || r.Complete {
				t.Fatal("failure not recorded")
			}
			api.failPost = false
			api.postStatus = 0
			api.wrongAck = false
			if _, err = c.Publish(context.Background(), d, "failed", []byte("report")); err == nil || api.posts != 1 {
				t.Fatal("uncertain POST repeated")
			}
		})
	}
}
func TestReadbackRecoveryDoesNotPostAgain(t *testing.T) {
	d := privateDir(t)
	api := &fakeAPI{failRead: true}
	c := api.client()
	r, err := c.Publish(context.Background(), d, "readback", []byte("report"))
	if err == nil || api.posts != 1 || r.Chunks[0].MessageID == "" || r.Complete {
		t.Fatal("readback failure lost acknowledgement")
	}
	api.failRead = false
	r, err = c.Publish(context.Background(), d, "readback", []byte("report"))
	if err != nil || !r.Complete || api.posts != 1 {
		t.Fatal("recovery did not use GET only")
	}
}
func TestCrashAttemptBlocksSend(t *testing.T) {
	d := privateDir(t)
	api := &fakeAPI{}
	c := api.client()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.Publish(ctx, d, "crash", []byte("report")); err == nil {
		t.Fatal("expected cancel")
	}
	s, err := openStore(d)
	if err != nil {
		t.Fatal(err)
	}
	name := "run-" + hashBytes([]byte("crash")) + ".json"
	b, _ := s.read(name)
	var r Receipt
	json.Unmarshal(b, &r)
	r.Chunks[0].State = "attempted"
	r.Chunks[0].AttemptedAt = stamp()
	if err = s.write(name, r); err != nil {
		t.Fatal(err)
	}
	s.close()
	if _, err = c.Publish(context.Background(), d, "crash", []byte("report")); err == nil || api.posts != 0 {
		t.Fatal("crashed attempt retried")
	}
}
func TestPreflightBlocksWrongIdentityChannelAndPermissions(t *testing.T) {
	for _, kind := range []string{"identity", "channel", "type", "permission"} {
		t.Run(kind, func(t *testing.T) {
			api := &fakeAPI{}
			switch kind {
			case "identity":
				api.wrongIdentity = true
			case "channel":
				api.wrongChannel = true
			case "type":
				api.omitType = true
			case "permission":
				api.permission = "1024"
			}
			c := api.client()
			if _, err := c.Publish(context.Background(), privateDir(t), "blocked", []byte("report")); err == nil || api.posts != 0 {
				t.Fatal("failed preflight sent")
			}
		})
	}
	api := &fakeAPI{}
	r, err := api.client().Inspect(context.Background())
	if err != nil || !r.Verified || api.posts != 0 {
		t.Fatal("inspect failed or posted")
	}
}
func TestCredentialContentRejected(t *testing.T) {
	for _, s := range []string{"sk-" + strings.Repeat("FAKE", 8), "AIza" + strings.Repeat("FAKE", 8), "API key： " + strings.Repeat("FAKE", 8), "token: '" + strings.Repeat("FAKE", 8), "api_key=" + strings.Repeat("FAKE", 8)} {
		api := &fakeAPI{}
		d := filepath.Join(t.TempDir(), "must-not-exist")
		r, err := api.client().Publish(context.Background(), d, "reject", []byte(s))
		if err == nil || api.posts != 0 || api.gets != 0 || len(r.Chunks) != 0 {
			t.Fatal("secret not blocked early")
		}
		if strings.Contains(err.Error(), "FAKE") {
			t.Fatal("secret echoed")
		}
		if _, err = os.Stat(d); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("secret created state")
		}
	}
	for _, s := range []string{"Coupon code: SAVE20", "API key signup is not required", "쿠폰 BENEFITS2026EXAMPLE, public offer"} {
		if rejectCredentialContent([]byte(s)) != nil {
			t.Fatal("public coupon rejected")
		}
	}
}
func TestPrivateStoreAndLock(t *testing.T) {
	d := privateDir(t)
	s, err := openStore(d)
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	if _, err = openStore(d); err == nil {
		t.Fatal("parallel writer accepted")
	}
	bad := privateDir(t)
	os.Chmod(bad, 0755)
	if s, err := openStore(bad); err == nil {
		s.close()
		t.Fatal("public state accepted")
	}
	link := filepath.Join(t.TempDir(), "link")
	os.Symlink(d, link)
	if _, err = openStore(link); err == nil {
		t.Fatal("symlink state accepted")
	}
	if err = s.write("test.json", map[string]string{"a": "b"}); err != nil {
		t.Fatal(err)
	}
	st, _ := os.Stat(filepath.Join(d, "test.json"))
	if st.Mode().Perm() != 0600 {
		t.Fatal("wrong ledger mode")
	}
	os.Symlink("test.json", filepath.Join(d, "evil.json"))
	if _, err = s.read("evil.json"); err == nil {
		t.Fatal("ledger symlink followed")
	}
}
func TestTokenSecurityUsesOnlyFixtures(t *testing.T) {
	d := t.TempDir()
	p := filepath.Join(d, "fixture-token")
	os.WriteFile(p, []byte("FAKE-TOKEN-FOR-TESTS-ONLY\n"), 0600)
	if _, err := loadTokenFile(p); err != nil {
		t.Fatal(err)
	}
	os.Chmod(p, 0644)
	if _, err := loadTokenFile(p); err == nil {
		t.Fatal("public token accepted")
	}
	os.Chmod(p, 0600)
	link := filepath.Join(d, "symlink")
	os.Symlink(p, link)
	if _, err := loadTokenFile(link); err == nil {
		t.Fatal("token symlink accepted")
	}
	hard := filepath.Join(d, "hardlink")
	os.Link(p, hard)
	if _, err := loadTokenFile(p); err == nil {
		t.Fatal("token hardlink accepted")
	}
}
func TestProxyAndTargetValidation(t *testing.T) {
	t.Setenv("HTTPS_PROXY", "http://127.0.0.1:8888")
	t.Setenv("NO_PROXY", "localhost,127.0.0.1")
	if _, err := loadProxy(""); err != nil {
		t.Fatal(err)
	}
	for _, proxy := range []string{"https://proxy:80", "http://user:pass@proxy", "http://proxy/path", "http://proxy?x=1", "http://proxy:65536", "http://a..b", ""} {
		t.Setenv("HTTPS_PROXY", proxy)
		t.Setenv("https_proxy", proxy)
		if _, err := loadProxy(""); err == nil {
			t.Fatalf("invalid proxy accepted: %s", proxy)
		}
	}
	t.Setenv("HTTPS_PROXY", "http://127.0.0.1:8888")
	t.Setenv("NO_PROXY", ".discord.com")
	if _, err := loadProxy(""); err == nil {
		t.Fatal("discord bypass accepted")
	}
	for _, x := range []Target{{}, {BotID: "11", GuildID: "22", ChannelID: "22"}, {BotID: "11", GuildID: "22", ChannelID: "../33"}} {
		if x.Validate() == nil {
			t.Fatal("invalid target accepted")
		}
	}
	p := filepath.Join(t.TempDir(), "target.json")
	os.WriteFile(p, []byte(`{"bot_id":"11","guild_id":"22","channel_id":"33","token":"FAKE"}`), 0600)
	if _, err := LoadTarget(p); err == nil {
		t.Fatal("unknown config field accepted")
	}
}
func TestPermissionOverwriteOrderAndTimeout(t *testing.T) {
	zero, one := 0, 1
	rs := []string{"55", "66"}
	ows := []overwrite{{ID: "22", Type: &zero, Allow: "0", Deny: "2048"}, {ID: "55", Type: &zero, Allow: "2048", Deny: "0"}, {ID: "66", Type: &zero, Allow: "0", Deny: "2048"}}
	g := guild{ID: "22", OwnerID: "44"}
	ch := channel{ID: "33", GuildID: "22", Type: &zero, Overwrites: &ows}
	m := member{User: user{ID: "11", Bot: true}, Roles: &rs}
	roles := []role{{ID: "22", Permissions: fmt.Sprint(requiredPermissions)}, {ID: "55", Permissions: "0"}, {ID: "66", Permissions: "0"}}
	p, err := effectivePermissions(testTarget, g, ch, m, roles, time.Now())
	if err != nil || p&requiredPermissions != requiredPermissions {
		t.Fatal("aggregated allow must win role deny")
	}
	ows = append(ows, overwrite{ID: "11", Type: &one, Allow: "0", Deny: "2048"})
	p, err = effectivePermissions(testTarget, g, ch, m, roles, time.Now())
	if err != nil || p&(1<<11) != 0 {
		t.Fatal("member deny must win")
	}
	roles[1].Permissions = "8"
	p, err = effectivePermissions(testTarget, g, ch, m, roles, time.Now())
	if err != nil || p&requiredPermissions != requiredPermissions {
		t.Fatal("admin override failed")
	}
	future := time.Now().Add(time.Hour).Format(time.RFC3339)
	m.Timeout = &future
	if _, err = effectivePermissions(testTarget, g, ch, m, roles, time.Now()); err == nil {
		t.Fatal("timeout accepted")
	}
}

func TestAdditionalCredentialFormsAndLivePin(t *testing.T) {
	samples := []string{`{"api_key":"FAKEFAKEFAKEFAKEFAKE"}`, "ghp_" + strings.Repeat("FAKE", 8), strings.Repeat("A", 25) + "." + strings.Repeat("B", 6) + "." + strings.Repeat("C", 32), "-----BEGIN PRIVATE KEY-----", "literal fake-test-token credential"}
	for _, sample := range samples {
		api := &fakeAPI{}
		if _, err := api.client().Publish(context.Background(), filepath.Join(t.TempDir(), "unused"), "secret", []byte(sample)); err == nil || api.posts != 0 || api.gets != 0 {
			t.Fatal("credential form accepted")
		}
	}
	if _, err := NewLiveClient(testTarget, "/nonexistent"); err == nil || err.Error() != "target_not_approved_for_live_delivery" {
		t.Fatal("live target not pinned before config and token I/O")
	}
}
func TestBoundaryNormalizationAndOptionalNonce(t *testing.T) {
	parts, err := Split([]byte("  First line\nSecond line\n"))
	if err != nil || len(parts) != 1 || parts[0] != "First line\nSecond line" {
		t.Fatal("boundary normalization mismatch")
	}
	zero := 0
	m := message{ID: "100", ChannelID: "33", Author: user{ID: "11", Bot: true}, Content: "report", Type: &zero}
	if validateMessage(m, testTarget, "report", "", strings.Repeat("a", 24)) != nil {
		t.Fatal("optional REST nonce rejected")
	}
	m.Nonce = json.RawMessage(`"wrong"`)
	if validateMessage(m, testTarget, "report", "", strings.Repeat("a", 24)) == nil {
		t.Fatal("wrong nonce accepted")
	}
}
