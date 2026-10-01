package collector

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func recallFixture(t *testing.T, titles ...string) *Fixture {
	t.Helper()
	rows := []Row{}
	for i, title := range titles {
		rows = append(rows, Row{Title: title, URL: fmt.Sprintf("https://fixture.example/t/%d", i)})
	}
	raw, err := json.Marshal(map[string]any{"listings": map[string]any{"linux.do-welfare": map[string]any{"rows": rows}}, "bodies": map[string]any{}})
	if err != nil {
		t.Fatal(err)
	}
	f, err := ParseFixture(raw)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func TestWideRecallCluesAndReviewHints(t *testing.T) {
	for _, title := range []string{"AI free access", "Groq credits available", "클로드 체험권 증정", "大模型 限时赠金", "GPT launch offer", "Claude referral discount", "Claude free release v2", "Claude free quota 推测"} {
		if WorthCollecting(title, "", "linux.do-welfare", "https://fixture.example/t/1") {
			t.Fatalf("test does not exercise broadened recall: %q", title)
		}
		if !WorthReviewing(title, "", "linux.do-welfare", "https://fixture.example/t/1") {
			t.Fatalf("missed broader clue: %q", title)
		}
	}
	f := recallFixture(t, "Claude referral discount")
	report, err := ScanWithOptions(context.Background(), f, NewState(), WideScanOptions())
	if err != nil || len(report.Candidates) != 1 {
		t.Fatal(report, err)
	}
	flags := strings.Join(report.Candidates[0].QualityFlags, ",")
	if !strings.Contains(flags, "review_affiliate_or_self_promo") || !strings.Contains(flags, "assistant_review_required") || !strings.Contains(flags, "broadened_recall") {
		t.Fatal(flags)
	}
}

func TestWideRecallRetainsCredentialRelayAndTrustBoundaries(t *testing.T) {
	for _, title := range []string{"Claude coupon sk-abcdefghijklmnop", "Claude base64 key free", "Claude API key 分享 coupon", "Claude 账号出售 discount", "Claude 中转站 优惠", "Claude 全模型 首充 100 discount", "Claude free 需要信任等级", "ordinary weather offer", "Claude latest model announcement"} {
		if WorthReviewing(title, "", "linux.do-welfare", "https://fixture.example/t/1") {
			t.Fatalf("unsafe or irrelevant candidate: %q", title)
		}
	}
	f := recallFixture(t, "Claude free credits", "Claude coupon $100", "Groq credits")
	listing := f.Listings["linux.do-welfare"]
	listing.Rows[0].URL = "https://user:password@fixture.example/t/0"
	listing.Rows[2].URL = "https://fixture.example/sk-abcdefghijklmnop"
	f.Listings["linux.do-welfare"] = listing
	f.Bodies["https://fixture.example/t/1"] = struct {
		Text  string `json:"text"`
		Error string `json:"error"`
	}{"登录后可见 sk-abcdefghijklmnop", "api_key=abcdefghijklmnop"}
	report, err := ScanWithOptions(context.Background(), f, NewState(), WideScanOptions())
	if err != nil || len(report.Candidates) != 1 {
		t.Fatal(report, err)
	}
	raw, _ := json.Marshal(report)
	if strings.Contains(string(raw), "abcdefghijklmnop") || strings.Contains(string(raw), "password") || report.Candidates[0].Excerpt != "" {
		t.Fatalf("unsafe report: %s", raw)
	}
}

func TestWideFingerprintReopensOnlyNewEligibilityOnce(t *testing.T) {
	f := recallFixture(t, "Claude referral discount", "Claude coupon $100")
	state := NewState()
	for _, r := range f.Listings["linux.do-welfare"].Rows {
		state.Set(r.URL, Fingerprint(r.Title+" "))
	}
	report, err := ScanWithOptions(context.Background(), f, state, WideScanOptions())
	if err != nil || len(report.Candidates) != 1 || report.Candidates[0].Title != "Claude referral discount" {
		t.Fatal(report, err)
	}
	report, err = ScanWithOptions(context.Background(), f, state, WideScanOptions())
	if err != nil || len(report.Candidates) != 0 {
		t.Fatal(report, err)
	}
}

func TestWideLimitsAndUnseenFirst(t *testing.T) {
	titles := []string{}
	for i := 0; i < 150; i++ {
		titles = append(titles, fmt.Sprintf("Claude student plan coupon $%03d", i))
	}
	f := recallFixture(t, titles...)
	state := NewState()
	first, err := ScanWithOptions(context.Background(), f, state, WideScanOptions())
	if err != nil || len(first.Candidates) != 12 || first.Collection.RowsScanned != 120 || first.Collection.DetailsSelected != 12 {
		t.Fatal(first.Collection, len(first.Candidates), err)
	}
	second, err := ScanWithOptions(context.Background(), f, state, WideScanOptions())
	if err != nil || len(second.Candidates) != 12 {
		t.Fatal(len(second.Candidates), err)
	}
	urls := map[string]bool{}
	for _, c := range first.Candidates {
		urls[c.URL] = true
	}
	for _, c := range second.Candidates {
		if urls[c.URL] {
			t.Fatal("unchanged known row starved new rows", c.URL)
		}
	}
	legacy, err := Scan(context.Background(), f, NewState())
	if err != nil || len(legacy.Candidates) != 4 {
		t.Fatal(len(legacy.Candidates), err)
	}
	options := WideScanOptions()
	options.MaxPerSource = 24
	options.MaxTotal = 5
	small, err := ScanWithOptions(context.Background(), f, NewState(), options)
	if err != nil || len(small.Candidates) != 5 {
		t.Fatal(len(small.Candidates), err)
	}
}

func TestScanOptionBounds(t *testing.T) {
	for _, profile := range []string{"wide", "legacy"} {
		o, err := OptionsForProfile(profile)
		if err != nil || o.Validate() != nil {
			t.Fatal(o, err)
		}
	}
	if _, err := OptionsForProfile("unbounded"); err == nil {
		t.Fatal("unknown profile accepted")
	}
	for _, change := range []func(*ScanOptions){func(o *ScanOptions) { o.MaxScanPerSource = 201 }, func(o *ScanOptions) { o.MaxPerSource = 25 }, func(o *ScanOptions) { o.MaxTotal = 97 }, func(o *ScanOptions) { o.MaxListingPages = 6 }, func(o *ScanOptions) { o.MaxTotal = 0 }} {
		o := WideScanOptions()
		change(&o)
		if _, err := ScanWithOptions(context.Background(), nil, nil, o); err == nil {
			t.Fatal("invalid limits accepted", o)
		}
	}
}

func TestWideMigrationAndRefreshCannotStarveKnownLowRank(t *testing.T) {
	titles := []string{}
	for i := 0; i < 12; i++ {
		titles = append(titles, fmt.Sprintf("Claude student plan coupon $%03d 教育 无需绑卡", i))
	}
	titles = append(titles, "Claude coupon")
	f := recallFixture(t, titles...)
	f.Bodies["https://fixture.example/t/12"] = struct {
		Text  string `json:"text"`
		Error string `json:"error"`
	}{"Claude referral discount", ""}
	state := NewState()
	for _, r := range f.Listings["linux.do-welfare"].Rows {
		part := ""
		if r.URL == "https://fixture.example/t/12" {
			part = "Claude referral discount"
		}
		state.Set(r.URL, Fingerprint(r.Title+" "+part))
	}
	first, err := ScanWithOptions(context.Background(), f, state, WideScanOptions())
	if err != nil || len(first.Candidates) != 0 {
		t.Fatal(first, err)
	}
	raw, _ := state.Bytes()
	state = DecodeState(raw)
	second, err := ScanWithOptions(context.Background(), f, state, WideScanOptions())
	if err != nil || len(second.Candidates) != 1 || second.Candidates[0].URL != "https://fixture.example/t/12" {
		t.Fatal(second, err)
	}
	third, err := ScanWithOptions(context.Background(), f, state, WideScanOptions())
	if err != nil || len(third.Candidates) != 0 {
		t.Fatal(third, err)
	}
	history, _, err := loadWideReviewHistory(state)
	if err != nil || len(history) != 13 {
		t.Fatal(history, err)
	}
}

func TestWideHistoryValidationAndPruning(t *testing.T) {
	for _, raw := range []string{`null`, `[]`, `{"x":0}`, `{"x":-1}`, `{"x":"1"}`, `{"x":18446744073709551615}`} {
		state := NewState()
		state.Fields[wideReviewField] = json.RawMessage(raw)
		if _, err := ScanWithOptions(context.Background(), recallFixture(t), state, WideScanOptions()); err == nil {
			t.Fatal("invalid history accepted", raw)
		}
	}
	state := NewState()
	state.Set("keep", "hash")
	history := map[string]uint64{"keep": 1, "stale": 2}
	if err := saveWideReviewHistory(state, history); err != nil {
		t.Fatal(err)
	}
	got, _, err := loadWideReviewHistory(state)
	if err != nil || len(got) != 1 || got["keep"] != 1 {
		t.Fatal(got, err)
	}
}

func TestWideDoesNotExpandGatedTitleOnlyException(t *testing.T) {
	f := recallFixture(t, "AI free access", "Claude coupon $100")
	for _, r := range f.Listings["linux.do-welfare"].Rows {
		f.Bodies[r.URL] = struct {
			Text  string `json:"text"`
			Error string `json:"error"`
		}{"登录后可见", ""}
	}
	report, err := ScanWithOptions(context.Background(), f, NewState(), WideScanOptions())
	if err != nil || len(report.Candidates) != 1 || report.Candidates[0].Title != "Claude coupon $100" || report.Candidates[0].Excerpt != "" {
		t.Fatal(report, err)
	}
}
