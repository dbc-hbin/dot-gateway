package collector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"insane-search-migration/internal/engine"
)

func TestListingPaginationAllowedRoutes(t *testing.T) {
	for _, tc := range []struct{ name, source, final, hint, want string }{
		{"linux.do-latest", "https://linux.do/latest.json", "https://linux.do/latest.json", "/latest?no_definitions=true&page=1", "https://linux.do/latest.json?no_definitions=true&page=1"},
		{"linux.do-welfare", "https://linux.do/c/welfare/36.json", "https://linux.do/c/welfare/36.json", "/c/welfare/36?page=1", "https://linux.do/c/welfare/36.json?page=1"},
		{"nodeloc-latest", "https://www.nodeloc.com/latest.json", "https://www.nodeloc.com/latest.json?page=1", "/latest?page=2", "https://www.nodeloc.com/latest.json?page=2"},
		{"nodeloc-deals", "https://www.nodeloc.com/c/business/information/10.json", "https://www.nodeloc.com/c/business/information/10.json", "https://www.nodeloc.com/c/business/information/10?page=1", "https://www.nodeloc.com/c/business/information/10.json?page=1"},
		{"dcinside-ai-utilize", "https://m.dcinside.com/board/ai_utilize", "https://m.dcinside.com/board/ai_utilize", "/board/ai_utilize?page=2", "https://m.dcinside.com/board/ai_utilize?page=2"},
		{"dcinside-ai-utilize", "https://m.dcinside.com/board/ai_utilize", "https://gall.dcinside.com/mgallery/board/lists/?id=ai_utilize", "/mgallery/board/lists/?id=ai_utilize&page=2", "https://gall.dcinside.com/mgallery/board/lists/?id=ai_utilize&page=2"},
	} {
		t.Run(tc.name+tc.hint, func(t *testing.T) {
			r, ok := routeForListing(Source{tc.name, tc.source})
			if !ok {
				t.Fatal("configured route rejected")
			}
			base, current, ok := r.parse(tc.final)
			if !ok {
				t.Fatal("observed final rejected")
			}
			if got := r.safeNext(base, current, tc.hint); got != tc.want {
				t.Fatalf("got %q want %q", got, tc.want)
			}
		})
	}
}

func TestListingPaginationHostileHints(t *testing.T) {
	r, _ := routeForListing(Source{"linux.do-latest", "https://linux.do/latest.json"})
	base, current, _ := r.parse("https://linux.do/latest.json")
	for _, hint := range []string{
		"https://evil.example/latest?page=1", "https://linux.do.evil.example/latest?page=1", "http://linux.do/latest?page=1",
		"https://user:secret@linux.do/latest?page=1", "https://linux.do:443/latest?page=1", "https://127.0.0.1/latest?page=1",
		"https://linux.do./latest?page=1", "//linux.do/latest?page=1", "javascript:alert(1)", "file:///latest?page=1",
		"/login?page=1", "/admin?page=1", "/c/welfare/36?page=1", "/%6catest?page=1", "/other/../latest?page=1",
		"/latest?page=1&page=2", "/latest?page=1&redirect=https://evil.example", "/latest?page=1&api_key=secret", "/latest?page=1#next",
		"/latest?page=0", "/latest?page=2", "/latest?page=-1", "/latest?page=01", "/latest?page=+1", "/latest?page=1%00",
		"/latest?%70age=1", "/latest?page=1;page=2", "/latest?page=999999999999999999999999", "/latest?page=1&no_definitions=false",
		" /latest?page=1", "/latest?page=1\n", "https://linux.do\\@evil.example/latest?page=1",
	} {
		t.Run(hint, func(t *testing.T) {
			if next := r.safeNext(base, current, hint); next != "" {
				t.Fatalf("unsafe hint %q became %q", hint, next)
			}
		})
	}
	dc, _ := routeForListing(Source{"dcinside-ai-utilize", "https://m.dcinside.com/board/ai_utilize"})
	base, current, _ = dc.parse("https://gall.dcinside.com/mgallery/board/lists/?id=ai_utilize")
	for _, hint := range []string{"?id=other&page=2", "?id=ai_utilize&id=other&page=2", "?id=ai_utilize&page=2&no=100", "/mgallery/board/view/?id=ai_utilize&page=2", "https://m.dcinside.com/board/ai_utilize?page=2", "?id=ai_utilize&page=3"} {
		if next := dc.safeNext(base, current, hint); next != "" {
			t.Fatalf("unsafe DCInside hint %q became %q", hint, next)
		}
	}
}

func TestListingPaginationRequiresObservedHint(t *testing.T) {
	r, _ := routeForListing(Source{"linux.do-latest", "https://linux.do/latest.json"})
	for _, body := range []string{`{"topic_list":{"topics":[]}}`, `<a href="/latest?page=1">2</a>`, `{"next":"/latest?page=1"}`} {
		if next, _ := r.next("https://linux.do/latest.json", body); next != "" {
			t.Fatal(next)
		}
	}
	if _, ok := routeForListing(Source{"v2ex-latest", "https://www.v2ex.com/api/topics/latest.json"}); ok {
		t.Fatal("V2EX gained unverified pagination")
	}
	if _, ok := routeForListing(Source{"linux.do-latest", "https://evil.example/latest.json"}); ok {
		t.Fatal("unconfigured host gained pagination")
	}
	dc, _ := routeForListing(Source{"dcinside-ai-utilize", "https://m.dcinside.com/board/ai_utilize"})
	next, failure := dc.next("https://gall.dcinside.com/mgallery/board/lists/?id=ai_utilize", `<a href="https://evil.example/?page=2">2</a><a href="?id=ai_utilize&amp;page=2">2</a>`)
	if failure != "" || next != "https://gall.dcinside.com/mgallery/board/lists/?id=ai_utilize&page=2" {
		t.Fatal(next, failure)
	}
}

type paginationHTTPFixture struct {
	calls    []string
	response func(context.Context, engine.HTTPRequest) (engine.HTTPResponse, error)
}

func (f *paginationHTTPFixture) Do(ctx context.Context, r engine.HTTPRequest) (engine.HTTPResponse, error) {
	f.calls = append(f.calls, r.URL)
	return f.response(ctx, r)
}
func paginationResponse(r engine.HTTPRequest, body string) engine.HTTPResponse {
	return engine.HTTPResponse{URL: r.URL, Status: 200, Body: []byte(body), Headers: http.Header{"Content-Type": {"application/json"}}}
}
func discoursePage(next string, ids ...int) string {
	topics := []map[string]any{}
	for _, id := range ids {
		topics = append(topics, map[string]any{"id": id, "title": fmt.Sprintf("Claude free credits %d", id), "slug": "offer"})
	}
	b, _ := json.Marshal(map[string]any{"topic_list": map[string]any{"topics": topics, "more_topics_url": next}})
	return string(b)
}
func newPaginationBackend(f *paginationHTTPFixture) *NativeBackend {
	b := NewNativeBackend(&engine.Fetcher{HTTP: f, Sleep: func(context.Context, time.Duration) error { return nil }}, time.Second)
	b.Options = WideScanOptions()
	b.listingSleep = func(context.Context, time.Duration) error { return nil }
	return b
}

var paginationSource = Source{"linux.do-latest", "https://linux.do/latest.json"}

func TestNativeListingPaginationCapsAndDeduplicates(t *testing.T) {
	for _, tc := range []struct {
		name                            string
		pages, cap, wantRows, wantPages int
		stop                            string
	}{
		{"page cap", 3, 120, 4, 3, "page_limit"}, {"row cap", 5, 3, 3, 2, "row_limit"}, {"legacy", 1, 40, 2, 1, "page_limit"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &paginationHTTPFixture{response: func(_ context.Context, r engine.HTTPRequest) (engine.HTTPResponse, error) {
				u, _ := url.Parse(r.URL)
				page := 0
				fmt.Sscan(u.Query().Get("page"), &page)
				return paginationResponse(r, discoursePage(fmt.Sprintf("/latest?page=%d", page+1), page+1, page+2)), nil
			}}
			b := newPaginationBackend(f)
			b.Options.MaxListingPages = tc.pages
			b.Options.MaxScanPerSource = tc.cap
			sleeps := 0
			b.listingSleep = func(ctx context.Context, d time.Duration) error {
				sleeps++
				if d < 250*time.Millisecond {
					t.Fatalf("delay too short: %v", d)
				}
				return nil
			}
			rows, failure, err := b.Listing(context.Background(), paginationSource)
			if err != nil || failure != "" || len(rows) != tc.wantRows || len(f.calls) != tc.wantPages || sleeps != tc.wantPages-1 {
				t.Fatal(rows, failure, err, f.calls, sleeps)
			}
			d := b.Diagnostics()[0]
			if d.PagesAttempted != tc.wantPages || d.PagesFetched != tc.wantPages || d.Scanned != tc.wantRows || d.PaginationStop != tc.stop {
				t.Fatal(d)
			}
		})
	}
}

func TestNativeListingPaginationStopsEmptyRepeatedAndBadHints(t *testing.T) {
	for _, tc := range []struct {
		name, second, stop string
		want               int
	}{
		{"empty", discoursePage("/latest?page=2"), "empty_page", 2},
		{"repeated rows", discoursePage("/latest?page=2", 1, 2), "repeated_page", 2},
		{"repeated hint", discoursePage("/latest?page=1", 3), "continuation_rejected", 3},
		{"malformed", `{"unexpected":true}`, "continuation_failed", 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &paginationHTTPFixture{response: func(_ context.Context, r engine.HTTPRequest) (engine.HTTPResponse, error) {
				body := discoursePage("/latest?page=1", 1, 2)
				if strings.Contains(r.URL, "page=1") {
					body = tc.second
				}
				return paginationResponse(r, body), nil
			}}
			b := newPaginationBackend(f)
			rows, failure, err := b.Listing(context.Background(), paginationSource)
			if err != nil || failure != "" || len(rows) != tc.want || len(f.calls) != 2 {
				t.Fatal(rows, failure, err, f.calls)
			}
			if d := b.Diagnostics()[0]; d.PaginationStop != tc.stop {
				t.Fatal(d)
			}
		})
	}
}

func TestNativeListingPartialPublicWallAndRedirectFailures(t *testing.T) {
	for _, mode := range []string{"auth", "trust", "redirect", "timeout"} {
		t.Run(mode, func(t *testing.T) {
			f := &paginationHTTPFixture{response: func(ctx context.Context, r engine.HTTPRequest) (engine.HTTPResponse, error) {
				if !strings.Contains(r.URL, "page=1") {
					return paginationResponse(r, discoursePage("/latest?page=1", 1)), nil
				}
				resp := paginationResponse(r, discoursePage("/latest?page=2", 2))
				switch mode {
				case "auth":
					resp.Status = 401
				case "trust":
					resp.Body = []byte(`<html><body>登录后可见</body></html>`)
				case "redirect":
					resp.URL = "https://evil.example/latest.json?page=1"
				case "timeout":
					<-ctx.Done()
					return engine.HTTPResponse{}, ctx.Err()
				}
				return resp, nil
			}}
			b := newPaginationBackend(f)
			b.OperationTimeout = 20 * time.Millisecond
			rows, failure, err := b.Listing(context.Background(), paginationSource)
			if err != nil || failure != "" || len(rows) != 1 || len(f.calls) != 2 {
				t.Fatal(rows, failure, err, f.calls)
			}
			d := b.Diagnostics()[0]
			if d.ContinuationError == "" || d.PagesAttempted != 2 || d.PaginationStop != "continuation_failed" {
				t.Fatal(d)
			}
		})
	}
}

func TestNativeListingCancellationAndDefaultLimits(t *testing.T) {
	f := &paginationHTTPFixture{response: func(_ context.Context, r engine.HTTPRequest) (engine.HTTPResponse, error) {
		return paginationResponse(r, discoursePage("/latest?page=1", 1)), nil
	}}
	b := newPaginationBackend(f)
	ctx, cancel := context.WithCancel(context.Background())
	b.listingSleep = func(ctx context.Context, _ time.Duration) error { cancel(); return ctx.Err() }
	rows, _, err := b.Listing(ctx, paginationSource)
	if !errors.Is(err, context.Canceled) || len(rows) != 1 || len(f.calls) != 1 {
		t.Fatal(rows, err, f.calls)
	}
	b.listingSleep = nil
	if err := b.waitForListingPage(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	b.Options = ScanOptions{}
	f.calls = nil
	rows, failure, err := b.Listing(context.Background(), paginationSource)
	if err != nil || failure != "" || len(rows) != 1 || len(f.calls) != 1 {
		t.Fatal(rows, failure, err, f.calls)
	}
	if got := NewNativeBackend(b.Fetcher, time.Second).Options; got != LegacyScanOptions() {
		t.Fatal(got)
	}
}

func TestNativeListingV2EXRemainsOnePage(t *testing.T) {
	f := &paginationHTTPFixture{response: func(_ context.Context, r engine.HTTPRequest) (engine.HTTPResponse, error) {
		return paginationResponse(r, discoursePage("/latest?page=1", 1)), nil
	}}
	b := newPaginationBackend(f)
	rows, failure, err := b.Listing(context.Background(), Source{"v2ex-latest", "https://www.v2ex.com/api/topics/latest.json"})
	if err != nil || failure != "" || len(rows) != 1 || len(f.calls) != 1 {
		t.Fatal(rows, failure, err, f.calls)
	}
	if d := b.Diagnostics()[0]; d.PaginationStop != "unsupported_pagination" {
		t.Fatal(d)
	}
}

func TestNativeLegacyKeepsDuplicateRowsBeforeScanBoundary(t *testing.T) {
	ids := []int{1, 1}
	for id := 2; id <= 40; id++ {
		ids = append(ids, id)
	}
	f := &paginationHTTPFixture{response: func(_ context.Context, r engine.HTTPRequest) (engine.HTTPResponse, error) {
		return paginationResponse(r, discoursePage("/latest?page=1", ids...)), nil
	}}
	b := NewNativeBackend(&engine.Fetcher{HTTP: f}, time.Second)
	rows, failure, err := b.Listing(context.Background(), paginationSource)
	if err != nil || failure != "" || len(rows) != 41 || rows[0].URL != rows[1].URL || !strings.HasSuffix(rows[40].URL, "/40") {
		t.Fatal(rows, failure, err)
	}
	for _, row := range rows[:MaxScanPerSource] {
		if strings.HasSuffix(row.URL, "/40") {
			t.Fatal("raw position 41 crossed legacy scan boundary")
		}
	}
	if d := b.Diagnostics()[0]; d.Scanned != 40 || d.Parsed != 41 || d.PagesAttempted != 1 {
		t.Fatal(d)
	}
}

func TestNativeDiagnosticErrorsAreRedacted(t *testing.T) {
	secret := "sk-123456789abcdefghijklmnop"
	b := NewNativeBackend(nil, time.Second)
	b.diagnostics = []SourceDiagnostic{{Error: "request failed: " + secret, ContinuationError: "page_2: " + secret}}
	d := b.Diagnostics()[0]
	if strings.Contains(d.Error, secret) || strings.Contains(d.ContinuationError, secret) || d.Error == "" || d.ContinuationError == "" {
		t.Fatal(d)
	}
	if !strings.Contains(b.diagnostics[0].Error, secret) {
		t.Fatal("Diagnostics mutated internal state")
	}
}

func TestNativeListingDCInsideObservedRedirectPagination(t *testing.T) {
	f := &paginationHTTPFixture{response: func(_ context.Context, r engine.HTTPRequest) (engine.HTTPResponse, error) {
		id, next := 1001, 2
		if strings.Contains(r.URL, "page=2") {
			id, next = 1002, 3
		}
		body := fmt.Sprintf(`<html><body><table><tr><td class="gall_tit"><a href="/mgallery/board/view/?id=ai_utilize&amp;no=%d">Synthetic Claude free credit coupon</a></td></tr></table><p>%s</p><a href="/mgallery/board/lists/?id=ai_utilize&amp;page=%d">%d</a></body></html>`, id, strings.Repeat("Public community listing information. ", 15), next, next)
		resp := paginationResponse(r, body)
		resp.Headers.Set("Content-Type", "text/html")
		if strings.Contains(r.URL, "m.dcinside.com") {
			resp.URL = "https://gall.dcinside.com/mgallery/board/lists/?id=ai_utilize"
		}
		return resp, nil
	}}
	b := newPaginationBackend(f)
	b.Options.MaxListingPages = 2
	rows, failure, err := b.Listing(context.Background(), Source{"dcinside-ai-utilize", "https://m.dcinside.com/board/ai_utilize"})
	if err != nil || failure != "" || len(rows) != 2 || len(f.calls) != 2 || f.calls[1] != "https://gall.dcinside.com/mgallery/board/lists/?id=ai_utilize&page=2" {
		t.Fatal(rows, failure, err, f.calls)
	}
}

func TestNativeListingNeverFetchesHostileContinuation(t *testing.T) {
	f := &paginationHTTPFixture{response: func(_ context.Context, r engine.HTTPRequest) (engine.HTTPResponse, error) {
		return paginationResponse(r, discoursePage("http://169.254.169.254/latest?page=1", 1)), nil
	}}
	b := newPaginationBackend(f)
	rows, failure, err := b.Listing(context.Background(), paginationSource)
	if err != nil || failure != "" || len(rows) != 1 || len(f.calls) != 1 {
		t.Fatal(rows, failure, err, f.calls)
	}
	if d := b.Diagnostics()[0]; d.ContinuationError != "unsafe_or_repeated_next_page" {
		t.Fatal(d)
	}
}

func TestListingPageWaitIsCancellable(t *testing.T) {
	b := NewNativeBackend(nil, time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	timer := time.AfterFunc(5*time.Millisecond, cancel)
	defer timer.Stop()
	if err := b.waitForListingPage(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
