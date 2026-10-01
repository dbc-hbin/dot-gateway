package collector

import (
	"context"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"insane-search-migration/internal/engine"
)

type nativeHTTPFixture struct {
	body     string
	finalURL string
	calls    int
}

func (f *nativeHTTPFixture) Do(_ context.Context, r engine.HTTPRequest) (engine.HTTPResponse, error) {
	f.calls++
	finalURL := f.finalURL
	if finalURL == "" {
		finalURL = r.URL
	}
	return engine.HTTPResponse{URL: finalURL, Status: 200, Body: []byte(f.body), Headers: http.Header{"Content-Type": []string{"text/html"}}}, nil
}
func TestNativeEmptyListingAndParserDiagnostics(t *testing.T) {
	for _, body := range []string{`[]`, `<html><body><pre>[]</pre></body></html>`, `{"topic_list":{"topics":[]}}`} {
		fake := &nativeHTTPFixture{body: body}
		fetcher := &engine.Fetcher{HTTP: fake, Sleep: func(context.Context, time.Duration) error { return nil }}
		backend := NewNativeBackend(fetcher, time.Second)
		rows, failure, e := backend.Listing(context.Background(), Source{Name: "v2ex-latest", URL: "https://public.example/api"})
		if e != nil || failure != "" || len(rows) != 0 || fake.calls != 1 {
			t.Fatalf("empty listing %s: %v %s %+v calls%d", body, e, failure, rows, fake.calls)
		}
		d := backend.Diagnostics()
		if len(d) != 1 || !d[0].Recognized || d[0].Parsed != 0 {
			t.Fatal(d)
		}
	}
	for _, body := range []string{`{}`, `<html><body><h1>Maintenance</h1>` + strings.Repeat("The public website is temporarily unavailable. ", 10) + `</body></html>`} {
		fake := &nativeHTTPFixture{body: body}
		fetcher := &engine.Fetcher{HTTP: fake, Sleep: func(context.Context, time.Duration) error { return nil }}
		backend := NewNativeBackend(fetcher, time.Second)
		_, failure, e := backend.Listing(context.Background(), Source{Name: "v2ex-latest", URL: "https://public.example/api"})
		if e != nil || failure == "" {
			t.Fatalf("unrecognized content silently accepted: %s %v", body, e)
		}
	}
}

func TestNativeListingUsesObservedFinalURL(t *testing.T) {
	body, e := os.ReadFile("../extract/testdata/dcinside_desktop_observed.html")
	if e != nil {
		t.Fatal(e)
	}
	fixture := &nativeHTTPFixture{body: string(body), finalURL: "https://gall.dcinside.com/mgallery/board/lists/?id=ai_utilize"}
	backend := NewNativeBackend(&engine.Fetcher{HTTP: fixture, Sleep: func(context.Context, time.Duration) error { return nil }}, time.Second)
	rows, failure, e := backend.Listing(context.Background(), Source{Name: "dcinside-ai-utilize", URL: "https://m.dcinside.com/board/ai_utilize"})
	if e != nil || failure != "" || len(rows) != 2 {
		t.Fatal(rows, failure, e)
	}
	keys := map[string]bool{}
	for _, row := range rows {
		if !strings.HasPrefix(row.URL, "https://gall.dcinside.com/mgallery/board/view/") {
			t.Fatalf("relative URL used original mobile base: %s", row.URL)
		}
		keys[CanonicalURL(row.URL)] = true
	}
	if len(keys) != 2 || !keys["https://gall.dcinside.com/mgallery/board/view/?id=ai_utilize&no=1001"] || !keys["https://gall.dcinside.com/mgallery/board/view/?id=ai_utilize&no=1002"] {
		t.Fatal(keys)
	}
}
func TestDCInsideCanonicalIdentityIsNarrowAndStable(t *testing.T) {
	prefix := "https://gall.dcinside.com/mgallery/board/view"
	a := CanonicalURL(prefix + "/?id=ai_utilize&no=1001&page=1")
	b := CanonicalURL(prefix + "/?page=8&t=cv&no=1001&id=ai_utilize#comment")
	c := CanonicalURL(prefix + "/?id=ai_utilize&no=1002")
	if a != b || a == c || a != prefix+"/?id=ai_utilize&no=1001" {
		t.Fatal(a, b, c)
	}
	for _, raw := range []string{prefix + "/?id=other&no=1001", prefix + "/?id=ai_utilize&no=abc", prefix + "/?id=ai_utilize&no=1&no=2", prefix + "/?id=ai_utilize&id=other&no=1", "https://www.gall.dcinside.com/mgallery/board/view/?id=ai_utilize&no=1", "https://gall.dcinside.com.evil.example/mgallery/board/view/?id=ai_utilize&no=1", "https://gall.dcinside.com/mgallery/board/lists/?id=ai_utilize&no=1", "https://gall.dcinside.com:443/mgallery/board/view/?id=ai_utilize&no=1", "https://user:secret@gall.dcinside.com/mgallery/board/view/?id=ai_utilize&no=1"} {
		if got := CanonicalURL(raw); strings.Contains(got, "?") {
			t.Fatalf("query preserved beyond observed exact route: %s => %s", raw, got)
		}
	}
	if got := CanonicalURL("http://www.v2ex.com/t//123/?id=ai_utilize&no=1001"); got != "https://v2ex.com/t/123" {
		t.Fatal("legacy behavior changed", got)
	}
}
