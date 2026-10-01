package search

import (
	"context"
	"encoding/base64"
	"errors"
	"net/url"
	"strings"
	"testing"
)

const ddgFixture = "<html><div class='result web-result'><div class='result__body'><h2><a class='result__a' href='//duckduckgo.com/l/?uddg=https%3A%2F%2Fexample.org%2Foffer'>Claude <b>student</b></a></h2><a class='result__snippet'>Free <b>month</b></a></div></div></html>"
const bingFixture = "<?xml version='1.0'?><rss version='2.0'><channel><item><title>Gemini offer</title><link>https://example.org/gemini</link><description>Public credit</description></item></channel></rss>"

func TestProviderParsersAndSchema(t *testing.T) {
	calls := []string{}
	got, err := Search(context.Background(), "AI student 한글", 5, func(ctx context.Context, raw string) (string, int, error) {
		calls = append(calls, raw)
		u, e := url.Parse(raw)
		if e != nil {
			t.Fatal(e)
		}
		if u.Query().Get("q") != "AI student 한글" {
			t.Fatal(raw)
		}
		if _, ok := ctx.Deadline(); !ok {
			t.Fatal("unbounded provider context")
		}
		if strings.Contains(u.Host, "duckduckgo") {
			return ddgFixture, 200, nil
		}
		return bingFixture, 200, nil
	})
	if err != nil || !got.Success || got.Data == nil || len(got.Data.Web) != 2 || len(calls) != 2 {
		t.Fatalf("%+v %v %v", got, err, calls)
	}
	first := got.Data.Web[0]
	if first.Title != "Claude student" || first.Description != "Free month" || first.URL != "https://example.org/offer" || first.Position != 1 {
		t.Fatal(first)
	}
	if got.Data.Web[1].Position != 2 {
		t.Fatal(got)
	}
}
func TestLimitsAndShortCircuit(t *testing.T) {
	for _, limit := range []int{-1, 0, 1} {
		calls := 0
		result, err := Search(context.Background(), "query", limit, func(context.Context, string) (string, int, error) { calls++; return ddgFixture, 200, nil })
		if err != nil || !result.Success || len(result.Data.Web) != 1 || calls != 1 {
			t.Fatalf("%+v %v %d", result, err, calls)
		}
	}
	var items strings.Builder
	for i := 0; i < 25; i++ {
		items.WriteString("<item><title>title</title><link>https://example.org/" + string(rune('a'+i)) + "</link></item>")
	}
	result, err := Search(context.Background(), "query", 100, func(_ context.Context, raw string) (string, int, error) {
		if strings.Contains(raw, "duckduckgo") {
			return "captcha", 403, nil
		}
		return "<rss><channel>" + items.String() + "</channel></rss>", 200, nil
	})
	if err != nil || len(result.Data.Web) != 20 {
		t.Fatalf("%+v %v", result, err)
	}
}
func TestBlockedEmptyAndFailureRemainDistinct(t *testing.T) {
	for _, body := range []string{"<h1>Site Unavailable</h1>", "<html><div id='app'></div></html>"} {
		result, err := Search(context.Background(), "query", 5, func(context.Context, string) (string, int, error) { return body, 200, nil })
		if err != nil || result.Success || result.Error == "" || result.Data != nil {
			t.Fatalf("blocked counted as empty: %+v %v", result, err)
		}
	}
	result, err := Search(context.Background(), "query", 5, func(context.Context, string) (string, int, error) { return "", 0, errors.New("provider offline") })
	if err != nil || result.Success || len(result.Diagnostics) != 2 {
		t.Fatalf("%+v %v", result, err)
	}
	result, err = Search(context.Background(), "query", 5, func(_ context.Context, raw string) (string, int, error) {
		if strings.Contains(raw, "duckduckgo") {
			return "<div class='no-results__message'>No results</div>", 200, nil
		}
		return "", 503, nil
	})
	if err != nil || !result.Success || result.Data == nil || len(result.Data.Web) != 0 {
		t.Fatalf("verified empty rejected: %+v %v", result, err)
	}
}
func TestCancellationAndInvalidInput(t *testing.T) {
	calls := 0
	fetch := func(context.Context, string) (string, int, error) { calls++; return ddgFixture, 200, nil }
	for _, query := range []string{"", "  ", strings.Repeat("한", 8193)} {
		if _, err := Search(context.Background(), query, 5, fetch); err == nil {
			t.Fatal("invalid query accepted")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Search(ctx, "query", 5, fetch); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if calls != 0 {
		t.Fatal("invalid or cancelled search reached provider")
	}
}
func TestWrappedURLsAndSafety(t *testing.T) {
	want := "https://example.org/native?q=1"
	encoded := base64.RawURLEncoding.EncodeToString([]byte(want))
	for _, raw := range []string{"https://www.bing.com/ck/a?u=a1" + encoded, "/ck/a?u=a1" + encoded, "/l/?uddg=" + url.QueryEscape(want)} {
		if got := safeResultURL(raw); got != want {
			t.Fatalf("%q -> %q", raw, got)
		}
	}
	for _, raw := range []string{"javascript:alert(1)", "https://user:secret@example.org", "file:///tmp/data", "https://duckduckgo.com/y.js?ad=1", "https://www.bing.com/aclick?q=ad", "/l/?uddg=javascript%3Aalert(1)"} {
		if got := safeResultURL(raw); got != "" {
			t.Fatal("unsafe search URL accepted", got)
		}
	}
	hits := normalize([]Hit{{URL: want}, {URL: want}, {URL: "https://example.org/second"}}, 5)
	if len(hits) != 2 || hits[1].Position != 2 {
		t.Fatal(hits)
	}
}
func TestBingHTMLFallback(t *testing.T) {
	hits, recognized, err := parseBing("<ol><li class='b_algo'><h2><a href='https://example.org'>Public <strong>offer</strong></a></h2><p>Credit information</p></li></ol>")
	if err != nil || !recognized || len(hits) != 1 || hits[0].Title != "Public offer" || hits[0].Description != "Credit information" {
		t.Fatalf("%+v %v %v", hits, recognized, err)
	}
}
