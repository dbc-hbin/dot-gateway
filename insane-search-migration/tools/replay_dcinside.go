// replay_dcinside performs no network requests. It replays an authorized saved
// untrusted fetch envelope through the production NativeBackend and canonicalizer.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"insane-search-migration/internal/collector"
	"insane-search-migration/internal/engine"
)

type fixtureHTTP struct{ body, url string }

func (f fixtureHTTP) Do(context.Context, engine.HTTPRequest) (engine.HTTPResponse, error) {
	return engine.HTTPResponse{URL: f.url, Status: 200, Body: []byte(f.body), Headers: http.Header{"Content-Type": []string{"text/html"}}}, nil
}
func main() {
	if len(os.Args) != 2 {
		panic("usage: go run tools/replay_dcinside.go <saved-fetch.json>")
	}
	raw, e := os.ReadFile(os.Args[1])
	if e != nil {
		panic(e)
	}
	var payload struct {
		Result  engine.Result `json:"result"`
		Content string        `json:"content"`
	}
	if e = json.Unmarshal(raw, &payload); e != nil {
		panic(e)
	}
	begin, end := payload.Result.UntrustedContentBoundary["begin"], payload.Result.UntrustedContentBoundary["end"]
	if begin == "" || end == "" {
		panic("missing safety envelope")
	}
	_, body, ok := strings.Cut(payload.Content, begin+"\n")
	if !ok {
		panic("missing opening boundary")
	}
	body, _, ok = strings.Cut(body, "\n"+end)
	if !ok {
		panic("missing closing boundary")
	}
	backend := collector.NewNativeBackend(&engine.Fetcher{HTTP: fixtureHTTP{body: body, url: payload.Result.FinalURL}, Sleep: func(context.Context, time.Duration) error { return nil }}, 5*time.Second)
	rows, failure, e := backend.Listing(context.Background(), collector.Source{Name: "dcinside-ai-utilize", URL: "https://m.dcinside.com/board/ai_utilize"})
	if e != nil {
		panic(e)
	}
	keys := map[string]bool{}
	for _, row := range rows {
		keys[collector.CanonicalURL(row.URL)] = true
	}
	out := map[string]any{"network_requests": 0, "final_url": payload.Result.FinalURL, "parsed": len(rows), "unique_canonical_keys": len(keys), "failure": failure, "source_diagnostics": backend.Diagnostics()}
	encoded, e := json.MarshalIndent(out, "", "  ")
	if e != nil {
		panic(e)
	}
	fmt.Println(string(encoded))
	if failure != "" || len(rows) == 0 || len(keys) != len(rows) {
		os.Exit(1)
	}
}
