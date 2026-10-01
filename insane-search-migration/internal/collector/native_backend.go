package collector

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"insane-search-migration/internal/engine"
	"insane-search-migration/internal/extract"
)

type SourceDiagnostic struct {
	Source            string `json:"source"`
	Format            string `json:"format,omitempty"`
	Recognized        bool   `json:"recognized"`
	Parsed            int    `json:"parsed"`
	Scanned           int    `json:"scanned"`
	FetchVerdict      string `json:"fetch_verdict,omitempty"`
	Error             string `json:"error,omitempty"`
	PagesAttempted    int    `json:"pages_attempted"`
	PagesFetched      int    `json:"pages_fetched"`
	ContinuationError string `json:"continuation_error,omitempty"`
	PaginationStop    string `json:"pagination_stop,omitempty"`
}
type NativeBackend struct {
	Fetcher          *engine.Fetcher
	OperationTimeout time.Duration
	Options          ScanOptions
	listingSleep     func(context.Context, time.Duration) error
	mu               sync.Mutex
	diagnostics      []SourceDiagnostic
}

func NewNativeBackend(fetcher *engine.Fetcher, timeout time.Duration) *NativeBackend {
	return &NativeBackend{Fetcher: fetcher, OperationTimeout: timeout, Options: LegacyScanOptions(), diagnostics: []SourceDiagnostic{}}
}
func (b *NativeBackend) Diagnostics() []SourceDiagnostic {
	b.mu.Lock()
	defer b.mu.Unlock()
	diagnostics := append([]SourceDiagnostic{}, b.diagnostics...)
	for i := range diagnostics {
		diagnostics[i].Error = Redact(diagnostics[i].Error)
		diagnostics[i].ContinuationError = Redact(diagnostics[i].ContinuationError)
	}
	return diagnostics
}
func (b *NativeBackend) fetch(ctx context.Context, raw string, markdown bool, acceptEmpty ...bool) (string, string, string, error) {
	text, failure, verdict, _, err := b.fetchWithFinalURL(ctx, raw, markdown, acceptEmpty...)
	return text, failure, verdict, err
}
func (b *NativeBackend) fetchWithFinalURL(ctx context.Context, raw string, markdown bool, acceptEmpty ...bool) (string, string, string, string, error) {
	if b.OperationTimeout <= 0 {
		return "", "", "", "", fmt.Errorf("operation timeout must be positive")
	}
	ctx, cancel := context.WithTimeout(ctx, b.OperationTimeout)
	defer cancel()
	request := engine.DefaultRequest(raw)
	request.AcceptEmptyJSONArray = len(acceptEmpty) > 0 && acceptEmpty[0]
	request.EnableMarkdown = markdown
	request.EnableMainContent = markdown
	result, e := b.Fetcher.Fetch(ctx, request)
	if e != nil {
		return "", "", "", "", e
	}
	if !result.OK {
		reason := result.Verdict
		if reason == "" {
			reason = result.StopReason
		}
		if reason == "" {
			reason = "fetch_failed"
		}
		return "", reason, result.Verdict, result.FinalURL, nil
	}
	return result.Content, "", result.Verdict, result.FinalURL, nil
}
func (b *NativeBackend) Listing(ctx context.Context, source Source) ([]Row, string, error) {
	opts := b.Options
	if opts == (ScanOptions{}) {
		opts = LegacyScanOptions()
	}
	if err := opts.Validate(); err != nil {
		return nil, "", err
	}
	diag := SourceDiagnostic{Source: source.Name}
	defer func() {
		b.mu.Lock()
		b.diagnostics = append(b.diagnostics, diag)
		b.mu.Unlock()
	}()
	rows := []Row{}
	seen := map[string]bool{}
	route, supported := routeForListing(source)
	raw := source.URL
	for page := 1; page <= opts.MaxListingPages; page++ {
		if err := ctx.Err(); err != nil {
			return rows, "", err
		}
		if page > 1 {
			if err := b.waitForListingPage(ctx); err != nil {
				return rows, "", err
			}
		}
		diag.PagesAttempted++
		text, failure, verdict, finalURL, err := b.fetchWithFinalURL(ctx, raw, false, true)
		diag.FetchVerdict = verdict
		if err != nil {
			if page == 1 || ctx.Err() != nil {
				diag.Error = err.Error()
				return rows, "", err
			}
			failure = err.Error()
		}
		if failure != "" {
			if page == 1 {
				diag.Error = failure
				return nil, failure, nil
			}
			diag.ContinuationError = fmt.Sprintf("page_%d: %s", page, failure)
			diag.PaginationStop = "continuation_failed"
			return rows, "", nil
		}
		diag.PagesFetched++
		if finalURL == "" {
			finalURL = raw
		}
		if supported {
			_, expectedPage, _ := route.parse(raw)
			_, actualPage, safe := route.parse(finalURL)
			if !safe || expectedPage != actualPage {
				failure = "unsafe_or_repeated_listing_redirect"
			}
		}
		parsed, d := extract.ParseListing(source.Name, finalURL, text)
		if failure == "" && !d.Recognized {
			failure = d.Error
			if failure == "" {
				failure = "unrecognized_listing"
			}
		}
		if failure != "" {
			if page == 1 {
				diag.Error = failure
				return []Row{}, failure, nil
			}
			diag.ContinuationError = fmt.Sprintf("page_%d: %s", page, failure)
			diag.PaginationStop = "continuation_failed"
			return rows, "", nil
		}
		diag.Format, diag.Recognized = d.Format, true
		diag.Parsed += d.Parsed
		// Legacy discovery counted the first raw rows before deduplication.
		// Keep that exact boundary for the original single-page profile.
		if opts.Profile == "legacy" && opts.MaxListingPages == 1 {
			for _, r := range parsed {
				rows = append(rows, Row{Source: r.Source, Title: r.Title, URL: r.URL, ListingExcerpt: StripPoison(r.ListingExcerpt)})
			}
			diag.Scanned = min(len(rows), opts.MaxScanPerSource)
			diag.PaginationStop = "page_limit"
			return rows, "", nil
		}
		added := 0
		for _, r := range parsed {
			key := CanonicalURL(r.URL)
			if seen[key] {
				continue
			}
			seen[key] = true
			rows = append(rows, Row{Source: r.Source, Title: r.Title, URL: r.URL, ListingExcerpt: StripPoison(r.ListingExcerpt)})
			added++
			if len(rows) >= opts.MaxScanPerSource {
				break
			}
		}
		diag.Scanned = len(rows)
		switch {
		case len(rows) >= opts.MaxScanPerSource:
			diag.PaginationStop = "row_limit"
		case len(parsed) == 0:
			diag.PaginationStop = "empty_page"
		case added == 0:
			diag.PaginationStop = "repeated_page"
		case !supported:
			diag.PaginationStop = "unsupported_pagination"
		case page >= opts.MaxListingPages:
			diag.PaginationStop = "page_limit"
		default:
			var continuationError string
			raw, continuationError = route.next(finalURL, text)
			if continuationError != "" {
				diag.ContinuationError = continuationError
				diag.PaginationStop = "continuation_rejected"
			} else if raw == "" {
				diag.PaginationStop = "no_next_page"
			}
		}
		if diag.PaginationStop != "" {
			break
		}
	}
	return rows, "", nil
}
func (b *NativeBackend) Body(ctx context.Context, row Row) (string, string, error) {
	source, raw := row.Source, row.URL
	clean := func(s string) string { return StripPoison(extract.CleanText(s)) }
	if strings.HasPrefix(source, "linux.do") || strings.HasPrefix(source, "nodeloc") {
		id := extract.TopicID(raw)
		body, reason := "", ""
		if id == "" {
			reason = "missing_topic_id"
		} else {
			u, e := url.Parse(raw)
			if e != nil {
				return "", "", e
			}
			text, failure, _, e := b.fetch(ctx, u.Scheme+"://"+u.Host+"/t/"+id+".json", false)
			if e != nil {
				return "", "", e
			}
			reason = failure
			if reason == "" {
				var payload struct {
					PostStream struct {
						Posts []struct {
							Cooked string `json:"cooked"`
							Raw    string `json:"raw"`
						} `json:"posts"`
					} `json:"post_stream"`
				}
				if json.Unmarshal([]byte(extract.UnwrapJSON(text)), &payload) != nil {
					reason = "invalid_topic_json"
				} else if len(payload.PostStream.Posts) == 0 {
					reason = "empty_topic_posts"
				} else {
					body = payload.PostStream.Posts[0].Cooked
					if body == "" {
						body = payload.PostStream.Posts[0].Raw
					}
					body = clean(body)
				}
			}
		}
		if body != "" {
			return body, reason, nil
		}
		if reason != "" {
			fallback, htmlError, _, e := b.fetch(ctx, raw, true)
			if reason == "" {
				reason = htmlError
			}
			return clean(fallback), reason, e
		}
		return "", reason, nil
	}
	if strings.HasPrefix(source, "v2ex") {
		id := extract.TopicID(raw)
		reason, body := "", ""
		if id == "" {
			reason = "missing_topic_id"
		} else {
			text, failure, _, e := b.fetch(ctx, "https://www.v2ex.com/api/topics/show.json?id="+id, false)
			if e != nil {
				return "", "", e
			}
			reason = failure
			if reason == "" {
				var payload any
				if json.Unmarshal([]byte(extract.UnwrapJSON(text)), &payload) != nil {
					reason = "invalid_v2ex_json"
				} else {
					var first map[string]any
					switch value := payload.(type) {
					case []any:
						if len(value) > 0 {
							first, _ = value[0].(map[string]any)
						}
					case map[string]any:
						first = value
					}
					if first == nil {
						reason = "empty_v2ex_topic"
					} else {
						body, _ = first["content"].(string)
						if body == "" {
							body, _ = first["content_rendered"].(string)
						}
						body = clean(body)
					}
				}
			}
		}
		if body != "" {
			return body, reason, nil
		}
		fallback, htmlError, _, e := b.fetch(ctx, raw, true)
		if reason == "" {
			reason = htmlError
		}
		return clean(fallback), reason, e
	}
	text, reason, _, e := b.fetch(ctx, raw, false)
	if text != "" && strings.HasPrefix(source, "dcinside") {
		return StripPoison(extract.DCInsideDescription(text)), reason, e
	}
	return clean(text), reason, e
}
