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
	Source       string `json:"source"`
	Format       string `json:"format,omitempty"`
	Recognized   bool   `json:"recognized"`
	Parsed       int    `json:"parsed"`
	Scanned      int    `json:"scanned"`
	FetchVerdict string `json:"fetch_verdict,omitempty"`
	Error        string `json:"error,omitempty"`
}
type NativeBackend struct {
	Fetcher          *engine.Fetcher
	OperationTimeout time.Duration
	mu               sync.Mutex
	diagnostics      []SourceDiagnostic
}

func NewNativeBackend(fetcher *engine.Fetcher, timeout time.Duration) *NativeBackend {
	return &NativeBackend{Fetcher: fetcher, OperationTimeout: timeout, diagnostics: []SourceDiagnostic{}}
}
func (b *NativeBackend) Diagnostics() []SourceDiagnostic {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]SourceDiagnostic{}, b.diagnostics...)
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
	text, failure, verdict, finalURL, e := b.fetchWithFinalURL(ctx, source.URL, false, true)
	diag := SourceDiagnostic{Source: source.Name, FetchVerdict: verdict, Error: failure}
	defer func() { b.mu.Lock(); b.diagnostics = append(b.diagnostics, diag); b.mu.Unlock() }()
	if e != nil {
		return nil, "", e
	}
	if failure != "" {
		return nil, failure, nil
	}
	if finalURL == "" {
		finalURL = source.URL
	}
	parsed, d := extract.ParseListing(source.Name, finalURL, text)
	diag.Format, diag.Recognized, diag.Parsed, diag.Scanned, diag.Error = d.Format, d.Recognized, d.Parsed, min(d.Parsed, MaxScanPerSource), d.Error
	if !d.Recognized {
		if diag.Error == "" {
			diag.Error = "unrecognized_listing"
		}
		return []Row{}, diag.Error, nil
	}
	rows := make([]Row, 0, len(parsed))
	for _, r := range parsed {
		rows = append(rows, Row{Source: r.Source, Title: r.Title, URL: r.URL, ListingExcerpt: StripPoison(r.ListingExcerpt)})
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
