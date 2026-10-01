// Package search provides bounded native-Go public search discovery.
// It preserves the source provider's result schema, not DDGS's provider ranking.
package search

import (
	"context"
	"encoding/base64"
	"encoding/xml"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"golang.org/x/net/html"
	"insane-search-migration/internal/extract"
)

type FetchFunc func(context.Context, string) (body string, status int, err error)
type Hit struct {
	Title       string "json:\"title\""
	URL         string "json:\"url\""
	Description string "json:\"description\""
	Position    int    "json:\"position\""
}
type Data struct {
	Web []Hit "json:\"web\""
}
type Diagnostic struct {
	Provider string "json:\"provider\""
	Status   int    "json:\"status\""
	Parsed   int    "json:\"parsed\""
	Error    string "json:\"error,omitempty\""
}
type Result struct {
	Success     bool         "json:\"success\""
	Data        *Data        "json:\"data,omitempty\""
	Error       string       "json:\"error,omitempty\""
	Diagnostics []Diagnostic "json:\"diagnostics,omitempty\""
}

func each(root *html.Node, fn func(*html.Node) bool) {
	stack := []*html.Node{root}
	for len(stack) > 0 {
		n := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if !fn(n) {
			continue
		}
		for c := n.LastChild; c != nil; c = c.PrevSibling {
			stack = append(stack, c)
		}
	}
}
func descendant(root *html.Node, fn func(*html.Node) bool) *html.Node {
	var found *html.Node
	each(root, func(n *html.Node) bool {
		if found != nil {
			return false
		}
		if fn(n) {
			found = n
			return false
		}
		return true
	})
	return found
}
func safeResultURL(raw string) string {
	if strings.HasPrefix(raw, "/l/?") {
		raw = "https://duckduckgo.com" + raw
	}
	if strings.HasPrefix(raw, "/ck/a?") {
		raw = "https://www.bing.com" + raw
	}
	if strings.HasPrefix(raw, "//") {
		raw = "https:" + raw
	}
	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	host := strings.ToLower(u.Hostname())
	if host == "duckduckgo.com" || strings.HasSuffix(host, ".duckduckgo.com") {
		if u.Path == "/y.js" {
			return ""
		}
		if target := u.Query().Get("uddg"); target != "" {
			raw = target
		}
	}
	if host == "bing.com" || strings.HasSuffix(host, ".bing.com") {
		if u.Path == "/aclick" {
			return ""
		}
		if u.Path == "/ck/a" {
			encoded := u.Query().Get("u")
			if strings.HasPrefix(encoded, "a1") {
				encoded = strings.TrimRight(encoded[2:], "=")
				if decoded, e := base64.RawURLEncoding.DecodeString(encoded); e == nil {
					raw = string(decoded)
				}
			}
		}
	}
	u, err = url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil {
		return ""
	}
	return u.String()
}
func normalize(hits []Hit, limit int) []Hit {
	out := []Hit{}
	seen := map[string]bool{}
	for _, hit := range hits {
		hit.URL = safeResultURL(hit.URL)
		if hit.URL == "" || seen[hit.URL] {
			continue
		}
		seen[hit.URL] = true
		hit.Title = extract.Cut(extract.CleanText(hit.Title), 4000)
		hit.Description = extract.Cut(extract.CleanText(hit.Description), 8000)
		hit.Position = len(out) + 1
		out = append(out, hit)
		if len(out) >= limit {
			break
		}
	}
	return out
}
func parseDuckDuckGo(body string) ([]Hit, bool, error) {
	doc, err := html.Parse(strings.NewReader(body))
	if err != nil {
		return nil, false, err
	}
	hits := []Hit{}
	recognized := false
	each(doc, func(n *html.Node) bool {
		if n.Type != html.ElementNode {
			return true
		}
		if extract.HasClass(n, "no-results") || extract.HasClass(n, "no-results__message") {
			recognized = true
		}
		if !extract.HasClass(n, "result") && !extract.HasClass(n, "web-result") && !extract.HasClass(n, "result__body") {
			return true
		}
		title := descendant(n, func(c *html.Node) bool {
			return c.Type == html.ElementNode && c.Data == "a" &&
				(extract.HasClass(c, "result__a") || c.Parent != nil && c.Parent.Data == "h2")
		})
		if title == nil {
			return true
		}
		snippet := descendant(n, func(c *html.Node) bool { return c.Type == html.ElementNode && extract.HasClass(c, "result__snippet") })
		description := ""
		if snippet != nil {
			description = extract.Text(snippet)
		}
		hits = append(hits, Hit{Title: extract.Text(title), URL: extract.Attr(title, "href"), Description: description})
		recognized = true
		return false // Do not emit the nested result__body twice.
	})
	if len(hits) == 0 {
		each(doc, func(n *html.Node) bool {
			if n.Type == html.ElementNode && n.Data == "a" && extract.HasClass(n, "result-link") {
				hits = append(hits, Hit{Title: extract.Text(n), URL: extract.Attr(n, "href")})
				recognized = true
			}
			return true
		})
	}
	return hits, recognized, nil
}
func parseBing(body string) ([]Hit, bool, error) {
	var rss struct {
		XMLName xml.Name "xml:\"rss\""
		Channel struct {
			Items []struct {
				Title       string "xml:\"title\""
				Link        string "xml:\"link\""
				Description string "xml:\"description\""
			} "xml:\"item\""
		} "xml:\"channel\""
	}
	if err := xml.Unmarshal([]byte(body), &rss); err == nil && rss.XMLName.Local == "rss" {
		hits := []Hit{}
		for _, item := range rss.Channel.Items {
			hits = append(hits, Hit{Title: item.Title, URL: item.Link, Description: item.Description})
		}
		return hits, true, nil
	}
	doc, err := html.Parse(strings.NewReader(body))
	if err != nil {
		return nil, false, err
	}
	hits := []Hit{}
	recognized := false
	each(doc, func(n *html.Node) bool {
		if n.Type != html.ElementNode {
			return true
		}
		if extract.HasClass(n, "b_no") {
			recognized = true
		}
		if n.Data != "li" || !extract.HasClass(n, "b_algo") {
			return true
		}
		title := descendant(n, func(c *html.Node) bool {
			return c.Type == html.ElementNode && c.Data == "a" && c.Parent != nil && c.Parent.Data == "h2"
		})
		if title == nil {
			return false
		}
		description := descendant(n, func(c *html.Node) bool { return c.Type == html.ElementNode && c.Data == "p" })
		snippet := ""
		if description != nil {
			snippet = extract.Text(description)
		}
		hits = append(hits, Hit{Title: extract.Text(title), URL: extract.Attr(title, "href"), Description: snippet})
		recognized = true
		return false
	})
	return hits, recognized, nil
}

// Search returns ordinary provider failures in Result.Success/Error as before;
// malformed input and cancelled/deadlined callers are explicit Go errors.
func Search(ctx context.Context, query string, limit int, fetch FetchFunc) (Result, error) {
	result := Result{Diagnostics: []Diagnostic{}}
	if strings.TrimSpace(query) == "" || len([]rune(query)) > 8192 {
		return result, errors.New("query must contain 1..8192 characters")
	}
	if fetch == nil {
		return result, errors.New("guarded search fetcher required")
	}
	limit = max(1, min(limit, 20))
	providers := []struct {
		name, endpoint string
		parse          func(string) ([]Hit, bool, error)
	}{
		{"duckduckgo_html", "https://html.duckduckgo.com/html/?q=" + url.QueryEscape(query), parseDuckDuckGo},
		{"bing_rss", "https://www.bing.com/search?format=rss&q=" + url.QueryEscape(query), parseBing},
	}
	hits := []Hit{}
	recognizedEmpty := false
	for _, provider := range providers {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		requestCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		body, status, err := fetch(requestCtx, provider.endpoint)
		cancel()
		diag := Diagnostic{Provider: provider.name, Status: status}
		switch {
		case err != nil:
			diag.Error = "provider_fetch_failed"
		case status < 200 || status >= 300:
			diag.Error = fmt.Sprintf("http_status_%d", status)
		case len(body) > extract.MaxInputBytes:
			diag.Error = "provider_response_too_large"
		default:
			parsed, recognized, parseErr := provider.parse(body)
			if parseErr != nil {
				diag.Error = "provider_parse_failed"
			} else if !recognized {
				diag.Error = "unrecognized_or_blocked_search_response"
			} else {
				normalized := normalize(parsed, limit)
				diag.Parsed = len(normalized)
				if len(parsed) > 0 && len(normalized) == 0 {
					diag.Error = "provider_results_invalid"
				} else {
					hits = append(hits, normalized...)
					recognizedEmpty = recognizedEmpty || len(parsed) == 0
				}
			}
		}
		result.Diagnostics = append(result.Diagnostics, diag)
		hits = normalize(hits, limit)
		if len(hits) >= limit {
			break
		}
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if len(hits) > 0 || recognizedEmpty {
		result.Success = true
		result.Data = &Data{Web: hits}
		return result, nil
	}
	result.Error = "Search discovery failed: no provider returned a recognized public result page"
	return result, nil
}
