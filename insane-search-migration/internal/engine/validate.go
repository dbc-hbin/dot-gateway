package engine

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/andybalholm/cascadia"
	"golang.org/x/net/html"
)

var hardMarkers = []string{"sec-if-cpt-container", "Powered and protected by Akamai", "Just a moment...", "cf-chl-bypass", "window._cf_chl_opt", "orchestrate/chl_page", "Attention Required! | Cloudflare", "<title>Bot Challenge</title>", "The requested URL was rejected", "Request unsuccessful. Incapsula", "Please enable JS and disable any ad blocker"}
var softMarkers = []string{"access denied", "checking your browser", "datadome", "captcha"}

func markerHits(body string, markers []string) []string {
	hits := []string{}
	for _, marker := range markers {
		needle := strings.ToLower(marker)
		for offset := 0; offset < len(body); {
			i := strings.Index(body[offset:], needle)
			if i < 0 {
				break
			}
			i += offset
			if i == 0 || !((body[i-1] >= 'a' && body[i-1] <= 'z') || (body[i-1] >= '0' && body[i-1] <= '9') || body[i-1] == '_') {
				hits = append(hits, marker)
				break
			}
			offset = i + 1
		}
	}
	return hits
}

var stripTags = regexp.MustCompile(`(?s)<[^>]+>`)
var scripts = regexp.MustCompile(`(?is)<script\b[^>]*>.*?</script>|<style\b[^>]*>.*?</style>`)
var passwordInput = regexp.MustCompile(`(?is)<input[^>]+type\s*=\s*["']?password`)

func VisibleText(text string) string {
	return strings.Join(strings.Fields(stripTags.ReplaceAllString(scripts.ReplaceAllString(text, " "), " ")), " ")
}
func WallVerdict(text string, status int) string {
	low := strings.ToLower(text)
	if status == 401 || status == 407 {
		return "auth_required"
	}
	if status == 402 {
		return "paywall"
	}
	if passwordInput.MatchString(low) && strings.Contains(low, "<form") {
		return "login_required"
	}
	visible := strings.ToLower(VisibleText(text))
	if len(visible) < 4000 {
		for _, m := range []string{"subscribe to read", "subscription required", "members-only content", "members only content"} {
			if strings.Contains(visible, m) {
				return "paywall"
			}
		}
		for _, m := range []string{"sign in to continue", "log in to view", "login required to view", "登录后可见"} {
			if strings.Contains(visible, m) {
				return "login_required"
			}
		}
	}
	return ""
}
func Validate(resp HTTPResponse, selectors []string, knownBad []int) (r Validation) {
	defer func() {
		if resp.Status >= 400 && r.OK() {
			r.Verdict = "blocked"
			r.Reasons = append(r.Reasons, "non_success_http_status")
		}
	}()
	text := string(resp.Body)
	size := len(resp.Body)
	r = Validation{Verdict: "unknown", Reasons: []string{}, MatchedSelectors: []string{}, BodySize: size, Status: resp.Status}
	switch {
	case resp.Status == 429:
		r.Verdict = "rate_limited"
		r.Reasons = append(r.Reasons, "status=429")
		return r
	case resp.Status == 401 || resp.Status == 407:
		r.Verdict = "auth_required"
		r.Reasons = append(r.Reasons, fmt.Sprint("status=", resp.Status))
		return r
	case resp.Status == 404 || resp.Status == 410:
		r.Verdict = "not_found"
		r.Reasons = append(r.Reasons, fmt.Sprint("status=", resp.Status))
		return r
	case resp.Status >= 500 && resp.Status <= 599:
		r.Verdict = "blocked"
		r.Reasons = append(r.Reasons, fmt.Sprint("status=", resp.Status))
		return r
	case resp.Status == 0:
		r.Reasons = append(r.Reasons, "status=0_unobserved")
		if strings.TrimSpace(text) == "" {
			return r
		}
	}
	if wall := WallVerdict(text, resp.Status); wall != "" {
		r.Verdict = wall
		r.Reasons = append(r.Reasons, "terminal_public_content_wall")
		return r
	}
	low := strings.ToLower(text)
	hard := markerHits(low, hardMarkers)
	if len(hard) > 0 {
		r.Verdict = "challenge"
		for _, m := range hard[:min(len(hard), 3)] {
			r.Reasons = append(r.Reasons, "hard:"+m)
		}
		return r
	}
	for _, bad := range knownBad {
		if abs(size-bad) <= 20 {
			r.Verdict = "challenge"
			r.Reasons = append(r.Reasons, fmt.Sprintf("size_fp:%d~%d", size, bad))
			return r
		}
	}
	ctype := strings.ToLower(resp.Headers.Get("Content-Type"))
	trim := strings.TrimSpace(text)
	if strings.Contains(ctype, "json") || strings.HasPrefix(trim, "{") || strings.HasPrefix(trim, "[") {
		var obj any
		if json.Unmarshal(resp.Body, &obj) == nil {
			empty := obj == nil
			switch v := obj.(type) {
			case map[string]any:
				empty = len(v) == 0
			case []any:
				empty = len(v) == 0
			case string:
				empty = v == ""
			}
			if resp.Status >= 400 {
				r.Verdict = "blocked"
				r.Reasons = append(r.Reasons, "non_success_json_status")
				return r
			}
			if empty {
				r.Verdict = "suspect_ok"
				r.Reasons = append(r.Reasons, "json_empty")
			} else {
				r.Verdict = "weak_ok"
				r.Reasons = append(r.Reasons, "json_ok")
			}
			return r
		}
	}
	abckBad := false
	for _, c := range resp.Cookies {
		if c.Name == "_abck" && strings.Contains(c.Value, "~-1~") {
			abckBad = true
		}
	}
	if len(selectors) > 0 {
		doc, e := html.Parse(bytes.NewReader(resp.Body))
		if e != nil {
			r.Reasons = append(r.Reasons, "html_parse_error")
			return r
		}
		for _, sel := range selectors {
			compiled, e := cascadia.Compile(sel)
			if e == nil && cascadia.Query(doc, compiled) != nil {
				r.MatchedSelectors = append(r.MatchedSelectors, sel)
			}
		}
		if len(r.MatchedSelectors) > 0 {
			if abckBad {
				r.Verdict = "suspect_ok"
				r.Reasons = append(r.Reasons, "abck_unresolved")
			} else {
				r.Verdict = "strong_ok"
			}
			return r
		}
		r.Verdict = "challenge"
		r.Reasons = append(r.Reasons, "no_success_selector")
		return r
	}
	soft := markerHits(low, softMarkers)
	if len(soft) > 0 {
		if len(soft) >= 2 || size <= 20000 {
			r.Verdict = "challenge"
			for _, m := range soft[:min(len(soft), 3)] {
				r.Reasons = append(r.Reasons, "soft:"+m)
			}
			return r
		}
		r.Reasons = append(r.Reasons, "soft_mention:"+soft[0])
	}
	if size < 3000 {
		if (strings.Contains(low, "</html>") || strings.Contains(low, "</body>")) && utf8.RuneCountInString(VisibleText(text)) >= 64 {
			r.Verdict = "weak_ok"
			r.Reasons = append(r.Reasons, fmt.Sprintf("small_but_complete:%d", size))
		} else {
			r.Verdict = "challenge"
			r.Reasons = append(r.Reasons, fmt.Sprintf("tiny_body:%d", size))
		}
		return r
	}
	if abckBad {
		r.Verdict = "suspect_ok"
		r.Reasons = append(r.Reasons, "abck_unresolved")
		return r
	}
	if resp.Status >= 400 {
		r.Verdict = "blocked"
		r.Reasons = append(r.Reasons, "non_success_http_status")
		return r
	}
	r.Verdict = "weak_ok"
	return r
}
func abs(n int) int {
	if n < 0 {
		return -n
	}
	return n
}
