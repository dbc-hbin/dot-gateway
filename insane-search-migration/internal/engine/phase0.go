package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"time"
)

type phaseRoute struct {
	Platform, Name, URL, FinalURL string
	Accept                        func(HTTPResponse) bool
}

func hostIs(host, parent string) bool { return host == parent || strings.HasSuffix(host, "."+parent) }
func phaseRoutes(raw string) []phaseRoute {
	u, e := url.Parse(raw)
	if e != nil {
		return nil
	}
	host := strings.TrimPrefix(strings.ToLower(u.Hostname()), "www.")
	statusOK := func(r HTTPResponse) bool { return r.Status == 200 }
	jsonKey := func(key string) func(HTTPResponse) bool {
		return func(r HTTPResponse) bool {
			if !statusOK(r) {
				return false
			}
			var d map[string]any
			if json.Unmarshal(r.Body, &d) != nil {
				return false
			}
			switch v := d[key].(type) {
			case string:
				return v != ""
			case map[string]any:
				return len(v) > 0
			}
			return false
		}
	}
	if hostIs(host, "reddit.com") || host == "redd.it" {
		base := strings.TrimRight(strings.SplitN(raw, "?", 2)[0], "/")
		slash := "/"
		if strings.Contains(base, "/comments/") {
			slash = ""
		}
		return []phaseRoute{{"reddit", "rss", base + slash + ".rss", base + slash + ".rss", func(r HTTPResponse) bool {
			return statusOK(r) && (strings.Contains(string(r.Body), "<rss") || strings.Contains(string(r.Body), "<feed"))
		}}, {"reddit", "json", base + slash + ".json", base + slash + ".json", func(r HTTPResponse) bool {
			t := strings.TrimSpace(string(r.Body))
			return statusOK(r) && (strings.HasPrefix(t, "{") || strings.HasPrefix(t, "["))
		}}}
	}
	if hostIs(host, "x.com") || hostIs(host, "twitter.com") {
		m := regexp.MustCompile(`/status(?:es)?/(\d+)`).FindStringSubmatch(u.Path)
		if len(m) > 1 {
			id := m[1]
			return []phaseRoute{{"x", "tweet-result", "https://cdn.syndication.twimg.com/tweet-result?id=" + id + "&token=a", raw, jsonKey("text")}, {"x", "oembed", "https://publish.twitter.com/oembed?url=https://twitter.com/i/status/" + id + "&omit_script=1", "", jsonKey("html")}}
		}
		handle := strings.Split(strings.Trim(u.Path, "/"), "/")[0]
		reserved := map[string]bool{"i": true, "search": true, "home": true, "explore": true, "messages": true, "notifications": true, "settings": true, "hashtag": true}
		if handle != "" && !reserved[strings.ToLower(handle)] {
			route := "https://syndication.twitter.com/srv/timeline-profile/screen-name/" + url.PathEscape(handle)
			accept := func(r HTTPResponse) bool { return statusOK(r) && strings.Contains(string(r.Body), "__NEXT_DATA__") }
			return []phaseRoute{{"x", "syndication-timeline#1", route, route, accept}, {"x", "syndication-timeline#2", route, route, accept}}
		}
	}
	if hostIs(host, "youtube.com") || host == "youtu.be" {
		return []phaseRoute{{"youtube", "watch-player-json", raw, raw, func(r HTTPResponse) bool {
			_, wall, ok := youtubePlayerMetadata(r.Body)
			return r.Status == 200 && (ok || wall != "")
		}}, {"youtube", "oembed_native", "https://www.youtube.com/oembed?url=" + url.QueryEscape(raw) + "&format=json", raw, jsonKey("title")}}
	}
	if hostIs(host, "threads.com") || hostIs(host, "threads.net") {
		if regexp.MustCompile(`/post/[A-Za-z0-9_-]+`).MatchString(u.Path) {
			return []phaseRoute{{"threads", "inline-json", raw, raw, func(r HTTPResponse) bool { return statusOK(r) && strings.Contains(string(r.Body), `"video_versions"`) }}}
		}
	}
	return nil
}
func (f *Fetcher) phase0(ctx context.Context, request Request, result *Result) (bool, error) {
	for _, route := range phaseRoutes(request.URL) {
		if e := ctx.Err(); e != nil {
			return false, e
		}
		started := time.Now()
		resp, e := f.HTTP.Do(ctx, HTTPRequest{URL: route.URL, Profile: "safari", Timeout: request.Timeout})
		att := Attempt{Phase: "phase0", Executor: "go_phase0:" + route.Name, URL: route.URL, URLTransform: "-", Referer: "", Verdict: "blocked", Reasons: []string{}, ElapsedS: time.Since(started).Seconds()}
		if e != nil {
			s := e.Error()
			att.Error = &s
			att.Verdict = "unknown"
			result.Trace = append(result.Trace, att)
			continue
		}
		att.Status = resp.Status
		att.BodySize = len(resp.Body)
		if route.Accept(resp) {
			if route.Platform == "youtube" && route.Name == "watch-player-json" {
				content, wall, ok := youtubePlayerMetadata(resp.Body)
				if wall != "" {
					att.Verdict = wall
					att.Reasons = append(att.Reasons, "terminal_platform_playability")
					result.Trace = append(result.Trace, att)
					result.Verdict = wall
					result.StopReason = wall
					result.Summary = "Public watch page requires authentication or is unavailable"
					return true, nil
				}
				if ok {
					resp.Body = content
				}
			}
			if route.Platform == "threads" {
				if content, ok := parseThreadsMedia(request.URL, resp.Body); ok {
					resp.Body = content
				} else {
					att.Reasons = append(att.Reasons, "no_matching_public_media")
					result.Trace = append(result.Trace, att)
					continue
				}
			}
			att.Verdict = "strong_ok"
			att.Reasons = append(att.Reasons, "official_public_route")
			result.Trace = append(result.Trace, att)
			result.OK = true
			result.Content = string(resp.Body)
			result.FinalURL = route.FinalURL
			if result.FinalURL == "" {
				result.FinalURL = resp.URL
			}
			result.Verdict = "strong_ok"
			profile := "phase0:" + route.Platform
			result.ProfileUsed = &profile
			result.Summary = fmt.Sprintf("Native official public route %s:%s", route.Platform, route.Name)
			result.StopReason = "success"
			result.ExtractionSource = "phase0"
			return true, nil
		}
		att.Reasons = append(att.Reasons, fmt.Sprintf("status=%d or expected structure absent", resp.Status))
		result.Trace = append(result.Trace, att)
	}
	return false, nil
}
