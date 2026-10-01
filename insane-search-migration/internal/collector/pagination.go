package collector

import (
	"context"
	"encoding/json"
	"io"
	"net/url"
	"strconv"
	"strings"
	"time"

	"golang.org/x/net/html"
	"insane-search-migration/internal/extract"
)

const listingPageDelay = 250 * time.Millisecond

// Only the configured public listings have pagination support. V2EX's public
// latest/hot API does not advertise a supported next page and stays single-page.
type listingRoute struct {
	host, path string
	dcinside   bool
}

func routeForListing(source Source) (listingRoute, bool) {
	var r listingRoute
	switch source.Name {
	case "linux.do-latest":
		r = listingRoute{host: "linux.do", path: "/latest"}
	case "linux.do-welfare":
		r = listingRoute{host: "linux.do", path: "/c/welfare/36"}
	case "nodeloc-latest":
		r = listingRoute{host: "www.nodeloc.com", path: "/latest"}
	case "nodeloc-deals":
		r = listingRoute{host: "www.nodeloc.com", path: "/c/business/information/10"}
	case "dcinside-ai-utilize":
		r = listingRoute{dcinside: true}
	default:
		return r, false
	}
	_, _, ok := r.parse(source.URL)
	return r, ok
}

// parse rejects URL credentials, ports, encoded paths, unrelated query keys,
// duplicate values and route changes. It never authorizes arbitrary hostnames.
func (r listingRoute) parse(raw string) (*url.URL, int, bool) {
	u, err := url.Parse(raw)
	if err != nil || u.User != nil || u.Scheme != "https" || u.Opaque != "" || u.Fragment != "" || u.RawPath != "" || strings.ContainsAny(raw, "\\\r\n\t ") {
		return nil, 0, false
	}
	q, err := url.ParseQuery(u.RawQuery)
	if err != nil || strings.ContainsAny(u.RawQuery, "%+;") {
		return nil, 0, false
	}
	page := 0
	if r.dcinside {
		page = 1
		switch u.Host {
		case "m.dcinside.com":
			if u.Path != "/board/ai_utilize" && u.Path != "/board/ai_utilize/" {
				return nil, 0, false
			}
		case "gall.dcinside.com":
			if (u.Path != "/mgallery/board/lists/" && u.Path != "/mgallery/board/lists") || len(q["id"]) != 1 || q.Get("id") != "ai_utilize" {
				return nil, 0, false
			}
		default:
			return nil, 0, false
		}
	} else if u.Host != r.host || (u.Path != r.path && u.Path != r.path+".json") {
		return nil, 0, false
	}
	for key, values := range q {
		if len(values) != 1 {
			return nil, 0, false
		}
		switch key {
		case "page":
			value, e := strconv.Atoi(values[0])
			if e != nil || value < page || value > 10000 || strconv.Itoa(value) != values[0] {
				return nil, 0, false
			}
			page = value
		case "no_definitions":
			if r.dcinside || values[0] != "true" {
				return nil, 0, false
			}
		case "id":
			if !r.dcinside || u.Host != "gall.dcinside.com" || values[0] != "ai_utilize" {
				return nil, 0, false
			}
		default:
			return nil, 0, false
		}
	}
	return u, page, true
}

func (r listingRoute) safeNext(base *url.URL, current int, hint string) string {
	if hint == "" || strings.HasPrefix(hint, "//") || strings.TrimSpace(hint) != hint {
		return ""
	}
	relative, err := url.Parse(hint)
	if err != nil {
		return ""
	}
	// Reject raw traversal before ResolveReference normalizes it away.
	for _, part := range strings.Split(relative.Path, "/") {
		if part == "." || part == ".." {
			return ""
		}
	}
	u, page, ok := r.parse(base.ResolveReference(relative).String())
	if !ok || u.Host != base.Host || page != current+1 || len(u.Query()["page"]) != 1 {
		return ""
	}
	// Rebuild from our allowlisted route and values, never return a server URL.
	q := url.Values{"page": {strconv.Itoa(page)}}
	if r.dcinside {
		path := "/board/ai_utilize"
		if u.Host == "gall.dcinside.com" {
			path = "/mgallery/board/lists/"
			q.Set("id", "ai_utilize")
		}
		return (&url.URL{Scheme: "https", Host: u.Host, Path: path, RawQuery: q.Encode()}).String()
	}
	if u.Query().Get("no_definitions") == "true" {
		q.Set("no_definitions", "true")
	}
	return (&url.URL{Scheme: "https", Host: r.host, Path: r.path + ".json", RawQuery: q.Encode()}).String()
}

func (r listingRoute) next(raw, text string) (string, string) {
	base, current, ok := r.parse(raw)
	if !ok {
		return "", "unsafe_listing_redirect"
	}
	if len(text) > extract.MaxInputBytes {
		return "", "listing_too_large"
	}
	if !r.dcinside {
		var payload struct {
			TopicList struct {
				MoreTopicsURL string `json:"more_topics_url"`
			} `json:"topic_list"`
		}
		if json.Unmarshal([]byte(extract.UnwrapJSON(text)), &payload) != nil || payload.TopicList.MoreTopicsURL == "" {
			return "", ""
		}
		next := r.safeNext(base, current, payload.TopicList.MoreTopicsURL)
		if next == "" {
			return "", "unsafe_or_repeated_next_page"
		}
		return next, ""
	}
	// DCInside exposes ordinary numbered pagination anchors. Only a link for
	// the immediately following page on the same observed listing is eligible.
	z := html.NewTokenizer(strings.NewReader(text))
	for {
		switch z.Next() {
		case html.ErrorToken:
			if z.Err() != io.EOF {
				return "", "invalid_pagination_html"
			}
			return "", ""
		case html.StartTagToken, html.SelfClosingTagToken:
			t := z.Token()
			if t.Data != "a" {
				continue
			}
			for _, a := range t.Attr {
				if a.Key == "href" {
					if next := r.safeNext(base, current, a.Val); next != "" {
						return next, ""
					}
				}
			}
		}
	}
}

func (b *NativeBackend) waitForListingPage(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if b.listingSleep != nil {
		if err := b.listingSleep(ctx, listingPageDelay); err != nil {
			return err
		}
		return ctx.Err()
	}
	timer := time.NewTimer(listingPageDelay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return ctx.Err()
	}
}
