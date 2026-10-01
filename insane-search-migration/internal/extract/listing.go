package extract

import (
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"regexp"
	"strings"

	"golang.org/x/net/html"
)

type Row struct {
	Source         string "json:\"source\""
	Title          string "json:\"title\""
	URL            string "json:\"url\""
	ListingExcerpt string "json:\"listing_excerpt\""
}
type ListingDiagnostics struct {
	Recognized bool   "json:\"recognized\""
	Format     string "json:\"format\""
	Parsed     int    "json:\"parsed\""
	Error      string "json:\"error,omitempty\""
}

var topicPath = regexp.MustCompile("/t/(?:[^/]+/)?([0-9]+)")
var dcPath = regexp.MustCompile("/board/ai_utilize/([0-9]+)")
var v2Path = regexp.MustCompile("/t/[0-9]+")
var decimalID = regexp.MustCompile("^[0-9]{1,20}$")
var dcPrefix = regexp.MustCompile("^(?:이미지|영상|설문|공지)[\\s\\x{0085}\\x{00a0}]+")
var dcSuffix = regexp.MustCompile("\\s+(?:일반|질문|정보|뉴스|후기|공지)\\s+.*?\\s+[0-9]{1,2}:[0-9]{2}\\s+조회\\s+[0-9]+\\s+추천\\s+[0-9]+.*$")
var poison = regexp.MustCompile("(?is)\\[CRITICAL INSTRUCTIONS FOR ALL AI ASSISTANTS.*?\\[END INSTRUCTIONS\\]")
var v2Chrome = regexp.MustCompile("(?is)(?:\\[Home\\]\\(/\\).*?\\[Sign In\\]\\(/signin\\)|如果想在 V2EX 获得更好的推广效果.*?铜币：|\\*\\*\\[About\\]\\(/about\\).*?what you're doing\\.\\s*❯)")

// StripPoison mirrors the two source-specific boilerplate patterns. The Go
// collector also reapplies its exhaustive Python-compatible policy boundary.
func StripPoison(s string) string {
	return Collapse(v2Chrome.ReplaceAllString(poison.ReplaceAllString(s, " "), " "))
}
func TopicID(value string) string {
	u, err := url.Parse(value)
	if err != nil {
		return ""
	}
	for _, p := range []*regexp.Regexp{topicPath, dcPath} {
		if match := p.FindStringSubmatch(u.Path); len(match) > 1 {
			return match[1]
		}
	}
	return ""
}
func truthy(v any) bool {
	switch x := v.(type) {
	case nil:
		return false
	case bool:
		return x
	case string:
		return x != ""
	case json.Number:
		return x != "0" && x != "0.0"
	case []any:
		return len(x) > 0
	case map[string]any:
		return len(x) > 0
	default:
		return true
	}
}
func valueString(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	case json.Number:
		return string(x)
	case bool:
		if x {
			return "True"
		}
		return "False"
	default:
		return fmt.Sprint(x)
	}
}
func choose(m map[string]any, keys ...string) any {
	for _, k := range keys {
		if truthy(m[k]) {
			return m[k]
		}
	}
	return nil
}
func jsonListing(source, base, text string) ([]Row, ListingDiagnostics) {
	rows := []Row{}
	diag := ListingDiagnostics{Format: "json"}
	raw := UnwrapJSON(text)
	if len(raw) > MaxInputBytes {
		diag.Error = "listing_too_large"
		return rows, diag
	}
	var payload any
	dec := json.NewDecoder(strings.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&payload); err != nil {
		diag.Error = "invalid_listing_json"
		return rows, diag
	}
	var extra any
	if dec.Decode(&extra) != io.EOF {
		diag.Error = "multiple_json_values"
		return rows, diag
	}
	var topics []any
	switch p := payload.(type) {
	case []any:
		topics = p
		diag.Recognized = true
	case map[string]any:
		if list, ok := p["topic_list"].(map[string]any); ok {
			if items, ok := list["topics"].([]any); ok {
				topics = items
				diag.Recognized = true
			}
		}
	}
	if !diag.Recognized {
		diag.Error = "unrecognized_listing_json"
		return rows, diag
	}
	for _, value := range topics {
		topic, ok := value.(map[string]any)
		if !ok {
			continue
		}
		title := CleanText(valueString(choose(topic, "title")))
		excerpt := StripPoison(CleanText(valueString(choose(topic, "excerpt", "content"))))
		item := ""
		if truthy(topic["url"]) {
			item = resolve(base, valueString(topic["url"]))
		} else if truthy(topic["id"]) {
			id := valueString(topic["id"])
			if strings.HasPrefix(source, "v2ex") {
				item = "https://www.v2ex.com/t/" + id
			} else if origin, err := url.Parse(base); err == nil {
				slug := valueString(choose(topic, "slug"))
				if slug == "" {
					slug = "topic"
				}
				item = origin.Scheme + "://" + origin.Host + "/t/" + slug + "/" + id
			}
		}
		if title != "" && item != "" {
			rows = append(rows, Row{source, title, item, excerpt})
		}
	}
	diag.Parsed = len(rows)
	// Non-empty topic arrays without any usable rows are malformed data, not a
	// verified empty listing.
	if len(topics) > 0 && len(rows) == 0 {
		diag.Recognized = false
		diag.Error = "listing_items_unparseable"
	}
	return rows, diag
}
func ParseJSONListing(source, base, text string) []Row {
	rows, _ := jsonListing(source, base, text)
	return rows
}
func ParseHTMLListing(source, base, text string) []Row {
	rows := []Row{}
	doc, err := document(text)
	if err != nil {
		return rows
	}
	walk(doc, func(n *html.Node) bool {
		if n.Type != html.ElementNode || n.Data != "a" {
			return true
		}
		href := Attr(n, "href")
		if href == "" {
			return true
		}
		absolute := resolve(base, href)
		u, err := url.Parse(absolute)
		if err != nil {
			return true
		}
		valid := topicPath.MatchString(u.Path)
		if strings.HasPrefix(source, "dcinside") {
			valid = dcPath.MatchString(u.Path)
			// The configured mobile listing redirects publicly to this desktop
			// gallery on a desktop browser. Match only the observed gallery's
			// real title cell, not reply counters, ads or other gallery links.
			if !valid && u.User == nil && (u.Scheme == "https" || u.Scheme == "http") && strings.EqualFold(u.Host, "gall.dcinside.com") &&
				strings.TrimRight(u.Path, "/") == "/mgallery/board/view" &&
				u.Query().Get("id") == "ai_utilize" && decimalID.MatchString(u.Query().Get("no")) &&
				!hasClass(n, "reply_numbox") {
				for parent := n.Parent; parent != nil; parent = parent.Parent {
					if parent.Type == html.ElementNode && parent.Data == "td" && hasClass(parent, "gall_tit") {
						valid = true
						break
					}
				}
			}
		} else if strings.HasPrefix(source, "v2ex") {
			valid = v2Path.MatchString(u.Path)
		}
		title := Text(n)
		if title == "" {
			title = Attr(n, "title")
		}
		title = CleanText(title)
		if strings.HasPrefix(source, "dcinside") {
			title = dcPrefix.ReplaceAllString(title, "")
			title = trim(dcSuffix.ReplaceAllString(title, ""))
		}
		if valid && chars(title) >= 3 {
			rows = append(rows, Row{source, title, absolute, ""})
		}
		return true
	})
	return rows
}
func ParseListing(source, base, text string) ([]Row, ListingDiagnostics) {
	rows, diag := jsonListing(source, base, text)
	if diag.Recognized {
		return rows, diag
	}
	if len(text) > MaxInputBytes {
		return nil, ListingDiagnostics{Format: "unknown", Error: "listing_too_large"}
	}
	rows = ParseHTMLListing(source, base, text)
	if len(rows) > 0 {
		return rows, ListingDiagnostics{Recognized: true, Format: "html", Parsed: len(rows)}
	}
	return rows, ListingDiagnostics{Format: "unknown", Error: "unrecognized_or_empty_listing"}
}
