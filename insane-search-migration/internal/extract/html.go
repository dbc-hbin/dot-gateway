// Package extract implements bounded, native-Go public-content extraction.
// Collector parsing semantics derive from the retained MIT-licensed Mac source.
package extract

import (
	"errors"
	stdhtml "html"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"golang.org/x/net/html"
)

const MaxInputBytes = 25 * 1024 * 1024
const ScanLimit = 2_000_000
const MaxText = 1_000_000

var htmlLooking = regexp.MustCompile("<[a-zA-Z!/?]")

func pySpace(r rune) bool      { return unicode.IsSpace(r) || r >= 0x1c && r <= 0x1f }
func Collapse(s string) string { return strings.Join(strings.FieldsFunc(s, pySpace), " ") }
func trim(s string) string     { return strings.TrimFunc(s, pySpace) }
func chars(s string) int       { return utf8.RuneCountInString(s) }
func Cut(s string, n int) string {
	if n <= 0 {
		return ""
	}
	count := 0
	for index := range s {
		if count == n {
			return s[:index]
		}
		count++
	}
	return s
}

func document(s string) (*html.Node, error) {
	if len(s) > MaxInputBytes {
		return nil, errors.New("HTML input exceeds 25 MiB")
	}
	return html.Parse(strings.NewReader(s))
}

func walk(root *html.Node, visit func(*html.Node) bool) {
	stack := []*html.Node{root}
	for len(stack) > 0 {
		n := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if !visit(n) {
			continue
		}
		for c := n.LastChild; c != nil; c = c.PrevSibling {
			stack = append(stack, c)
		}
	}
}

func Attr(n *html.Node, name string) string {
	for _, a := range n.Attr {
		if a.Key == name {
			return a.Val
		}
	}
	return ""
}
func hasClass(n *html.Node, name string) bool {
	for _, c := range strings.Fields(Attr(n, "class")) {
		if c == name {
			return true
		}
	}
	return false
}
func HasClass(n *html.Node, name string) bool { return hasClass(n, name) }
func Text(n *html.Node) string {
	parts := []string{}
	walk(n, func(x *html.Node) bool {
		if x.Type == html.ElementNode {
			switch x.Data {
			case "script", "style", "template":
				return false
			}
		}
		if x.Type == html.TextNode {
			if value := trim(x.Data); value != "" {
				parts = append(parts, value)
			}
		}
		return true
	})
	return strings.Join(parts, " ")
}
func CleanText(value string) string {
	raw := stdhtml.UnescapeString(value)
	if htmlLooking.MatchString(raw) {
		doc, err := document(raw)
		if err != nil {
			return ""
		}
		raw = Text(doc)
	}
	return Collapse(raw)
}

// VisibleText excludes scripts/hidden boilerplate containers. It is a bounded
// DOM-derived length yardstick; malformed HTML repair can differ from Python.
func VisibleText(value string) string {
	doc, err := document(Cut(value, ScanLimit))
	if err != nil {
		return ""
	}
	parts := []string{}
	walk(doc, func(n *html.Node) bool {
		if n.Type == html.ElementNode {
			switch n.Data {
			case "script", "style", "noscript", "svg", "template":
				return false
			}
		}
		if n.Type == html.TextNode {
			parts = append(parts, n.Data)
		}
		return true
	})
	return Collapse(strings.Join(parts, " "))
}

// UnwrapJSON handles browser JSON viewers, which serialize an API response as
// HTML with its untouched JSON text inside a pre element.
func UnwrapJSON(value string) string {
	s := trim(strings.TrimPrefix(value, "\ufeff"))
	if strings.HasPrefix(s, "{") || strings.HasPrefix(s, "[") {
		return s
	}
	if !htmlLooking.MatchString(s) {
		return s
	}
	doc, err := document(s)
	if err != nil {
		return s
	}
	var found string
	walk(doc, func(n *html.Node) bool {
		if found != "" {
			return false
		}
		if n.Type == html.ElementNode && n.Data == "pre" {
			var b strings.Builder
			walk(n, func(c *html.Node) bool {
				if c.Type == html.TextNode {
					b.WriteString(c.Data)
				}
				return true
			})
			text := trim(b.String())
			if strings.HasPrefix(text, "[") || strings.HasPrefix(text, "{") {
				found = text
			}
			return false
		}
		return true
	})
	if found != "" {
		return found
	}
	return s
}

func resolve(base, reference string) string {
	// urllib.parse.urljoin preserves raw Unicode/space spelling; net/url.String
	// otherwise percent-encodes it and changes collector state keys. Protect
	// those runes during reference resolution without decoding existing escapes.
	if len(base) > 8192 || len(reference) > 8192 {
		return ""
	}
	prefix := "INSANEURLTOKEN"
	for strings.Contains(base, prefix) || strings.Contains(reference, prefix) {
		prefix += "X"
	}
	replacements := []string{}
	protect := func(s string) string {
		var b strings.Builder
		for _, r := range s {
			if r > 127 || r == ' ' {
				token := prefix + strconv.Itoa(len(replacements)/2) + "Z"
				replacements = append(replacements, token, string(r))
				b.WriteString(token)
			} else {
				b.WriteRune(r)
			}
		}
		return b.String()
	}
	base, reference = protect(base), protect(reference)
	b, err := url.Parse(base)
	if err != nil {
		return ""
	}
	r, err := url.Parse(reference)
	if err != nil {
		return ""
	}
	value := b.ResolveReference(r).String()
	if len(replacements) > 0 {
		value = strings.NewReplacer(replacements...).Replace(value)
	}
	return value
}

func DCInsideDescription(value string) string {
	doc, err := document(value)
	if err != nil {
		return ""
	}
	var og, normal string
	ogFound := false
	walk(doc, func(n *html.Node) bool {
		if n.Type == html.ElementNode && n.Data == "meta" {
			if Attr(n, "property") == "og:description" && !ogFound {
				og = Attr(n, "content")
				ogFound = true
			}
			if Attr(n, "name") == "description" && normal == "" {
				normal = Attr(n, "content")
			}
		}
		return true
	})
	if ogFound {
		return CleanText(og)
	}
	return CleanText(normal)
}
