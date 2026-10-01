package extract

import (
	"regexp"
	"strconv"
	"strings"

	"golang.org/x/net/html"
)

var blankLines = regexp.MustCompile("\n{3,}")
var inlineSpaces = regexp.MustCompile("[\\t\\r\\n\\f ]+")
var markdownNoise = map[string]bool{"head": true, "script": true, "style": true, "noscript": true, "template": true, "svg": true}

func rawText(root *html.Node) string {
	var b strings.Builder
	walk(root, func(n *html.Node) bool {
		if n.Type == html.TextNode {
			b.WriteString(n.Data)
		}
		return true
	})
	return b.String()
}
func longestRun(s string, needle rune) int {
	best, run := 0, 0
	for _, r := range s {
		if r == needle {
			run++
			if run > best {
				best = run
			}
		} else {
			run = 0
		}
	}
	return best
}
func inlineCode(s string) string {
	fence := strings.Repeat(string(rune(96)), max(1, longestRun(s, rune(96))+1))
	if strings.HasPrefix(s, " ") || strings.HasSuffix(s, " ") || strings.ContainsRune(s, rune(96)) {
		return fence + " " + s + " " + fence
	}
	return fence + s + fence
}
func markdownChildren(n *html.Node, depth int) string {
	var b strings.Builder
	for c := n.FirstChild; c != nil; c = c.NextSibling {
		b.WriteString(markdownNode(c, depth+1))
		if b.Len() > MaxInputBytes {
			break
		}
	}
	return b.String()
}
func markdownTable(n *html.Node) string {
	rows := [][]string{}
	header := false
	walk(n, func(c *html.Node) bool {
		if c != n && c.Type == html.ElementNode && c.Data == "table" {
			return false
		}
		if c.Type != html.ElementNode || c.Data != "tr" {
			return true
		}
		cells := []string{}
		for cell := c.FirstChild; cell != nil; cell = cell.NextSibling {
			if cell.Type != html.ElementNode || (cell.Data != "th" && cell.Data != "td") {
				continue
			}
			if len(rows) == 0 && cell.Data == "th" {
				header = true
			}
			value := Collapse(markdownChildren(cell, 1))
			cells = append(cells, strings.ReplaceAll(value, "|", "\\|"))
		}
		if len(cells) > 0 {
			rows = append(rows, cells)
		}
		return false
	})
	if len(rows) == 0 {
		return ""
	}
	width := 0
	for _, r := range rows {
		width = max(width, len(r))
	}
	line := func(r []string) string {
		cells := append([]string{}, r...)
		for len(cells) < width {
			cells = append(cells, "")
		}
		return "| " + strings.Join(cells, " | ") + " |\n"
	}
	var b strings.Builder
	b.WriteString("\n\n")
	start := 0
	if header {
		b.WriteString(line(rows[0]))
		start = 1
	} else {
		b.WriteString(line(nil))
	}
	separator := make([]string, width)
	for i := range separator {
		separator[i] = "---"
	}
	b.WriteString(line(separator))
	for _, r := range rows[start:] {
		b.WriteString(line(r))
	}
	return b.String() + "\n"
}
func markdownNode(n *html.Node, depth int) string {
	if depth > 256 {
		return ""
	}
	if n.Type == html.TextNode {
		return inlineSpaces.ReplaceAllString(n.Data, " ")
	}
	if n.Type != html.ElementNode {
		return markdownChildren(n, depth)
	}
	if markdownNoise[n.Data] {
		return ""
	}
	if n.Data == "pre" {
		text := strings.Trim(rawText(n), "\n")
		fence := strings.Repeat(string(rune(96)), max(3, longestRun(text, rune(96))+1))
		language := ""
		if c := n.FirstChild; c != nil && c.Type == html.ElementNode && c.Data == "code" {
			for _, class := range strings.Fields(Attr(c, "class")) {
				if strings.HasPrefix(class, "language-") {
					language = strings.TrimPrefix(class, "language-")
					break
				}
			}
		}
		return "\n\n" + fence + language + "\n" + text + "\n" + fence + "\n\n"
	}
	if n.Data == "table" {
		return markdownTable(n)
	}
	body := markdownChildren(n, depth)
	switch n.Data {
	case "h1", "h2", "h3", "h4", "h5", "h6":
		level, _ := strconv.Atoi(n.Data[1:])
		return "\n\n" + strings.Repeat("#", level) + " " + trim(body) + "\n\n"
	case "p", "div", "article", "section", "main", "header", "footer", "figure":
		if trim(body) == "" {
			return ""
		}
		return "\n\n" + trim(body) + "\n\n"
	case "br":
		return "  \n"
	case "hr":
		return "\n\n---\n\n"
	case "strong", "b":
		return "**" + trim(body) + "**"
	case "em", "i":
		return "*" + trim(body) + "*"
	case "del", "s", "strike":
		return "~~" + trim(body) + "~~"
	case "code":
		return inlineCode(body)
	case "blockquote":
		lines := strings.Split(trim(body), "\n")
		for i := range lines {
			lines[i] = "> " + lines[i]
		}
		return "\n\n" + strings.Join(lines, "\n") + "\n\n"
	case "ul", "ol":
		return "\n" + trim(body) + "\n"
	case "li":
		prefix := "* "
		if n.Parent != nil && n.Parent.Data == "ol" {
			index := 1
			for c := n.PrevSibling; c != nil; c = c.PrevSibling {
				if c.Type == html.ElementNode && c.Data == "li" {
					index++
				}
			}
			prefix = strconv.Itoa(index) + ". "
		}
		lines := strings.Split(trim(body), "\n")
		for i := 1; i < len(lines); i++ {
			lines[i] = "  " + lines[i]
		}
		return prefix + strings.Join(lines, "\n") + "\n"
	case "a":
		href := Attr(n, "href")
		if href == "" || strings.HasPrefix(strings.ToLower(trim(href)), "javascript:") || strings.HasPrefix(strings.ToLower(trim(href)), "data:") {
			return body
		}
		return "[" + trim(body) + "](" + strings.ReplaceAll(href, ")", "%29") + ")"
	case "img":
		src, alt := Attr(n, "src"), Attr(n, "alt")
		if src == "" || strings.HasPrefix(strings.ToLower(src), "data:") {
			return alt
		}
		return "![" + alt + "](" + strings.ReplaceAll(src, ")", "%29") + ")"
	}
	return body
}
func HTMLToMarkdown(value string) string {
	doc, err := document(Cut(value, ScanLimit))
	if err != nil {
		return ""
	}
	return Cut(trim(blankLines.ReplaceAllString(markdownNode(doc, 0), "\n\n")), MaxText)
}

// MainContent uses a deterministic DOM heuristic, not a claim of byte-identical
// Resiliparse behavior. It prefers semantic article/main regions, removes
// boilerplate and refuses results shorter than the original 200-character gate.
func MainContent(value string) string {
	doc, err := document(Cut(value, ScanLimit))
	if err != nil {
		return ""
	}
	candidates := []*html.Node{}
	var body *html.Node
	walk(doc, func(n *html.Node) bool {
		if n.Type != html.ElementNode {
			return true
		}
		if n.Data == "body" {
			body = n
		}
		if n.Data == "article" || n.Data == "main" || Attr(n, "role") == "main" {
			candidates = append(candidates, n)
		}
		return true
	})
	if len(candidates) == 0 && body != nil {
		candidates = append(candidates, body)
	}
	best := ""
	for _, root := range candidates {
		parts := []string{}
		walk(root, func(n *html.Node) bool {
			if n.Type == html.ElementNode {
				switch n.Data {
				case "script", "style", "noscript", "svg", "template", "nav", "aside", "footer", "header", "form", "button":
					return false
				}
				if Attr(n, "aria-hidden") == "true" || Attr(n, "hidden") != "" {
					return false
				}
			}
			if n.Type == html.TextNode {
				if v := trim(n.Data); v != "" {
					parts = append(parts, v)
				}
			}
			return true
		})
		text := Collapse(strings.Join(parts, " "))
		if chars(text) > chars(best) {
			best = text
		}
	}
	if chars(best) < 200 {
		return ""
	}
	return Cut(best, MaxText)
}
