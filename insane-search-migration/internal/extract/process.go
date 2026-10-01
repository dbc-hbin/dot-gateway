package extract

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"

	"golang.org/x/net/html"
)

type Options struct {
	Enabled, Markdown, MainContent bool
	InnerText, ContentType         string
}
type Result struct {
	Title, Content, Source string
	Quality                float64
	Meta                   map[string]any
}

func Quality(value string) float64 {
	if value == "" {
		return 0
	}
	runes := []rune(value)
	sentences := 1
	for i, r := range runes {
		if i+1 < len(runes) && strings.ContainsRune(".!?。…", r) && pySpace(runes[i+1]) {
			sentences++
		}
	}
	words := max(len(strings.FieldsFunc(value, pySpace)), 1)
	length := math.Min(float64(len(runes))/3000, 1)
	structure := math.Min(float64(sentences)/(float64(words)/18+1), 1)
	return math.Round(math.Min(1, 0.6*length+0.4*structure)*100) / 100
}
func JSONLD(value string) string {
	doc, err := document(Cut(value, ScanLimit))
	if err != nil {
		return ""
	}
	parts := []string{}
	blocks, total := 0, 0
	walk(doc, func(n *html.Node) bool {
		if blocks >= 10 || total >= MaxText {
			return false
		}
		if n.Type != html.ElementNode || n.Data != "script" || !strings.EqualFold(Attr(n, "type"), "application/ld+json") {
			return true
		}
		blocks++
		raw := rawText(n)
		if chars(raw) > 200000 {
			return false
		}
		var payload any
		if json.Unmarshal([]byte(raw), &payload) != nil {
			return false
		}
		if values, ok := payload.([]any); ok {
			if len(values) == 0 {
				return false
			}
			payload = values[0]
		}
		data, ok := payload.(map[string]any)
		if !ok {
			return false
		}
		t, _ := data["@type"].(string)
		_, hasBody := data["articleBody"]
		if t != "Article" && t != "NewsArticle" && t != "BlogPosting" && !hasBody {
			return false
		}
		content, ok := choose(data, "articleBody", "description").(string)
		if !ok || content == "" {
			return false
		}
		content = Cut(content, MaxText-total)
		parts = append(parts, content)
		total += chars(content)
		return false
	})
	return Cut(strings.Join(parts, "\n\n"), MaxText)
}
func metadata(value string) (string, map[string]string) {
	doc, err := document(Cut(value, ScanLimit))
	if err != nil {
		return "", nil
	}
	title := ""
	og := map[string]string{}
	walk(doc, func(n *html.Node) bool {
		if n.Type != html.ElementNode {
			return true
		}
		if n.Data == "title" && title == "" {
			title = Cut(Collapse(rawText(n)), 300)
		}
		if n.Data == "meta" && len(og) < 32 {
			key := Attr(n, "property")
			if key == "" {
				key = Attr(n, "name")
			}
			if strings.HasPrefix(key, "og:") || key == "description" {
				if _, exists := og[key]; !exists {
					og[key] = Cut(Attr(n, "content"), 2000)
				}
			}
		}
		return true
	})
	return title, og
}
func Process(url, content string, options Options) (Result, error) {
	result := Result{Content: content, Source: "raw", Meta: map[string]any{"source": "raw", "error": "", "inner_text_used": false}}
	if !options.Enabled {
		result.Source = "raw_disabled"
		result.Meta["source"] = result.Source
		return result, nil
	}
	pdf := strings.HasPrefix(content, "%PDF-") || strings.Contains(strings.ToLower(options.ContentType), "pdf")
	if pdf {
		title, text, code := ExtractPDF([]byte(content))
		if text == "" {
			text = fmt.Sprintf("[PDF binary, %d bytes; extractor=%s]", len(content), code)
		}
		result.Title = title
		result.Content = text
		result.Source = "pdf"
		result.Quality = Quality(text)
		if code != "" {
			result.Quality = 0
		}
		result.Meta = map[string]any{"source": "pdf", "error": code, "inner_text_used": false, "extractor": "native_go"}
		return result, nil
	}
	if len(content) > MaxInputBytes {
		return result, fmt.Errorf("extraction input exceeds %d bytes", MaxInputBytes)
	}
	if content == "" {
		result.Source = "empty"
		result.Meta["source"] = "empty"
		result.Meta["error"] = "empty_body"
		return result, nil
	}
	scan := Cut(content, ScanLimit)
	var og map[string]string
	result.Title, og = metadata(scan)
	if len(og) > 0 {
		result.Meta["metadata"] = og
	}
	visible := VisibleText(scan)
	result.Quality = Quality(visible)
	if ld := JSONLD(scan); chars(ld) > 100 && chars(ld) > chars(visible) {
		result.Content = ld
		result.Source = "json_ld"
		result.Quality = Quality(ld)
	}
	chosen := chars(visible)
	if result.Source != "raw" {
		chosen = chars(result.Content)
	}
	inner := Cut(trim(options.InnerText), MaxText)
	if chars(inner) > max(chosen, 200) {
		result.Content = inner
		result.Source += "+inner_text"
		result.Quality = Quality(inner)
		result.Meta["inner_text_used"] = true
	}
	if options.MainContent && result.Source == "raw" {
		if main := MainContent(content); main != "" {
			result.Content = main
			result.Source = "maincontent"
			result.Quality = Quality(main)
			result.Meta["algorithm"] = "native_dom_heuristic"
		}
	}
	if options.Markdown && result.Source == "raw" && htmlLooking.MatchString(content) {
		if md := HTMLToMarkdown(content); md != "" {
			result.Content = md
			result.Source = "raw+md"
			result.Quality = Quality(md)
		}
	}
	result.Meta["source"] = result.Source
	return result, nil
}
