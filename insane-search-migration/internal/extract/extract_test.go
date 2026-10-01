package extract

import (
	"bytes"
	"compress/zlib"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
)

func TestJSONListingsAndBrowserViewers(t *testing.T) {
	input := "{\"topic_list\":{\"topics\":[{\"id\":123,\"slug\":\"offer\",\"title\":\"Claude &amp; student\",\"excerpt\":\"<p>Free <b>month</b></p>\"}]}}"
	for _, body := range []string{input, "<html><body><pre>" + strings.ReplaceAll(input, "&", "&amp;") + "</pre></body></html>"} {
		rows, diag := ParseListing("linux.do-welfare", "https://linux.do/latest.json", body)
		if !diag.Recognized || diag.Format != "json" || len(rows) != 1 {
			t.Fatalf("%+v %+v", rows, diag)
		}
		if rows[0].URL != "https://linux.do/t/offer/123" || rows[0].Title != "Claude & student" || rows[0].ListingExcerpt != "Free month" {
			t.Fatal(rows)
		}
	}
	for _, body := range []string{"[]", "{\"topic_list\":{\"topics\":[]}}", "<pre>[]</pre>"} {
		rows, diag := ParseListing("v2ex-hot", "https://v2ex.com/api/topics/hot.json", body)
		if !diag.Recognized || len(rows) != 0 || diag.Error != "" {
			t.Fatalf("valid empty: %q %+v", body, diag)
		}
	}
	for _, body := range []string{"{}", "{\"error\":\"blocked\"}", "[] garbage", "<h1>Site Unavailable</h1>", "<html><div id=\"app\"></div></html>", "[{\"not_a_topic\":true}]"} {
		_, diag := ParseListing("linux.do-latest", "https://linux.do/latest.json", body)
		if diag.Recognized || diag.Error == "" {
			t.Fatalf("false empty success: %q %+v", body, diag)
		}
	}
}
func TestHTMLListingAndDescription(t *testing.T) {
	body := "<a href='/t/123'>Claude <b>student</b> coupon</a><a href='/no'>not topic</a>"
	rows, diag := ParseListing("v2ex-hot", "https://www.v2ex.com/", body)
	if !diag.Recognized || diag.Format != "html" || len(rows) != 1 || rows[0].Title != "Claude student coupon" || rows[0].URL != "https://www.v2ex.com/t/123" {
		t.Fatalf("%+v %+v", rows, diag)
	}
	dc := "<a href='/board/ai_utilize/555'>이미지 Claude 무료 정보 댓글 12:03 조회 50 추천 2</a>"
	rows = ParseHTMLListing("dcinside-ai-utilize", "https://m.dcinside.com", dc)
	if len(rows) != 1 || rows[0].Title != "Claude 무료" {
		t.Fatal(rows)
	}
	if got := DCInsideDescription("<meta name='description' content='fallback'><meta property='og:description' content='First &amp; best'>"); got != "First & best" {
		t.Fatal(got)
	}
	if got := DCInsideDescription("<meta property='og:description'><meta name='description' content='fallback'>"); got != "" {
		t.Fatal("empty existing OG must win", got)
	}
}
func TestDCInsideObservedDesktopRedirectListing(t *testing.T) {
	data, err := os.ReadFile("testdata/dcinside_desktop_observed.html")
	if err != nil {
		t.Fatal(err)
	}
	rows, diag := ParseListing("dcinside-ai-utilize", "https://gall.dcinside.com/mgallery/board/lists/?id=ai_utilize", string(data))
	if !diag.Recognized || diag.Format != "html" || diag.Parsed != 2 || len(rows) != 2 {
		t.Fatalf("%+v %+v", rows, diag)
	}
	if rows[0].Title != "Synthetic Claude 학생 무료 정보" || rows[1].Title != "Synthetic Gemini 쿠폰" {
		t.Fatal(rows)
	}
	if rows[0].URL != "https://gall.dcinside.com/mgallery/board/view/?id=ai_utilize&no=1001&page=1" {
		t.Fatal(rows[0].URL)
	}
	if rows[0].URL == rows[1].URL {
		t.Fatal("distinct desktop posts collapsed")
	}
	// Merely seeing a gallery title or an empty table is still not a verified
	// empty listing, and must never mask a challenge/maintenance response.
	_, empty := ParseListing("dcinside-ai-utilize", "https://gall.dcinside.com/mgallery/board/lists/?id=ai_utilize", "<title>AI 활용 갤러리</title><table></table>")
	if empty.Recognized {
		t.Fatal("empty shell accepted")
	}
}
func TestCleanText(t *testing.T) {
	for _, pair := range [][2]string{{"a <b>b</b> c", "a b c"}, {"x<script>poison</script><style>noise</style>y", "x y"}, {"&amp;lt;", "&lt;"}, {"韓国\u001c\u0085日本", "韓国 日本"}, {"<p>one</p><p>two</p>", "one two"}} {
		if actual := CleanText(pair[0]); actual != pair[1] {
			t.Errorf("%q -> %q wanted %q", pair[0], actual, pair[1])
		}
	}
}
func TestURLJoinPreservesUnicodeAndExistingEscapes(t *testing.T) {
	for _, pair := range [][2]string{
		{"/t/한글/123", "https://example.org/t/한글/123"},
		{"/t/%ED%95%9C%EA%B8%80/123", "https://example.org/t/%ED%95%9C%EA%B8%80/123"},
		{"/t/a b/123", "https://example.org/t/a b/123"},
		{"../t/日本/123?q=值#절", "https://example.org/t/日本/123?q=值#절"},
	} {
		if got := resolve("https://example.org/base/list", pair[0]); got != pair[1] {
			t.Errorf("%q -> %q wanted %q", pair[0], got, pair[1])
		}
	}
}
func TestStructuredMarkdown(t *testing.T) {
	body := "<head><title>noise</title></head><h1>Guide</h1><p>Hello <strong>world</strong>. <a href='https://example.com'>Link</a></p><ul><li>First</li><li>Second</li></ul><pre><code class='language-go'>fmt.Println(&quot;ok&quot;)\n</code></pre><table><tr><th>Name</th><th>Value</th></tr><tr><td>AI</td><td>Free</td></tr></table><script>secret</script>"
	got := HTMLToMarkdown(body)
	for _, needle := range []string{"# Guide", "**world**", "[Link](https://example.com)", "* First", "* Second", strings.Repeat(string(rune(96)), 3) + "go", "fmt.Println(\"ok\")", "| Name | Value |", "| AI | Free |"} {
		if !strings.Contains(got, needle) {
			t.Errorf("missing %q:\n%s", needle, got)
		}
	}
	if strings.Contains(got, "noise") || strings.Contains(got, "secret") {
		t.Fatal(got)
	}
}
func TestJSONLDAndRenderRescue(t *testing.T) {
	article := strings.Repeat("Important public offer. ", 50)
	blob, _ := json.Marshal(map[string]any{"@type": "Article", "articleBody": article})
	body := "<title>Public Offer</title><p>Loading</p><script type='application/ld+json'>" + string(blob) + "</script>"
	got, err := Process("https://example.com", body, Options{Enabled: true, Markdown: true})
	if err != nil || got.Source != "json_ld" || got.Content != article || got.Title != "Public Offer" {
		t.Fatalf("%+v %v", got, err)
	}
	longer := strings.Repeat("Rendered actual public article. ", 100)
	got, err = Process("https://example.com", body, Options{Enabled: true, Markdown: true, InnerText: longer})
	if err != nil || got.Source != "json_ld+inner_text" || got.Meta["inner_text_used"] != true || got.Content != strings.TrimSpace(longer) {
		t.Fatalf("%+v %v", got, err)
	}
	ordinary := "<article>" + strings.Repeat("Actual body. ", 100) + "</article><script type='application/ld+json'>{\"@type\":\"Article\",\"description\":\"short teaser\"}</script>"
	got, _ = Process("https://example.com", ordinary, Options{Enabled: true})
	if got.Source != "raw" || got.Content != ordinary {
		t.Fatal(got)
	}
}
func TestMainContentAndCaps(t *testing.T) {
	body := "<nav>navigation advertisement</nav><article>" + strings.Repeat("Real article text. ", 40) + "</article><footer>footer junk</footer>"
	result, err := Process("https://example.com", body, Options{Enabled: true, Markdown: true, MainContent: true})
	if err != nil || result.Source != "maincontent" || strings.Contains(result.Content, "advertisement") || strings.Contains(result.Content, "footer") {
		t.Fatalf("%+v %v", result, err)
	}
	if result.Meta["algorithm"] != "native_dom_heuristic" {
		t.Fatal("must disclose heuristic")
	}
	if Cut("한글abc", 2) != "한글" {
		t.Fatal("rune truncation")
	}
	disabled, _ := Process("https://example.com", body, Options{})
	if disabled.Source != "raw_disabled" || disabled.Content != body {
		t.Fatal(disabled)
	}
	oversized := strings.Repeat("x", MaxInputBytes+1)
	if _, e := Process("https://example.com", oversized, Options{Enabled: true}); e == nil {
		t.Fatal("missing input cap")
	}
}

// makePDF creates a tiny real PDF directly in Go; no external fixture generator.
func makePDF(stream string, compressed bool) []byte {
	content := []byte(stream)
	filter := ""
	if compressed {
		var b bytes.Buffer
		z := zlib.NewWriter(&b)
		z.Write(content)
		z.Close()
		content = b.Bytes()
		filter = "/Filter /FlateDecode "
	}
	objects := []string{
		"<< /Type /Catalog /Pages 2 0 R >>",
		"<< /Type /Pages /Count 1 /Kids [3 0 R] >>",
		"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 600 800] /Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R >>",
		"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
		fmt.Sprintf("<< %s/Length %d >>\nstream\n%s\nendstream", filter, len(content), content),
		"<< /Title (Synthetic public PDF) >>",
	}
	var out bytes.Buffer
	out.WriteString("%PDF-1.4\n")
	offsets := []int{0}
	for i, object := range objects {
		offsets = append(offsets, out.Len())
		fmt.Fprintf(&out, "%d 0 obj\n%s\nendobj\n", i+1, object)
	}
	xref := out.Len()
	fmt.Fprintf(&out, "xref\n0 %d\n0000000000 65535 f \n", len(objects)+1)
	for _, offset := range offsets[1:] {
		fmt.Fprintf(&out, "%010d 00000 n \n", offset)
	}
	fmt.Fprintf(&out, "trailer\n<< /Size %d /Root 1 0 R /Info 6 0 R >>\nstartxref\n%d\n%%%%EOF\n", len(objects)+1, xref)
	return out.Bytes()
}
func TestNativePDFTextAndMetadata(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		body := makePDF("BT /F1 12 Tf 72 720 Td (Hello public PDF) Tj T* [(Second) 10 ( line)] TJ ET", compressed)
		title, text, code := ExtractPDF(body)
		if code != "" || title != "Synthetic public PDF" || !strings.Contains(text, "Hello public PDF") || !strings.Contains(text, "Second line") {
			t.Fatalf("%q %q %q", title, text, code)
		}
		result, err := Process("https://example.com/file.pdf", string(body), Options{Enabled: true})
		if err != nil || result.Source != "pdf" || result.Meta["error"] != "" {
			t.Fatalf("%+v %v", result, err)
		}
	}
}
func TestNativePDFFailuresAndDecodedStreamLimit(t *testing.T) {
	if _, _, code := ExtractPDF([]byte("not pdf")); code != "pdf_invalid_header" {
		t.Fatal(code)
	}
	if _, _, code := ExtractPDF([]byte("%PDF-1.4\nbroken")); code == "" {
		t.Fatal("malformed PDF accepted")
	}
	if _, _, code := ExtractPDF(makePDF("", false)); code != "pdf_no_text_layer" {
		t.Fatal(code)
	}
	if _, _, code := ExtractPDF(makePDF(strings.Repeat(" ", pdfStreamLimit+1), true)); code != "pdf_resource_limit" {
		t.Fatal("compressed stream ceiling:", code)
	}
	if _, _, code := ExtractPDF(append([]byte("%PDF-"), make([]byte, MaxInputBytes)...)); code != "pdf_too_large" {
		t.Fatal(code)
	}
}
func TestGoldenParserFixtures(t *testing.T) {
	var cases []struct {
		Source, URL, Text string
		Rows              []Row
	}
	data, err := os.ReadFile("testdata/parser_golden.json")
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(data, &cases); err != nil {
		t.Fatal(err)
	}
	for _, fixture := range cases {
		rows := ParseJSONListing(fixture.Source, fixture.URL, fixture.Text)
		if len(rows) == 0 {
			rows = ParseHTMLListing(fixture.Source, fixture.URL, fixture.Text)
		}
		got, _ := json.Marshal(rows)
		want, _ := json.Marshal(fixture.Rows)
		if string(got) != string(want) {
			t.Errorf("%s\nGo %s\noriginal %s", fixture.Source, got, want)
		}
	}
}
