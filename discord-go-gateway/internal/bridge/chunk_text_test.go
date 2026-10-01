package bridge

import (
	"strings"
	"testing"
	"unicode/utf8"
)

func checkExactChunks(t *testing.T, text string) []string {
	t.Helper()
	parts, err := SplitText(text)
	if err != nil || strings.Join(parts, "") != text {
		t.Fatalf("lossless split failed: %v, chunks=%d", err, len(parts))
	}
	for _, part := range parts {
		if TextUnits(part) > replyChunkUnits || trimText(part) == "" || !utf8.ValidString(part) {
			t.Fatal("invalid chunk")
		}
	}
	return parts
}

func TestSplitWhitespaceBoundaryRegression(t *testing.T) {
	for _, tail := range []string{" ", "\n", "\r\n", "\t \n", strings.Repeat(" ", 500)} {
		checkExactChunks(t, strings.Repeat("x", 1900)+tail)
		checkExactChunks(t, strings.Repeat("😀", 950)+tail)
	}
	s, _ := testStore(t)
	ingestLedger(t, s, "one", "same")
	id := replyLedger(t, s, claimLedger(t, s), strings.Repeat("x", 1900)+"\r\n")
	d, err := s.Delivery(id)
	if err != nil || d.State != "queued" || len(d.Chunks) != 2 {
		t.Fatal("valid reply was not durably queued", d, err)
	}
}

func TestSplitCommonUnicodeClusters(t *testing.T) {
	for name, cluster := range map[string]string{
		"accent": "e\u0301", "family": "👨‍👩‍👧‍👦", "modifier": "👍🏽", "flag": "🇰🇷",
		"variation": "☀️", "keycap": "1️⃣", "hangul": "각", "crlf": "\r\n",
	} {
		t.Run(name, func(t *testing.T) {
			text := strings.Repeat("x", 1899) + cluster + "tail"
			parts := checkExactChunks(t, text)
			if len(parts) != 2 || !strings.Contains(parts[1], cluster) {
				t.Fatal("cluster split across messages")
			}
		})
	}
	// Pair regional indicators in sequence rather than forming an oversized atom.
	checkExactChunks(t, strings.Repeat("🇰🇷", 1000))
	if _, err := SplitText("x" + strings.Repeat("\u0301", 1900)); err == nil {
		t.Fatal("oversized indivisible cluster accepted")
	}
}

func TestSplitPrefersBoundariesAndKeepsShortFences(t *testing.T) {
	for _, fence := range []string{"```go\n" + strings.Repeat("x", 300) + "\n```\n", "~~~python\ncode\n~~~\n"} {
		text := strings.Repeat("a", 1890) + "\n" + fence + "after"
		parts := checkExactChunks(t, text)
		if len(parts) != 2 || !strings.Contains(parts[1], fence) {
			t.Fatal("short fenced block split")
		}
	}
	for _, text := range []string{
		strings.Repeat("hello world\n", 500),
		"```go\n" + strings.Repeat("x", 5000) + "\n```",
		strings.Repeat("한글😀 ", 500),
		strings.Repeat("x", 16000),
	} {
		checkExactChunks(t, text)
	}
}
