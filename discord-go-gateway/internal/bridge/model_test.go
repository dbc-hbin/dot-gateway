package bridge

import (
	"encoding/json"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestSplitUTF16(t *testing.T) {
	s := strings.Repeat("😀", 1000) + "한글"
	parts, e := SplitText(s)
	if e != nil || len(parts) != 2 || strings.Join(parts, "") != s {
		t.Fatalf("split: %v %d", e, len(parts))
	}
	for _, p := range parts {
		if TextUnits(p) > 1900 || !utf8.ValidString(p) {
			t.Fatal("bad chunk")
		}
	}
}
func TestSplitReject(t *testing.T) {
	for _, s := range []string{"", "  ", strings.Repeat("x", 16001), string([]byte{255})} {
		if _, e := SplitText(s); e == nil {
			t.Fatal("accepted invalid text")
		}
	}
}

func TestEnvelopeJSONNullCompatibility(t *testing.T) {
	b, e := json.Marshal(testEnvelope())
	if e != nil {
		t.Fatal(e)
	}
	var v map[string]any
	json.Unmarshal(b, &v)
	for _, k := range []string{"guild_id", "reply_to_event_id"} {
		value, ok := v[k]
		if !ok || value != nil {
			t.Fatalf("missing null %s", k)
		}
	}
	var env Envelope
	if e = json.Unmarshal([]byte(`{"platform":"discord","guild_id":null,"reply_to_event_id":null}`), &env); e != nil || env.RouteKind != "dm" {
		t.Fatal(e)
	}
}
