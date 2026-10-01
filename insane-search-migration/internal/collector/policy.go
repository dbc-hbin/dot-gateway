package collector

import (
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/dlclark/regexp2"
)

//go:embed policy_data.json
var policyJSON []byte

//go:embed casefold_data.json
var casefoldJSON []byte

//go:embed unicode_classes.json
var unicodeJSON []byte

var vocab map[string]json.RawMessage
var folds map[rune]string
var lowers map[rune]string
var lowerCased, lowerIgnorable [][2]rune
var classes map[string]string
var patterns map[string]*regexp2.Regexp

type redactionSpec struct {
	regex        *regexp2.Regexp
	fold, prefix bool
}

var redactions []redactionSpec
var lists map[string][]string
var Sources []Source
var latinTerm = regexp.MustCompile(`^[a-z0-9][a-z0-9 .+\-]{0,48}$`)
var regexSpecial = strings.NewReplacer("İ", "i", "ı", "i", "ſ", "s", "K", "k")

func init() {
	mustJSON(policyJSON, &vocab)
	var f struct {
		UnicodeVersion string            `json:"unicode_version"`
		Map            map[string]string `json:"map"`
		Lower          map[string]string `json:"lower"`
		Cased          [][2]rune         `json:"cased"`
		Ignorable      [][2]rune         `json:"case_ignorable"`
	}
	mustJSON(casefoldJSON, &f)
	folds = map[rune]string{}
	for k, v := range f.Map {
		n, e := strconv.Atoi(k)
		if e != nil {
			panic(e)
		}
		folds[rune(n)] = v
	}
	lowerCased, lowerIgnorable = f.Cased, f.Ignorable
	lowers = map[rune]string{}
	for k, v := range f.Lower {
		n, e := strconv.Atoi(k)
		if e != nil {
			panic(e)
		}
		lowers[rune(n)] = v
	}
	mustJSON(unicodeJSON, &classes)
	patterns = map[string]*regexp2.Regexp{}
	lists = map[string][]string{}
	for k, v := range vocab {
		if strings.HasSuffix(k, "_RE") {
			var s string
			mustJSON(v, &s)
			if k == "ANTI_AI_RE" || k == "V2EX_CHROME_RE" {
				s = "(?s)" + s
			}
			patterns[k] = compilePython(s, true)
		} else if k != "SOURCES" {
			var s []string
			mustJSON(v, &s)
			lists[k] = s
		}
	}
	redactions = newRedactionSpecs()
	lists["AI_TERMS"] = append(append([]string{}, lists["GENERIC_AI_TERMS"]...), lists["SPECIFIC_AI_TERMS"]...)
	var src [][]string
	mustJSON(vocab["SOURCES"], &src)
	for _, s := range src {
		Sources = append(Sources, Source{Name: s[0], URL: s[1]})
	}
}
func mustJSON(b []byte, v any) {
	if e := json.Unmarshal(b, v); e != nil {
		panic(e)
	}
}

// Python \w, \d and \s use Python's generated Unicode 15 tables, not
// RE2's ASCII classes or regexp2's different .NET word classification.
// Lookarounds remain actual assertions; regexp2 runs in native Go.
func compilePython(pattern string, ignoreCase bool) *regexp2.Regexp {
	word := "[" + classes["word"] + "]"
	boundary := "(?:(?<!" + word + ")(?=" + word + ")|(?<=" + word + ")(?!" + word + "))"
	pattern = strings.ReplaceAll(pattern, `\b`, boundary)
	pattern = strings.ReplaceAll(pattern, `\w`, classes["word"])
	pattern = strings.ReplaceAll(pattern, `\d`, "["+classes["digit"]+"]")
	pattern = strings.ReplaceAll(pattern, `\s`, "["+classes["space"]+"]")
	opts := regexp2.RegexOptions(0)
	if ignoreCase {
		opts |= regexp2.IgnoreCase
	}
	r, e := regexp2.Compile(pattern, opts)
	if e != nil {
		panic(fmt.Errorf("compile Python-compatible regex: %w", e))
	}
	r.MatchTimeout = 250 * time.Millisecond
	return r
}
func search(name, text string) bool {
	ok, e := patterns[name].MatchString(regexSpecial.Replace(text))
	if e != nil {
		panic(fmt.Errorf("policy regex %s failed: %w", name, e))
	}
	return ok
}
func Casefold(s string) string {
	var b strings.Builder
	for _, r := range s {
		if f, ok := folds[r]; ok {
			b.WriteString(f)
		} else {
			b.WriteRune(r)
		}
	}
	return b.String()
}
func inRanges(r rune, ranges [][2]rune) bool {
	i := sort.Search(len(ranges), func(i int) bool { return ranges[i][1] >= r })
	return i < len(ranges) && ranges[i][0] <= r
}
func pythonLower(s string) string {
	var b strings.Builder
	runes := []rune(s)
	for i, r := range runes {
		if r == 'Σ' {
			before, after := false, false
			for j := i - 1; j >= 0; j-- {
				if inRanges(runes[j], lowerIgnorable) {
					continue
				}
				before = inRanges(runes[j], lowerCased)
				break
			}
			for j := i + 1; j < len(runes); j++ {
				if inRanges(runes[j], lowerIgnorable) {
					continue
				}
				after = inRanges(runes[j], lowerCased)
				break
			}
			if before && !after {
				b.WriteRune('ς')
				continue
			}
		}
		if v, ok := lowers[r]; ok {
			b.WriteString(v)
		} else {
			b.WriteRune(r)
		}
	}
	return b.String()
}
func pythonSpace(r rune) bool {
	return strings.ContainsRune("\t\n\v\f\r\x1c\x1d\x1e\x1f \u0085\u00a0\u1680\u2000\u2001\u2002\u2003\u2004\u2005\u2006\u2007\u2008\u2009\u200a\u2028\u2029\u202f\u205f\u3000", r)
}
func collapse(s string) string { return strings.Join(strings.FieldsFunc(s, pythonSpace), " ") }
func runeSlice(s string, n int) string {
	if utf8.RuneCountInString(s) <= n {
		return s
	}
	return string([]rune(s)[:n])
}
func ContainsTerm(text, term string) bool {
	h := Casefold(text)
	n := strings.TrimFunc(Casefold(term), pythonSpace)
	if n == "" {
		return false
	}
	if !latinTerm.MatchString(n) {
		return strings.Contains(h, n)
	}
	for off := 0; off <= len(h); {
		i := strings.Index(h[off:], n)
		if i < 0 {
			break
		}
		i += off
		end := i + len(n)
		before := i == 0 || h[i-1] < 'a' || h[i-1] > 'z'
		after := end == len(h) || h[end] < 'a' || h[end] > 'z'
		if before && after {
			return true
		}
		off = i + 1
	}
	return false
}
func matchList(text string, terms []string) []string {
	out := []string{}
	seen := map[string]bool{}
	for _, t := range terms {
		if !seen[t] && ContainsTerm(text, t) {
			out = append(out, t)
			seen[t] = true
		}
	}
	sort.Strings(out)
	return out
}
func Matches(text string) (ai, strong, weak []string) {
	return matchList(text, lists["AI_TERMS"]), matchList(text, lists["STRONG_DEAL_TERMS"]), matchList(text, lists["WEAK_DEAL_TERMS"])
}
func specificAI(ai []string) bool {
	for _, t := range ai {
		generic := false
		for _, g := range lists["GENERIC_AI_TERMS"] {
			if Casefold(t) == Casefold(g) {
				generic = true
				break
			}
		}
		if !generic {
			return true
		}
	}
	return false
}
func MentionsVendor(text string) bool {
	for _, t := range lists["OFFICIAL_VENDORS"] {
		if ContainsTerm(text, t) {
			return true
		}
	}
	return false
}
func FirstParty(url string) bool {
	_, host, _ := splitURL(url)
	host = strings.TrimPrefix(pythonLower(host), "www.")
	if host == "" {
		return false
	}
	for _, h := range lists["OFFICIAL_HOSTS"] {
		if host == h || strings.HasSuffix(host, "."+h) {
			return true
		}
	}
	return false
}
func ListingRejected(title, excerpt, url string) string {
	blob := title + " " + excerpt
	official := FirstParty(url)
	switch {
	case search("JOB_RE", title) || search("QUESTION_RE", title) || search("QUESTION_RE", runeSlice(blob, 80)):
		return "question_or_job"
	case search("EDITORIAL_RE", blob) && !official:
		return "editorial_or_interview"
	case search("KEY_SHARE_RE", blob):
		return "public_key_or_account_pool"
	case search("TRUST_GATE_RE", blob) && !search("CONCRETE_VALUE_RE", title):
		return "inaccessible_trust_level"
	case search("SPECULATION_RE", blob):
		return "quota_speculation"
	case search("SOFTWARE_RELEASE_RE", blob) && !search("CONCRETE_VALUE_RE", blob) && !official:
		return "ordinary_software_release"
	case search("NON_AI_AFF_RE", blob) || search("ACCOUNT_SELL_RE", blob):
		return "affiliate_or_account_sale"
	case (search("AFF_RE", blob) || search("SELF_PROMO_RE", blob)) && !official:
		return "affiliate_or_self_promo"
	}
	if !official && search("RELAY_COMMERCIAL_RE", blob) {
		if !(search("OFFICIAL_RIDE_RE", blob) && !regexp.MustCompile(`倍率|线路|充值|首充|到账|全模型`).MatchString(blob)) {
			return "relay_or_self_promo"
		}
	}
	if search("RELAY_RE", blob) && !official {
		return "relay_or_self_promo"
	}
	return ""
}
func WorthCollecting(title, excerpt, source, url string) bool {
	blob := title + " " + excerpt
	ai, strong, weak := Matches(blob)
	if !specificAI(ai) && !strings.HasPrefix(source, "dcinside") {
		return false
	}
	if ListingRejected(title, excerpt, url) != "" {
		return false
	}
	return len(strong) > 0 || search("CONCRETE_BENEFIT_RE", blob) || (search("CONCRETE_NUMERIC_RE", blob) && (len(strong) > 0 || len(weak) > 0)) || (len(weak) > 0 && MentionsVendor(blob))
}
func RankScore(title, excerpt, source, url string) int {
	blob := title + " " + excerpt
	score := 0
	official := FirstParty(url)
	if official {
		score += 8
	} else if MentionsVendor(blob) && !search("AFF_RE", blob) {
		score += 2
	}
	_, strong, weak := Matches(blob)
	if search("CONCRETE_BENEFIT_RE", blob) || (search("CONCRETE_NUMERIC_RE", blob) && (len(strong) > 0 || len(weak) > 0)) {
		score += 6
	}
	score += 3*min(len(strong), 4) + min(len(weak), 2)
	if strings.HasSuffix(source, "-welfare") || strings.HasSuffix(source, "-deals") || strings.HasPrefix(source, "dcinside") {
		score += 2
	}
	if strings.Contains(blob, "学生") || strings.Contains(blob, "教育") || strings.Contains(Casefold(blob), "student") {
		score += 4
	}
	if strings.Contains(blob, "无需绑卡") {
		score += 4
	}
	if strings.Contains(blob, "抽奖") || strings.Contains(Casefold(blob), "giveaway") {
		if official {
			score--
		} else {
			score -= 4
		}
	}
	if LikelyRelay(blob) {
		score -= 12
	}
	return score
}
func LikelyRelay(blob string) bool {
	return search("RELAY_RE", blob) || search("RELAY_COMMERCIAL_RE", blob) || search("SELF_PROMO_RE", blob) || search("AFF_RE", blob)
}
func StripPoison(s string) string {
	for _, n := range []string{"ANTI_AI_RE", "V2EX_CHROME_RE"} {
		r := patterns[n] // These source regexes have DOTALL in Python.
		s0, e := r.Replace(s, " ", -1, -1)
		if e != nil {
			panic(e)
		}
		s = s0
	}
	return collapse(s)
}
func Fingerprint(s string) string {
	sum := sha256.Sum256([]byte(runeSlice(collapse(Casefold(s)), 12000)))
	return hex.EncodeToString(sum[:])
}
func newRedactionSpecs() []redactionSpec {
	return []redactionSpec{
		{compilePython(`\bsk-[A-Za-z0-9_-]{12,}\b`, false), false, false},
		{compilePython(`\bAIza[A-Za-z0-9_-]{20,}\b`, false), false, false},
		{compilePython(`(api[_ -]?key\s*[:=]\s*)[A-Za-z0-9_.-]{12,}`, true), true, true},
	}
}

// regexp2 documents Regexp as safe for concurrent use. Keep patterns and
// MatchTimeout immutable after initialization; replacement buffers stay local.
func Redact(s string) string { return redactWithSpecs(s, redactions) }
func redactWithSpecs(s string, specs []redactionSpec) string {
	for _, spec := range specs {
		r := spec.regex
		input := s
		if spec.fold {
			input = regexSpecial.Replace(s)
		}
		original := []rune(s)
		var out strings.Builder
		last := 0
		for m, e := r.FindStringMatch(input); m != nil || e != nil; {
			if e != nil {
				panic(e)
			}
			out.WriteString(string(original[last:m.Index]))
			if spec.prefix {
				g := m.Groups()[1]
				out.WriteString(string(original[g.Index : g.Index+g.Length]))
			}
			out.WriteString("[REDACTED_SECRET]")
			last = m.Index + m.Length
			m, e = r.FindNextMatch(m)
		}
		out.WriteString(string(original[last:]))
		s = out.String()
	}
	return s
}
