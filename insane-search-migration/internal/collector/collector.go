package collector

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"regexp"
	"sort"
	"strings"
)

const BridgeLimit = 16 * 1024 * 1024
const MaxScanPerSource = 40
const MaxPerSource = 4
const MaxTotal = 16
const MaxFingerprints = 2500

type Source struct {
	Name string `json:"name"`
	URL  string `json:"url"`
}
type Row struct {
	Source         string `json:"source"`
	Title          string `json:"title"`
	URL            string `json:"url"`
	ListingExcerpt string `json:"listing_excerpt"`
	Rank           int    `json:"rank"`
}
type Candidate struct {
	Source       string   `json:"source"`
	Title        string   `json:"title"`
	URL          string   `json:"url"`
	Change       string   `json:"change"`
	Rank         int      `json:"rank"`
	QualityFlags []string `json:"quality_flags"`
	MatchedAI    []string `json:"matched_ai_terms"`
	MatchedDeals []string `json:"matched_deal_terms"`
	Excerpt      string   `json:"excerpt"`
	DetailError  *string  `json:"detail_fetch_error"`
}
type Failure struct {
	Source string `json:"source"`
	Error  string `json:"error"`
}
type Report struct {
	Candidates        []Candidate           `json:"candidates"`
	FailedSources     []Failure             `json:"failed_sources"`
	SourceDiagnostics []SourceDiagnostic    `json:"source_diagnostics,omitempty"`
	Collection        *CollectionDiagnostic `json:"collection,omitempty"`
}
type Backend interface {
	Listing(context.Context, Source) ([]Row, string, error)
	Body(context.Context, Row) (string, string, error)
}

// splitURL mirrors the source's URL split for HTTP(S) forum/vendor URLs while
// preserving raw Unicode/percent encoding (net/url.String would re-escape it).
func splitURL(s string) (scheme, host, path string) {
	s = strings.TrimLeftFunc(s, func(r rune) bool { return r <= 32 })
	s = strings.NewReplacer("\r", "", "\n", "", "\t", "").Replace(s)
	if i := strings.IndexAny(s, "?#"); i >= 0 {
		s = s[:i]
	}
	if i := strings.Index(s, ":"); i >= 0 && regexp.MustCompile(`^[A-Za-z][A-Za-z0-9+.-]*$`).MatchString(s[:i]) {
		scheme = strings.ToLower(s[:i])
		s = s[i+1:]
	}
	if strings.HasPrefix(s, "//") {
		s = s[2:]
		i := strings.IndexByte(s, '/')
		if i < 0 {
			return scheme, s, ""
		}
		host = s[:i]
		s = s[i:]
	}
	return scheme, host, s
}
func CanonicalURL(s string) string {
	_, host, path := splitURL(s)
	host = strings.TrimPrefix(pythonLower(host), "www.")
	path = regexp.MustCompile(`/+`).ReplaceAllString(path, "/")
	path = strings.TrimRight(path, "/")
	if path == "" {
		path = "/"
	}
	if host == "" {
		if strings.HasPrefix(path, "/") {
			return "https://" + path
		}
		return "https:" + path
	}
	base := "https://" + host + path
	// The observed DCInside desktop redirect identifies posts by id/no query
	// parameters. Preserve only this exact validated route; retain all legacy
	// query-stripping behavior for every other host/path or malformed identity.
	parsed, parseErr := url.Parse(s)
	if parseErr == nil && parsed.User == nil && (parsed.Scheme == "http" || parsed.Scheme == "https") && strings.EqualFold(parsed.Host, "gall.dcinside.com") && (parsed.Path == "/mgallery/board/view/" || parsed.Path == "/mgallery/board/view") {
		query, queryErr := url.ParseQuery(parsed.RawQuery)
		if queryErr == nil && len(query["id"]) == 1 && query.Get("id") == "ai_utilize" && len(query["no"]) == 1 && regexp.MustCompile(`^[0-9]+$`).MatchString(query.Get("no")) {
			return "https://gall.dcinside.com/mgallery/board/view/?id=ai_utilize&no=" + query.Get("no")
		}
	}
	return base
}
func Scan(ctx context.Context, backend Backend, state *State) (report Report, err error) {
	return ScanWithOptions(ctx, backend, state, LegacyScanOptions())
}

// ScanWithOptions widens the bounded review queue without deciding acceptance.
// Scan preserves the original profile for callers and differential fixtures.
func ScanWithOptions(ctx context.Context, backend Backend, state *State, options ScanOptions) (report Report, err error) {
	if err = options.Validate(); err != nil {
		return report, err
	}
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("collector aborted without saving state: %v", p)
		}
	}()
	report = Report{Candidates: []Candidate{}, FailedSources: []Failure{}}
	diagnostic := &CollectionDiagnostic{Options: options, ReviewOnly: true}
	var reviewHistory map[string]uint64
	var reviewSequence uint64
	if options.Profile != "legacy" {
		report.Collection = diagnostic
		reviewHistory, reviewSequence, err = loadWideReviewHistory(state)
		if err != nil {
			return report, err
		}
	}
	old := map[string]string{}
	for k, v := range state.Fingerprints {
		old[k] = v
	}
	unique := map[string]Row{}
	uniqueOrder := []string{}
	for _, source := range Sources {
		if e := ctx.Err(); e != nil {
			return report, e
		}
		rows, failure, e := backend.Listing(ctx, source)
		if e != nil {
			return report, e
		}
		if failure != "" {
			report.FailedSources = append(report.FailedSources, Failure{source.Name, Redact(failure)})
			continue
		}
		for i, row := range rows {
			if i >= options.MaxScanPerSource {
				break
			}
			diagnostic.RowsScanned++
			if options.Profile == "wide" {
				parsedURL, parseErr := url.Parse(row.URL)
				if parseErr != nil || parsedURL.User != nil || parsedURL.Host == "" || (parsedURL.Scheme != "https" && parsedURL.Scheme != "http") || Redact(row.URL) != row.URL {
					continue
				}
			}
			key := CanonicalURL(row.URL)
			if _, ok := unique[key]; ok {
				continue
			}
			excerpt := StripPoison(row.ListingExcerpt)
			if !options.worth(row.Title, excerpt, source.Name, key) {
				continue
			}
			row.URL = key
			row.ListingExcerpt = excerpt
			row.Rank = RankScore(row.Title, excerpt, source.Name, key)
			unique[key] = row
			uniqueOrder = append(uniqueOrder, key)
		}
	}
	diagnostic.EligibleUnique = len(unique)
	ranked := []Row{}
	for _, key := range uniqueOrder {
		ranked = append(ranked, unique[key])
	}
	sort.SliceStable(ranked, func(i, j int) bool {
		a, b := ranked[i], ranked[j]
		if options.Profile == "wide" {
			_, seenA := old[a.URL]
			_, seenB := old[b.URL]
			if seenA != seenB {
				return !seenA
			}
			if reviewHistory[a.URL] != reviewHistory[b.URL] {
				return reviewHistory[a.URL] < reviewHistory[b.URL]
			}
		}
		if a.Rank != b.Rank {
			return a.Rank > b.Rank
		}
		if a.Source != b.Source {
			return a.Source < b.Source
		}
		return a.Title < b.Title
	})
	perSource := map[string]int{}
	selected := []Row{}
	for _, r := range ranked {
		if perSource[r.Source] >= options.MaxPerSource {
			continue
		}
		perSource[r.Source]++
		selected = append(selected, r)
		if len(selected) >= options.MaxTotal {
			break
		}
	}
	diagnostic.DetailsSelected = len(selected)
	for _, row := range selected {
		if e := ctx.Err(); e != nil {
			return report, e
		}
		key := row.URL
		body, detailError, e := backend.Body(ctx, row)
		if e != nil {
			return report, e
		}
		if options.Profile == "wide" {
			reviewSequence++
			reviewHistory[key] = reviewSequence
		}
		excerpt := row.ListingExcerpt
		gated := search("TRUST_GATE_RE", body) || search("TRUST_GATE_RE", excerpt)
		if search("TRUST_GATE_RE", body) {
			body = ""
			if detailError == "" {
				detailError = "trust_level_gated"
			}
		}
		usable := body
		if gated {
			usable = ""
		}
		independentlyOK := options.rejected(row.Title, usable, key) == "" && options.worth(row.Title, usable, row.Source, key)
		if gated {
			// Wider lexical hints must not enlarge the legacy title-only
			// exception when the supporting content is inaccessible.
			independentlyOK = WorthCollecting(row.Title, "", row.Source, key) && ListingRejected(row.Title, "", key) == "" && !regexp.MustCompile(`合集|汇总|最值钱`).MatchString(row.Title)
			usable = ""
		}
		if !independentlyOK {
			fingerprintPart := excerpt
			if fingerprintPart == "" {
				fingerprintPart = runeSlice(body, 4000)
			}
			state.Set(key, Fingerprint(row.Title+" "+fingerprintPart))
			continue
		}
		combined := row.Title + " " + usable
		ai, strong, weak := Matches(combined)
		if options.Profile == "wide" {
			ai, strong, weak = ReviewMatches(combined)
		}
		deals := append(append([]string{}, strong...), weak...)
		if len(deals) > 16 {
			deals = deals[:16]
		}
		fingerprintPart := excerpt
		if fingerprintPart == "" {
			fingerprintPart = runeSlice(usable, 4000)
		}
		fingerprintInput := row.Title + " " + fingerprintPart
		broadened := options.Profile == "wide" && !WorthCollecting(row.Title, usable, row.Source, key)
		if broadened {
			// Legacy records include rejected detail rows. Reopen only newly
			// reviewable rows once; keep old emitted offers deduplicated.
			fingerprintInput = "wide:v1 " + fingerprintInput
		}
		fingerprint := Fingerprint(fingerprintInput)
		change := "new"
		if _, ok := old[key]; ok {
			change = "updated"
		}
		state.Set(key, fingerprint)
		if old[key] == fingerprint {
			diagnostic.Unchanged++
			continue
		}
		quality := []string{}
		if FirstParty(key) {
			quality = append(quality, "official_vendor")
		} else if MentionsVendor(combined) {
			quality = append(quality, "vendor_mentioned")
		}
		if search("CONCRETE_BENEFIT_RE", combined) || (search("CONCRETE_NUMERIC_RE", combined) && (len(strong) > 0 || len(weak) > 0)) {
			quality = append(quality, "concrete_value")
		}
		if LikelyRelay(combined) {
			quality = append(quality, "likely_relay")
		}
		if options.Profile == "wide" {
			quality = append(quality, "assistant_review_required")
			if reason := ListingRejected(row.Title, usable, key); reason != "" {
				quality = append(quality, "review_"+reason)
			}
			if broadened {
				quality = append(quality, "broadened_recall")
			}
		}
		if len(ai) > 12 {
			ai = ai[:12]
		}
		var errorPtr *string
		if detailError != "" {
			detailError = Redact(detailError)
			errorPtr = &detailError
		}
		report.Candidates = append(report.Candidates, Candidate{row.Source, Redact(row.Title), row.URL, change, row.Rank, quality, ai, deals, Redact(runeSlice(usable, 3000)), errorPtr})
	}
	state.Trim(MaxFingerprints)
	if options.Profile == "wide" {
		if err = saveWideReviewHistory(state, reviewHistory); err != nil {
			return report, err
		}
	}
	if d, ok := backend.(interface{ Diagnostics() []SourceDiagnostic }); ok {
		report.SourceDiagnostics = d.Diagnostics()
	}
	return report, nil
}

type Fixture struct {
	Listings map[string]struct {
		Rows  []Row  `json:"rows"`
		Error string `json:"error"`
	} `json:"listings"`
	Bodies map[string]struct {
		Text  string `json:"text"`
		Error string `json:"error"`
	} `json:"bodies"`
}

func ParseFixture(data []byte) (*Fixture, error) {
	var f Fixture
	e := json.Unmarshal(data, &f)
	return &f, e
}
func (f *Fixture) Listing(_ context.Context, s Source) ([]Row, string, error) {
	v := f.Listings[s.Name]
	for i := range v.Rows {
		v.Rows[i].Source = s.Name
	}
	return v.Rows, v.Error, nil
}
func (f *Fixture) Body(_ context.Context, r Row) (string, string, error) {
	v := f.Bodies[r.URL]
	return v.Text, v.Error, nil
}
