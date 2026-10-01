package engine

import (
	_ "embed"
	"encoding/json"
	"net/url"
	"path"
	"sort"
	"strings"
)

//go:embed profiles_data.json
var profileJSON []byte

type Profile struct {
	Detectors struct {
		Cookie, Header []string
		ServerContains []string `json:"server_contains"`
		Body           []string
	}
	Confidence    struct{ Strong, Weak int } `json:"confidence_rules"`
	TLSCandidates [][]string                 `json:"tls_impersonate_candidates"`
	TLSAvoid      []string                   `json:"tls_impersonate_avoid"`
	Referers      []string                   `json:"referer_strategies"`
	Transforms    []string                   `json:"url_transform_order"`
	KnownBadSizes []int                      `json:"known_bad_sizes"`
}

var profiles map[string]Profile
var profileOrder = []string{"akamai_bot_manager", "cloudflare_turnstile", "f5_big_ip", "aws_waf", "datadome_probable", "perimeterx_human", "kasada_ips", "imperva_incapsula", "unknown_challenge"}

type Detection struct {
	Profile    string
	Confidence float64
	Signals    []string
}
type Candidate struct {
	Profile, Transform, URL, TLS, Referer string
	KnownBadSizes                         []int
}

func init() {
	if e := json.Unmarshal(profileJSON, &profiles); e != nil {
		panic(e)
	}
}
func Detect(resp HTTPResponse) []Detection {
	hits := []Detection{}
	body := strings.ToLower(string(resp.Body))
	for _, name := range profileOrder {
		p := profiles[name]
		signals := []string{}
		for _, pattern := range p.Detectors.Cookie {
			for _, c := range resp.Cookies {
				ok, _ := path.Match(strings.ToLower(pattern), strings.ToLower(c.Name))
				if ok {
					signals = append(signals, "cookie:"+pattern)
					break
				}
			}
		}
		for _, pattern := range p.Detectors.Header {
			for key := range resp.Headers {
				ok, _ := path.Match(strings.ToLower(pattern), strings.ToLower(key))
				if ok {
					signals = append(signals, "header:"+pattern)
					break
				}
			}
		}
		for _, s := range p.Detectors.ServerContains {
			if strings.Contains(strings.ToLower(resp.Headers.Get("Server")), strings.ToLower(s)) {
				signals = append(signals, "server:"+s)
			}
		}
		for _, s := range p.Detectors.Body {
			if strings.Contains(body, strings.ToLower(s)) {
				signals = append(signals, "body:"+s)
			}
		}
		if len(signals) == 0 {
			continue
		}
		strong, weak := p.Confidence.Strong, p.Confidence.Weak
		if strong == 0 {
			strong = 2
		}
		if weak == 0 {
			weak = 1
		}
		confidence := 0.3
		if len(signals) >= strong {
			confidence = 0.9
		} else if len(signals) >= weak {
			confidence = 0.6
		}
		hits = append(hits, Detection{name, confidence, signals})
	}
	sort.SliceStable(hits, func(i, j int) bool { return hits[i].Confidence > hits[j].Confidence })
	if len(hits) == 0 {
		hits = append(hits, Detection{"unknown_challenge", 0.1, []string{"fallback"}})
	}
	return hits
}
func TransformURL(name, raw string) string {
	u, e := url.Parse(raw)
	if e != nil {
		return ""
	}
	host := u.Hostname()
	port := u.Port()
	replace := func(h string) string {
		if port != "" {
			u.Host = h + ":" + port
		} else {
			u.Host = h
		}
		return u.String()
	}
	switch name {
	case "original":
		return raw
	case "mobile_subdomain":
		if strings.HasPrefix(host, "www.") {
			return replace("m." + host[4:])
		}
	case "am_prefix":
		if host != "" && !strings.HasPrefix(host, "m.") && !strings.HasPrefix(host, "www.") && strings.Count(host, ".") < 2 {
			return replace("m." + host)
		}
	case "drop_www":
		if strings.HasPrefix(host, "www.") {
			return replace(host[4:])
		}
	}
	return ""
}
func family(name string) string {
	if strings.Contains(name, "ios") {
		return "safari_ios"
	}
	if strings.Contains(name, "android") {
		return "chrome_android"
	}
	for _, f := range []string{"safari_ios", "safari", "chrome_android", "chrome", "edge", "firefox"} {
		if strings.HasPrefix(name, f) {
			return f
		}
	}
	return name
}
func mobile(name string) bool {
	return strings.Contains(name, "ios") || strings.Contains(name, "android")
}
func planProfile(raw, name, device string) []Candidate {
	p := profiles[name]
	groups := [][]string{}
	for _, group := range p.TLSCandidates {
		native := []string{}
		seen := map[string]bool{}
		for _, candidate := range group {
			f := family(candidate)
			if _, ok := tlsProfiles[f]; !ok || seen[f] {
				continue
			}
			if device == "mobile" && !mobile(f) || device == "desktop" && mobile(f) {
				continue
			}
			seen[f] = true
			native = append(native, f)
		}
		if len(native) > 0 {
			groups = append(groups, native)
		}
	}
	if len(groups) == 0 {
		groups = [][]string{{"safari", "chrome"}}
	}
	refs := p.Referers
	if len(refs) == 0 {
		refs = []string{"self_root"}
	}
	transforms := append([]string{}, p.Transforms...)
	if len(transforms) == 0 {
		transforms = []string{"original"}
	}
	if device == "mobile" {
		transforms = append(transforms, "mobile_subdomain", "am_prefix")
	}
	pairs := [][2]string{}
	seenURL := map[string]bool{}
	for _, name := range transforms {
		if device == "desktop" && (name == "mobile_subdomain" || name == "am_prefix") {
			continue
		}
		u := TransformURL(name, raw)
		if u != "" && !seenURL[u] {
			pairs = append(pairs, [2]string{name, u})
			seenURL[u] = true
		}
	}
	if len(pairs) == 0 {
		pairs = append(pairs, [2]string{"original", raw})
	}
	depthMax := 0
	for _, g := range groups {
		depthMax = max(depthMax, len(g))
	}
	out := []Candidate{}
	seen := map[string]bool{}
	for _, ref := range refs {
		for depth := 0; depth < depthMax; depth++ {
			for _, pair := range pairs {
				for _, g := range groups {
					if depth >= len(g) {
						continue
					}
					key := pair[1] + "\x00" + g[depth] + "\x00" + ref
					if seen[key] {
						continue
					}
					seen[key] = true
					out = append(out, Candidate{name, pair[0], pair[1], g[depth], ref, p.KnownBadSizes})
				}
			}
		}
	}
	return out
}
func BuildPlan(raw string, hits []Detection, device, probeTLS, probeReferer string) []Candidate {
	plans := [][]Candidate{}
	for _, h := range hits[:min(3, len(hits))] {
		plans = append(plans, planProfile(raw, h.Profile, device))
	}
	out := []Candidate{}
	seen := map[string]bool{raw + "\x00" + probeTLS + "\x00" + probeReferer: true}
	for i := 0; ; i++ {
		any := false
		for _, p := range plans {
			if i >= len(p) {
				continue
			}
			any = true
			c := p[i]
			key := c.URL + "\x00" + c.TLS + "\x00" + c.Referer
			if !seen[key] {
				seen[key] = true
				out = append(out, c)
			}
		}
		if !any {
			break
		}
	}
	return out
}
func refererURL(name, raw string) string {
	switch name {
	case "google_search":
		return "https://www.google.com/"
	case "self_root":
		u, e := url.Parse(raw)
		if e == nil {
			return u.Scheme + "://" + u.Host + "/"
		}
	}
	return ""
}
