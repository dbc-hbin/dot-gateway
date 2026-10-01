package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"insane-search-migration/internal/extract"
)

type Fetcher struct {
	Learning *LearningStore
	HTTP     HTTPExecutor
	Browser  Browser
	Sleep    func(context.Context, time.Duration) error
	mu       sync.Mutex
	learned  map[string]Candidate
}

func NewFetcher(browser Browser) *Fetcher {
	return &Fetcher{HTTP: NewTransport(), Browser: browser, learned: map[string]Candidate{}}
}
func (f *Fetcher) sleep(ctx context.Context, d time.Duration) error {
	if f.Sleep != nil {
		return f.Sleep(ctx, d)
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
func baseResult(raw string) Result {
	return Result{FinalURL: raw, Trace: []Attempt{}, UntriedRoutes: []string{}, PromptInjectionSignals: []string{}, UntrustedContentBoundary: map[string]string{}, ExtractionMeta: map[string]any{}, Verdict: "unknown"}
}
func (f *Fetcher) Fetch(ctx context.Context, request Request) (result Result, err error) {
	result = baseResult(request.URL)
	defer func() { AttachSafety(&result) }()
	if request.Timeout <= 0 {
		request.Timeout = 25 * time.Second
	}
	if f.HTTP == nil {
		f.HTTP = NewTransport()
	}
	if request.MaxAttempts < 0 || request.MaxBrowserAttempts < 0 {
		return result, fmt.Errorf("negative attempt budget")
	}
	if request.URL == "" {
		return result, fmt.Errorf("URL is required")
	}
	parsed, e := url.Parse(request.URL)
	if e != nil || parsed.User != nil || !(parsed.Scheme == "http" || parsed.Scheme == "https") {
		return result, fmt.Errorf("unsafe URL scheme, credentials, or parse failure")
	}
	if request.EnablePhase0 {
		done, e := f.phase0(ctx, request, &result)
		if e != nil {
			return result, e
		}
		if done {
			return result, nil
		}
	}
	baseTLS := "safari"
	if request.DeviceClass == "mobile" {
		baseTLS = "safari_ios"
	}
	baseRef := "self_root"
	f.mu.Lock()
	learned, hasLearned := f.learned[learningKey(request.URL, request.DeviceClass)]
	f.mu.Unlock()
	if f.Learning != nil {
		if route, ok, learningErr := f.Learning.Lookup(request.URL, request.DeviceClass); learningErr == nil && ok {
			learned = Candidate{Transform: route.Transform, TLS: route.Impersonate, Referer: route.Referer}
			hasLearned = true
		}
	}
	if hasLearned {
		baseTLS, baseRef = learned.TLS, learned.Referer
	}
	defer func() {
		if f.Learning != nil && hasLearned && !result.OK {
			_ = f.Learning.Failure(request.URL, request.DeviceClass, result.StopReason)
		}
	}()
	if transport, ok := f.HTTP.(*Transport); ok {
		started := time.Now()
		if attempted, warmErr := transport.Warmup(ctx, request.URL, baseTLS, request.Timeout); attempted {
			wa := Attempt{Phase: "warmup", Executor: "utls_go", URL: parsed.Scheme + "://" + parsed.Host + "/", URLTransform: "original", Impersonate: &baseTLS, Referer: "none", Verdict: "unknown", Reasons: []string{"once_per_host_profile_cookie_warmup"}, ElapsedS: time.Since(started).Seconds()}
			if warmErr != nil {
				s := warmErr.Error()
				wa.Error = &s
			}
			result.Trace = append(result.Trace, wa)
		}
	}
	probe := Candidate{Transform: "original", URL: request.URL, TLS: baseTLS, Referer: baseRef}
	resp, att, e := f.attempt(ctx, request, probe, "probe", request.EnableRetry)
	result.Trace = append(result.Trace, att)
	result.ExecutedAttempts++
	if ctx.Err() != nil {
		return result, ctx.Err()
	}
	if e == nil && isOK(att.Verdict) {
		return f.success(result, request, resp, probe, "", nil)
	}
	if e == nil && isWall(att.Verdict) {
		result.StopReason = att.Verdict
		result.Verdict = att.Verdict
		result.Content = string(resp.Body)
		result.Summary = "Terminal public-content wall; no escalation"
		return result, nil
	}
	var safety *SafetyError
	dnsBlocked := errors.As(e, &safety) && safety.DNSUnresolved
	if errors.As(e, &safety) && !safety.DNSUnresolved {
		result.StopReason = "safety_blocked"
		result.Summary = e.Error()
		return result, nil
	}
	lastResp := resp
	lastVerdict := att.Verdict
	bestSuspect := HTTPResponse{}
	if att.Verdict == "suspect_ok" {
		bestSuspect = resp
	}
	hits := Detect(resp)
	profile := hits[0].Profile
	result.ProfileUsed = &profile
	plan := BuildPlan(request.URL, hits, request.DeviceClass, baseTLS, baseRef)
	if hasLearned {
		for i, c := range plan {
			if c.Transform == learned.Transform && c.TLS == learned.TLS && c.Referer == learned.Referer {
				copy(plan[1:i+1], plan[:i])
				plan[0] = c
				break
			}
		}
	}
	result.PlannedAttempts = len(plan)
	if dnsBlocked {
		result.StopReason = "dns_resolution_failed"
		result.UntriedRoutes = append(result.UntriedRoutes, "native HTTP grid requires verified public DNS; delegated to guarded headed browser")
	} else if att.Verdict == "rate_limited" {
		result.StopReason = "rate_limited"
	} else {
		for _, candidate := range plan {
			if request.MaxAttempts > 0 && result.ExecutedAttempts >= request.MaxAttempts {
				result.StopReason = "budget"
				break
			}
			resp, att, e = f.attempt(ctx, request, candidate, "grid", false)
			result.Trace = append(result.Trace, att)
			result.ExecutedAttempts++
			if ctx.Err() != nil {
				return result, ctx.Err()
			}
			if e == nil {
				lastResp, lastVerdict = resp, att.Verdict
				if isOK(att.Verdict) {
					return f.success(result, request, resp, candidate, "", nil)
				}
				if att.Verdict == "suspect_ok" && len(bestSuspect.Body) == 0 {
					bestSuspect = resp
				}
				if Terminal(att.Verdict) {
					result.StopReason = att.Verdict
					break
				}
			}
			if errors.As(e, &safety) {
				result.StopReason = "safety_blocked"
				break
			}
			if e = f.sleep(ctx, 150*time.Millisecond); e != nil {
				return result, e
			}
		}
		if result.StopReason == "" {
			result.GridExhausted = true
			result.StopReason = "exhausted"
		}
	}
	if request.EnableBrowser && !isWall(result.StopReason) && result.StopReason != "safety_blocked" {
		if f.Browser == nil {
			result.UntriedRoutes = append(result.UntriedRoutes, "headed browser unavailable")
		} else {
			for pass := 0; pass < request.MaxBrowserAttempts; pass++ {
				timeout := max(request.Timeout, 180*time.Second)
				started := time.Now()
				br, e := f.Browser.Fetch(ctx, BrowserRequest{URL: request.URL, Timeout: timeout, Pass: pass, Device: request.DeviceClass, WaitSelector: strings.Join(request.SuccessSelectors, ",")})
				ba := Attempt{Phase: "fallback", Executor: "playwright_headed_chromium", URL: br.URL, URLTransform: "original", Referer: "", Status: br.Status, BodySize: len(br.HTML), Verdict: "unknown", Reasons: []string{}, ElapsedS: time.Since(started).Seconds()}
				if ba.URL == "" {
					ba.URL = request.URL
				}
				if e != nil {
					s := e.Error()
					ba.Error = &s
					result.Trace = append(result.Trace, ba)
					if ctx.Err() != nil {
						return result, ctx.Err()
					}
					break
				}
				if br.Headless || !br.ChromiumSandbox {
					ba.Reasons = append(ba.Reasons, "browser_security_contract_failed")
					result.Trace = append(result.Trace, ba)
					result.StopReason = "browser_security_contract_failed"
					break
				}
				headers := make(http.Header)
				for k, v := range br.Headers {
					headers.Set(k, v)
				}
				bresp := HTTPResponse{URL: ba.URL, Status: br.Status, Body: []byte(br.HTML), Headers: headers}
				for _, c := range br.Cookies {
					bresp.Cookies = append(bresp.Cookies, &http.Cookie{Name: c.Name, Value: c.Value})
				}
				validation := validateRequestResponse(bresp, request, request.KnownBadSizes)
				ba.Verdict, ba.Reasons = validation.Verdict, validation.Reasons
				if br.TerminalReason != "" {
					ba.Reasons = append(ba.Reasons, br.TerminalReason)
					if isWall(br.TerminalReason) || strings.Contains(br.TerminalReason, "auth") || strings.Contains(br.TerminalReason, "login") {
						ba.Verdict = "auth_required"
					}
					if strings.Contains(br.TerminalReason, "paywall") {
						ba.Verdict = "paywall"
					}
					if br.TerminalReason == "access_unavailable" {
						ba.Verdict = "blocked"
					}
				}
				result.Trace = append(result.Trace, ba)
				lastResp, lastVerdict = bresp, ba.Verdict
				if isOK(ba.Verdict) {
					if transport, ok := f.HTTP.(*Transport); ok {
						_ = transport.InjectCookies(request.URL, "chrome", br.Cookies, br.UserAgent)
					}
					return f.success(result, request, bresp, Candidate{}, br.VisibleText, nil)
				}
				if isWall(ba.Verdict) {
					result.StopReason = ba.Verdict
					break
				}
				if strings.Contains(strings.Join(ba.Reasons, " "), "browser_control_required") {
					result.StopReason = "browser_control_required"
					break
				}
				if e = f.sleep(ctx, time.Second); e != nil {
					return result, e
				}
			}
		}
	}
	if len(bestSuspect.Body) > 0 {
		result.Content = string(bestSuspect.Body)
		result.Verdict = "suspect_ok"
	} else {
		result.Content = string(lastResp.Body)
		result.Verdict = lastVerdict
	}
	if result.Verdict == "" {
		result.Verdict = "unknown"
	}
	if isWall(result.StopReason) {
		result.UntriedRoutes = []string{}
		result.BlockClass = "infra_or_auth"
	} else if result.StopReason != "safety_blocked" && result.StopReason != "browser_security_contract_failed" {
		if result.StopReason == "budget" {
			result.UntriedRoutes = append(result.UntriedRoutes, "native TLS grid not exhausted; increase attempt budget")
		}
		if len(result.Trace) > 0 && result.Trace[len(result.Trace)-1].Executor == "playwright_headed_chromium" && result.Trace[len(result.Trace)-1].Error == nil {
			result.InteractiveBrowserRequired = true
			result.RecommendedTool = "playwright-headed"
			result.RecommendedAction = "Inspect the public page in the visible browser; stop at authentication/paywall boundaries"
			result.FallbackStage = "browser"
		}
		result.BlockClass = "bot_detection"
	}
	result.Summary = "Native fetch did not establish verified success: " + result.StopReason
	return result, nil
}
func isOK(v string) bool { return v == "strong_ok" || v == "weak_ok" }
func isWall(v string) bool {
	return v == "auth_required" || v == "not_found" || v == "paywall" || v == "login_required"
}
func (f *Fetcher) attempt(ctx context.Context, r Request, c Candidate, phase string, retry bool) (HTTPResponse, Attempt, error) {
	started := time.Now()
	imp := c.TLS
	att := Attempt{Phase: phase, Executor: "utls_go", URL: c.URL, URLTransform: c.Transform, Impersonate: &imp, Referer: c.Referer, Reasons: []string{"tls_client_hello=" + TLSProfileVersion(c.TLS)}}
	var response HTTPResponse
	var err error
	slept := time.Duration(0)
	for n := 0; ; n++ {
		response, err = f.HTTP.Do(ctx, HTTPRequest{URL: c.URL, Profile: c.TLS, Referer: refererURL(c.Referer, c.URL), Timeout: r.Timeout})
		if err != nil || !retry || n >= 2 || (response.Status != 429 && response.Status != 502 && response.Status != 503 && response.Status != 504) {
			break
		}
		delay := time.Duration(1500*(1<<n)) * time.Millisecond
		if value, e := strconv.ParseFloat(response.Headers.Get("Retry-After"), 64); e == nil && value >= 0 {
			delay = time.Duration(value * float64(time.Second))
		}
		delay = min(delay, 10*time.Second-slept)
		if delay <= 0 {
			break
		}
		if err = f.sleep(ctx, delay); err != nil {
			break
		}
		slept += delay
		att.Reasons = append(att.Reasons, fmt.Sprintf("probe_retry:%d", n+1))
	}
	att.ElapsedS = time.Since(started).Seconds()
	if err != nil {
		s := err.Error()
		att.Error = &s
		att.Verdict = "unknown"
		return response, att, err
	}
	v := validateRequestResponse(response, r, c.KnownBadSizes)
	att.Status, att.BodySize, att.Verdict = response.Status, len(response.Body), v.Verdict
	att.Reasons = append(att.Reasons, v.Reasons...)
	return response, att, nil
}
func (f *Fetcher) success(result Result, r Request, response HTTPResponse, c Candidate, inner string, _ error) (Result, error) {
	out, e := extract.Process(response.URL, string(response.Body), extract.Options{Enabled: r.EnableExtraction, Markdown: r.EnableMarkdown, MainContent: r.EnableMainContent, InnerText: inner, ContentType: response.Headers.Get("Content-Type")})
	if e != nil {
		return result, e
	}
	result.OK = true
	result.Content = out.Content
	result.FinalURL = response.URL
	result.Verdict = result.Trace[len(result.Trace)-1].Verdict
	result.StopReason = "success"
	result.Summary = "Native Go fetch succeeded"
	result.ExtractionSource = out.Source
	result.ExtractionQuality = out.Quality
	result.ExtractionMeta = out.Meta
	if c.TLS != "" {
		if f.Learning != nil {
			phase := "probe"
			if len(result.Trace) > 0 {
				phase = result.Trace[len(result.Trace)-1].Phase
			}
			_ = f.Learning.Success(r.URL, r.DeviceClass, LearnedRoute{Transform: c.Transform, Impersonate: c.TLS, Referer: c.Referer, Phase: phase})
		}
		f.mu.Lock()
		if f.learned == nil {
			f.learned = map[string]Candidate{}
		}
		if len(f.learned) >= 2048 {
			for k := range f.learned {
				delete(f.learned, k)
				break
			}
		}
		f.learned[learningKey(r.URL, r.DeviceClass)] = c
		f.mu.Unlock()
	}
	return result, nil
}

func validateRequestResponse(response HTTPResponse, request Request, knownBad []int) Validation {
	result := Validate(response, request.SuccessSelectors, knownBad)
	if request.AcceptEmptyJSONArray && response.Status >= 200 && response.Status < 300 && !isWall(result.Verdict) {
		var values []any
		raw := extract.UnwrapJSON(string(response.Body))
		if json.Unmarshal([]byte(raw), &values) == nil && values != nil && len(values) == 0 {
			result.Verdict = "weak_ok"
			result.Reasons = append(result.Reasons, "explicit_empty_listing_array")
		}
	}
	return result
}
