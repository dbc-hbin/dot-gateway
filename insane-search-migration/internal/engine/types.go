package engine

import (
	"context"
	"net/http"
	"time"
)

type Request struct {
	URL                                                                                           string
	Timeout                                                                                       time.Duration
	MaxAttempts, MaxBrowserAttempts                                                               int
	DeviceClass                                                                                   string
	SuccessSelectors                                                                              []string
	AcceptEmptyJSONArray                                                                          bool
	KnownBadSizes                                                                                 []int
	EnableBrowser, EnablePhase0, EnableExtraction, EnableRetry, EnableMarkdown, EnableMainContent bool
}

func DefaultRequest(url string) Request {
	return Request{URL: url, Timeout: 25 * time.Second, MaxBrowserAttempts: 3, DeviceClass: "auto", EnableBrowser: true, EnablePhase0: true, EnableExtraction: true, EnableRetry: true, EnableMarkdown: true}
}

type Attempt struct {
	Phase        string   `json:"phase"`
	Executor     string   `json:"executor"`
	URL          string   `json:"url"`
	URLTransform string   `json:"url_transform"`
	Impersonate  *string  `json:"impersonate"`
	Referer      string   `json:"referer"`
	Status       int      `json:"status"`
	BodySize     int      `json:"body_size"`
	Verdict      string   `json:"verdict"`
	Reasons      []string `json:"reasons"`
	ElapsedS     float64  `json:"elapsed_s"`
	Error        *string  `json:"error"`
}
type Result struct {
	OK                         bool              `json:"ok"`
	Content                    string            `json:"-"`
	FinalURL                   string            `json:"final_url"`
	Verdict                    string            `json:"verdict"`
	ProfileUsed                *string           `json:"profile_used"`
	Trace                      []Attempt         `json:"trace"`
	Summary                    string            `json:"summary"`
	ContentLength              int               `json:"content_length"`
	PlannedAttempts            int               `json:"planned_attempts"`
	ExecutedAttempts           int               `json:"executed_attempts"`
	GridExhausted              bool              `json:"grid_exhausted"`
	StopReason                 string            `json:"stop_reason"`
	UntriedRoutes              []string          `json:"untried_routes"`
	InteractiveBrowserRequired bool              `json:"interactive_browser_required"`
	MustInvokePlaywrightMCP    bool              `json:"must_invoke_playwright_mcp"`
	RecommendedTool            string            `json:"recommended_tool,omitempty"`
	RecommendedAction          string            `json:"recommended_action,omitempty"`
	FallbackStage              string            `json:"fallback_stage,omitempty"`
	ContentTrust               string            `json:"content_trust"`
	PromptInjectionRisk        string            `json:"prompt_injection_risk"`
	PromptInjectionSignals     []string          `json:"prompt_injection_signals"`
	UntrustedContentBoundary   map[string]string `json:"untrusted_content_boundary"`
	ExtractionQuality          float64           `json:"extraction_quality"`
	ExtractionSource           string            `json:"extraction_source"`
	ExtractionMeta             map[string]any    `json:"extraction_meta"`
	BlockClass                 string            `json:"block_class"`
}

type HTTPRequest struct {
	URL, Profile, Referer string
	Headers               http.Header
	Timeout               time.Duration
}
type HTTPResponse struct {
	URL     string
	Status  int
	Body    []byte
	Headers http.Header
	Cookies []*http.Cookie
}
type HTTPExecutor interface {
	Do(context.Context, HTTPRequest) (HTTPResponse, error)
}

type Cookie struct {
	Name, Value, Domain, Path string
	ExpiresUnix               int64
	Secure, HTTPOnly          bool
	SameSite                  string
}
type BrowserRequest struct {
	URL                  string
	Timeout              time.Duration
	Pass                 int
	Device, WaitSelector string
}
type BrowserResult struct {
	URL, HTML, VisibleText                string
	Status                                int
	Headers                               map[string]string
	Cookies                               []Cookie
	Error                                 string
	Automation, TerminalReason, UserAgent string
	Headless, ChromiumSandbox             bool
	Observations                          []string
	BrowserVersion                        string
}
type Browser interface {
	Fetch(context.Context, BrowserRequest) (BrowserResult, error)
}

type Validation struct {
	Verdict          string   `json:"verdict"`
	Reasons          []string `json:"reasons"`
	MatchedSelectors []string `json:"matched_selectors"`
	BodySize         int      `json:"body_size"`
	Status           int      `json:"status"`
}

func (v Validation) OK() bool { return v.Verdict == "strong_ok" || v.Verdict == "weak_ok" }
func Terminal(verdict string) bool {
	return verdict == "auth_required" || verdict == "not_found" || verdict == "rate_limited" || verdict == "paywall" || verdict == "login_required"
}
