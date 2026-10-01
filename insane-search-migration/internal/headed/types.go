// Package headed implements a private native desktop browser client and launcher.
// Its Go client uses the verified Node/Playwright headed service; no Python is used.
package headed

import "encoding/json"

const Backend = "playwright_headed_chromium"
const MaxRequestBytes = 32 << 10
const MaxResponseBytes = 12 << 20
const MaxHTMLBytes = 8 << 20

type Request struct {
	URL          string `json:"url"`
	TimeoutMS    int64  `json:"timeout,omitempty"`
	Device       string `json:"device,omitempty"`
	WaitSelector string `json:"waitSelector,omitempty"`
}
type Cookie struct {
	Name     string  `json:"name"`
	Value    string  `json:"value"`
	Domain   string  `json:"domain"`
	Path     string  `json:"path,omitempty"`
	Expires  float64 `json:"expires,omitempty"`
	HTTPOnly bool    `json:"httpOnly,omitempty"`
	Secure   bool    `json:"secure,omitempty"`
	SameSite string  `json:"sameSite,omitempty"`
}
type Challenge struct {
	InitialMarkers []string `json:"initialMarkers"`
	FinalMarkers   []string `json:"finalMarkers"`
	Resolved       bool     `json:"resolved"`
	WaitedMS       int64    `json:"waitedMs"`
	DOMStable      bool     `json:"domStable"`
	CaptchaClicks  int      `json:"captchaClicks"`
}
type Envelope struct {
	HTML            string            `json:"html"`
	FinalURL        string            `json:"finalUrl"`
	Status          int               `json:"status"`
	Headers         map[string]string `json:"headers,omitempty"`
	Cookies         []Cookie          `json:"cookies"`
	UserAgent       string            `json:"userAgent"`
	Automation      string            `json:"automation"`
	InnerText       string            `json:"innerText"`
	Challenge       Challenge         `json:"challenge"`
	Observations    []json.RawMessage `json:"observations"`
	TerminalReason  string            `json:"terminalReason,omitempty"`
	Headless        bool              `json:"headless"`
	ChromiumSandbox bool              `json:"chromiumSandbox"`
	BrowserVersion  string            `json:"browserVersion"`
}
