package headed

import (
	"context"
	"encoding/json"
	"insane-search-migration/internal/engine"
)

var _ engine.Browser = Client{}

func (c Client) Fetch(ctx context.Context, req engine.BrowserRequest) (engine.BrowserResult, error) {
	env, err := c.Capture(ctx, Request{URL: req.URL, TimeoutMS: req.Timeout.Milliseconds(), Device: req.Device, WaitSelector: req.WaitSelector})
	if err != nil {
		return engine.BrowserResult{Error: sanitizeError(err)}, err
	}
	result := engine.BrowserResult{URL: env.FinalURL, HTML: env.HTML, VisibleText: env.InnerText, Status: env.Status, Headers: env.Headers,
		Automation: env.Automation, TerminalReason: env.TerminalReason, UserAgent: env.UserAgent, Headless: env.Headless, ChromiumSandbox: env.ChromiumSandbox, BrowserVersion: env.BrowserVersion}
	for _, cookie := range env.Cookies {
		result.Cookies = append(result.Cookies, engine.Cookie{Name: cookie.Name, Value: cookie.Value, Domain: cookie.Domain, Path: cookie.Path, ExpiresUnix: int64(cookie.Expires), Secure: cookie.Secure, HTTPOnly: cookie.HTTPOnly, SameSite: cookie.SameSite})
	}
	for _, observation := range env.Observations {
		var s string
		if json.Unmarshal(observation, &s) != nil {
			s = string(observation)
		}
		result.Observations = append(result.Observations, s)
	}
	return result, nil
}
