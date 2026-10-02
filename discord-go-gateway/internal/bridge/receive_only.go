package bridge

import (
	"errors"
	"net/http"
)

var errReceiveOnly = errors.New("receive_only_write_blocked")

func readOnlyHTTPMethod(method string) bool {
	return method == "" || method == http.MethodGet || method == http.MethodHead
}

// The final transport fence also covers SDK discovery clients, interaction
// callbacks, diagnostics and any future HTTP writer sharing this client.
// WebSocket identify/heartbeat frames are intentionally outside this fence.
type receiveOnlyTransport struct{ next http.RoundTripper }

func (t *receiveOnlyTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if !readOnlyHTTPMethod(req.Method) {
		if req.Body != nil {
			_ = req.Body.Close()
		}
		return nil, errReceiveOnly
	}
	return t.next.RoundTrip(req)
}
func (t *receiveOnlyTransport) CloseIdleConnections() {
	if closer, ok := t.next.(interface{ CloseIdleConnections() }); ok {
		closer.CloseIdleConnections()
	}
}

// Gate before a sender loop starts, so queued chunks/diagnostics are never
// dequeued or marked attempted merely because delivery is disabled.
func startGatewayWriter(receiveOnly bool, start func(func()), fn func()) {
	if !receiveOnly {
		start(fn)
	}
}
