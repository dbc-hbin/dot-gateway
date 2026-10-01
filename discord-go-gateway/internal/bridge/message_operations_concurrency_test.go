package bridge

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"
)

// Two separately launched CLI clients may both resume the same prepared row.
// A losing client's preflight failure must not erase the winner's lost ACK.
func TestReviewConcurrentPreparedPreflightCannotEraseUncertainty(t *testing.T) {
	f := newOperationFixture(t)
	f.lose = true
	spec := f.spec("edit_text", "resume-shared")
	f.rest.sendMu.Lock()
	type result struct {
		o   MessageOperation
		err error
	}
	first := make(chan result, 1)
	go func() { o, e := f.execute(spec); first <- result{o, e} }()
	id := operationID(f.claim.InboundID, spec.Key)
	deadline := time.Now().Add(3 * time.Second)
	for {
		if o, e := f.store.MessageOperation(id); e == nil && o.State == "prepared" {
			break
		}
		if time.Now().After(deadline) {
			f.rest.sendMu.Unlock()
			t.Fatal("not prepared")
		}
		time.Sleep(time.Millisecond)
	}
	secondREST, e := NewRESTClient(f.rest.settings)
	if e != nil {
		f.rest.sendMu.Unlock()
		t.Fatal(e)
	}
	defer secondREST.Close()
	secondREST.baseURL = f.rest.baseURL
	waiting, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	base := http.DefaultTransport.(*http.Transport).Clone()
	defer base.CloseIdleConnections()
	secondREST.client.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.Method == "GET" && req.URL.Path == "/channels/2/messages/9" {
			once.Do(func() { close(waiting); <-release })
		}
		return base.RoundTrip(req)
	})
	second := make(chan result, 1)
	go func() {
		o, e := secondREST.ExecuteMessageOperation(context.Background(), f.store, f.claim.InboundID, f.claim.Claim, spec)
		second <- result{o, e}
	}()
	select {
	case <-waiting:
	case <-time.After(3 * time.Second):
		f.rest.sendMu.Unlock()
		close(release)
		t.Fatal("second client didn't enter prepared preflight")
	}
	f.rest.sendMu.Unlock()
	var winner result
	select {
	case winner = <-first:
	case <-time.After(3 * time.Second):
		close(release)
		t.Fatal("winner didn't finish")
	}
	if winner.err != nil || winner.o.State != "uncertain" {
		close(release)
		t.Fatal("winner", winner)
	}
	close(release)
	var loser result
	select {
	case loser = <-second:
	case <-time.After(3 * time.Second):
		t.Fatal("loser didn't finish")
	}
	final, e := f.store.MessageOperation(id)
	if e != nil || final.State != "uncertain" || final.Attempts != 1 {
		t.Fatalf("losing preflight erased real lost-ACK: final=%+v err=%v loser=%+v", final, e, loser)
	}
}
