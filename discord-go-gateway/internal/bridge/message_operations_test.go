package bridge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type operationFixture struct {
	store      *Store
	rest       *RESTClient
	claim      *Claim
	chunk      Chunk
	mu         sync.Mutex
	target     map[string]any
	writes     atomic.Int32
	fail       int
	lose       bool
	afterWrite func()
}

func newOperationFixture(t *testing.T) *operationFixture {
	t.Helper()
	s, _, _ := ingressFixture(t)
	e := testEnvelope()
	if out, err := s.Ingest(e); out != "accepted" || err != nil {
		t.Fatal(out, err)
	}
	c, err := s.ClaimNext(300, 0)
	if err != nil {
		t.Fatal(err)
	}
	reply, err := s.QueueReply(c.InboundID, c.Claim, "original answer")
	if err != nil {
		t.Fatal(err)
	}
	chunk, err := s.NextChunk()
	if err != nil || chunk == nil {
		t.Fatal(chunk, err)
	}
	if err = s.RecordResult(*chunk, SendResult{State: "sent", MessageID: "9"}); err != nil {
		t.Fatal(err)
	}
	if reply != chunk.ReplyID {
		t.Fatal("wrong reply")
	}
	request := testEnvelope()
	request.EventID = "10"
	request.Text = "Edit exactly message 9 in channel 2"
	if out, err := s.Ingest(request); out != "accepted" || err != nil {
		t.Fatal(out, err)
	}
	claim, err := s.ClaimNext(300, 0)
	if err != nil || claim == nil {
		t.Fatal(claim, err)
	}
	f := &operationFixture{store: s, claim: claim, chunk: *chunk, target: ackFor(*chunk)}
	f.target["type"] = 19
	f.target["flags"] = 4
	f.target["pinned"] = false
	f.target["mentions"] = []any{}
	f.target["attachments"] = []any{}
	f.target["embeds"] = []any{}
	f.target["reactions"] = []any{}
	f.rest = localREST(t, func(w http.ResponseWriter, q *http.Request) {
		if preflightHandler(w, q) {
			return
		}
		if q.Method == "GET" && q.URL.Path == "/channels/2/messages/10" {
			json.NewEncoder(w).Encode(map[string]any{"id": "10", "channel_id": "2", "author": User{ID: "1"}, "content": request.Text})
			return
		}
		if q.Method == "GET" && q.URL.Path == "/channels/2/messages/3" {
			json.NewEncoder(w).Encode(map[string]any{"id": "3", "channel_id": "2", "author": User{ID: "1"}, "content": e.Text})
			return
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		if q.Method == "GET" && q.URL.Path == "/channels/2/messages/9" {
			json.NewEncoder(w).Encode(f.target)
			return
		}
		f.writes.Add(1)
		if f.fail != 0 {
			w.WriteHeader(f.fail)
			return
		}
		switch {
		case q.Method == "PATCH" && q.URL.Path == "/channels/2/messages/9":
			var payload map[string]any
			if json.NewDecoder(q.Body).Decode(&payload) != nil {
				t.Error("bad payload")
			}
			if operationJSON(payload["allowed_mentions"]) != `{"parse":[],"replied_user":false,"roles":[],"users":[]}` {
				t.Error("unsafe mentions", payload)
			}
			if payload["flags"] != float64(4) {
				t.Error("flags lost")
			}
			f.target["content"] = payload["content"]
		case q.URL.Path == "/channels/2/messages/pins/9" && (q.Method == "PUT" || q.Method == "DELETE"):
			f.target["pinned"] = q.Method == "PUT"
		case strings.HasPrefix(q.URL.Path, "/channels/2/messages/9/reactions/") && strings.HasSuffix(q.URL.Path, "/@me") && (q.Method == "PUT" || q.Method == "DELETE"):
			emoji := strings.TrimSuffix(strings.TrimPrefix(q.URL.Path, "/channels/2/messages/9/reactions/"), "/@me")
			name, id, custom := strings.Cut(emoji, ":")
			em := map[string]any{"name": name}
			if custom {
				em["id"] = id
			}
			f.target["reactions"] = []any{map[string]any{"me": q.Method == "PUT", "emoji": em}}
		default:
			t.Error("unexpected mutation", q.Method, q.URL.Path)
			w.WriteHeader(500)
			return
		}
		if f.afterWrite != nil {
			f.afterWrite()
		}
		if f.lose {
			conn, _, err := w.(http.Hijacker).Hijack()
			if err == nil {
				conn.Close()
			}
			return
		}
		w.WriteHeader(204)
	})
	return f
}
func (f *operationFixture) spec(action, key string) MessageOperationSpec {
	s := MessageOperationSpec{Version: 1, Key: key, Action: action, ChannelID: "2", MessageID: "9"}
	if action == "edit_text" {
		s.Text = "revised answer"
	}
	if action == "add_reaction" || action == "remove_own_reaction" {
		s.Emoji = "👍"
	}
	return s
}
func (f *operationFixture) execute(spec MessageOperationSpec) (MessageOperation, error) {
	return f.rest.ExecuteMessageOperation(context.Background(), f.store, f.claim.InboundID, f.claim.Claim, spec)
}
func TestMessageOperationEditImmutableRevisionAndMemory(t *testing.T) {
	f := newOperationFixture(t)
	spec := f.spec("edit_text", "edit-1")
	out, err := f.execute(spec)
	if err != nil || out.State != "verified" || out.Attempts != 1 || out.Revision != 1 {
		t.Fatal(out, err)
	}
	original, receipt, err := f.store.ReplyReadback(f.chunk.ReplyID, 0)
	if err != nil || original.Text != "original answer" || receipt.MessageID != "9" {
		t.Fatal(original, receipt, err)
	}
	body, revision, err := f.store.CurrentEditedMessage("2", "9")
	if err != nil || body != spec.Text || revision != 1 {
		t.Fatal(body, revision, err)
	}
	recall, err := f.store.RecallMemory(f.claim.InboundID, f.claim.Claim, "")
	if err != nil || !memoryContains(recall, "revised answer") || memoryContains(recall, "original answer") {
		t.Fatal(recall, err)
	}
	again, err := f.execute(spec)
	if err != nil || again.ID != out.ID || f.writes.Load() != 1 {
		t.Fatal(again, err, f.writes.Load())
	}
	spec.Text = "must not overwrite immutable key"
	if _, err = f.execute(spec); err == nil {
		t.Fatal("key mutation accepted")
	}
	spec.Key = "edit-2"
	spec.Text = "second revision"
	out, err = f.execute(spec)
	if err != nil || out.Revision != 2 {
		t.Fatal(out, err)
	}
	var revisions int
	f.store.call(func(db *storeConn) (any, error) {
		return nil, db.QueryRow("SELECT count(*) FROM message_edit_revisions").Scan(&revisions)
	})
	if revisions != 3 {
		t.Fatal(revisions)
	}
}
func TestMessageOperationLostAckHoldAndExplicitReconcile(t *testing.T) {
	f := newOperationFixture(t)
	f.lose = true
	spec := f.spec("edit_text", "lost")
	out, err := f.execute(spec)
	if err != nil || out.State != "uncertain" || f.writes.Load() != 1 {
		t.Fatal(out, err)
	}
	if _, _, err = f.store.CurrentEditedMessage("2", "9"); err == nil {
		t.Fatal("uncertain edit trusted")
	}
	recall, err := f.store.RecallMemory(f.claim.InboundID, f.claim.Claim, "")
	if err != nil || memoryContains(recall, "revised answer") || memoryContains(recall, "original answer") {
		t.Fatal(recall, err)
	}
	out, err = f.execute(spec)
	if err != nil || out.State != "uncertain" || f.writes.Load() != 1 {
		t.Fatal(out, err)
	}
	next := f.spec("pin", "no-bypass")
	if _, err = f.execute(next); err == nil {
		t.Fatal("held target mutated")
	}
	if _, err = f.rest.ReconcileMessageOperation(context.Background(), f.store, f.claim.InboundID, f.claim.Claim, out.ID, false); err == nil {
		t.Fatal("implicit reconciliation")
	}
	out, err = f.rest.ReconcileMessageOperation(context.Background(), f.store, f.claim.InboundID, f.claim.Claim, out.ID, true)
	if err != nil || out.State != "verified" || out.Revision != 1 || f.writes.Load() != 1 {
		t.Fatal(out, err)
	}
}
func TestMessageOperationFailedEditRestoresProjection(t *testing.T) {
	for _, status := range []int{400, 401, 403, 404, 429} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			f := newOperationFixture(t)
			f.fail = status
			out, err := f.execute(f.spec("edit_text", "rejected"))
			if err != nil || out.State != "failed" {
				t.Fatal(out, err)
			}
			text, revision, err := f.store.CurrentEditedMessage("2", "9")
			if err != nil || text != "original answer" || revision != 0 {
				t.Fatal(text, revision, err)
			}
			recall, err := f.store.RecallMemory(f.claim.InboundID, f.claim.Claim, "")
			if err != nil || !memoryContains(recall, "original answer") || memoryContains(recall, "revised answer") {
				t.Fatal(recall, err)
			}
		})
	}
}
func TestMessageOperationPinAndOwnReactionsOnly(t *testing.T) {
	f := newOperationFixture(t)
	for i, action := range []string{"pin", "unpin", "add_reaction", "remove_own_reaction"} {
		out, err := f.execute(f.spec(action, fmt.Sprint(i)))
		if err != nil || out.State != "verified" {
			t.Fatal(out, err)
		}
	}
	if f.writes.Load() != 4 {
		t.Fatal(f.writes.Load())
	}
	for _, action := range []string{"delete", "delete_all_reactions", "join_thread", "unarchive", "change_permissions"} {
		if _, err := f.execute(f.spec(action, "bad")); err == nil {
			t.Fatal("unsupported action accepted", action)
		}
	}
}
func TestMessageOperationConcurrentAttemptFence(t *testing.T) {
	f := newOperationFixture(t)
	spec := f.spec("edit_text", "race")
	var wg sync.WaitGroup
	var bad atomic.Int32
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			o, err := f.execute(spec)
			if err != nil {
				bad.Add(1)
				t.Log(err)
			} else if o.ID == "" {
				bad.Add(1)
			}
		}()
	}
	wg.Wait()
	if bad.Load() != 0 || f.writes.Load() != 1 {
		t.Fatal("duplicate calls", bad.Load(), f.writes.Load())
	}
}
func TestMessageOperationRevokedClaimBeforeAttemptReleasesHold(t *testing.T) {
	f := newOperationFixture(t)
	f.rest.sendMu.Lock()
	done := make(chan error, 1)
	go func() { _, err := f.execute(f.spec("pin", "expire")); done <- err }()
	id := operationID(f.claim.InboundID, "expire")
	deadline := time.Now().Add(3 * time.Second)
	for {
		o, err := f.store.MessageOperation(id)
		if err == nil && o.State == "prepared" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("not prepared")
		}
		time.Sleep(time.Millisecond)
	}
	if err := f.store.Ignore(f.claim.InboundID, f.claim.Claim); err != nil {
		t.Fatal(err)
	}
	f.rest.sendMu.Unlock()
	<-done
	out, err := f.store.MessageOperation(id)
	if err != nil || out.State != "failed" || out.Attempts != 0 || f.writes.Load() != 0 {
		t.Fatal(out, err)
	}
}
func TestMessageOperationSnapshotAttachmentFlagsPreserved(t *testing.T) {
	f := newOperationFixture(t)
	f.target["attachments"] = []any{map[string]any{"id": "77", "filename": "old.txt", "size": 12, "content_type": "text/plain"}}
	f.target["embeds"] = []any{map[string]any{"type": "rich", "description": "keep me"}}
	out, err := f.execute(f.spec("edit_text", "preserve"))
	if err != nil || out.State != "verified" {
		t.Fatal(out, err)
	}
	o, err := f.store.MessageOperation(out.ID)
	if err != nil {
		t.Fatal(err)
	}
	method, _, body := operationRequest(o)
	var v map[string]json.RawMessage
	json.Unmarshal(body, &v)
	if method != "PATCH" || string(v["attachments"]) != `[{"id":"77"}]` || string(v["flags"]) != "4" || v["embeds"] != nil || v["components"] != nil {
		t.Fatal(string(body))
	}
}
func TestMessageOperationOriginalTargetMismatchFailsBeforeWrite(t *testing.T) {
	f := newOperationFixture(t)
	f.target["content"] = "external edit"
	if _, err := f.execute(f.spec("edit_text", "bad")); err == nil || f.writes.Load() != 0 {
		t.Fatal(err, f.writes.Load())
	}
	f = newOperationFixture(t)
	f.target["author"] = User{ID: "8", Bot: true}
	if _, err := f.execute(f.spec("edit_text", "bad")); err == nil || f.writes.Load() != 0 {
		t.Fatal(err, f.writes.Load())
	}
}
func TestMessageOperationRecoveryNoReplay(t *testing.T) {
	f := newOperationFixture(t)
	f.lose = true
	spec := f.spec("pin", "restart")
	o, err := f.execute(spec)
	if err != nil || o.State != "uncertain" {
		t.Fatal(o, err)
	}
	path := f.store.path
	f.store.Close()
	s, err := OpenStore(path, testPolicy())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	f.store = s
	o, err = f.execute(spec)
	if err != nil || o.State != "uncertain" || f.writes.Load() != 1 {
		t.Fatal(o, err, f.writes.Load())
	}
}
func TestMessageOperationCustomReactionRename(t *testing.T) {
	f := newOperationFixture(t)
	spec := f.spec("add_reaction", "custom")
	spec.Emoji = "oldname:88"
	f.afterWrite = func() {
		f.target["reactions"] = []any{map[string]any{"me": true, "emoji": map[string]any{"name": "newname", "id": "88"}}}
	}
	out, err := f.execute(spec)
	if err != nil || out.State != "verified" {
		t.Fatal(out, err)
	}
}
func TestMessageOperationSpecStrictAndClaimRequired(t *testing.T) {
	for _, raw := range []string{`{"version":1,"key":"x","action":"pin","channel_id":"2","message_id":"9","url":"https://evil"}`, `{"version":1,"key":"x","action":"add_reaction","channel_id":"2","message_id":"9","emoji":"%2F"}`} {
		if _, err := ParseMessageOperation([]byte(raw)); err == nil {
			t.Fatal(raw)
		}
	}
	f := newOperationFixture(t)
	_, err := f.rest.ExecuteMessageOperation(context.Background(), f.store, f.claim.InboundID, "wrong", f.spec("pin", "claim"))
	if !errors.Is(err, ErrClaim) || f.writes.Load() != 0 {
		t.Fatal(err)
	}
}

var _ = io.Discard
