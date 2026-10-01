package bridge

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestInteractionRichOutputMultipartAndReceipt(t *testing.T) {
	s, _, cl, dir := outputFixture(t)
	data := tinyReplyPNG(t)
	os.WriteFile(filepath.Join(dir, "image.png"), data, 0600)
	_, c := queueOutput(t, s, cl, ReplyManifest{Version: 1, Attachments: []ReplyFile{{Path: "image.png"}}, Embeds: []ReplyEmbed{{Title: "Authored image", Image: &ReplyEmbedImage{URL: "attachment://image.png"}}}})
	c.Source.ReplyKind = "interaction"
	c.Source.Control = encodeControl(ControlEvent{Version: 1, Kind: "ask", ID: "ask-3", InteractionID: c.Source.EventID, ActorID: c.Source.SenderID})
	tr, e := newInteractionTransport(Settings{Policy: testPolicy(), ExpectedBotID: "4", DBPath: strings.TrimSuffix(s.replyStateDir, ".reply")})
	if e != nil {
		t.Fatal(e)
	}
	defer tr.close()
	tr.remember(c.Source.EventID, "synthetic-offline-token")
	var posts atomic.Int32
	tr.client.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
		posts.Add(1)
		if req.Method != http.MethodPatch || !strings.HasSuffix(req.URL.Path, "/messages/@original") || req.GetBody != nil || req.Header.Get("Authorization") != "" {
			t.Error("bad interaction transport")
		}
		typ, params, e := mime.ParseMediaType(req.Header.Get("Content-Type"))
		if e != nil || typ != "multipart/form-data" {
			t.Fatal("not multipart", e)
		}
		reader := multipart.NewReader(req.Body, params["boundary"])
		part, e := reader.NextPart()
		if e != nil || part.FormName() != "payload_json" {
			t.Fatal(e)
		}
		var body map[string]any
		json.NewDecoder(part).Decode(&body)
		if body["flags"] != float64(64) {
			t.Error("ephemeral rich flag absent", body)
		}
		for _, key := range []string{"nonce", "enforce_nonce", "message_reference"} {
			if _, exists := body[key]; exists {
				t.Error("ordinary field leaked", key)
			}
		}
		part, e = reader.NextPart()
		if e != nil {
			t.Fatal(e)
		}
		got, _ := io.ReadAll(part)
		if !bytes.Equal(got, data) {
			t.Error("wrong attachment bytes")
		}
		ack := outputACK(*c)
		delete(ack, "message_reference")
		delete(ack, "nonce")
		ack["webhook_id"] = "4"
		ack["flags"] = 64
		raw, _ := json.Marshal(ack)
		return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader(raw))}, nil
	})
	got := tr.send(context.Background(), *c, func() bool { return true })
	if got.State != "sent" || got.OutputReceipt == "" || posts.Load() != 1 {
		t.Fatal(got, posts.Load())
	}
	if tr.client.Timeout != 2500*time.Millisecond {
		t.Fatal("callback deadline changed")
	}
}
func TestInteractionMultipartLostACKNotRetried(t *testing.T) {
	s, _, cl, dir := outputFixture(t)
	os.WriteFile(filepath.Join(dir, "x.txt"), []byte("safe bytes"), 0600)
	_, c := queueOutput(t, s, cl, ReplyManifest{Version: 1, Attachments: []ReplyFile{{Path: "x.txt"}}})
	c.Source.ReplyKind = "interaction"
	c.Source.Control = encodeControl(ControlEvent{Version: 1, Kind: "ask", ID: "ask-3", InteractionID: c.Source.EventID, ActorID: c.Source.SenderID})
	tr, e := newInteractionTransport(Settings{Policy: testPolicy(), ExpectedBotID: "4", DBPath: strings.TrimSuffix(s.replyStateDir, ".reply")})
	if e != nil {
		t.Fatal(e)
	}
	defer tr.close()
	tr.remember(c.Source.EventID, "synthetic-offline-token")
	var calls atomic.Int32
	tr.client.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) { calls.Add(1); return nil, io.ErrUnexpectedEOF })
	got := tr.send(context.Background(), *c, func() bool { return true })
	if got.State != "uncertain" || calls.Load() != 1 {
		t.Fatal(got, calls.Load())
	}
}

func TestInteractionACKContentNormalizationMatchesOrdinary(t *testing.T) {
	for _, tc := range []struct {
		expected, actual string
		accepted         bool
	}{{"answer\n", "answer", true}, {"answer \n", "answer ", false}, {"answer\n\n", "answer\n", false}, {"answer\r\n", "answer", false}, {"answer", " answer", false}, {"answer", "answer ", false}, {"", "", true}, {"\n", "", false}} {
		ack := map[string]any{"id": "9", "channel_id": "2", "author": User{ID: "4", Bot: true}, "webhook_id": "4", "flags": 64, "content": tc.actual}
		raw, _ := json.Marshal(ack)
		var a map[string]json.RawMessage
		json.Unmarshal(raw, &a)
		if ok := validateInteractionAck(a, "4", "2", tc.expected) != ""; ok != tc.accepted {
			t.Fatal(tc, ok)
		}
		delete(a, "webhook_id")
		a["message_reference"] = json.RawMessage(`{"message_id":"3","channel_id":"2"}`)
		c := testChunk()
		c.Text = tc.expected
		if id, _ := validateAck(a, c, "4"); (id != "") != tc.accepted {
			t.Fatal("ordinary parity", tc, id)
		}
	}
	for _, value := range []json.RawMessage{nil, json.RawMessage(`null`), json.RawMessage(`0`)} {
		c := testChunk()
		c.Text = ""
		ack := ackFor(c)
		raw, _ := json.Marshal(ack)
		var a map[string]json.RawMessage
		json.Unmarshal(raw, &a)
		a["content"] = value
		if id, _ := validateAck(a, c, "4"); id != "" {
			t.Fatal("missing/nonstring ordinary content accepted")
		}
		a["webhook_id"] = json.RawMessage(`"4"`)
		a["flags"] = json.RawMessage(`64`)
		delete(a, "message_reference")
		if id := validateInteractionAck(a, "4", "2", ""); id != "" {
			t.Fatal("missing/nonstring interaction content accepted")
		}
	}
}
func TestInteractionSendRecordsOnlyBoundedTerminalLFNormalization(t *testing.T) {
	_, svc, _ := interactionFixture(t, func(w http.ResponseWriter, req *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{"id": "9", "channel_id": "2", "author": User{ID: "4", Bot: true}, "webhook_id": "4", "flags": 64, "content": "answer"})
	})
	e := testEnvelope()
	e.ReplyKind = "interaction"
	e.Control = encodeControl(ControlEvent{Version: 1, Kind: "ask", ID: "test", InteractionID: e.EventID, ActorID: e.SenderID})
	svc.transport.remember(e.EventID, "synthetic-offline-token")
	got := svc.transport.send(context.Background(), Chunk{Source: e, ReplyID: "test", Text: "answer\n"}, nil)
	if got.State != "sent" || got.Code != "ack_terminal_lf_removed" {
		t.Fatal(got)
	}
}
