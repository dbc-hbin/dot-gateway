package bridge

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
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

	"golang.org/x/sys/unix"
)

func outputFixture(t *testing.T) (*Store, string, *Claim, string) {
	t.Helper()
	dir := t.TempDir()
	os.Chmod(dir, 0700)
	path := filepath.Join(dir, "queue.db")
	s, e := OpenStore(path, testPolicy())
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { s.Close() })
	if _, e = s.Ingest(testEnvelope()); e != nil {
		t.Fatal(e)
	}
	claim, e := s.ClaimNext(300, 60)
	if e != nil || claim == nil {
		t.Fatal(e)
	}
	out, e := s.ReplyOutputDir()
	if e != nil {
		t.Fatal(e)
	}
	return s, path, claim, out
}
func queueOutput(t *testing.T, s *Store, cl *Claim, m ReplyManifest) (string, *Chunk) {
	t.Helper()
	raw, _ := json.Marshal(m)
	id, e := s.QueueReplyManifest(cl.InboundID, cl.Claim, raw)
	if e != nil {
		t.Fatal(e)
	}
	c, e := s.NextChunk()
	if e != nil || c == nil {
		t.Fatal(e)
	}
	return id, c
}
func outputACK(c Chunk) map[string]any {
	ack := ackFor(c)
	out, _ := decodeReplyOutput(c.Output)
	files := []map[string]any{}
	for i, f := range out.Attachments {
		files = append(files, map[string]any{"id": fmt.Sprint(800 + i), "filename": f.Filename, "size": f.Size, "content_type": f.ContentType, "description": f.Description, "url": "https://cdn.discordapp.com/attachments/2/800/" + f.Filename + "?ex=refresh"})
	}
	ack["attachments"] = files
	ack["embeds"] = out.Embeds
	return ack
}
func tinyReplyPNG(t *testing.T) []byte {
	t.Helper()
	b, e := base64.StdEncoding.DecodeString("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVQIHWP4z8DwHwAFgAI/ScLbtAAAAABJRU5ErkJggg==")
	if e != nil {
		t.Fatal(e)
	}
	return b
}
func TestReplyManifestMultipartAndImmutableBytes(t *testing.T) {
	s, path, cl, dir := outputFixture(t)
	original := tinyReplyPNG(t)
	if e := os.WriteFile(filepath.Join(dir, "answer.png"), original, 0600); e != nil {
		t.Fatal(e)
	}
	m := ReplyManifest{Version: 1, Text: "An authored answer", Attachments: []ReplyFile{{Path: "answer.png", Description: "One pixel"}}, Embeds: []ReplyEmbed{{Title: "Result", Image: &ReplyEmbedImage{URL: "attachment://answer.png"}}}}
	id, c := queueOutput(t, s, cl, m)
	out, _ := decodeReplyOutput(c.Output)
	if out.Attachments[0].ContentType != "image/png" {
		t.Fatal(out)
	}
	// Sender uses the snapshot, never the mutable caller-owned outbox source.
	os.WriteFile(filepath.Join(dir, "answer.png"), []byte("changed after queue"), 0600)
	var posts atomic.Int32
	r := localREST(t, func(w http.ResponseWriter, req *http.Request) {
		if preflightHandler(w, req) {
			return
		}
		posts.Add(1)
		typ, params, e := mime.ParseMediaType(req.Header.Get("Content-Type"))
		if e != nil || typ != "multipart/form-data" {
			t.Error("not multipart", typ, e)
			return
		}
		reader := multipart.NewReader(req.Body, params["boundary"])
		part, e := reader.NextPart()
		if e != nil || part.FormName() != "payload_json" {
			t.Error("missing payload")
			return
		}
		var p map[string]any
		json.NewDecoder(part).Decode(&p)
		if p["nonce"] != Nonce(*c) || p["enforce_nonce"] != true || p["flags"] != float64(0) || p["content"] != c.Text {
			t.Error("wrong payload", p)
		}
		mentions := p["allowed_mentions"].(map[string]any)
		if len(mentions["parse"].([]any)) != 0 || mentions["replied_user"] != false {
			t.Error("mentions enabled")
		}
		ref := p["message_reference"].(map[string]any)
		if ref["message_id"] != "3" || ref["channel_id"] != "2" || ref["fail_if_not_exists"] != true {
			t.Error("reference lost")
		}
		part, e = reader.NextPart()
		if e != nil || part.FormName() != "files[0]" || part.FileName() != "answer.png" || part.Header.Get("Content-Type") != "image/png" {
			t.Error("bad file part", e)
			return
		}
		data, e := io.ReadAll(part)
		if e != nil || !bytes.Equal(data, original) {
			t.Error("mutable source leaked")
		}
		if _, e = reader.NextPart(); e != io.EOF {
			t.Error("extra parts")
		}
		json.NewEncoder(w).Encode(outputACK(*c))
	})
	r.settings.DBPath = path
	result := r.Send(context.Background(), *c)
	if result.State != "sent" || result.OutputReceipt == "" || posts.Load() != 1 {
		t.Fatal(result, posts.Load())
	}
	if e := s.RecordResult(*c, result); e != nil {
		t.Fatal(e)
	}
	stored, receipt, e := s.ReplyReadback(id, 0)
	if e != nil || stored.Output != c.Output || receipt.OutputReceipt != result.OutputReceipt {
		t.Fatal(stored, receipt, e)
	}
	info, e := os.Stat(filepath.Join(ReplyStateDir(path), "reply-spool", out.Attachments[0].SHA256+".blob"))
	if e != nil || info.Mode().Perm() != 0400 {
		t.Fatal("spool not private immutable", e)
	}
}
func TestReplyManifestAttachmentsOnlyAudioAndFiles(t *testing.T) {
	for _, tt := range []struct {
		name string
		data []byte
		mime string
	}{{"note.txt", []byte("model-authored file\n"), "text/plain"}, {"voice.wav", append([]byte("RIFF\x24\x00\x00\x00WAVEfmt "), make([]byte, 32)...), "audio/wav"}, {"report.pdf", []byte("%PDF-1.7\n1 0 obj\n"), "application/pdf"}} {
		t.Run(tt.name, func(t *testing.T) {
			s, path, cl, dir := outputFixture(t)
			os.WriteFile(filepath.Join(dir, tt.name), tt.data, 0600)
			_, c := queueOutput(t, s, cl, ReplyManifest{Version: 1, Attachments: []ReplyFile{{Path: tt.name}}})
			if c.Text != "" {
				t.Fatal(c.Text)
			}
			out, _ := decodeReplyOutput(c.Output)
			if out.Attachments[0].ContentType != tt.mime {
				t.Fatal(out.Attachments[0].ContentType)
			}
			r := localREST(t, func(w http.ResponseWriter, req *http.Request) {
				if preflightHandler(w, req) {
					return
				}
				json.NewEncoder(w).Encode(outputACK(*c))
			})
			r.settings.DBPath = path
			if result := r.Send(context.Background(), *c); result.State != "sent" {
				t.Fatal(result)
			}
		})
	}
}
func TestReplyManifestValidationAndAuthority(t *testing.T) {
	cases := []string{`{}`, `{"version":1,"version":1,"text":"x"}`, `{"Version":1,"version":1,"text":"x"}`, `{"version":2,"text":"x"}`, `{"version":1,"text":"x","channel_id":"9"}`, `{"version":1,"attachments":[{"path":"../token"}]}`, `{"version":1,"attachments":[{"path":"/etc/passwd"}]}`, `{"version":1,"attachments":[{"path":".env"}]}`, `{"version":1,"attachments":[{"path":"a","filename":"../../token"}]}`, `{"version":1,"embeds":[{"description":"x","image":{"url":"https://evil.invalid/pixel"}}]}`, `{"version":1,"embeds":[{"description":"x","url":"javascript:alert(1)"}]}`, `{"version":1,"embeds":[{"description":"x","provider":{"name":"fake"}}]}`, `{"version":1,"flags":32768,"text":"x"}`, `{"version":1,"text":"x"} {}`}
	for _, raw := range cases {
		if _, e := ParseReplyManifest([]byte(raw)); e == nil {
			t.Fatalf("accepted %s", raw)
		}
	}
	for _, field := range []string{"title", "description"} {
		raw := fmt.Sprintf(`{"version":1,"embeds":[{"%s":%q}]}`, field, strings.Repeat("x", 6001))
		if _, e := ParseReplyManifest([]byte(raw)); e == nil {
			t.Fatal("accepted excessive embed")
		}
	}
	s, _, cl, dir := outputFixture(t)
	os.WriteFile(filepath.Join(dir, "a.txt"), []byte("answer"), 0600)
	if _, e := s.QueueReplyManifest(cl.InboundID, "wrong", []byte(`{"version":1,"attachments":[{"path":"a.txt"}]}`)); e != ErrClaim {
		t.Fatal(e)
	}
	if _, e := os.Stat(filepath.Join(s.replyStateDir, "reply-spool")); !os.IsNotExist(e) {
		t.Fatal("staged with invalid claim", e)
	}
}
func TestReplyManifestRejectsUnsafeFiles(t *testing.T) {
	for _, kind := range []string{"symlink", "hardlink", "fifo", "directory", "public_write", "oversize", "outbox_symlink", "ancestor_symlink"} {
		t.Run(kind, func(t *testing.T) {
			s, _, cl, dir := outputFixture(t)
			name := filepath.Join(dir, "data.bin")
			outside := filepath.Join(s.replyStateDir, "outside.bin")
			os.WriteFile(outside, []byte("not output"), 0600)
			switch kind {
			case "symlink":
				os.Symlink(outside, name)
			case "hardlink":
				os.Link(outside, name)
			case "fifo":
				unix.Mkfifo(name, 0600)
			case "directory":
				os.Mkdir(name, 0700)
			case "public_write":
				os.WriteFile(name, []byte("unsafe"), 0666)
				os.Chmod(name, 0666)
			case "oversize":
				f, _ := os.OpenFile(name, os.O_CREATE|os.O_WRONLY, 0600)
				f.Truncate(MaxReplyFileBytes + 1)
				f.Close()
			case "outbox_symlink":
				os.Remove(dir)
				os.Symlink(s.replyStateDir, dir)
			case "ancestor_symlink":
				alias := filepath.Join(t.TempDir(), "alias")
				os.Symlink(s.replyStateDir, alias)
				s.replyStateDir = alias
			}
			if _, e := s.QueueReplyManifest(cl.InboundID, cl.Claim, []byte(`{"version":1,"attachments":[{"path":"data.bin"}]}`)); e == nil {
				t.Fatal("unsafe file accepted")
			}
		})
	}
}
func TestReplySpoolTamperNeverPosts(t *testing.T) {
	for _, kind := range []string{"bytes", "symlink", "size", "digest", "mime"} {
		t.Run(kind, func(t *testing.T) {
			s, path, cl, dir := outputFixture(t)
			os.WriteFile(filepath.Join(dir, "x.txt"), []byte("original"), 0600)
			_, c := queueOutput(t, s, cl, ReplyManifest{Version: 1, Attachments: []ReplyFile{{Path: "x.txt"}}})
			out, _ := decodeReplyOutput(c.Output)
			blob := filepath.Join(s.replyStateDir, "reply-spool", out.Attachments[0].SHA256+".blob")
			switch kind {
			case "bytes":
				os.Chmod(blob, 0600)
				os.WriteFile(blob, []byte("tampered"), 0600)
			case "symlink":
				os.Remove(blob)
				os.Symlink(filepath.Join(dir, "x.txt"), blob)
			case "size":
				out.Attachments[0].Size++
			case "digest":
				out.Attachments[0].SHA256 = strings.Repeat("f", 64)
			case "mime":
				out.Attachments[0].ContentType = "image/png"
			}
			raw, _ := json.Marshal(out)
			c.Output = ReplyOutputSnapshot(raw)
			var calls atomic.Int32
			r := localREST(t, func(w http.ResponseWriter, r *http.Request) { calls.Add(1) })
			r.settings.DBPath = path
			if got := r.Send(context.Background(), *c); got.State != "failed" || calls.Load() != 0 {
				t.Fatal(got, calls.Load())
			}
		})
	}
}
func TestReplyOutputAckMismatchIsUncertain(t *testing.T) {
	for _, kind := range []string{"missing", "size", "type", "filename", "id", "description", "embed", "reference"} {
		t.Run(kind, func(t *testing.T) {
			s, path, cl, dir := outputFixture(t)
			os.WriteFile(filepath.Join(dir, "x.txt"), []byte("answer"), 0600)
			_, c := queueOutput(t, s, cl, ReplyManifest{Version: 1, Attachments: []ReplyFile{{Path: "x.txt", Description: "proof"}}, Embeds: []ReplyEmbed{{Description: "authored"}}})
			ack := outputACK(*c)
			file := ack["attachments"].([]map[string]any)[0]
			switch kind {
			case "missing":
				delete(ack, "attachments")
			case "size":
				file["size"] = 1
			case "type":
				file["content_type"] = "image/png"
			case "filename":
				file["filename"] = "other.txt"
			case "id":
				file["id"] = "0"
			case "description":
				file["description"] = "changed"
			case "embed":
				ack["embeds"] = []ReplyEmbed{{Description: "changed"}}
			case "reference":
				ack["message_reference"] = map[string]any{"message_id": "8", "channel_id": "2"}
			}
			var posts atomic.Int32
			r := localREST(t, func(w http.ResponseWriter, req *http.Request) {
				if preflightHandler(w, req) {
					return
				}
				posts.Add(1)
				json.NewEncoder(w).Encode(ack)
			})
			r.settings.DBPath = path
			if got := r.Send(context.Background(), *c); got.State != "uncertain" || posts.Load() != 1 {
				t.Fatal(got, posts.Load())
			}
		})
	}
}
func TestMultipartFailureAndLostACKNeverReplay(t *testing.T) {
	for _, status := range []int{0, 429, 500, 307} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			s, path, cl, dir := outputFixture(t)
			os.WriteFile(filepath.Join(dir, "x.txt"), []byte("answer"), 0600)
			_, c := queueOutput(t, s, cl, ReplyManifest{Version: 1, Attachments: []ReplyFile{{Path: "x.txt"}}})
			var posts atomic.Int32
			r := localREST(t, func(w http.ResponseWriter, req *http.Request) {
				if preflightHandler(w, req) {
					return
				}
				posts.Add(1)
				if status == 0 {
					conn, _, _ := w.(http.Hijacker).Hijack()
					conn.Close()
					return
				}
				w.Header().Set("Location", "/other")
				w.WriteHeader(status)
			})
			r.settings.DBPath = path
			got := r.Send(context.Background(), *c)
			want := "uncertain"
			if status == 429 {
				want = "failed"
			}
			if got.State != want || posts.Load() != 1 {
				t.Fatal(got, posts.Load())
			}
			if e := s.RecordResult(*c, got); e != nil {
				t.Fatal(e)
			}
			if status != 429 {
				if n, e := s.RetryFailed(c.ReplyID); e != nil || n != 0 {
					t.Fatal("uncertainty retried", n, e)
				}
			}
		})
	}
}
func TestReplyOutputRestartAndIdempotency(t *testing.T) {
	s, path, cl, dir := outputFixture(t)
	os.WriteFile(filepath.Join(dir, "x.txt"), []byte("stable"), 0600)
	raw := []byte(`{"version":1,"attachments":[{"path":"x.txt"}]}`)
	id, e := s.QueueReplyManifest(cl.InboundID, cl.Claim, raw)
	if e != nil {
		t.Fatal(e)
	}
	again, e := s.QueueReplyManifest(cl.InboundID, cl.Claim, raw)
	if e != nil || again != id {
		t.Fatal(again, e)
	}
	if _, e = s.QueueReplyManifest(cl.InboundID, "stale", raw); e != ErrClaim {
		t.Fatal("wrong claim idempotency", e)
	}
	c, e := s.NextChunk()
	if e != nil || c == nil {
		t.Fatal(e)
	}
	nonce := Nonce(*c)
	s.Close()
	other, e := OpenStore(path, testPolicy())
	if e != nil {
		t.Fatal(e)
	}
	defer other.Close()
	if n, e := other.RecoverInterrupted(); e != nil || n != 1 {
		t.Fatal(n, e)
	}
	if n, e := other.RetryFailed(id); e != nil || n != 0 {
		t.Fatal(n, e)
	}
	if next, e := other.NextChunk(); e != nil || next != nil {
		t.Fatal(next, e)
	}
	if nonce != Nonce(*c) {
		t.Fatal("unstable nonce")
	}
	if _, _, e := BuildReplyBody(*c, ReplyStateDir(path)); e != nil {
		t.Fatal("spool lost across restart", e)
	}
}
func TestReplyReadbackChecksPersistedAttachmentIDs(t *testing.T) {
	s, path, cl, dir := outputFixture(t)
	os.WriteFile(filepath.Join(dir, "x.txt"), []byte("stable"), 0600)
	id, c := queueOutput(t, s, cl, ReplyManifest{Version: 1, Attachments: []ReplyFile{{Path: "x.txt"}}})
	ack := outputACK(*c)
	r := localREST(t, func(w http.ResponseWriter, req *http.Request) {
		if preflightHandler(w, req) {
			return
		}
		json.NewEncoder(w).Encode(ack)
	})
	r.settings.DBPath = path
	result := r.Send(context.Background(), *c)
	if result.State != "sent" {
		t.Fatal(result)
	}
	if e := s.RecordResult(*c, result); e != nil {
		t.Fatal(e)
	}
	stored, receipt, e := s.ReplyReadback(id, 0)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = r.VerifyStoredReply(context.Background(), stored, receipt); e != nil {
		t.Fatal(e)
	}
	ack["attachments"].([]map[string]any)[0]["id"] = "999"
	if _, e = r.VerifyStoredReply(context.Background(), stored, receipt); e == nil {
		t.Fatal("attachment replacement accepted")
	}
}
func TestRichAndPlainEmbedPolicyAndInteractionBody(t *testing.T) {
	for _, rich := range []bool{false, true} {
		c := testChunk()
		if rich {
			raw, _ := json.Marshal(replyOutput{Version: 1, Embeds: []ReplyEmbed{{Title: "explicit"}}})
			c.Output = ReplyOutputSnapshot(raw)
		}
		raw, e := BuildReplyPayload(c)
		if e != nil {
			t.Fatal(e)
		}
		var p map[string]any
		json.Unmarshal(raw, &p)
		want := float64(4)
		if rich {
			want = 0
		}
		if p["flags"] != want {
			t.Fatal(p)
		}
		raw, typ, e := BuildInteractionReplyBody(c, t.TempDir())
		if e != nil || typ != "application/json" {
			t.Fatal(e, typ)
		}
		p = map[string]any{}
		json.Unmarshal(raw, &p)
		for _, key := range []string{"message_reference", "nonce", "enforce_nonce"} {
			if _, ok := p[key]; ok {
				t.Fatal("interaction has message field", key)
			}
		}
	}
}
func TestReplySpoolQuotaAndPruning(t *testing.T) {
	s, path, cl, dir := outputFixture(t)
	os.WriteFile(filepath.Join(dir, "x.txt"), []byte("stable"), 0600)
	_, c := queueOutput(t, s, cl, ReplyManifest{Version: 1, Attachments: []ReplyFile{{Path: "x.txt"}}})
	out, _ := decodeReplyOutput(c.Output)
	spool := filepath.Join(ReplyStateDir(path), "reply-spool")
	blob := filepath.Join(spool, out.Attachments[0].SHA256+".blob")
	old := time.Now().Add(-8 * 24 * time.Hour)
	os.Chtimes(blob, old, old)
	if n, e := s.PruneReplySpool(); e != nil || n != 0 {
		t.Fatal("pruned in-flight", n, e)
	}
	if e := s.RecordResult(*c, SendResult{State: "uncertain", Code: "lost_ack"}); e != nil {
		t.Fatal(e)
	}
	if n, e := s.PruneReplySpool(); e != nil || n != 0 {
		t.Fatal("pruned uncertain", n, e)
	}
	orphan := filepath.Join(spool, strings.Repeat("f", 64)+".blob")
	os.WriteFile(orphan, []byte("orphan"), 0400)
	os.Chtimes(orphan, old, old)
	if n, e := s.PruneReplySpool(); e != nil || n != 1 {
		t.Fatal("orphan cleanup", n, e)
	}
	for i := 0; i < maxReplySpoolFiles; i++ {
		name := fmt.Sprintf("%064x.blob", i)
		os.WriteFile(filepath.Join(spool, name), []byte("quota"), 0400)
	}
	os.WriteFile(filepath.Join(dir, "y.txt"), []byte("new unique payload"), 0600)
	m, e := ParseReplyManifest([]byte(`{"version":1,"attachments":[{"path":"y.txt"}]}`))
	if e != nil {
		t.Fatal(e)
	}
	if _, e = stageReplyOutput(s.replyStateDir, m); e == nil || e.Error() != "reply_spool_quota_exceeded" {
		t.Fatal(e)
	}
}
func TestManifestSafeReaderRejectsSymlinkAndFIFO(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "manifest.json")
	os.WriteFile(file, []byte(`{"version":1,"text":"safe"}`), 0600)
	if _, e := ReadReplyManifestFile(file); e != nil {
		t.Fatal(e)
	}
	link := filepath.Join(dir, "alias.json")
	os.Symlink(file, link)
	if _, e := ReadReplyManifestFile(link); e == nil {
		t.Fatal("symlink accepted")
	}
	fifo := filepath.Join(dir, "pipe")
	unix.Mkfifo(fifo, 0600)
	if _, e := ReadReplyManifestFile(fifo); e == nil {
		t.Fatal("FIFO accepted")
	}
}

func TestReplyEmbedRequiresExactFilenameAttachmentIdentity(t *testing.T) {
	out := replyOutput{Version: 1, Attachments: []StagedReplyFile{{Filename: "first.png", Size: 1, SHA256: strings.Repeat("1", 64), ContentType: "image/png"}, {Filename: "second.png", Size: 1, SHA256: strings.Repeat("2", 64), ContentType: "image/png"}}, Embeds: []ReplyEmbed{{Image: &ReplyEmbedImage{URL: "attachment://first.png"}}}}
	raw, _ := json.Marshal(out)
	c := testChunk()
	c.Output = ReplyOutputSnapshot(raw)
	for _, correct := range []bool{false, true} {
		ack := outputACK(c)
		id := "801"
		if correct {
			id = "800"
		}
		ack["embeds"] = []ReplyEmbed{{Image: &ReplyEmbedImage{URL: "https://cdn.discordapp.com/attachments/2/" + id + "/first.png?ex=renewed"}}}
		b, _ := json.Marshal(ack)
		var a map[string]json.RawMessage
		json.Unmarshal(b, &a)
		err := ValidateReplyOutputAck(a, c)
		if correct && err != nil || !correct && err == nil {
			t.Fatal(correct, err)
		}
	}
}
func TestReplyEmbedRejectsNonImageAttachment(t *testing.T) {
	s, _, cl, dir := outputFixture(t)
	os.WriteFile(filepath.Join(dir, "fake.png"), []byte("not an image"), 0600)
	_, e := s.QueueReplyManifest(cl.InboundID, cl.Claim, []byte(`{"version":1,"attachments":[{"path":"fake.png"}],"embeds":[{"image":{"url":"attachment://fake.png"}}]}`))
	if e == nil || e.Error() != "reply_embed_attachment_not_image" {
		t.Fatal(e)
	}
}
func TestMultipartPostHasNoReplayableBodyAndFreshGuard(t *testing.T) {
	s, path, cl, dir := outputFixture(t)
	os.WriteFile(filepath.Join(dir, "x.txt"), []byte("answer"), 0600)
	_, c := queueOutput(t, s, cl, ReplyManifest{Version: 1, Attachments: []ReplyFile{{Path: "x.txt"}}})
	r, e := NewRESTClient(Settings{Policy: testPolicy(), ExpectedBotID: "4", DBPath: path})
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close()
	var posts atomic.Int32
	r.baseURL = "https://discord.com"
	r.client.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.GetBody != nil {
			t.Error("body replayable")
		}
		raw := `{"id":"4","bot":true}`
		if req.URL.Path == "/channels/2" {
			raw = `{"id":"2","type":1,"recipients":[{"id":"1"}]}`
		}
		if req.Method == "POST" {
			posts.Add(1)
			if !strings.HasPrefix(req.Header.Get("Content-Type"), "multipart/form-data;") {
				t.Error("not multipart")
			}
			b, _ := json.Marshal(outputACK(*c))
			raw = string(b)
		}
		return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(raw))}, nil
	})
	var checks atomic.Int32
	result := r.SendGuarded(context.Background(), *c, func() bool { return checks.Add(1) == 1 })
	if result.State != "failed" || result.Code != "connection_changed_before_send" || posts.Load() != 0 {
		t.Fatal(result, posts.Load())
	}
	if result = r.Send(context.Background(), *c); result.State != "sent" || posts.Load() != 1 {
		t.Fatal(result, posts.Load())
	}
}
func TestReplyReceiptFailureRollsBackSent(t *testing.T) {
	s, _, cl, dir := outputFixture(t)
	os.WriteFile(filepath.Join(dir, "x.txt"), []byte("answer"), 0600)
	_, c := queueOutput(t, s, cl, ReplyManifest{Version: 1, Attachments: []ReplyFile{{Path: "x.txt"}}})
	if e := s.RecordResult(*c, SendResult{State: "sent", MessageID: "9"}); e == nil {
		t.Fatal("missing receipt accepted")
	}
	d, e := s.Delivery(c.ReplyID)
	if e != nil || d.Chunks[0].State != "sending" {
		t.Fatal(d, e)
	}
	if n, e := s.RecoverInterrupted(); e != nil || n != 1 {
		t.Fatal(n, e)
	}
	d, e = s.Delivery(c.ReplyID)
	if e != nil || d.Chunks[0].State != "uncertain" {
		t.Fatal(d, e)
	}
}
func TestReplyOutputOnlyFirstChunk(t *testing.T) {
	s, _, cl, dir := outputFixture(t)
	os.WriteFile(filepath.Join(dir, "x.txt"), []byte("answer"), 0600)
	id, c := queueOutput(t, s, cl, ReplyManifest{Version: 1, Text: strings.Repeat("a", 2000), Attachments: []ReplyFile{{Path: "x.txt"}}})
	ack := outputACK(*c)
	raw, _ := json.Marshal(ack)
	var a map[string]json.RawMessage
	json.Unmarshal(raw, &a)
	receipt, e := ReplyOutputAckReceipt(a, *c)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.RecordResult(*c, SendResult{State: "sent", MessageID: "9", OutputReceipt: receipt}); e != nil {
		t.Fatal(e)
	}
	next, e := s.NextChunk()
	if e != nil || next == nil || next.Output != "" || next.Index != 1 {
		t.Fatal(next, e)
	}
	d, e := s.Delivery(id)
	if e != nil || d.Chunks[0].OutputReceipt == "" || d.Chunks[1].OutputReceipt != "" {
		t.Fatal(d, e)
	}
}

func TestReplySpoolRecoversAbandonedTempAndIsolatesLedgers(t *testing.T) {
	s, path, cl, dir := outputFixture(t)
	os.WriteFile(filepath.Join(dir, "x.txt"), []byte("stable"), 0600)
	spool, e := ensureReplyDirectory(s.replyStateDir, "reply-spool")
	if e != nil {
		t.Fatal(e)
	}
	spool.Close()
	orphan := filepath.Join(s.replyStateDir, "reply-spool", ".stage-"+strings.Repeat("a", 32)+".tmp")
	os.WriteFile(orphan, []byte("interrupted write"), 0400)
	_, c := queueOutput(t, s, cl, ReplyManifest{Version: 1, Attachments: []ReplyFile{{Path: "x.txt"}}})
	if _, e := os.Stat(orphan); !os.IsNotExist(e) {
		t.Fatal("unpublished temp not recovered", e)
	}
	other, e := OpenStore(filepath.Join(filepath.Dir(path), "other.db"), testPolicy())
	if e != nil {
		t.Fatal(e)
	}
	defer other.Close()
	otherOut, e := other.ReplyOutputDir()
	if e != nil || otherOut == dir {
		t.Fatal("shared outbox", otherOut, e)
	}
	out, _ := decodeReplyOutput(c.Output)
	blob := filepath.Join(s.replyStateDir, "reply-spool", out.Attachments[0].SHA256+".blob")
	old := time.Now().Add(-8 * 24 * time.Hour)
	os.Chtimes(blob, old, old)
	if n, e := other.PruneReplySpool(); e != nil || n != 0 {
		t.Fatal("sibling ledger touched spool", n, e)
	}
	if _, e := os.Stat(blob); e != nil {
		t.Fatal("active blob removed", e)
	}
}

func TestRichReplyReconcilesOnlyExactReadbackNoPost(t *testing.T) {
	s, path, cl, dir := outputFixture(t)
	os.WriteFile(filepath.Join(dir, "x.txt"), []byte("stable"), 0600)
	id, c := queueOutput(t, s, cl, ReplyManifest{Version: 1, Attachments: []ReplyFile{{Path: "x.txt"}}})
	if e := s.RecordResult(*c, SendResult{State: "uncertain", Code: "lost_ack"}); e != nil {
		t.Fatal(e)
	}
	if e := s.ResolveSent(id, 0, "9"); e == nil || e.Error() != "rich_reply_requires_reconcile_reply" {
		t.Fatal("receipt bypass", e)
	}
	ack := outputACK(*c)
	ack["content"] = "wrong"
	var posts atomic.Int32
	r := localREST(t, func(w http.ResponseWriter, req *http.Request) {
		if req.Method != "GET" {
			posts.Add(1)
		}
		if preflightHandler(w, req) {
			return
		}
		json.NewEncoder(w).Encode(ack)
	})
	r.settings.DBPath = path
	if _, e := r.ReconcileReply(context.Background(), s, id, 0, "9"); e == nil {
		t.Fatal("wrong remote accepted")
	}
	d, e := s.Delivery(id)
	if e != nil || d.State != "uncertain" {
		t.Fatal(d, e)
	}
	ack = outputACK(*c)
	result, e := r.ReconcileReply(context.Background(), s, id, 0, "9")
	if e != nil || result.State != "sent" || result.OutputReceipt == "" || posts.Load() != 0 {
		t.Fatal(result, e, posts.Load())
	}
	d, e = s.Delivery(id)
	if e != nil || d.State != "sent" || d.Chunks[0].Attempts != 1 || d.Chunks[0].OutputReceipt == "" {
		t.Fatal(d, e)
	}
	if _, e := r.ReconcileReply(context.Background(), s, id, 0, "9"); e == nil {
		t.Fatal("reconciled twice")
	}
}
