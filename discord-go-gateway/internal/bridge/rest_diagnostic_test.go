package bridge

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"sync/atomic"
	"testing"
)

func transportDiagnostic() Diagnostic {
	d := Diagnostic{ID: "diag-test", Index: 0, Text: "Offline diagnostic", BotID: "4", OwnerID: "1", ChannelID: "2", GuildID: "5"}
	d.Nonce = Nonce(Chunk{ReplyID: d.ID, Index: d.Index})
	return d
}
func diagnosticAck(d Diagnostic) map[string]any {
	return map[string]any{"id": "9", "channel_id": d.ChannelID, "author": User{ID: d.BotID, Bot: true}, "content": d.Text, "nonce": d.Nonce}
}
func TestStandaloneDiagnosticOneAttempt(t *testing.T) {
	d := transportDiagnostic()
	var posts atomic.Int32
	r := diagnosticREST(t, func(w http.ResponseWriter, r *http.Request) {
		if preflightHandler(w, r) {
			return
		}
		posts.Add(1)
		var payload map[string]any
		json.NewDecoder(r.Body).Decode(&payload)
		if _, ok := payload["message_reference"]; ok {
			t.Error("diagnostic has reply binding")
		}
		if payload["nonce"] != d.Nonce || payload["enforce_nonce"] != true {
			t.Error("nonce mismatch")
		}
		json.NewEncoder(w).Encode(diagnosticAck(d))
	})
	got := r.SendDiagnostic(context.Background(), d, func() bool { return true })
	if got.State != "sent" || posts.Load() != 1 || r.Diagnostics().Operation != "diagnostic" {
		t.Fatal(got)
	}
}
func TestDiagnosticLostAckNoRetry(t *testing.T) {
	var posts atomic.Int32
	r := diagnosticREST(t, func(w http.ResponseWriter, r *http.Request) {
		if preflightHandler(w, r) {
			return
		}
		posts.Add(1)
		conn, _, _ := w.(http.Hijacker).Hijack()
		conn.Close()
	})
	got := r.SendDiagnostic(context.Background(), transportDiagnostic(), nil)
	if got.State != "uncertain" || posts.Load() != 1 {
		t.Fatal(got)
	}
}
func TestDiagnosticInvalidBindingNeverPosts(t *testing.T) {
	var posts atomic.Int32
	r := diagnosticREST(t, func(w http.ResponseWriter, r *http.Request) {
		if preflightHandler(w, r) {
			return
		}
		posts.Add(1)
		io.WriteString(w, `{}`)
	})
	for _, mut := range []func(*Diagnostic){func(d *Diagnostic) { d.Nonce = "bad" }, func(d *Diagnostic) { d.OwnerID = "5" }, func(d *Diagnostic) { d.BotID = "5" }, func(d *Diagnostic) { d.Index = 3 }, func(d *Diagnostic) { d.GuildID = "6" }} {
		d := transportDiagnostic()
		mut(&d)
		if got := r.SendDiagnostic(context.Background(), d, nil); got.State != "failed" {
			t.Fatal(got)
		}
	}
	if posts.Load() != 0 {
		t.Fatal("invalid diagnostic posted")
	}
}
func TestDiagnosticAckRejectsReplyOrNonceMismatch(t *testing.T) {
	for _, key := range []string{"message_reference", "nonce", "webhook_id", "channel_id", "author", "content"} {
		t.Run(key, func(t *testing.T) {
			d := transportDiagnostic()
			r := diagnosticREST(t, func(w http.ResponseWriter, r *http.Request) {
				if preflightHandler(w, r) {
					return
				}
				ack := diagnosticAck(d)
				ack[key] = "wrong"
				json.NewEncoder(w).Encode(ack)
			})
			if got := r.SendDiagnostic(context.Background(), d, nil); got.State != "uncertain" {
				t.Fatal(got)
			}
		})
	}
}

func diagnosticREST(t *testing.T, h http.HandlerFunc) *RESTClient {
	r := localREST(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/channels/2" {
			io.WriteString(w, `{"id":"2","type":0,"guild_id":"5"}`)
			return
		}
		h(w, r)
	})
	r.settings.Policy.GuildID = "5"
	r.settings.Policy.GuildChannelID = "2"
	return r
}

func TestDiagnosticMeasuredCapturesOwnOperation(t *testing.T) {
	d := transportDiagnostic()
	r := diagnosticREST(t, func(w http.ResponseWriter, r *http.Request) {
		if preflightHandler(w, r) {
			return
		}
		json.NewEncoder(w).Encode(diagnosticAck(d))
	})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range 15 {
			r.Send(context.Background(), testChunk())
		}
	}()
	for range 15 {
		got, diag := r.SendDiagnosticMeasured(context.Background(), d, nil)
		if got.State != "sent" || diag.Operation != "diagnostic" || diag.State != got.State {
			t.Fatalf("result=%+v diagnostics=%+v", got, diag)
		}
	}
	<-done
}
