package bridge

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
)

func threadPolicy() Policy { p := testPolicy(); p.GuildID = "5"; p.GuildChannelID = "2"; return p }
func threadEnvelope() Envelope {
	e := testEnvelope()
	e.GuildID = "5"
	e.ConversationID = "6"
	e.RouteKind = "guild_thread"
	e.ParentChannelID = "2"
	e.ThreadType = 11
	e.BotMentioned = true
	return e
}
func threadCandidate() Envelope {
	e := threadEnvelope()
	e.RouteKind = "guild_thread_candidate"
	e.ParentChannelID = ""
	e.ThreadType = 0
	return e
}
func threadJSON() map[string]any {
	return map[string]any{"id": "6", "type": 11, "guild_id": "5", "parent_id": "2", "name": "Untrusted thread title", "thread_metadata": map[string]any{"archived": false, "locked": false}}
}
func parentJSON() map[string]any {
	return map[string]any{"id": "2", "type": 0, "guild_id": "5", "permission_overwrites": []any{}}
}
func threadBits() uint64 {
	return permissionViewChannel | permissionReadHistory | permissionSendThreads | permissionAddReactions
}
func threadREST(t *testing.T, tweak func(*http.Request, map[string]any), writes *atomic.Int32) *RESTClient {
	t.Helper()
	r := localREST(t, func(w http.ResponseWriter, q *http.Request) {
		var v map[string]any
		switch q.URL.Path {
		case "/users/@me":
			v = map[string]any{"id": "4", "bot": true}
		case "/channels/6":
			v = threadJSON()
		case "/channels/2":
			v = parentJSON()
		case "/guilds/5/members/4":
			v = map[string]any{"user": map[string]any{"id": "4", "bot": true}, "roles": []string{}}
		case "/guilds/5/roles":
			v = map[string]any{"id": "5", "permissions": fmt.Sprint(threadBits())}
			if tweak != nil {
				tweak(q, v)
			}
			json.NewEncoder(w).Encode([]any{v})
			return
		case "/channels/6/messages/3":
			v = map[string]any{"id": "3", "channel_id": "6", "type": 0, "author": map[string]any{"id": "1"}, "content": "real recovered text", "mentions": []any{map[string]any{"id": "4", "bot": true}}}
		default:
			if q.Method == http.MethodGet {
				t.Errorf("unexpected GET %s", q.URL.Path)
				w.WriteHeader(404)
				return
			}
			writes.Add(1)
			if !strings.HasPrefix(q.URL.Path, "/channels/6/") {
				t.Errorf("write escaped thread: %s", q.URL.Path)
			}
			if strings.HasSuffix(q.URL.Path, "/messages") {
				var body map[string]any
				json.NewDecoder(q.Body).Decode(&body)
				ref := body["message_reference"].(map[string]any)
				if ref["channel_id"] != "6" || ref["message_id"] != "3" {
					t.Error("reply reference escaped original thread")
				}
				json.NewEncoder(w).Encode(map[string]any{"id": "9", "channel_id": "6", "guild_id": "5", "author": map[string]any{"id": "4", "bot": true}, "content": body["content"], "nonce": body["nonce"], "message_reference": ref})
			} else {
				w.WriteHeader(204)
			}
			return
		}
		if tweak != nil {
			tweak(q, v)
		}
		json.NewEncoder(w).Encode(v)
	})
	r.settings.Policy = threadPolicy()
	return r
}
func threadStore(t *testing.T) (*Store, string) {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "thread.db")
	s, err := OpenStore(path, threadPolicy())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s, path
}
func readyThreadGateway() *gatewayState { g := &gatewayState{}; g.publishReady(0); return g }

func TestThreadQuarantinePromotionRestartAndReplyBinding(t *testing.T) {
	s, path := threadStore(t)
	e := threadEnvelope()
	e.ThreadName = "caller-forged title"
	// A caller-supplied verified envelope is downgraded to quarantine too.
	if outcome, err := s.Ingest(e); err != nil || outcome != "validation_staged" {
		t.Fatal(outcome, err)
	}
	in, err := s.NextValidation(epoch())
	if err != nil || in == nil || in.Event.RouteKind != "guild_thread_candidate" || in.Event.ThreadName != "" {
		t.Fatal(in, err)
	}
	if c, _ := s.ClaimNext(60, 0); c != nil {
		t.Fatal("candidate claimable")
	}
	if f, _ := s.FeedbackRows(); len(f) != 0 {
		t.Fatal("candidate feedback")
	}
	if outcome, _ := s.StageIngress(e); outcome != "duplicate" {
		t.Fatal(outcome)
	}
	s.Close()
	s, err = OpenStore(path, threadPolicy())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	next, _ := s.NextValidation(epoch())
	if next == nil || next.ID != in.ID || next.Event != in.Event || next.Created != in.Created {
		t.Fatal("restart lost exact candidate")
	}
	var writes atomic.Int32
	r := threadREST(t, nil, &writes)
	if err := processValidation(context.Background(), s, r, readyThreadGateway(), *next, 0); err != nil {
		t.Fatal(err)
	}
	c, err := s.ClaimNext(60, 0)
	if err != nil || c == nil {
		t.Fatal(c, err)
	}
	if c.InboundID != in.ID || c.Envelope.ConversationID != "6" || c.Envelope.ParentChannelID != "2" || c.Envelope.ThreadType != 11 || c.Envelope.ThreadName != "Untrusted thread title" || c.Envelope.Text != e.Text {
		t.Fatal(c)
	}
	id, err := s.QueueReply(c.InboundID, c.Claim, "answer")
	if err != nil {
		t.Fatal(err)
	}
	chunk, err := s.NextChunk()
	if err != nil || chunk == nil || chunk.ReplyID != id || chunk.Source != c.Envelope {
		t.Fatal(chunk, err)
	}
	got := r.Send(context.Background(), *chunk)
	if got.State != "sent" || writes.Load() != 1 {
		t.Fatal(got, writes.Load())
	}
	if err = s.RecordResult(*chunk, got); err != nil {
		t.Fatal(err)
	}
	if duplicate, _ := s.Ingest(e); duplicate != "duplicate" {
		t.Fatal(duplicate)
	}
}

func TestThreadValidationRejectsOtherRoutesAndUnsafeState(t *testing.T) {
	for _, tc := range []struct {
		name, path string
		mutate     func(map[string]any)
		want       string
	}{
		{"report_text_channel", "/channels/6", func(v map[string]any) { v["type"] = 0; delete(v, "parent_id") }, "rejected"},
		{"wrong_parent", "/channels/6", func(v map[string]any) { v["parent_id"] = "8" }, "rejected"},
		{"wrong_guild", "/channels/6", func(v map[string]any) { v["guild_id"] = "8" }, "rejected"},
		{"announcement_thread_under_text", "/channels/6", func(v map[string]any) { v["type"] = 10 }, "rejected"},
		{"forum", "/channels/6", func(v map[string]any) { v["type"] = 15 }, "rejected"},
		{"media", "/channels/6", func(v map[string]any) { v["type"] = 16 }, "rejected"},
		{"parent_became_announcement", "/channels/2", func(v map[string]any) { v["type"] = 5 }, "rejected"},
		{"parent_type_null", "/channels/2", func(v map[string]any) { v["type"] = nil }, "pending"},
		{"parent_wrong_guild", "/channels/2", func(v map[string]any) { v["guild_id"] = "8" }, "rejected"},
		{"archived", "/channels/6", func(v map[string]any) { v["thread_metadata"].(map[string]any)["archived"] = true }, "pending"},
		{"locked", "/channels/6", func(v map[string]any) { v["thread_metadata"].(map[string]any)["locked"] = true }, "pending"},
		{"missing_metadata", "/channels/6", func(v map[string]any) { delete(v, "thread_metadata") }, "pending"},
		{"missing_archived", "/channels/6", func(v map[string]any) { delete(v["thread_metadata"].(map[string]any), "archived") }, "pending"},
		{"missing_locked", "/channels/6", func(v map[string]any) { delete(v["thread_metadata"].(map[string]any), "locked") }, "pending"},
		{"private_not_member", "/channels/6", func(v map[string]any) { v["type"] = 12 }, "pending"},
		{"private_wrong_member", "/channels/6", func(v map[string]any) { v["type"] = 12; v["member"] = map[string]any{"id": "6", "user_id": "8"} }, "pending"},
		{"parent_missing_overwrites", "/channels/2", func(v map[string]any) { delete(v, "permission_overwrites") }, "pending"},
		{"member_missing_roles", "/guilds/5/members/4", func(v map[string]any) { delete(v, "roles") }, "pending"},
		{"member_wrong_identity", "/guilds/5/members/4", func(v map[string]any) { v["user"] = map[string]any{"id": "8", "bot": true} }, "pending"},
		{"normal_send_only", "/guilds/5/roles", func(v map[string]any) {
			v["permissions"] = fmt.Sprint(permissionViewChannel | permissionReadHistory | uint64(discordgo.PermissionSendMessages))
		}, "pending"},
		{"no_view", "/guilds/5/roles", func(v map[string]any) { v["permissions"] = fmt.Sprint(threadBits() &^ permissionViewChannel) }, "pending"},
		{"no_history", "/guilds/5/roles", func(v map[string]any) { v["permissions"] = fmt.Sprint(threadBits() &^ permissionReadHistory) }, "pending"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _ := threadStore(t)
			in := stageValidation(t, s, threadCandidate())
			var writes atomic.Int32
			r := threadREST(t, func(q *http.Request, v map[string]any) {
				if q.URL.Path == tc.path {
					tc.mutate(v)
				}
			}, &writes)
			if err := processValidation(context.Background(), s, r, readyThreadGateway(), *in, 0); err != nil {
				t.Fatal(err)
			}
			if c, _ := s.ClaimNext(60, 0); c != nil {
				t.Fatal("unsafe thread claimable")
			}
			if f, _ := s.FeedbackRows(); len(f) != 0 {
				t.Fatal("unsafe thread feedback")
			}
			var state string
			s.call(func(db *storeConn) (any, error) {
				return nil, db.QueryRow("SELECT state FROM ingress_validation WHERE id=?", in.ID).Scan(&state)
			})
			if state != tc.want || writes.Load() != 0 {
				t.Fatal(state, writes.Load())
			}
		})
	}
}

func TestThreadValidPrivateAndManagedLocked(t *testing.T) {
	for _, private := range []bool{false, true} {
		t.Run(fmt.Sprint(private), func(t *testing.T) {
			var writes atomic.Int32
			r := threadREST(t, func(q *http.Request, v map[string]any) {
				if q.URL.Path == "/channels/6" {
					v["thread_metadata"].(map[string]any)["locked"] = true
					if private {
						v["type"] = 12
						v["member"] = map[string]any{"id": "6", "user_id": "4"}
					}
				}
				if q.URL.Path == "/guilds/5/roles" {
					v["permissions"] = fmt.Sprint(threadBits() | permissionManageThreads)
				}
			}, &writes)
			e := threadEnvelope()
			if private {
				e.ThreadType = 12
			}
			c := testChunk()
			c.Source = e
			if got := r.Send(context.Background(), c); got.State != "sent" {
				t.Fatal(got)
			}
			for _, fn := range []func() error{func() error { return r.Typing(context.Background(), e) }, func() error { return r.Reaction(context.Background(), e, "👀") }, func() error { return r.RemoveReaction(context.Background(), e, "👀") }} {
				if err := fn(); err != nil {
					t.Fatal(err)
				}
			}
			if writes.Load() != 4 {
				t.Fatal(writes.Load())
			}
		})
	}
}

func TestThreadEveryWriteRejectsChangedRouteOrPermissions(t *testing.T) {
	for _, action := range []string{"send", "typing", "reaction", "remove"} {
		for _, change := range []string{"parent", "type", "archive", "permissions"} {
			t.Run(action+"_"+change, func(t *testing.T) {
				var writes atomic.Int32
				r := threadREST(t, func(q *http.Request, v map[string]any) {
					if q.URL.Path == "/channels/6" {
						switch change {
						case "parent":
							v["parent_id"] = "8"
						case "type":
							v["type"] = 12
						case "archive":
							v["thread_metadata"].(map[string]any)["archived"] = true
						}
					}
					if q.URL.Path == "/guilds/5/roles" && change == "permissions" {
						v["permissions"] = "0"
					}
				}, &writes)
				e := threadEnvelope()
				var err error
				switch action {
				case "send":
					c := testChunk()
					c.Source = e
					if got := r.Send(context.Background(), c); got.State != "failed" {
						t.Fatal(got)
					}
				case "typing":
					err = r.Typing(context.Background(), e)
				case "reaction":
					err = r.Reaction(context.Background(), e, "👀")
				case "remove":
					err = r.RemoveReaction(context.Background(), e, "👀")
				}
				if action != "send" && err == nil {
					t.Fatal("write allowed")
				}
				if writes.Load() != 0 {
					t.Fatal("unsafe write attempted")
				}
			})
		}
	}
}

func TestThreadArchiveDuringSlowPreflightOrBudgetWaitNeverPosts(t *testing.T) {
	for _, phase := range []string{"roles", "identity", "budget"} {
		t.Run(phase, func(t *testing.T) {
			var archived atomic.Bool
			var threadGets atomic.Int32
			var writes atomic.Int32
			r := threadREST(t, func(q *http.Request, v map[string]any) {
				if q.URL.Path == "/channels/6" {
					threadGets.Add(1)
					v["thread_metadata"].(map[string]any)["archived"] = archived.Load()
				}
				if phase == "roles" && q.URL.Path == "/guilds/5/roles" {
					archived.Store(true)
				}
				if phase == "identity" && q.URL.Path == "/users/@me" {
					deadline := time.Now().Add(time.Second)
					for threadGets.Load() == 0 && time.Now().Before(deadline) {
						time.Sleep(time.Millisecond)
					}
					archived.Store(true)
				}
			}, &writes)
			if phase == "budget" {
				r.limitMu.Lock()
				r.limits = map[string]time.Time{"POST:channels/6/messages": time.Now().Add(80 * time.Millisecond)}
				r.buckets = map[string]string{}
				r.limitMu.Unlock()
				time.AfterFunc(35*time.Millisecond, func() { archived.Store(true) })
			}
			c := testChunk()
			c.Source = threadEnvelope()
			got := r.Send(context.Background(), c)
			if got.State != "failed" || got.Code != "preflight_thread_archived" || writes.Load() != 0 || r.Diagnostics().Post.Attempted {
				t.Fatal(got, writes.Load())
			}
		})
	}
}

func TestRejectedThreadTombstonesDoNotConsumeCapacityOrBlockLaterEvents(t *testing.T) {
	s, _ := threadStore(t)
	_, err := s.call(func(db *storeConn) (any, error) {
		return transact(db, func(db *storeConn) (any, error) {
			for i := 0; i < 1010; i++ {
				if _, err := db.Exec("INSERT INTO ingress_validation(id,platform,event_id,envelope,created,state) VALUES(?,?,?,'{}',?,'rejected')", fmt.Sprint(i), "discord", fmt.Sprint(100+i), epoch()); err != nil {
					return nil, err
				}
			}
			return nil, nil
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	e0 := threadCandidate()
	e0.EventID = "1800"
	in0 := stageValidation(t, s, e0)
	if err := s.rejectValidation(in0.ID); err != nil {
		t.Fatal(err)
	}
	var active, total int
	s.call(func(db *storeConn) (any, error) {
		var err error
		active, err = activeInboundCount(db)
		if err != nil {
			return nil, err
		}
		return nil, db.QueryRow("SELECT count(*) FROM ingress_validation").Scan(&total)
	})
	if active != 0 || total != 1000 {
		t.Fatal(active, total)
	}
	e := threadCandidate()
	e.EventID = "2000"
	in := stageValidation(t, s, e)
	if in.Event.EventID != "2000" {
		t.Fatal("terminal sibling blocks future work")
	}
	if state, err := s.Ingest(testEnvelope()); err != nil || state != "accepted" {
		t.Fatal("rejected thread starved DM", state, err)
	}
}

func TestRecoverThreadFetchesRealOwnerMessageAndStagesOnly(t *testing.T) {
	for _, bad := range []string{"", "author", "webhook", "type", "channel", "guild"} {
		t.Run(bad, func(t *testing.T) {
			s, _ := threadStore(t)
			var writes atomic.Int32
			r := threadREST(t, func(q *http.Request, v map[string]any) {
				if q.URL.Path != "/channels/6/messages/3" {
					return
				}
				switch bad {
				case "author":
					v["author"] = map[string]any{"id": "8"}
				case "webhook":
					v["webhook_id"] = "8"
				case "type":
					v["type"] = 21
				case "channel":
					v["channel_id"] = "8"
				case "guild":
					v["guild_id"] = "8"
				}
			}, &writes)
			outcome, err := r.RecoverThreadMessage(context.Background(), s, "6", "3")
			if bad == "" {
				if err != nil || outcome != "validation_staged" {
					t.Fatal(outcome, err)
				}
				in, _ := s.NextValidation(epoch())
				if in == nil || in.Event.Text != "real recovered text" || in.Event.GuildID != "5" {
					t.Fatal(in)
				}
			} else if err == nil && outcome != "rejected" {
				t.Fatal("invalid message staged", outcome)
			}
			if c, _ := s.ClaimNext(60, 0); c != nil {
				t.Fatal("recovery bypassed quarantine")
			}
			if writes.Load() != 0 {
				t.Fatal("recovery wrote Discord")
			}
		})
	}
}

func TestThreadAdmissionGatewayOwnerAndRouteFilters(t *testing.T) {
	for _, change := range []string{"", "owner", "bot", "webhook", "guild", "mention", "type"} {
		t.Run(change, func(t *testing.T) {
			s, _ := threadStore(t)
			cfg := Settings{Policy: threadPolicy(), ExpectedBotID: "4"}
			m := &discordgo.Message{ID: "3", ChannelID: "6", GuildID: "5", Author: &discordgo.User{ID: "1"}, Content: "question", Mentions: []*discordgo.User{{ID: "4"}}}
			switch change {
			case "owner":
				m.Author.ID = "8"
			case "bot":
				m.Author.Bot = true
			case "webhook":
				m.WebhookID = "8"
			case "guild":
				m.GuildID = "8"
			case "mention":
				m.Mentions = nil
			case "type":
				m.Type = 21
			}
			outcome, err := receiveMessage(context.Background(), nil, s, cfg, m)
			want := "rejected"
			if change == "" {
				want = "validation_staged"
			}
			if err != nil || outcome != want {
				t.Fatal(outcome, err)
			}
		})
	}
}

func TestThreadTransportDenialKeepsQuarantineAndNoWrite(t *testing.T) {
	for _, status := range []int{401, 403, 404, 429, 503} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			s, _ := threadStore(t)
			in := stageValidation(t, s, threadCandidate())
			var calls atomic.Int32
			r := localREST(t, func(w http.ResponseWriter, q *http.Request) {
				calls.Add(1)
				w.WriteHeader(status)
				io.WriteString(w, "{}")
			})
			r.settings.Policy = threadPolicy()
			err := processValidation(context.Background(), s, r, readyThreadGateway(), *in, 0)
			if status == 401 {
				if err == nil || err.Error() != "authentication_failed" {
					t.Fatal(err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if c, _ := s.ClaimNext(60, 0); c != nil {
				t.Fatal("denied thread admitted")
			}
			if calls.Load() != 1 {
				t.Fatal(calls.Load())
			}
		})
	}
}

func TestThreadReversibleConditionsResumeWithoutReconnect(t *testing.T) {
	s, _ := threadStore(t)
	in := stageValidation(t, s, threadCandidate())
	var archived atomic.Bool
	archived.Store(true)
	var writes atomic.Int32
	r := threadREST(t, func(q *http.Request, v map[string]any) {
		if q.URL.Path == "/channels/6" {
			v["thread_metadata"].(map[string]any)["archived"] = archived.Load()
		}
	}, &writes)
	g := readyThreadGateway()
	if err := processValidation(context.Background(), s, r, g, *in, 0); err != nil {
		t.Fatal(err)
	}
	if next, _ := s.NextValidation(epoch()); next != nil {
		t.Fatal("read retry ignored backoff")
	}
	archived.Store(false)
	next, err := s.NextValidation(epoch() + 60)
	if err != nil || next == nil || next.ID != in.ID {
		t.Fatal("reopen cannot resume", next, err)
	}
	if err := processValidation(context.Background(), s, r, g, *next, 0); err != nil {
		t.Fatal(err)
	}
	if c, _ := s.ClaimNext(60, 0); c == nil {
		t.Fatal("reopened thread needs reconnect")
	}
}

func TestThreadPromotionRejectsChangedInputAndConnectionEpoch(t *testing.T) {
	s, _ := threadStore(t)
	in := stageValidation(t, s, threadCandidate())
	verified := threadEnvelope()
	verified.Text = "changed"
	if _, err := s.promoteThreadValidation(*in, verified); err == nil {
		t.Fatal("promotion altered user content")
	}
	var writes atomic.Int32
	g := readyThreadGateway()
	r := threadREST(t, func(q *http.Request, v map[string]any) {
		if q.URL.Path == "/guilds/5/roles" {
			g.disconnect()
		}
	}, &writes)
	if err := processValidation(context.Background(), s, r, g, *in, 0); err != nil {
		t.Fatal(err)
	}
	if c, _ := s.ClaimNext(60, 0); c != nil {
		t.Fatal("old connection promoted route")
	}
}

func TestThreadOnlyRouteGuardKeepsParentHidden(t *testing.T) {
	s, _ := threadStore(t)
	e := testEnvelope()
	e.RouteKind = "guild_text"
	e.GuildID = "5"
	e.BotMentioned = true
	in := stageValidation(t, s, e)
	var writes atomic.Int32
	r := threadREST(t, nil, &writes)
	r.routePermission = func(e Envelope) bool { return e.RouteKind == "guild_thread" }
	if err := processValidation(context.Background(), s, r, readyThreadGateway(), *in, 0); err != nil {
		t.Fatal(err)
	}
	if c, _ := s.ClaimNext(60, 0); c != nil {
		t.Fatal("thread-only gate admitted parent message")
	}
	if err := r.Typing(context.Background(), e); err == nil || writes.Load() != 0 {
		t.Fatal("thread-only gate posted parent feedback", err)
	}
}
