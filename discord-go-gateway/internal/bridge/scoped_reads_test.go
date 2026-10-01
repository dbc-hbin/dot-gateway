package bridge

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
)

func readFixtureMessage(id, channel, author string) map[string]any {
	return map[string]any{"id": id, "channel_id": channel, "type": 0, "author": map[string]any{"id": author, "bot": false}, "content": "real owner request " + id, "mentions": []any{map[string]any{"id": "4", "bot": true}}, "attachments": []any{}, "embeds": []any{}}
}
func readREST(t *testing.T, handler func(http.ResponseWriter, *http.Request) bool) *RESTClient {
	t.Helper()
	r := localREST(t, func(w http.ResponseWriter, q *http.Request) {
		if q.Method != "GET" {
			t.Errorf("unexpected mutation %s", q.Method)
			w.WriteHeader(500)
			return
		}
		if handler != nil && handler(w, q) {
			return
		}
		var v any
		switch q.URL.Path {
		case "/users/@me":
			v = map[string]any{"id": "4", "bot": true}
		case "/channels/2":
			v = parentJSON()
		case "/channels/6":
			v = threadJSON()
		case "/channels/7":
			v = map[string]any{"id": "7", "type": 1, "recipients": []any{map[string]any{"id": "1"}}}
		case "/guilds/5/members/4":
			v = map[string]any{"user": map[string]any{"id": "4", "bot": true}, "roles": []string{}}
		case "/guilds/5/roles":
			v = []any{map[string]any{"id": "5", "permissions": fmt.Sprint(permissionViewChannel | permissionReadHistory)}}
		default:
			t.Errorf("unexpected path %s", q.URL.Path)
			w.WriteHeader(404)
			return
		}
		json.NewEncoder(w).Encode(v)
	})
	r.settings.Policy = threadPolicy()
	return r
}
func TestScopedReadHistoryIndependentPermissionsAndNoAdmission(t *testing.T) {
	s, _ := threadStore(t)
	var calls atomic.Int32
	r := readREST(t, func(w http.ResponseWriter, q *http.Request) bool {
		if q.URL.Path == "/channels/2/messages" {
			calls.Add(1)
			if q.URL.Query().Get("before") != "100" || q.URL.Query().Get("limit") != "2" {
				t.Error(q.URL.RawQuery)
			}
			json.NewEncoder(w).Encode([]any{readFixtureMessage("99", "2", "1"), readFixtureMessage("98", "2", "9")})
			return true
		}
		return false
	})
	r.routePermission = func(Envelope) bool { return false }
	out, err := r.ReadHistory(context.Background(), s, "2", "100", 2)
	if err != nil || len(out.Messages) != 2 || !out.HasMore || out.NextBefore != "98" || out.Trust != "untrusted_external_context" || out.Complete {
		t.Fatal(out, err)
	}
	if calls.Load() != 1 {
		t.Fatal(calls.Load())
	}
	if in, _ := s.NextValidation(epoch()); in != nil {
		t.Fatal("context admitted")
	}
	if claim, _ := s.ClaimNext(30, 0); claim != nil {
		t.Fatal("context claimed")
	}
	if _, err = r.ReadHistory(context.Background(), s, "8", "", 1); err == nil || err.Error() != "read_route_not_verified" {
		t.Fatal(err)
	}
}
func TestScopedReadHistoryRejectsWholeMalformedPage(t *testing.T) {
	tests := map[string]any{"null": nil, "oversize": []any{readFixtureMessage("90", "2", "1"), readFixtureMessage("80", "2", "1"), readFixtureMessage("70", "2", "1")}, "duplicate": []any{readFixtureMessage("90", "2", "1"), readFixtureMessage("90", "2", "1")}, "reversed": []any{readFixtureMessage("80", "2", "1"), readFixtureMessage("90", "2", "1")}, "cross_channel": []any{readFixtureMessage("90", "6", "1")}, "outside_cursor": []any{readFixtureMessage("100", "2", "1")}, "missing_fields": []any{map[string]any{"id": "90", "channel_id": "2", "type": 0, "author": map[string]any{"id": "1"}, "content": "partial"}}}
	for name, response := range tests {
		t.Run(name, func(t *testing.T) {
			s, _ := threadStore(t)
			r := readREST(t, func(w http.ResponseWriter, q *http.Request) bool {
				if q.URL.Path == "/channels/2/messages" {
					json.NewEncoder(w).Encode(response)
					return true
				}
				return false
			})
			if _, err := r.ReadHistory(context.Background(), s, "2", "100", 2); err == nil {
				t.Fatal("accepted malformed page")
			}
		})
	}
}
func TestScopedReadArchivedPrivateMembershipAndRevocation(t *testing.T) {
	s, _ := threadStore(t)
	e := threadEnvelope()
	e.ThreadType = 12
	if _, err := s.StageIngress(e); err != nil {
		t.Fatal(err)
	}
	in, _ := s.NextValidation(epoch())
	if _, err := s.promoteThreadValidation(*in, e); err != nil {
		t.Fatal(err)
	}
	lost := false
	r := readREST(t, func(w http.ResponseWriter, q *http.Request) bool {
		if q.URL.Path == "/channels/6" {
			v := threadJSON()
			v["type"] = 12
			v["thread_metadata"] = map[string]any{"archived": true, "locked": true}
			if !lost {
				v["member"] = map[string]any{"id": "6", "user_id": "4"}
			}
			json.NewEncoder(w).Encode(v)
			return true
		}
		if q.URL.Path == "/channels/6/messages/10" {
			json.NewEncoder(w).Encode(readFixtureMessage("10", "6", "1"))
			return true
		}
		return false
	})
	if out, err := r.ReadMessage(context.Background(), s, "6", "10"); err != nil || len(out.Messages) != 1 {
		t.Fatal(out, err)
	}
	lost = true
	if _, err := r.ReadMessage(context.Background(), s, "6", "10"); err == nil || err.Error() != "read_thread_membership_required" {
		t.Fatal(err)
	}
}
func TestScopedReadPinsTimestampAndSearchIndexing(t *testing.T) {
	s, _ := threadStore(t)
	mode := 0
	r := readREST(t, func(w http.ResponseWriter, q *http.Request) bool {
		switch q.URL.Path {
		case "/channels/2/messages/pins":
			if q.URL.Query().Get("before") != "2026-10-01T00:00:00Z" {
				t.Error(q.URL.RawQuery)
			}
			json.NewEncoder(w).Encode(map[string]any{"items": []any{map[string]any{"pinned_at": "2026-09-30T22:00:00Z", "message": readFixtureMessage("90", "2", "1")}}, "has_more": true})
			return true
		case "/guilds/5/messages/search":
			if q.URL.Query().Get("channel_id") != "2" || q.URL.Query().Get("author_id") != "1" || q.URL.Query().Get("content") != "a&b" {
				t.Error(q.URL.RawQuery)
			}
			if mode == 0 {
				w.WriteHeader(202)
				fmt.Fprint(w, `{"retry_after":2}`)
			} else {
				json.NewEncoder(w).Encode(map[string]any{"messages": [][]any{{readFixtureMessage("90", "2", "1")}}, "total_results": 2, "doing_deep_historical_index": false})
			}
			return true
		}
		return false
	})
	out, err := r.ReadPins(context.Background(), s, "2", "2026-10-01T00:00:00Z", 2)
	if err != nil || out.NextBefore != "2026-09-30T22:00:00Z" || !out.HasMore {
		t.Fatal(out, err)
	}
	out, err = r.SearchMessages(context.Background(), s, "2", "a&b", 0, 1)
	if err != nil || out.Status != "indexing" || out.RetryAfter != 2 || out.Complete {
		t.Fatal(out, err)
	}
	mode = 1
	out, err = r.SearchMessages(context.Background(), s, "2", "a&b", 0, 1)
	if err != nil || out.Complete || !out.HasMore || out.NextOffset != 1 {
		t.Fatal(out, err)
	}
	if _, err = r.ReadPins(context.Background(), s, "2", "90", 2); err == nil {
		t.Fatal("snowflake pin cursor")
	}
	if _, err = r.SearchMessages(context.Background(), s, "2", strings.Repeat("x", 1025), 0, 1); err == nil {
		t.Fatal("oversize search")
	}
}
func TestScopedReadDMRequiresKnownRouteAndExactRecipient(t *testing.T) {
	s, _ := threadStore(t)
	r := readREST(t, func(w http.ResponseWriter, q *http.Request) bool {
		if q.URL.Path == "/channels/7/messages/10" {
			json.NewEncoder(w).Encode(readFixtureMessage("10", "7", "1"))
			return true
		}
		return false
	})
	if _, err := r.ReadMessage(context.Background(), s, "7", "10"); err == nil {
		t.Fatal("unknown dm read")
	}
	e := threadEnvelope()
	e.RouteKind = "dm"
	e.GuildID = ""
	e.ParentChannelID = ""
	e.ThreadType = 0
	e.ConversationID = "7"
	if _, err := s.Ingest(e); err != nil {
		t.Fatal(err)
	}
	if _, err := r.ReadMessage(context.Background(), s, "7", "10"); err != nil {
		t.Fatal(err)
	}
}

func TestScopedReadPermissionRevokedAfterResponse(t *testing.T) {
	s, _ := threadStore(t)
	revoked := false
	r := readREST(t, func(w http.ResponseWriter, q *http.Request) bool {
		if q.URL.Path == "/channels/2/messages/pins" {
			revoked = true
			json.NewEncoder(w).Encode(map[string]any{"items": []any{map[string]any{"pinned_at": "2026-09-30T22:00:00Z", "message": readFixtureMessage("90", "2", "1")}}, "has_more": false})
			return true
		}
		if q.URL.Path == "/guilds/5/roles" && revoked {
			json.NewEncoder(w).Encode([]any{map[string]any{"id": "5", "permissions": "0"}})
			return true
		}
		return false
	})
	out, err := r.ReadPins(context.Background(), s, "2", "", 2)
	if err == nil || err.Error() != "read_permissions_missing" || len(out.Messages) != 0 {
		t.Fatal("exposed a revoked page", out, err)
	}
}
