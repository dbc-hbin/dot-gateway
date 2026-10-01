package bridge

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/bwmarrin/discordgo"
)

func TestCatchupLargeValidPageShrinksAndRecoversAllIDs(t *testing.T) {
	s, cfg, _ := catchupFixture(t)
	floor := catchupRow(t, s).Floor
	in := catchupForceGap(t, s, catchupAdd(floor, 100))
	requests := []int{}
	r := readREST(t, func(w http.ResponseWriter, q *http.Request) bool {
		if q.URL.Path != "/channels/2/messages" {
			return false
		}
		limit, _ := strconv.Atoi(q.URL.Query().Get("limit"))
		requests = append(requests, limit)
		before := q.URL.Query().Get("before")
		out := []any{}
		for n := uint64(100); n > 0 && len(out) < limit; n-- {
			id := catchupAdd(floor, n)
			if !snowLess(id, before) {
				continue
			}
			m := readFixtureMessage(id, "2", "1")
			m["content"] = strings.Repeat("한", 3900)
			out = append(out, m)
		}
		json.NewEncoder(w).Encode(out)
		return true
	})
	for step := 0; step < 20; step++ {
		messages, err := r.historyPage(context.Background(), in.Route, in.Before, in.PageLimit)
		if step == 0 {
			if err == nil || err.Error() != "history_body_incomplete" {
				t.Fatal("expected bounded large-page failure", err)
			}
		}
		if err != nil {
			if err = s.deferCatchup(*in, err.Error()); err != nil {
				t.Fatal(err)
			}
		} else {
			if _, err = s.applyCatchupPage(cfg, *in, messages); err != nil {
				t.Fatal(err)
			}
		}
		row := catchupRow(t, s)
		if step == 0 && (row.PageLimit != 50 || row.Floor != floor || row.Before != in.Before) {
			t.Fatal("shrink moved cursor", row)
		}
		if row.State == "idle" {
			break
		}
		in, err = s.nextCatchup(epoch() + 100)
		if err != nil || in == nil {
			t.Fatal(in, err)
		}
	}
	row := catchupRow(t, s)
	if row.State != "idle" || row.Floor != catchupAdd(floor, 100) || len(requests) < 2 || requests[0] != 100 || requests[1] != 50 {
		t.Fatal(row, requests)
	}
	var rows, distinct int
	_, err := s.call(func(db *storeConn) (any, error) {
		e := db.QueryRow(`SELECT count(*),count(DISTINCT event_id) FROM ingress_validation`).Scan(&rows, &distinct)
		return nil, e
	})
	if err != nil || rows != 100 || distinct != 100 {
		t.Fatal(rows, distinct, err)
	}
}
func TestCatchupSparseFullOldestPageDoesNotLoopForever(t *testing.T) {
	s, cfg, _ := catchupFixture(t)
	floor := catchupRow(t, s).Floor
	in := catchupForceGap(t, s, catchupAdd(floor, 300))
	var page []*discordgo.Message
	for n := uint64(200); n > 100; n-- {
		page = append(page, catchupMessage(t, in.Route, catchupAdd(floor, n), "1"))
	}
	if out, err := s.applyCatchupPage(cfg, *in, page); err != nil || out != "page_pending" {
		t.Fatal(out, err)
	}
	in, _ = s.nextCatchup(epoch() + 100)
	if _, err := s.applyCatchupPage(cfg, *in, nil); err != nil {
		t.Fatal(err)
	}
	in, _ = s.nextCatchup(epoch() + 100)
	if _, err := s.applyCatchupPage(cfg, *in, page); err != nil {
		t.Fatal(err)
	}
	if row := catchupRow(t, s); row.State != "idle" || row.Floor != catchupAdd(floor, 300) {
		t.Fatal("full oldest page repeatedly discarded", row)
	}
}
func TestCatchupProofDecodeFailureDoesNotShrinkHistoryPage(t *testing.T) {
	s, _, _ := catchupFixture(t)
	in := catchupForceGap(t, s, catchupAdd(catchupRow(t, s).Floor, 10))
	r := readREST(t, func(w http.ResponseWriter, q *http.Request) bool {
		if q.URL.Path == "/guilds/5/roles" {
			w.Write([]byte(`{}`))
			return true
		}
		return false
	})
	_, err := r.historyPage(context.Background(), in.Route, in.Before, in.PageLimit)
	if err == nil || err.Error() != "preflight_invalid_ack" {
		t.Fatal(err)
	}
	if err = s.deferCatchup(*in, err.Error()); err != nil {
		t.Fatal(err)
	}
	if row := catchupRow(t, s); row.PageLimit != 100 {
		t.Fatal(row)
	}
	for i := 0; i < 9; i++ {
		in, _ = s.nextCatchup(epoch() + 100)
		if err = s.deferCatchup(*in, "history_body_incomplete"); err != nil {
			t.Fatal(err)
		}
	}
	if row := catchupRow(t, s); row.PageLimit != 1 || row.State != "gap" || row.Floor != in.Floor {
		t.Fatal("limit1 must remain incomplete", row)
	}
}
