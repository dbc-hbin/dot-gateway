package bridge

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestReadRouteActualQueriesRemainIndexedWithLargeHistory(t *testing.T) {
	for _, count := range []int{0, 20000} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			s, _, _ := catchupFixture(t)
			_, err := s.call(func(db *storeConn) (any, error) {
				return transact(db, func(db *storeConn) (any, error) {
					for i := 0; i < count; i++ {
						e := testEnvelope()
						e.ConversationID = "7"
						e.EventID = fmt.Sprint(i + 100)
						raw, _ := json.Marshal(e)
						if _, err := db.Exec(`INSERT INTO inbound(id,platform,event_id,envelope,state,created)VALUES(?,'discord',?,?,'ignored',?)`, fmt.Sprint(i), e.EventID, string(raw), float64(i)); err != nil {
							return nil, err
						}
					}
					e := testEnvelope()
					e.ConversationID = "7"
					return nil, rememberReadRouteDB(db, e)
				})
			})
			if err != nil {
				t.Fatal(err)
			}
			_, err = s.call(func(db *storeConn) (any, error) {
				cases := []struct {
					name, query, index string
					args               []any
				}{
					{"exact_route", readRouteLookupSQL, "inbound_read_route_lookup", []any{"7", "1"}},
					{"reconnect_routes", registeredReadRoutesSQL, "read_route_registry_owner", []any{"1", 128}},
					{"earlier_pending", catchupEarlierPendingSQL, "inbound_active_source_order", []any{"2", "2", epoch(), "9999999999999999999", "9999999999999999999", "9999999999999999999"}},
				}
				for _, tc := range cases {
					rows, err := db.Query("EXPLAIN QUERY PLAN "+tc.query, tc.args...)
					if err != nil {
						return nil, err
					}
					plan := ""
					for rows.Next() {
						var a, b, c int
						var d string
						if err = rows.Scan(&a, &b, &c, &d); err != nil {
							rows.Close()
							return nil, err
						}
						plan += d + "; "
					}
					rows.Close()
					if !strings.Contains(plan, tc.index) {
						t.Fatalf("%s unbounded plan: %s", tc.name, plan)
					}
					start := time.Now()
					for i := 0; i < 5; i++ {
						rows, err = db.Query(tc.query, tc.args...)
						if err != nil {
							return nil, err
						}
						for rows.Next() {
						}
						if err = rows.Err(); err != nil {
							rows.Close()
							return nil, err
						}
						rows.Close()
					}
					t.Logf("history=%d %s average=%s plan=%s", count, tc.name, time.Since(start)/5, plan)
				}
				return nil, nil
			})
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}
