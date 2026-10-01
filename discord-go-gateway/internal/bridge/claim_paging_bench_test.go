package bridge

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// BenchmarkClaimPendingBacklog measures one fresh, durable claim with an 8000-
// character envelope and an unchanged pending backlog. Setup/reset is excluded;
// each iteration starts with no live claim, exactly backlog rows, and no timings.
// Both sizes use temporary file-backed SQLite with the normal durability settings.
func BenchmarkClaimPendingBacklog(b *testing.B) {
	for _, backlog := range []int{1, 1000} {
		b.Run(fmt.Sprintf("rows_%d_text_8000", backlog), func(b *testing.B) {
			dir := b.TempDir()
			if err := os.Chmod(dir, 0700); err != nil {
				b.Fatal(err)
			}
			s, err := OpenStore(filepath.Join(dir, "queue.db"), ledgerPolicy{})
			if err != nil {
				b.Fatal(err)
			}
			defer s.Close()
			_, err = s.call(func(db *storeConn) (any, error) {
				return transact(db, func(db *storeConn) (any, error) {
					for i := 0; i < backlog; i++ {
						id := fmt.Sprintf("row-%04d", i)
						e := ledgerEvent(id, "conversation")
						e.Text = strings.Repeat("x", 8000)
						raw, err := json.Marshal(e)
						if err != nil {
							return nil, err
						}
						if _, err := db.Exec("INSERT INTO inbound(id,platform,event_id,envelope,created) VALUES(?,?,?,?,?)", id, e.Platform, e.EventID, string(raw), float64(i+1)); err != nil {
							return nil, err
						}
					}
					return nil, nil
				})
			})
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				claim, err := s.ClaimNext(300, 0)
				b.StopTimer()
				if err != nil || claim == nil || claim.InboundID != "row-0000" {
					b.Fatalf("claim=%#v error=%v", claim, err)
				}
				_, err = s.call(func(db *storeConn) (any, error) {
					return transact(db, func(db *storeConn) (any, error) {
						if _, err := db.Exec("UPDATE inbound SET state='pending',claim=NULL,lease_until=NULL WHERE id=?", claim.InboundID); err != nil {
							return nil, err
						}
						_, err := db.Exec("DELETE FROM timings")
						return nil, err
					})
				})
				if err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
			}
		})
	}
}
