package bridge

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func BenchmarkMemoryRecall(b *testing.B) {
	for _, size := range []int{1000, 10000, 100000} {
		for _, query := range []string{"구글", "일본", "deployment"} {
			b.Run(fmt.Sprintf("rows_%d/%s", size, query), func(b *testing.B) {
				dir := b.TempDir()
				os.Chmod(dir, 0700)
				s, err := OpenStore(filepath.Join(dir, "memory.db"), ledgerPolicy{})
				if err != nil {
					b.Fatal(err)
				}
				defer s.Close()
				e := ledgerEvent("current", "c")
				scope := memoryScope(e)
				_, err = s.call(func(db *storeConn) (any, error) {
					return transact(db, func(db *storeConn) (any, error) {
						_, err := db.Exec(`WITH RECURSIVE seq(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM seq WHERE n<?) INSERT INTO memory_documents(record_key,scope,role,platform,event_id,source_revision,body,at,origin) SELECT 'bench:'||n,?,'user','discord','bench-source',0,'구글 프로젝트 일본 deployment schedule meeting notes',n,? FROM seq`, size, scope, memoryOriginJSON(Envelope{Platform: e.Platform, EventID: "bench-source", ConversationID: e.ConversationID, SenderID: e.SenderID, RouteKind: e.RouteKind}))
						if err != nil {
							return nil, err
						}
						if _, err = db.Exec(`INSERT INTO message_sources(platform,event_id,channel_id,guild_id,envelope) VALUES('discord','bench-source','c','','{}')`); err != nil {
							return nil, err
						}
						_, err = db.Exec(`INSERT INTO memory_terms(scope,term,doc_id) SELECT scope,t.term,id FROM memory_documents CROSS JOIN (SELECT 'c:구글' term UNION ALL SELECT 'c:일본' UNION ALL SELECT 'w:deployment') t`)
						return nil, err
					})
				})
				if err != nil {
					b.Fatal(err)
				}
				check, err := s.call(func(db *storeConn) (any, error) { return recallMemoryDB(db, "now", e, query) })
				if err != nil || len(check.(*MemoryRecall).Items) == 0 {
					b.Fatalf("benchmark must return actual recall: %v %v", check, err)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for n := 0; n < b.N; n++ {
					_, err = s.call(func(db *storeConn) (any, error) { return recallMemoryDB(db, "now", e, query) })
					if err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
			})
		}
	}
}
func BenchmarkMemoryIncrementalIndex(b *testing.B) {
	for _, lang := range []string{"korean", "english"} {
		b.Run(lang, func(b *testing.B) {
			dir := b.TempDir()
			os.Chmod(dir, 0700)
			s, err := OpenStore(filepath.Join(dir, "memory.db"), ledgerPolicy{})
			if err != nil {
				b.Fatal(err)
			}
			defer s.Close()
			text := "구글 일본 프로젝트 배포 일정은 금요일이고 회의에서 계획을 확인합니다"
			if lang == "english" {
				text = "Google Japan project deployment is scheduled for Friday and the team will confirm the plan in the meeting"
			}
			b.ReportAllocs()
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				e := ledgerEvent(fmt.Sprint(n), "c")
				e.Text = text
				_, err = s.call(func(db *storeConn) (any, error) {
					return transact(db, func(db *storeConn) (any, error) { return nil, rememberInboundDB(db, fmt.Sprint(n), e) })
				})
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
