package bridge

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
)

func memoryIngest(t *testing.T, s *Store, id, conv, text string) Envelope {
	t.Helper()
	e := ledgerEvent(id, conv)
	e.Text = text
	if st, err := s.Ingest(e); err != nil || st != "accepted" {
		t.Fatal(st, err)
	}
	return e
}
func memoryContains(m *MemoryRecall, text string) bool {
	for _, i := range m.Items {
		if strings.Contains(i.Text, text) {
			return true
		}
	}
	return false
}
func TestMemoryClaimRecallAndExactScope(t *testing.T) {
	s, path := testStore(t)
	memoryIngest(t, s, "one", "private-dm", "구글 프로젝트는 금요일 배포한다")
	c := claimLedger(t, s)
	if c.Memory == nil || c.Memory.Status != "ready" || c.Memory.Trust != memoryTrust {
		t.Fatalf("%+v", c.Memory)
	}
	m := MemoryMutation{Key: "project.release", Kind: "project", Text: "구글 프로젝트 배포 예정일은 금요일", Sources: []MemoryRef{c.Memory.CurrentSource}}
	fact, err := s.PutMemory(c.InboundID, c.Claim, m)
	if err != nil || fact.Version != 1 {
		t.Fatal(fact, err)
	}
	if err = s.Ignore(c.InboundID, c.Claim); err != nil {
		t.Fatal(err)
	}
	memoryIngest(t, s, "two", "public-channel", "구글 배포 언제?")
	public := claimLedger(t, s)
	if memoryContains(public.Memory, "금요일") {
		t.Fatal("private memory crossed channel")
	}
	s.Ignore(public.InboundID, public.Claim)
	memoryIngest(t, s, "three", "private-dm", "구글 배포 언제?")
	current := claimLedger(t, s)
	if !memoryContains(current.Memory, "금요일") {
		t.Fatalf("missing recall %+v", current.Memory)
	}
	// Same conversation ID cannot bridge guild/route or changed owner identity.
	if memoryScope(current.Envelope) == memoryScope(Envelope{Platform: "discord", ConversationID: "private-dm", SenderID: "owner", RouteKind: "guild_channel", GuildID: "guild"}) {
		t.Fatal("scope collision")
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := OpenStore(path, ledgerPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	recalled, err := s2.RecallMemory(current.InboundID, current.Claim, "구글")
	if err != nil || !memoryContains(recalled, "금요일") {
		t.Fatal(recalled, err)
	}
}
func TestMemorySemanticCASClaimAndProvenance(t *testing.T) {
	s, _ := testStore(t)
	memoryIngest(t, s, "one", "c", "간단한 설명을 선호해")
	c := claimLedger(t, s)
	m := MemoryMutation{Key: "style", Kind: "preference", Text: "사용자는 간단한 설명을 선호한다", Sources: []MemoryRef{c.Memory.CurrentSource}}
	if _, err := s.PutMemory(c.InboundID, "wrong", m); !errors.Is(err, ErrClaim) {
		t.Fatal(err)
	}
	fact, err := s.PutMemory(c.InboundID, c.Claim, m)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.PutMemory(c.InboundID, c.Claim, m); err == nil || err.Error() != "memory_version_conflict" {
		t.Fatal(err)
	}
	m.ExpectedVersion = fact.Version
	m.Sources[0].SourceRevision++
	if _, err = s.PutMemory(c.InboundID, c.Claim, m); err == nil || err.Error() != "memory_source_not_current" {
		t.Fatal(err)
	}
	m.Sources[0] = c.Memory.CurrentSource
	m.Text = "password: hunter2"
	if _, err = s.PutMemory(c.InboundID, c.Claim, m); err == nil || err.Error() != "memory_sensitive_content_rejected" {
		t.Fatal(err)
	}
	m.Delete = true
	m.Text = ""
	deleted, err := s.PutMemory(c.InboundID, c.Claim, m)
	if err != nil || deleted.Version != 2 {
		t.Fatal(deleted, err)
	}
	state, err := s.MemoryFactState(c.InboundID, c.Claim, "style")
	if err != nil || state["active"] != false || state["version"] != int64(2) {
		t.Fatal(state, err)
	}
}
func TestMemoryOnlyAcknowledgedSentChunks(t *testing.T) {
	s, _ := testStore(t)
	memoryIngest(t, s, "one", "c", "배포 설명해")
	c := claimLedger(t, s)
	rid := replyLedger(t, s, c, strings.Repeat("x", 1900)+"still uncertain remainder")
	chunk, err := s.NextChunk()
	if err != nil {
		t.Fatal(err)
	}
	if err = s.RecordResult(*chunk, SendResult{State: "sent", MessageID: "remote-one"}); err != nil {
		t.Fatal(err)
	}
	chunk, err = s.NextChunk()
	if err != nil || chunk == nil {
		t.Fatal(chunk, err)
	}
	if err = s.RecordResult(*chunk, SendResult{State: "uncertain", Code: "timeout"}); err != nil {
		t.Fatal(err)
	}
	memoryIngest(t, s, "two", "c", "다시 설명")
	next := claimLedger(t, s)
	var got bool
	for _, i := range next.Memory.Items {
		if i.Role == "assistant" {
			got = true
			if i.RemoteMessageID != "remote-one" || i.Delivery != "partial_sent_reply_uncertain" || strings.Contains(i.Text, "still uncertain") {
				t.Fatalf("false delivery %#v", i)
			}
		}
	}
	if !got {
		t.Fatal("no sent history")
	}
	if err = s.ResolveSent(rid, 1, "remote-two"); err != nil {
		t.Fatal(err)
	}
	m, err := s.RecallMemory(next.InboundID, next.Claim, "uncertain")
	if err != nil || !memoryContains(m, "still uncertain") {
		t.Fatal(m, err)
	}
}
func TestMemoryInvalidateDeleteForgetNoResurrection(t *testing.T) {
	s, _ := testStore(t)
	e := memoryIngest(t, s, "one", "c", "일본 출장은 금요일")
	c := claimLedger(t, s)
	m := MemoryMutation{Key: "trip", Kind: "fact", Text: "일본 출장은 금요일", Sources: []MemoryRef{c.Memory.CurrentSource}}
	if _, err := s.PutMemory(c.InboundID, c.Claim, m); err != nil {
		t.Fatal(err)
	}
	if changed, err := s.InvalidateSourceUpdate("c", "", "one"); !changed || err != nil {
		t.Fatal(changed, err)
	}
	v, err := s.call(func(db *storeConn) (any, error) { return recallMemoryDB(db, "new", e, "일본") })
	if err != nil {
		t.Fatal(err)
	}
	if memoryContains(v.(*MemoryRecall), "금요일") {
		t.Fatal("invalidated text recalled")
	}
	refresh, err := s.NextSourceRefresh(epoch())
	if err != nil {
		t.Fatal(err)
	}
	if st, err := s.ApplySourceRefresh(*refresh, e); st != "unchanged" || err != nil {
		t.Fatal(st, err)
	}
	c = claimLedger(t, s)
	state, err := s.MemoryFactState(c.InboundID, c.Claim, "trip")
	if err != nil || state["active"] != false {
		t.Fatal("facts silently reactivated", state, err)
	}
	if err = s.ForgetMemory(c.InboundID, c.Claim, c.Memory.CurrentSource); err != nil {
		t.Fatal(err)
	}
	if _, err = s.BackfillMemory(100); err != nil {
		t.Fatal(err)
	}
	v, err = s.call(func(db *storeConn) (any, error) { return recallMemoryDB(db, "new", e, "일본") })
	if err != nil || memoryContains(v.(*MemoryRecall), "금요일") {
		t.Fatal("forget resurrected", v, err)
	}
	if changed, err := s.DeleteSource("c", "", "one"); !changed || err != nil {
		t.Fatal(changed, err)
	}
	_, err = s.call(func(db *storeConn) (any, error) {
		var count int
		err := db.QueryRow(`SELECT count(*) FROM memory_documents WHERE event_id='one' AND body!=''`).Scan(&count)
		if count != 0 {
			t.Fatal("deleted body retained")
		}
		return nil, err
	})
	if err != nil {
		t.Fatal(err)
	}
}
func TestMemoryRedactsBeforeIndexAndBoundedRecall(t *testing.T) {
	s, _ := testStore(t)
	secret := "sk-proj-abcdefghijklmnopqrstuvwxyz0123456789"
	e := memoryIngest(t, s, "one", "c", "검색 가능한 설명\npassword = hunter2\n"+secret+"\nhttps://example.invalid/?token=tokenvalue\n"+strings.Repeat("긴문장 ", 4000))
	c := claimLedger(t, s)
	v, err := s.call(func(db *storeConn) (any, error) {
		var body string
		err := db.QueryRow(`SELECT body FROM memory_documents WHERE record_key=?`, c.Memory.CurrentSource.DocumentID).Scan(&body)
		return body, err
	})
	if err != nil {
		t.Fatal(err)
	}
	body := v.(string)
	if strings.Contains(body, "hunter2") || strings.Contains(body, secret) || strings.Contains(body, "tokenvalue") || len([]rune(body)) > memoryBodyLimit {
		t.Fatal("unsafe or unbounded body")
	}
	s.Ignore(c.InboundID, c.Claim)
	memoryIngest(t, s, "two", "c", "설명")
	current := claimLedger(t, s)
	total := 0
	for _, i := range current.Memory.Items {
		total += len([]rune(i.Text))
	}
	if total > memoryRecallBudget || len(current.Memory.Items) > 12 {
		t.Fatal("unbounded recall")
	}
	if memoryScope(e) == "" {
		t.Fatal("no scope")
	}
}
func TestMemoryIndexFailureDoesNotFailIngestClaimOrSentACK(t *testing.T) {
	s, _ := testStore(t)
	_, err := s.call(func(db *storeConn) (any, error) { _, err := db.Exec(`DROP TABLE memory_terms`); return nil, err })
	if err != nil {
		t.Fatal(err)
	}
	memoryIngest(t, s, "one", "c", "normal question")
	c := claimLedger(t, s)
	if c.Memory.Status != "unavailable" {
		t.Fatal(c.Memory)
	}
	rid := replyLedger(t, s, c, "normal answer")
	chunk, err := s.NextChunk()
	if err != nil {
		t.Fatal(err)
	}
	if err = s.RecordResult(*chunk, SendResult{State: "sent", MessageID: "remote"}); err != nil {
		t.Fatal(err)
	}
	d, err := s.Delivery(rid)
	if err != nil || d.State != "sent" {
		t.Fatal(d, err)
	}
}
func TestMemoryBackfillExplicitPagedAndQueryPlan(t *testing.T) {
	s, path := testStore(t)
	_, err := s.call(func(db *storeConn) (any, error) {
		return transact(db, func(db *storeConn) (any, error) {
			for i := 0; i < 7; i++ {
				e := ledgerEvent(fmt.Sprint(i), "c")
				e.Text = "구글과 일본 배포"
				raw, _ := json.Marshal(e)
				if _, err := db.Exec(`INSERT INTO inbound(id,platform,event_id,envelope,state,created) VALUES(?,'discord',?,?,'ignored',?)`, fmt.Sprint(i), e.EventID, string(raw), epoch()); err != nil {
					return nil, err
				}
			}
			return nil, nil
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
	s, err = OpenStore(path, ledgerPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	_, err = s.call(func(db *storeConn) (any, error) {
		var count int
		err := db.QueryRow(`SELECT count(*) FROM memory_documents`).Scan(&count)
		if count != 0 {
			t.Fatal("open backfilled history")
		}
		return nil, err
	})
	if err != nil {
		t.Fatal(err)
	}
	r, err := s.BackfillMemory(2)
	if err != nil || r["inbound_examined"] != 2 || r["more"] != true {
		t.Fatal(r, err)
	}
	r, err = s.BackfillMemory(2)
	if err != nil || r["inbound_cursor"] != int64(4) {
		t.Fatal(r, err)
	}
	_, err = s.call(func(db *storeConn) (any, error) {
		rows, err := db.Query(`EXPLAIN QUERY PLAN SELECT doc_id FROM memory_terms WHERE scope=? AND term=? ORDER BY doc_id DESC LIMIT 32`, memoryScope(ledgerEvent("", "c")), "c:일본")
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		for rows.Next() {
			var a, b, c int
			var plan string
			if err = rows.Scan(&a, &b, &c, &plan); err != nil {
				return nil, err
			}
			if !strings.Contains(plan, "SEARCH memory_terms USING PRIMARY KEY") {
				t.Fatal(plan)
			}
		}
		return nil, rows.Err()
	})
	if err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(path); err != nil || info.Mode().Perm() != 0600 {
		t.Fatal(info, err)
	}
}
func TestMemoryBackupPrivateSanitizedAndRestore(t *testing.T) {
	s, _ := testStore(t)
	memoryIngest(t, s, "one", "c", "일본 출장은 금요일\npassword=hunter2")
	c := claimLedger(t, s)
	if _, err := s.PutMemory(c.InboundID, c.Claim, MemoryMutation{Key: "trip", Kind: "project", Text: "일본 출장은 금요일", Sources: []MemoryRef{c.Memory.CurrentSource}}); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	os.Chmod(dir, 0700)
	path := dir + "/private.memory.jsonl"
	result, err := s.ExportMemory(path)
	if err != nil || result["documents"] != 2 {
		t.Fatal(result, err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "hunter2") || strings.Contains(string(data), c.Claim) {
		t.Fatal("raw secret/claim exported")
	}
	if info, err := os.Stat(path); err != nil || info.Mode().Perm() != 0600 {
		t.Fatal(info, err)
	}
	if _, err = s.ExportMemory(path); err == nil {
		t.Fatal("backup overwritten")
	}
	// Restore only derived rows, keeping the authoritative source/delivery ledger.
	_, err = s.call(func(db *storeConn) (any, error) {
		_, err := db.Exec(`DELETE FROM memory_fact_sources;DELETE FROM memory_facts;DELETE FROM memory_documents;`)
		return nil, err
	})
	if err != nil {
		t.Fatal(err)
	}
	restored, err := s.ImportMemory(path, memoryFileSHA(t, path))
	if err != nil || restored["documents_restored"] != 2 {
		t.Fatal(restored, err)
	}
	recalled, err := s.RecallMemory(c.InboundID, c.Claim, "일본")
	if err != nil || !memoryContains(recalled, "금요일") {
		t.Fatal(recalled, err)
	}
	if err = s.ForgetMemory(c.InboundID, c.Claim, c.Memory.CurrentSource); err != nil {
		t.Fatal(err)
	}
	if _, err = s.ImportMemory(path, memoryFileSHA(t, path)); err != nil {
		t.Fatal(err)
	}
	recalled, err = s.RecallMemory(c.InboundID, c.Claim, "일본")
	if err != nil || memoryContains(recalled, "금요일") {
		t.Fatal("older backup resurrected forget", recalled, err)
	}
	// A corrupt archive rolls back ALL rows, not a partial restore.
	broken := dir + "/broken.memory.jsonl"
	os.WriteFile(broken, data[:len(data)-10], 0600)
	if _, err = s.ImportMemory(broken, memoryFileSHA(t, broken)); err == nil {
		t.Fatal("accepted incomplete backup")
	}
	other, _ := testStore(t)
	if _, err = other.ImportMemory(path, memoryFileSHA(t, path)); err == nil || err.Error() != "memory_backup_ledger_mismatch" {
		t.Fatal(err)
	}
}
func TestMemoryBackupSentAndDeletedTombstones(t *testing.T) {
	s, _ := testStore(t)
	memoryIngest(t, s, "one", "c", "구글 배포")
	c := claimLedger(t, s)
	replyLedger(t, s, c, "배포 완료")
	chunk, err := s.NextChunk()
	if err != nil {
		t.Fatal(err)
	}
	s.RecordResult(*chunk, SendResult{State: "sent", MessageID: "remote"})
	if _, err = s.DeleteSource("c", "", "one"); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	os.Chmod(dir, 0700)
	path := dir + "/deleted.memory.jsonl"
	if _, err = s.ExportMemory(path); err != nil {
		t.Fatal(err)
	}
	_, err = s.call(func(db *storeConn) (any, error) {
		_, err := db.Exec(`DELETE FROM memory_fact_sources;DELETE FROM memory_facts;DELETE FROM memory_documents;`)
		return nil, err
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.ImportMemory(path, memoryFileSHA(t, path)); err != nil {
		t.Fatal(err)
	}
	_, err = s.call(func(db *storeConn) (any, error) {
		var n int
		err := db.QueryRow(`SELECT count(*) FROM memory_documents WHERE body!='' OR active!=0`).Scan(&n)
		if n != 0 {
			t.Fatal("delete resurrected through backup")
		}
		return nil, err
	})
	if err != nil {
		t.Fatal(err)
	}
}
func TestMemoryImportRequiresStoppedDispatcher(t *testing.T) {
	s, path := testStore(t)
	dir := t.TempDir()
	os.Chmod(dir, 0700)
	backup := dir + "/memory.jsonl"
	if _, err := s.ExportMemory(backup); err != nil {
		t.Fatal(err)
	}
	lock, err := LockDispatcher(path)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	if _, err = s.ImportMemory(backup, memoryFileSHA(t, backup)); err == nil || err.Error() != "another_dispatcher_is_running" {
		t.Fatal(err)
	}
}

func memoryFileSHA(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}
func TestMemoryCorruptionNeverPreventsAuthoritativeSourceRevocation(t *testing.T) {
	for _, op := range []string{"update", "delete"} {
		t.Run(op, func(t *testing.T) {
			s, _ := testStore(t)
			memoryIngest(t, s, "one", "c", "배포는 금요일")
			c := claimLedger(t, s)
			_, err := s.PutMemory(c.InboundID, c.Claim, MemoryMutation{Key: "release", Kind: "project", Text: "배포는 금요일", Sources: []MemoryRef{c.Memory.CurrentSource}})
			if err != nil {
				t.Fatal(err)
			}
			_, err = s.call(func(db *storeConn) (any, error) { _, err := db.Exec(`DROP TABLE memory_terms`); return nil, err })
			if err != nil {
				t.Fatal(err)
			}
			var changed bool
			if op == "update" {
				changed, err = s.InvalidateSourceUpdate("c", "", "one")
			} else {
				changed, err = s.DeleteSource("c", "", "one")
			}
			if err != nil || !changed {
				t.Fatalf("authority blocked by derived index: %v %v", changed, err)
			}
			if current, err := s.SourceCurrent(c.Envelope); err != nil || current {
				t.Fatal("source remains current", current, err)
			}
			if _, err = s.QueueReply(c.InboundID, c.Claim, "must not send"); !errors.Is(err, ErrClaim) {
				t.Fatal("stale claim not revoked", err)
			}
			_, err = s.call(func(db *storeConn) (any, error) {
				var doc int64
				if err := db.QueryRow(`SELECT id FROM memory_documents WHERE record_key=?`, c.Memory.CurrentSource.DocumentID).Scan(&doc); err != nil {
					return nil, err
				}
				item, err := loadMemoryItemDB(db, doc, memoryScope(c.Envelope))
				if item != nil {
					t.Fatal("invalidated memory readable", item)
				}
				return nil, err
			})
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestMemorySecretKeysAndUnexpectedBackupMetadataRejected(t *testing.T) {
	s, _ := testStore(t)
	memoryIngest(t, s, "one", "c", "normal source")
	c := claimLedger(t, s)
	key := "sk-proj-abcdefghijklmnopqrstuvwxyz"
	if _, err := s.PutMemory(c.InboundID, c.Claim, MemoryMutation{Key: key, Kind: "fact", Text: "ordinary fact", Sources: []MemoryRef{c.Memory.CurrentSource}}); err == nil {
		t.Fatal("secret key accepted")
	}
	if _, err := s.MemoryFactState(c.InboundID, c.Claim, key); err == nil {
		t.Fatal("secret key echoed by read")
	}
	origin := memoryOrigin(c.Envelope)
	record := memoryBackupRecord{Type: "document", Scope: memoryScope(c.Envelope), Active: true, Item: &MemoryItem{DocumentID: c.Memory.CurrentSource.DocumentID, Role: "user", EventID: c.Envelope.EventID, SourceRevision: 0, Origin: &origin, RemoteMessageID: "sk-proj-abcdefghijklmnopqrstuvwxyz"}}
	if _, err := validateMemoryBackupOrigin(ledgerPolicy{}, record); err == nil {
		t.Fatal("unexpected secret delivery metadata accepted")
	}
}
func TestMemoryRetainedKeyCapReuseAndImport(t *testing.T) {
	s, _ := testStore(t)
	e := memoryIngest(t, s, "one", "c", "ordinary source")
	c := claimLedger(t, s)
	scope := memoryScope(e)
	_, err := s.call(func(db *storeConn) (any, error) {
		return transact(db, func(db *storeConn) (any, error) {
			for n := 0; n < memoryRetainedKeyLimit; n++ {
				key := fmt.Sprintf("retained.%03d", n)
				res, err := db.Exec(`INSERT INTO memory_documents(record_key,scope,role,platform,event_id,source_revision,body,at,active,origin) VALUES(?,?,'fact','discord',?,0,'',0,0,?)`, "fact:"+scope+":"+key, scope, e.EventID, memoryOriginJSON(e))
				if err != nil {
					return nil, err
				}
				doc, err := res.LastInsertId()
				if err != nil {
					return nil, err
				}
				if _, err = db.Exec(`INSERT INTO memory_facts VALUES(?,?,?,'fact',1)`, doc, scope, key); err != nil {
					return nil, err
				}
			}
			return nil, nil
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	mutation := MemoryMutation{Key: "new-key", Kind: "fact", Text: "ordinary new fact", Sources: []MemoryRef{c.Memory.CurrentSource}}
	if _, err = s.PutMemory(c.InboundID, c.Claim, mutation); err == nil || err.Error() != "memory_key_budget_exceeded" {
		t.Fatal("new key above cap", err)
	}
	mutation.Key = "retained.000"
	mutation.ExpectedVersion = 1
	updated, err := s.PutMemory(c.InboundID, c.Claim, mutation)
	if err != nil || updated.Version != 2 {
		t.Fatal("existing key cannot be reused", updated, err)
	}
	mutation.Delete = true
	mutation.ExpectedVersion = 2
	if _, err = s.PutMemory(c.InboundID, c.Claim, mutation); err != nil {
		t.Fatal("existing key cannot be deleted", err)
	}
	origin := memoryOrigin(e)
	record := memoryBackupRecord{Type: "document", Scope: scope, Active: false, Item: &MemoryItem{DocumentID: "fact:" + scope + ":new-key", Role: "fact", Key: "new-key", Kind: "fact", Version: 1, EventID: e.EventID, Origin: &origin}}
	_, err = s.call(func(db *storeConn) (any, error) {
		return transact(db, func(db *storeConn) (any, error) {
			_, err := importMemoryRecordDB(db, ledgerPolicy{}, record)
			return nil, err
		})
	})
	if err == nil || err.Error() != "memory_key_budget_exceeded" {
		t.Fatal("normal import exceeded key cap", err)
	}
	_, err = s.call(func(db *storeConn) (any, error) {
		return transact(db, func(db *storeConn) (any, error) {
			if _, err := db.Exec(`CREATE TABLE memory_source_fences(platform TEXT,event_id TEXT,revision INTEGER,state TEXT,scope TEXT);CREATE TABLE memory_restore_fences(record_key TEXT PRIMARY KEY,version INTEGER,forgotten INTEGER);INSERT INTO memory_meta VALUES('restored_owner_id','owner')`); err != nil {
				return nil, err
			}
			if _, err := db.Exec(`UPDATE inbound SET state='blocked',envelope='{}' WHERE id=?`, c.InboundID); err != nil {
				return nil, err
			}
			_, err := importHistoricalMemoryRecordDB(db, ledgerPolicy{}, record)
			return nil, err
		})
	})
	if err == nil || err.Error() != "memory_key_budget_exceeded" {
		t.Fatal("historical import exceeded key cap", err)
	}
	state, err := s.MemoryFactState(c.InboundID, c.Claim, "retained.000")
	if err != nil || state["version"] != int64(3) {
		t.Fatal("failed import did not roll back", state, err)
	}
}
