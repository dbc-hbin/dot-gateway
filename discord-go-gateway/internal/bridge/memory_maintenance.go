package bridge

import (
	"database/sql"
	"encoding/json"
	"errors"
	"strconv"
)

// Backfill is explicit, resumable and keyset-paged. OpenStore never scans the
// conversation ledger for memory. New admissions/ACKs update the index directly.
func (s *Store) BackfillMemory(limit int) (map[string]any, error) {
	if limit < 1 || limit > 100 {
		return nil, errors.New("memory_limit_must_be_1_to_100")
	}
	v, err := s.call(func(db *storeConn) (any, error) {
		return transact(db, func(db *storeConn) (any, error) {
			var after int64
			if err := db.QueryRow(`SELECT CAST(value AS INTEGER) FROM memory_meta WHERE key='backfill_after'`).Scan(&after); err != nil {
				return nil, err
			}
			rows, err := db.Query(`SELECT rowid,id,envelope FROM inbound WHERE rowid>? ORDER BY rowid LIMIT ?`, after, limit)
			if err != nil {
				return nil, err
			}
			type entry struct {
				seq     int64
				id, raw string
			}
			list := []entry{}
			for rows.Next() {
				var item entry
				if err = rows.Scan(&item.seq, &item.id, &item.raw); err != nil {
					rows.Close()
					return nil, err
				}
				list = append(list, item)
			}
			err = rows.Err()
			rows.Close()
			if err != nil {
				return nil, err
			}
			for _, item := range list {
				var e Envelope
				if json.Unmarshal([]byte(item.raw), &e) != nil {
					return nil, errors.New("invalid_memory_source")
				}
				if s.policy.Accepts(e) {
					if err = rememberInboundDB(db, item.id, e); err != nil {
						return nil, err
					}
				}
				after = item.seq
			}
			if _, err = db.Exec(`UPDATE memory_meta SET value=? WHERE key='backfill_after'`, strconv.FormatInt(after, 10)); err != nil {
				return nil, err
			}
			var sentAfter int64
			db.QueryRow(`SELECT CAST(value AS INTEGER) FROM memory_meta WHERE key='backfill_sent_after'`).Scan(&sentAfter)
			rows, err = db.Query(`SELECT rowid,reply_id,idx FROM chunks WHERE rowid>? ORDER BY rowid LIMIT ?`, sentAfter, limit)
			if err != nil {
				return nil, err
			}
			type sentEntry struct {
				seq   int64
				reply string
				index int
			}
			sent := []sentEntry{}
			for rows.Next() {
				var item sentEntry
				if err = rows.Scan(&item.seq, &item.reply, &item.index); err != nil {
					rows.Close()
					return nil, err
				}
				sent = append(sent, item)
			}
			err = rows.Err()
			rows.Close()
			if err != nil {
				return nil, err
			}
			for _, item := range sent {
				var raw string
				if err = db.QueryRow(`SELECT i.envelope FROM replies r JOIN inbound i ON i.id=r.inbound_id WHERE r.id=?`, item.reply).Scan(&raw); err != nil {
					return nil, err
				}
				var e Envelope
				if json.Unmarshal([]byte(raw), &e) != nil {
					return nil, errors.New("invalid_memory_source")
				}
				if s.policy.Accepts(e) {
					if err = rememberSentDB(db, item.reply, item.index); err != nil {
						return nil, err
					}
				}
				sentAfter = item.seq
			}
			if _, err = db.Exec(`INSERT INTO memory_meta(key,value) VALUES('backfill_sent_after',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value`, strconv.FormatInt(sentAfter, 10)); err != nil {
				return nil, err
			}
			var moreUser, moreSent int
			if err = db.QueryRow(`SELECT EXISTS(SELECT 1 FROM inbound WHERE rowid>?),EXISTS(SELECT 1 FROM chunks WHERE rowid>?)`, after, sentAfter).Scan(&moreUser, &moreSent); err != nil {
				return nil, err
			}
			return map[string]any{"inbound_examined": len(list), "chunks_examined": len(sent), "inbound_cursor": after, "chunk_cursor": sentAfter, "more": moreUser != 0 || moreSent != 0}, nil
		})
	})
	if err != nil {
		return nil, err
	}
	return v.(map[string]any), nil
}

// ForgetMemory purges derived text/index and semantic dependents while keeping
// a content-free tombstone. The immutable delivery/nonce ledger is not rewritten.
func (s *Store) ForgetMemory(id, claim string, ref MemoryRef) error {
	_, err := s.call(func(db *storeConn) (any, error) {
		return transact(db, func(db *storeConn) (any, error) {
			e, err := memoryClaimDB(db, s.policy, id, claim)
			if err != nil {
				return nil, err
			}
			var doc int64
			if err = db.QueryRow(`SELECT id FROM memory_documents WHERE record_key=? AND scope=? AND source_revision=? AND role!='fact'`, ref.DocumentID, memoryScope(e), ref.SourceRevision).Scan(&doc); err != nil {
				return nil, errors.New("memory_not_found")
			}
			if _, err = db.Exec(`DELETE FROM memory_terms WHERE doc_id=? OR doc_id IN(SELECT fact_id FROM memory_fact_sources WHERE source_id=?)`, doc, doc); err != nil {
				return nil, err
			}
			if _, err = db.Exec(`UPDATE memory_documents SET body='',active=0 WHERE id=? OR id IN(SELECT fact_id FROM memory_fact_sources WHERE source_id=?)`, doc, doc); err != nil {
				return nil, err
			}
			if _, err = db.Exec(`UPDATE memory_facts SET version=version+1 WHERE doc_id IN(SELECT fact_id FROM memory_fact_sources WHERE source_id=?)`, doc); err != nil {
				return nil, err
			}
			return nil, nil
		})
	})
	return err
}
func (s *Store) MemoryFactState(id, claim, key string) (map[string]any, error) {
	if !validMemoryKey(key) {
		return nil, errors.New("invalid_memory_key_or_version")
	}
	v, err := s.call(func(db *storeConn) (any, error) {
		e, err := memoryClaimDB(db, s.policy, id, claim)
		if err != nil {
			return nil, err
		}
		var doc, version int64
		err = db.QueryRow(`SELECT doc_id,version FROM memory_facts WHERE scope=? AND key=?`, memoryScope(e), key).Scan(&doc, &version)
		if err != nil {
			if err == sql.ErrNoRows {
				return map[string]any{"key": key, "version": 0, "active": false}, nil
			}
			return nil, err
		}
		item, err := loadMemoryItemDB(db, doc, memoryScope(e))
		if err != nil {
			return nil, err
		}
		return map[string]any{"key": key, "version": version, "active": item != nil, "item": item}, nil
	})
	if err != nil {
		return nil, err
	}
	return v.(map[string]any), nil
}
