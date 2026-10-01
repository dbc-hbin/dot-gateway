package bridge

import (
	"bufio"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

const memoryBackupMaxBytes = 64 << 20

// Memory-only backup. The transport inbox, authored outbox, auth/config and raw
// media are deliberately never exported. The ledger identity prevents mixing
// unrelated accounts or visibility scopes during recovery.
type memoryBackupRecord struct {
	Type     string      `json:"type"`
	Format   int         `json:"format,omitempty"`
	LedgerID string      `json:"ledger_id,omitempty"`
	Count    int         `json:"count,omitempty"`
	Digest   string      `json:"sha256,omitempty"`
	Scope    string      `json:"scope,omitempty"`
	Active   bool        `json:"active,omitempty"`
	Item     *MemoryItem `json:"item,omitempty"`
}

func ensureMemoryIdentity(db *storeConn) error {
	var id string
	err := db.QueryRow(`SELECT value FROM memory_meta WHERE key='ledger_identity'`).Scan(&id)
	if err == nil {
		return nil
	}
	if err != sql.ErrNoRows {
		return err
	}
	id, err = uuidHex()
	if err != nil {
		return err
	}
	_, err = db.Exec(`INSERT INTO memory_meta(key,value) VALUES('ledger_identity',?)`, id)
	return err
}
func privateMemoryBackupFile(path string, write bool) (*os.File, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	dir := filepath.Dir(absolute)
	if write {
		if err = os.MkdirAll(dir, 0700); err != nil {
			return nil, err
		}
	}
	st, err := os.Lstat(dir)
	if err != nil || !st.IsDir() || st.Mode().Perm()&0077 != 0 {
		return nil, errors.New("memory_backup_directory_must_be_private")
	}
	owner, ok := st.Sys().(*syscall.Stat_t)
	if !ok || int(owner.Uid) != os.Getuid() {
		return nil, errors.New("memory_backup_directory_owner_mismatch")
	}
	flags := syscall.O_RDONLY | syscall.O_NOFOLLOW | syscall.O_CLOEXEC | syscall.O_NONBLOCK
	if write {
		flags = syscall.O_WRONLY | syscall.O_CREAT | syscall.O_EXCL | syscall.O_NOFOLLOW | syscall.O_CLOEXEC
	}
	fd, err := syscall.Open(absolute, flags, 0600)
	if err != nil {
		return nil, errors.New("memory_backup_open_failed")
	}
	file := os.NewFile(uintptr(fd), absolute)
	st, err = file.Stat()
	if err != nil || !st.Mode().IsRegular() || st.Mode().Perm()&0077 != 0 {
		file.Close()
		return nil, errors.New("memory_backup_file_must_be_private")
	}
	owner, ok = st.Sys().(*syscall.Stat_t)
	if !ok || int(owner.Uid) != os.Getuid() || owner.Nlink != 1 {
		file.Close()
		return nil, errors.New("memory_backup_file_owner_mismatch")
	}
	if !write && st.Size() > memoryBackupMaxBytes {
		file.Close()
		return nil, errors.New("memory_backup_too_large")
	}
	return file, nil
}
func (s *Store) ExportMemory(path string) (map[string]any, error) {
	file, err := privateMemoryBackupFile(path, true)
	if err != nil {
		return nil, err
	}
	success := false
	defer func() {
		file.Close()
		if !success {
			os.Remove(file.Name())
		}
	}()
	v, err := s.call(func(db *storeConn) (any, error) {
		if _, err := db.Exec("BEGIN"); err != nil {
			return nil, err
		}
		defer db.Exec("ROLLBACK")
		hash := sha256.New()
		fileHash := sha256.New()
		writer := bufio.NewWriter(file)
		count, bytes := 0, 0
		write := func(record memoryBackupRecord, hashed bool) error {
			data, err := json.Marshal(record)
			if err != nil {
				return err
			}
			data = append(data, '\n')
			bytes += len(data)
			if bytes > memoryBackupMaxBytes {
				return errors.New("memory_backup_too_large")
			}
			if hashed {
				hash.Write(data)
			}
			fileHash.Write(data)
			_, err = writer.Write(data)
			return err
		}
		var identity string
		if err := db.QueryRow(`SELECT value FROM memory_meta WHERE key='ledger_identity'`).Scan(&identity); err != nil {
			return nil, err
		}
		if err := write(memoryBackupRecord{Type: "header", Format: 1, LedgerID: identity}, true); err != nil {
			return nil, err
		}
		// Source records first; facts may reference newer source IDs after updates.
		for _, facts := range []bool{false, true} {
			var after int64
			for {
				rows, err := db.Query(`SELECT d.id,d.scope,d.record_key,d.role,d.event_id,d.source_revision,d.reply_id,d.remote_id,d.origin,COALESCE(f.key,''),COALESCE(f.kind,''),COALESCE(f.version,0) FROM memory_documents d LEFT JOIN memory_facts f ON f.doc_id=d.id WHERE d.id>? AND (d.role='fact')=? ORDER BY d.id LIMIT 100`, after, facts)
				if err != nil {
					return nil, err
				}
				type entry struct {
					id     int64
					scope  string
					item   MemoryItem
					origin string
				}
				list := []entry{}
				for rows.Next() {
					var e entry
					if err = rows.Scan(&e.id, &e.scope, &e.item.DocumentID, &e.item.Role, &e.item.EventID, &e.item.SourceRevision, &e.item.ReplyID, &e.item.RemoteMessageID, &e.origin, &e.item.Key, &e.item.Kind, &e.item.Version); err != nil {
						rows.Close()
						return nil, err
					}
					list = append(list, e)
				}
				err = rows.Err()
				rows.Close()
				if err != nil {
					return nil, err
				}
				if len(list) == 0 {
					break
				}
				for _, e := range list {
					item, err := loadMemoryItemDB(db, e.id, e.scope)
					if err != nil {
						return nil, err
					}
					active := item != nil
					if item == nil {
						item = &e.item
						item.Origin = &MemoryOrigin{}
						if json.Unmarshal([]byte(e.origin), item.Origin) != nil {
							return nil, errors.New("invalid_memory_origin")
						}
					} else {
						clean, redacted, truncated := sanitizeMemory(item.Text, memoryBodyLimit)
						item.Text = clean
						item.Redacted = item.Redacted || redacted
						item.Truncated = item.Truncated || truncated
					}
					if err = write(memoryBackupRecord{Type: "document", Scope: e.scope, Active: active, Item: item}, true); err != nil {
						return nil, err
					}
					count++
					after = e.id
				}
			}
		}
		digest := hex.EncodeToString(hash.Sum(nil))
		if err := write(memoryBackupRecord{Type: "footer", Count: count, Digest: digest}, false); err != nil {
			return nil, err
		}
		if err := writer.Flush(); err != nil {
			return nil, err
		}
		if err := file.Sync(); err != nil {
			return nil, err
		}
		if _, err := db.Exec("COMMIT"); err != nil {
			return nil, err
		}
		return map[string]any{"documents": count, "bytes": bytes, "sha256": hex.EncodeToString(fileHash.Sum(nil)), "content_sha256": digest, "privacy": "private_redacted_memory_only"}, nil
	})
	if err != nil {
		return nil, err
	}
	success = true
	return v.(map[string]any), nil
}

// Import only fills absent records of this same ledger. Existing records,
// revisions, newer facts and forget tombstones ALWAYS win over older backups.
// All source text is re-derived from the corresponding admitted/sent ledger row;
// archive text cannot invent user messages or an unacknowledged assistant reply.
func (s *Store) ImportMemory(path, expectedSHA string) (map[string]any, error) {
	lock, err := LockDispatcher(s.path)
	if err != nil {
		return nil, err
	}
	defer lock.Close()
	file, err := privateMemoryBackupFile(path, false)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	if len(expectedSHA) != 64 {
		return nil, errors.New("memory_backup_trusted_hash_required")
	}
	hash := sha256.New()
	if _, err = io.Copy(hash, io.LimitReader(file, memoryBackupMaxBytes+1)); err != nil {
		return nil, errors.New("memory_backup_read_failed")
	}
	if hex.EncodeToString(hash.Sum(nil)) != expectedSHA {
		return nil, errors.New("memory_backup_hash_mismatch")
	}
	if _, err = file.Seek(0, 0); err != nil {
		return nil, err
	}
	v, err := s.call(func(db *storeConn) (any, error) {
		return transact(db, func(db *storeConn) (any, error) {
			importHash := sha256.New()
			scanner := bufio.NewScanner(io.TeeReader(io.LimitReader(file, memoryBackupMaxBytes+1), importHash))
			scanner.Buffer(make([]byte, 4096), 128<<10)
			hash := sha256.New()
			line, count, inserted := 0, 0, 0
			footer := false
			for scanner.Scan() {
				data := append(append([]byte{}, scanner.Bytes()...), '\n')
				var record memoryBackupRecord
				decoder := json.NewDecoder(strings.NewReader(string(data)))
				decoder.DisallowUnknownFields()
				if err := decoder.Decode(&record); err != nil {
					return nil, errors.New("invalid_memory_backup")
				}
				var extra any
				if decoder.Decode(&extra) != io.EOF {
					return nil, errors.New("invalid_memory_backup")
				}
				if footer {
					return nil, errors.New("invalid_memory_backup_trailing_data")
				}
				if line == 0 {
					var identity string
					if err := db.QueryRow(`SELECT value FROM memory_meta WHERE key='ledger_identity'`).Scan(&identity); err != nil {
						return nil, err
					}
					if record.Type != "header" || record.Format != 1 || record.LedgerID != identity {
						return nil, errors.New("memory_backup_ledger_mismatch")
					}
					hash.Write(data)
					line++
					continue
				}
				if record.Type == "footer" {
					if record.Count != count || record.Digest != hex.EncodeToString(hash.Sum(nil)) {
						return nil, errors.New("memory_backup_checksum_mismatch")
					}
					footer = true
					continue
				}
				if record.Type != "document" || record.Item == nil || len(record.Scope) != 64 {
					return nil, errors.New("invalid_memory_backup")
				}
				hash.Write(data)
				count++
				var restoredMarker string
				db.QueryRow(`SELECT value FROM memory_meta WHERE key='restored_metadata_only'`).Scan(&restoredMarker)
				var added bool
				var err error
				if restoredMarker == "1" {
					added, err = importHistoricalMemoryRecordDB(db, s.policy, record)
				} else {
					added, err = importMemoryRecordDB(db, s.policy, record)
				}
				if err != nil {
					return nil, err
				}
				if added {
					inserted++
				}
				line++
			}
			if scanner.Err() != nil {
				return nil, errors.New("memory_backup_read_failed")
			}
			if !footer {
				return nil, errors.New("memory_backup_incomplete")
			}
			if hex.EncodeToString(importHash.Sum(nil)) != expectedSHA {
				return nil, errors.New("memory_backup_hash_mismatch")
			}
			return map[string]any{"documents_examined": count, "documents_restored": inserted, "existing_records_preserved": true}, nil
		})
	})
	if err != nil {
		return nil, err
	}
	return v.(map[string]any), nil
}
func importMemoryRecordDB(db *storeConn, policy StorePolicy, record memoryBackupRecord) (bool, error) {
	item := record.Item
	if _, err := validateMemoryBackupOrigin(policy, record); err != nil {
		return false, err
	}
	var exists int
	if err := db.QueryRow(`SELECT EXISTS(SELECT 1 FROM memory_documents WHERE record_key=?)`, item.DocumentID).Scan(&exists); err != nil {
		return false, err
	}
	if exists != 0 {
		return false, nil
	}
	if item.Role == "fact" {
		if err := memoryKeyCapacityDB(db, record.Scope); err != nil {
			return false, err
		}
		if !validMemoryKey(item.Key) || item.DocumentID != "fact:"+record.Scope+":"+item.Key || item.Version < 1 || len([]rune(item.Text)) > 600 {
			return false, errors.New("invalid_memory_backup_fact")
		}
		if item.Kind != "fact" && item.Kind != "preference" && item.Kind != "project" && item.Kind != "summary" {
			return false, errors.New("invalid_memory_backup_fact")
		}
		clean, redacted, _ := sanitizeMemory(item.Text, 600)
		if redacted {
			return false, errors.New("memory_sensitive_content_rejected")
		}
		active := record.Active
		sourceIDs := []int64{}
		var e Envelope
		if active {
			if len(item.Sources) < 1 || len(item.Sources) > 8 {
				return false, errors.New("invalid_memory_backup_sources")
			}
			for _, ref := range item.Sources {
				var sid int64
				err := db.QueryRow(`SELECT d.id FROM memory_documents d WHERE d.record_key=? AND d.scope=? AND d.source_revision=? AND d.role!='fact' AND `+memoryCurrentSQL, ref.DocumentID, record.Scope, ref.SourceRevision).Scan(&sid)
				if err == sql.ErrNoRows {
					active = false
					break
				}
				if err != nil {
					return false, err
				}
				sourceIDs = append(sourceIDs, sid)
			}
		}
		// Only source-bound facts recover active; revoked/incomplete facts become tombstones.
		if active {
			var raw string
			err := db.QueryRow(`SELECT envelope FROM inbound WHERE platform='discord' AND event_id=?`, sourceLedgerKey(Envelope{EventID: item.EventID, SourceRevision: item.SourceRevision})).Scan(&raw)
			if err != nil {
				return false, errors.New("memory_backup_source_missing")
			}
			if json.Unmarshal([]byte(raw), &e) != nil || memoryScope(e) != record.Scope || !policy.Accepts(e) || !isMessageSource(e) {
				return false, errors.New("memory_backup_scope_mismatch")
			}
		}
		if !active {
			clean = ""
			e = item.Origin.envelope()
		}
		var chars, n int
		if err := db.QueryRow(`SELECT COALESCE(sum(length(d.body)),0),count(*) FROM memory_facts f JOIN memory_documents d ON d.id=f.doc_id WHERE f.scope=? AND d.active=1`, record.Scope).Scan(&chars, &n); err != nil {
			return false, err
		}
		if active && (chars+len([]rune(clean)) > memoryFactBudget || n >= 64) {
			return false, errors.New("memory_budget_exceeded")
		}
		res, err := db.Exec(`INSERT INTO memory_documents(record_key,scope,role,platform,event_id,source_revision,body,at,active,origin) VALUES(?,?,'fact',?,?,?,?,?,?,?)`, item.DocumentID, record.Scope, e.Platform, item.EventID, item.SourceRevision, clean, item.At, active, memoryOriginJSON(e))
		if err != nil {
			return false, err
		}
		doc, err := res.LastInsertId()
		if err != nil {
			return false, err
		}
		if _, err = db.Exec(`INSERT INTO memory_facts VALUES(?,?,?,?,?)`, doc, record.Scope, item.Key, item.Kind, item.Version); err != nil {
			return false, err
		}
		if active {
			for _, sid := range sourceIDs {
				if _, err = db.Exec(`INSERT OR IGNORE INTO memory_fact_sources SELECT ?,id,source_revision FROM memory_documents WHERE id=?`, doc, sid); err != nil {
					return false, err
				}
			}
		}
		return true, indexMemoryDB(db, doc, record.Scope, clean)
	}
	var raw, body, reply, remote string
	var at float64
	switch item.Role {
	case "user":
		if !strings.HasPrefix(item.DocumentID, "user:") {
			return false, errors.New("invalid_memory_backup_document")
		}
		if err := db.QueryRow(`SELECT envelope,created FROM inbound WHERE id=?`, strings.TrimPrefix(item.DocumentID, "user:")).Scan(&raw, &at); err != nil {
			return false, errors.New("memory_backup_source_missing")
		}
	case "assistant":
		if operationID, edited := operationIDFromMemoryKey(item.DocumentID); edited {
			var snapshot, channel string
			if err := db.QueryRow(`SELECT r.snapshot,r.created,o.channel,o.message FROM message_edit_revisions r JOIN message_operations o ON o.id=r.operation_id WHERE o.id=? AND o.state='verified' AND json_extract(o.spec,'$.action')='edit_text'`, operationID).Scan(&snapshot, &at, &channel, &remote); err != nil {
				return false, errors.New("memory_backup_edit_evidence_missing")
			}
			var snap operationSnapshot
			if json.Unmarshal([]byte(snapshot), &snap) != nil {
				return false, errors.New("memory_backup_edit_evidence_missing")
			}
			c, _, err := operationOwnedChunkDB(db, channel, remote)
			if err != nil || c.ReplyID != item.ReplyID || remote != item.RemoteMessageID {
				return false, errors.New("memory_backup_edit_evidence_missing")
			}
			raw = operationJSON(c.Source)
			body = snap.Content
			reply = c.ReplyID
		} else {
			var index int
			if _, err := fmt.Sscanf(item.DocumentID, "assistant:"+item.ReplyID+":%d", &index); err != nil || item.ReplyID == "" || index < 0 || fmt.Sprintf("assistant:%s:%d", item.ReplyID, index) != item.DocumentID {
				return false, errors.New("invalid_memory_backup_document")
			}
			var state string
			if err := db.QueryRow(`SELECT i.envelope,c.text,c.state,COALESCE(c.message_id,''),r.created FROM chunks c JOIN replies r ON r.id=c.reply_id JOIN inbound i ON i.id=r.inbound_id WHERE c.reply_id=? AND c.idx=?`, item.ReplyID, index).Scan(&raw, &body, &state, &remote, &at); err != nil {
				return false, errors.New("memory_backup_source_missing")
			}
			if state != "sent" || remote == "" {
				return false, nil
			}
			reply = item.ReplyID
		}
	default:
		return false, errors.New("invalid_memory_backup_role")
	}
	var e Envelope
	if json.Unmarshal([]byte(raw), &e) != nil || !policy.Accepts(e) || !isMessageSource(e) || memoryScope(e) != record.Scope || e.EventID != item.EventID || e.SourceRevision != item.SourceRevision {
		return false, errors.New("memory_backup_scope_mismatch")
	}
	if item.Role == "user" {
		body = e.Text
	}
	current, err := sourceCurrentDB(db, e)
	if err != nil {
		return false, err
	}
	active := record.Active && current
	body, redacted, truncated := sanitizeMemory(body, memoryBodyLimit)
	if !active {
		body = ""
	}
	res, err := db.Exec(`INSERT INTO memory_documents(record_key,scope,role,platform,event_id,source_revision,body,at,active,redacted,truncated,reply_id,remote_id,origin) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, item.DocumentID, record.Scope, item.Role, e.Platform, e.EventID, e.SourceRevision, body, at, active, redacted, truncated, reply, remote, memoryOriginJSON(e))
	if err != nil {
		return false, err
	}
	doc, err := res.LastInsertId()
	if err != nil {
		return false, err
	}
	return true, indexMemoryDB(db, doc, record.Scope, body)
}

func validateMemoryBackupOrigin(policy StorePolicy, record memoryBackupRecord) (Envelope, error) {
	item := record.Item
	if item != nil {
		if len(item.DocumentID) > 256 || len(item.ReplyID) > 128 || len(item.RemoteMessageID) > 128 || len(item.Sources) > 8 {
			return Envelope{}, errors.New("invalid_memory_backup_metadata")
		}
		if item.Role != "assistant" && (item.ReplyID != "" || item.RemoteMessageID != "" || item.Delivery != "") {
			return Envelope{}, errors.New("invalid_memory_backup_metadata")
		}
	}
	if item == nil || item.Origin == nil {
		return Envelope{}, errors.New("memory_backup_provenance_required")
	}
	e := item.Origin.envelope()
	for _, value := range []string{e.Platform, e.EventID, e.ConversationID, e.GuildID, e.SenderID, e.RouteKind, e.ParentChannelID} {
		if len(value) > 128 {
			return Envelope{}, errors.New("memory_backup_scope_mismatch")
		}
	}
	if e.Platform != "discord" || e.SourceRevision < 0 || e.EventID != item.EventID || e.SourceRevision != item.SourceRevision || memoryScope(e) != record.Scope || !isMessageSource(e) || !policy.Accepts(e) {
		return Envelope{}, errors.New("memory_backup_scope_mismatch")
	}
	return e, nil
}
func importHistoricalMemoryRecordDB(db *storeConn, policy StorePolicy, record memoryBackupRecord) (bool, error) {
	e, err := validateMemoryBackupOrigin(policy, record)
	if err != nil {
		return false, err
	}
	item := record.Item
	var owner string
	if err = db.QueryRow(`SELECT value FROM memory_meta WHERE key='restored_owner_id'`).Scan(&owner); err != nil || owner != e.SenderID {
		return false, errors.New("memory_backup_owner_mismatch")
	}
	var exists int
	if err = db.QueryRow(`SELECT EXISTS(SELECT 1 FROM memory_documents WHERE record_key=?)`, item.DocumentID).Scan(&exists); err != nil {
		return false, err
	}
	if exists != 0 {
		return false, nil
	}
	// The metadata recovery path kept exact source deduplication IDs but replaced
	// source content with {}. Never mutate those tombstones or register new heads.
	if err = db.QueryRow(`SELECT EXISTS(SELECT 1 FROM inbound WHERE platform=? AND event_id=? AND state='blocked' AND envelope='{}')`, e.Platform, sourceLedgerKey(e)).Scan(&exists); err != nil {
		return false, err
	}
	if exists != 1 {
		return false, errors.New("memory_backup_recovery_source_mismatch")
	}
	clean, redacted, truncated := sanitizeMemory(item.Text, memoryBodyLimit)
	if redacted {
		return false, errors.New("memory_sensitive_content_rejected")
	}
	active := record.Active && clean != ""
	var fenceVersion int64
	var forgotten bool
	err = db.QueryRow(`SELECT version,forgotten FROM memory_restore_fences WHERE record_key=?`, item.DocumentID).Scan(&fenceVersion, &forgotten)
	if err != nil && err != sql.ErrNoRows {
		return false, err
	}
	if forgotten || fenceVersion > item.Version {
		active = false
	}
	if item.Role == "fact" && fenceVersion > item.Version {
		item.Version = fenceVersion
	}
	var fenceState, fenceScope string
	var fenceRevision int64
	err = db.QueryRow(`SELECT state,revision,scope FROM memory_source_fences WHERE platform=? AND event_id=?`, e.Platform, e.EventID).Scan(&fenceState, &fenceRevision, &fenceScope)
	if err != nil && err != sql.ErrNoRows {
		return false, err
	}
	if err == nil && fenceScope != "" && fenceScope != record.Scope {
		return false, errors.New("memory_backup_scope_mismatch")
	}
	if err == sql.ErrNoRows || fenceState != "current" || fenceRevision != e.SourceRevision || fenceScope == "" {
		active = false
	}

	var sourceState string
	var revision int64
	err = db.QueryRow(`SELECT state,revision FROM message_sources WHERE platform=? AND event_id=?`, e.Platform, e.EventID).Scan(&sourceState, &revision)
	if err != nil && err != sql.ErrNoRows {
		return false, err
	}
	if err == nil && (sourceState != "current" || revision != e.SourceRevision) {
		active = false
	}
	refs := []int64{}
	switch item.Role {
	case "user":
		if !strings.HasPrefix(item.DocumentID, "user:") {
			return false, errors.New("invalid_memory_backup_document")
		}
		if err = db.QueryRow(`SELECT EXISTS(SELECT 1 FROM inbound WHERE id=? AND platform=? AND event_id=? AND state='blocked' AND envelope='{}')`, strings.TrimPrefix(item.DocumentID, "user:"), e.Platform, sourceLedgerKey(e)).Scan(&exists); err != nil || exists != 1 {
			return false, errors.New("memory_backup_recovery_source_mismatch")
		}
	case "assistant":
		if operationID, edited := operationIDFromMemoryKey(item.DocumentID); edited {
			var channel, message, state, platform, event string
			if err = db.QueryRow(`SELECT channel,message,state FROM message_operation_recovery_fences WHERE id=? AND action='edit_text'`, operationID).Scan(&channel, &message, &state); err != nil || state != "verified" || channel != e.ConversationID || message != item.RemoteMessageID {
				return false, errors.New("memory_backup_edit_evidence_missing")
			}
			if err = db.QueryRow(`SELECT i.platform,i.event_id FROM chunks c JOIN replies r ON r.id=c.reply_id JOIN inbound i ON i.id=r.inbound_id WHERE c.reply_id=? AND c.message_id=? AND c.state='sent'`, item.ReplyID, message).Scan(&platform, &event); err != nil || platform != e.Platform || event != sourceLedgerKey(e) {
				return false, errors.New("memory_backup_edit_evidence_missing")
			}
			// Preserve text as inert imported history. Restored projection markers
			// suppress both this document and every dependent fact on all reads.
		} else {
			var index int
			prefix := "assistant:" + item.ReplyID + ":"
			if !strings.HasPrefix(item.DocumentID, prefix) || item.ReplyID == "" {
				return false, errors.New("invalid_memory_backup_document")
			}
			if _, err = fmt.Sscanf(strings.TrimPrefix(item.DocumentID, prefix), "%d", &index); err != nil || fmt.Sprintf("%s%d", prefix, index) != item.DocumentID || index < 0 {
				return false, errors.New("invalid_memory_backup_document")
			}
			var state, remote, platform, event string
			if err = db.QueryRow(`SELECT c.state,COALESCE(c.message_id,''),i.platform,i.event_id FROM chunks c JOIN replies r ON r.id=c.reply_id JOIN inbound i ON i.id=r.inbound_id WHERE c.reply_id=? AND c.idx=?`, item.ReplyID, index).Scan(&state, &remote, &platform, &event); err != nil {
				return false, errors.New("memory_backup_recovery_source_mismatch")
			}
			if platform != e.Platform || event != sourceLedgerKey(e) || state != "sent" || remote == "" || remote != item.RemoteMessageID {
				return false, errors.New("memory_backup_unverified_delivery")
			}
		}
	case "fact":
		if err := memoryKeyCapacityDB(db, record.Scope); err != nil {
			return false, err
		}
		if !validMemoryKey(item.Key) || item.DocumentID != "fact:"+record.Scope+":"+item.Key || item.Version < 1 || len([]rune(clean)) > 600 {
			return false, errors.New("invalid_memory_backup_fact")
		}
		if item.Kind != "fact" && item.Kind != "preference" && item.Kind != "project" && item.Kind != "summary" {
			return false, errors.New("invalid_memory_backup_fact")
		}
		if active {
			if len(item.Sources) < 1 || len(item.Sources) > 8 {
				return false, errors.New("invalid_memory_backup_sources")
			}
			for _, ref := range item.Sources {
				var sid int64
				err = db.QueryRow(`SELECT d.id FROM memory_documents d WHERE d.record_key=? AND d.scope=? AND d.source_revision=? AND d.role!='fact' AND `+memoryCurrentSQL, ref.DocumentID, record.Scope, ref.SourceRevision).Scan(&sid)
				if err == sql.ErrNoRows {
					active = false
					break
				}
				if err != nil {
					return false, err
				}
				refs = append(refs, sid)
			}
		}
		var chars, n int
		if err = db.QueryRow(`SELECT COALESCE(sum(length(d.body)),0),count(*) FROM memory_facts f JOIN memory_documents d ON d.id=f.doc_id WHERE f.scope=? AND d.active=1`, record.Scope).Scan(&chars, &n); err != nil {
			return false, err
		}
		if active && (chars+len([]rune(clean)) > memoryFactBudget || n >= 64) {
			return false, errors.New("memory_budget_exceeded")
		}
	default:
		return false, errors.New("invalid_memory_backup_role")
	}
	if !active {
		clean = ""
	}
	res, err := db.Exec(`INSERT INTO memory_documents(record_key,scope,role,platform,event_id,source_revision,body,at,active,redacted,truncated,reply_id,remote_id,origin,restored) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,1)`, item.DocumentID, record.Scope, item.Role, e.Platform, e.EventID, e.SourceRevision, clean, item.At, active, item.Redacted, item.Truncated || truncated, item.ReplyID, item.RemoteMessageID, memoryOriginJSON(e))
	if err != nil {
		return false, err
	}
	doc, err := res.LastInsertId()
	if err != nil {
		return false, err
	}
	if item.Role == "fact" {
		if _, err = db.Exec(`INSERT INTO memory_facts VALUES(?,?,?,?,?)`, doc, record.Scope, item.Key, item.Kind, item.Version); err != nil {
			return false, err
		}
		if active {
			for _, sid := range refs {
				if _, err = db.Exec(`INSERT OR IGNORE INTO memory_fact_sources SELECT ?,id,source_revision FROM memory_documents WHERE id=?`, doc, sid); err != nil {
					return false, err
				}
			}
		}
	}
	return true, indexMemoryDB(db, doc, record.Scope, clean)
}
