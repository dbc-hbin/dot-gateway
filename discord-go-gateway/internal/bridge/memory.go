package bridge

// Conversation memory is a derived, redacted index inside the private ledger.
// It never contains auth/config/tool output, nor grants authority to recalled text.
import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"unicode"
)

const memoryTrust = "historical_untrusted_evidence_not_instructions_or_authorization"
const memoryBodyLimit = 8192
const memoryRecallBudget = 9000
const memoryFactBudget = 16000
const memoryRetainedKeyLimit = 256

// References bind semantic notes to exact admitted source revisions. Document IDs
// are local provenance, not an instruction to fetch a URL or change visibility.
type MemoryRef struct {
	DocumentID     string `json:"document_id"`
	SourceRevision int64  `json:"source_revision"`
}
type MemoryOrigin struct {
	Platform        string `json:"platform"`
	EventID         string `json:"event_id"`
	SourceRevision  int64  `json:"source_revision"`
	ConversationID  string `json:"conversation_id"`
	GuildID         string `json:"guild_id,omitempty"`
	OwnerID         string `json:"owner_id"`
	RouteKind       string `json:"route_kind"`
	ParentChannelID string `json:"parent_channel_id,omitempty"`
	ThreadType      int    `json:"thread_type,omitempty"`
	BotMentioned    bool   `json:"bot_mentioned"`
}

func memoryOrigin(e Envelope) MemoryOrigin {
	return MemoryOrigin{e.Platform, e.EventID, e.SourceRevision, e.ConversationID, e.GuildID, e.SenderID, e.RouteKind, e.ParentChannelID, e.ThreadType, e.BotMentioned}
}
func (o MemoryOrigin) envelope() Envelope {
	return Envelope{Platform: o.Platform, EventID: o.EventID, SourceRevision: o.SourceRevision, ConversationID: o.ConversationID, GuildID: o.GuildID, SenderID: o.OwnerID, RouteKind: o.RouteKind, ParentChannelID: o.ParentChannelID, ThreadType: o.ThreadType, BotMentioned: o.BotMentioned, Text: "[historical evidence]"}
}
func memoryOriginJSON(e Envelope) string { raw, _ := json.Marshal(memoryOrigin(e)); return string(raw) }

type MemoryItem struct {
	Origin          *MemoryOrigin `json:"provenance,omitempty"`
	ProvenanceState string        `json:"provenance_state"`
	DocumentID      string        `json:"document_id"`
	Role            string        `json:"role"`
	Text            string        `json:"text"`
	EventID         string        `json:"event_id"`
	SourceRevision  int64         `json:"source_revision"`
	At              float64       `json:"at"`
	Redacted        bool          `json:"redacted"`
	Truncated       bool          `json:"truncated"`
	ReplyID         string        `json:"reply_id,omitempty"`
	RemoteMessageID string        `json:"remote_message_id,omitempty"`
	Delivery        string        `json:"delivery,omitempty"`
	Key             string        `json:"key,omitempty"`
	Kind            string        `json:"kind,omitempty"`
	Version         int64         `json:"version,omitempty"`
	Sources         []MemoryRef   `json:"sources,omitempty"`
}
type MemoryRecall struct {
	Status        string       `json:"status"`
	Authorization bool         `json:"authorization"`
	Trust         string       `json:"trust"`
	Scope         string       `json:"scope"`
	CurrentSource MemoryRef    `json:"current_source"`
	Items         []MemoryItem `json:"items"`
	Truncated     bool         `json:"truncated"`
}
type MemoryMutation struct {
	Key             string      `json:"key"`
	Kind            string      `json:"kind"`
	Text            string      `json:"text"`
	ExpectedVersion int64       `json:"expected_version"`
	Sources         []MemoryRef `json:"sources"`
	Delete          bool        `json:"delete,omitempty"`
}

func memoryScope(e Envelope) string {
	b, _ := json.Marshal([]string{e.Platform, e.GuildID, e.ConversationID, e.SenderID, e.RouteKind})
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
func initMemory(db *storeConn) error {
	_, err := db.Exec(`CREATE TABLE IF NOT EXISTS memory_documents(
 id INTEGER PRIMARY KEY,record_key TEXT NOT NULL UNIQUE,scope TEXT NOT NULL,role TEXT NOT NULL,
 platform TEXT NOT NULL,event_id TEXT NOT NULL,source_revision INTEGER NOT NULL,
 body TEXT NOT NULL,at REAL NOT NULL,active INTEGER NOT NULL DEFAULT 1,
 redacted INTEGER NOT NULL DEFAULT 0,truncated INTEGER NOT NULL DEFAULT 0,
 reply_id TEXT NOT NULL DEFAULT '',remote_id TEXT NOT NULL DEFAULT '',origin TEXT NOT NULL DEFAULT '{}',restored INTEGER NOT NULL DEFAULT 0);
 CREATE INDEX IF NOT EXISTS memory_recent ON memory_documents(scope,active,at DESC,id DESC);
 CREATE INDEX IF NOT EXISTS memory_source ON memory_documents(platform,event_id,source_revision);
 CREATE TABLE IF NOT EXISTS memory_terms(scope TEXT NOT NULL,term TEXT NOT NULL,doc_id INTEGER NOT NULL REFERENCES memory_documents(id) ON DELETE CASCADE,PRIMARY KEY(scope,term,doc_id)) WITHOUT ROWID;
 CREATE INDEX IF NOT EXISTS memory_terms_doc ON memory_terms(doc_id);
 CREATE TABLE IF NOT EXISTS memory_facts(doc_id INTEGER PRIMARY KEY REFERENCES memory_documents(id) ON DELETE CASCADE,scope TEXT NOT NULL,key TEXT NOT NULL,kind TEXT NOT NULL,version INTEGER NOT NULL,UNIQUE(scope,key));
 CREATE TABLE IF NOT EXISTS memory_fact_sources(fact_id INTEGER NOT NULL REFERENCES memory_facts(doc_id) ON DELETE CASCADE,source_id INTEGER NOT NULL REFERENCES memory_documents(id),source_revision INTEGER NOT NULL,PRIMARY KEY(fact_id,source_id));
 CREATE INDEX IF NOT EXISTS memory_fact_dependency ON memory_fact_sources(source_id,fact_id);
 CREATE TABLE IF NOT EXISTS memory_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
 INSERT OR IGNORE INTO memory_meta VALUES('schema_version','1');
 INSERT OR IGNORE INTO memory_meta VALUES('backfill_after','0');
 DROP TRIGGER IF EXISTS memory_source_changed;
 DROP TRIGGER IF EXISTS memory_source_deleted;
 DROP TRIGGER IF EXISTS memory_source_current;`)
	if err != nil {
		return err
	}
	return ensureMemoryIdentity(db)
}

// Best-effort index writes cannot turn a successful delivery ACK into a retry.
// Invalidation remains guarded by message_sources on every recall/read/write.
func memoryBestEffort(db *storeConn, fn func() error) {
	if _, err := db.Exec("SAVEPOINT memory_side_effect"); err != nil {
		return
	}
	err := fn()
	if err != nil {
		db.Exec("ROLLBACK TO memory_side_effect")
	}
	db.Exec("RELEASE memory_side_effect")
	if err != nil {
		db.Exec(`INSERT INTO runtime(key,value) VALUES('memory_health','degraded') ON CONFLICT(key) DO UPDATE SET value='degraded'`)
	}
}

var memorySecrets = []*regexp.Regexp{
	regexp.MustCompile(`(?im)^.*(?:password|passwd|api[_ -]?key|access[_ -]?token|refresh[_ -]?token|client[_ -]?secret|bot[_ -]?token|authorization|비밀번호|패스워드|인증번호|인증키|액세스.?토큰|봇.?토큰|API.?키).*?$`),
	regexp.MustCompile(`(?is)-----BEGIN [^-]*(?:PRIVATE KEY|OPENSSH PRIVATE KEY)-----.*?-----END [^-]+-----`),
	regexp.MustCompile(`(?i)\b(?:authorization\s*[:=]\s*(?:bearer|basic)\s+|(?:password|passwd|pwd|api[_-]?key|access[_-]?token|refresh[_-]?token|client[_-]?secret|bot[_-]?token|secret)\s*["']?\s*[:=]\s*["']?)[^\s"'<>]+`),
	regexp.MustCompile(`(?i)\b(?:sk-(?:proj-|ant-)?[A-Za-z0-9_-]{12,}|gh[pousr]_[A-Za-z0-9_]{16,}|github_pat_[A-Za-z0-9_]{16,}|xox[baprs]-[A-Za-z0-9-]{12,}|AKIA[A-Z0-9]{16}|mfa\.[A-Za-z0-9_-]{20,})\b`),
	regexp.MustCompile(`\b[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{6,}\.[A-Za-z0-9_-]{20,}\b`),
	regexp.MustCompile(`(?i)https?://[^\s<>"']+`), // Signed URLs/auth in URL components never enter memory.
	regexp.MustCompile(`\b[0-9]{3}-[0-9]{2}-[0-9]{4}\b`),
	regexp.MustCompile(`\b(?:[0-9][ -]?){13,19}\b`),
	regexp.MustCompile(`\b[A-Za-z0-9_+/=-]{40,}\b`),
}

func sanitizeMemory(text string, limit int) (string, bool, bool) {
	original := text
	for _, re := range memorySecrets {
		text = re.ReplaceAllString(text, "[redacted]")
	}
	// Truncate AFTER redaction, so splitting a credential cannot bypass detection.
	r := []rune(strings.TrimSpace(text))
	truncated := len(r) > limit
	if truncated {
		r = r[:limit]
	}
	return string(r), text != original, truncated
}
func cjkRune(r rune) bool {
	return unicode.In(r, unicode.Han, unicode.Hangul, unicode.Hiragana, unicode.Katakana)
}

// English words and overlapping CJK bigrams share an ordinary B-tree postings
// index. No loadable extension, LIKE scan, arbitrary SQL or FTS query syntax.
func memoryTokens(text string, limit int) []string {
	seen := map[string]bool{}
	out := []string{}
	add := func(s string) {
		if s != "" && !seen[s] && len(out) < limit {
			seen[s] = true
			out = append(out, s)
		}
	}
	var word []rune
	var run []rune
	flushWord := func() {
		if len(word) > 1 {
			add("w:" + string(word))
		}
		word = nil
	}
	flushRun := func() {
		if len(run) == 1 {
			add("c:" + string(run))
		}
		for i := 0; i+1 < len(run); i++ {
			add("c:" + string(run[i:i+2]))
		}
		run = nil
	}
	for _, r := range strings.ToLower(text) {
		if cjkRune(r) {
			flushWord()
			run = append(run, r)
		} else if unicode.IsLetter(r) || unicode.IsNumber(r) {
			flushRun()
			word = append(word, r)
		} else {
			flushWord()
			flushRun()
		}
	}
	flushWord()
	flushRun()
	return out
}
func indexMemoryDB(db *storeConn, id int64, scope, body string) error {
	if _, err := db.Exec(`DELETE FROM memory_terms WHERE doc_id=?`, id); err != nil {
		return err
	}
	for _, term := range memoryTokens(body, 1024) {
		if _, err := db.Exec(`INSERT INTO memory_terms(scope,term,doc_id) VALUES(?,?,?)`, scope, term, id); err != nil {
			return err
		}
	}
	return nil
}
func storeMemoryDocumentDB(db *storeConn, key, role string, e Envelope, body, replyID, remoteID string, at float64) error {
	if !isMessageSource(e) {
		return nil
	}
	body, redacted, truncated := sanitizeMemory(body, memoryBodyLimit)
	current, err := sourceCurrentDB(db, e)
	if err != nil {
		return err
	}
	if !current {
		if role != "assistant" {
			return nil
		}
		var state string
		if err := db.QueryRow(`SELECT state FROM message_sources WHERE platform=? AND event_id=?`, e.Platform, e.EventID).Scan(&state); err != nil {
			return err
		}
		if state != "refresh" {
			return nil
		}
	}
	// Dedup and forget tombstones never resurrect. No raw media URLs or contexts.
	res, err := db.Exec(`INSERT OR IGNORE INTO memory_documents(record_key,scope,role,platform,event_id,source_revision,body,at,active,redacted,truncated,reply_id,remote_id,origin) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, key, memoryScope(e), role, e.Platform, e.EventID, e.SourceRevision, body, at, body != "" && current, redacted, truncated, replyID, remoteID, memoryOriginJSON(e))
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return nil
	}
	id, err := res.LastInsertId()
	if err != nil {
		return err
	}
	return indexMemoryDB(db, id, memoryScope(e), body)
}
func rememberInboundDB(db *storeConn, id string, e Envelope) error {
	if !isMessageSource(e) {
		return nil
	}
	return storeMemoryDocumentDB(db, "user:"+id, "user", e, e.Text, "", "", e.ReceivedAt)
}
func rememberSentDB(db *storeConn, replyID string, index int) error {
	var raw, body, remote, state string
	var at float64
	err := db.QueryRow(`SELECT i.envelope,c.text,COALESCE(c.message_id,''),c.state,r.created FROM chunks c JOIN replies r ON r.id=c.reply_id JOIN inbound i ON i.id=r.inbound_id WHERE c.reply_id=? AND c.idx=?`, replyID, index).Scan(&raw, &body, &remote, &state, &at)
	if err != nil {
		return err
	}
	if state != "sent" || remote == "" {
		return nil
	}
	var e Envelope
	if json.Unmarshal([]byte(raw), &e) != nil {
		return errors.New("invalid_memory_source")
	}
	return storeMemoryDocumentDB(db, fmt.Sprintf("assistant:%s:%d", replyID, index), "assistant", e, body, replyID, remote, at)
}

// The live source join is defense in depth even if an index side effect failed.
var memoryCurrentSQL = operationMemoryCurrentSQL("d") + ` AND d.active=1 AND d.body!='' AND (d.restored=1 OR NOT EXISTS(SELECT 1 FROM inbound i WHERE i.platform=d.platform AND i.event_id=CASE WHEN d.source_revision=0 THEN d.event_id ELSE d.event_id||':revision:'||d.source_revision END AND i.state IN('cancelled','blocked','superseded'))) AND ((d.restored=0 AND EXISTS(SELECT 1 FROM message_sources ms WHERE ms.platform=d.platform AND ms.event_id=d.event_id AND ms.state='current' AND ms.revision=d.source_revision)) OR (d.restored=1 AND NOT EXISTS(SELECT 1 FROM message_sources ms WHERE ms.platform=d.platform AND ms.event_id=d.event_id AND (ms.state!='current' OR ms.revision!=d.source_revision)))) AND NOT EXISTS(SELECT 1 FROM memory_fact_sources fs JOIN memory_documents src ON src.id=fs.source_id LEFT JOIN message_sources ms ON ms.platform=src.platform AND ms.event_id=src.event_id WHERE fs.fact_id=d.id AND (NOT (` + operationMemoryCurrentSQL("src") + `) OR src.active=0 OR src.body='' OR src.source_revision!=fs.source_revision OR (src.restored=0 AND EXISTS(SELECT 1 FROM inbound i WHERE i.platform=src.platform AND i.event_id=CASE WHEN src.source_revision=0 THEN src.event_id ELSE src.event_id||':revision:'||src.source_revision END AND i.state IN('cancelled','blocked','superseded'))) OR (ms.platform IS NULL AND src.restored=0) OR (ms.platform IS NOT NULL AND (ms.state!='current' OR ms.revision!=src.source_revision))))`

func loadMemoryItemDB(db *storeConn, id int64, scope string) (*MemoryItem, error) {
	var item MemoryItem
	var origin string
	var restored bool
	err := db.QueryRow(`SELECT d.record_key,d.role,d.body,d.event_id,d.source_revision,d.at,d.redacted,d.truncated,d.reply_id,d.remote_id,d.origin,d.restored,COALESCE(f.key,''),COALESCE(f.kind,''),COALESCE(f.version,0) FROM memory_documents d LEFT JOIN memory_facts f ON f.doc_id=d.id WHERE d.id=? AND d.scope=? AND `+memoryCurrentSQL, id, scope).Scan(&item.DocumentID, &item.Role, &item.Text, &item.EventID, &item.SourceRevision, &item.At, &item.Redacted, &item.Truncated, &item.ReplyID, &item.RemoteMessageID, &origin, &restored, &item.Key, &item.Kind, &item.Version)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	item.Origin = &MemoryOrigin{}
	if json.Unmarshal([]byte(origin), item.Origin) != nil {
		return nil, errors.New("invalid_memory_origin")
	}
	item.ProvenanceState = "source_revision_evidence"
	if restored {
		item.ProvenanceState = "restored_unverified_history"
	}
	if item.ReplyID != "" {
		state, err := memoryDeliveryStateDB(db, item.ReplyID)
		if err != nil {
			return nil, err
		}
		item.Delivery = state
		if state != "sent" {
			item.Delivery = "partial_sent_reply_" + state
		}
	}
	if item.Role == "fact" {
		rows, err := db.Query(`SELECT d.record_key,fs.source_revision FROM memory_fact_sources fs JOIN memory_documents d ON d.id=fs.source_id WHERE fs.fact_id=? ORDER BY d.id LIMIT 8`, id)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var ref MemoryRef
			if err = rows.Scan(&ref.DocumentID, &ref.SourceRevision); err != nil {
				rows.Close()
				return nil, err
			}
			item.Sources = append(item.Sources, ref)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return nil, err
		}
	}
	return &item, nil
}
func recallMemoryDB(db *storeConn, inbound string, e Envelope, query string) (*MemoryRecall, error) {
	if !isMessageSource(e) {
		return &MemoryRecall{Status: "excluded", Trust: memoryTrust, Scope: "non_message_control_excluded", Items: []MemoryItem{}}, nil
	}
	scope := memoryScope(e)
	out := &MemoryRecall{Status: "ready", Trust: memoryTrust, Scope: "exact_conversation_only", CurrentSource: MemoryRef{"user:" + inbound, e.SourceRevision}, Items: []MemoryItem{}}
	var health string
	if err := db.QueryRow(`SELECT value FROM runtime WHERE key='memory_health'`).Scan(&health); err == nil && health == "degraded" {
		out.Status = "degraded"
	}
	type candidate struct {
		id    int64
		score int
	}
	scores := map[int64]int{}
	collect := func(q string, weight int, args ...any) error {
		rows, err := db.Query(q, args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var id int64
			if err = rows.Scan(&id); err != nil {
				return err
			}
			scores[id] += weight
		}
		return rows.Err()
	}
	// At most 8 * 32 posting candidates + 8 recent + 8 curated. No full scans.
	clean, _, _ := sanitizeMemory(query, 512)
	for _, term := range memoryTokens(clean, 8) {
		if err := collect(`SELECT doc_id FROM memory_terms WHERE scope=? AND term=? ORDER BY doc_id DESC LIMIT 32`, 10, scope, term); err != nil {
			return nil, err
		}
	}
	if err := collect(`SELECT d.id FROM memory_documents d WHERE d.scope=? AND d.active=1 AND d.role!='fact' AND d.record_key!=? ORDER BY d.at DESC,d.id DESC LIMIT 8`, 2, scope, "user:"+inbound); err != nil {
		return nil, err
	}
	if err := collect(`SELECT d.id FROM memory_facts f JOIN memory_documents d ON d.id=f.doc_id WHERE f.scope=? AND d.active=1 ORDER BY d.at DESC,d.id DESC LIMIT 8`, 5, scope); err != nil {
		return nil, err
	}
	list := make([]candidate, 0, len(scores))
	for id, score := range scores {
		list = append(list, candidate{id, score})
	}
	sort.Slice(list, func(i, j int) bool {
		if list[i].score != list[j].score {
			return list[i].score > list[j].score
		}
		return list[i].id > list[j].id
	})
	budget := memoryRecallBudget
	for i, c := range list {
		if i >= 64 {
			out.Truncated = true
			break
		}
		item, err := loadMemoryItemDB(db, c.id, scope)
		if err != nil {
			return nil, err
		}
		if item == nil || item.DocumentID == "user:"+inbound {
			continue
		}
		cap := 1200
		if item.Role == "fact" {
			cap = 600
		}
		r := []rune(item.Text)
		if len(r) > cap {
			r = r[:cap]
			item.Truncated = true
		}
		if len(r) > budget {
			out.Truncated = true
			break
		}
		item.Text = string(r)
		budget -= len(r)
		out.Items = append(out.Items, *item)
		if len(out.Items) >= 12 {
			out.Truncated = len(list) > 12
			break
		}
	}
	return out, nil
}
func claimWithMemoryDB(db *storeConn, id, claim string, until float64, e Envelope) *Claim {
	memoryBestEffort(db, func() error { return rememberInboundDB(db, id, e) })
	memory, err := recallMemoryDB(db, id, e, e.Text)
	if err != nil {
		memory = &MemoryRecall{Status: "unavailable", Trust: memoryTrust, Scope: "exact_conversation_only", CurrentSource: MemoryRef{"user:" + id, e.SourceRevision}, Items: []MemoryItem{}}
	}
	return &Claim{InboundID: id, Claim: claim, LeaseUntil: until, Trust: "untrusted_message_text", Envelope: e, Memory: memory}
}
func memoryClaimDB(db *storeConn, p StorePolicy, id, claim string) (Envelope, error) {
	rows, err := readInbound(db, `SELECT id,state,claim,lease_until,envelope FROM inbound WHERE id=?`, id)
	if err != nil {
		return Envelope{}, err
	}
	if len(rows) != 1 || !isMessageSource(rows[0].event) || rows[0].state != "claimed" || rows[0].claim != claim || rows[0].lease <= epoch() || !p.Accepts(rows[0].event) {
		return Envelope{}, ErrClaim
	}
	current, err := sourceCurrentDB(db, rows[0].event)
	if err != nil {
		return Envelope{}, err
	}
	if !current {
		return Envelope{}, ErrClaim
	}
	return rows[0].event, nil
}
func (s *Store) RecallMemory(id, claim, query string) (*MemoryRecall, error) {
	if len([]rune(query)) > 512 {
		return nil, errors.New("memory_query_too_large")
	}
	v, err := s.call(func(db *storeConn) (any, error) {
		e, err := memoryClaimDB(db, s.policy, id, claim)
		if err != nil {
			return nil, err
		}
		return recallMemoryDB(db, id, e, query)
	})
	if err != nil {
		return nil, err
	}
	return v.(*MemoryRecall), nil
}

var memoryKeyPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_.-]{0,79}$`)

func validMemoryKey(key string) bool {
	if !memoryKeyPattern.MatchString(key) {
		return false
	}
	_, redacted, _ := sanitizeMemory(key, 80)
	return !redacted
}
func (s *Store) PutMemory(id, claim string, m MemoryMutation) (*MemoryItem, error) {
	if !validMemoryKey(m.Key) || m.ExpectedVersion < 0 {
		return nil, errors.New("invalid_memory_key_or_version")
	}
	if !m.Delete && (m.Kind != "fact" && m.Kind != "preference" && m.Kind != "project" && m.Kind != "summary") {
		return nil, errors.New("invalid_memory_kind")
	}
	if !m.Delete && (strings.TrimSpace(m.Text) == "" || len([]rune(m.Text)) > 600 || len(m.Sources) < 1 || len(m.Sources) > 8) {
		return nil, errors.New("invalid_memory_content_or_sources")
	}
	text, redacted, _ := sanitizeMemory(m.Text, 600)
	// Curated notes should omit secrets rather than store redaction placeholders.
	if !m.Delete && redacted {
		return nil, errors.New("memory_sensitive_content_rejected")
	}
	v, err := s.call(func(db *storeConn) (any, error) {
		return transact(db, func(db *storeConn) (any, error) {
			e, err := memoryClaimDB(db, s.policy, id, claim)
			if err != nil {
				return nil, err
			}
			scope := memoryScope(e)
			var doc, version int64
			err = db.QueryRow(`SELECT doc_id,version FROM memory_facts WHERE scope=? AND key=?`, scope, m.Key).Scan(&doc, &version)
			if err != nil && err != sql.ErrNoRows {
				return nil, err
			}
			if version != m.ExpectedVersion {
				return nil, errors.New("memory_version_conflict")
			}
			if m.Delete {
				if doc == 0 {
					return nil, errors.New("memory_not_found")
				}
				if _, err = db.Exec(`UPDATE memory_documents SET body='',active=0 WHERE id=?`, doc); err != nil {
					return nil, err
				}
				if _, err = db.Exec(`UPDATE memory_facts SET version=version+1 WHERE doc_id=?`, doc); err != nil {
					return nil, err
				}
				if err = indexMemoryDB(db, doc, scope, ""); err != nil {
					return nil, err
				}
				return &MemoryItem{DocumentID: "fact:" + scope + ":" + m.Key, Key: m.Key, Version: version + 1}, nil
			}
			sourceIDs := []int64{}
			hasCurrent := false
			seen := map[int64]bool{}
			for _, ref := range m.Sources {
				var sid int64
				if err = db.QueryRow(`SELECT d.id FROM memory_documents d WHERE d.record_key=? AND d.scope=? AND d.source_revision=? AND d.role!='fact' AND d.restored=0 AND `+memoryCurrentSQL, ref.DocumentID, scope, ref.SourceRevision).Scan(&sid); err != nil {
					return nil, errors.New("memory_source_not_current")
				}
				if ref.DocumentID == "user:"+id {
					hasCurrent = true
				}
				if !seen[sid] {
					sourceIDs = append(sourceIDs, sid)
					seen[sid] = true
				}
			}
			if !hasCurrent {
				return nil, errors.New("memory_current_claim_source_required")
			}
			var chars, count int
			if err = db.QueryRow(`SELECT COALESCE(sum(length(d.body)),0),count(*) FROM memory_facts f JOIN memory_documents d ON d.id=f.doc_id WHERE f.scope=? AND d.active=1 AND d.id!=?`, scope, doc).Scan(&chars, &count); err != nil {
				return nil, err
			}
			if chars+len([]rune(text)) > memoryFactBudget || count >= 64 {
				return nil, errors.New("memory_budget_exceeded")
			}
			if doc == 0 {
				if err = memoryKeyCapacityDB(db, scope); err != nil {
					return nil, err
				}
				res, err := db.Exec(`INSERT INTO memory_documents(record_key,scope,role,platform,event_id,source_revision,body,at,origin) VALUES(?,?,'fact',?,?,?,?,?,?)`, "fact:"+scope+":"+m.Key, scope, e.Platform, e.EventID, e.SourceRevision, text, epoch(), memoryOriginJSON(e))
				if err != nil {
					return nil, err
				}
				doc, err = res.LastInsertId()
				if err != nil {
					return nil, err
				}
				if _, err = db.Exec(`INSERT INTO memory_facts VALUES(?,?,?,?,?)`, doc, scope, m.Key, m.Kind, 1); err != nil {
					return nil, err
				}
			} else {
				if _, err = db.Exec(`UPDATE memory_documents SET body=?,active=1,platform=?,event_id=?,source_revision=?,at=?,origin=?,restored=0 WHERE id=?`, text, e.Platform, e.EventID, e.SourceRevision, epoch(), memoryOriginJSON(e), doc); err != nil {
					return nil, err
				}
				if _, err = db.Exec(`UPDATE memory_facts SET version=version+1,kind=? WHERE doc_id=?`, m.Kind, doc); err != nil {
					return nil, err
				}
				if _, err = db.Exec(`DELETE FROM memory_fact_sources WHERE fact_id=?`, doc); err != nil {
					return nil, err
				}
			}
			for _, sid := range sourceIDs {
				if _, err = db.Exec(`INSERT INTO memory_fact_sources(fact_id,source_id,source_revision) SELECT ?,id,source_revision FROM memory_documents WHERE id=?`, doc, sid); err != nil {
					return nil, err
				}
			}
			if err = indexMemoryDB(db, doc, scope, text); err != nil {
				return nil, err
			}
			return loadMemoryItemDB(db, doc, scope)
		})
	})
	if err != nil {
		return nil, err
	}
	return v.(*MemoryItem), nil
}

// Source lifecycle authority must commit even when any derived memory table is
// missing/corrupt. Text invalidation and posting cleanup are independent savepoints.
func invalidateMemorySourceDB(db *storeConn, e Envelope) {
	var state string
	if db.QueryRow(`SELECT state FROM message_sources WHERE platform=? AND event_id=?`, e.Platform, e.EventID).Scan(&state) != nil {
		return
	}
	memoryBestEffort(db, func() error {
		if _, err := db.Exec(`UPDATE memory_facts SET version=version+1 WHERE doc_id IN(SELECT fs.fact_id FROM memory_fact_sources fs JOIN memory_documents src ON src.id=fs.source_id WHERE src.platform=? AND src.event_id=?)`, e.Platform, e.EventID); err != nil {
			return err
		}
		if _, err := db.Exec(`UPDATE memory_documents SET active=0,body='' WHERE id IN(SELECT fs.fact_id FROM memory_fact_sources fs JOIN memory_documents src ON src.id=fs.source_id WHERE src.platform=? AND src.event_id=?)`, e.Platform, e.EventID); err != nil {
			return err
		}
		if state == "deleted" {
			_, err := db.Exec(`UPDATE memory_documents SET active=0,body='' WHERE platform=? AND event_id=?`, e.Platform, e.EventID)
			return err
		}
		_, err := db.Exec(`UPDATE memory_documents SET active=0 WHERE platform=? AND event_id=? AND role!='fact'`, e.Platform, e.EventID)
		return err
	})
	memoryBestEffort(db, func() error {
		if _, err := db.Exec(`DELETE FROM memory_terms WHERE doc_id IN(SELECT fs.fact_id FROM memory_fact_sources fs JOIN memory_documents src ON src.id=fs.source_id WHERE src.platform=? AND src.event_id=?)`, e.Platform, e.EventID); err != nil {
			return err
		}
		if state == "deleted" {
			_, err := db.Exec(`DELETE FROM memory_terms WHERE doc_id IN(SELECT id FROM memory_documents WHERE platform=? AND event_id=?)`, e.Platform, e.EventID)
			return err
		}
		return nil
	})
}
func restoreMemorySourceDB(db *storeConn, e Envelope) {
	memoryBestEffort(db, func() error {
		_, err := db.Exec(`UPDATE memory_documents SET active=1 WHERE platform=? AND event_id=? AND source_revision=? AND body!='' AND role!='fact'`, e.Platform, e.EventID, e.SourceRevision)
		return err
	})
}

// Indexed existence probes avoid hydrating all chunks of a long reply for recall.
func memoryDeliveryStateDB(db *storeConn, id string) (string, error) {
	var state string
	err := db.QueryRow(`SELECT CASE
 WHEN EXISTS(SELECT 1 FROM reply_cancellations WHERE reply_id=?) THEN 'cancelled'
 WHEN EXISTS(SELECT 1 FROM chunks WHERE state='uncertain' AND reply_id=?) THEN 'uncertain'
 WHEN EXISTS(SELECT 1 FROM chunks WHERE state='failed' AND reply_id=?) THEN 'failed'
 WHEN EXISTS(SELECT 1 FROM chunks WHERE state='sending' AND reply_id=?) THEN 'sending'
 WHEN EXISTS(SELECT 1 FROM chunks WHERE state='pending' AND reply_id=?) THEN 'queued'
 ELSE 'sent' END`, id, id, id, id, id).Scan(&state)
	return state, err
}

// Deleted keys retain version/forget semantics, so cap them as well as active
// facts. Existing keys remain reusable; every scope aggregate is then bounded.
func memoryKeyCapacityDB(db *storeConn, scope string) error {
	var count int
	if err := db.QueryRow(`SELECT count(*) FROM (SELECT 1 FROM memory_facts WHERE scope=? LIMIT ?)`, scope, memoryRetainedKeyLimit).Scan(&count); err != nil {
		return err
	}
	if count >= memoryRetainedKeyLimit {
		return errors.New("memory_key_budget_exceeded")
	}
	return nil
}
