package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"golang.org/x/sys/unix"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	_ "modernc.org/sqlite"
)

type Event struct {
	ID       string  `json:"id"`
	Platform string  `json:"platform"`
	EventID  string  `json:"event_id"`
	State    string  `json:"state"`
	Created  float64 `json:"created"`
}
type Reply struct {
	ID        string  `json:"id"`
	InboundID string  `json:"inbound_id"`
	Created   float64 `json:"created"`
}
type Chunk struct {
	ReplyID   string `json:"reply_id"`
	Index     int    `json:"idx"`
	State     string `json:"state"`
	MessageID string `json:"message_id"`
	Attempts  int    `json:"attempts"`
}
type Diagnostic struct {
	ID        string  `json:"id"`
	Index     int     `json:"idx"`
	Nonce     string  `json:"nonce"`
	BotID     string  `json:"bot_id"`
	OwnerID   string  `json:"owner_id"`
	GuildID   string  `json:"guild_id"`
	ChannelID string  `json:"channel_id"`
	State     string  `json:"state"`
	Attempts  int     `json:"attempts"`
	MessageID string  `json:"message_id"`
	Created   float64 `json:"created"`
}
type TestSend struct {
	BotID     string  `json:"bot_id"`
	GuildID   string  `json:"guild_id"`
	ChannelID string  `json:"channel_id"`
	OwnerID   string  `json:"owner_id"`
	Nonce     string  `json:"nonce"`
	State     string  `json:"state"`
	Attempted int     `json:"attempted"`
	MessageID string  `json:"message_id"`
	Created   float64 `json:"created"`
	Updated   float64 `json:"updated"`
}
type Target struct {
	BotID     string `json:"bot_id"`
	GuildID   string `json:"guild_id"`
	ChannelID string `json:"channel_id"`
}
type ReportChunk struct {
	Index       int    `json:"index"`
	ContentHash string `json:"content_sha256"`
	Nonce       string `json:"nonce"`
	State       string `json:"state"`
	AttemptedAt string `json:"attempted_at,omitempty"`
	ConfirmedAt string `json:"confirmed_at,omitempty"`
	MessageID   string `json:"message_id,omitempty"`
	MessageURL  string `json:"message_url,omitempty"`
	Code        string `json:"code,omitempty"`
}
type Receipt struct {
	Version     int           `json:"version"`
	RunID       string        `json:"run_id"`
	Target      Target        `json:"target"`
	PayloadHash string        `json:"payload_sha256"`
	CreatedAt   string        `json:"created_at"`
	Chunks      []ReportChunk `json:"chunks"`
	Complete    bool          `json:"complete"`
}
type Operation struct {
	OwnerID          string `json:"owner_id"`
	BotID            string `json:"bot_id"`
	GuildID          string `json:"guild_id"`
	GatewayChannelID string `json:"gateway_channel_id"`
	ReportChannelID  string `json:"report_channel_id"`
	GuildMode        string `json:"guild_mode"`
	MessageContent   bool   `json:"message_content_approved"`
}
type MemorySourceFence struct {
	Scope    string `json:"scope,omitempty"`
	Platform string `json:"platform"`
	EventID  string `json:"event_id"`
	Revision int64  `json:"revision"`
	State    string `json:"state"`
}
type MemoryDocumentFence struct {
	DocumentID string `json:"document_id"`
	Version    int64  `json:"version"`
	Forgotten  bool   `json:"forgotten"`
}
type Snapshot struct {
	WorkerControl     *WorkerControlState    `json:"worker_control,omitempty"`
	MessageOperations *MessageOperationState `json:"message_operations,omitempty"`
	Phase3            *Phase3State           `json:"phase3,omitempty"`
	MemorySources     []MemorySourceFence    `json:"memory_sources,omitempty"`
	MemoryFences      []MemoryDocumentFence  `json:"memory_fences,omitempty"`
	MemoryLedgerID    string                 `json:"memory_ledger_id,omitempty"`
	Schema            int                    `json:"schema"`
	Created           string                 `json:"created_at"`
	ManifestSHA       string                 `json:"source_manifest_sha256"`
	Operation         Operation              `json:"operation"`
	Events            []Event                `json:"events"`
	Ingress           []Event                `json:"ingress"`
	Replies           []Reply                `json:"replies"`
	Chunks            []Chunk                `json:"chunks"`
	Diagnostics       []Diagnostic           `json:"diagnostics"`
	TestSends         []TestSend             `json:"test_sends"`
	Reports           []Receipt              `json:"reports"`
}

var ident = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$`)
var snowflake = regexp.MustCompile(`^[1-9][0-9]{0,19}$`)

func validID(s string) bool {
	return ident.MatchString(s) && !strings.HasPrefix(s, "ghp_") && !strings.HasPrefix(s, "github_pat_") && !strings.HasPrefix(s, "sk-")
}
func snowflakeID(s string) bool {
	if !snowflake.MatchString(s) {
		return false
	}
	_, e := strconv.ParseUint(s, 10, 64)
	return e == nil
}
func optionalID(s string) bool      { return s == "" || snowflakeID(s) }
func finitePositive(f float64) bool { return f > 0 && !math.IsNaN(f) && !math.IsInf(f, 0) }
func stampOK(s string) bool         { _, e := time.Parse(time.RFC3339Nano, s); return e == nil }
func operationOK(o Operation) bool {
	return snowflakeID(o.OwnerID) && snowflakeID(o.BotID) && snowflakeID(o.GuildID) && snowflakeID(o.GatewayChannelID) && snowflakeID(o.ReportChannelID) && (o.GuildMode == "mention" && !o.MessageContent || o.GuildMode == "all" && o.MessageContent)
}
func targetOK(t Target) bool {
	return snowflakeID(t.BotID) && snowflakeID(t.GuildID) && snowflakeID(t.ChannelID) && t.BotID != t.GuildID && t.BotID != t.ChannelID && t.GuildID != t.ChannelID
}

type readDB struct {
	*sql.DB
	dir  *os.File
	file *os.File
}

func (d *readDB) Close() error { e := d.DB.Close(); d.file.Close(); d.dir.Close(); return e }
func dbRO(p string) (*readDB, error) {
	d, e := openDir(filepath.Dir(p), true)
	if e != nil {
		return nil, e
	}
	fail := func(e error) (*readDB, error) { d.Close(); return nil, e }
	name := filepath.Base(p)
	fd, e := unix.Openat(int(d.Fd()), name, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if e != nil {
		return fail(errors.New("database open failed"))
	}
	f := os.NewFile(uintptr(fd), "database")
	safe := func(st os.FileInfo) bool {
		u, ok := st.Sys().(*syscall.Stat_t)
		return ok && st.Mode().IsRegular() && owner(st) && st.Mode().Perm()&0077 == 0 && u.Nlink == 1
	}
	st, e := f.Stat()
	if e != nil || !safe(st) {
		f.Close()
		return fail(errors.New("database must be owner-private unlinked regular file"))
	}
	base := "/proc/self/fd/" + itoa(int(d.Fd())) + "/" + name
	for _, suffix := range []string{"-wal", "-shm", "-journal"} {
		side, e := os.Lstat(base + suffix)
		if os.IsNotExist(e) {
			continue
		}
		if e != nil || !safe(side) {
			f.Close()
			return fail(errors.New("unsafe SQLite sidecar metadata"))
		}
	}
	u := url.URL{Scheme: "file", Path: base}
	q := u.Query()
	q.Set("mode", "ro")
	q.Add("_pragma", "query_only(1)")
	q.Add("_pragma", "busy_timeout(5000)")
	u.RawQuery = q.Encode()
	db, e := sql.Open("sqlite", u.String())
	if e != nil {
		f.Close()
		return fail(e)
	}
	db.SetMaxOpenConns(1)
	if e = db.Ping(); e != nil {
		db.Close()
		f.Close()
		return fail(e)
	}
	after, e := os.Lstat(base)
	if e != nil || !safe(after) || !os.SameFile(st, after) {
		db.Close()
		f.Close()
		return fail(errors.New("database replaced during open"))
	}
	return &readDB{db, d, f}, nil
}

// Deliberately fixed SQL allowlist. Never SELECT *, envelope, text, content,
// runtime values, claims, authentication configuration, or cookies.
func snapshotDB(p string, s *Snapshot) error {
	db, e := dbRO(p)
	if e != nil {
		return e
	}
	defer db.Close()
	tx, e := db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
	if e != nil {
		return e
	}
	defer tx.Rollback()
	query := func(q string, scan func(*sql.Rows) error) error {
		r, e := tx.Query(q)
		if e != nil {
			return e
		}
		defer r.Close()
		for r.Next() {
			if e = scan(r); e != nil {
				return e
			}
		}
		return r.Err()
	}
	for _, table := range []string{"inbound", "ingress_validation"} {
		e = query("SELECT id,platform,event_id,state,created FROM "+table+" ORDER BY id", func(r *sql.Rows) error {
			var v Event
			e := r.Scan(&v.ID, &v.Platform, &v.EventID, &v.State, &v.Created)
			if table == "inbound" {
				s.Events = append(s.Events, v)
			} else {
				s.Ingress = append(s.Ingress, v)
			}
			return e
		})
		if e != nil {
			return e
		}
	}
	if e = query("SELECT id,inbound_id,created FROM replies ORDER BY id", func(r *sql.Rows) error {
		var v Reply
		e := r.Scan(&v.ID, &v.InboundID, &v.Created)
		s.Replies = append(s.Replies, v)
		return e
	}); e != nil {
		return e
	}
	if e = query("SELECT reply_id,idx,state,COALESCE(message_id,''),attempts FROM chunks ORDER BY reply_id,idx", func(r *sql.Rows) error {
		var v Chunk
		e := r.Scan(&v.ReplyID, &v.Index, &v.State, &v.MessageID, &v.Attempts)
		s.Chunks = append(s.Chunks, v)
		return e
	}); e != nil {
		return e
	}
	if e = query("SELECT id,idx,nonce,bot_id,owner_id,guild_id,channel_id,state,attempts,COALESCE(message_id,''),created FROM go_transport_diagnostics ORDER BY idx", func(r *sql.Rows) error {
		var v Diagnostic
		e := r.Scan(&v.ID, &v.Index, &v.Nonce, &v.BotID, &v.OwnerID, &v.GuildID, &v.ChannelID, &v.State, &v.Attempts, &v.MessageID, &v.Created)
		s.Diagnostics = append(s.Diagnostics, v)
		return e
	}); e != nil {
		return e
	}
	if e = query("SELECT bot_id,guild_id,channel_id,owner_id,nonce,state,attempted,COALESCE(message_id,''),created,updated FROM test_sends ORDER BY bot_id,guild_id,channel_id", func(r *sql.Rows) error {
		var v TestSend
		e := r.Scan(&v.BotID, &v.GuildID, &v.ChannelID, &v.OwnerID, &v.Nonce, &v.State, &v.Attempted, &v.MessageID, &v.Created, &v.Updated)
		s.TestSends = append(s.TestSends, v)
		return e
	}); e != nil {
		return e
	}
	var hasMemory int
	if e = tx.QueryRow("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='memory_meta'").Scan(&hasMemory); e != nil {
		return e
	}
	if hasMemory != 0 {
		e = tx.QueryRow("SELECT value FROM memory_meta WHERE key='ledger_identity'").Scan(&s.MemoryLedgerID)
		if e != nil && e != sql.ErrNoRows {
			return e
		}
		if s.MemoryLedgerID != "" && !regexp.MustCompile(`^[0-9a-f]{32}$`).MatchString(s.MemoryLedgerID) {
			return errors.New("invalid memory ledger identity")
		}
	}
	if s.MemoryLedgerID != "" {
		if e = query("SELECT ms.platform,ms.event_id,ms.revision,CASE WHEN EXISTS(SELECT 1 FROM inbound i WHERE i.platform=ms.platform AND i.event_id=CASE WHEN ms.revision=0 THEN ms.event_id ELSE ms.event_id||':revision:'||ms.revision END AND i.state='cancelled') THEN 'rejected' ELSE ms.state END,COALESCE((SELECT d.scope FROM memory_documents d WHERE d.platform=ms.platform AND d.event_id=ms.event_id AND d.source_revision=ms.revision AND d.role='user' ORDER BY d.id DESC LIMIT 1),'') FROM message_sources ms ORDER BY ms.platform,ms.event_id", func(r *sql.Rows) error {
			var v MemorySourceFence
			if e := r.Scan(&v.Platform, &v.EventID, &v.Revision, &v.State, &v.Scope); e != nil {
				return e
			}
			s.MemorySources = append(s.MemorySources, v)
			return nil
		}); e != nil {
			return e
		}
		if e = query("SELECT d.record_key,COALESCE(f.version,0),d.active=0 OR d.body='' FROM memory_documents d LEFT JOIN memory_facts f ON f.doc_id=d.id WHERE d.active=0 OR d.body='' OR f.doc_id IS NOT NULL ORDER BY d.id", func(r *sql.Rows) error {
			var v MemoryDocumentFence
			if e := r.Scan(&v.DocumentID, &v.Version, &v.Forgotten); e != nil {
				return e
			}
			s.MemoryFences = append(s.MemoryFences, v)
			return nil
		}); e != nil {
			return e
		}
	}

	if s.MemoryLedgerID != "" {
		sources := map[string]MemorySourceFence{}
		for _, f := range s.MemorySources {
			sources[f.Platform+":"+f.EventID] = f
		}
		documents := map[string]MemoryDocumentFence{}
		for _, f := range s.MemoryFences {
			documents[f.DocumentID] = f
		}
		var oldSources, oldDocuments int
		if e = tx.QueryRow("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='memory_source_fences'").Scan(&oldSources); e != nil {
			return e
		}
		if e = tx.QueryRow("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='memory_restore_fences'").Scan(&oldDocuments); e != nil {
			return e
		}
		if oldSources != 0 {
			if e = query("SELECT platform,event_id,revision,state,scope FROM memory_source_fences", func(r *sql.Rows) error {
				var f MemorySourceFence
				if e := r.Scan(&f.Platform, &f.EventID, &f.Revision, &f.State, &f.Scope); e != nil {
					return e
				}
				key := f.Platform + ":" + f.EventID
				current, ok := sources[key]
				if !ok || f.State == "deleted" || f.Revision > current.Revision || (f.Revision == current.Revision && f.State != "current") {
					sources[key] = f
				}
				return nil
			}); e != nil {
				return e
			}
		}
		if oldDocuments != 0 {
			if e = query("SELECT record_key,version,forgotten FROM memory_restore_fences", func(r *sql.Rows) error {
				var f MemoryDocumentFence
				if e := r.Scan(&f.DocumentID, &f.Version, &f.Forgotten); e != nil {
					return e
				}
				current, ok := documents[f.DocumentID]
				if !ok || f.Version > current.Version || (f.Version == current.Version && f.Forgotten) {
					documents[f.DocumentID] = f
				}
				return nil
			}); e != nil {
				return e
			}
		}
		s.MemorySources = nil
		for _, f := range sources {
			s.MemorySources = append(s.MemorySources, f)
		}
		sort.Slice(s.MemorySources, func(i, j int) bool { return s.MemorySources[i].EventID < s.MemorySources[j].EventID })
		s.MemoryFences = nil
		for _, f := range documents {
			s.MemoryFences = append(s.MemoryFences, f)
		}
		sort.Slice(s.MemoryFences, func(i, j int) bool { return s.MemoryFences[i].DocumentID < s.MemoryFences[j].DocumentID })
	}
	if e = snapshotPhase3(tx, s); e != nil {
		return e
	}
	if e = snapshotMessageOperations(tx, s); e != nil {
		return e
	}
	if e = snapshotWorkerControl(tx, s); e != nil {
		return e
	}
	return tx.Commit()
}
func snapshotReports(dir string, s *Snapshot) error {
	d, e := openDir(dir, true)
	if e != nil {
		return e
	}
	defer d.Close()
	r, e := os.OpenRoot("/proc/self/fd/" + itoa(int(d.Fd())))
	if e != nil {
		return e
	}
	defer r.Close()
	targetBytes, e := readRegular(r, "target.json", true, 16<<10)
	if e != nil {
		return errors.New("report directory must contain private pinned target.json")
	}
	var target Target
	if strict(targetBytes, &target) != nil || !targetOK(target) || target != (Target{s.Operation.BotID, s.Operation.GuildID, s.Operation.ReportChannelID}) {
		return errors.New("report directory target binding mismatch")
	}
	entries, e := d.ReadDir(-1)
	if e != nil {
		return e
	}
	for _, entry := range entries {
		n := entry.Name()
		if !strings.HasPrefix(n, "run-") || !strings.HasSuffix(n, ".json") {
			continue
		}
		b, e := readRegular(r, n, true, 256<<10)
		if e != nil {
			return e
		}
		var v Receipt
		if strict(b, &v) != nil {
			return errors.New("invalid receipt schema")
		}
		if n != "run-"+digest([]byte(v.RunID))+".json" {
			return errors.New("receipt filename binding mismatch")
		}
		s.Reports = append(s.Reports, v)
	}
	return nil
}
func sourceEventID(s string) bool {
	if snowflakeID(s) {
		return true
	}
	base, revision, ok := strings.Cut(s, ":revision:")
	if !ok || !snowflakeID(base) || revision == "" || revision[0] == '0' {
		return false
	}
	n, err := strconv.ParseInt(revision, 10, 64)
	return err == nil && n > 0 && strconv.FormatInt(n, 10) == revision
}
func transportEventID(s string) bool {
	if sourceEventID(s) {
		return true
	}
	id, ok := strings.CutPrefix(s, "control:")
	return ok && (snowflakeID(id) || regexp.MustCompile(`^[0-9a-f]{32}$`).MatchString(id) || validReactionTransition(id))
}
func validateSnapshot(s Snapshot) error {
	if err := validateWorkerControl(s); err != nil {
		return err
	}
	if err := validateMessageOperations(s); err != nil {
		return err
	}
	if err := validatePhase3(s); err != nil {
		return err
	}
	if s.MemoryLedgerID == "" && (len(s.MemorySources) > 0 || len(s.MemoryFences) > 0) {
		return errors.New("memory fences require ledger identity")
	}
	seenSources, seenDocuments := map[string]bool{}, map[string]bool{}
	for _, v := range s.MemorySources {
		key := v.Platform + ":" + v.EventID
		if v.Platform != "discord" || !snowflakeID(v.EventID) || v.Revision < 0 || (v.Scope != "" && !hashPattern.MatchString(v.Scope)) || seenSources[key] || (v.State != "current" && v.State != "refresh" && v.State != "deleted" && v.State != "rejected") {
			return errors.New("invalid memory source fence")
		}
		seenSources[key] = true
	}
	for _, v := range s.MemoryFences {
		if !regexp.MustCompile(`^(user:[A-Za-z0-9_-]{1,128}|assistant:[A-Za-z0-9_-]{1,128}:[0-9]{1,10}|assistant-edit:[0-9a-f]{64}|fact:[0-9a-f]{64}:[a-z0-9][a-z0-9_.-]{0,79})$`).MatchString(v.DocumentID) || v.Version < 0 || seenDocuments[v.DocumentID] || (strings.HasPrefix(v.DocumentID, "fact:") && v.Version == 0) {
			return errors.New("invalid memory document fence")
		}
		seenDocuments[v.DocumentID] = true
	}

	if s.MemoryLedgerID != "" && !regexp.MustCompile(`^[0-9a-f]{32}$`).MatchString(s.MemoryLedgerID) {
		return errors.New("invalid memory ledger identity")
	}
	if s.Schema != 1 || !stampOK(s.Created) || !hashPattern.MatchString(s.ManifestSHA) || !operationOK(s.Operation) {
		return errors.New("invalid snapshot header or operation")
	}
	ids, events, replies, chunks := map[string]bool{}, map[string]bool{}, map[string]bool{}, map[string]bool{}
	for _, v := range append(append([]Event{}, s.Events...), s.Ingress...) {
		key := v.Platform + "/" + v.EventID
		if !validID(v.ID) || v.Platform != "discord" || !transportEventID(v.EventID) || !validID(v.State) || !finitePositive(v.Created) || ids[v.ID] || events[key] {
			return errors.New("invalid or duplicate event")
		}
		if owner := reactionTransitionOwner(v.EventID); owner != "" && owner != s.Operation.OwnerID {
			return errors.New("reaction event owner mismatch")
		}
		ids[v.ID] = true
		events[key] = true
	}
	replyInbound := map[string]bool{}
	for _, v := range s.Replies {
		if !validID(v.ID) || !ids[v.InboundID] || replies[v.ID] || replyInbound[v.InboundID] || !finitePositive(v.Created) {
			return errors.New("invalid or duplicate reply")
		}
		replies[v.ID] = true
		replyInbound[v.InboundID] = true
	}
	for _, v := range s.Chunks {
		key := fmt.Sprintf("%s/%d", v.ReplyID, v.Index)
		if !replies[v.ReplyID] || v.Index < 0 || v.Index > 1000 || !validID(v.State) || !optionalID(v.MessageID) || v.Attempts < 0 || chunks[key] || v.State == "sent" && v.MessageID == "" {
			return errors.New("invalid or duplicate chunk")
		}
		chunks[key] = true
	}
	diagnostics := map[int]bool{}
	diagnosticIDs := map[string]bool{}
	for _, v := range s.Diagnostics {
		if !validID(v.ID) || v.Index < 0 || v.Index > 2 || !validID(v.Nonce) || !snowflakeID(v.BotID) || !snowflakeID(v.OwnerID) || !snowflakeID(v.GuildID) || !snowflakeID(v.ChannelID) || !validID(v.State) || !optionalID(v.MessageID) || !finitePositive(v.Created) || diagnostics[v.Index] || diagnosticIDs[v.ID] {
			return errors.New("invalid diagnostic metadata")
		}
		diagnostics[v.Index] = true
		diagnosticIDs[v.ID] = true
	}
	tests := map[string]bool{}
	for _, v := range s.TestSends {
		key := v.BotID + "/" + v.GuildID + "/" + v.ChannelID
		if !snowflakeID(v.BotID) || !snowflakeID(v.OwnerID) || !snowflakeID(v.GuildID) || !snowflakeID(v.ChannelID) || !validID(v.Nonce) || !validID(v.State) || !optionalID(v.MessageID) || !finitePositive(v.Created) || !finitePositive(v.Updated) || tests[key] {
			return errors.New("invalid test-send metadata")
		}
		tests[key] = true
	}
	runs := map[string]bool{}
	for _, r := range s.Reports {
		if r.Version != 1 || !validID(r.RunID) || runs[r.RunID] || !targetOK(r.Target) || r.Target != (Target{s.Operation.BotID, s.Operation.GuildID, s.Operation.ReportChannelID}) || !hashPattern.MatchString(r.PayloadHash) || !stampOK(r.CreatedAt) || len(r.Chunks) == 0 || len(r.Chunks) > 40 {
			return errors.New("invalid or duplicate receipt")
		}
		runs[r.RunID] = true
		all, unconfirmed := true, false
		for i, c := range r.Chunks {
			nonceInput, _ := json.Marshal([]any{"discord-report/v1", r.Target, r.RunID, r.PayloadHash, i})
			if c.Nonce != digest(nonceInput)[:24] {
				return errors.New("invalid receipt nonce binding")
			}
			if c.Index != i || !hashPattern.MatchString(c.ContentHash) || !validID(c.Nonce) || !optionalID(c.MessageID) || c.Code != "" && !validID(c.Code) {
				return errors.New("invalid receipt chunk")
			}
			if c.State != "confirmed" {
				all = false
				unconfirmed = true
			} else if unconfirmed {
				return errors.New("invalid receipt order")
			}
			switch c.State {
			case "prepared":
				if c.AttemptedAt != "" || c.MessageID != "" || c.ConfirmedAt != "" {
					return errors.New("invalid prepared receipt")
				}
			case "attempted", "failed":
				if !stampOK(c.AttemptedAt) || c.MessageID != "" || c.ConfirmedAt != "" {
					return errors.New("invalid attempted receipt")
				}
			case "uncertain", "acknowledged":
				if !stampOK(c.AttemptedAt) || c.ConfirmedAt != "" || c.State == "acknowledged" && c.MessageID == "" {
					return errors.New("invalid uncertain receipt")
				}
			case "confirmed":
				if !stampOK(c.AttemptedAt) || !stampOK(c.ConfirmedAt) || c.MessageID == "" {
					return errors.New("invalid confirmed receipt")
				}
			default:
				return errors.New("invalid receipt state")
			}
			if c.MessageID != "" && c.MessageURL != "https://discord.com/channels/"+r.Target.GuildID+"/"+r.Target.ChannelID+"/"+c.MessageID || c.MessageID == "" && c.MessageURL != "" {
				return errors.New("invalid receipt URL")
			}
		}
		if r.Complete != all {
			return errors.New("invalid receipt complete flag")
		}
	}
	return nil
}

const restoreSchema = `PRAGMA foreign_keys=ON; PRAGMA journal_mode=DELETE; PRAGMA synchronous=FULL;
CREATE TABLE catchup_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
INSERT INTO catchup_meta VALUES('state','disarmed_restore');
CREATE TABLE inbound(id TEXT PRIMARY KEY,platform TEXT NOT NULL,event_id TEXT NOT NULL,envelope TEXT NOT NULL,state TEXT NOT NULL DEFAULT 'pending',claim TEXT,lease_until REAL,created REAL NOT NULL,UNIQUE(platform,event_id));
CREATE TABLE replies(id TEXT PRIMARY KEY,inbound_id TEXT NOT NULL UNIQUE REFERENCES inbound(id),text TEXT NOT NULL,created REAL NOT NULL);
CREATE TABLE chunks(reply_id TEXT NOT NULL REFERENCES replies(id),idx INTEGER NOT NULL,text TEXT NOT NULL,state TEXT NOT NULL DEFAULT 'pending',message_id TEXT,code TEXT,attempts INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(reply_id,idx));
CREATE TABLE go_transport_diagnostics(id TEXT PRIMARY KEY,idx INTEGER NOT NULL UNIQUE CHECK(idx BETWEEN 0 AND 2),nonce TEXT NOT NULL,content TEXT NOT NULL,bot_id TEXT NOT NULL,owner_id TEXT NOT NULL,guild_id TEXT NOT NULL,channel_id TEXT NOT NULL,state TEXT NOT NULL,attempts INTEGER NOT NULL DEFAULT 0 CHECK(attempts BETWEEN 0 AND 1),message_id TEXT,code TEXT,created REAL NOT NULL,send_started REAL,acknowledged REAL,remote_created REAL,timings TEXT);
CREATE TABLE test_sends(bot_id TEXT NOT NULL,guild_id TEXT NOT NULL,channel_id TEXT NOT NULL,owner_id TEXT NOT NULL,nonce TEXT NOT NULL,content TEXT NOT NULL,state TEXT NOT NULL,attempted INTEGER NOT NULL DEFAULT 0,message_id TEXT,code TEXT,created REAL NOT NULL,updated REAL NOT NULL,verified_at REAL,PRIMARY KEY(bot_id,guild_id,channel_id),CHECK(attempted IN (0,1)));`

func restoreState(s Snapshot, dest string, failAfter int) error {
	if e := validateSnapshot(s); e != nil {
		return e
	}
	return atomicDir(dest, func(stage string) error {
		if e := writeFile(stage, "bridge/bridge.sqlite3", nil); e != nil {
			return e
		}
		db, e := sql.Open("sqlite", rootJoin(stage, "bridge/bridge.sqlite3"))
		if e != nil {
			return e
		}
		defer db.Close()
		if _, e = db.Exec(restoreSchema); e != nil {
			return e
		}
		tx, e := db.Begin()
		if e != nil {
			return e
		}
		defer tx.Rollback()
		for i, v := range append(append([]Event{}, s.Events...), s.Ingress...) {
			if i == failAfter {
				return errors.New("injected interrupted state write")
			}
			if _, e = tx.Exec("INSERT INTO inbound(id,platform,event_id,envelope,state,created) VALUES(?,?,?,'{}','blocked',?)", v.ID, v.Platform, v.EventID, v.Created); e != nil {
				return e
			}
		}
		for _, v := range s.Replies {
			if _, e = tx.Exec("INSERT INTO replies VALUES(?,?,'',?)", v.ID, v.InboundID, v.Created); e != nil {
				return e
			}
		}
		for _, v := range s.Chunks {
			state := "uncertain"
			if v.State == "sent" {
				state = "sent"
			}
			if _, e = tx.Exec("INSERT INTO chunks VALUES(?,?,'',?,?,?,?)", v.ReplyID, v.Index, state, v.MessageID, "recovery_history_no_replay", v.Attempts); e != nil {
				return e
			}
		}
		for _, v := range s.Diagnostics {
			if _, e = tx.Exec("INSERT INTO go_transport_diagnostics(id,idx,nonce,content,bot_id,owner_id,guild_id,channel_id,state,attempts,message_id,code,created) VALUES(?,?,?,'',?,?,?,?,'uncertain',1,?,'recovery_history_no_replay',?)", v.ID, v.Index, v.Nonce, v.BotID, v.OwnerID, v.GuildID, v.ChannelID, v.MessageID, v.Created); e != nil {
				return e
			}
		}
		for _, v := range s.TestSends {
			if _, e = tx.Exec("INSERT INTO test_sends(bot_id,guild_id,channel_id,owner_id,nonce,content,state,attempted,message_id,code,created,updated) VALUES(?,?,?,?,?,'','uncertain',1,?,'recovery_history_no_replay',?,?)", v.BotID, v.GuildID, v.ChannelID, v.OwnerID, v.Nonce, v.MessageID, v.Created, v.Updated); e != nil {
				return e
			}
		}
		if s.MemoryLedgerID != "" {
			if _, e = tx.Exec("CREATE TABLE memory_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL)"); e != nil {
				return e
			}
			if _, e = tx.Exec("CREATE TABLE memory_source_fences(platform TEXT NOT NULL,event_id TEXT NOT NULL,revision INTEGER NOT NULL,state TEXT NOT NULL,scope TEXT NOT NULL,PRIMARY KEY(platform,event_id)); CREATE TABLE memory_restore_fences(record_key TEXT PRIMARY KEY,version INTEGER NOT NULL,forgotten INTEGER NOT NULL)"); e != nil {
				return e
			}
			for _, f := range s.MemorySources {
				if _, e = tx.Exec("INSERT INTO memory_source_fences VALUES(?,?,?,?,?)", f.Platform, f.EventID, f.Revision, f.State, f.Scope); e != nil {
					return e
				}
			}
			for _, f := range s.MemoryFences {
				if _, e = tx.Exec("INSERT INTO memory_restore_fences VALUES(?,?,?)", f.DocumentID, f.Version, f.Forgotten); e != nil {
					return e
				}
			}
			for key, value := range map[string]string{"ledger_identity": s.MemoryLedgerID, "restored_metadata_only": "1", "restored_owner_id": s.Operation.OwnerID, "restored_guild_id": s.Operation.GuildID, "restored_channel_id": s.Operation.GatewayChannelID} {
				if _, e = tx.Exec("INSERT INTO memory_meta(key,value) VALUES(?,?)", key, value); e != nil {
					return e
				}
			}
		}
		if e = restorePhase3(tx, s); e != nil {
			return e
		}
		if e = restoreMessageOperations(tx, s); e != nil {
			return e
		}
		if e = restoreWorkerControl(tx, s); e != nil {
			return e
		}
		if e = tx.Commit(); e != nil {
			return e
		}
		if e = db.Close(); e != nil {
			return e
		}
		f, e := os.Open(rootJoin(stage, "bridge/bridge.sqlite3"))
		if e != nil {
			return e
		}
		e = f.Sync()
		f.Close()
		if e != nil {
			return e
		}
		if e = writeFile(stage, "operation.json", jsonBytes(s.Operation)); e != nil {
			return e
		}
		t := Target{s.Operation.BotID, s.Operation.GuildID, s.Operation.ReportChannelID}
		if e = writeFile(stage, "reports/target.json", jsonBytes(t)); e != nil {
			return e
		}
		for _, r := range s.Reports { // Clone slices so verification inputs are never mutated.
			r.Chunks = append([]ReportChunk{}, r.Chunks...)
			for i := range r.Chunks {
				c := &r.Chunks[i]
				if c.State != "confirmed" {
					c.State = "uncertain"
					c.Code = "recovery_manual_review"
					c.ConfirmedAt = ""
					if c.AttemptedAt == "" {
						c.AttemptedAt = s.Created
					}
				}
			}
			if e = writeFile(stage, "reports/run-"+digest([]byte(r.RunID))+".json", jsonBytes(r)); e != nil {
				return e
			}
		}
		if e = writeFile(stage, "RESTORED_SNAPSHOT.json", jsonBytes(s)); e != nil {
			return e
		}
		if e = writeFile(stage, "RECOVERY_STATE.json", jsonBytes(RecoveryState{1, digest(jsonBytes(s)), s.ManifestSHA, len(s.Events) + len(s.Ingress)})); e != nil {
			return e
		}
		return writeFile(stage, "RECOVERY_BLOCK.json", jsonBytes(map[string]any{"schema": 1, "source_manifest_sha256": s.ManifestSHA, "snapshot_created_at": s.Created, "delivery_enabled": false, "requires": "verify source/private target, latest history and securely restore credentials; separate authorization before live activation", "consumer_ready": false, "history_content_restored": false}))
	})
}
