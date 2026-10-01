package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/url"
	"os"
	"reflect"
	"regexp"
	"strings"
	"time"
)

// These types are deliberately a closed metadata schema, not arbitrary file
// backup. Unknown fields fail closed rather than silently losing their meaning.
type CollectorState struct {
	Version        int               `json:"version"`
	Fingerprints   map[string]string `json:"fingerprints"`
	WideReviewedV1 map[string]uint64 `json:"wide_reviewed_v1,omitempty"`
}

// Absent wide marker is legacy-compatible; explicitly null is corrupted state
// and the actual collector rejects it. Do not collapse null into absence.
func (c *CollectorState) UnmarshalJSON(b []byte) error {
	type plain CollectorState
	var decoded plain
	if e := strict(b, &decoded); e != nil {
		return e
	}
	var raw map[string]json.RawMessage
	if e := json.Unmarshal(b, &raw); e != nil {
		return e
	}
	for key := range raw {
		if key != "version" && key != "fingerprints" && key != "wide_reviewed_v1" {
			return errors.New("unknown collector field spelling")
		}
	}
	if marker, exists := raw["wide_reviewed_v1"]; exists {
		marker = bytes.TrimSpace(marker)
		if len(marker) == 0 || marker[0] != '{' {
			return errors.New("wide review marker must be an object when present")
		}
	}
	*c = CollectorState(decoded)
	return nil
}

type ReviewedPost struct {
	URL        string `json:"url"`
	Title      string `json:"title,omitempty"`
	Source     string `json:"source,omitempty"`
	Digest     string `json:"content_digest_sha256,omitempty"`
	Decision   string `json:"decision"`
	ReasonCode string `json:"reason_code,omitempty"`
	ReasonKO   string `json:"reason_ko,omitempty"`
	FirstRun   string `json:"first_review_run"`
	LatestRun  string `json:"latest_review_run,omitempty"`
	ReviewedAt string `json:"reviewed_at,omitempty"`
}
type ReviewedOffer struct {
	ID           string   `json:"offer_id"`
	CanonicalURL string   `json:"canonical_url"`
	OriginalURL  string   `json:"original_url"`
	FirstRun     string   `json:"first_seen_run"`
	Decision     string   `json:"decision"`
	Delivered    bool     `json:"delivered"`
	MessageIDs   []string `json:"delivery_message_ids"`
	RunID        string   `json:"delivery_run_id"`
	DeliveredAt  string   `json:"delivered_at"`
	MessageURLs  []string `json:"delivery_message_urls"`
	PayloadHash  string   `json:"payload_sha256"`
}
type ReviewedOffers struct {
	Version int             `json:"schema_version"`
	Status  string          `json:"status"`
	Updated string          `json:"updated_at"`
	Posts   []ReviewedPost  `json:"reviewed_posts"`
	Offers  []ReviewedOffer `json:"offers"`
	LastRun string          `json:"last_completed_run_id"`
}
type FirstNotification struct {
	RunID            string `json:"run_id"`
	Status           string `json:"status"`
	ChatGPTMessageID string `json:"chatgpt_message_id"`
	AcceptedAt       string `json:"accepted_at"`
	DiscordMessageID string `json:"discord_message_id"`
	Scanned          int    `json:"scanned"`
	Candidates       int    `json:"candidates"`
	AcceptedOffers   int    `json:"accepted_offers"`
}
type ScheduleRecord struct {
	Scheduler        string `json:"scheduler"`
	AutomationID     string `json:"automation_id"`
	Enabled          bool   `json:"enabled"`
	IntervalHours    int    `json:"interval_hours"`
	Timezone         string `json:"timezone"`
	GuildID          string `json:"guild_id"`
	ChannelID        string `json:"channel_id"`
	MacLegacyEnabled bool   `json:"mac_legacy_job_enabled"`
}
type OperationsSnapshot struct {
	Schema          int               `json:"schema"`
	Created         string            `json:"created_at"`
	ManifestSHA     string            `json:"source_manifest_sha256"`
	MainSnapshotSHA string            `json:"main_snapshot_sha256"`
	Collector       CollectorState    `json:"collector_state"`
	Reviewed        ReviewedOffers    `json:"reviewed_offers"`
	First           FirstNotification `json:"first_auto_notification"`
	Schedule        ScheduleRecord    `json:"schedule"`
	Runbook         string            `json:"runbook_inert_text"`
}

var sentinelPattern = regexp.MustCompile(`^Sentinel_[a-f0-9]{32}$`)
var secretContentPatterns = []*regexp.Regexp{
	regexp.MustCompile(`\b(?:gh[pousr]_|github_pat_)[A-Za-z0-9_]{20,}`),
	regexp.MustCompile(`\bsk-(?:proj-|svcacct-)?[A-Za-z0-9_-]{16,}`),
	regexp.MustCompile(`\bAIza[A-Za-z0-9_-]{20,}`),
	regexp.MustCompile(`\b[A-Za-z0-9_-]{24,}\.[A-Za-z0-9_-]{6,}\.[A-Za-z0-9_-]{27,}`),
	regexp.MustCompile(`-----BEGIN (?:RSA |EC |OPENSSH |ENCRYPTED )?PRIVATE KEY-----`),
	regexp.MustCompile(`(?i)(?:api[ _-]*key|access[ _-]*token|bot[ _-]*token|auth[ _-]*token|password|secret[ _-]*(?:key|token))["']?[\t ]*[:=][\t ]*["']?[A-Za-z0-9_./+~=-]{16,}`),
}

func safeMetadataText(s string, max int) bool {
	if len(s) > max {
		return false
	}
	for _, r := range s {
		if r < 32 && r != '\n' && r != '\r' && r != '\t' {
			return false
		}
	}
	for _, p := range secretContentPatterns {
		if p.MatchString(s) {
			return false
		}
	}
	return true
}
func metadataURL(s string) bool {
	u, e := url.Parse(s)
	if e != nil || len(s) > 2048 || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.Fragment != "" || u.Opaque != "" || !safeMetadataText(s, 2048) {
		return false
	}
	if u.RawQuery == "" {
		return true
	}
	// One reviewed public article locale selector; never a generic query exception.
	if u.Host == "support.google.com" && u.RawPath == "" && regexp.MustCompile(`^/googleone/answer/[1-9][0-9]{0,19}$`).MatchString(u.Path) {
		q, err := url.ParseQuery(u.RawQuery)
		return err == nil && len(q) == 1 && len(q["hl"]) == 1 && q.Get("hl") == "ko" && u.RawQuery == "hl=ko" && !strings.Contains(s, "#")
	}
	if u.Host != "gall.dcinside.com" || (u.Path != "/mgallery/board/view/" && u.Path != "/mgallery/board/view") {
		return false
	}
	q, e := url.ParseQuery(u.RawQuery)
	return e == nil && len(q) == 2 && len(q["id"]) == 1 && q.Get("id") == "ai_utilize" && len(q["no"]) == 1 && regexp.MustCompile(`^[0-9]+$`).MatchString(q.Get("no"))
}

// Duplicate keys are ambiguous and must not silently replace suppression data.
func strictOperations(b []byte, out any) error {
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	var parse func(int) error
	parse = func(depth int) error {
		if depth > 64 {
			return errors.New("operation JSON too deep")
		}
		token, e := d.Token()
		if e != nil {
			return e
		}
		delim, ok := token.(json.Delim)
		if !ok {
			return nil
		}
		switch delim {
		case '{':
			seen := map[string]bool{}
			for d.More() {
				key, e := d.Token()
				if e != nil {
					return e
				}
				k, ok := key.(string)
				if !ok || seen[k] {
					return errors.New("duplicate operation JSON field")
				}
				seen[k] = true
				if e = parse(depth + 1); e != nil {
					return e
				}
			}
		case '[':
			for d.More() {
				if e = parse(depth + 1); e != nil {
					return e
				}
			}
		default:
			return errors.New("invalid operation JSON")
		}
		_, e = d.Token()
		return e
	}
	if e := parse(0); e != nil {
		return e
	}
	if _, e := d.Token(); e != io.EOF {
		return errors.New("trailing operation JSON")
	}
	if e := exactOperationFields(b, reflect.TypeOf(out).Elem()); e != nil {
		return e
	}
	return strict(b, out)
}

// encoding/json otherwise accepts case-insensitive struct field aliases. Only
// exact documented JSON keys are allowed; map keys remain schema-validated URLs.
func exactOperationFields(b []byte, t reflect.Type) error {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	switch t.Kind() {
	case reflect.Struct:
		var object map[string]json.RawMessage
		if e := json.Unmarshal(b, &object); e != nil {
			return e
		}
		fields := map[string]reflect.Type{}
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			name := strings.Split(f.Tag.Get("json"), ",")[0]
			if name != "" && name != "-" {
				fields[name] = f.Type
			}
		}
		for key, raw := range object {
			field, ok := fields[key]
			if !ok {
				return errors.New("unknown or noncanonical operation JSON field")
			}
			if e := exactOperationFields(raw, field); e != nil {
				return e
			}
		}
	case reflect.Slice, reflect.Array:
		var values []json.RawMessage
		if e := json.Unmarshal(b, &values); e != nil {
			return e
		}
		for _, raw := range values {
			if e := exactOperationFields(raw, t.Elem()); e != nil {
				return e
			}
		}
	case reflect.Map:
		var values map[string]json.RawMessage
		if e := json.Unmarshal(b, &values); e != nil {
			return e
		}
		for _, raw := range values {
			if e := exactOperationFields(raw, t.Elem()); e != nil {
				return e
			}
		}
	}
	return nil
}
func operationsValid(o OperationsSnapshot, s Snapshot) error {
	if e := validateSnapshot(s); e != nil {
		return e
	}
	if o.Schema != 1 || !stampOK(o.Created) || o.ManifestSHA != s.ManifestSHA || o.MainSnapshotSHA != digest(jsonBytes(s)) {
		return errors.New("operations/main snapshot binding mismatch")
	}
	if o.Collector.Version != 1 || o.Collector.Fingerprints == nil || len(o.Collector.Fingerprints) > 100000 {
		return errors.New("invalid collector metadata")
	}
	for k, v := range o.Collector.Fingerprints {
		if !metadataURL(k) || !hashPattern.MatchString(v) {
			return errors.New("invalid collector URL fingerprint")
		}
	}
	if len(o.Collector.WideReviewedV1) > 2500 {
		return errors.New("wide review marker limit exceeded")
	}
	for key, sequence := range o.Collector.WideReviewedV1 {
		if _, ok := o.Collector.Fingerprints[key]; !ok || sequence <= 0 || sequence > ^uint64(0)-96 {
			return errors.New("wide review marker must reference an existing fingerprint with positive sequence and collector overflow reserve")
		}
	}
	r := o.Reviewed
	if r.Version != 1 || r.Status != "committed" || !stampOK(r.Updated) || !validID(r.LastRun) || len(r.Posts) > 100000 || len(r.Offers) > 10000 {
		return errors.New("review index not committed or invalid")
	}
	seen := map[string]bool{}
	for _, p := range r.Posts {
		if !metadataURL(p.URL) || seen[p.URL] || p.Decision != "accepted" && p.Decision != "excluded" || !validID(p.FirstRun) || p.LatestRun != "" && !validID(p.LatestRun) || p.ReviewedAt != "" && !stampOK(p.ReviewedAt) || p.Digest != "" && !hashPattern.MatchString(p.Digest) || p.ReasonCode != "" && !validID(p.ReasonCode) || !safeMetadataText(p.Title, 2048) || !safeMetadataText(p.Source, 256) || !safeMetadataText(p.ReasonKO, 4096) {
			return errors.New("invalid or duplicate reviewed post metadata")
		}
		seen[p.URL] = true
	}
	receipts := map[string]Receipt{}
	messages := map[string]bool{}
	for _, r := range s.Reports {
		receipts[r.RunID] = r
		for _, c := range r.Chunks {
			if r.Complete && c.State == "confirmed" {
				messages[c.MessageID] = true
			}
		}
	}
	offerIDs := map[string]bool{}
	for _, offer := range r.Offers {
		if !validID(offer.ID) || offerIDs[offer.ID] || !metadataURL(offer.CanonicalURL) || !metadataURL(offer.OriginalURL) || !validID(offer.FirstRun) || offer.Decision != "accepted" || !offer.Delivered || !stampOK(offer.DeliveredAt) || !hashPattern.MatchString(offer.PayloadHash) || len(offer.MessageIDs) == 0 || len(offer.MessageIDs) != len(offer.MessageURLs) {
			return errors.New("invalid delivered-offer proof")
		}
		offerIDs[offer.ID] = true
		receipt, ok := receipts[offer.RunID]
		if !ok || !receipt.Complete || receipt.PayloadHash != offer.PayloadHash || len(receipt.Chunks) != len(offer.MessageIDs) {
			return errors.New("offer/main outbox binding mismatch")
		}
		for i, id := range offer.MessageIDs {
			if !snowflakeID(id) || receipt.Chunks[i].MessageID != id || receipt.Chunks[i].MessageURL != offer.MessageURLs[i] {
				return errors.New("offer receipt mismatch")
			}
		}
	}
	f := o.First
	if !validID(f.RunID) || f.Status != "accepted" || !sentinelPattern.MatchString(f.ChatGPTMessageID) || !stampOK(f.AcceptedAt) || !snowflakeID(f.DiscordMessageID) || !messages[f.DiscordMessageID] || f.Scanned < 0 || f.Candidates < 0 || f.Candidates > f.Scanned || f.AcceptedOffers < 0 || f.AcceptedOffers > f.Candidates {
		return errors.New("invalid first-notification suppression proof")
	}
	firstReceipt, ok := receipts[f.RunID]
	if !ok || !firstReceipt.Complete {
		return errors.New("first notification run has no completed receipt")
	}
	firstMessageFound := false
	for _, chunk := range firstReceipt.Chunks {
		if chunk.State == "confirmed" && chunk.MessageID == f.DiscordMessageID {
			firstMessageFound = true
		}
	}
	if !firstMessageFound {
		return errors.New("first notification Discord receipt belongs to another run")
	}
	c := o.Schedule
	if c.Scheduler != "dot_cloud_automation" || !validID(c.AutomationID) || c.IntervalHours < 1 || c.IntervalHours > 168 || c.MacLegacyEnabled || c.GuildID != s.Operation.GuildID || c.ChannelID != s.Operation.ReportChannelID {
		return errors.New("invalid schedule metadata")
	}
	if _, e := time.LoadLocation(c.Timezone); e != nil {
		return errors.New("invalid schedule timezone")
	}
	if o.Runbook == "" || !safeMetadataText(o.Runbook, 128<<10) {
		return errors.New("invalid or credential-like runbook text")
	}
	return nil
}

var operationInputNames = []string{"state.json", "reviewed-offers.json", "first-auto-notification.json", "schedule.json", "RUNBOOK.md"}

func snapshotOperations(rootPath string, s Snapshot) (OperationsSnapshot, error) {
	o := OperationsSnapshot{Schema: 1, Created: time.Now().UTC().Format(time.RFC3339Nano), ManifestSHA: s.ManifestSHA, MainSnapshotSHA: digest(jsonBytes(s))}
	d, e := openDir(rootPath, true)
	if e != nil {
		return o, e
	}
	defer d.Close()
	r, e := os.OpenRoot("/proc/self/fd/" + itoa(int(d.Fd())))
	if e != nil {
		return o, e
	}
	defer r.Close()
	raw := map[string][]byte{}
	for _, name := range operationInputNames {
		b, e := readRegular(r, name, true, 16<<20)
		if e != nil {
			return o, e
		}
		raw[name] = b
	}
	for name, dest := range map[string]any{"state.json": &o.Collector, "reviewed-offers.json": &o.Reviewed, "first-auto-notification.json": &o.First, "schedule.json": &o.Schedule} {
		if strictOperations(raw[name], dest) != nil {
			return o, errors.New("unknown fields or invalid operation metadata schema")
		}
	}
	o.Runbook = string(raw["RUNBOOK.md"])
	// A run may atomically replace these files separately. Refuse if any changed
	// during collection; metadata/receipt bindings must also agree.
	for _, name := range operationInputNames {
		b, e := readRegular(r, name, true, 16<<20)
		if e != nil || !bytes.Equal(raw[name], b) {
			return o, errors.New("operation metadata changed during snapshot; wait for committed run")
		}
	}
	return o, operationsValid(o, s)
}
func readOperations(file, pin string, s Snapshot) (OperationsSnapshot, error) {
	var o OperationsSnapshot
	b, e := readFile(file, true, 32<<20)
	if e != nil {
		return o, e
	}
	if !hashPattern.MatchString(pin) || digest(b) != pin {
		return o, errors.New("operations snapshot hash mismatch")
	}
	if strictOperations(b, &o) != nil {
		return o, errors.New("invalid operations snapshot schema")
	}
	return o, operationsValid(o, s)
}
func restoreOperations(o OperationsSnapshot, s Snapshot, dest string, failAfter int) error {
	if e := operationsValid(o, s); e != nil {
		return e
	}
	return atomicDir(dest, func(stage string) error {
		values := map[string]any{"state.json": o.Collector, "reviewed-offers.json": o.Reviewed, "first-auto-notification.json": o.First, "schedule.json": o.Schedule}
		for i, name := range operationInputNames {
			if i == failAfter {
				return errors.New("injected interrupted operations write")
			}
			b := []byte(o.Runbook)
			if name != "RUNBOOK.md" {
				b = jsonBytes(values[name])
			}
			if e := writeFile(stage, name, b); e != nil {
				return e
			}
		}
		// Keep the scanner's conventional outbox sibling alongside its indices.
		// It is the same receipt set bound by the main snapshot; never a fresh ledger.
		if e := writeFile(stage, "outbox/target.json", jsonBytes(Target{s.Operation.BotID, s.Operation.GuildID, s.Operation.ReportChannelID})); e != nil {
			return e
		}
		for _, receipt := range s.Reports {
			receipt.Chunks = append([]ReportChunk{}, receipt.Chunks...)
			for i := range receipt.Chunks {
				chunk := &receipt.Chunks[i]
				if chunk.State != "confirmed" {
					chunk.State = "uncertain"
					chunk.Code = "recovery_manual_review"
					chunk.ConfirmedAt = ""
					if chunk.AttemptedAt == "" {
						chunk.AttemptedAt = s.Created
					}
				}
			}
			if e := writeFile(stage, "outbox/run-"+digest([]byte(receipt.RunID))+".json", jsonBytes(receipt)); e != nil {
				return e
			}
		}
		// Original schedule.enabled is a historical fact, not scheduler registration.
		if e := writeFile(stage, "RECOVERY_BLOCK.json", jsonBytes(map[string]any{"schema": 1, "operations_snapshot_sha256": digest(jsonBytes(o)), "source_manifest_sha256": o.ManifestSHA, "main_snapshot_sha256": o.MainSnapshotSHA, "delivery_enabled": false, "scheduler_registered": false, "history_is_inert": true, "first_notification_already_accepted": true})); e != nil {
			return e
		}
		return writeFile(stage, "RESTORE_NOTES.txt", []byte("Private operational state only. No scheduler was created and no notification was sent.\nstate.json retains collector URL fingerprints; reviewed-offers.json retains delivered receipts.\noutbox/ retains the main snapshot receipt set; never replace it with an empty ledger.\nfirst-auto-notification.json records the already accepted one-time ChatGPT report and must not be reset.\nRUNBOOK.md is inert text with historical paths: review/update paths before authorized activation.\nReconcile newest source history, offer receipts and existing scheduler before activating. Never register duplicate jobs.\n"))
	})
}
