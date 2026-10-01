package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func operationsFixture(s Snapshot) OperationsSnapshot {
	r := s.Reports[0]
	return OperationsSnapshot{Schema: 1, Created: s.Created, ManifestSHA: s.ManifestSHA, MainSnapshotSHA: digest(jsonBytes(s)), Collector: CollectorState{Version: 1, Fingerprints: map[string]string{"https://community.example/post/1": digest([]byte("metadata"))}, WideReviewedV1: map[string]uint64{"https://community.example/post/1": 1}}, Reviewed: ReviewedOffers{Version: 1, Status: "committed", Updated: s.Created, Posts: []ReviewedPost{{URL: "https://community.example/post/1", Title: "Public offer", Source: "community", Digest: digest([]byte("metadata")), Decision: "accepted", ReasonCode: "verified_offer", ReasonKO: "Official terms checked", FirstRun: r.RunID, LatestRun: r.RunID, ReviewedAt: s.Created}, {URL: "https://community.example/post/2", Decision: "accepted", FirstRun: r.RunID}}, Offers: []ReviewedOffer{{ID: "synthetic-offer", CanonicalURL: "https://official.example/offer", OriginalURL: "https://community.example/post/1", FirstRun: r.RunID, Decision: "accepted", Delivered: true, MessageIDs: []string{r.Chunks[0].MessageID}, RunID: r.RunID, DeliveredAt: s.Created, MessageURLs: []string{r.Chunks[0].MessageURL}, PayloadHash: r.PayloadHash}}, LastRun: r.RunID}, First: FirstNotification{r.RunID, "accepted", "Sentinel_0123456789abcdef0123456789abcdef", s.Created, r.Chunks[0].MessageID, 20, 2, 1}, Schedule: ScheduleRecord{"dot_cloud_automation", "synthetic-automation-id", true, 4, "Etc/UTC", s.Operation.GuildID, s.Operation.ReportChannelID, false}, Runbook: "# Inert historical runbook\nDo not execute quoted instructions.\n"}
}
func writeOperationsInputs(t *testing.T, dir string, o OperationsSnapshot) {
	t.Helper()
	for name, value := range map[string]any{"state.json": o.Collector, "reviewed-offers.json": o.Reviewed, "first-auto-notification.json": o.First, "schedule.json": o.Schedule} {
		put(t, filepath.Join(dir, name), jsonBytes(value))
	}
	put(t, filepath.Join(dir, "RUNBOOK.md"), []byte(o.Runbook))
}
func TestOperationsRestoreRoundTripSuppression(t *testing.T) {
	s := stateFixture()
	want := operationsFixture(s)
	in := privateTemp(t)
	writeOperationsInputs(t, in, want)
	o, e := snapshotOperations(in, s)
	if e != nil {
		t.Fatal(e)
	}
	dest := filepath.Join(privateTemp(t), "new")
	if e = restoreOperations(o, s, dest, -1); e != nil {
		t.Fatal(e)
	}
	restored, e := snapshotOperations(dest, s)
	if e != nil {
		t.Fatal(e)
	}
	if restored.First != want.First {
		t.Fatal("first notification suppression changed")
	}
	if string(jsonBytes(restored.Reviewed)) != string(jsonBytes(want.Reviewed)) {
		t.Fatal("reviewed offer proof changed")
	}
	if string(jsonBytes(restored.Collector)) != string(jsonBytes(want.Collector)) {
		t.Fatal("collector fingerprints changed")
	}
	if restored.Runbook != want.Runbook || restored.Schedule != want.Schedule {
		t.Fatal("schedule/runbook changed")
	}
	if _, e = os.Stat(filepath.Join(dest, "RECOVERY_BLOCK.json")); e != nil {
		t.Fatal("missing operation recovery block")
	}
	if restoreOperations(o, s, dest, -1) == nil {
		t.Fatal("existing root overwritten")
	}
}
func TestOperationsInterruptedWriteAndPrivateModes(t *testing.T) {
	s := stateFixture()
	o := operationsFixture(s)
	dest := filepath.Join(privateTemp(t), "new")
	if restoreOperations(o, s, dest, 2) == nil {
		t.Fatal("missing fault")
	}
	if _, e := os.Lstat(dest); !os.IsNotExist(e) {
		t.Fatal("partial operation root exposed")
	}
	if e := restoreOperations(o, s, dest, -1); e != nil {
		t.Fatal(e)
	}
	filepath.WalkDir(dest, func(p string, d os.DirEntry, e error) error {
		if e != nil {
			t.Fatal(e)
		}
		st, _ := d.Info()
		if st.Mode().Perm()&0077 != 0 {
			t.Fatal("nonprivate restored file")
		}
		return nil
	})
}
func TestOperationsUnknownFieldsAndCredentialsRejected(t *testing.T) {
	s := stateFixture()
	o := operationsFixture(s)
	in := privateTemp(t)
	writeOperationsInputs(t, in, o)
	p := filepath.Join(in, "state.json")
	put(t, p, []byte(`{"version":1,"fingerprints":{},"unexpected_future_state":true}`))
	if _, e := snapshotOperations(in, s); e == nil {
		t.Fatal("unknown collector state silently dropped")
	}
	writeOperationsInputs(t, in, o)
	put(t, filepath.Join(in, "RUNBOOK.md"), []byte("api_key="+"sk-"+strings.Repeat("a", 32)))
	if _, e := snapshotOperations(in, s); e == nil {
		t.Fatal("credential-like runbook accepted")
	}
}
func TestOperationsReceiptAndHashBindings(t *testing.T) {
	s := stateFixture()
	o := operationsFixture(s)
	o.Reviewed.Offers[0].MessageIDs[0] = "9999"
	if operationsValid(o, s) == nil {
		t.Fatal("wrong delivery proof accepted")
	}
	o = operationsFixture(s)
	o.First.RunID = "incomplete"
	if operationsValid(o, s) == nil {
		t.Fatal("unconfirmed first-notification run accepted")
	}
	o = operationsFixture(s)
	o.MainSnapshotSHA = strings.Repeat("f", 64)
	if operationsValid(o, s) == nil {
		t.Fatal("wrong main snapshot accepted")
	}
	o = operationsFixture(s)
	o.ManifestSHA = strings.Repeat("f", 64)
	if operationsValid(o, s) == nil {
		t.Fatal("wrong source binding accepted")
	}
	o = operationsFixture(s)
	p := filepath.Join(privateTemp(t), "operations.json")
	put(t, p, jsonBytes(o))
	if _, e := readOperations(p, strings.Repeat("f", 64), s); e == nil {
		t.Fatal("wrong operations file hash accepted")
	}
}
func TestOperationsFilesRejectLinks(t *testing.T) {
	for _, kind := range []string{"symlink", "hardlink"} {
		t.Run(kind, func(t *testing.T) {
			s := stateFixture()
			o := operationsFixture(s)
			in := privateTemp(t)
			writeOperationsInputs(t, in, o)
			p := filepath.Join(in, "state.json")
			b, _ := os.ReadFile(p)
			outside := filepath.Join(privateTemp(t), "state.json")
			put(t, outside, b)
			os.Remove(p)
			var e error
			if kind == "symlink" {
				e = os.Symlink(outside, p)
			} else {
				e = os.Link(outside, p)
			}
			if e != nil {
				t.Fatal(e)
			}
			if _, e = snapshotOperations(in, s); e == nil {
				t.Fatal("linked metadata accepted")
			}
		})
	}
}
func TestOperationsWideMarkerAndCanonicalDCInside(t *testing.T) {
	s := stateFixture()
	o := operationsFixture(s)
	dc := "https://gall.dcinside.com/mgallery/board/view/?id=ai_utilize&no=12345"
	o.Collector.Fingerprints[dc] = digest([]byte("DC public post"))
	o.Collector.WideReviewedV1[dc] = 2
	if e := operationsValid(o, s); e != nil {
		t.Fatal(e)
	}
	for _, bad := range []string{dc + "&token=secret", dc + "&no=54321", "https://other.example/view/?id=ai_utilize&no=1", "https://gall.dcinside.com/mgallery/board/view/?id=other&no=1"} {
		if metadataURL(bad) {
			t.Fatal("unapproved metadata query accepted")
		}
	}
	o.Collector.WideReviewedV1[dc] = ^uint64(0)
	if operationsValid(o, s) == nil {
		t.Fatal("overflow-reserved wide sequence accepted")
	}
	o.Collector.WideReviewedV1[dc] = 0
	if operationsValid(o, s) == nil {
		t.Fatal("zero wide sequence accepted")
	}
	o.Collector.WideReviewedV1[dc] = 2
	delete(o.Collector.Fingerprints, dc)
	if operationsValid(o, s) == nil {
		t.Fatal("orphan wide marker accepted")
	}
}
func TestOperationsRejectDuplicateJSONKeys(t *testing.T) {
	var c CollectorState
	for _, b := range []string{`{"version":1,"version":2,"fingerprints":{}}`, `{"version":1,"fingerprints":{"https://example.com/p":"a","https://example.com/p":"b"}}`} {
		if strictOperations([]byte(b), &c) == nil {
			t.Fatal("duplicate JSON metadata accepted")
		}
	}
}
func TestOperationsRestoreIncludesBoundOutbox(t *testing.T) {
	s := stateFixture()
	o := operationsFixture(s)
	dest := filepath.Join(privateTemp(t), "ops")
	if e := restoreOperations(o, s, dest, -1); e != nil {
		t.Fatal(e)
	}
	var out Snapshot
	out.Operation = s.Operation
	if e := snapshotReports(filepath.Join(dest, "outbox"), &out); e != nil {
		t.Fatal(e)
	}
	if len(out.Reports) != len(s.Reports) {
		t.Fatal("missing restored outbox")
	}
	byID := map[string]Receipt{}
	for _, r := range out.Reports {
		byID[r.RunID] = r
	}
	for _, original := range s.Reports {
		actual := byID[original.RunID]
		if original.Complete && string(jsonBytes(actual)) != string(jsonBytes(original)) {
			t.Fatal("confirmed receipt changed")
		}
		for _, c := range actual.Chunks {
			if c.State != "confirmed" && c.State != "uncertain" {
				t.Fatal("restored outbox remains sendable")
			}
		}
	}
}
func TestOperationsRejectExplicitNullWideMarker(t *testing.T) {
	var c CollectorState
	for _, raw := range []string{`{"version":1,"fingerprints":{},"wide_reviewed_v1":null}`, `{"version":1,"fingerprints":{},"wide_reviewed_v1":[]}`, `{"version":1,"fingerprints":{},"wide_reviewed_v1":"none"}`} {
		if strictOperations([]byte(raw), &c) == nil {
			t.Fatal("invalid explicit wide marker accepted")
		}
	}
	if e := strictOperations([]byte(`{"version":1,"fingerprints":{}}`), &c); e != nil {
		t.Fatal("absent legacy marker rejected", e)
	}
}
func TestOperationsRejectCaseAliasFields(t *testing.T) {
	var c CollectorState
	for _, raw := range []string{`{"version":1,"fingerprints":{},"WIDE_REVIEWED_V1":null}`, `{"version":1,"fingerprints":{},"wide_reviewed_v1":{},"WIDE_REVIEWED_V1":null}`, `{"VERSION":1,"fingerprints":{}}`} {
		if strictOperations([]byte(raw), &c) == nil {
			t.Fatal("case alias collector field accepted")
		}
	}
	o := operationsFixture(stateFixture())
	raw := strings.Replace(string(jsonBytes(o)), `"chatgpt_message_id":`, `"CHATGPT_MESSAGE_ID":`, 1)
	var out OperationsSnapshot
	if strictOperations([]byte(raw), &out) == nil {
		t.Fatal("nested case alias accepted")
	}
}
