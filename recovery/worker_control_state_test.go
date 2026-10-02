package main

import (
	"database/sql"
	"encoding/json"
	"math"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func workerControlFixture() Snapshot {
	s := stateFixture()
	s.WorkerControl = &WorkerControlState{Version: 1, ApplicationID: s.Operation.BotID, GuildID: s.Operation.GuildID, OwnerID: s.Operation.OwnerID, UnresolvedRequests: []string{"in1"}, RecoveryPending: []string{"in2"}, Cancellations: []WorkerCancellationMetadata{{"in1", "cancel_requested", 1, 0}, {"in2", "acknowledged", 2, 3}}}
	return s
}

func TestWorkerControlRestoreInertRoundTrip(t *testing.T) {
	s := workerControlFixture()
	root := filepath.Join(privateTemp(t), "restored")
	if err := restoreState(s, root, -1); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "bridge", "bridge.sqlite3")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	var n int
	if err = db.QueryRow(`SELECT count(*) FROM sqlite_master WHERE name IN('worker_runtime','worker_incarnations','worker_bindings','worker_cancellations','processing','consumer_claims')`).Scan(&n); err != nil || n != 0 {
		t.Fatal("runnable worker authority recreated", n, err)
	}
	if err = db.QueryRow(`SELECT count(*) FROM inbound WHERE state!='blocked' OR envelope!='{}' OR claim IS NOT NULL OR lease_until IS NOT NULL`).Scan(&n); err != nil || n != 0 {
		t.Fatal("historical request resumed", n, err)
	}
	var state string
	if err = db.QueryRow(`SELECT state FROM recovery_worker_cancellation_fences WHERE inbound_id='in1'`).Scan(&state); err != nil || state != "cancel_requested" {
		t.Fatal("pending cancellation converted to stopped", state, err)
	}
	if _, err = db.Exec(`CREATE TABLE ingress_validation(id TEXT,platform TEXT,event_id TEXT,state TEXT,created REAL)`); err != nil {
		t.Fatal(err)
	}
	db.Close()
	second := Snapshot{Schema: 1, Created: s.Created, ManifestSHA: s.ManifestSHA, Operation: s.Operation}
	if err = snapshotDB(path, &second); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(second.WorkerControl, s.WorkerControl) {
		t.Fatal("worker markers lost on rebackup", second.WorkerControl)
	}
	counts := recoveryMetadataCounts(second)
	if counts["unresolved_worker_requests"] != 1 || counts["worker_recovery_pending"] != 1 || counts["worker_cancellation_fences"] != 2 || counts["pending_worker_cancellations"] != 1 {
		t.Fatal(counts)
	}
}

func TestWorkerControlAndRichFollowupSnapshotExcludesAuthorityAndBodies(t *testing.T) {
	s := stateFixture()
	root := filepath.Join(privateTemp(t), "source")
	if err := restoreState(s, root, -1); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "bridge", "bridge.sqlite3")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	// Actual table shapes, synthetic values only. NULL inbound claim and stale
	// observation cannot erase an immutable execution binding.
	_, err = db.Exec(`CREATE TABLE ingress_validation(id TEXT,platform TEXT,event_id TEXT,state TEXT,created REAL);
 CREATE TABLE worker_runtime(worker TEXT,incarnation TEXT,controller TEXT,state TEXT,observed REAL,lease_until REAL,evidence TEXT);
 CREATE TABLE worker_incarnations(worker TEXT,incarnation TEXT,controller TEXT,previous TEXT,evidence TEXT,created REAL);
 CREATE TABLE worker_bindings(inbound_id TEXT,claim TEXT,worker TEXT,incarnation TEXT,controller TEXT,bound REAL,evidence TEXT);
 CREATE TABLE worker_cancellations(inbound_id TEXT,claim TEXT,revision TEXT,worker TEXT,incarnation TEXT,controller TEXT,state TEXT,requested REAL,acknowledged REAL,evidence TEXT);
 INSERT INTO worker_runtime VALUES('PRIVATE_WORKER','PRIVATE_TURN','PRIVATE_CONTROLLER','running',1,2,'PRIVATE_EVIDENCE');
 INSERT INTO worker_bindings VALUES('in1','PRIVATE_CLAIM','PRIVATE_WORKER','PRIVATE_TURN','PRIVATE_CONTROLLER',1,'PRIVATE_BINDING_EVIDENCE');
 INSERT INTO worker_runtime VALUES('PRIVATE_STOPPED','PRIVATE_STOPPED_TURN','PRIVATE_CONTROLLER','completed',1,2,'PRIVATE_STOPPED_EVIDENCE');
 INSERT INTO worker_bindings VALUES('in2','PRIVATE_OLD_CLAIM','PRIVATE_STOPPED','PRIVATE_STOPPED_TURN','PRIVATE_CONTROLLER',1,'PRIVATE_EVIDENCE');
 INSERT INTO worker_cancellations VALUES('in1','PRIVATE_CLAIM','PRIVATE_REVISION','PRIVATE_WORKER','PRIVATE_TURN','PRIVATE_CONTROLLER','cancel_requested',1,0,'PRIVATE_EVIDENCE');
 UPDATE inbound SET state='worker_recovery_pending',envelope='{"text":"PRIVATE_PROMPT"}' WHERE id='in2';
 CREATE TABLE reply_followups(reply_id TEXT,key TEXT,text TEXT,start_idx INTEGER,chunk_count INTEGER);
 CREATE TABLE reply_followup_outputs(reply_id TEXT,idx INTEGER,payload TEXT);
 CREATE TABLE reply_output_receipts(reply_id TEXT,idx INTEGER,receipt TEXT);
 INSERT INTO reply_followups VALUES('reply1','PRIVATE_FOLLOWUP_KEY','PRIVATE_BODY',1,1);
 INSERT INTO reply_followup_outputs VALUES('reply1',1,'{"filename":"PRIVATE_NAME","bytes":"PRIVATE_BYTES","embeds":"PRIVATE_EMBEDS"}');
 UPDATE chunks SET state='sent',message_id='8002',text='PRIVATE_BODY' WHERE reply_id='reply1' AND idx=1;
 INSERT INTO reply_output_receipts VALUES('reply1',1,'[{"id":"8003","size":42,"filename":"PRIVATE_NAME","description":"PRIVATE_DESCRIPTION","url":"https://example.invalid/PRIVATE_TOKEN"}]');`)
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
	snap := Snapshot{Schema: 1, Created: s.Created, ManifestSHA: s.ManifestSHA, Operation: s.Operation}
	if err = snapshotDB(path, &snap); err != nil {
		t.Fatal(err)
	}
	if err = validateSnapshot(snap); err != nil {
		t.Fatal(err)
	}
	if snap.WorkerControl == nil || !reflect.DeepEqual(snap.WorkerControl.UnresolvedRequests, []string{"in1"}) || !reflect.DeepEqual(snap.WorkerControl.RecoveryPending, []string{"in2"}) || len(snap.WorkerControl.Cancellations) != 1 {
		t.Fatal("revoked execution/cancellation lost", snap.WorkerControl)
	}
	if snap.Phase3 == nil || len(snap.Phase3.RichReceipts) != 1 || snap.Phase3.RichReceipts[0].Index != 1 || snap.Phase3.RichReceipts[0].Files[0] != (AttachmentMetadata{"8003", 42}) {
		t.Fatal("followup receipt lost", snap.Phase3)
	}
	raw, _ := json.Marshal(snap)
	if strings.Contains(string(raw), "PRIVATE_") || strings.Contains(string(raw), "example.invalid") {
		t.Fatal("authority or body exported")
	}
	dest := filepath.Join(privateTemp(t), "restored")
	if err = restoreState(snap, dest, -1); err != nil {
		t.Fatal(err)
	}
	restored, err := sql.Open("sqlite", filepath.Join(dest, "bridge", "bridge.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	var n int
	if err = restored.QueryRow(`SELECT count(*) FROM sqlite_master WHERE name IN('worker_runtime','worker_incarnations','worker_bindings','worker_cancellations','reply_followups','reply_followup_outputs','reply_output_receipts')`).Scan(&n); err != nil || n != 0 {
		t.Fatal("active worker/followup restored", n, err)
	}
}

func TestWorkerControlValidationAndBackwardCompatibility(t *testing.T) {
	for _, change := range []func(*Snapshot){
		func(s *Snapshot) { s.WorkerControl.Version = 2 },
		func(s *Snapshot) { s.WorkerControl.OwnerID = "999" },
		func(s *Snapshot) { s.WorkerControl.GuildID = "999" },
		func(s *Snapshot) { s.WorkerControl.ApplicationID = "999" },
		func(s *Snapshot) { s.WorkerControl.UnresolvedRequests = []string{"missing"} },
		func(s *Snapshot) { s.WorkerControl.UnresolvedRequests = []string{"in1", "in1"} },
		func(s *Snapshot) { s.WorkerControl.RecoveryPending = []string{"in3"} },
		func(s *Snapshot) { s.WorkerControl.Cancellations[0].State = "running" },
		func(s *Snapshot) { s.WorkerControl.Cancellations[0].Acknowledged = 1 },
		func(s *Snapshot) { s.WorkerControl.Cancellations[1].Acknowledged = 0 },
		func(s *Snapshot) { s.WorkerControl.Cancellations[1].Acknowledged = 1 },
		func(s *Snapshot) { s.WorkerControl.Cancellations[0].Requested = math.Inf(1) },
		func(s *Snapshot) { s.WorkerControl.Cancellations[0].InboundID = "in3" },
		func(s *Snapshot) {
			s.WorkerControl.Cancellations = append(s.WorkerControl.Cancellations, s.WorkerControl.Cancellations[0])
		},
	} {
		s := workerControlFixture()
		change(&s)
		if validateSnapshot(s) == nil {
			t.Fatal("malformed worker metadata accepted")
		}
	}
	if err := validateSnapshot(stateFixture()); err != nil {
		t.Fatal("legacy snapshot rejected", err)
	}
	raw := jsonBytes(workerControlFixture())
	raw = []byte(strings.Replace(string(raw), `"worker_control": {`, `"worker_control": {"claim":"PRIVATE_TOKEN",`, 1))
	if !strings.Contains(string(raw), "PRIVATE_TOKEN") {
		t.Fatal("injection failed")
	}
	var decoded Snapshot
	if strict(raw, &decoded) == nil {
		t.Fatal("unknown authority field accepted")
	}
}
