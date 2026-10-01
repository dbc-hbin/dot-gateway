package bridge

import "encoding/json"

// SendMeasurement contains only immutable numeric/boolean measurements. In
// particular it cannot store request URLs, headers, message text, IDs, or errors.
// Ledger correlation is carried by the owning chunk/attempt, not a global latest
// diagnostics sample (which can belong to an unrelated diagnostic send).
type SendMeasurement struct {
	Seconds             float64            `json:"seconds"`
	PreflightSeconds    float64            `json:"preflight_seconds"`
	PostSeconds         float64            `json:"post_seconds"`
	Reused              bool               `json:"reused"`
	SendLockWaitSeconds float64            `json:"send_lock_wait_seconds"`
	IdentityGET         RequestDiagnostics `json:"identity_get"`
	ChannelGET          RequestDiagnostics `json:"channel_get"`
	Post                RequestDiagnostics `json:"post"`
}

func (s *Store) RecordResultMeasured(c Chunk, r SendResult, d Diagnostics) error {
	m := SendMeasurement{d.Seconds, d.PreflightSeconds, d.PostSeconds, d.Reused, d.SendLockWaitSeconds, d.IdentityGET, d.ChannelGET, d.Post}
	return s.recordResult(c, r, &m)
}

// Best-effort telemetry shares the existing result transaction. The savepoint
// isolates ordinary statement errors (for example a missing diagnostics table)
// so delivery can still commit. Transaction-wide I/O/rollback failures cannot be
// contained by a savepoint: the caller must stop, retaining the existing sending
// -> uncertain recovery contract rather than retrying an acknowledged send.
func insertSendMeasurement(db *storeConn, c Chunk, m *SendMeasurement) {
	if m == nil {
		return
	}
	raw, err := json.Marshal(m)
	if err != nil || len(raw) > 8192 {
		return
	}
	if _, err = db.Exec("SAVEPOINT send_measurement"); err != nil {
		return
	}
	_, err = db.Exec(`INSERT INTO send_measurements(reply_id,chunk_index,attempt,at,measurement) SELECT reply_id,idx,attempts,?,? FROM chunks WHERE reply_id=? AND idx=?`, epoch(), string(raw), c.ReplyID, c.Index)
	if err == nil {
		_, err = db.Exec("DELETE FROM send_measurements WHERE id NOT IN (SELECT id FROM send_measurements ORDER BY id DESC LIMIT 2048)")
	}
	if err != nil {
		_, _ = db.Exec("ROLLBACK TO send_measurement")
	}
	_, _ = db.Exec("RELEASE send_measurement")
}
