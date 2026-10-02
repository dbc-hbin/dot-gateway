package reporting

import (
	"bytes"
	"context"
	"errors"
	"os"
	"regexp"
	"time"
)

var secretPatterns = []*regexp.Regexp{
	regexp.MustCompile(`\bsk-(?:proj-|svcacct-)?[A-Za-z0-9_-]{16,}`),
	regexp.MustCompile(`\bAIza[A-Za-z0-9_-]{20,}`),
	regexp.MustCompile(`\b(?:gh[pousr]_|github_pat_)[A-Za-z0-9_]{20,}`),
	regexp.MustCompile(`\b[A-Za-z0-9_-]{24,}\.[A-Za-z0-9_-]{6,}\.[A-Za-z0-9_-]{27,}`),
	regexp.MustCompile(`-----BEGIN (?:RSA |EC |OPENSSH |ENCRYPTED )?PRIVATE KEY-----`),
	regexp.MustCompile(`(?i)(?:api[ _-]*(?:key|token|키)|access[ _-]*token|secret[ _-]*(?:key|token)|bot[ _-]*token|auth[ _-]*token|key|token|키|토큰)["']?[\t ]*[:：=][\t ]*["']?[A-Za-z0-9_./+~=-]{16,}`),
}

func rejectCredentialContent(report []byte) error {
	for _, p := range secretPatterns {
		if p.Match(report) {
			return errors.New("credential_like_report_content_rejected")
		}
	}
	return nil
}

type ChunkReceipt struct {
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
	Version     int            `json:"version"`
	RunID       string         `json:"run_id"`
	Target      Target         `json:"target"`
	PayloadHash string         `json:"payload_sha256"`
	CreatedAt   string         `json:"created_at"`
	Chunks      []ChunkReceipt `json:"chunks"`
	Complete    bool           `json:"complete"`
}

func stamp() string            { return time.Now().UTC().Format(time.RFC3339Nano) }
func validStamp(s string) bool { _, err := time.Parse(time.RFC3339Nano, s); return err == nil }
func validateReceipt(r Receipt, t Target, runID, payloadHash string, chunks []string) error {
	if r.Version != 1 || r.RunID != runID || r.Target != t || r.PayloadHash != payloadHash || len(r.Chunks) != len(chunks) || !validStamp(r.CreatedAt) {
		return errors.New("immutable_run_binding_mismatch")
	}
	allConfirmed := true
	seenUnconfirmed := false
	for i, c := range r.Chunks {
		if c.Index != i || c.ContentHash != hashBytes([]byte(chunks[i])) || c.Nonce != nonce(t, runID, payloadHash, i) {
			return errors.New("immutable_chunk_binding_mismatch")
		}
		if c.State != "confirmed" {
			allConfirmed = false
			seenUnconfirmed = true
		} else if seenUnconfirmed {
			return errors.New("invalid_ledger_order")
		}
		switch c.State {
		case "prepared":
			if c.AttemptedAt != "" || c.MessageID != "" || c.ConfirmedAt != "" {
				return errors.New("invalid_prepared_ledger")
			}
		case "attempted", "failed":
			if !validStamp(c.AttemptedAt) || c.MessageID != "" || c.ConfirmedAt != "" {
				return errors.New("invalid_attempt_ledger")
			}
		case "uncertain":
			if !validStamp(c.AttemptedAt) || c.ConfirmedAt != "" || (c.MessageID != "" && !snowflake(c.MessageID)) {
				return errors.New("invalid_uncertain_ledger")
			}
		case "acknowledged":
			if !validStamp(c.AttemptedAt) || !snowflake(c.MessageID) || c.ConfirmedAt != "" {
				return errors.New("invalid_ack_ledger")
			}
		case "confirmed":
			if !validStamp(c.AttemptedAt) || !snowflake(c.MessageID) || !validStamp(c.ConfirmedAt) {
				return errors.New("invalid_confirmed_ledger")
			}
		default:
			return errors.New("invalid_ledger_state")
		}
		if c.MessageID != "" && c.MessageURL != messageURL(t, c.MessageID) {
			return errors.New("invalid_receipt_url")
		}
	}
	if r.Complete != allConfirmed {
		return errors.New("invalid_ledger_completion")
	}
	return nil
}

// Publish permits at most one POST per chunk for a stable run ID. Attempted,
// failed and uncertain chunks are never resent. Known message IDs can be
// reconciled by GET, and already confirmed receipts are returned as-is.
func (c *Client) Publish(ctx context.Context, stateDir, runID string, report []byte) (Receipt, error) {
	var receipt Receipt
	if err := c.target.Validate(); err != nil {
		return receipt, err
	}
	if !runPattern.MatchString(runID) {
		return receipt, errors.New("invalid_run_id")
	}
	if c.token != "" && bytes.Contains(report, []byte(c.token)) {
		return receipt, errors.New("credential_like_report_content_rejected")
	}
	if err := rejectCredentialContent(report); err != nil {
		return receipt, err
	}
	chunks, err := Split(report)
	if err != nil {
		return receipt, err
	}
	guard, err := newRecoveryGuard(stateDir, c.target)
	if err != nil {
		return receipt, err
	}
	if err = guard.check(runID, report, chunks); err != nil {
		return receipt, err
	}
	s, err := openStore(stateDir)
	if err != nil {
		return receipt, err
	}
	defer s.close()
	if err = s.pin(c.target); err != nil {
		return receipt, err
	}
	payloadHash := hashBytes(report)
	name := "run-" + hashBytes([]byte(runID)) + ".json"
	b, err := s.read(name)
	if errors.Is(err, os.ErrNotExist) {
		receipt = Receipt{Version: 1, RunID: runID, Target: c.target, PayloadHash: payloadHash, CreatedAt: stamp(), Chunks: make([]ChunkReceipt, len(chunks))}
		for i, text := range chunks {
			receipt.Chunks[i] = ChunkReceipt{Index: i, ContentHash: hashBytes([]byte(text)), Nonce: nonce(c.target, runID, payloadHash, i), State: "prepared"}
		}
		if err = s.write(name, receipt); err != nil {
			return receipt, err
		}
	} else if err != nil {
		return receipt, err
	} else if strictJSON(b, &receipt) != nil {
		return Receipt{}, errors.New("invalid_ledger_json")
	}
	if err = validateReceipt(receipt, c.target, runID, payloadHash, chunks); err != nil {
		return Receipt{}, err
	}
	if receipt.Complete {
		return receipt, nil
	}
	for i, text := range chunks {
		cr := &receipt.Chunks[i]
		if cr.State == "confirmed" {
			continue
		}
		if cr.State == "attempted" || cr.State == "failed" || (cr.State == "uncertain" && cr.MessageID == "") {
			return receipt, errors.New("run_blocked_no_resend")
		}
		if ctx.Err() != nil {
			return receipt, errors.New("publish_cancelled")
		}
		if cr.State == "prepared" {
			if _, err = c.Inspect(ctx); err != nil {
				return receipt, err
			}
			if ctx.Err() != nil {
				return receipt, errors.New("publish_cancelled")
			}
			cr.State = "attempted"
			cr.AttemptedAt = stamp()
			if err = s.write(name, receipt); err != nil {
				return receipt, err
			}
			// Re-read recovery controls after preflight and the durable attempt
			// marker, immediately before POST. A newly added block or removed
			// fence is never converted into permission to replay.
			if err = guard.check(runID, report, chunks); err != nil {
				return receipt, err
			}
			cr.MessageID, cr.State, cr.Code = c.postOnce(ctx, text, cr.Nonce)
			if cr.MessageID != "" {
				cr.MessageURL = messageURL(c.target, cr.MessageID)
			}
			// Persist acknowledgement before read-back so a failed GET is recoverable
			// without any additional POST. A save failure leaves the attempted marker.
			if err = s.write(name, receipt); err != nil {
				return receipt, err
			}
			if cr.State != "acknowledged" {
				return receipt, errors.New("run_blocked_no_resend")
			}
		}
		if err = c.readback(ctx, cr.MessageID, text); err != nil {
			cr.State = "uncertain"
			cr.Code = "readback_" + err.Error()
			if saveErr := s.write(name, receipt); saveErr != nil {
				return receipt, saveErr
			}
			return receipt, errors.New("readback_unconfirmed_no_resend")
		}
		cr.State = "confirmed"
		cr.Code = ""
		cr.ConfirmedAt = stamp()
		receipt.Complete = i == len(chunks)-1
		if err = s.write(name, receipt); err != nil {
			return receipt, err
		}
	}
	return receipt, nil
}
