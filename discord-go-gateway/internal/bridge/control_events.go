package bridge

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"unicode/utf8"
)

// ControlJSON remains comparable so the immutable ingress equality proof still
// works. It marshals as a structured object, never fabricated user-message text.
type ControlJSON string

func (v ControlJSON) MarshalJSON() ([]byte, error) {
	if v == "" {
		return []byte("null"), nil
	}
	if !json.Valid([]byte(v)) {
		return nil, errors.New("invalid_control")
	}
	return []byte(v), nil
}
func (v *ControlJSON) UnmarshalJSON(b []byte) error {
	if string(b) == "null" {
		*v = ""
		return nil
	}
	var e ControlEvent
	if err := json.Unmarshal(b, &e); err != nil {
		return err
	}
	raw, err := json.Marshal(e)
	*v = ControlJSON(raw)
	return err
}

type ControlEvent struct {
	Version           int             `json:"version"`
	Kind              string          `json:"kind"`
	ID                string          `json:"id"`
	InteractionID     string          `json:"interaction_id,omitempty"`
	ActorID           string          `json:"actor_id"`
	TargetMessageID   string          `json:"target_message_id,omitempty"`
	TargetRequestID   string          `json:"target_request_id,omitempty"`
	TargetRevision    string          `json:"target_revision,omitempty"`
	TargetText        string          `json:"target_text,omitempty"`
	TargetOutput      json.RawMessage `json:"target_output,omitempty"`
	TargetReceipt     json.RawMessage `json:"target_receipt,omitempty"`
	Emoji             string          `json:"emoji,omitempty"`
	Added             bool            `json:"added,omitempty"`
	PendingResponseID string          `json:"pending_response_id,omitempty"`
	// No authorization decisions are encoded. A reaction is contextual evidence.
	ApprovalGranted bool `json:"approval_granted"`
}

func (e Envelope) ControlEvent() (ControlEvent, error) {
	var c ControlEvent
	err := json.Unmarshal([]byte(e.Control), &c)
	if err != nil || c.Version != 1 || c.ActorID != e.SenderID || c.ApprovalGranted || c.ID == "" {
		return c, errors.New("invalid_control")
	}
	switch c.Kind {
	case "ask":
		if e.ReplyKind != "interaction" || c.InteractionID != e.EventID {
			return c, errors.New("invalid_control")
		}
	case "reaction":
		if e.ReplyKind != "message" || c.TargetMessageID != e.EventID || c.TargetRequestID == "" || len(c.TargetRevision) != 64 || c.Emoji == "" {
			return c, errors.New("invalid_control")
		}
	default:
		return c, errors.New("invalid_control")
	}
	return c, nil
}
func encodeControl(c ControlEvent) ControlJSON { b, _ := json.Marshal(c); return ControlJSON(b) }
func controlRevision(e Envelope) string {
	b, _ := json.Marshal(e)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
func validControl(e Envelope) bool {
	c, err := e.ControlEvent()
	return err == nil && utf8.ValidString(c.TargetText) && TextUnits(c.TargetText) <= 1900 && len(c.Emoji) <= 128 && len(c.TargetOutput) <= 65536 && len(c.TargetReceipt) <= 16384 && (len(c.TargetOutput) == 0 || json.Valid(c.TargetOutput) && json.Valid(c.TargetReceipt))
}
