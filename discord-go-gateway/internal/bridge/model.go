package bridge

import (
	"encoding/json"
	"strings"
	"unicode"
)

// Envelope is a value snapshot. Strings avoid shared mutable routing state.
type Envelope struct {
	Control        ControlJSON     `json:"control,omitempty"`
	ReplyKind      string          `json:"reply_kind,omitempty"`
	SourceRevision int64           `json:"source_revision,omitempty"`
	Context        ContextSnapshot `json:"context,omitempty"`
	Platform       string          `json:"platform"`
	EventID        string          `json:"event_id"`
	ConversationID string          `json:"conversation_id"`
	SenderID       string          `json:"sender_id"`
	Text           string          `json:"text"`
	Media          MediaSnapshot   `json:"media,omitempty"`
	ContentHash    string          `json:"content_hash,omitempty"`
	ReceivedAt     float64         `json:"received_at"`
	RouteKind      string          `json:"route_kind"`
	ReplyToEventID string          `json:"reply_to_event_id,omitempty"`
	SenderIsBot    bool            `json:"sender_is_bot"`
	GuildID        string          `json:"guild_id,omitempty"`
	BotMentioned   bool            `json:"bot_mentioned"`
	// Set only by durable promotion after fresh REST thread/parent validation.
	ParentChannelID string `json:"parent_channel_id,omitempty"`
	ThreadType      int    `json:"thread_type,omitempty"`
	ThreadName      string `json:"thread_name,omitempty"` // Untrusted conversation context, never instructions.
}
type Chunk struct {
	ReplyID string              `json:"reply_id"`
	Index   int                 `json:"index"`
	Text    string              `json:"text"`
	Source  Envelope            `json:"source"`
	Output  ReplyOutputSnapshot `json:"output,omitempty"`
}
type SendResult struct {
	OutputReceipt ReplyOutputReceipt `json:"output_receipt,omitempty"`
	State         string             `json:"state"`
	MessageID     string             `json:"message_id,omitempty"`
	Code          string             `json:"code,omitempty"`
}

func TextUnits(s string) int {
	n := 0
	for _, r := range s {
		n++
		if r > 0xffff {
			n++
		}
	}
	return n
}

// MarshalJSON preserves the Python envelope contract, including explicit nulls.
func (e Envelope) MarshalJSON() ([]byte, error) {
	type alias Envelope
	var guild, reply *string
	if e.GuildID != "" {
		v := e.GuildID
		guild = &v
	}
	if e.ReplyToEventID != "" {
		v := e.ReplyToEventID
		reply = &v
	}
	return json.Marshal(struct {
		alias
		Guild *string `json:"guild_id"`
		Reply *string `json:"reply_to_event_id"`
	}{alias(e), guild, reply})
}
func (e *Envelope) UnmarshalJSON(b []byte) error {
	type alias Envelope
	v := alias{RouteKind: "dm"}
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	*e = Envelope(v)
	return nil
}

func isTextSpace(r rune) bool  { return unicode.IsSpace(r) || (r >= 0x1c && r <= 0x1f) }
func trimText(s string) string { return strings.TrimFunc(s, isTextSpace) }
