package bridge

import (
	"context"
	"encoding/json"
	"errors"
	"unicode/utf8"

	"github.com/bwmarrin/discordgo"
)

// Context is external evidence, never an instruction or independent task. It is
// bounded, one-hop and source-labelled. A comparable immutable JSON value keeps
// the existing full-envelope promotion proof meaningful.
type ContextSnapshot string

type ContextMessage struct {
	Relation   string        `json:"relation"`
	Status     string        `json:"status"`
	Provenance string        `json:"provenance"`
	Trust      string        `json:"trust"`
	EventID    string        `json:"message_id"`
	ChannelID  string        `json:"channel_id"`
	AuthorID   string        `json:"author_id,omitempty"`
	Text       string        `json:"text,omitempty"`
	Media      MediaSnapshot `json:"media,omitempty"`
	Truncated  bool          `json:"truncated,omitempty"`
}

func (c ContextSnapshot) MarshalJSON() ([]byte, error) {
	if c == "" {
		return []byte("[]"), nil
	}
	var messages []ContextMessage
	if err := json.Unmarshal([]byte(c), &messages); err != nil {
		return nil, errors.New("invalid message context")
	}
	if err := validateContext(messages); err != nil {
		return nil, err
	}
	return []byte(c), nil
}
func (c *ContextSnapshot) UnmarshalJSON(raw []byte) error {
	var messages []ContextMessage
	if err := json.Unmarshal(raw, &messages); err != nil {
		return err
	}
	if err := validateContext(messages); err != nil {
		return err
	}
	b, err := json.Marshal(messages)
	if err == nil {
		*c = ContextSnapshot(b)
	}
	return err
}
func (c ContextSnapshot) Messages() []ContextMessage {
	var out []ContextMessage
	json.Unmarshal([]byte(c), &out)
	return out
}
func validateContext(messages []ContextMessage) error {
	if len(messages) > 2 {
		return errors.New("too much message context")
	}
	for _, m := range messages {
		switch m.Status {
		case "available", "unresolved", "unavailable", "deleted", "out_of_scope", "invalid_source", "no_parent_source":
		default:
			return errors.New("invalid context status")
		}
		switch m.Provenance {
		case "discord_gateway_resolved", "discord_rest_exact", "discord_message_reference":
		default:
			return errors.New("invalid context provenance")
		}
		if m.Status != "available" && (m.Text != "" || m.Media != "" || m.AuthorID != "") {
			return errors.New("unavailable context contains body")
		}

		if (m.Relation != "reply" && m.Relation != "thread_starter") || !Snowflake(m.EventID) || !Snowflake(m.ChannelID) || (m.AuthorID != "" && !Snowflake(m.AuthorID)) || !utf8.ValidString(m.Text) || TextUnits(m.Text) > 2000 || len(m.Media) > MaxMediaJSON || m.Trust != "untrusted_external_context" {
			return errors.New("invalid message context")
		}
	}
	return nil
}
func contextSnapshot(messages []ContextMessage) ContextSnapshot {
	if len(messages) == 0 {
		return ""
	}
	b, err := json.Marshal(messages)
	if err != nil {
		return ""
	}
	return ContextSnapshot(b)
}
func boundedContextText(text string) (string, bool) {
	used := 0
	for i, r := range text {
		size := 1
		if r > 0xffff {
			size = 2
		}
		if used+size > 2000 {
			return text[:i], true
		}
		used += size
	}
	return text, false
}
func contextFromMessage(relation, provenance string, m *discordgo.Message) ContextMessage {
	c := ContextMessage{Relation: relation, Status: "available", Provenance: provenance, Trust: "untrusted_external_context", EventID: m.ID, ChannelID: m.ChannelID, Text: m.Content, Media: ProjectMedia(m)}
	if m.Author != nil && Snowflake(m.Author.ID) {
		c.AuthorID = m.Author.ID
	}
	c.Text, c.Truncated = boundedContextText(m.Content)
	return c
}
func unresolvedContext(relation, channel, id, status string) ContextMessage {
	return ContextMessage{Relation: relation, Status: status, Provenance: "discord_message_reference", Trust: "untrusted_external_context", ChannelID: channel, EventID: id}
}
func projectMessageContext(m *discordgo.Message) ContextSnapshot {
	if m.MessageReference == nil || m.Type != discordgo.MessageTypeReply || m.MessageReference.Type != discordgo.MessageReferenceTypeDefault || !Snowflake(m.MessageReference.MessageID) {
		return ""
	}
	ref := m.MessageReference
	channel := ref.ChannelID
	if channel == "" {
		channel = m.ChannelID
	}
	if !Snowflake(channel) {
		return ""
	}
	c := unresolvedContext("reply", channel, ref.MessageID, "unresolved")
	if channel != m.ChannelID || (ref.GuildID != "" && ref.GuildID != m.GuildID) {
		c.Status = "out_of_scope"
	} else if quoted := m.ReferencedMessage; quoted != nil && quoted.ID == ref.MessageID && quoted.ChannelID == channel && (quoted.GuildID == "" || quoted.GuildID == m.GuildID) {
		c = contextFromMessage("reply", "discord_gateway_resolved", quoted)
	}
	return contextSnapshot([]ContextMessage{c})
}
func (r *RESTClient) exactContext(ctx context.Context, relation, channel, id, guild string) ContextMessage {
	c := unresolvedContext(relation, channel, id, "unavailable")
	var m discordgo.Message
	if err := r.get(ctx, "/channels/"+channel+"/messages/"+id, &m); err != nil {
		if err.Error() == "preflight_http_404" {
			c.Status = "deleted"
		}
		return c
	}
	if m.ID != id || m.ChannelID != channel || (m.GuildID != "" && m.GuildID != guild) {
		c.Status = "invalid_source"
		return c
	}
	return contextFromMessage(relation, "discord_rest_exact", &m)
}
func (r *RESTClient) resolveMessageContext(ctx context.Context, e Envelope) ContextSnapshot {
	messages := e.Context.Messages()
	for i, c := range messages {
		if c.Relation == "reply" && c.Status == "unresolved" && c.ChannelID == e.ConversationID {
			messages[i] = r.exactContext(ctx, "reply", c.ChannelID, c.EventID, e.GuildID)
		}
	}
	if e.RouteKind == "guild_thread" && e.ThreadType == 11 && e.EventID != e.ConversationID && len(messages) < 2 {
		// A single exact starter lookup, not history enumeration. Only type21's
		// explicit reference can authorize reading the pinned parent source.
		c := unresolvedContext("thread_starter", e.ConversationID, e.ConversationID, "unavailable")
		c.Provenance = "discord_rest_exact"
		var starter discordgo.Message
		if err := r.get(ctx, "/channels/"+e.ConversationID+"/messages/"+e.ConversationID, &starter); err == nil {
			if starter.ID != e.ConversationID || starter.ChannelID != e.ConversationID || (starter.GuildID != "" && starter.GuildID != e.GuildID) {
				c.Status = "invalid_source"
			} else if starter.Type != discordgo.MessageTypeThreadStarterMessage || starter.MessageReference == nil {
				c.Status = "no_parent_source"
			} else {
				ref := starter.MessageReference
				if ref.ChannelID == e.ParentChannelID && Snowflake(ref.MessageID) && (ref.GuildID == "" || ref.GuildID == e.GuildID) {
					c = r.exactContext(ctx, "thread_starter", ref.ChannelID, ref.MessageID, e.GuildID)
				} else {
					c.Status = "out_of_scope"
				}
			}
		}
		messages = append(messages, c)

	}
	return contextSnapshot(messages)
}
