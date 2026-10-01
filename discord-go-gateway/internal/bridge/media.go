package bridge

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math"
	"mime"
	"net/url"
	"strings"
	"unicode/utf8"

	"github.com/bwmarrin/discordgo"
)

const (
	MaxMediaJSON              = 96 * 1024
	MaxAttachmentBytes        = 10 * 1024 * 1024
	MaxMessageAttachmentBytes = 40 * 1024 * 1024
)

// MediaSnapshot is immutable canonical JSON, not a pointer, slice, or map.
// Keeping Envelope comparable preserves the full-value ingress promotion proof.
// Metadata() always decodes a fresh value; no consumer can mutate a staged copy.
type MediaSnapshot string

type MediaMetadata struct {
	Version        int                  `json:"version"`
	Trust          string               `json:"trust"`
	Interpretation string               `json:"interpretation"`
	Attachments    []AttachmentMetadata `json:"attachments,omitempty"`
	Stickers       []StickerMetadata    `json:"stickers,omitempty"`
	Embeds         []EmbedMetadata      `json:"embeds,omitempty"`
	Poll           *PollMetadata        `json:"poll,omitempty"`
	Forwards       []ForwardMetadata    `json:"forwards,omitempty"`
	Truncated      bool                 `json:"truncated,omitempty"`
}
type AttachmentMetadata struct {
	ID                string  `json:"id"`
	Filename          string  `json:"filename"`
	ContentType       string  `json:"content_type,omitempty"`
	Size              int64   `json:"size"`
	Width             int     `json:"width,omitempty"`
	Height            int     `json:"height,omitempty"`
	DurationSeconds   float64 `json:"duration_seconds,omitempty"`
	Waveform          string  `json:"waveform,omitempty"`
	Voice             bool    `json:"voice,omitempty"`
	Ephemeral         bool    `json:"ephemeral,omitempty"`
	Materialization   string  `json:"materialization"`
	UnavailableReason string  `json:"unavailable_reason,omitempty"`
}
type StickerMetadata struct {
	ID              string `json:"id"`
	Name            string `json:"name"`
	Format          int    `json:"format"`
	Materialization string `json:"materialization"`
}
type EmbedMetadata struct {
	Type            string               `json:"type,omitempty"`
	Title           string               `json:"title,omitempty"`
	Description     string               `json:"description,omitempty"`
	URL             string               `json:"url,omitempty"`
	Author          string               `json:"author,omitempty"`
	Footer          string               `json:"footer,omitempty"`
	Fields          []EmbedFieldMetadata `json:"fields,omitempty"`
	Image           *EmbedImageMetadata  `json:"image,omitempty"`
	Thumbnail       *EmbedImageMetadata  `json:"thumbnail,omitempty"`
	Video           *EmbedImageMetadata  `json:"video,omitempty"`
	Materialization string               `json:"materialization"`
}
type EmbedFieldMetadata struct {
	Name   string `json:"name"`
	Value  string `json:"value"`
	Inline bool   `json:"inline,omitempty"`
}
type EmbedImageMetadata struct {
	URL    string `json:"url,omitempty"`
	Width  int    `json:"width,omitempty"`
	Height int    `json:"height,omitempty"`
}
type PollMetadata struct {
	Question         string               `json:"question"`
	Answers          []PollAnswerMetadata `json:"answers"`
	AllowMultiselect bool                 `json:"allow_multiselect"`
	Expiry           string               `json:"expiry,omitempty"`
	ResultsFinalized bool                 `json:"results_finalized,omitempty"`
}
type PollAnswerMetadata struct {
	ID        int    `json:"id"`
	Text      string `json:"text,omitempty"`
	EmojiID   string `json:"emoji_id,omitempty"`
	EmojiName string `json:"emoji_name,omitempty"`
	Count     int    `json:"count,omitempty"`
}
type ForwardMetadata struct {
	Text            string        `json:"text,omitempty"`
	SourceMessageID string        `json:"source_message_id,omitempty"`
	SourceChannelID string        `json:"source_channel_id,omitempty"`
	SourceGuildID   string        `json:"source_guild_id,omitempty"`
	Media           MediaSnapshot `json:"media,omitempty"`
	Trust           string        `json:"trust"`
}

func (s MediaSnapshot) MarshalJSON() ([]byte, error) {
	if s == "" {
		return []byte("null"), nil
	}
	if _, err := s.Metadata(); err != nil {
		return nil, err
	}
	return []byte(s), nil
}
func (s *MediaSnapshot) UnmarshalJSON(b []byte) error {
	if bytes.Equal(bytes.TrimSpace(b), []byte("null")) {
		*s = ""
		return nil
	}
	var m MediaMetadata
	if len(b) > MaxMediaJSON || !utf8.Valid(b) || !mediaJSONDepthOK(b) {
		return errors.New("invalid_media_metadata")
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if err := d.Decode(&m); err != nil {
		return errors.New("invalid_media_metadata")
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return errors.New("invalid_media_metadata")
	}
	canonical, err := makeMedia(m)
	if err != nil {
		return err
	}
	*s = canonical
	return nil
}
func (s MediaSnapshot) Metadata() (MediaMetadata, error) {
	var m MediaMetadata
	if s == "" {
		return m, nil
	}
	if len(s) > MaxMediaJSON || !utf8.ValidString(string(s)) || !mediaJSONDepthOK([]byte(s)) {
		return m, errors.New("invalid_media_metadata")
	}
	d := json.NewDecoder(strings.NewReader(string(s)))
	d.DisallowUnknownFields()
	if d.Decode(&m) != nil || validateMedia(m) != nil {
		return MediaMetadata{}, errors.New("invalid_media_metadata")
	}
	// Reject trailing JSON and noncanonical constructed string values as well.
	b, err := json.Marshal(m)
	if err != nil || string(b) != string(s) {
		return MediaMetadata{}, errors.New("invalid_media_metadata")
	}
	return m, nil
}
func (s MediaSnapshot) HasContent() bool {
	m, err := s.Metadata()
	return err == nil && m.hasContent()
}
func (m MediaMetadata) hasContent() bool {
	return len(m.Attachments)+len(m.Stickers)+len(m.Embeds)+len(m.Forwards) > 0 || m.Poll != nil
}
func makeMedia(m MediaMetadata) (MediaSnapshot, error) {
	if !m.hasContent() {
		return "", nil
	}
	if err := validateMedia(m); err != nil {
		return "", err
	}
	b, err := json.Marshal(m)
	if err != nil || len(b) > MaxMediaJSON {
		return "", errors.New("invalid_media_metadata")
	}
	return MediaSnapshot(b), nil
}
func boundedString(s string, n int) bool { return utf8.ValidString(s) && TextUnits(s) <= n }
func validateMedia(m MediaMetadata) error {
	invalid := errors.New("invalid_media_metadata")
	if m.Version != 1 || m.Trust != "untrusted_message_media" || m.Interpretation != "metadata_only" || len(m.Attachments) > 10 || len(m.Stickers) > 3 || len(m.Embeds) > 10 || len(m.Forwards) > 1 {
		return invalid
	}
	seen := map[string]bool{}
	for _, a := range m.Attachments {
		if !Snowflake(a.ID) || seen[a.ID] || !boundedString(a.Filename, 256) || !boundedString(a.ContentType, 128) || a.Size < 0 || a.Width < 0 || a.Width > 100000 || a.Height < 0 || a.Height > 100000 || math.IsNaN(a.DurationSeconds) || math.IsInf(a.DurationSeconds, 0) || a.DurationSeconds < 0 || a.DurationSeconds > 86400 {
			return invalid
		}
		seen[a.ID] = true
		if a.Waveform != "" {
			if len(a.Waveform) > 344 {
				return invalid
			}
			b, err := base64.StdEncoding.DecodeString(a.Waveform)
			if err != nil || len(b) > 256 {
				return invalid
			}
		}
		if a.Materialization != "available_via_materialize" && a.Materialization != "unavailable" {
			return invalid
		}
		switch a.UnavailableReason {
		case "", "unsupported_content_type", "attachment_too_large", "message_attachment_budget", "invalid_cdn_url", "forward_metadata_only", "incomplete_metadata":
		default:
			return invalid
		}
		if (a.Materialization == "unavailable") != (a.UnavailableReason != "") {
			return invalid
		}
	}
	for _, s := range m.Stickers {
		if !Snowflake(s.ID) || !boundedString(s.Name, 128) || s.Format < 1 || s.Format > 4 || s.Materialization != "metadata_only" {
			return invalid
		}
	}
	for _, e := range m.Embeds {
		if !boundedString(e.Type, 32) || !boundedString(e.Title, 256) || !boundedString(e.Description, 2048) || !boundedString(e.Author, 256) || !boundedString(e.Footer, 1024) || len(e.Fields) > 10 || e.Materialization != "metadata_only" || !validDisplayURL(e.URL) {
			return invalid
		}
		for _, f := range e.Fields {
			if !boundedString(f.Name, 256) || !boundedString(f.Value, 1024) {
				return invalid
			}
		}
		for _, i := range []*EmbedImageMetadata{e.Image, e.Thumbnail, e.Video} {
			if i != nil && (!validDisplayURL(i.URL) || i.Width < 0 || i.Height < 0 || i.Width > 100000 || i.Height > 100000) {
				return invalid
			}
		}
	}
	if p := m.Poll; p != nil {
		if !boundedString(p.Question, 300) || len(p.Answers) > 10 || !boundedString(p.Expiry, 64) {
			return invalid
		}
		for _, a := range p.Answers {
			if a.ID < 0 || !boundedString(a.Text, 300) || !boundedString(a.EmojiName, 128) || (a.EmojiID != "" && !Snowflake(a.EmojiID)) || a.Count < 0 {
				return invalid
			}
		}
	}
	for _, f := range m.Forwards {
		if !boundedString(f.Text, 4000) || f.Trust != "untrusted_forwarded_content" || (f.SourceMessageID != "" && !Snowflake(f.SourceMessageID)) || (f.SourceChannelID != "" && !Snowflake(f.SourceChannelID)) || (f.SourceGuildID != "" && !Snowflake(f.SourceGuildID)) {
			return invalid
		}
		fm, err := f.Media.Metadata()
		if err != nil || len(fm.Forwards) > 0 {
			return invalid
		}
		for _, a := range fm.Attachments {
			if a.Materialization != "unavailable" || a.UnavailableReason != "forward_metadata_only" {
				return invalid
			}
		}
	}
	return nil
}
func validDisplayURL(s string) bool {
	if s == "" {
		return true
	}
	if !boundedString(s, 2048) {
		return false
	}
	u, e := url.Parse(s)
	return e == nil && (u.Scheme == "https" || u.Scheme == "http") && u.Hostname() != "" && u.User == nil
}

// A shared projection budget prevents multiplicative embed/forward fields from
// turning one message into an unbounded persisted object. URLs are display-only.
type mediaBudget struct {
	left      int
	truncated bool
}

func (b *mediaBudget) text(s string, limit int) string {
	if !utf8.ValidString(s) {
		s = strings.ToValidUTF8(s, "�")
		b.truncated = true
	}
	if limit > b.left {
		limit = b.left
	}
	used, end := 0, 0
	for i, r := range s {
		n := 1
		if r > 0xffff {
			n = 2
		}
		if used+n > limit {
			b.truncated = true
			break
		}
		used += n
		end = i + utf8.RuneLen(r)
	}
	if end < len(s) {
		b.truncated = true
	}
	b.left -= used
	return s[:end]
}
func (b *mediaBudget) url(s string) string {
	if !validDisplayURL(s) {
		if s != "" {
			b.truncated = true
		}
		return ""
	}
	if TextUnits(s) > b.left {
		b.truncated = true
		return ""
	}
	return b.text(s, 2048)
}
func mediaDimension(n int) int {
	if n < 0 {
		return 0
	}
	if n > 100000 {
		return 100000
	}
	return n
}
func mediaContentType(s string) string {
	m, _, e := mime.ParseMediaType(s)
	if e != nil || len(m) > 128 {
		return ""
	}
	return strings.ToLower(m)
}
func supportedAttachmentType(t string) bool {
	switch t {
	case "image/png", "image/jpeg", "image/gif", "image/webp", "audio/ogg", "audio/mpeg", "audio/mp4", "audio/wav", "audio/x-wav", "audio/webm", "audio/flac", "video/mp4", "video/webm", "application/pdf", "text/plain", "text/csv", "application/json":
		return true
	}
	return false
}
func ProjectMedia(m *discordgo.Message) MediaSnapshot {
	if m == nil {
		return ""
	}
	b := &mediaBudget{left: 12000}
	meta := projectMedia(m, b, false)
	meta.Truncated = b.truncated
	snapshot, err := makeMedia(meta)
	if err != nil {
		return ""
	}
	return snapshot
}
func projectMedia(m *discordgo.Message, b *mediaBudget, forward bool) MediaMetadata {
	out := MediaMetadata{Version: 1, Trust: "untrusted_message_media", Interpretation: "metadata_only"}
	seen := map[string]bool{}
	total := int64(0)
	if len(m.Attachments) > 10 || len(m.StickerItems) > 3 || len(m.Embeds) > 10 || len(m.MessageSnapshots) > 1 {
		b.truncated = true
	}
	for i, a := range m.Attachments {
		if i >= 10 {
			break
		}
		if a == nil || !Snowflake(a.ID) || seen[a.ID] || a.Size < 0 {
			b.truncated = true
			continue
		}
		seen[a.ID] = true
		v := AttachmentMetadata{ID: a.ID, Filename: b.text(a.Filename, 256), ContentType: mediaContentType(a.ContentType), Size: int64(a.Size), Width: mediaDimension(a.Width), Height: mediaDimension(a.Height), Voice: m.Flags&discordgo.MessageFlagsIsVoiceMessage != 0, Ephemeral: a.Ephemeral, Materialization: "available_via_materialize"}
		if a.DurationSecs >= 0 && a.DurationSecs <= 86400 && !math.IsNaN(a.DurationSecs) && !math.IsInf(a.DurationSecs, 0) {
			v.DurationSeconds = a.DurationSecs
		}
		if len(a.Waveform) > 344 {
			b.truncated = true
		}
		if a.Waveform != "" && len(a.Waveform) <= 344 {
			if data, err := base64.StdEncoding.DecodeString(a.Waveform); err == nil && len(data) <= 256 {
				v.Waveform = a.Waveform
			} else {
				b.truncated = true
			}
		}
		if v.Size > MaxMessageAttachmentBytes || total > MaxMessageAttachmentBytes-v.Size {
			total = MaxMessageAttachmentBytes + 1
		} else {
			total += v.Size
		}
		switch {
		case forward:
			v.UnavailableReason = "forward_metadata_only"
		case v.Filename != a.Filename:
			v.UnavailableReason = "incomplete_metadata"
		case v.Size > MaxAttachmentBytes:
			v.UnavailableReason = "attachment_too_large"
		case total > MaxMessageAttachmentBytes:
			v.UnavailableReason = "message_attachment_budget"
		case !supportedAttachmentType(v.ContentType):
			v.UnavailableReason = "unsupported_content_type"
		case validAttachmentURL(a.URL, m.ChannelID, a.ID) != nil:
			v.UnavailableReason = "invalid_cdn_url"
		}
		if v.UnavailableReason != "" {
			v.Materialization = "unavailable"
		}
		out.Attachments = append(out.Attachments, v)
	}
	for i, s := range m.StickerItems {
		if i >= 3 {
			break
		}
		if s == nil || !Snowflake(s.ID) || s.FormatType < 1 || s.FormatType > 4 {
			b.truncated = true
			continue
		}
		out.Stickers = append(out.Stickers, StickerMetadata{s.ID, b.text(s.Name, 128), int(s.FormatType), "metadata_only"})
	}
	for i, e := range m.Embeds {
		if i >= 10 {
			break
		}
		if e == nil {
			continue
		}
		v := EmbedMetadata{Type: b.text(string(e.Type), 32), Title: b.text(e.Title, 256), Description: b.text(e.Description, 2048), URL: b.url(e.URL), Materialization: "metadata_only"}
		if e.Author != nil {
			v.Author = b.text(e.Author.Name, 256)
		}
		if e.Footer != nil {
			v.Footer = b.text(e.Footer.Text, 1024)
		}
		if len(e.Fields) > 10 {
			b.truncated = true
		}
		for j, f := range e.Fields {
			if j >= 10 {
				break
			}
			if f != nil {
				v.Fields = append(v.Fields, EmbedFieldMetadata{b.text(f.Name, 256), b.text(f.Value, 1024), f.Inline})
			}
		}
		if e.Image != nil {
			v.Image = &EmbedImageMetadata{b.url(e.Image.URL), mediaDimension(e.Image.Width), mediaDimension(e.Image.Height)}
		}
		if e.Thumbnail != nil {
			v.Thumbnail = &EmbedImageMetadata{b.url(e.Thumbnail.URL), mediaDimension(e.Thumbnail.Width), mediaDimension(e.Thumbnail.Height)}
		}
		if e.Video != nil {
			v.Video = &EmbedImageMetadata{b.url(e.Video.URL), mediaDimension(e.Video.Width), mediaDimension(e.Video.Height)}
		}
		out.Embeds = append(out.Embeds, v)
	}
	if p := m.Poll; p != nil {
		v := &PollMetadata{Question: b.text(p.Question.Text, 300), AllowMultiselect: p.AllowMultiselect}
		if p.Expiry != nil {
			v.Expiry = p.Expiry.UTC().Format("2006-01-02T15:04:05Z")
		}
		if p.Results != nil {
			v.ResultsFinalized = p.Results.Finalized
		}
		if len(p.Answers) > 10 {
			b.truncated = true
		}
		for i, a := range p.Answers {
			if i >= 10 {
				break
			}
			if a.AnswerID < 0 {
				b.truncated = true
				continue
			}
			x := PollAnswerMetadata{ID: a.AnswerID}
			if a.Media != nil {
				x.Text = b.text(a.Media.Text, 300)
				if a.Media.Emoji != nil {
					x.EmojiName = b.text(a.Media.Emoji.Name, 128)
					if Snowflake(a.Media.Emoji.ID) {
						x.EmojiID = a.Media.Emoji.ID
					}
				}
			}
			if p.Results != nil {
				for j, c := range p.Results.AnswerCounts {
					if j >= 10 {
						break
					}
					if c != nil && c.ID == a.AnswerID && c.Count >= 0 {
						x.Count = c.Count
					}
				}
			}
			v.Answers = append(v.Answers, x)
		}
		out.Poll = v
	}
	if !forward && len(m.MessageSnapshots) > 0 && m.MessageReference != nil && m.MessageReference.Type == discordgo.MessageReferenceTypeForward {
		if s := m.MessageSnapshots[0].Message; s != nil {
			f := ForwardMetadata{Text: b.text(s.Content, 4000), Trust: "untrusted_forwarded_content"}
			r := m.MessageReference
			if Snowflake(r.MessageID) {
				f.SourceMessageID = r.MessageID
			}
			if Snowflake(r.ChannelID) {
				f.SourceChannelID = r.ChannelID
			}
			if Snowflake(r.GuildID) {
				f.SourceGuildID = r.GuildID
			}
			nested := projectMedia(s, b, true)
			nested.Truncated = b.truncated
			f.Media, _ = makeMedia(nested)
			out.Forwards = append(out.Forwards, f)
		}
	}
	return out
}

// MessageContentFingerprint binds the bounded semantic snapshot, not expiring
// CDN signatures. All materialization still refreshes and validates exact IDs.
func MessageContentFingerprint(m *discordgo.Message) string {
	if m == nil {
		return ""
	}
	// Remove dynamic server additions before projection, so a large URL preview
	// cannot consume the bounded budget and change later stable content fields.
	stableMessage := *m
	stableMessage.Embeds = nil
	if m.Poll != nil {
		poll := *m.Poll
		poll.Results = nil
		stableMessage.Poll = &poll
	}
	stableMessage.MessageSnapshots = nil
	if len(m.MessageSnapshots) > 0 && m.MessageSnapshots[0].Message != nil {
		nested := *m.MessageSnapshots[0].Message
		nested.Embeds = nil
		if nested.Poll != nil {
			poll := *nested.Poll
			poll.Results = nil
			nested.Poll = &poll
		}
		stableMessage.MessageSnapshots = []discordgo.MessageSnapshot{{Message: &nested}}
	}
	type reference struct {
		Type                          discordgo.MessageReferenceType
		MessageID, ChannelID, GuildID string
	}
	var ref *reference
	if m.MessageReference != nil {
		r := m.MessageReference
		ref = &reference{r.Type, r.MessageID, r.ChannelID, r.GuildID}
	}
	v := struct {
		Text      string
		Type      discordgo.MessageType
		Reference *reference
		Media     MediaSnapshot
	}{Text: m.Content, Type: m.Type, Reference: ref, Media: ProjectMedia(&stableMessage)}
	raw, _ := json.Marshal(v)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func validEnvelopeMedia(e Envelope) bool {
	if _, err := e.Media.Metadata(); err != nil {
		return false
	}
	if e.ContentHash == "" {
		return true
	}
	b, err := hex.DecodeString(e.ContentHash)
	return err == nil && len(b) == 32 && strings.ToLower(e.ContentHash) == e.ContentHash
}

// Bound decoder nesting before invoking recursive MediaSnapshot unmarshalling.
// Braces inside strings/escapes are data and do not count as structure.
func mediaJSONDepthOK(b []byte) bool {
	depth := 0
	quoted := false
	escaped := false
	for _, c := range b {
		if quoted {
			if escaped {
				escaped = false
			} else if c == '\\' {
				escaped = true
			} else if c == '"' {
				quoted = false
			}
			continue
		}
		switch c {
		case '"':
			quoted = true
		case '{', '[':
			depth++
			if depth > 16 {
				return false
			}
		case '}', ']':
			depth--
			if depth < 0 {
				return false
			}
		}
	}
	return depth == 0 && !quoted
}
