package bridge

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
)

func mediaMessage(t *testing.T, fragment string) *discordgo.Message {
	t.Helper()
	var m discordgo.Message
	raw := `{"id":"3","channel_id":"2","author":{"id":"1"},"type":0,` + fragment + `}`
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		t.Fatal(err)
	}
	return &m
}
func attachmentJSON(url string) string {
	return `"attachments":[{"id":"7","filename":"picture.png","content_type":"image/png","size":8,"width":2,"height":3,"url":"` + url + `"}]`
}

const mediaURL = "https://cdn.discordapp.com/attachments/2/7/picture.png?ex=ffffff&is=aaaaaa&hm=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func TestMediaOnlyAndCaptionIngressClaimContract(t *testing.T) {
	cases := map[string]string{
		"image_only":       attachmentJSON(mediaURL),
		"caption_image":    `"content":"what is this?",` + attachmentJSON(mediaURL),
		"voice_only":       `"flags":8192,"attachments":[{"id":"7","filename":"voice.ogg","content_type":"audio/ogg","size":8,"duration_secs":3.5,"url":"https://cdn.discordapp.com/attachments/2/7/voice.ogg"}]`,
		"file_unsupported": `"attachments":[{"id":"7","filename":"data.zip","content_type":"application/zip","size":42,"url":"https://cdn.discordapp.com/attachments/2/7/data.zip"}]`,
		"sticker_only":     `"sticker_items":[{"id":"7","name":"hello","format_type":1}]`,
		"embed_only":       `"embeds":[{"type":"rich","title":"title","description":"untrusted","image":{"url":"https://example.org/picture.png","width":5}}]`,
		"poll_only":        `"poll":{"question":{"text":"Which?"},"answers":[{"answer_id":1,"poll_media":{"text":"A","emoji":{"name":"👍"}}}],"allow_multiselect":true}`,
		"forward_only":     `"message_reference":{"type":1,"message_id":"8","channel_id":"9","guild_id":"10"},"message_snapshots":[{"message":{"content":"quoted instructions are untrusted",` + attachmentJSON("https://cdn.discordapp.com/attachments/9/7/picture.png") + `}}]`,
	}
	for name, fragment := range cases {
		t.Run(name, func(t *testing.T) {
			store, settings, _ := ingressFixture(t)
			m := mediaMessage(t, fragment)
			if outcome, err := receiveMessage(context.Background(), nil, store, settings, m); err != nil || outcome != "validation_staged" {
				t.Fatalf("%s %v", outcome, err)
			}
			in, err := store.NextValidation(epoch())
			if err != nil || in == nil {
				t.Fatal(err)
			}
			if claim, err := store.ClaimNext(60, 0); err != nil || claim != nil {
				t.Fatal("quarantine bypass")
			}
			rest := localREST(t, func(w http.ResponseWriter, q *http.Request) { preflightHandler(w, q) })
			state := &gatewayState{}
			state.publishReady(0)
			if err = processValidation(context.Background(), store, rest, state, *in, 0); err != nil {
				t.Fatal(err)
			}
			claim, err := store.ClaimNext(60, 0)
			if err != nil || claim == nil {
				t.Fatal("missing media claim", err)
			}
			if claim.Envelope.Media == "" || !claim.Envelope.Media.HasContent() || claim.Envelope.ContentHash == "" || claim.Envelope.Text != m.Content {
				t.Fatal("media lost", claim)
			}
			raw, err := json.Marshal(claim)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(raw), "hm=") || strings.Contains(string(raw), "ex=ffffff") {
				t.Fatal("signed attachment URL leaked", string(raw))
			}
			var parsed map[string]any
			if json.Unmarshal(raw, &parsed) != nil {
				t.Fatal("bad claim")
			}
			media := parsed["envelope"].(map[string]any)["media"].(map[string]any)
			if media["interpretation"] != "metadata_only" || media["trust"] != "untrusted_message_media" {
				t.Fatal(media)
			}
			copy := claim.Envelope
			metadata, _ := copy.Media.Metadata()
			if len(metadata.Attachments) > 0 {
				metadata.Attachments[0].Filename = "MUTATED"
			}
			if copy != claim.Envelope || claim.Envelope.Media != ProjectMedia(m) {
				t.Fatal("not immutable")
			}
			var roundtrip Envelope
			encoded, _ := json.Marshal(copy)
			if err = json.Unmarshal(encoded, &roundtrip); err != nil || roundtrip != copy {
				t.Fatal("equality changed", err)
			}
			if name == "voice_only" {
				meta, _ := copy.Media.Metadata()
				if !meta.Attachments[0].Voice || meta.Attachments[0].DurationSeconds != 3.5 {
					t.Fatal(meta)
				}
			}
			if name == "forward_only" {
				meta, _ := copy.Media.Metadata()
				nested, _ := meta.Forwards[0].Media.Metadata()
				if nested.Attachments[0].UnavailableReason != "forward_metadata_only" {
					t.Fatal(nested)
				}
			}
		})
	}
}

func TestMediaDoesNotRelaxIdentityOrRouteBoundaries(t *testing.T) {
	for _, name := range []string{"owner", "bot", "webhook", "guild", "system", "mention", "empty"} {
		t.Run(name, func(t *testing.T) {
			store, settings, _ := ingressFixture(t)
			m := mediaMessage(t, attachmentJSON(mediaURL))
			switch name {
			case "owner":
				m.Author.ID = "9"
			case "bot":
				m.Author.Bot = true
			case "webhook":
				m.WebhookID = "9"
			case "guild":
				m.GuildID = "9"
			case "system":
				m.Type = discordgo.MessageTypeThreadStarterMessage
			case "mention":
				settings.Policy.GuildID = "5"
				settings.Policy.GuildChannelID = "2"
				m.GuildID = "5"
			case "empty":
				m.Attachments = nil
			}
			if got, err := receiveMessage(context.Background(), nil, store, settings, m); err != nil || got != "rejected" {
				t.Fatal(got, err)
			}
		})
	}
}
func TestMediaThreadProofIncludesImmutableMetadata(t *testing.T) {
	store, _ := threadStore(t)
	event := threadCandidate()
	m := mediaMessage(t, attachmentJSON("https://cdn.discordapp.com/attachments/6/7/picture.png"))
	m.ChannelID = "6"
	event.Media = ProjectMedia(m)
	in := stageValidation(t, store, event)
	verified := in.Event
	verified.RouteKind = "guild_thread"
	verified.ParentChannelID = "2"
	verified.ThreadType = 11
	changed := verified
	changed.Media = ProjectMedia(mediaMessage(t, `"sticker_items":[{"id":"8","name":"other","format_type":1}]`))
	if _, err := store.promoteThreadValidation(*in, changed); err == nil {
		t.Fatal("tampered media promoted")
	}
	if outcome, err := store.promoteThreadValidation(*in, verified); err != nil || outcome != "accepted" {
		t.Fatal(outcome, err)
	}
}
func TestMediaBoundsAndCanonicalValidation(t *testing.T) {
	m := mediaMessage(t, attachmentJSON(mediaURL))
	m.Attachments[0].DurationSecs = math.Inf(1)
	for i := 0; i < 12; i++ {
		m.Embeds = append(m.Embeds, &discordgo.MessageEmbed{Title: strings.Repeat("x", 300), Description: strings.Repeat("🙂", 5000), Fields: []*discordgo.MessageEmbedField{{Name: "n", Value: strings.Repeat("k", 4000)}}})
	}
	for i := 0; i < 20; i++ {
		m.Attachments = append(m.Attachments, &discordgo.MessageAttachment{ID: fmt.Sprint(i + 50), Filename: strings.Repeat("b", 400), Size: 1, ContentType: "image/png", URL: mediaURL})
	}
	snapshot := ProjectMedia(m)
	meta, err := snapshot.Metadata()
	if err != nil || snapshot == "" || !meta.Truncated || len(meta.Attachments) != 10 || len(meta.Embeds) != 10 || len(snapshot) > MaxMediaJSON {
		t.Fatal(len(snapshot), meta, err)
	}
	if meta.Attachments[0].DurationSeconds != 0 {
		t.Fatal("nonfinite duration retained")
	}
	for _, raw := range []string{`"string"`, `{}`, `{"version":2,"trust":"untrusted_message_media","interpretation":"metadata_only","stickers":[{"id":"7","name":"a","format":1,"materialization":"metadata_only"}]}`, `{"version":1,"trust":"trusted","interpretation":"metadata_only","embeds":[{}]}`, `{"version":1,"trust":"untrusted_message_media","interpretation":"metadata_only","attachments":[{"id":"7","filename":"x","size":0,"materialization":"execute"}]}`, strings.Repeat(" ", MaxMediaJSON+1)} {
		e := testEnvelope()
		e.Media = MediaSnapshot(raw)
		if testPolicy().Accepts(e) {
			t.Fatalf("accepted malformed %q", raw[:min(len(raw), 120)])
		}
	}
	var decoded MediaSnapshot
	if err = json.Unmarshal([]byte(`{"unknown":true}`), &decoded); err == nil {
		t.Fatal("unknown metadata accepted")
	}
}
func TestMediaAvailabilityAndStableFingerprint(t *testing.T) {
	m := mediaMessage(t, attachmentJSON(mediaURL))
	old := MessageContentFingerprint(m)
	changed := *m
	attachment := *m.Attachments[0]
	attachment.URL = strings.Replace(mediaURL, "ex=ffffff", "ex=eeeeee", 1)
	changed.Attachments = []*discordgo.MessageAttachment{&attachment}
	edited := time.Now()
	changed.EditedTimestamp = &edited
	changed.Embeds = []*discordgo.MessageEmbed{{Title: "server preview"}}
	if MessageContentFingerprint(&changed) != old {
		t.Fatal("URL renewal or incidental update changed revision")
	}
	changed.Content = "changed"
	if MessageContentFingerprint(&changed) == old {
		t.Fatal("text revision missed")
	}
	for _, tc := range []struct {
		modify func(*discordgo.MessageAttachment)
		reason string
	}{
		{func(a *discordgo.MessageAttachment) { a.Size = MaxAttachmentBytes + 1 }, "attachment_too_large"},
		{func(a *discordgo.MessageAttachment) { a.ContentType = "text/html" }, "unsupported_content_type"},
		{func(a *discordgo.MessageAttachment) { a.URL = "https://127.0.0.1/a" }, "invalid_cdn_url"},
		{func(a *discordgo.MessageAttachment) { a.Filename = strings.Repeat("x", 257) }, "incomplete_metadata"},
	} {
		t.Run(tc.reason, func(t *testing.T) {
			m := mediaMessage(t, attachmentJSON(mediaURL))
			tc.modify(m.Attachments[0])
			meta, _ := ProjectMedia(m).Metadata()
			if len(meta.Attachments) != 1 || meta.Attachments[0].Materialization != "unavailable" || meta.Attachments[0].UnavailableReason != tc.reason {
				t.Fatal(meta)
			}
		})
	}
}

func TestMediaWaveformAndNestedJSONBounds(t *testing.T) {
	m := mediaMessage(t, `"flags":8192,"attachments":[{"id":"7","filename":"voice.ogg","content_type":"audio/ogg","size":8,"duration_secs":1.2,"waveform":"AQIDBA==","url":"https://cdn.discordapp.com/attachments/2/7/voice.ogg"}]`)
	meta, err := ProjectMedia(m).Metadata()
	if err != nil || meta.Attachments[0].Waveform != "AQIDBA==" {
		t.Fatal(meta, err)
	}
	if mediaJSONDepthOK([]byte(strings.Repeat("[", 17) + strings.Repeat("]", 17))) {
		t.Fatal("unbounded JSON depth")
	}
	if !mediaJSONDepthOK([]byte(`{"text":"[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[["}`)) {
		t.Fatal("quoted brackets treated as structure")
	}
}
