package reporting

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

func hashBytes(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }
func textUnits(s string) int {
	n := 0
	for _, r := range s {
		n++
		if r > 0xffff {
			n++
		}
	}
	return n
}

// Split never splits a UTF-8 rune. Newlines and spaces are preferred boundaries.
// Message-boundary whitespace is trimmed to avoid Discord's terminal-newline
// normalization; the raw input is still separately hashed as the run binding.
func Split(report []byte) ([]string, error) {
	if len(report) == 0 || len(report) > MaxReportBytes || !utf8.Valid(report) || strings.TrimSpace(string(report)) == "" {
		return nil, errors.New("invalid_report_size_or_utf8")
	}
	for _, r := range string(report) {
		if r < 32 && r != '\n' && r != '\r' && r != '\t' {
			return nil, errors.New("invalid_report_control_character")
		}
	}
	remaining := strings.TrimSpace(string(report))
	var chunks []string
	for len(remaining) > 0 {
		units, end, preferred := 0, 0, 0
		for i, r := range remaining {
			n := 1
			if r > 0xffff {
				n = 2
			}
			if units+n > MaxChunkUnits {
				break
			}
			units += n
			end = i + utf8.RuneLen(r)
			if (r == '\n' || r == ' ') && units >= MaxChunkUnits/2 {
				preferred = end
			}
		}
		if end < len(remaining) && preferred > 0 {
			end = preferred
		}
		part := strings.TrimSpace(remaining[:end])
		if strings.TrimSpace(part) == "" {
			return nil, errors.New("whitespace_only_report_chunk")
		}
		chunks = append(chunks, part)
		remaining = strings.TrimLeftFunc(remaining[end:], unicode.IsSpace)
	}
	if len(chunks) > MaxChunks {
		return nil, errors.New("too_many_report_chunks")
	}
	return chunks, nil
}
func nonce(target Target, runID, payloadHash string, index int) string {
	b, _ := json.Marshal([]any{"discord-report/v1", target, runID, payloadHash, index})
	return hashBytes(b)[:24]
}
func postPayload(text, nonce string) []byte {
	b, _ := json.Marshal(struct {
		Content         string `json:"content"`
		Nonce           string `json:"nonce"`
		EnforceNonce    bool   `json:"enforce_nonce"`
		Flags           int    `json:"flags"`
		AllowedMentions struct {
			Parse       []string `json:"parse"`
			Users       []string `json:"users"`
			Roles       []string `json:"roles"`
			RepliedUser bool     `json:"replied_user"`
		} `json:"allowed_mentions"`
	}{Content: text, Nonce: nonce, EnforceNonce: true, Flags: 4, AllowedMentions: struct {
		Parse       []string `json:"parse"`
		Users       []string `json:"users"`
		Roles       []string `json:"roles"`
		RepliedUser bool     `json:"replied_user"`
	}{Parse: []string{}, Users: []string{}, Roles: []string{}}})
	return b
}
func messageURL(t Target, id string) string {
	return fmt.Sprintf("https://discord.com/channels/%s/%s/%s", t.GuildID, t.ChannelID, id)
}
