package engine

import (
	"encoding/json"
	"regexp"
	"strings"
)

// youtubePlayerMetadata reads a public page's literal JSON only. It never
// executes JavaScript, authenticates, downloads media, or requests caption URLs.
func youtubePlayerMetadata(body []byte) ([]byte, string, bool) {
	if len(body) > MaxResponseBytes {
		return nil, "", false
	}
	raw := string(body)
	markers := []string{"ytInitialPlayerResponse =", "ytInitialPlayerResponse=", `"ytInitialPlayerResponse":`}
	for _, marker := range markers {
		offset := strings.Index(raw, marker)
		if offset < 0 {
			continue
		}
		start := offset + len(marker)
		for start < len(raw) && (raw[start] == ' ' || raw[start] == '\n' || raw[start] == '\r' || raw[start] == '\t') {
			start++
		}
		if start >= len(raw) || raw[start] != '{' {
			continue
		}
		end := balancedJSONObject(raw, start)
		if end < 0 {
			continue
		}
		var player map[string]json.RawMessage
		if json.Unmarshal([]byte(raw[start:end]), &player) != nil {
			continue
		}
		var playability struct {
			Status string `json:"status"`
			Reason string `json:"reason"`
		}
		json.Unmarshal(player["playabilityStatus"], &playability)
		switch playability.Status {
		case "LOGIN_REQUIRED", "AGE_CHECK_REQUIRED", "CONTENT_CHECK_REQUIRED":
			return nil, "auth_required", false
		case "UNPLAYABLE", "ERROR":
			return nil, "not_found", false
		}
		var details map[string]any
		if json.Unmarshal(player["videoDetails"], &details) != nil || details["title"] == nil {
			continue
		}
		metadata := map[string]any{"source": "public_watch_player_json", "video_details": details}
		var micro any
		if json.Unmarshal(player["microformat"], &micro) == nil {
			metadata["microformat"] = micro
		}
		var captions struct {
			Renderer struct {
				Tracks []map[string]any `json:"captionTracks"`
			} `json:"playerCaptionsTracklistRenderer"`
		}
		if json.Unmarshal(player["captions"], &captions) == nil {
			if len(captions.Renderer.Tracks) > 100 {
				captions.Renderer.Tracks = captions.Renderer.Tracks[:100]
			}
			metadata["caption_tracks"] = captions.Renderer.Tracks
		} else {
			metadata["caption_tracks"] = []any{}
		}
		encoded, e := json.Marshal(metadata)
		if e == nil && len(encoded) <= 2*1024*1024 {
			return encoded, "", true
		}
	}
	return nil, "", false
}
func balancedJSONObject(raw string, start int) int {
	depth := 0
	quoted, escaped := false, false
	for i := start; i < len(raw) && i-start <= 5*1024*1024; i++ {
		c := raw[i]
		if quoted {
			if escaped {
				escaped = false
				continue
			}
			if c == '\\' {
				escaped = true
			} else if c == '"' {
				quoted = false
			}
			continue
		}
		if c == '"' {
			quoted = true
			continue
		}
		if c == '{' {
			depth++
		}
		if c == '}' {
			depth--
			if depth == 0 {
				return i + 1
			}
		}
	}
	return -1
}
func parseThreadsMedia(rawURL string, body []byte) ([]byte, bool) {
	pathParts := strings.Split(strings.SplitN(rawURL, "?", 2)[0], "/post/")
	if len(pathParts) != 2 {
		return nil, false
	}
	code := strings.SplitN(pathParts[1], "/", 2)[0]
	raw := string(body)
	codePattern := regexp.MustCompile(`"code"\s*:\s*"` + regexp.QuoteMeta(code) + `"`)
	positions := codePattern.FindAllStringIndex(raw, -1)
	if len(positions) == 0 {
		return nil, false
	}
	blocks := regexp.MustCompile(`"video_versions"\s*:\s*\[`).FindAllStringIndex(raw, -1)
	bestStart, bestDistance := -1, len(raw)+1
	for _, block := range blocks {
		for _, position := range positions {
			distance := abs(block[0] - position[0])
			if distance < bestDistance {
				bestDistance = distance
				bestStart = block[1] - 1
			}
		}
	}
	if bestStart < 0 {
		return nil, false
	}
	depth := 0
	quoted, escaped := false, false
	end := -1
	for i := bestStart; i < len(raw); i++ {
		c := raw[i]
		if quoted {
			if escaped {
				escaped = false
				continue
			}
			if c == '\\' {
				escaped = true
			} else if c == '"' {
				quoted = false
			}
			continue
		}
		if c == '"' {
			quoted = true
			continue
		}
		if c == '[' {
			depth++
		}
		if c == ']' {
			depth--
			if depth == 0 {
				end = i + 1
				break
			}
		}
	}
	if end < 0 {
		return nil, false
	}
	var versions []struct {
		URL string `json:"url"`
	}
	if json.Unmarshal([]byte(raw[bestStart:end]), &versions) != nil {
		return nil, false
	}
	urls := []string{}
	seen := map[string]bool{}
	for _, v := range versions {
		if v.URL != "" && !seen[v.URL] {
			seen[v.URL] = true
			urls = append(urls, v.URL)
		}
	}
	if len(urls) == 0 {
		return nil, false
	}
	out, _ := json.Marshal(map[string]any{"post_code": code, "video_urls": urls})
	return out, true
}
