package bridge

import (
	"errors"
	"strings"
	"unicode"
	"unicode/utf8"
)

const replyChunkUnits = 1900

// SplitText preserves authored bytes exactly. Prefer line/word boundaries and
// keep short fenced blocks together. Long fences are not rewritten with
// synthetic close/reopen markers: individual-message rendering may differ.
func SplitText(s string) ([]string, error) {
	if !utf8.ValidString(s) || trimText(s) == "" || TextUnits(s) > 16000 {
		return nil, errors.New("invalid_reply_text")
	}
	atoms := textAtoms(s)
	for _, atom := range atoms {
		if atom.units > replyChunkUnits {
			return nil, errors.New("text_cluster_exceeds_chunk_limit")
		}
	}
	fences := shortFencedBlocks(s)
	parts := []string{}
	for start := 0; start < len(atoms); {
		end, used := start, 0
		for end < len(atoms) && used+atoms[end].units <= replyChunkUnits {
			used += atoms[end].units
			end++
		}
		if end < len(atoms) {
			// A short code block can move intact to the next message. A long
			// block cannot fit in one chunk and is intentionally left verbatim.
			for _, fence := range fences {
				if atoms[start].start < fence.start && atoms[end].start > fence.start && atoms[end].start < fence.end {
					for end > start && atoms[end].start > fence.start {
						end--
					}
					break
				}
			}
			// Prefer a late complete line, then a word. Avoid tiny chunks when
			// there is no convenient boundary in the latter half of the budget.
			line, word, units := -1, -1, 0
			for i := start; i < end; i++ {
				units += atoms[i].units
				boundary := atoms[i].end
				if units < replyChunkUnits/2 || insideFence(boundary, fences) {
					continue
				}
				if atoms[i].space {
					word = i + 1
				}
				if strings.ContainsAny(s[atoms[i].start:boundary], "\r\n") {
					line = i + 1
				}
			}
			if line > start {
				end = line
			} else if word > start {
				end = word
			}
		}
		// A hard boundary immediately before a short whitespace-only tail
		// used to reject the entire reply. Move a whole nonblank atom into the
		// tail instead, preserving whitespace and both nonempty chunks.
		if end < len(atoms) && trimText(s[atoms[end].start:]) == "" {
			last := end - 1
			for last > start && atoms[last].space {
				last--
			}
			if last > start && TextUnits(s[atoms[last].start:]) <= replyChunkUnits {
				end = last
			}
		}
		if end <= start {
			return nil, errors.New("invalid_reply_text")
		}
		part := s[atoms[start].start:atoms[end-1].end]
		if trimText(part) == "" {
			// A whitespace run longer than a complete message cannot be
			// transported losslessly without an empty Discord message.
			return nil, errors.New("whitespace_only_chunk")
		}
		parts = append(parts, part)
		start = end
	}
	return parts, nil
}

type textAtom struct {
	start, end, units int
	space             bool
}

// Conservative cluster boundaries protect common combining sequences, emoji
// variation/modifier/ZWJ/tag sequences, regional-indicator pairs, CRLF and Hangul
// syllables. This is not a complete Unicode grapheme or Markdown parser.
func textAtoms(s string) []textAtom {
	atoms := []textAtom{}
	var prev rune
	regionalRun := 0
	for i, r := range s {
		units := 1
		if r > 0xffff {
			units++
		}
		join := len(atoms) > 0 && (unicode.Is(unicode.Mn, r) || unicode.Is(unicode.Mc, r) || unicode.Is(unicode.Me, r) ||
			r == '\u200d' || prev == '\u200d' || r >= 0x1f3fb && r <= 0x1f3ff || r >= 0xe0020 && r <= 0xe007f ||
			prev == '\r' && r == '\n' || regional(r) && regional(prev) && regionalRun%2 == 1 || hangulJoined(prev, r))
		if join {
			a := &atoms[len(atoms)-1]
			a.end = i + utf8.RuneLen(r)
			a.units += units
			a.space = a.space && isTextSpace(r)
		} else {
			atoms = append(atoms, textAtom{i, i + utf8.RuneLen(r), units, isTextSpace(r)})
		}
		if regional(r) {
			regionalRun++
		} else {
			regionalRun = 0
		}
		prev = r
	}
	return atoms
}
func regional(r rune) bool { return r >= 0x1f1e6 && r <= 0x1f1ff }
func hangulClass(r rune) byte {
	switch {
	case r >= 0x1100 && r <= 0x115f || r >= 0xa960 && r <= 0xa97c:
		return 'L'
	case r >= 0x1160 && r <= 0x11a7 || r >= 0xd7b0 && r <= 0xd7c6:
		return 'V'
	case r >= 0x11a8 && r <= 0x11ff || r >= 0xd7cb && r <= 0xd7fb:
		return 'T'
	case r >= 0xac00 && r <= 0xd7a3:
		if (r-0xac00)%28 == 0 {
			return 'A' // LV
		}
		return 'B' // LVT
	}
	return 0
}
func hangulJoined(a, b rune) bool {
	x, y := hangulClass(a), hangulClass(b)
	return x == 'L' && strings.ContainsRune("LVAB", rune(y)) ||
		(x == 'A' || x == 'V') && (y == 'V' || y == 'T') || (x == 'B' || x == 'T') && y == 'T'
}

type fencedBlock struct{ start, end int }

func shortFencedBlocks(s string) []fencedBlock {
	blocks := []fencedBlock{}
	start, markerCount, offset := -1, 0, 0
	var marker byte
	for _, line := range strings.SplitAfter(s, "\n") {
		trimmed := strings.TrimLeft(line, " ")
		indent := len(line) - len(trimmed)
		if indent <= 3 && len(trimmed) >= 3 && (trimmed[0] == '`' || trimmed[0] == '~') {
			count := 0
			for count < len(trimmed) && trimmed[count] == trimmed[0] {
				count++
			}
			if count >= 3 {
				if start < 0 {
					start, marker, markerCount = offset, trimmed[0], count
				} else if trimmed[0] == marker && count >= markerCount && trimText(trimmed[count:]) == "" {
					end := offset + len(line)
					if TextUnits(s[start:end]) <= replyChunkUnits {
						blocks = append(blocks, fencedBlock{start, end})
					}
					start = -1
				}
			}
		}
		offset += len(line)
	}
	return blocks
}
func insideFence(boundary int, blocks []fencedBlock) bool {
	for _, block := range blocks {
		if boundary > block.start && boundary < block.end {
			return true
		}
	}
	return false
}
