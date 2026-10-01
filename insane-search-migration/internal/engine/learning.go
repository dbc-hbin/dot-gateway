package engine

import (
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"
)

type LearnedRoute struct {
	Transform   string `json:"transform"`
	Impersonate string `json:"impersonate"`
	Referer     string `json:"referer"`
	Phase       string `json:"phase"`
}
type LearnedEntry struct {
	Route            LearnedRoute `json:"route"`
	Wins             int          `json:"wins"`
	ConsecutiveFails int          `json:"consecutive_fails"`
	LastUsed         string       `json:"last_used"`
	LastSuccess      string       `json:"last_success"`
}
type LearningStore struct {
	Path       string
	TTL        time.Duration
	MaxEntries int
	Now        func() time.Time
}

func NewLearningStore(path string) *LearningStore {
	return &LearningStore{Path: path, TTL: 30 * 24 * time.Hour, MaxEntries: 500}
}
func (s *LearningStore) now() time.Time {
	if s.Now != nil {
		return s.Now().UTC()
	}
	return time.Now().UTC()
}
func learningKey(raw, device string) string {
	u, _ := url.Parse(raw)
	host := ""
	if u != nil {
		host = strings.ToLower(u.Hostname())
	}
	dev := "desktop"
	if device == "mobile" {
		dev = "mobile"
	}
	return host + "::" + dev
}
func (s *LearningStore) prune(data map[string]LearnedEntry) map[string]LearnedEntry {
	now := s.now()
	ttl := s.TTL
	if ttl <= 0 {
		ttl = 30 * 24 * time.Hour
	}
	out := map[string]LearnedEntry{}
	for k, v := range data {
		last, e := time.Parse(time.RFC3339Nano, v.LastUsed)
		if e == nil && last.Before(now.Add(-ttl)) {
			continue
		}
		out[k] = v
	}
	maximum := s.MaxEntries
	if maximum <= 0 {
		maximum = 500
	}
	if len(out) > maximum {
		keys := make([]string, 0, len(out))
		for k := range out {
			keys = append(keys, k)
		}
		sort.SliceStable(keys, func(i, j int) bool {
			a, e := time.Parse(time.RFC3339Nano, out[keys[i]].LastUsed)
			if e != nil {
				a = now
			}
			b, e := time.Parse(time.RFC3339Nano, out[keys[j]].LastUsed)
			if e != nil {
				b = now
			}
			if a.Equal(b) {
				return keys[i] < keys[j]
			}
			return a.After(b)
		})
		for _, k := range keys[maximum:] {
			delete(out, k)
		}
	}
	return out
}
func (s *LearningStore) load() (map[string]LearnedEntry, error) {
	f, e := os.Open(s.Path)
	if os.IsNotExist(e) {
		return map[string]LearnedEntry{}, nil
	}
	if e != nil {
		return nil, e
	}
	defer f.Close()
	data, e := io.ReadAll(io.LimitReader(f, 2*1024*1024+1))
	if e != nil {
		return nil, e
	}
	if len(data) > 2*1024*1024 {
		return nil, fmt.Errorf("learning data exceeds 2 MiB")
	}
	var state map[string]LearnedEntry
	if e = json.Unmarshal(data, &state); e != nil {
		return nil, e
	}
	if state == nil {
		state = map[string]LearnedEntry{}
	}
	return s.prune(state), nil
}
func (s *LearningStore) Lookup(raw, device string) (LearnedRoute, bool, error) {
	state, e := s.load()
	if e != nil {
		return LearnedRoute{}, false, e
	}
	entry, ok := state[learningKey(raw, device)]
	if _, known := tlsProfiles[entry.Route.Impersonate]; !known {
		ok = false
	}
	return entry.Route, ok, nil
}
func (s *LearningStore) update(fn func(map[string]LearnedEntry)) error {
	if e := os.MkdirAll(filepath.Dir(s.Path), 0700); e != nil {
		return e
	}
	lock, e := os.OpenFile(s.Path+".lock", os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		return e
	}
	defer lock.Close()
	if e = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); e != nil {
		return e
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	state, e := s.load()
	if e != nil {
		return e
	}
	fn(state)
	state = s.prune(state)
	data, e := json.MarshalIndent(state, "", "  ")
	if e != nil {
		return e
	}
	tmp, e := os.CreateTemp(filepath.Dir(s.Path), ".learned-*.tmp")
	if e != nil {
		return e
	}
	name := tmp.Name()
	defer os.Remove(name)
	if e = tmp.Chmod(0600); e != nil {
		tmp.Close()
		return e
	}
	if _, e = tmp.Write(data); e != nil {
		tmp.Close()
		return e
	}
	if e = tmp.Sync(); e != nil {
		tmp.Close()
		return e
	}
	if e = tmp.Close(); e != nil {
		return e
	}
	return os.Rename(name, s.Path)
}
func (s *LearningStore) Success(raw, device string, route LearnedRoute) error {
	return s.update(func(state map[string]LearnedEntry) {
		key := learningKey(raw, device)
		prior := state[key]
		wins := 1
		if prior.Route == route {
			wins = prior.Wins + 1
		}
		now := s.now().Format(time.RFC3339Nano)
		state[key] = LearnedEntry{Route: route, Wins: wins, LastUsed: now, LastSuccess: now}
	})
}
func (s *LearningStore) Failure(raw, device, reason string) error {
	return s.update(func(state map[string]LearnedEntry) {
		key := learningKey(raw, device)
		entry, ok := state[key]
		if !ok {
			return
		}
		if reason == "exhausted" || reason == "challenge" || reason == "blocked" {
			entry.ConsecutiveFails++
		}
		entry.LastUsed = s.now().Format(time.RFC3339Nano)
		if entry.ConsecutiveFails >= 2 {
			delete(state, key)
		} else {
			state[key] = entry
		}
	})
}
