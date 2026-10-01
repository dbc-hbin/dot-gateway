package collector

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type State struct {
	Fields       map[string]json.RawMessage
	Fingerprints map[string]string
	Order        []string
}

func NewState() *State {
	return &State{Fields: map[string]json.RawMessage{"version": json.RawMessage("1")}, Fingerprints: map[string]string{}, Order: []string{}}
}
func (s *State) Set(key, value string) {
	if _, ok := s.Fingerprints[key]; !ok {
		s.Order = append(s.Order, key)
	}
	s.Fingerprints[key] = value
}
func (s *State) Trim(n int) {
	if len(s.Order) <= n {
		return
	}
	for _, k := range s.Order[:len(s.Order)-n] {
		delete(s.Fingerprints, k)
	}
	s.Order = s.Order[len(s.Order)-n:]
}
func DecodeState(data []byte) *State {
	var fields map[string]json.RawMessage
	if json.Unmarshal(data, &fields) != nil || fields == nil {
		return NewState()
	}
	raw, ok := fields["fingerprints"]
	if !ok || len(raw) == 0 || raw[0] != '{' {
		return NewState()
	}
	var dict map[string]string
	if json.Unmarshal(raw, &dict) != nil {
		return NewState()
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	if _, e := d.Token(); e != nil {
		return NewState()
	}
	s := &State{Fields: fields, Fingerprints: map[string]string{}, Order: []string{}}
	for d.More() {
		k, e := d.Token()
		if e != nil {
			return NewState()
		}
		var v string
		if d.Decode(&v) != nil {
			return NewState()
		}
		s.Set(k.(string), v)
	}
	return s
}
func LoadState(path string) *State {
	f, e := os.Open(path)
	if e != nil {
		return NewState()
	}
	defer f.Close()
	b, e := io.ReadAll(io.LimitReader(f, 16*1024*1024+1))
	if e != nil || len(b) > 16*1024*1024 {
		return NewState()
	}
	return DecodeState(b)
}
func (s *State) Bytes() ([]byte, error) {
	fields := map[string]json.RawMessage{}
	for k, v := range s.Fields {
		fields[k] = v
	}
	b, e := json.Marshal(s.Fingerprints)
	if e != nil {
		return nil, e
	}
	fields["fingerprints"] = b
	var out bytes.Buffer
	enc := json.NewEncoder(&out)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if e = enc.Encode(fields); e != nil {
		return nil, e
	}
	return out.Bytes(), nil
}
func (s *State) Save(path string) error {
	b, e := s.Bytes()
	if e != nil {
		return e
	}
	return AtomicWrite(path, b)
}
func AtomicWrite(path string, b []byte) error {
	dir := filepath.Dir(path)
	if e := os.MkdirAll(dir, 0700); e != nil {
		return e
	}
	f, e := os.CreateTemp(dir, ".insane-state-*.tmp")
	if e != nil {
		return e
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if e = f.Chmod(0600); e != nil {
		f.Close()
		return e
	}
	if _, e = f.Write(b); e != nil {
		f.Close()
		return e
	}
	if e = f.Sync(); e != nil {
		f.Close()
		return e
	}
	if e = f.Close(); e != nil {
		return e
	}
	if e = os.Rename(tmp, path); e != nil {
		return fmt.Errorf("atomic rename: %w", e)
	}
	return nil
}

// LoadStateStrict is the deployed CLI's fail-closed boundary. The compatibility
// decoder above intentionally mirrors the original for differential fixtures;
// an existing broken state must never be silently replaced in a live run.
func LoadStateStrict(path string) (*State, error) {
	f, e := os.Open(path)
	if os.IsNotExist(e) {
		return NewState(), nil
	}
	if e != nil {
		return nil, fmt.Errorf("read existing state: %w", e)
	}
	defer f.Close()
	b, e := io.ReadAll(io.LimitReader(f, 16*1024*1024+1))
	if e != nil {
		return nil, e
	}
	if len(b) > 16*1024*1024 {
		return nil, fmt.Errorf("existing state exceeds 16 MiB; refusing overwrite")
	}
	var fields map[string]json.RawMessage
	if e = json.Unmarshal(b, &fields); e != nil {
		return nil, fmt.Errorf("invalid existing state; refusing overwrite: %w", e)
	}
	raw, ok := fields["fingerprints"]
	if !ok || len(raw) == 0 || raw[0] != '{' {
		return nil, fmt.Errorf("existing state has no fingerprint object; refusing overwrite")
	}
	var typed map[string]json.RawMessage
	if e = json.Unmarshal(raw, &typed); e != nil {
		return nil, fmt.Errorf("existing fingerprint object is invalid: %w", e)
	}
	for _, value := range typed {
		if len(value) == 0 || value[0] != '"' {
			return nil, fmt.Errorf("existing fingerprint is not a string; refusing overwrite")
		}
	}
	var values map[string]string
	if e = json.Unmarshal(raw, &values); e != nil {
		return nil, fmt.Errorf("existing state fingerprints are invalid; refusing overwrite: %w", e)
	}
	return DecodeState(b), nil
}
