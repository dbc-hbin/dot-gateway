package reporting

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"syscall"
)

// configure-source replaces this sentinel and the synthetic target below using
// the operator's explicit state root and validated private snapshot. They are
// compiled in: --state-dir and environment variables cannot select another root.
// A configured deployment requires matching fences at the root and reports/.
// Collector/outbox state outside this root must be reconciled into these fences
// before authorization; this guard does not discover arbitrary external roots.
const restoredGatewayRoot = "/opt/assistant-recovery/UNCONFIGURED"

func recoveredLiveTarget() Target {
	return Target{BotID: "100000000000000001", GuildID: "100000000000000002", ChannelID: "100000000000000003"}
}

type recoveredDelivery struct {
	Label                          string   `json:"label"`
	RunID                          string   `json:"run_id"`
	RunIDBasis                     string   `json:"run_id_basis"`
	CanonicalURLs                  []string `json:"canonical_urls"`
	SourceURL                      string   `json:"source_url"`
	ExpectedContentHash            string   `json:"expected_content_sha256"`
	RetainedOriginalPayloadHash    string   `json:"retained_original_payload_sha256,omitempty"`
	RetainedPayloadHashBasis       string   `json:"retained_payload_sha256_basis,omitempty"`
	RetainedOriginalGetConfirmedAt string   `json:"retained_original_get_confirmed_at,omitempty"`
	MessageID                      string   `json:"message_id"`
	MessageURL                     string   `json:"message_url"`
	DiscordCreatedAt               string   `json:"discord_created_at"`
	DiscordContentHash             string   `json:"discord_content_sha256"`
	DiscordContentBytes            int      `json:"discord_content_bytes"`
	OriginalNonceRecovered         bool     `json:"original_nonce_recovered"`
	OrdinaryPublisherReconstructed bool     `json:"ordinary_publisher_receipt_reconstructed"`
	DoNotReannounce                bool     `json:"do_not_reannounce"`
}

type recoveredEmptyRun struct {
	RunID       string `json:"run_id"`
	Scanned     int    `json:"scanned"`
	Candidates  int    `json:"candidates"`
	Accepted    int    `json:"accepted"`
	DiscordPost bool   `json:"discord_post"`
	Basis       string `json:"basis"`
}

type recoveryFence struct {
	Schema                    int                 `json:"schema"`
	Target                    Target              `json:"target"`
	Basis                     string              `json:"basis"`
	Deliveries                []recoveredDelivery `json:"recovered_deliveries"`
	BlockedRunIDs             []string            `json:"blocked_run_ids"`
	LatestEmptyRun            *recoveredEmptyRun  `json:"latest_empty_run"`
	FirstNotificationAccepted string              `json:"first_chatgpt_notification_already_accepted"`
	Unrecoverable             []string            `json:"unrecoverable_from_discord"`
	OriginalReceiptsComplete  bool                `json:"original_publisher_receipts_complete"`
	DeliveryEnabled           *bool               `json:"delivery_enabled"`
	ActivationAuthorized      *bool               `json:"activation_authorized"`
}

var recoveryHashPattern = regexp.MustCompile(`^[a-f0-9]{64}$`)
var recoveryURLPattern = regexp.MustCompile("(?i)https?://[^\\s<>\"'`]+")

// Recovery keys intentionally ignore fragments, query/tracking strings, scheme,
// default ports and trailing slashes. False-positive blocks are safer than
// reannouncing a recovered delivery using a cosmetically changed URL.
func recoveryURLKey(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil || (strings.ToLower(u.Scheme) != "https" && strings.ToLower(u.Scheme) != "http") || u.Hostname() == "" || u.User != nil || u.Opaque != "" {
		return "", errors.New("invalid_recovery_url")
	}
	host := strings.ToLower(u.Hostname())
	if port := u.Port(); port != "" && port != "443" && port != "80" {
		host += ":" + port
	}
	return host + strings.TrimSuffix(path.Clean("/"+u.Path), "/"), nil
}

// Disallow duplicate keys as well as unknown fields: JSON's normal last-value
// behavior must not let a malformed fence override an earlier blocking value.
func uniqueRecoveryJSON(b []byte) bool {
	d := json.NewDecoder(bytes.NewReader(b))
	var value func(int) bool
	value = func(depth int) bool {
		if depth > 32 {
			return false
		}
		token, err := d.Token()
		if err != nil {
			return false
		}
		delim, ok := token.(json.Delim)
		if !ok {
			return true
		}
		switch delim {
		case '{':
			keys := map[string]bool{}
			for d.More() {
				token, err = d.Token()
				key, ok := token.(string)
				if err != nil || !ok || keys[strings.ToLower(key)] {
					return false
				}
				keys[strings.ToLower(key)] = true
				if !value(depth + 1) {
					return false
				}
			}
			end, err := d.Token()
			return err == nil && end == json.Delim('}')
		case '[':
			for d.More() {
				if !value(depth + 1) {
					return false
				}
			}
			end, err := d.Token()
			return err == nil && end == json.Delim(']')
		}
		return false
	}
	if !value(0) {
		return false
	}
	_, err := d.Token()
	return err == io.EOF
}

func decodeRecoveryFence(b []byte, target Target) (recoveryFence, error) {
	var f recoveryFence
	invalid := errors.New("invalid_recovery_fence")
	if !uniqueRecoveryJSON(b) || strictJSON(b, &f) != nil || f.Schema != 1 || f.Target.Validate() != nil || f.Target != target || f.Deliveries == nil || f.BlockedRunIDs == nil || f.DeliveryEnabled == nil || f.ActivationAuthorized == nil {
		return f, invalid
	}
	for _, id := range f.BlockedRunIDs {
		if !runPattern.MatchString(id) {
			return f, invalid
		}
	}
	if r := f.LatestEmptyRun; r != nil && (!runPattern.MatchString(r.RunID) || r.Scanned < 0 || r.Candidates < 0 || r.Accepted != 0 || r.DiscordPost) {
		return f, invalid
	}
	for _, d := range f.Deliveries {
		if !runPattern.MatchString(d.RunID) || !d.DoNotReannounce || len(d.CanonicalURLs) == 0 || !recoveryHashPattern.MatchString(d.ExpectedContentHash) || !recoveryHashPattern.MatchString(d.DiscordContentHash) || (d.RetainedOriginalPayloadHash != "" && !recoveryHashPattern.MatchString(d.RetainedOriginalPayloadHash)) || !snowflake(d.MessageID) || d.MessageURL != messageURL(target, d.MessageID) || !validStamp(d.DiscordCreatedAt) || d.DiscordContentBytes <= 0 {
			return f, invalid
		}
		for _, raw := range append(append([]string{}, d.CanonicalURLs...), d.SourceURL) {
			if _, err := recoveryURLKey(raw); err != nil {
				return f, invalid
			}
		}
	}
	return f, nil
}

// Recovery reads never create directories/locks and reject symlinks in every
// directory component, plus the existing private, regular, single-link rules.
func openRecoveryDirectory(dir string) (*os.File, error) {
	fd, err := syscall.Open("/", syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_CLOEXEC, 0)
	if err != nil {
		return nil, errors.New("recovery_directory_open_failed")
	}
	for _, part := range strings.Split(strings.TrimPrefix(dir, "/"), "/") {
		if part == "" {
			continue
		}
		next, err := syscall.Openat(fd, part, syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
		syscall.Close(fd)
		if err == syscall.ENOENT {
			return nil, os.ErrNotExist
		}
		if err != nil {
			return nil, errors.New("recovery_directory_open_failed")
		}
		fd = next
	}
	return os.NewFile(uintptr(fd), "recovery-directory"), nil
}

type recoveryGuard struct {
	stateDir string
	target   Target
	// Retain discovered requirements across checks, even if markers disappear.
	roots map[string]string
}

func newRecoveryGuard(stateDir string, target Target) (*recoveryGuard, error) {
	abs, err := filepath.Abs(stateDir)
	if err != nil || abs == "/" {
		return nil, errors.New("invalid_state_directory")
	}
	g := &recoveryGuard{stateDir: abs, target: target, roots: map[string]string{}}
	if target == recoveredLiveTarget() {
		g.roots[restoredGatewayRoot] = filepath.Join(restoredGatewayRoot, "reports")
	}
	return g, nil
}

// Marker bodies are not authorization. Only their safe presence is needed to
// require the separately validated fences; snapshots may exceed the 256 KiB
// publisher ledger limit. Still reject unsafe, linked, or unreadable markers.
func recoveryMarkerPresent(dir *os.File, name string) error {
	fd, err := syscall.Openat(int(dir.Fd()), name, syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC|syscall.O_NONBLOCK, 0)
	if err == syscall.ENOENT {
		return os.ErrNotExist
	}
	if err != nil {
		return errors.New("recovery_marker_open_failed")
	}
	f := os.NewFile(uintptr(fd), "recovery-marker")
	defer f.Close()
	if !privateRegular(f) {
		return errors.New("insecure_recovery_marker")
	}
	return nil
}

func (g *recoveryGuard) discover() error {
	for dir := g.stateDir; ; dir = filepath.Dir(dir) {
		f, err := openRecoveryDirectory(dir)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if err == nil {
			s := &store{dir: f}
			for _, name := range []string{"RECOVERY_BLOCK.json", "RECOVERED_DELIVERY_FENCES.json", "RECOVERY_STATE.json", "RESTORED_SNAPSHOT.json", "POST_SNAPSHOT_RECONCILIATION_REQUIRED.json", "recovery-no-replay.json"} {
				err := recoveryMarkerPresent(f, name)
				if err == nil && (name == "RECOVERED_DELIVERY_FENCES.json" || name == "recovery-no-replay.json") {
					_, err = s.read(name)
				}
				if errors.Is(err, os.ErrNotExist) {
					continue
				}
				if name == "RECOVERY_BLOCK.json" {
					f.Close()
					return errors.New("recovery_publish_blocked")
				}
				if err != nil {
					f.Close()
					return errors.New("recovery_fence_read_failed")
				}
				root, local := dir, g.stateDir
				if name == "recovery-no-replay.json" {
					root, local = filepath.Dir(dir), dir
				}
				// Never replace a compile-time requirement (or a previously
				// discovered requirement) with a caller-selected state directory.
				if _, exists := g.roots[root]; !exists {
					g.roots[root] = local
				}
			}
			f.Close()
		}
		if dir == "/" {
			break
		}
	}
	return nil
}

func readRequiredRecoveryFence(dir, name string, target Target) (recoveryFence, error) {
	var fence recoveryFence
	f, err := openRecoveryDirectory(dir)
	if err != nil {
		return fence, errors.New("recovery_fence_missing_or_unreadable")
	}
	defer f.Close()
	s := &store{dir: f}
	if err := recoveryMarkerPresent(f, "RECOVERY_BLOCK.json"); !errors.Is(err, os.ErrNotExist) {
		return fence, errors.New("recovery_publish_blocked")
	}
	b, err := s.read(name)
	if err != nil {
		return fence, errors.New("recovery_fence_missing_or_unreadable")
	}
	return decodeRecoveryFence(b, target)
}

func (g *recoveryGuard) check(runID string, report []byte, chunks []string) error {
	if err := g.discover(); err != nil {
		return err
	}
	hashes := map[string]bool{hashBytes(report): true, hashBytes(bytes.TrimSpace(report)): true}
	for _, chunk := range chunks {
		hashes[hashBytes([]byte(chunk))] = true
	}
	urls := map[string]bool{}
	for _, raw := range recoveryURLPattern.FindAllString(string(report), -1) {
		if key, err := recoveryURLKey(strings.TrimRight(raw, ").,;:!?]}")); err == nil {
			urls[key] = true
		}
	}
	for root, local := range g.roots {
		f, err := readRequiredRecoveryFence(root, "RECOVERED_DELIVERY_FENCES.json", g.target)
		if err != nil {
			return err
		}
		copy, err := readRequiredRecoveryFence(local, "recovery-no-replay.json", g.target)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(f, copy) {
			return errors.New("recovery_fence_copies_mismatch")
		}
		if !*f.DeliveryEnabled || !*f.ActivationAuthorized {
			return errors.New("recovery_delivery_not_authorized")
		}
		for _, blocked := range f.BlockedRunIDs {
			if runID == blocked {
				return errors.New("recovered_run_no_replay")
			}
		}
		if f.LatestEmptyRun != nil && f.LatestEmptyRun.RunID == runID {
			return errors.New("recovered_run_no_replay")
		}
		for _, d := range f.Deliveries {
			if d.RunID == runID {
				return errors.New("recovered_run_no_replay")
			}
			if hashes[d.ExpectedContentHash] || hashes[d.DiscordContentHash] || hashes[d.RetainedOriginalPayloadHash] {
				return errors.New("recovered_content_no_replay")
			}
			for _, raw := range append(append([]string{}, d.CanonicalURLs...), d.SourceURL) {
				key, _ := recoveryURLKey(raw) // Already validated above.
				if urls[key] {
					return errors.New("recovered_url_no_replay")
				}
			}
		}
	}
	return nil
}
