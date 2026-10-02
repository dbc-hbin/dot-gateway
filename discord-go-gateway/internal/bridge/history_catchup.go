package bridge

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/bwmarrin/discordgo"
)

const catchupRouteLimit = 128
const catchupPageSize = 100
const catchupLiveCapacity = 900 // Reserve one page for older recovery admission.

type CatchupStatus struct {
	PageLimit   int       `json:"page_limit"`
	Generation  int64     `json:"generation"`
	ChannelID   string    `json:"channel_id"`
	Floor       string    `json:"covered_through"`
	Upper       string    `json:"window_upper,omitempty"`
	Before      string    `json:"next_before,omitempty"`
	State       string    `json:"state"`
	Code        string    `json:"code,omitempty"`
	Attempts    int       `json:"attempts"`
	NextAttempt float64   `json:"next_attempt"`
	Route       ReadRoute `json:"route"`
}

func initCatchup(db *storeConn) error {
	_, err := db.Exec(`CREATE TABLE IF NOT EXISTS catchup_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
 CREATE TABLE IF NOT EXISTS catchup_routes(channel_id TEXT PRIMARY KEY,route TEXT NOT NULL,floor TEXT NOT NULL,upper_id TEXT NOT NULL DEFAULT '',scan_before TEXT NOT NULL DEFAULT '',state TEXT NOT NULL DEFAULT 'idle',code TEXT NOT NULL DEFAULT '',attempts INTEGER NOT NULL DEFAULT 0,next_attempt REAL NOT NULL DEFAULT 0,updated REAL NOT NULL,generation INTEGER NOT NULL DEFAULT 0,page_limit INTEGER NOT NULL DEFAULT 100 CHECK(page_limit BETWEEN 1 AND 100));
 CREATE TABLE IF NOT EXISTS catchup_requested(channel_id TEXT PRIMARY KEY,upper_id TEXT NOT NULL);
 CREATE INDEX IF NOT EXISTS catchup_due ON catchup_routes(state,next_attempt,updated);`)
	return err
}
func snowAt(t time.Time) string {
	n := t.UnixMilli() - 1420070400000
	if n < 1 {
		n = 1
	}
	return strconv.FormatUint(uint64(n)<<22, 10)
}
func snowPrevious(id string) string {
	n, _ := strconv.ParseUint(id, 10, 64)
	if n <= 1 {
		return "1"
	}
	return strconv.FormatUint(n-1, 10)
}
func snowNext(id string) string {
	n, _ := strconv.ParseUint(id, 10, 64)
	return strconv.FormatUint(n+1, 10)
}
func snowCreated(id string) float64 {
	n, _ := strconv.ParseUint(id, 10, 64)
	return float64(int64(n>>22)+1420070400000) / 1000
}
func catchupEnabledDB(db *storeConn) (bool, error) {
	var state string
	err := db.QueryRow(`SELECT value FROM catchup_meta WHERE key='state'`).Scan(&state)
	if err == sql.ErrNoRows {
		return false, nil
	}
	return state == "armed", err
}
func catchupFencedDB(db *storeConn, e Envelope) (bool, error) {
	if e.Platform != "discord" || e.Control != "" {
		return false, nil
	}
	var floor, state string
	err := db.QueryRow(`SELECT floor,state FROM catchup_routes WHERE channel_id=?`, e.ConversationID).Scan(&floor, &state)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return state != "idle" && state != "disarmed" && snowLess(floor, e.EventID), nil
}
func catchupRegisterDB(db *storeConn, e Envelope, baseline string) error {
	enabled, err := catchupEnabledDB(db)
	if err != nil || !enabled {
		return err
	}
	if e.RouteKind != "dm" && e.RouteKind != "guild_text" && e.RouteKind != "guild_thread" {
		return nil
	}
	route := ReadRoute{e.ConversationID, e.GuildID, e.RouteKind, e.ParentChannelID, e.ThreadType}
	raw, err := json.Marshal(route)
	if err != nil {
		return err
	}
	// No route eviction: evicting an unresolved gap could hide lost messages.
	_, err = db.Exec(`INSERT OR IGNORE INTO catchup_routes(channel_id,route,floor,updated) SELECT ?,?,?,? WHERE (SELECT count(*) FROM catchup_routes)<?`, e.ConversationID, string(raw), baseline, epoch(), catchupRouteLimit)
	return err
}
func catchupLiveProgressDB(db *storeConn, e Envelope, outcome string) error {
	if e.Platform != "discord" || e.Control != "" || !Snowflake(e.EventID) {
		return nil
	}
	if outcome == "queue_full" {
		return requestCatchupDB(db, e.ConversationID, e.EventID, "live_queue_full")
	}

	if outcome != "validation_staged" && outcome != "duplicate" && outcome != "accepted" {
		return nil
	}
	var state string
	err := db.QueryRow(`SELECT state FROM catchup_routes WHERE channel_id=?`, e.ConversationID).Scan(&state)
	if err == sql.ErrNoRows {
		return nil
	}
	if err != nil {
		return err
	}
	if state == "gap" {
		return requestCatchupDB(db, e.ConversationID, e.EventID, "live_during_gap")
	}
	_, err = db.Exec(`UPDATE catchup_routes SET floor=?,updated=? WHERE channel_id=? AND state='idle' AND (length(floor)<length(?) OR (length(floor)=length(?) AND floor<?))`, e.EventID, epoch(), e.ConversationID, e.EventID, e.EventID, e.EventID)
	return err
}

// A receive bootstrap must not create witnesses, arm catchup, or mutate a
// restored disarm reason. Reject any runnable leftover rather than rewriting it.
func prepareGatewayCatchup(s *Store, settings Settings, now time.Time) error {
	if !settings.KeepCatchupDisarmed {
		return s.ActivateCatchup(settings, now)
	}
	_, err := s.call(func(db *storeConn) (any, error) {
		var state string
		if err := db.QueryRow(`SELECT value FROM catchup_meta WHERE key='state'`).Scan(&state); err != nil && err != sql.ErrNoRows {
			return nil, err
		}
		var runnable int
		if err := db.QueryRow(`SELECT (SELECT count(*) FROM catchup_routes WHERE state!='disarmed')+(SELECT count(*) FROM catchup_requested)`).Scan(&runnable); err != nil {
			return nil, err
		}
		if (state != "" && !strings.HasPrefix(state, "disarmed")) || runnable != 0 {
			return nil, errors.New("catchup_must_already_be_disarmed")
		}
		return nil, nil
	})
	return err
}

// ActivateCatchup is daemon-only (under the dispatcher lock). It never derives
// a cursor from inbox times. Its first baseline is now. Copied/restored ledgers
// cannot inherit activation: both a private local witness and inode/path bind it.
func (s *Store) ActivateCatchup(settings Settings, now time.Time) error {
	if settings.KeepCatchupDisarmed {
		return errors.New("catchup_activation_disabled")
	}
	path, err := filepath.Abs(settings.DBPath)
	if err != nil {
		return err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return errors.New("catchup_origin_unavailable")
	}
	origin := fmt.Sprintf("%s:%d:%d:%s:%s:%s:%s", path, st.Dev, st.Ino, settings.ExpectedBotID, settings.Policy.OwnerID, settings.Policy.GuildID, settings.Policy.GuildChannelID)
	witnessPath := path + ".catchup-live"
	_, err = s.call(func(db *storeConn) (any, error) {
		return transact(db, func(db *storeConn) (any, error) {
			// Pair this observation with the DB token under BEGIN IMMEDIATE;
			// another legitimate store may have rotated it while we waited.
			witness, readErr := readCatchupWitness(witnessPath)
			var state, saved, token string
			e := db.QueryRow(`SELECT value FROM catchup_meta WHERE key='state'`).Scan(&state)
			if e != nil && e != sql.ErrNoRows {
				return nil, e
			}
			disarm := func(code string) (any, error) {
				_, e := db.Exec(`INSERT INTO catchup_meta(key,value)VALUES('state',?) ON CONFLICT(key)DO UPDATE SET value=excluded.value;UPDATE catchup_routes SET state='disarmed',code='continuity_unverified'`, code)
				return nil, e
			}
			if state != "" {
				if state != "armed" {
					return nil, nil
				}
				if db.QueryRow(`SELECT value FROM catchup_meta WHERE key='origin'`).Scan(&saved) != nil || db.QueryRow(`SELECT value FROM catchup_meta WHERE key='witness'`).Scan(&token) != nil || saved != origin || readErr != nil || witness != token {
					return disarm("disarmed_continuity")
				}
				return nil, markCatchupGapsDB(db, snowPrevious(snowAt(now)), "cold_start")
			}
			// Defense in depth for older metadata-only backup tooling.
			var restored int
			if e := db.QueryRow(`SELECT (SELECT count(*) FROM inbound WHERE state='blocked' AND envelope='{}')+(SELECT count(*) FROM chunks WHERE code='recovery_history_no_replay')`).Scan(&restored); e != nil {
				return nil, e
			}
			if restored > 0 {
				return disarm("disarmed_restore")
			}
			// Existing witness with no ledger marker is a replaced/rolled-back DB.
			if !os.IsNotExist(readErr) {
				return disarm("disarmed_continuity")
			}
			token, e = uuidHex()
			if e != nil {
				return nil, e
			}
			if e = writeCatchupWitness(witnessPath, token); e != nil {
				return nil, e
			}
			if _, e = db.Exec(`INSERT INTO catchup_meta(key,value)VALUES('state','armed'),('origin',?),('witness',?)`, origin, token); e != nil {
				return nil, e
			}
			if settings.Policy.GuildChannelID != "" {
				e = catchupRegisterDB(db, Envelope{ConversationID: settings.Policy.GuildChannelID, GuildID: settings.Policy.GuildID, RouteKind: "guild_text"}, snowPrevious(snowAt(now.Add(time.Millisecond))))
			}
			return nil, e
		})
	})
	return err
}
func readCatchupWitness(path string) (string, error) {
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC|syscall.O_NONBLOCK, 0)
	if err != nil {
		return "", err
	}
	f := os.NewFile(uintptr(fd), path)
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return "", err
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 || int(st.Uid) != os.Getuid() || st.Nlink != 1 {
		return "", errors.New("catchup_witness_insecure")
	}
	raw, err := io.ReadAll(io.LimitReader(f, 65))
	if err != nil || len(raw) != 32 {
		return "", errors.New("catchup_witness_invalid")
	}
	return string(raw), nil
}
func writeCatchupWitness(path, token string) error {
	fd, err := syscall.Open(path, syscall.O_WRONLY|syscall.O_CREAT|syscall.O_EXCL|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0600)
	if err != nil {
		return errors.New("catchup_witness_create_failed")
	}
	f := os.NewFile(uintptr(fd), path)
	if _, err = f.WriteString(token); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
func requestCatchupDB(db *storeConn, channel, upper, code string) error {
	var state, floor string
	err := db.QueryRow(`SELECT state,floor FROM catchup_routes WHERE channel_id=?`, channel).Scan(&state, &floor)
	if err == sql.ErrNoRows {
		return nil
	}
	if err != nil {
		return err
	}
	if state == "disarmed" || !snowLess(floor, upper) {
		return nil
	}
	requested := upper
	if state == "idle" {
		upper = catchupWindowUpper(floor, upper)
		if upper != requested {
			if _, err = db.Exec(`INSERT INTO catchup_requested(channel_id,upper_id)VALUES(?,?) ON CONFLICT(channel_id)DO UPDATE SET upper_id=excluded.upper_id`, channel, requested); err != nil {
				return err
			}
		}
		_, err = db.Exec(`UPDATE catchup_routes SET state='gap',upper_id=?,scan_before=?,code=?,next_attempt=0,updated=? WHERE channel_id=?`, upper, snowNext(upper), code, epoch(), channel)
		return err
	}
	_, err = db.Exec(`INSERT INTO catchup_requested(channel_id,upper_id)VALUES(?,?) ON CONFLICT(channel_id)DO UPDATE SET upper_id=excluded.upper_id WHERE length(upper_id)<length(excluded.upper_id) OR (length(upper_id)=length(excluded.upper_id) AND upper_id<excluded.upper_id)`, channel, upper)
	return err
}
func markCatchupGapsDB(db *storeConn, upper, code string) error {
	enabled, err := catchupEnabledDB(db)
	if err != nil || !enabled {
		return err
	}
	rows, err := db.Query(`SELECT channel_id FROM catchup_routes LIMIT ?`, catchupRouteLimit)
	if err != nil {
		return err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, id := range ids {
		if err = requestCatchupDB(db, id, upper, code); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) MarkCatchupGaps(now time.Time, code string) error {
	_, err := s.call(func(db *storeConn) (any, error) {
		return nil, markCatchupGapsDB(db, snowPrevious(snowAt(now)), symbolicCode(code))
	})
	return err
}
func (s *Store) CatchupStatus() (map[string]any, error) {
	v, err := s.call(func(db *storeConn) (any, error) {
		var state string
		if err := db.QueryRow(`SELECT value FROM catchup_meta WHERE key='state'`).Scan(&state); err != nil && err != sql.ErrNoRows {
			return nil, err
		}
		if state == "" {
			state = "unarmed"
		}
		rows, err := db.Query(`SELECT channel_id,route,floor,upper_id,scan_before,state,code,attempts,next_attempt,generation,page_limit FROM catchup_routes ORDER BY channel_id LIMIT ?`, catchupRouteLimit)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		list := []CatchupStatus{}
		for rows.Next() {
			item, err := scanCatchup(rows)
			if err != nil {
				return nil, err
			}
			list = append(list, item)
		}
		return map[string]any{"state": state, "routes": list, "unregistered_routes": "uncovered", "historical_bootstrap": false}, rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return v.(map[string]any), nil
}

type rowScanner interface{ Scan(...any) error }

func scanCatchup(row rowScanner) (CatchupStatus, error) {
	var s CatchupStatus
	var raw string
	err := row.Scan(&s.ChannelID, &raw, &s.Floor, &s.Upper, &s.Before, &s.State, &s.Code, &s.Attempts, &s.NextAttempt, &s.Generation, &s.PageLimit)
	if err == nil && (json.Unmarshal([]byte(raw), &s.Route) != nil || !Snowflake(s.Floor) || s.Route.ChannelID != s.ChannelID || s.PageLimit < 1 || s.PageLimit > catchupPageSize) {
		err = errors.New("catchup_state_invalid")
	}
	return s, err
}
func (s *Store) nextCatchup(now float64) (*CatchupStatus, error) {
	v, err := s.call(func(db *storeConn) (any, error) {
		enabled, e := catchupEnabledDB(db)
		if e != nil || !enabled {
			return (*CatchupStatus)(nil), e
		}
		item, e := scanCatchup(db.QueryRow(`SELECT channel_id,route,floor,upper_id,scan_before,state,code,attempts,next_attempt,generation,page_limit FROM catchup_routes WHERE state='gap' AND next_attempt<=? ORDER BY updated,channel_id LIMIT 1`, now))
		if e == sql.ErrNoRows {
			return (*CatchupStatus)(nil), nil
		}
		if e != nil {
			return nil, e
		}
		if item.Before == "" {
			item.Before = snowNext(item.Upper)
		}
		if !Snowflake(item.Before) || !Snowflake(item.Upper) || !snowLess(item.Floor, item.Before) {
			return nil, errors.New("catchup_state_invalid")
		}
		return &item, nil
	})
	if err != nil {
		return nil, err
	}
	return v.(*CatchupStatus), nil
}
func (s *Store) deferCatchup(in CatchupStatus, code string) error {
	attempt := in.Attempts + 1
	if attempt > 5 {
		attempt = 5
	}
	delay := time.Duration(1<<attempt) * time.Second
	if delay > 30*time.Second {
		delay = 30 * time.Second
	}
	_, err := s.call(func(db *storeConn) (any, error) {
		limit := in.PageLimit
		if code == "history_body_incomplete" && limit > 1 {
			limit /= 2
			if limit < 1 {
				limit = 1
			}
		}
		_, e := db.Exec(`UPDATE catchup_routes SET code=?,page_limit=?,attempts=attempts+1,next_attempt=?,updated=? WHERE channel_id=? AND floor=? AND upper_id=? AND page_limit=? AND state='gap'`, symbolicCode(code), limit, epoch()+delay.Seconds(), epoch(), in.ChannelID, in.Floor, in.Upper, in.PageLimit)
		return nil, e
	})
	return err
}

// applyCatchupPage validates the whole response before entering this function.
// A newest-to-oldest walk first finds the oldest complete subwindow. No content
// buffer grows across pages. Admission and the covered floor commit together;
// queue_full leaves the first unadmitted event and later range resumable.
func (s *Store) applyCatchupPage(settings Settings, in CatchupStatus, messages []*discordgo.Message) (string, error) {
	if in.PageLimit < 1 || in.PageLimit > catchupPageSize || len(messages) > in.PageLimit || !Snowflake(in.Before) || !Snowflake(in.Floor) || !Snowflake(in.Upper) {
		return "", errors.New("catchup_page_invalid")
	}
	last := in.Before
	for _, m := range messages {
		if m == nil || m.Author == nil || !Snowflake(m.ID) || !Snowflake(m.Author.ID) || m.ChannelID != in.ChannelID || m.GuildID != in.Route.GuildID || !snowLess(m.ID, last) {
			return "", errors.New("catchup_page_invalid")
		}
		last = m.ID
	}
	v, err := s.call(func(db *storeConn) (any, error) {
		admitting := false
		return transact(db, func(db *storeConn) (any, error) {
			// A lost witness can be observed after a page was fetched. Disarm
			// before any source registration, then leave this page untouched.
			if err := sealCatchupDB(db); err != nil {
				return nil, err
			}
			enabled, err := catchupEnabledDB(db)
			if err != nil {
				return nil, err
			}
			if !enabled {
				return "disarmed", nil
			}
			admitting = true
			current, e := scanCatchup(db.QueryRow(`SELECT channel_id,route,floor,upper_id,scan_before,state,code,attempts,next_attempt,generation,page_limit FROM catchup_routes WHERE channel_id=?`, in.ChannelID))
			if e != nil {
				return nil, e
			}
			if current.Before == "" {
				current.Before = snowNext(current.Upper)
			}
			if current.State != "gap" || current.Floor != in.Floor || current.Upper != in.Upper || current.Before != in.Before || current.Generation != in.Generation || current.PageLimit != in.PageLimit {
				_, e = db.Exec(`UPDATE catchup_routes SET code='page_changed',next_attempt=?,updated=? WHERE channel_id=? AND state='gap'`, epoch()+1, epoch(), in.ChannelID)
				return "stale", e
			}
			if len(messages) == in.PageLimit && snowLess(in.Floor, snowPrevious(messages[len(messages)-1].ID)) {
				_, e = db.Exec(`UPDATE catchup_routes SET scan_before=?,code='page_budget_pending',attempts=0,next_attempt=0,updated=? WHERE channel_id=?`, messages[len(messages)-1].ID, epoch(), in.ChannelID)
				return "page_pending", e
			}
			floor := in.Floor
			outcome := "progress"
			for i := len(messages) - 1; i >= 0; i-- {
				m := messages[i]
				if !snowLess(floor, m.ID) {
					continue
				}
				if m.Author.ID == settings.Policy.OwnerID && !m.Author.Bot && m.WebhookID == "" && (m.Type == discordgo.MessageTypeDefault || m.Type == discordgo.MessageTypeReply) {
					event := projectGatewayMessage(settings, m)
					if s.stages(event) {
						result, e := s.stageIngressDB(db, event, snowCreated(m.ID), 1000)
						if e != nil {
							return nil, e
						}
						if result == "queue_full" {
							outcome = "queue_full"
							break
						}
					}
				}
				floor = m.ID
			}
			before := in.Before
			state := "gap"
			code := "queue_full"
			if outcome != "queue_full" {
				floor = snowPrevious(in.Before)
				before = snowNext(in.Upper)
				code = "subwindow_pending"
				if !snowLess(floor, in.Upper) {
					floor = in.Upper
					state = "idle"
					before = ""
					code = "complete"
				}
			}
			upper := in.Upper
			if state == "idle" {
				var requested string
				e = db.QueryRow(`SELECT upper_id FROM catchup_requested WHERE channel_id=?`, in.ChannelID).Scan(&requested)
				if e != nil && e != sql.ErrNoRows {
					return nil, e
				}
				if e == nil && snowLess(floor, requested) {
					upper = catchupWindowUpper(floor, requested)
					before = snowNext(upper)
					state = "gap"
					code = "next_window_pending"
				}
				if requested == "" || !snowLess(upper, requested) {
					if _, e = db.Exec(`DELETE FROM catchup_requested WHERE channel_id=?`, in.ChannelID); e != nil {
						return nil, e
					}
				}

			}
			_, e = db.Exec(`UPDATE catchup_routes SET floor=?,upper_id=?,scan_before=?,state=?,code=?,attempts=0,next_attempt=?,updated=? WHERE channel_id=?`, floor, upper, before, state, code, epoch()+1, epoch(), in.ChannelID)
			return outcome, e
		}, func(db *storeConn) error {
			if !admitting {
				return nil
			}
			return catchupAdmissionCommitGuard(db)
		})
	})
	if errors.Is(err, errCatchupContinuity) {
		// The final seal observed loss after staging. The transaction rolled
		// back all page content/progress; persist only its disarmed state.
		_, disarmErr := s.call(func(db *storeConn) (any, error) {
			return transact(db, func(db *storeConn) (any, error) { return nil, disarmCatchupDB(db) })
		})
		return "disarmed", disarmErr
	}
	if err != nil {
		return "", err
	}
	return v.(string), nil
}
func catchupLoop(ctx context.Context, s *Store, r *RESTClient, hub *WakeHub, g *gatewayState) error {
	if r.settings.KeepCatchupDisarmed {
		return nil
	}
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	var lastEpoch uint64 = ^uint64(0)
	for {
		ready, current := g.readiness()
		if ready && ctx.Err() == nil {
			if current != lastEpoch {
				if err := s.MarkCatchupGaps(time.Now(), "reconnected"); err != nil {
					return err
				}
				lastEpoch = current
				work, cancel := context.WithTimeout(ctx, 20*time.Second)
				routeErr := s.registerKnownCatchupRoutes(work, r)
				cancel()
				if routeErr != nil {
					if ctx.Err() != nil {
						return nil
					}
					if !errors.Is(routeErr, context.DeadlineExceeded) {
						return routeErr
					}
				}

			}
			// Bounded scheduling quantum; another route gets its turn after each page.
			for n := 0; n < 2; n++ {
				in, err := s.nextCatchup(epoch())
				if err != nil {
					return err
				}
				if in == nil {
					break
				}
				work, cancel := context.WithTimeout(ctx, 20*time.Second)
				messages, readErr := r.historyPage(work, in.Route, in.Before, in.PageLimit)
				cancel()
				if readErr == nil {
					still, revision := g.readiness()
					if !still || revision != current {
						readErr = errors.New("catchup_connection_changed")
					}
				}
				if readErr != nil {
					if err = s.deferCatchup(*in, readErr.Error()); err != nil {
						return err
					}
					if readErr.Error() == "preflight_http_401" {
						return errors.New("authentication_failed")
					}
				} else {
					if _, err = s.applyCatchupPage(r.settings, *in, messages); err != nil {
						return err
					}
					hub.Notify()
				}
			}
		}
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
		}
	}
}

// An edit/delete in the current cursor interval invalidates an in-flight page,
// even when its create is absent from the ledger. Covered or newer discarded
// subwindows do not invalidate it; known-source/control revocation is independent.
func (s *Store) catchupObserveMutation(channel, guild, id string) error {
	if !Snowflake(id) {
		return nil
	}
	_, err := s.call(func(db *storeConn) (any, error) {
		var floor, upper, before string
		err := db.QueryRow(`SELECT floor,upper_id,scan_before FROM catchup_routes WHERE channel_id=? AND (?='' OR json_extract(route,'$.guild_id')=?) AND state='gap'`, channel, guild, guild).Scan(&floor, &upper, &before)
		if err == sql.ErrNoRows {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		// No response content is retained across reverse pages. Mutations in a
		// later discarded subwindow will be seen when that subwindow is re-read.
		if !snowLess(floor, id) || snowLess(upper, id) || (before != "" && !snowLess(id, before)) {
			return nil, nil
		}
		_, err = db.Exec(`UPDATE catchup_routes SET generation=generation+1 WHERE channel_id=?`, channel)
		return nil, err
	})
	return err
}

// Seed route coverage, never history, from already known ledger conversations.
// Every route still needs a fresh read proof and starts at the time of that proof.
func (s *Store) registerKnownCatchupRoutes(ctx context.Context, r *RESTClient) error {
	v, err := s.call(func(db *storeConn) (any, error) {
		enabled, e := catchupEnabledDB(db)
		if e != nil || !enabled {
			return []string{}, e
		}
		rows, e := db.Query(registeredReadRoutesSQL, r.settings.Policy.OwnerID, catchupRouteLimit)
		if e != nil {
			return nil, e
		}
		defer rows.Close()
		ids := []string{}
		for rows.Next() {
			var id string
			if e = rows.Scan(&id); e != nil {
				return nil, e
			}
			ids = append(ids, id)
		}
		return ids, rows.Err()
	})
	if err != nil {
		return err
	}
	for _, id := range v.([]string) {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		route, e := r.ResolveReadRoute(ctx, s, id)
		if e != nil {
			if e.Error() == "preflight_http_401" {
				return errors.New("authentication_failed")
			}
			continue
		}
		_, e = s.call(func(db *storeConn) (any, error) {
			return nil, catchupRegisterDB(db, route.envelope(), snowPrevious(snowAt(time.Now().Add(time.Millisecond))))
		})
		if e != nil {
			return e
		}
	}
	return nil
}
func (s *Store) recordCatchupEventGap(channel, id string) error {
	if !Snowflake(channel) || !Snowflake(id) {
		return nil
	}
	_, err := s.call(func(db *storeConn) (any, error) {
		return nil, requestCatchupDB(db, channel, id, "gateway_event_overflow")
	})
	return err
}

func catchupEarlierPendingDB(db *storeConn, e Envelope, now float64) (bool, error) {
	if e.Platform != "discord" || e.Control != "" {
		return false, nil
	}
	var count int
	err := db.QueryRow(catchupEarlierPendingSQL, e.ConversationID, e.ConversationID, now, e.EventID, e.EventID, e.EventID).Scan(&count)
	return count > 0, err
}

// Fixed prospective windows are at most one hour; the remaining requested end
// is durable and becomes a successor window rather than being silently dropped.
func catchupWindowUpper(floor, target string) string {
	lo, _ := strconv.ParseUint(floor, 10, 64)
	hi, _ := strconv.ParseUint(target, 10, 64)
	const span = uint64(time.Hour/time.Millisecond) << 22
	if hi > lo && hi-lo > span {
		return strconv.FormatUint(lo+span, 10)
	}
	return target
}

// A floor seal is rotated in the same SQLite transaction as admission/floor
// progress. The external witness is fsynced first: a crash between witness and
// COMMIT fails closed at next open. An in-place older ledger rollback cannot
// replay an already eligible ID even when its path and inode are unchanged.
func sealCatchupDB(db *storeConn) error {
	enabled, err := catchupEnabledDB(db)
	if err != nil || !enabled {
		return err
	}
	var token, origin, previous string
	if err = db.QueryRow(`SELECT value FROM catchup_meta WHERE key='witness'`).Scan(&token); err != nil {
		return err
	}
	if err = db.QueryRow(`SELECT value FROM catchup_meta WHERE key='origin'`).Scan(&origin); err != nil {
		return err
	}
	witness, readErr := readCatchupWitness(db.path + ".catchup-live")
	info, statErr := os.Lstat(db.path)
	bound := false
	if statErr == nil {
		if st, ok := info.Sys().(*syscall.Stat_t); ok {
			bound = strings.HasPrefix(origin, fmt.Sprintf("%s:%d:%d:", db.path, st.Dev, st.Ino))
		}
	}
	if readErr != nil || witness != token || !bound {
		_, err = db.Exec(`UPDATE catchup_meta SET value='disarmed_continuity' WHERE key='state';UPDATE catchup_routes SET state='disarmed',code='continuity_unverified'`)
		return err
	}
	rows, err := db.Query(`SELECT channel_id,route,floor FROM catchup_routes ORDER BY channel_id`)
	if err != nil {
		return err
	}
	h := sha256.New()
	for rows.Next() {
		var id, route, floor string
		if err = rows.Scan(&id, &route, &floor); err != nil {
			rows.Close()
			return err
		}
		raw, _ := json.Marshal([]string{id, route, floor})
		h.Write(raw)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	digest := hex.EncodeToString(h.Sum(nil))
	err = db.QueryRow(`SELECT value FROM catchup_meta WHERE key='floor_seal'`).Scan(&previous)
	if err != nil && err != sql.ErrNoRows {
		return err
	}
	if previous == digest {
		return nil
	}
	next, err := uuidHex()
	if err != nil {
		return err
	}
	if err = rotateCatchupWitness(db.path+".catchup-live", next); err != nil {
		return err
	}
	_, err = db.Exec(`UPDATE catchup_meta SET value=? WHERE key='witness';INSERT INTO catchup_meta(key,value)VALUES('floor_seal',?) ON CONFLICT(key)DO UPDATE SET value=excluded.value`, next, digest)
	return err
}
func rotateCatchupWitness(path, token string) error {
	nonce, err := uuidHex()
	if err != nil {
		return err
	}
	temp := path + "." + nonce
	defer os.Remove(temp)
	fd, err := syscall.Open(temp, syscall.O_WRONLY|syscall.O_CREAT|syscall.O_EXCL|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0600)
	if err != nil {
		return errors.New("catchup_witness_write_failed")
	}
	f := os.NewFile(uintptr(fd), temp)
	if _, err = f.WriteString(token); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if err = os.Rename(temp, path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

var errCatchupContinuity = errors.New("catchup_continuity_lost")

func catchupAdmissionCommitGuard(db *storeConn) error {
	enabled, err := catchupEnabledDB(db)
	if err != nil {
		return err
	}
	if !enabled {
		return errCatchupContinuity
	}
	return nil
}
func disarmCatchupDB(db *storeConn) error {
	_, err := db.Exec(`UPDATE catchup_meta SET value='disarmed_continuity' WHERE key='state';UPDATE catchup_routes SET state='disarmed',code='continuity_unverified'`)
	return err
}

const registeredReadRoutesSQL = `SELECT channel_id FROM read_route_registry WHERE owner_id=? AND channel_id NOT IN(SELECT channel_id FROM catchup_routes) ORDER BY seen DESC LIMIT ?`

const catchupEarlierPendingSQL = `SELECT EXISTS(SELECT 1 FROM inbound older INDEXED BY inbound_active_source_order WHERE json_valid(older.envelope) AND older.state IN ('pending','claimed') AND EXISTS(SELECT 1 FROM catchup_routes h WHERE h.channel_id=?) AND older.platform='discord' AND json_extract(older.envelope,'$.conversation_id')=? AND COALESCE(json_extract(older.envelope,'$.control'),'')='' AND (older.state='pending' OR (older.state='claimed' AND older.lease_until<=?)) AND NOT EXISTS(SELECT 1 FROM message_sources src WHERE src.platform=older.platform AND src.event_id=json_extract(older.envelope,'$.event_id') AND (src.state!='current' OR src.revision!=COALESCE(json_extract(older.envelope,'$.source_revision'),0))) AND (length(json_extract(older.envelope,'$.event_id'))<length(?) OR (length(json_extract(older.envelope,'$.event_id'))=length(?) AND json_extract(older.envelope,'$.event_id')<?)) LIMIT 1)`
