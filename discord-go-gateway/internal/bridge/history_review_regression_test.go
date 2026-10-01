package bridge

// Regression fixtures supplied by independent review of the initial candidate.
import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/bwmarrin/discordgo"
	"net/http"
	"os"
	"sync/atomic"
	"testing"
	"time"
)

func TestReviewerReadDenialObservedByFinalChannelProof(t *testing.T) {
	s, _ := threadStore(t)
	var served, denied atomic.Bool
	r := readREST(t, func(w http.ResponseWriter, q *http.Request) bool {
		switch q.URL.Path {
		case "/channels/2/messages/90":
			served.Store(true)
			json.NewEncoder(w).Encode(readFixtureMessage("90", "2", "1"))
			return true
		case "/guilds/5/roles":
			if served.Load() {
				denied.Store(true)
			}
			json.NewEncoder(w).Encode([]any{map[string]any{"id": "5", "permissions": fmt.Sprint(permissionViewChannel | permissionReadHistory)}})
			return true
		case "/channels/2":
			p := parentJSON()
			if denied.Load() {
				p["permission_overwrites"] = []any{map[string]any{"id": "5", "type": 0, "allow": "0", "deny": fmt.Sprint(permissionReadHistory)}}
			}
			json.NewEncoder(w).Encode(p)
			return true
		}
		return false
	})
	out, err := r.ReadMessage(context.Background(), s, "2", "90")
	if err == nil || len(out.Messages) != 0 {
		t.Fatalf("returned content after final channel proof visibly denied READ_MESSAGE_HISTORY: err=%v returned=%d", err, len(out.Messages))
	}
}

func TestReviewerContinuityLostBeforeApplyCannotAdmit(t *testing.T) {
	s, cfg, _ := catchupFixture(t)
	floor := catchupRow(t, s).Floor
	in := catchupForceGap(t, s, catchupAdd(floor, 10))
	if err := os.WriteFile(cfg.DBPath+".catchup-live", []byte("00000000000000000000000000000000"), 0600); err != nil {
		t.Fatal(err)
	}
	out, err := s.applyCatchupPage(cfg, *in, []*discordgo.Message{catchupMessage(t, in.Route, catchupAdd(floor, 1), "1")})
	if err != nil {
		t.Logf("failed closed: %v", err)
		return
	}
	var n int
	_, err = s.call(func(db *storeConn) (any, error) {
		e := db.QueryRow("SELECT count(*) FROM ingress_validation").Scan(&n)
		return nil, e
	})
	if err != nil {
		t.Fatal(err)
	}
	st, _ := s.CatchupStatus()
	if n != 0 {
		t.Fatalf("untrusted-continuity page still admitted: outcome=%s quarantine=%d catchup_state=%v", out, n, st["state"])
	}
}

func TestReadFinalThreadParentProofDenialAndMissingACL(t *testing.T) {
	for _, malformed := range []bool{false, true} {
		t.Run(fmt.Sprint(malformed), func(t *testing.T) {
			s, _ := threadStore(t)
			e := threadEnvelope()
			s.StageIngress(e)
			in, _ := s.NextValidation(epoch())
			if _, err := s.promoteThreadValidation(*in, e); err != nil {
				t.Fatal(err)
			}
			served, denied := false, false
			r := readREST(t, func(w http.ResponseWriter, q *http.Request) bool {
				switch q.URL.Path {
				case "/channels/6/messages/90":
					served = true
					json.NewEncoder(w).Encode(readFixtureMessage("90", "6", "1"))
					return true
				case "/guilds/5/roles":
					if served {
						denied = true
					}
					json.NewEncoder(w).Encode([]any{map[string]any{"id": "5", "permissions": fmt.Sprint(permissionViewChannel | permissionReadHistory)}})
					return true
				case "/channels/2":
					p := parentJSON()
					if denied {
						if malformed {
							delete(p, "permission_overwrites")
						} else {
							p["permission_overwrites"] = []any{map[string]any{"id": "5", "type": 0, "allow": "0", "deny": fmt.Sprint(permissionReadHistory)}}
						}
					}
					json.NewEncoder(w).Encode(p)
					return true
				}
				return false
			})
			out, err := r.ReadMessage(context.Background(), s, "6", "90")
			if err == nil || len(out.Messages) != 0 {
				t.Fatal(out, err)
			}
		})
	}
}

func TestReviewerWitnessRotationSQLFailureFailsClosed(t *testing.T) {
	s, cfg, now := catchupFixture(t)
	floor := catchupRow(t, s).Floor
	in := catchupForceGap(t, s, catchupAdd(floor, 10))
	_, err := s.call(func(db *storeConn) (any, error) {
		_, e := db.Exec(`CREATE TRIGGER reviewer_fail_seal BEFORE UPDATE OF value ON catchup_meta WHEN OLD.key='witness' BEGIN SELECT RAISE(ABORT,'reviewer_seal_failure'); END`)
		return nil, e
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.applyCatchupPage(cfg, *in, []*discordgo.Message{catchupMessage(t, in.Route, catchupAdd(floor, 1), "1")}); err == nil {
		t.Fatal("failed seal committed")
	}
	var n int
	_, err = s.call(func(db *storeConn) (any, error) {
		e := db.QueryRow("SELECT count(*) FROM ingress_validation").Scan(&n)
		return nil, e
	})
	if err != nil || n != 0 {
		t.Fatalf("partial admission survived err=%v n=%d", err, n)
	}
	if row := catchupRow(t, s); row.Floor != floor {
		t.Fatal("floor advanced on failed seal")
	}
	s.Close()
	reopened, err := OpenStore(cfg.DBPath, cfg.Policy)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if err = reopened.ActivateCatchup(cfg, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	st, err := reopened.CatchupStatus()
	if err != nil || st["state"] != "disarmed_continuity" {
		t.Fatalf("restart not disarmed err=%v status=%v", err, st)
	}
}

func TestCatchupFinalSealDisarmRollsBackPageAndCommitsOnlyDisarm(t *testing.T) {
	s, cfg, _ := catchupFixture(t)
	floor := catchupRow(t, s).Floor
	in := catchupForceGap(t, s, catchupAdd(floor, 10))
	_, err := s.call(func(db *storeConn) (any, error) {
		_, e := db.Exec(`CREATE TRIGGER reviewer_change_witness AFTER INSERT ON ingress_validation BEGIN UPDATE catchup_meta SET value='00000000000000000000000000000000' WHERE key='witness'; END`)
		return nil, e
	})
	if err != nil {
		t.Fatal(err)
	}
	out, err := s.applyCatchupPage(cfg, *in, []*discordgo.Message{catchupMessage(t, in.Route, catchupAdd(floor, 1), "1")})
	if err != nil || out != "disarmed" {
		t.Fatal(out, err)
	}
	var rows, sources int
	s.call(func(db *storeConn) (any, error) {
		err := db.QueryRow(`SELECT (SELECT count(*) FROM ingress_validation),(SELECT count(*) FROM message_sources)`).Scan(&rows, &sources)
		return nil, err
	})
	if rows != 0 || sources != 0 {
		t.Fatal("partial admission survived final guard", rows, sources)
	}
	row := catchupRow(t, s)
	status, _ := s.CatchupStatus()
	if row.Floor != floor || status["state"] != "disarmed_continuity" {
		t.Fatal(row, status)
	}
}

func TestReviewerActivationWitnessReadSerializedWithLedger(t *testing.T) {
	s, cfg, now := catchupFixture(t)
	second, err := OpenStore(cfg.DBPath, cfg.Policy)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	floor := catchupRow(t, s).Floor
	entered := make(chan struct{})
	release := make(chan struct{})
	blockedDone := make(chan struct{})
	go func() {
		defer close(blockedDone)
		s.call(func(db *storeConn) (any, error) { close(entered); <-release; return nil, nil })
	}()
	<-entered
	activated := make(chan error, 1)
	go func() { activated <- s.ActivateCatchup(cfg, now.Add(time.Minute)) }()
	// Let activation finish its OS witness read and queue behind the actor.
	time.Sleep(50 * time.Millisecond)
	e := threadEnvelope()
	e.RouteKind = "guild_text"
	e.ConversationID = "2"
	e.ParentChannelID = ""
	e.ThreadType = 0
	e.EventID = catchupAdd(floor, 10)
	if _, err = second.Ingest(e); err != nil {
		close(release)
		t.Fatal(err)
	}
	close(release)
	<-blockedDone
	if err = <-activated; err != nil {
		t.Fatal(err)
	}
	st, err := s.CatchupStatus()
	if err != nil || st["state"] != "armed" {
		t.Fatalf("legitimate concurrent admission falsely disarmed activation: %v %v", st, err)
	}
}

func TestReviewerOutsideWindowMutationDoesNotInvalidate(t *testing.T) {
	for _, delta := range []int{-1, 20} {
		t.Run(fmt.Sprint(delta), func(t *testing.T) {
			s, cfg, _ := catchupFixture(t)
			floor := catchupRow(t, s).Floor
			in := catchupForceGap(t, s, catchupAdd(floor, 10))
			id := snowPrevious(floor)
			if delta > 0 {
				id = catchupAdd(floor, uint64(delta))
			}
			if _, err := receiveSourceEvent(context.Background(), nil, s, cfg, sourceGatewayEvent{Delete: &discordgo.Message{ID: id, ChannelID: "2", GuildID: "5"}}); err != nil {
				t.Fatal(err)
			}
			out, err := s.applyCatchupPage(cfg, *in, []*discordgo.Message{catchupMessage(t, in.Route, catchupAdd(floor, 1), "1")})
			if err != nil || out != "progress" {
				t.Fatalf("irrelevant mutation outside frozen (floor,upper] invalidated recovery: %s %v", out, err)
			}
		})
	}
}
