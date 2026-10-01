package bridge

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestMessageOperationPinRequiresCurrentBit51(t *testing.T) {
	for _, allow := range []bool{false, true} {
		t.Run(fmt.Sprint(allow), func(t *testing.T) {
			bits := permissionViewChannel | permissionReadHistory | uint64(1<<13)
			if allow {
				bits |= permissionPinMessages
			}
			r := localREST(t, func(w http.ResponseWriter, q *http.Request) {
				switch q.URL.Path {
				case "/users/@me":
					json.NewEncoder(w).Encode(User{ID: "4", Bot: true})
				case "/channels/2":
					json.NewEncoder(w).Encode(Channel{ID: "2", Type: 0, GuildID: "5", PermissionOverwrites: []PermissionOverwrite{}})
				case "/guilds/5/members/4":
					json.NewEncoder(w).Encode(guildMember{User: User{ID: "4", Bot: true}, Roles: []string{}})
				case "/guilds/5/roles":
					json.NewEncoder(w).Encode([]guildRole{{ID: "5", Permissions: permissionTestBits(bits)}})
				default:
					t.Fatal("unexpected", q.URL.Path)
				}
			})
			r.settings.Policy.GuildID = "5"
			r.settings.Policy.GuildChannelID = "2"
			err := r.operationRoute(context.Background(), ReadRoute{ChannelID: "2", GuildID: "5", Kind: "guild_text"}, MessageOperationSpec{Action: "pin"})
			if (err == nil) != allow {
				t.Fatal(allow, err)
			}
		})
	}
}
func TestMessageOperationGenerationRevokesUnattemptedWrite(t *testing.T) {
	f := newOperationFixture(t)
	base := f.rest.client.Transport
	var targetReads atomic.Int32
	f.rest.client.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.Method == "GET" && req.URL.Path == "/channels/2/messages/9" && targetReads.Add(1) == 2 {
			changed, err := f.store.InvalidateControlTarget("2", "", "9", "target_updated")
			if err != nil || !changed {
				t.Error(changed, err)
			}
		}
		return base.RoundTrip(req)
	})
	out, err := f.execute(f.spec("edit_text", "generation"))
	if err != nil || out.State != "failed" || out.Attempts != 0 || f.writes.Load() != 0 {
		t.Fatal(out, err, f.writes.Load())
	}
}
func TestMessageOperationRateWaitRevokesClaimAndTarget(t *testing.T) {
	for _, target := range []bool{false, true} {
		t.Run(fmt.Sprint(target), func(t *testing.T) {
			f := newOperationFixture(t)
			spec := f.spec("edit_text", "wait")
			route, _ := limitRoute(http.MethodPatch, "/channels/2/messages/9")
			f.rest.limitMu.Lock()
			f.rest.limits = map[string]time.Time{route: time.Now().Add(150 * time.Millisecond)}
			f.rest.limitMu.Unlock()
			done := make(chan MessageOperation, 1)
			go func() { o, _ := f.execute(spec); done <- o }()
			id := operationID(f.claim.InboundID, spec.Key)
			until := time.Now().Add(3 * time.Second)
			for {
				if o, err := f.store.MessageOperation(id); err == nil && o.State == "uncertain" {
					break
				}
				if time.Now().After(until) {
					t.Fatal("attempt not reserved")
				}
				time.Sleep(time.Millisecond)
			}
			if target {
				f.store.InvalidateControlTarget("2", "", "9", "target_updated")
			} else {
				f.store.Ignore(f.claim.InboundID, f.claim.Claim)
			}
			out := <-done
			if out.State != "failed" || f.writes.Load() != 0 {
				t.Fatal(out, f.writes.Load())
			}
		})
	}
}
func TestMessageOperationExplicitAbandonPreparedOnly(t *testing.T) {
	f := newOperationFixture(t)
	f.rest.sendMu.Lock()
	done := make(chan struct{})
	spec := f.spec("pin", "abandon")
	go func() { defer close(done); f.execute(spec) }()
	id := operationID(f.claim.InboundID, spec.Key)
	until := time.Now().Add(3 * time.Second)
	for {
		if o, err := f.store.MessageOperation(id); err == nil && o.State == "prepared" {
			break
		}
		if time.Now().After(until) {
			t.Fatal("not prepared")
		}
		time.Sleep(time.Millisecond)
	}
	out, err := f.store.AbandonMessageOperation(f.claim.InboundID, f.claim.Claim, id)
	if err != nil || out.State != "failed" || out.Attempts != 0 {
		t.Fatal(out, err)
	}
	f.rest.sendMu.Unlock()
	<-done
	if f.writes.Load() != 0 {
		t.Fatal("abandoned write")
	}
	f.lose = true
	out, err = f.execute(f.spec("pin", "unknown"))
	if err != nil || out.State != "uncertain" {
		t.Fatal(out, err)
	}
	if _, err = f.store.AbandonMessageOperation(f.claim.InboundID, f.claim.Claim, out.ID); err == nil {
		t.Fatal("uncertain abandoned")
	}
}
func TestMessageOperationDelayedGatewayRequiresReconcileAndNewBinding(t *testing.T) {
	f := newOperationFixture(t)
	before, err := f.store.sentControlTarget("2", "9")
	if err != nil {
		t.Fatal(err)
	}
	out, err := f.execute(f.spec("edit_text", "binding"))
	if err != nil || out.State != "verified" {
		t.Fatal(out, err)
	}
	target, err := f.store.sentControlTarget("2", "9")
	if err != nil || target.Text != "revised answer" || target.Revision == before.Revision {
		t.Fatal(target, err)
	}
	if _, err = f.store.InvalidateControlTarget("2", "", "9", "target_updated"); err != nil {
		t.Fatal(err)
	}
	if _, err = f.store.sentControlTarget("2", "9"); err == nil {
		t.Fatal("unknown target trusted")
	}
	if _, err = f.rest.ReconcileMessageOperation(context.Background(), f.store, f.claim.InboundID, f.claim.Claim, out.ID, true); err != nil {
		t.Fatal(err)
	}
	target, err = f.store.sentControlTarget("2", "9")
	if err != nil || target.Text != "revised answer" || target.Revision == before.Revision {
		t.Fatal(target, err)
	}
	f.rest.controlStore = f.store
	e := f.claim.Envelope
	e.EventID = "9"
	e.Text = ""
	e.ReplyKind = "message"
	e.Control = encodeControl(ControlEvent{Version: 1, Kind: "reaction", ID: "new", ActorID: "1", TargetMessageID: "9", TargetRequestID: target.Request, TargetRevision: target.Revision, TargetText: target.Text, Emoji: "👍", Added: true})
	if err = f.rest.verifyReactionTarget(context.Background(), e); err != nil {
		t.Fatal(err)
	}
}
func TestMessageOperationInvalidationBindsGuildAndNonBotTargets(t *testing.T) {
	f := newOperationFixture(t)
	if _, err := f.store.bindOperationTarget(ReadRoute{ChannelID: "2", Kind: "dm"}, "77"); err != nil {
		t.Fatal(err)
	}
	changed, err := f.store.InvalidateControlTarget("2", "5", "77", "target_updated")
	if err != nil || changed {
		t.Fatal(changed, err)
	}
	gen, _ := f.store.operationTargetGeneration("2", "77")
	if gen != 0 {
		t.Fatal(gen)
	}
	changed, err = f.store.InvalidateControlTarget("2", "", "77", "target_updated")
	if err != nil || !changed {
		t.Fatal(changed, err)
	}
	gen, _ = f.store.operationTargetGeneration("2", "77")
	if gen != 1 {
		t.Fatal(gen)
	}
}
func TestMessageOperationTransportEmbedsAndLF(t *testing.T) {
	f := newOperationFixture(t)
	f.target["embeds"] = []any{map[string]any{"type": "rich", "image": map[string]any{"url": "https://cdn.discordapp.com/attachments/2/77/p.png?ex=1&is=2&hm=a", "proxy_url": "https://media.discordapp.net/one"}}}
	f.afterWrite = func() {
		f.target["embeds"] = []any{map[string]any{"type": "rich", "image": map[string]any{"url": "https://cdn.discordapp.com/attachments/2/77/p.png?ex=3&is=4&hm=b", "proxy_url": "https://media.discordapp.net/two"}}}
	}
	out, err := f.execute(f.spec("edit_text", "url"))
	if err != nil || out.State != "verified" {
		t.Fatal(out, err)
	}
	spec := f.spec("edit_text", "lf")
	spec.Text = "text\n"
	if validOperationSpec(spec) {
		t.Fatal("unsupported terminal LF accepted")
	}
}
func TestMessageOperationRecoveryFencesBlockReplayAndTarget(t *testing.T) {
	f := newOperationFixture(t)
	spec := f.spec("pin", "restore")
	id := operationID(f.claim.InboundID, spec.Key)
	_, err := f.store.call(func(db *storeConn) (any, error) {
		_, err := db.Exec(`INSERT INTO message_operation_recovery_fences VALUES(?,?,?,1,'pin','uncertain',1,?)`, id, "2", "9", epoch())
		return nil, err
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.execute(spec); err == nil || !strings.Contains(err.Error(), "recovery_fence") {
		t.Fatal(err)
	}
	spec.Key = "new"
	if _, err = f.execute(spec); err == nil {
		t.Fatal("uncertain recovered target bypass")
	}
}

func TestMessageOperationAttachmentOrderAndAllFlagsPreserved(t *testing.T) {
	f := newOperationFixture(t)
	f.target["attachments"] = []any{map[string]any{"id": "99", "filename": "first.png", "size": 12}, map[string]any{"id": "77", "filename": "second.png", "size": 13}}
	route, err := f.rest.ResolveReadRoute(context.Background(), f.store, "2")
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(f.target)
	spec := f.spec("edit_text", "order")
	snap, err := readOperationSnapshot(raw, route, spec)
	if err != nil {
		t.Fatal(err)
	}
	snap.Flags = 4 | 32
	_, _, body := operationRequest(MessageOperation{Spec: spec, before: snap})
	var fields map[string]json.RawMessage
	json.Unmarshal(body, &fields)
	if string(fields["attachments"]) != `[{"id":"99"},{"id":"77"}]` || string(fields["flags"]) != "36" {
		t.Fatal(string(body))
	}
}

func TestMessageOperationReservationRollbackIsUnattempted(t *testing.T) {
	f := newOperationFixture(t)
	_, err := f.store.call(func(db *storeConn) (any, error) {
		_, err := db.Exec(`CREATE TRIGGER operation_projection_fault BEFORE INSERT ON message_edit_projection BEGIN SELECT RAISE(ABORT,'offline injection'); END`)
		return nil, err
	})
	if err != nil {
		t.Fatal(err)
	}
	out, err := f.execute(f.spec("edit_text", "rollback"))
	if err != nil || out.State != "failed" || out.Attempts != 0 || f.writes.Load() != 0 {
		t.Fatal(out, err)
	}
	var n int
	f.store.call(func(db *storeConn) (any, error) {
		return nil, db.QueryRow(`SELECT count(*) FROM control_target_invalidations`).Scan(&n)
	})
	if n != 0 {
		t.Fatal("transaction leaked invalidation")
	}
}
func TestMessageOperationReadbackCommitFailureStaysUncertain(t *testing.T) {
	f := newOperationFixture(t)
	_, err := f.store.call(func(db *storeConn) (any, error) {
		_, err := db.Exec(`CREATE TRIGGER operation_finish_fault BEFORE UPDATE OF state ON message_operations WHEN NEW.state='verified' BEGIN SELECT RAISE(ABORT,'offline injection'); END`)
		return nil, err
	})
	if err != nil {
		t.Fatal(err)
	}
	spec := f.spec("edit_text", "commit-fault")
	_, err = f.execute(spec)
	if err == nil {
		t.Fatal("injected error missing")
	}
	out, err := f.store.MessageOperation(operationID(f.claim.InboundID, spec.Key))
	if err != nil || out.State != "uncertain" || out.Attempts != 1 || f.writes.Load() != 1 {
		t.Fatal(out, err)
	}
	if _, _, err = f.store.CurrentEditedMessage("2", "9"); err == nil {
		t.Fatal("rolled back projection trusted")
	}
	if _, err = f.execute(spec); err != nil || f.writes.Load() != 1 {
		t.Fatal("uncertain automatically retried", err)
	}
}

func TestMessageOperationRejectsAmbiguousReactionIDs(t *testing.T) {
	f := newOperationFixture(t)
	spec := f.spec("remove_own_reaction", "ambiguous")
	spec.Emoji = "old:7777"
	f.target["reactions"] = []any{map[string]any{"me": true, "emoji": map[string]any{"name": "old", "id": "7777"}}, map[string]any{"me": false, "emoji": map[string]any{"name": "new", "id": "7777"}}}
	raw, _ := json.Marshal(f.target)
	_, err := readOperationSnapshot(raw, ReadRoute{ChannelID: "2", Kind: "dm"}, spec)
	if err == nil {
		t.Fatal("duplicate custom emoji identity accepted")
	}
}
