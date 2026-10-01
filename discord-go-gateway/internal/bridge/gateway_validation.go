package bridge

import (
	"context"
	"errors"
	"time"
)

func validationLoop(ctx context.Context, store *Store, rest *RESTClient, hub *WakeHub, g *gatewayState, dbPath string) error {
	wake, unsubscribe := hub.Subscribe()
	defer unsubscribe()
	tick := time.NewTicker(250 * time.Millisecond)
	defer tick.Stop()
	lastReadyEpoch := ^uint64(0)
	for {
		for ctx.Err() == nil {
			ready, epoch := g.readiness()
			if !ready {
				break
			}
			if epoch != lastReadyEpoch {
				if err := store.ResumeValidation(); err != nil {
					return err
				}
				lastReadyEpoch = epoch
			}
			in, err := store.NextValidation(wall())
			if err != nil {
				return err
			}
			if in == nil {
				break
			}
			if err = processValidation(ctx, store, rest, g, *in, epoch); err != nil {
				return err
			}
			hub.Notify()
			NotifyFile(dbPath + ".sock.wake")
		}
		select {
		case <-ctx.Done():
			return nil
		case <-wake:
		case <-tick.C:
		}
	}
}

func processValidation(parent context.Context, store *Store, rest *RESTClient, g *gatewayState, in ValidationInput, epoch uint64) error {
	if !store.stages(in.Event) {
		if err := store.DeferValidation(in.ID, "authorization_revoked", 0, true); err != nil {
			return err
		}
		g.recordIngress("validation_blocked")
		return nil
	}
	// The deadline includes waiting for an already learned route/global budget,
	// not just the HTTP operation. Ingress staging continues independently.
	ctx, cancel := context.WithTimeout(parent, 20*time.Second)
	defer cancel()
	channel, err := rest.Channel(ctx, in.Event.ConversationID)
	verified := in.Event
	failure := "route_lookup_failed"
	if err == nil {
		verified, err = rest.validateIngressRoute(ctx, channel, in.Event)
		failure = "route_validation_failed"
	}
	if parent.Err() != nil {
		return nil // Retain the exact pending record for the next daemon.
	}
	ready, currentEpoch := g.readiness()
	if err == nil && (!ready || currentEpoch != epoch) {
		err = errors.New("validation_connection_changed")
	}
	if err == nil {
		if rest.contextEnabled {
			verified.Context = rest.resolveMessageContext(ctx, verified)
		}
		var outcome string
		var promoteErr error
		if in.Event.RouteKind == "guild_thread_candidate" {
			outcome, promoteErr = store.promoteThreadValidation(in, verified)
		} else {
			outcome, promoteErr = store.promoteContextValidation(in, verified)
		}
		if promoteErr == nil {
			g.recordIngress(outcome)
		}
		return promoteErr
	}
	g.recordIngress(failure)
	if in.Event.RouteKind == "guild_thread_candidate" && err.Error() == "preflight_channel_mismatch" {
		g.recordIngress("rejected")
		return store.rejectValidation(in.ID)
	}
	blocked := false
	switch err.Error() {
	case "preflight_http_401", "preflight_http_403", "preflight_http_404", "preflight_channel_mismatch", "preflight_recipient_mismatch", "invalid_channel_id":
		blocked = true
	}
	attempt := in.Attempts + 1
	if attempt > 5 {
		attempt = 5
	}
	delay := time.Duration(1<<attempt) * time.Second
	if delay > 30*time.Second {
		delay = 30 * time.Second
	}
	if e := store.DeferValidation(in.ID, err.Error(), wall()+delay.Seconds(), blocked); e != nil {
		return e
	}
	if blocked {
		g.recordIngress("validation_blocked")
	} else {
		g.recordIngress("validation_retry")
	}
	if err.Error() == "preflight_http_401" {
		return errors.New("authentication_failed")
	}
	return nil
}
