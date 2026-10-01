package bridge

import (
	"context"
	"errors"
	"net/url"
	"time"

	"github.com/bwmarrin/discordgo"
)

// DiscordGo adds version/encoding to READY's bare resume URL at each Open.
// Validate before readiness and again through ProxyRequest before every dial;
// accepting query-bearing endpoints would produce ambiguous protocol options.
func validateResumeGateway(p *ProxyConfig, raw string) error {
	route, err := p.DiscordRoute()
	if err != nil {
		return err
	}
	if err := p.ValidateGateway(raw, route); err != nil {
		return err
	}
	u, err := url.Parse(raw)
	if err != nil || u.RawQuery != "" || u.ForceQuery {
		return errors.New("unexpected_gateway_endpoint")
	}
	return nil
}

func (g *gatewayState) setGuildReadiness(epoch uint64, ready bool, failure string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.epoch.Load() != epoch || !g.ready.Load() {
		return false
	}
	g.guildReady, g.guildFailure = ready, failure
	return true
}

func (g *gatewayState) guildReadiness() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.guildReady
}

// SDK event dispatch is synchronous. Replace any queued old-epoch wake with the
// newest READY/RESUMED/GUILD_CREATE epoch, so stale validation cannot consume the
// only wake for a newly connected session.
func queueGatewayValidation(ch chan uint64, epoch uint64) {
	select {
	case <-ch:
	default:
	}
	select {
	case ch <- epoch:
	default:
	}
}

func gatewayRoutePermissions(g *gatewayState, s *discordgo.Session, c Settings, e Envelope) bool {
	if e.RouteKind != "dm" {
		if !g.guildReadiness() {
			return false
		}
	}
	return guildRoutePermissions(s, c, e)
}

// A valid bot identity and connected session make owner DMs available even if
// GUILD_CREATE has not arrived, the parent is missing, or guild access is lost.
// Guild ingress and every guild write retain their own fail-closed route gate.
func refreshGatewayReadiness(parent context.Context, rest *RESTClient, s *discordgo.Session, settings Settings, g *gatewayState, expectedEpoch uint64) error {
	ready, epoch := g.readiness()
	if epoch != expectedEpoch || parent.Err() != nil {
		return nil
	}
	if !ready {
		ctx, cancel := context.WithTimeout(parent, 20*time.Second)
		_, err := rest.Identity(ctx)
		cancel()
		if err != nil {
			return err
		}
		if parent.Err() != nil || !g.publishReady(epoch) {
			return nil
		}
	}
	if settings.Policy.GuildID == "" {
		return nil
	}
	if !guildPermissions(s, settings) {
		g.setGuildReadiness(epoch, false, "configured_channel_permissions_missing")
		return nil
	}
	ctx, cancel := context.WithTimeout(parent, 20*time.Second)
	defer cancel()
	channel, err := rest.Channel(ctx, settings.Policy.GuildChannelID)
	if err == nil {
		err = rest.ValidateChannel(channel, Envelope{ConversationID: settings.Policy.GuildChannelID, RouteKind: "guild_text", GuildID: settings.Policy.GuildID})
	}
	if err != nil {
		g.setGuildReadiness(epoch, false, guildReadinessFailure(err))
		// An invalid credential is global. A forbidden/missing/mismatched guild
		// route is local; in particular HTTP 403 here is not an identity failure.
		if err.Error() == "preflight_http_401" {
			return errors.New("authentication_failed")
		}
		return nil
	}
	if parent.Err() == nil {
		allowed, failure := guildPermissions(s, settings), ""
		if !allowed {
			failure = "configured_channel_permissions_missing"
		}
		g.setGuildReadiness(epoch, allowed, failure)
	}
	return nil
}

// Runtime status contains only fixed symbolic errors, never transport strings,
// response content, credentials, URLs, channel names or other untrusted data.
func guildReadinessFailure(err error) string {
	switch err.Error() {
	case "preflight_http_401", "preflight_http_403", "preflight_http_404", "preflight_http_429", "preflight_channel_mismatch", "preflight_channel_invalid", "preflight_invalid_ack":
		return err.Error()
	default:
		return "configured_channel_lookup_failed"
	}
}
