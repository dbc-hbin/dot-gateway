package bridge

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/gorilla/websocket"
)

type gatewayState struct {
	ready                     atomic.Bool
	epoch                     atomic.Uint64
	mu                        sync.Mutex
	state                     string
	guildReady                bool
	guildFailure              string
	readyCount, disconnects   int
	lastReady, lastDisconnect float64
	ingressCounts             map[string]int
}

// Only bounded, content-free outcomes enter runtime diagnostics. Rejected routes
// remain rejected; diagnostics do not relax the admission/preflight boundary.
func (g *gatewayState) recordIngress(outcome string) {
	switch outcome {
	case "source_update", "source_deleted", "accepted", "duplicate", "rejected", "queue_full", "route_lookup_failed", "route_validation_failed", "validation_staged", "validation_retry", "validation_blocked":
	default:
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.ingressCounts == nil {
		g.ingressCounts = make(map[string]int)
	}
	g.ingressCounts[outcome]++
}

func (g *gatewayState) disconnect() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.ready.Store(false)
	g.guildReady = false
	g.epoch.Add(1)
	g.state = "reconnecting"
	g.disconnects++
	g.lastDisconnect = wall()
}
func (g *gatewayState) publishReady(epoch uint64) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.epoch.Load() != epoch {
		return false
	}
	if !g.ready.Swap(true) {
		g.readyCount++
		g.lastReady = wall()
	}
	g.state = "connected"
	return true
}
func wall() float64 { return float64(time.Now().UnixNano()) / 1e9 }
func (g *gatewayState) health(r *RESTClient) map[string]any {
	g.mu.Lock()
	defer g.mu.Unlock()
	ingress, warnings := map[string]int{}, map[string]int{}
	for outcome, count := range g.ingressCounts {
		ingress[outcome] = count
		switch outcome {
		case "queue_full":
			warnings["inbound_queue_full"] = count
		case "route_lookup_failed":
			warnings["owner_route_lookup_failed"] = count
		case "route_validation_failed":
			warnings["owner_route_validation_failed"] = count
		}
	}
	if g.guildFailure != "" {
		warnings["configured_guild_unavailable"] = 1
	}
	return map[string]any{"guild_ready": g.guildReady, "guild_failure": g.guildFailure, "state": g.state, "transport": "go_discord_gateway", "ready_count": g.readyCount, "disconnects": g.disconnects, "last_ready_at": g.lastReady, "last_disconnect_at": g.lastDisconnect, "raw_sender": r.Diagnostics(), "receive_only": r.settings.ReceiveOnly, "keep_catchup_disarmed": r.settings.KeepCatchupDisarmed, "ingress_counts": ingress, "warning_counts": warnings}
}

func persistGatewayHealth(store *Store, details map[string]any) error {
	warnings, _ := details["warning_counts"].(map[string]int)
	return store.SetRuntime(details["state"].(string), warnings, details)
}
func guildPermissions(s *discordgo.Session, c Settings) bool {
	if c.Policy.GuildID == "" {
		return true
	}
	guild, e := s.State.Guild(c.Policy.GuildID)
	if e != nil || guild.Unavailable {
		return false
	}
	ch, e := s.State.Channel(c.Policy.GuildChannelID)
	if e != nil || ch.Type != discordgo.ChannelTypeGuildText || ch.GuildID != c.Policy.GuildID {
		return false
	}
	p, e := s.State.UserChannelPermissions(c.ExpectedBotID, c.Policy.GuildChannelID)
	need := int64(discordgo.PermissionViewChannel | discordgo.PermissionReadMessageHistory)
	return e == nil && (p&discordgo.PermissionAdministrator != 0 || p&need == need && p&int64(discordgo.PermissionSendMessages|discordgo.PermissionSendMessagesInThreads) != 0)
}

// ClassifyGatewayError preserves the supervisor's permanent/transient contract.
// No raw HTTP body, token or exception crosses the process boundary.
func ClassifyGatewayError(err error) error {
	if err == nil {
		return nil
	}
	var restError *discordgo.RESTError
	if errors.As(err, &restError) && restError.Response != nil {
		return fmt.Errorf("gateway_discovery_http_%d", restError.Response.StatusCode)
	}
	if errors.Is(err, websocket.ErrBadHandshake) {
		return errors.New("gateway_websocket_bad_handshake")
	}
	var networkError *net.OpError
	if errors.As(err, &networkError) {
		if networkError.Timeout() {
			return errors.New("gateway_network_timeout")
		}
		return errors.New("gateway_network_connection_failed")
	}
	var closed *websocket.CloseError
	if errors.As(err, &closed) {
		switch closed.Code {
		case 4004:
			return errors.New("authentication_failed")
		case 4013, 4014:
			return errors.New("privileged_intents_required")
		case 4010, 4011, 4012:
			return errors.New("gateway_configuration_failed")
		}
	}
	switch err.Error() {
	case "preflight_http_401", "preflight_http_403":
		return errors.New("authentication_failed")
	case "preflight_identity_mismatch":
		return errors.New("bot_identity_mismatch")
	case "preflight_channel_mismatch", "preflight_recipient_mismatch":
		return errors.New("configured_channel_mismatch")
	case "preflight_http_404":
		return errors.New("configured_channel_lookup_failed")
	case "configured_channel_permissions_missing", "authentication_failed", "bot_identity_mismatch", "privileged_intents_required", "gateway_configuration_failed", "configured_channel_mismatch", "configured_channel_lookup_failed", "gateway_proxy_route_or_endpoint_invalid", "gateway_startup_timeout", "gateway_connection_failed":
		return err
	}
	if strings.Contains(err.Error(), "unexpected_gateway_endpoint") || strings.Contains(err.Error(), "gateway_proxy_route_mismatch") {
		return errors.New("gateway_proxy_route_or_endpoint_invalid")
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return errors.New("gateway_startup_timeout")
	}
	for _, code := range []string{"preflight_transport_failed", "preflight_invalid_ack", "request_build_failed", "receive_only_write_blocked", "invalid_ack", "response_read_failed", "oversize_ack"} {
		if err.Error() == code {
			return errors.New("gateway_cause_" + code)
		}
	}
	text := strings.ToLower(err.Error())
	for _, pair := range [][2]string{{"forbidden", "forbidden"}, {"bad handshake", "websocket_bad_handshake"}, {"proxyconnect", "proxy_connect_failed"}, {"unexpected eof", "unexpected_eof"}, {"eof", "eof"}, {"not found", "not_found"}, {"permission denied", "permission_denied"}, {"connection refused", "connection_refused"}, {"403", "http_403"}, {"timeout", "timeout"}, {"receive_only_write_blocked", "unexpected_write_blocked"}, {"400", "http_400"}, {"status", "http_status"}, {"connect", "connect"}} {
		if strings.Contains(text, pair[0]) {
			return errors.New("gateway_connection_" + pair[1])
		}
	}
	return errors.New("gateway_connection_failed")
}

// GatewayExitCode is consumed by the existing bounded-restart supervisor.
func GatewayExitCode(err error) int {
	if err == nil {
		return 0
	}
	switch err.Error() {
	case "authentication_failed", "privileged_intents_required", "bot_identity_mismatch", "configured_channel_mismatch", "configured_channel_lookup_failed", "configured_channel_permissions_missing", "gateway_proxy_route_or_endpoint_invalid", "gateway_configuration_failed", "another_dispatcher_is_running", "dispatcher_lock_insecure", "dispatcher_lock_failed", "ipc_path_not_socket", "ipc_path_too_long", "wake_file_insecure":
		return 2
	}
	return 1
}

// RunGateway owns the legacy dispatcher lock for its entire network lifetime.
func RunGateway(parent context.Context, settings Settings, store *Store) (retErr error) {
	lock, e := LockDispatcher(settings.DBPath)
	if e != nil {
		return e
	}
	defer lock.Close()
	hub := NewWakeHub()
	fileWake, e := StartFileWake(settings.DBPath+".sock.wake", hub)
	if e != nil {
		return e
	}
	defer fileWake.Close()
	ipc, e := StartIPC(settings.DBPath+".sock", hub)
	if e != nil {
		if e.Error() != "ipc_listen_failed" {
			return e
		}
	} else {
		defer ipc.Close()
	}
	if _, e = store.RecoverInterrupted(); e != nil {
		return e
	}
	if e = store.RecoverDiagnostics(); e != nil {
		return e
	}
	if e = store.SetRuntime("starting", nil, nil); e != nil {
		return e
	}
	rest, e := NewRESTClient(settings)
	if e != nil {
		return e
	}
	defer rest.Close()
	if e = prepareGatewayCatchup(store, settings, time.Now()); e != nil {
		return e
	}
	rest.contextEnabled = true
	rest.controlStore = store
	g := &gatewayState{state: "connecting"}
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	fatal := make(chan error, 1)
	fail := func(code string) {
		select {
		case fatal <- errors.New(code):
		default:
		}
		cancel()
	}
	defer func() {
		state := "stopped"
		details := g.health(rest)
		if retErr != nil {
			state = "failed"
			details["fatal_code"] = retErr.Error()
		}
		details["state"] = state
		persistGatewayHealth(store, details)
	}()
	startup, cancelStartup := context.WithTimeout(ctx, 60*time.Second)
	defer cancelStartup()
	if _, e = rest.Identity(startup); e != nil {
		return ClassifyGatewayError(e)
	}
	s, e := discordgo.New("Bot " + settings.Token)
	if e != nil {
		return errors.New("gateway_configuration_failed")
	}
	s.LogLevel = -1
	s.ShouldReconnectOnError = false
	s.ShouldRetryOnRateLimit = false
	s.MaxRestRetries = 0
	s.SyncEvents = true
	s.Client = rest.Client()
	rest.routePermission = func(e Envelope) bool { return gatewayRoutePermissions(g, s, settings, e) }
	s.Identify.Intents = discordgo.IntentsDirectMessages | discordgo.IntentsDirectMessageReactions
	if settings.Policy.GuildID != "" {
		s.Identify.Intents |= discordgo.IntentsGuilds | discordgo.IntentsGuildMessages | discordgo.IntentsGuildMessageReactions
	}
	if settings.Policy.MessageContentApproved {
		s.Identify.Intents |= discordgo.IntentsMessageContent
	}
	dial := newGatewayDialer(ctx)
	defer dial.closeConnections()
	s.Dialer = &websocket.Dialer{Proxy: settings.ProxyConfig.ProxyRequest, HandshakeTimeout: 20 * time.Second, NetDialContext: dial.dial}
	validate := make(chan uint64, 1)
	requestValidation := func() {
		queueGatewayValidation(validate, g.epoch.Load())
	}
	incoming := make(chan sourceGatewayEvent, 1000)
	controls, e := NewInteractionService(settings, store, func() { hub.Notify(); NotifyFile(settings.DBPath + ".sock.wake") })
	if e != nil {
		return e
	}
	defer controls.Close()
	if err := store.RecoverInteractionTokens(); err != nil {
		return err
	}
	controls.rest.routePermission = rest.routePermission
	// Bound concurrency without placing acknowledgements behind message lookup.
	controlSlots := make(chan struct{}, 16)
	var controlWG sync.WaitGroup
	startControl := func(fn func()) {
		select {
		case controlSlots <- struct{}{}:
			controlWG.Add(1)
			go func() { defer controlWG.Done(); defer func() { <-controlSlots }(); fn() }()
		default:
		}
	}
	defer func() { cancel(); controlWG.Wait() }()
	s.AddHandler(func(_ *discordgo.Session, i *discordgo.InteractionCreate) {
		if !settings.ReceiveOnly && i != nil && i.Interaction != nil {
			received := time.Now()
			startControl(func() { controls.Handle(ctx, i.Interaction, received) })
		}
	})

	type reactionInput struct {
		event *discordgo.MessageReaction
		added bool
	}
	reactions := make(chan reactionInput, 1000)
	controlWG.Add(1)
	go func() {
		defer controlWG.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case in := <-reactions:
				work, done := context.WithTimeout(ctx, 5*time.Second)
				controls.HandleReaction(work, in.event, in.added)
				done()
			}
		}
	}()
	s.AddHandler(func(_ *discordgo.Session, m *discordgo.MessageReactionAdd) {
		if !settings.ReceiveOnly && m != nil && m.MessageReaction != nil && m.UserID == settings.Policy.OwnerID {
			select {
			case reactions <- reactionInput{m.MessageReaction, true}:
			default:
				fail("gateway_control_capacity_exceeded")
			}
		}
	})
	s.AddHandler(func(_ *discordgo.Session, m *discordgo.MessageReactionRemove) {
		if !settings.ReceiveOnly && m != nil && m.MessageReaction != nil && m.UserID == settings.Policy.OwnerID {
			select {
			case reactions <- reactionInput{m.MessageReaction, false}:
			default:
				fail("gateway_control_capacity_exceeded")
			}
		}
	})

	s.AddHandler(func(_ *discordgo.Session, r *discordgo.Ready) {
		if r.User == nil || r.User.ID != settings.ExpectedBotID || !r.User.Bot {
			fail("bot_identity_mismatch")
			return
		}
		if err := validateResumeGateway(settings.ProxyConfig, r.ResumeGatewayURL); err != nil {
			fail("gateway_proxy_route_or_endpoint_invalid")
			return
		}
		requestValidation()
	})
	s.AddHandler(func(_ *discordgo.Session, _ *discordgo.Resumed) { requestValidation() })
	s.AddHandler(func(_ *discordgo.Session, r *discordgo.GuildCreate) {
		if r.Guild.ID == settings.Policy.GuildID {
			requestValidation()
		}
	})
	disconnected := make(chan struct{}, 1)
	s.AddHandler(func(_ *discordgo.Session, _ *discordgo.Disconnect) {
		g.disconnect()
		if !settings.KeepCatchupDisarmed {
			if err := store.MarkCatchupGaps(time.Now(), "disconnected"); err != nil {
				fail("catchup_persistence_failed")
			}
		}
		hub.Notify()
		select {
		case disconnected <- struct{}{}:
		default:
		}
	})
	s.AddHandler(func(_ *discordgo.Session, m *discordgo.MessageCreate) {
		if m.Message == nil || m.Author == nil || m.Author.ID != settings.Policy.OwnerID || m.Author.Bot || m.WebhookID != "" {
			return
		}
		if m.Type != discordgo.MessageTypeDefault && m.Type != discordgo.MessageTypeReply {
			return
		}
		select {
		case incoming <- sourceGatewayEvent{Create: m.Message}:
		default:
			if !settings.KeepCatchupDisarmed {
				if err := store.recordCatchupEventGap(m.ChannelID, m.ID); err != nil {
					fail("catchup_persistence_failed")
				}
			}
			fail("gateway_inbound_capacity_exceeded")
		}
	})
	enqueueSource := func(event sourceGatewayEvent) {
		select {
		case incoming <- event:
		default:
			fail("gateway_inbound_capacity_exceeded")
		}
	}
	s.AddHandler(func(_ *discordgo.Session, m *discordgo.MessageUpdate) {
		if m.Message != nil && (m.GuildID == "" || m.GuildID == settings.Policy.GuildID) {
			enqueueSource(sourceGatewayEvent{Update: m.Message})
		}
	})
	s.AddHandler(func(_ *discordgo.Session, m *discordgo.MessageDelete) {
		if m.Message != nil && (m.GuildID == "" || m.GuildID == settings.Policy.GuildID) {
			enqueueSource(sourceGatewayEvent{Delete: m.Message})
		}
	})
	s.AddHandler(func(_ *discordgo.Session, m *discordgo.MessageDeleteBulk) {
		if m.GuildID != "" && m.GuildID != settings.Policy.GuildID {
			return
		}
		for _, id := range m.Messages {
			enqueueSource(sourceGatewayEvent{Delete: &discordgo.Message{ID: id, ChannelID: m.ChannelID, GuildID: m.GuildID}})
		}
	})
	var wg sync.WaitGroup
	start := func(fn func()) { wg.Add(1); go func() { defer wg.Done(); fn() }() }
	start(func() {
		// Guild route failures are independent of owner DM readiness. Periodic
		// read-only revalidation also recovers a restored route without forcing
		// a reconnect or replaying any message POST.
		tick := time.NewTicker(30 * time.Second)
		defer tick.Stop()
		for {
			var epoch uint64
			select {
			case <-ctx.Done():
				return
			case epoch = <-validate:
			case <-tick.C:
				var ready bool
				ready, epoch = g.readiness()
				if !ready {
					continue
				}
			}
			guildWasReady := g.guildReadiness()
			if err := refreshGatewayReadiness(ctx, rest, s, settings, g, epoch); err != nil {
				fail(ClassifyGatewayError(err).Error())
				return
			}
			if !guildWasReady && g.guildReadiness() {
				if err := store.ResumeValidation(); err != nil {
					fail("gateway_validation_failed")
					return
				}
			}
			hub.Notify()
		}
	})
	start(func() {
		for {
			select {
			case <-ctx.Done():
				return
			case event := <-incoming:
				outcome, err := receiveSourceEvent(ctx, rest, store, settings, event)
				if err != nil {
					fail("gateway_ingest_failed")
					return
				}
				g.recordIngress(outcome)
				if outcome == "queue_full" {
					fail("gateway_inbound_capacity_exceeded")
				}
				hub.Notify()
				NotifyFile(settings.DBPath + ".sock.wake")
			}
		}
	})
	start(func() {
		if err := validationLoop(ctx, store, rest, hub, g, settings.DBPath); err != nil {
			if err.Error() == "authentication_failed" {
				fail("authentication_failed")
			} else {
				fail("gateway_validation_failed")
			}
		}
	})
	if !settings.KeepCatchupDisarmed {
		start(func() {
			if err := catchupLoop(ctx, store, rest, hub, g); err != nil {
				if err.Error() == "authentication_failed" {
					fail("authentication_failed")
				} else {
					fail("catchup_failed")
				}
			}
		})
	}
	start(func() {
		if err := messageSourceLoop(ctx, store, rest, hub, g); err != nil {
			fail("source_refresh_failed")
		}
	})
	startGatewayWriter(settings.ReceiveOnly, start, func() {
		if err := dispatchLoopMeasured(ctx, store, hub, g, func(ctx context.Context, c Chunk) (SendResult, Diagnostics) {
			epoch := g.epoch.Load()
			if c.Source.ReplyKind == "interaction" {
				return controls.Send(ctx, c, func() bool {
					return ctx.Err() == nil && g.ready.Load() && g.epoch.Load() == epoch && gatewayRoutePermissions(g, s, settings, c.Source)
				})
			}
			return rest.SendCurrentSourceMeasured(ctx, store, c, func() bool {
				return ctx.Err() == nil && g.ready.Load() && g.epoch.Load() == epoch && gatewayRoutePermissions(g, s, settings, c.Source) && store.controlRequestCurrent(c.Source)
			})
		}); err != nil {
			fail("dispatcher_failed")
		}
	})
	startGatewayWriter(settings.ReceiveOnly, start, func() {
		if err := diagnosticLoop(ctx, store, rest, hub, g, func() bool { return gatewayRoutePermissions(g, s, settings, Envelope{RouteKind: "guild_text"}) }); err != nil {
			fail("diagnostic_dispatch_failed")
		}
	})
	startGatewayWriter(settings.ReceiveOnly, start, func() {
		if err := feedbackLoop(ctx, store, rest, hub, g); err != nil {
			fail("feedback_persistence_failed")
		}
	})
	startGatewayWriter(settings.ReceiveOnly, start, func() {
		if err := typingLoop(ctx, store, rest, hub, g); err != nil {
			fail("typing_persistence_failed")
		}
	})
	start(func() {
		tick := time.NewTicker(3 * time.Second)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				d := g.health(rest)
				if err := persistGatewayHealth(store, d); err != nil {
					fail("runtime_persistence_failed")
					return
				}
			}
		}
	})
	startupEpoch := g.epoch.Load()
	if err := openGateway(startup, s, dial); err != nil {
		retErr = ClassifyGatewayError(err)
	}
	if retErr == nil {
		retErr = waitForGatewayReady(startup, g, startupEpoch, fatal)
	}
	if retErr == nil {
	running:
		for {
			select {
			case <-parent.Done():
				break running
			case err := <-fatal:
				retErr = err
				break running
			case <-disconnected:
				timer := time.NewTimer(time.Second)
				select {
				case <-ctx.Done():
					timer.Stop()
					break running
				case <-timer.C:
				}
				// One deadline covers Open AND the READY/RESUMED validation.
				// A live socket with incomplete state is not successful recovery.
				// On failure the existing process supervisor owns bounded retry.
				attempt, c := context.WithTimeout(ctx, 60*time.Second)
				epoch := g.epoch.Load()
				err := openGateway(attempt, s, dial)
				if err == nil {
					err = waitForGatewayReady(attempt, g, epoch, fatal)
				}
				c()
				if err != nil {
					if parent.Err() == nil {
						retErr = ClassifyGatewayError(err)
					}
					break running
				}
			}
		}
	}
	if parent.Err() != nil {
		retErr = nil
	} else if retErr == nil {
		select {
		case retErr = <-fatal:
		default:
		}
	}
	cancel()
	g.disconnect()
	dial.closeConnections()
	s.Close()
	wg.Wait()
	return retErr
}

// The epoch fence rejects stale validation and disconnects during an attempt.
// The caller's deadline covers the entire connection/validation attempt.
func waitForGatewayReady(ctx context.Context, g *gatewayState, epoch uint64, fatal <-chan error) error {
	tick := time.NewTicker(25 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case err := <-fatal:
			return err
		default:
		}
		if ctx.Err() != nil {
			select {
			case err := <-fatal:
				return err
			default:
			}
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return errors.New("gateway_startup_timeout")
			}
			return ctx.Err()
		}
		ready, currentEpoch := g.readiness()
		if currentEpoch != epoch {
			return errors.New("gateway_connection_failed")
		}
		if ready {
			return nil
		}
		select {
		case err := <-fatal:
			return err
		case <-ctx.Done():
			// fail() cancels ctx after publishing its permanent/transient code.
			// Preserve that code instead of misclassifying it as a timeout.
			select {
			case err := <-fatal:
				return err
			default:
			}
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return errors.New("gateway_startup_timeout")
			}
			return ctx.Err()
		case <-tick.C:
		}
	}
}

func (g *gatewayState) readiness() (bool, uint64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.ready.Load(), g.epoch.Load()
}

// Closing tracked TCP connections interrupts TLS, Hello and READY reads even
// while DiscordGo Open owns its session mutex. SDK reconnect is disabled; our
// cancellable supervisor is the sole owner of reconnect attempts.
type gatewayDialer struct {
	ctx   context.Context
	mu    sync.Mutex
	conns map[*gatewayConn]bool
}
type gatewayConn struct {
	net.Conn
	owner *gatewayDialer
}

func newGatewayDialer(ctx context.Context) *gatewayDialer {
	return &gatewayDialer{ctx: ctx, conns: map[*gatewayConn]bool{}}
}
func (d *gatewayDialer) dial(ctx context.Context, network, address string) (net.Conn, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(d.ctx, cancel)
	defer stop()
	raw, e := (&net.Dialer{Timeout: 15 * time.Second, KeepAlive: 30 * time.Second}).DialContext(ctx, network, address)
	if e != nil {
		return nil, e
	}
	c := &gatewayConn{Conn: raw, owner: d}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.ctx.Err() != nil {
		raw.Close()
		return nil, d.ctx.Err()
	}
	d.conns[c] = true
	return c, nil
}
func (c *gatewayConn) Close() error {
	c.owner.mu.Lock()
	delete(c.owner.conns, c)
	c.owner.mu.Unlock()
	return c.Conn.Close()
}
func (c *gatewayConn) Read(p []byte) (int, error) {
	c.Conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	return c.Conn.Read(p)
}
func (d *gatewayDialer) closeConnections() {
	d.mu.Lock()
	list := make([]*gatewayConn, 0, len(d.conns))
	for c := range d.conns {
		list = append(list, c)
	}
	d.mu.Unlock()
	for _, c := range list {
		c.Close()
	}
}
func openGateway(ctx context.Context, s *discordgo.Session, d *gatewayDialer) error {
	s.Dialer.NetDialContext = func(parent context.Context, network, address string) (net.Conn, error) {
		dialCtx, cancel := context.WithCancel(parent)
		defer cancel()
		stop := context.AfterFunc(ctx, cancel)
		defer stop()
		return d.dial(dialCtx, network, address)
	}
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		done := make(chan error, 1)
		go func() { done <- s.Open() }()
		select {
		case err := <-done:
			if !errors.Is(err, discordgo.ErrGatewayReconnect) {
				return err
			}
			// Invalid-session retry jitter follows Discord's 1–5 second
			// guidance. Every attempt shares the caller's original deadline.
			timer := time.NewTimer(time.Duration(1+rand.IntN(5)) * time.Second)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			}
		case <-ctx.Done():
			d.closeConnections()
			<-done // Patched handshake paths never re-lock Open's mutex.
			return ctx.Err()
		}
	}
}

func receiveMessage(ctx context.Context, rest *RESTClient, store *Store, s Settings, m *discordgo.Message) (string, error) {
	if m == nil || m.Author == nil || m.Author.ID != s.Policy.OwnerID || m.Author.Bot || m.WebhookID != "" || (m.Type != discordgo.MessageTypeDefault && m.Type != discordgo.MessageTypeReply) || (m.GuildID != "" && m.GuildID != s.Policy.GuildID) {
		return "rejected", nil
	}
	event := projectGatewayMessage(s, m)
	if !s.Policy.Stages(event) {
		return "rejected", nil
	}
	// Durable quarantine precedes all network lookup. It is deliberately separate
	// from the consumable inbox; exact validation happens in validationLoop.
	return store.StageIngress(event)
}

func projectGatewayMessage(s Settings, m *discordgo.Message) Envelope {
	event := Envelope{Media: ProjectMedia(m), ContentHash: MessageContentFingerprint(m), Context: projectMessageContext(m), Platform: "discord", EventID: m.ID, ConversationID: m.ChannelID, SenderID: m.Author.ID, Text: m.Content, ReceivedAt: wall(), RouteKind: "dm", GuildID: m.GuildID, SenderIsBot: m.Author.Bot}
	if m.GuildID != "" {
		event.RouteKind = "guild_text"
		if m.ChannelID != s.Policy.GuildChannelID {
			event.RouteKind = "guild_thread_candidate"
		}
	}
	for _, u := range m.Mentions {
		if u != nil && u.ID == s.ExpectedBotID {
			event.BotMentioned = true
		}
	}
	if m.MessageReference != nil {
		event.ReplyToEventID = m.MessageReference.MessageID
	}
	return event
}

// Drain immediately after each acknowledgement. A maintenance timer only covers
// a producer crash between SQLite COMMIT and its private-socket notification.
func dispatchLoop(ctx context.Context, store *Store, hub *WakeHub, g *gatewayState, send func(context.Context, Chunk) SendResult) error {
	return dispatchLoopMeasured(ctx, store, hub, g, func(ctx context.Context, c Chunk) (SendResult, Diagnostics) {
		return send(ctx, c), Diagnostics{}
	})
}

func dispatchLoopMeasured(ctx context.Context, store *Store, hub *WakeHub, g *gatewayState, send func(context.Context, Chunk) (SendResult, Diagnostics)) error {
	wake, unsub := hub.Subscribe()
	defer unsub()
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		for g.ready.Load() && ctx.Err() == nil {
			c, e := store.NextChunk()
			if e != nil {
				return e
			}
			if c == nil {
				break
			}
			started := time.Now()
			result, measurement := send(ctx, *c)
			if e = store.RecordResultMeasured(*c, result, measurement); e != nil {
				return e
			}
			store.RecordTiming("send", time.Since(started))
			hub.Notify()
		}
		select {
		case <-ctx.Done():
			return nil
		case <-wake:
		case <-tick.C:
		}
	}
}

func feedbackLoop(ctx context.Context, store *Store, rest *RESTClient, hub *WakeHub, g *gatewayState) error {
	wake, unsub := hub.Subscribe()
	defer unsub()
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	retry := map[string]time.Time{}
	attempt := map[string]int{}
	blocked := map[string]bool{}
	lastEpoch := g.epoch.Load()
	for {
		if epoch := g.epoch.Load(); epoch != lastEpoch {
			blocked = map[string]bool{}
			retry = map[string]time.Time{}
			attempt = map[string]int{}
			lastEpoch = epoch
		}
		if g.ready.Load() {
			rows, e := store.FeedbackRows()
			if e != nil {
				return e
			}
			if e == nil {
				for _, r := range rows {
					target := "received"
					if r.Delivery == "sent" || r.Delivery == "failed" || r.Delivery == "uncertain" {
						target = r.Delivery
					} else if r.State == "ignored" {
						target = "ignored"
					}
					key := r.ID + ":" + target
					if r.FeedbackStage == target || blocked[key] || time.Now().Before(retry[key]) {
						continue
					}
					c, cancel := context.WithTimeout(ctx, 20*time.Second)
					err := updateFeedback(c, rest, r.Event, r.FeedbackStage, target)
					cancel()
					if err == nil {
						if err := store.RecordFeedback(r.ID, target); err != nil {
							return err
						}
						delete(retry, key)
						delete(attempt, key)
					} else {
						attempt[key]++
						n := attempt[key]
						if n > 4 {
							n = 4
						}
						retry[key] = time.Now().Add(time.Duration(1<<n) * time.Second)
						if feedbackPermissionDenied(err) {
							blocked[key] = true
						}
					}
					if ctx.Err() != nil {
						return nil
					}
				}
			}
		}
		select {
		case <-ctx.Done():
			return nil
		case <-wake:
		case <-tick.C:
		}
	}
}
func updateFeedback(ctx context.Context, rest *RESTClient, e Envelope, old, target string) error {
	if (old == "failed" || old == "uncertain") && target != old {
		if err := rest.RemoveReaction(ctx, e, "❌"); err != nil {
			return err
		}
	}
	if target == "received" {
		return rest.Reaction(ctx, e, "👀")
	}
	if err := rest.RemoveReaction(ctx, e, "👀"); err != nil {
		return err
	}
	if target == "ignored" || target == "sent" {
		// The reply itself is successful completion feedback. Clear receipt
		// (and any earlier failure above), without adding a success reaction.
		return nil
	}
	return rest.Reaction(ctx, e, "❌")
}

func diagnosticLoop(ctx context.Context, store *Store, rest *RESTClient, hub *WakeHub, g *gatewayState, permissions func() bool) error {
	wake, unsub := hub.Subscribe()
	defer unsub()
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		for g.ready.Load() && ctx.Err() == nil {
			d, e := store.NextDiagnostic()
			if e != nil {
				return e
			}
			if d == nil {
				break
			}
			epoch := g.epoch.Load()
			r, metrics := rest.SendDiagnosticMeasured(ctx, *d, func() bool { return ctx.Err() == nil && g.ready.Load() && epoch == g.epoch.Load() && permissions() })
			if e = store.RecordDiagnosticResult(*d, r, metrics); e != nil {
				return e
			}
			hub.Notify()
		}
		select {
		case <-ctx.Done():
			return nil
		case <-wake:
		case <-tick.C:
		}
	}
}

type typingJob struct {
	cancel   context.CancelFunc
	deadline float64
	done     chan struct{}
	epoch    uint64
	denied   *atomic.Bool
}

func feedbackPermissionDenied(err error) bool {
	if err == nil {
		return false
	}
	switch err.Error() {
	case "feedback_http_401", "feedback_http_403", "preflight_http_401", "preflight_http_403":
		return true
	}
	return false
}

func typingLoop(ctx context.Context, store *Store, rest *RESTClient, hub *WakeHub, g *gatewayState) error {
	wake, unsub := hub.Subscribe()
	defer unsub()
	tick := time.NewTicker(250 * time.Millisecond)
	defer tick.Stop()
	jobs := map[string]typingJob{}
	// A denied permission belongs to the connection epoch, not a processing
	// lease. Extending real work must not retry a denied endpoint.
	blocked := map[string]uint64{}
	lastReadyEpoch := g.epoch.Load()
	defer func() {
		for _, j := range jobs {
			j.cancel()
			<-j.done
		}
	}()
	for {
		wanted := map[string]FeedbackRow{}
		if g.ready.Load() {
			if epoch := g.epoch.Load(); epoch != lastReadyEpoch {
				blocked = map[string]uint64{}
				lastReadyEpoch = epoch
			}
			rows, e := store.FeedbackRows()
			if e != nil {
				return e
			}
			if e == nil {
				for _, r := range rows {
					if r.Thinking {
						wanted[r.Event.ConversationID] = r
					}
				}
			}
		}
		for id, j := range jobs {
			finished := false
			select {
			case <-j.done:
				finished = true
			default:
			}
			r, ok := wanted[id]
			deadline := r.ProcessingUntil
			if r.LeaseUntil < deadline {
				deadline = r.LeaseUntil
			}
			if !ok || deadline != j.deadline || finished || j.epoch != g.epoch.Load() {
				j.cancel()
				<-j.done
				if j.denied.Load() && j.epoch == g.epoch.Load() {
					blocked[id] = j.epoch
				}
				delete(jobs, id)
			}
		}
		for id, r := range wanted {
			if epoch, denied := blocked[id]; denied && epoch == g.epoch.Load() {
				continue
			}
			if _, ok := jobs[id]; ok {
				continue
			}
			deadline := r.ProcessingUntil
			if r.LeaseUntil < deadline {
				deadline = r.LeaseUntil
			}
			c, cancel := context.WithDeadline(ctx, time.Unix(0, int64(deadline*1e9)))
			done := make(chan struct{})
			denied := &atomic.Bool{}
			jobs[id] = typingJob{cancel: cancel, deadline: deadline, done: done, epoch: g.epoch.Load(), denied: denied}
			go func(event Envelope) {
				defer close(done)
				timer := time.NewTimer(0)
				defer timer.Stop()
				failures := 0
				for {
					select {
					case <-c.Done():
						return
					case <-timer.C:
						if !g.ready.Load() {
							return
						}
						if err := rest.Typing(c, event); err != nil {
							if feedbackPermissionDenied(err) {
								denied.Store(true)
								return
							}
							failures++
							if failures > 4 {
								failures = 4
							}
							timer.Reset(time.Duration(1<<failures) * time.Second)
							continue
						}
						failures = 0
						timer.Reset(8 * time.Second)
					}
				}
			}(r.Event)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-wake:
		case <-tick.C:
		}
	}
}
