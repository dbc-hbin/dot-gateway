package bridge

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/bwmarrin/discordgo"
)

type InteractionService struct {
	settings  Settings
	store     *Store
	rest      *RESTClient
	transport *interactionTransport
	notify    func()
}

func NewInteractionService(s Settings, store *Store, notify func()) (*InteractionService, error) {
	r, err := NewRESTClient(s)
	if err != nil {
		return nil, err
	}
	t, err := newInteractionTransport(s)
	if err != nil {
		r.Close()
		return nil, err
	}
	r.controlStore = store
	t.onExpire = func(id string) {
		_ = store.expireInteractionID(id)
		if notify != nil {
			notify()
		}
	}
	return &InteractionService{s, store, r, t, notify}, nil
}
func (s *InteractionService) Close() { s.rest.Close(); s.transport.close() }
func (s *InteractionService) route(ctx context.Context, id, channel, guild, owner string) (Envelope, error) {
	e := Envelope{Platform: "discord", EventID: id, ConversationID: channel, SenderID: owner, ReceivedAt: wall(), RouteKind: "dm", GuildID: guild, BotMentioned: true, Text: "route validation"}
	if guild != "" {
		e.RouteKind = "guild_text"
		if channel != s.settings.Policy.GuildChannelID {
			e.RouteKind = "guild_thread_candidate"
		}
	}
	if !s.settings.Policy.Stages(e) {
		return e, errInteractionRejected
	}
	c, err := s.rest.Channel(ctx, channel)
	if err != nil {
		return e, err
	}
	return s.rest.validateIngressRoute(ctx, c, e)
}
func interactionOwner(i *discordgo.Interaction) string {
	if i == nil {
		return ""
	}
	if i.GuildID != "" {
		if i.Member == nil || i.Member.User == nil || i.Member.User.Bot || i.User != nil {
			return ""
		}
		return i.Member.User.ID
	}
	if i.User == nil || i.User.Bot || i.Member != nil {
		return ""
	}
	return i.User.ID
}
func initialDeadline(id string, received time.Time) (time.Time, error) {
	n, err := strconv.ParseUint(id, 10, 64)
	if err != nil {
		return time.Time{}, errInteractionRejected
	}
	created := time.UnixMilli(int64(n>>22) + 1420070400000)
	if created.After(received.Add(time.Second)) {
		return time.Time{}, errInteractionRejected
	}
	deadline := created.Add(2800 * time.Millisecond)
	if local := received.Add(2400 * time.Millisecond); local.Before(deadline) {
		deadline = local
	}
	if !received.Before(deadline) {
		return time.Time{}, errInteractionRejected
	}
	return deadline, nil
}

// Handle must be launched immediately in a bounded Gateway worker, independently
// of ordinary messages and model latency. An unvalidated route is never deferred.
func (s *InteractionService) Handle(parent context.Context, i *discordgo.Interaction, received time.Time) string {
	if i == nil || i.AppID != s.settings.ExpectedBotID || interactionOwner(i) != s.settings.Policy.OwnerID || !Snowflake(i.ID) || !safeInteractionToken(i.Token) {
		return "rejected"
	}
	deadline, err := initialDeadline(i.ID, received)
	if err != nil {
		return "expired"
	}
	ctx, cancel := context.WithDeadline(parent, deadline)
	defer cancel()
	route, err := s.route(ctx, i.ID, i.ChannelID, i.GuildID, interactionOwner(i))
	if err != nil {
		return "rejected"
	}
	action, prompt, request, binding := "", "", "", ""
	switch i.Type {
	case discordgo.InteractionApplicationCommand:
		d, ok := i.Data.(discordgo.ApplicationCommandInteractionData)
		if !ok || d.CommandType != discordgo.ChatApplicationCommand || !Snowflake(d.ID) {
			return "rejected"
		}
		action = d.Name
		if action != "ask" && action != "status" && action != "cancel" {
			return "rejected"
		}
		if !s.store.commandIDMatches(i.GuildID, action, d.ID) {
			return "rejected"
		}
		seen := map[string]bool{}
		for _, opt := range d.Options {
			if opt == nil || seen[opt.Name] || len(opt.Options) > 0 || opt.Type != discordgo.ApplicationCommandOptionString {
				return "rejected"
			}
			seen[opt.Name] = true
			v, ok := opt.Value.(string)
			if !ok {
				return "rejected"
			}
			switch {
			case action == "ask" && opt.Name == "prompt":
				prompt = v
			case (action == "status" || action == "cancel") && opt.Name == "request":
				request = v
			default:
				return "rejected"
			}
		}
		if !utf8.ValidString(prompt) || TextUnits(prompt) > 4000 || len(request) > 32 {
			return "rejected"
		}
		if action == "ask" && trimText(prompt) == "" {
			action = "open_ask"
		}
	case discordgo.InteractionModalSubmit:
		d, ok := i.Data.(discordgo.ModalSubmitInteractionData)
		if !ok || len(d.Components) != 1 {
			return "rejected"
		}
		row, ok := d.Components[0].(*discordgo.ActionsRow)
		if !ok || len(row.Components) != 1 {
			return "rejected"
		}
		input, ok := row.Components[0].(*discordgo.TextInput)
		if !ok || input.CustomID != "prompt" || trimText(input.Value) == "" || TextUnits(input.Value) > 4000 {
			return "rejected"
		}
		action, prompt, binding = "ask", input.Value, d.CustomID
	case discordgo.InteractionMessageComponent:
		d, ok := i.Data.(discordgo.MessageComponentInteractionData)
		if !ok || d.ComponentType != discordgo.ButtonComponent || len(d.Values) != 0 || i.Message == nil || i.Message.Author == nil || i.Message.Author.ID != s.settings.ExpectedBotID || !i.Message.Author.Bot || i.Message.ChannelID != route.ConversationID {
			return "rejected"
		}
		action, binding = "cancel", d.CustomID
	default:
		return "rejected"
	}
	fresh, err := s.store.reserveInteraction(i.ID, route, action)
	if err != nil {
		return "store_failed"
	}
	if !fresh {
		return "duplicate"
	}
	finish := func(state string) string { _ = s.store.interactionState(i.ID, state); return state }
	if binding != "" {
		kind := "ask"
		message := ""
		if action == "cancel" {
			kind = "cancel"
			message = i.Message.ID
		}
		b, err := s.store.consumeBinding(binding, route, kind, message)
		if err != nil {
			return finish("rejected")
		}
		request = b.Request
	}
	if action == "open_ask" {
		b, err := s.store.newBinding(route, "ask", "", "", "")
		if err != nil {
			return finish("store_failed")
		}
		body := map[string]any{"type": 9, "data": map[string]any{"custom_id": b.ID, "title": "Ask dot", "components": []any{map[string]any{"type": 1, "components": []any{map[string]any{"type": 4, "custom_id": "prompt", "label": "What would you like me to do?", "style": 2, "min_length": 1, "max_length": 4000, "required": true}}}}}}
		return finish(s.transport.callback(ctx, i.ID, i.Token, body))
	}
	rejectReply := func(state, message string) string {
		callback := s.transport.callback(ctx, i.ID, i.Token, map[string]any{"type": 4, "data": interactionBody(message, nil)})
		if callback != "sent" {
			return finish("ack_" + callback)
		}
		return finish(state)
	}
	var selected *ControlRequest
	if action == "cancel" {
		var requests []ControlRequest
		if request == "" {
			requests, err = s.store.activeControlRequests(route)
		} else {
			requests, err = s.store.ControlRequests(route, request)
		}
		if err != nil {
			return rejectReply("rejected", "No matching request in this conversation.")
		}
		for _, r := range requests {
			if r.State == "pending" || r.State == "processing" || r.State == "sending" || r.State == "uncertain" || r.State == "failed" || r.State == "cancel_requested" || (r.State == "recovery_required" || r.Worker != nil && !workerStopped(r.Worker.AttestedState)) {
				if selected != nil {
					return rejectReply("ambiguous_request", "More than one request is active. Use /status, then cancel an exact request ID.")
				}
				copy := r
				selected = &copy
			}
		}
		if selected == nil {
			return rejectReply("no_active_request", "No matching active request in this conversation.")
		}
	}
	if ctx.Err() != nil {
		return finish("expired")
	}
	state := s.transport.callback(ctx, i.ID, i.Token, map[string]any{"type": 5, "data": map[string]any{"flags": 64}})
	if state != "sent" {
		return finish("ack_" + state)
	}
	if err = s.store.interactionState(i.ID, "acknowledged"); err != nil {
		return "store_failed"
	}
	s.transport.remember(i.ID, i.Token)
	if action != "ask" {
		defer s.transport.forget(i.ID)
	}
	// The initial deadline is over after acknowledged defer. This fresh scope is
	// short and still uses the independent transport; no public fallback exists.
	work, done := context.WithTimeout(parent, 5*time.Second)
	defer done()
	switch action {
	case "ask":
		id, err := uuidHex()
		if err != nil {
			return finish("store_failed")
		}
		route.Text, route.ReplyKind = prompt, "interaction"
		route.Control = encodeControl(ControlEvent{Version: 1, Kind: "ask", ID: id, InteractionID: i.ID, ActorID: route.SenderID})
		_, err = s.store.ingestControl(route, i.ID)
		if err != nil {
			s.transport.forget(i.ID)
			return finish("queue_failed")
		}
		if s.notify != nil {
			s.notify()
		}
		return "queued"
	case "cancel":
		r, err := s.store.CancelControlRequest(route, selected.ID, selected.Revision)
		if err != nil {
			return finish("cancel_failed")
		}
		text := "Request " + r.ID + ": " + r.State + ". Already sent messages and external actions are unchanged."
		if r.State == "cancel_requested" {
			text = "Request " + r.ID + ": cancellation requested; unsent output is suppressed. Worker interruption is not yet acknowledged."
			if r.Cancellation != nil && r.Cancellation.ExecutionState == "worker_unbound" {
				text += " No controller binding is known; stopping the worker needs controller reconciliation."
			}
			if r.DeliveryState == "uncertain" || r.DeliveryState == "sending" {
				text += " An in-flight or uncertain delivery remains unresolved."
			}
		} else if r.State == "uncertain" || r.State == "sending" {
			text = "Request " + r.ID + ": cancellation recorded; an in-flight or uncertain delivery is unresolved."
		}
		_, state = s.transport.edit(work, i.ID, route.ConversationID, text, nil)
		if s.notify != nil {
			s.notify()
		}
		return finish("result_" + state)
	case "status":
		requests, err := s.store.ControlRequests(route, request)
		if err != nil {
			_, state = s.transport.edit(work, i.ID, route.ConversationID, "No matching request in this conversation.", nil)
			return finish("result_" + state)
		}
		lines := []string{"Request status (execution cancellation and delivery):"}
		components := []any{}
		for n, r := range requests {
			if n >= 10 {
				lines = append(lines, "More requests omitted; use an exact request ID.")
				break
			}
			line := fmt.Sprintf("%s: %s", r.ID, r.State)
			if r.Cancellation != nil {
				line += "; cancellation=" + r.Cancellation.State + " (" + r.Cancellation.ExecutionState + ")"
			}
			if r.Worker != nil {
				line += "; worker=" + r.Worker.State + " (controller-attested)"
			}
			if r.DeliveryState != "" {
				line += "; delivery=" + r.DeliveryState
			}
			lines = append(lines, line)
			if len(components) < 5 && (r.State == "pending" || r.State == "processing" || r.State == "recovery_required" || r.Worker != nil && !workerStopped(r.Worker.AttestedState)) {
				b, err := s.store.newBinding(route, "cancel", r.ID, r.Revision, "")
				if err == nil {
					components = append(components, map[string]any{"type": 2, "style": 4, "label": "Cancel " + r.ID[:8], "custom_id": b.ID})
				}
			}
		}
		if len(requests) == 0 {
			lines = append(lines, "No requests in this conversation.")
		}
		var rows any
		if len(components) > 0 {
			rows = []any{map[string]any{"type": 1, "components": components}}
		}
		mid, state := s.transport.edit(work, i.ID, route.ConversationID, strings.Join(lines, "\n"), rows)
		if state == "sent" && len(components) > 0 {
			for _, item := range components {
				custom := item.(map[string]any)["custom_id"].(string)
				_ = s.store.bindControlMessage(custom, mid)
			}
		}
		return finish("result_" + state)
	}
	return finish("rejected")
}
func (s *Store) bindControlMessage(id, message string) error {
	_, err := s.call(func(db *storeConn) (any, error) {
		_, err := db.Exec("UPDATE control_bindings SET message=? WHERE id=? AND used=0 AND message=''", message, id)
		return nil, err
	})
	return err
}
func (s *InteractionService) Send(ctx context.Context, c Chunk, guard func() bool) (SendResult, Diagnostics) {
	if c.Source.ReplyKind != "interaction" {
		return SendResult{State: "failed", Code: "not_interaction"}, Diagnostics{}
	}
	start := time.Now()
	d := Diagnostics{Operation: "interaction_send"}
	channel, err := s.rest.Channel(ctx, c.Source.ConversationID)
	if err == nil {
		err = s.rest.validateRoute(ctx, channel, c.Source)
	}
	d.PreflightSeconds = time.Since(start).Seconds()
	var result SendResult
	if err != nil {
		result = SendResult{State: "failed", Code: "interaction_route_unavailable"}
	} else {
		result = s.transport.send(ctx, c, func() bool { return (guard == nil || guard()) && s.store.controlRequestCurrent(c.Source) })
	}
	d.State, d.Code, d.Seconds = result.State, result.Code, time.Since(start).Seconds()
	return result, d
}
