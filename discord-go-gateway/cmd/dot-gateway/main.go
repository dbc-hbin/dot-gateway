package main

import (
	"context"
	"dot-gateway/internal/bridge"
	"encoding/json"
	"errors"
	"io"
	"math"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func output(v any) error { return json.NewEncoder(os.Stdout).Encode(v) }

var runGateway = bridge.RunGateway

func main() {
	// Report a failed stdout write when possible rather than taking Go's default
	// SIGPIPE exit. Either outcome still leaves committed claims recoverable.
	signal.Ignore(syscall.SIGPIPE)
	code := run(os.Args[1:])
	os.Exit(code)
}

// Parse flags independently of position, preserving the Python CLI contract.
func parse(args []string) (string, []string, map[string]string, error) {
	if len(args) == 0 {
		return "", nil, nil, errors.New("command_required")
	}
	cmd := args[0]
	pos := []string{}
	flags := map[string]string{}
	bools := map[string]bool{"begin": true, "verified-in-discord": true}
	for i := 1; i < len(args); i++ {
		a := args[i]
		if !strings.HasPrefix(a, "--") {
			pos = append(pos, a)
			continue
		}
		k, v, has := strings.Cut(strings.TrimPrefix(a, "--"), "=")
		if _, ok := flags[k]; ok {
			return "", nil, nil, errors.New("duplicate_flag")
		}
		if !has {
			if bools[k] {
				v = "true"
			} else {
				i++
				if i >= len(args) {
					return "", nil, nil, errors.New("flag_value_required")
				}
				v = args[i]
			}
		}
		flags[k] = v
	}
	return cmd, pos, flags, nil
}
func run(args []string) int {
	cmd, pos, f, e := parse(args)
	if e != nil {
		output(map[string]string{"error": e.Error()})
		return 2
	}
	allowed := map[string]string{"check": "", "run-discord": "", "gateway": "", "status": "", "next": "wait lease-seconds begin processing-seconds consumer-id", "reply": "claim text-file", "renew": "claim lease-seconds", "begin": "claim lease-seconds", "ignore": "claim", "delivery": "", "retry-failed": "", "resolve-sent": "chunk message-id verified-in-discord", "test-send-status": ""}
	allowed["diagnostic-send"] = "index"
	allowed["diagnostic-status"] = "index"
	spec, ok := allowed[cmd]
	if !ok {
		output(map[string]string{"error": "unsupported_command"})
		return 2
	}
	for k := range f {
		if !strings.Contains(" "+spec+" ", " "+k+" ") {
			output(map[string]string{"error": "unknown_flag"})
			return 2
		}
	}
	expected := 0
	switch cmd {
	case "reply", "renew", "begin", "ignore", "delivery", "retry-failed", "resolve-sent":
		expected = 1
	}
	if len(pos) != expected {
		output(map[string]string{"error": "invalid_arguments"})
		return 2
	}
	settings, e := bridge.LoadSettings(cmd == "gateway" || cmd == "run-discord", cmd == "check")
	if e != nil {
		output(map[string]string{"error": e.Error()})
		return 2
	}
	if cmd == "check" {
		route, e := settings.ProxyConfig.DiscordRoute()
		if e != nil {
			output(map[string]string{"error": e.Error()})
			return 2
		}
		transport := "direct"
		if route != "" {
			transport = "proxy"
		}
		scope := "owner_only_dm"
		if settings.Policy.GuildID != "" {
			scope = "owner_dm_and_pinned_guild_channel"
		}
		output(map[string]any{"configuration": "valid", "scope": scope, "guild_id": nullable(settings.Policy.GuildID), "guild_channel_id": nullable(settings.Policy.GuildChannelID), "guild_mode": nullable(settings.Policy.GuildMode), "message_content_intent": settings.Policy.MessageContentApproved, "discord_transport": transport, "token_checked": false, "network_used": false})
		return 0
	}
	store, e := bridge.OpenStore(settings.DBPath, settings.Policy)
	if e != nil {
		output(map[string]string{"error": "local_store_failed"})
		return 2
	}
	defer store.Close()
	integer := func(k string, d int) (int, error) {
		if v, ok := f[k]; ok {
			return strconv.Atoi(v)
		}
		return d, nil
	}
	claim := f["claim"]
	if (cmd == "reply" || cmd == "renew" || cmd == "begin" || cmd == "ignore") && claim == "" {
		output(map[string]string{"error": "claim_required"})
		return 2
	}
	var result any
	mutated := false
	switch cmd {
	case "gateway", "run-discord":
		ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer cancel()
		e = runGateway(ctx, settings, store)
		result = map[string]string{"gateway": "stopped"}
	case "status":
		result, e = store.Status()
	case "next":
		consumer := os.Getenv("BRIDGE_CONSUMER_ID")
		if value, ok := f["consumer-id"]; ok {
			consumer = value
			if consumer == "" {
				e = errors.New("invalid_consumer_identity")
				break
			}
		}
		wait := 0.0
		if v, ok := f["wait"]; ok {
			wait, e = strconv.ParseFloat(v, 64)
		}
		if e != nil || math.IsNaN(wait) || math.IsInf(wait, 0) || wait < 0 || wait > 300 {
			e = errors.New("invalid_wait")
			break
		}
		lease, err := integer("lease-seconds", 300)
		if err != nil {
			e = errors.New("invalid_lease")
			break
		}
		begin := 0
		if f["begin"] == "true" {
			begin, e = integer("processing-seconds", 60)
			if e != nil || begin < 1 || begin > 300 {
				e = errors.New("invalid_processing_lease")
				break
			}
		}
		deadline := time.Now().Add(time.Duration(wait * float64(time.Second)))
		// A ready/recoverable claim, or a nonblocking check, needs no watcher.
		// In particular, do not put an IPC handshake ahead of durable work.
		item, err := store.ClaimNextForConsumer(lease, begin, consumer)
		if err != nil {
			e = err
			break
		}
		if item != nil {
			result = map[string]any{"message": item}
			mutated = true
			break
		}
		if !time.Now().Before(deadline) {
			result = map[string]any{"message": nil}
			break
		}
		// This observation proves only that this CLI is waiting, never model
		// liveness. Diagnostic failures must not prevent durable claim recovery.
		pollHealth, _ := store.StartConsumerPoll(consumer, deadline)
		if pollHealth != nil {
			defer pollHealth.Close()
		}
		// Subscribe before RECHECKING durable state below. An arrival between
		// the initial fast-path check and subscription cannot be missed.
		c, r, _ := bridge.WatchIPC(settings.DBPath + ".sock")
		if c != nil {
			defer c.Close()
		}
		fileHub := bridge.NewWakeHub()
		fileEvents, unsubscribe := fileHub.Subscribe()
		defer unsubscribe()
		fileWatch, _ := bridge.StartFileWake(settings.DBPath+".sock.wake", fileHub)
		if fileWatch != nil {
			defer fileWatch.Close()
		}
		for {
			if pollHealth != nil {
				_ = pollHealth.Touch()
			}
			var item *bridge.Claim
			item, e = store.ClaimNextForConsumer(lease, begin, consumer)
			if e != nil {
				break
			}
			if item != nil {
				result = map[string]any{"message": item}
				mutated = true
				break
			}
			if !time.Now().Before(deadline) {
				result = map[string]any{"message": nil}
				break
			}
			if c != nil {
				until := time.Now().Add(time.Second)
				if deadline.Before(until) {
					until = deadline
				}
				c.SetReadDeadline(until)
				if _, err = r.ReadString('\n'); err != nil {
					if ne, ok := err.(interface{ Timeout() bool }); !ok || !ne.Timeout() {
						c.Close()
						c = nil
					}
				}
			} else {
				remaining := time.Until(deadline)
				if remaining > 250*time.Millisecond {
					remaining = 250 * time.Millisecond
				}
				timer := time.NewTimer(remaining)
				select {
				case <-fileEvents:
					timer.Stop()
				case <-timer.C:
				}
			}
		}
		if pollHealth != nil {
			_ = pollHealth.Close()
		}
	case "reply":
		var reader io.Reader = os.Stdin
		if p, ok := f["text-file"]; ok && p != "-" {
			var file *os.File
			file, e = os.Open(p)
			if e != nil {
				e = errors.New("reply_file_failed")
				break
			}
			defer file.Close()
			reader = file
		}
		var data []byte
		data, e = io.ReadAll(io.LimitReader(reader, 64005))
		if e != nil {
			e = errors.New("reply_read_failed")
			break
		}
		var id string
		id, e = store.QueueReply(pos[0], claim, string(data))
		if e == nil {
			result, e = store.Delivery(id)
			mutated = true
		}
	case "renew", "begin":
		d := 300
		if cmd == "begin" {
			d = 60
		}
		var seconds int
		seconds, e = integer("lease-seconds", d)
		if e != nil {
			e = errors.New("invalid_lease")
			break
		}
		if cmd == "begin" {
			e = store.BeginProcessing(pos[0], claim, seconds)
			result = map[string]any{"processing": true, "lease_seconds": seconds}
		} else {
			e = store.Renew(pos[0], claim, seconds)
			result = map[string]bool{"renewed": true}
		}
		mutated = e == nil
	case "ignore":
		e = store.Ignore(pos[0], claim)
		result = map[string]bool{"ignored": true}
		mutated = e == nil
	case "delivery":
		result, e = store.Delivery(pos[0])
	case "retry-failed":
		var n int
		n, e = store.RetryFailed(pos[0])
		result = map[string]int{"requeued_chunks": n}
		mutated = e == nil
	case "resolve-sent":
		if f["verified-in-discord"] != "true" || f["message-id"] == "" {
			e = errors.New("verification_required")
			break
		}
		var index int
		index, e = integer("chunk", -1)
		if e == nil {
			e = store.ResolveSent(pos[0], index, f["message-id"])
		}
		if e == nil {
			result, e = store.Delivery(pos[0])
			mutated = true
		}
	case "test-send-status":
		result, e = store.TestSendStatus(settings)
	case "diagnostic-send", "diagnostic-status":
		var index int
		index, e = integer("index", 0)
		if e != nil || index < 1 || index > 3 {
			e = errors.New("diagnostic_index_must_be_1_to_3")
			break
		}
		if cmd == "diagnostic-send" {
			result, e = store.QueueDiagnostic(settings, index-1)
			mutated = e == nil
		} else {
			result, e = store.DiagnosticStatus(index - 1)
		}
	}
	if e != nil {
		output(map[string]string{"error": safeError(e)})
		if cmd == "gateway" || cmd == "run-discord" {
			return bridge.GatewayExitCode(e)
		}
		return 2
	}
	if mutated {
		bridge.NotifyIPC(settings.DBPath + ".sock")
	}
	if err := output(result); err != nil {
		// The operation may already be committed. Do not release a claim or retry
		// a send on output failure; next with the same identity recovers the claim.
		io.WriteString(os.Stderr, "result_output_failed\n")
		return 1
	}
	return 0
}
func nullable(s string) any {
	if s == "" {
		return nil
	}
	return s
}
func safeError(e error) string {
	s := e.Error()
	if len(s) > 120 {
		return "operation_failed"
	}
	for _, r := range s {
		if !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '_' || r == ' ') {
			return "operation_failed"
		}
	}
	return s
}
