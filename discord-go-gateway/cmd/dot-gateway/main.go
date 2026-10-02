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
	bools := map[string]bool{"begin": true, "verified-in-discord": true, "dry-run": true, "fetch": true, "apply-reviewed": true}
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
	trace := newPhaseTrace(os.Getenv("BRIDGE_PHASE_TRACE") == "1")
	defer trace.flush()
	output := func(v any) error {
		trace.mark("stdout_ready")
		trace.mark("stdout_write_started")
		err := output(v)
		if err == nil {
			trace.mark("stdout_write_finished")
		} else {
			trace.mark("stdout_write_failed")
		}
		return err
	}
	cmd, pos, f, e := parse(args)
	if e != nil {
		output(map[string]string{"error": e.Error()})
		return 2
	}
	allowed := map[string]string{"check": "", "run-discord": "", "gateway": "", "status": "", "next": "wait lease-seconds begin processing-seconds consumer-id", "reply": "claim text-file manifest-file", "followup": "claim key text-file manifest-file", "renew": "claim lease-seconds", "begin": "claim lease-seconds", "ignore": "claim", "delivery": "", "retry-failed": "", "cancel-reply": "", "resolve-sent": "chunk message-id verified-in-discord", "test-send-status": ""}
	allowed["read-message"] = ""
	allowed["read-history"] = "before limit"
	allowed["read-pins"] = "before limit"
	allowed["search-messages"] = "query offset limit"
	allowed["catchup-status"] = ""
	allowed["reply-output-dir"] = ""
	allowed["prune-reply-spool"] = ""
	allowed["verify-reply"] = "chunk"
	allowed["reconcile-reply"] = "chunk message-id verified-in-discord"
	allowed["recover-thread-message"] = ""
	allowed["materialize"] = "claim attachment"
	allowed["register-commands"] = "dry-run snapshot-file fetch apply-reviewed plan-file"
	allowed["bind-response"] = "claim message-id"
	allowed["message-operation"] = "claim json-file"
	allowed["message-operation-status"] = ""
	allowed["abandon-message-operation"] = "claim operation-id"
	allowed["reconcile-message-operation"] = "claim operation-id verified-in-discord"
	allowed["memory-search"] = "claim query"
	allowed["memory-put"] = "claim json-file"
	allowed["memory-get"] = "claim key"
	allowed["memory-forget"] = "claim document revision"
	allowed["memory-backfill"] = "limit"
	allowed["memory-export"] = ""
	allowed["memory-import"] = "sha256"
	allowed["diagnostic-send"] = "index"
	allowed["diagnostic-status"] = "index"
	allowed["worker-register"] = "incarnation controller state lease-seconds previous-incarnation evidence-ref"
	allowed["worker-observe"] = "incarnation controller state lease-seconds evidence-ref"
	allowed["worker-bind"] = "claim worker-id incarnation controller evidence-ref"
	allowed["worker-status"] = ""
	allowed["worker-cancellations"] = "incarnation controller"
	allowed["worker-cancel-ack"] = "worker-id incarnation controller evidence-ref"
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
	case "worker-register", "worker-observe", "worker-bind", "worker-cancellations", "worker-cancel-ack":
		expected = 1
	case "recover-thread-message", "read-message":
		expected = 2
	case "read-history", "read-pins", "search-messages":
		expected = 1
	case "message-operation", "message-operation-status", "abandon-message-operation", "reconcile-message-operation", "reply", "followup", "renew", "begin", "ignore", "delivery", "retry-failed", "cancel-reply", "resolve-sent", "materialize", "bind-response", "verify-reply", "reconcile-reply", "memory-search", "memory-put", "memory-get", "memory-forget", "memory-export", "memory-import":
		expected = 1
	}
	if len(pos) != expected {
		output(map[string]string{"error": "invalid_arguments"})
		return 2
	}
	settings, e := bridge.LoadSettings(cmd == "message-operation" || cmd == "reconcile-message-operation" || cmd == "read-message" || cmd == "read-history" || cmd == "read-pins" || cmd == "search-messages" || cmd == "gateway" || cmd == "run-discord" || cmd == "recover-thread-message" || cmd == "materialize" || cmd == "verify-reply" || cmd == "reconcile-reply" || cmd == "register-commands" && (f["fetch"] == "true" || f["apply-reviewed"] == "true"), cmd == "check")
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
			scope = "owner_dm_and_pinned_guild_channel_and_verified_child_threads"
		}
		output(map[string]any{"configuration": "valid", "scope": scope, "guild_id": nullable(settings.Policy.GuildID), "guild_channel_id": nullable(settings.Policy.GuildChannelID), "guild_mode": nullable(settings.Policy.GuildMode), "message_content_intent": settings.Policy.MessageContentApproved, "discord_transport": transport, "token_checked": false, "network_used": false})
		return 0
	}
	if cmd == "register-commands" && f["fetch"] != "true" && f["apply-reviewed"] != "true" {
		if f["dry-run"] != "true" || f["snapshot-file"] == "" {
			output(map[string]string{"error": "only_explicit_dry_run_supported"})
			return 2
		}
		file, err := os.Open(f["snapshot-file"])
		if err != nil {
			output(map[string]string{"error": "command_snapshot_read_failed"})
			return 2
		}
		raw, err := io.ReadAll(io.LimitReader(file, 1048577))
		file.Close()
		if err != nil {
			output(map[string]string{"error": "command_snapshot_read_failed"})
			return 2
		}
		snapshot, err := bridge.ParseCommandSnapshot(raw)
		if err != nil {
			output(map[string]string{"error": "invalid_command_snapshot"})
			return 2
		}
		plan, err := bridge.PlanGuildCommands(settings, snapshot)
		if err != nil {
			output(map[string]string{"error": err.Error()})
			return 2
		}
		output(plan)
		return 0
	}
	trace.setPath(settings.DBPath)
	trace.mark("store_open_started")
	store, e := bridge.OpenStore(settings.DBPath, settings.Policy)
	trace.mark("store_open_finished")
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
	if (cmd == "abandon-message-operation" || cmd == "message-operation" || cmd == "reconcile-message-operation" || cmd == "reply" || cmd == "followup" || cmd == "renew" || cmd == "begin" || cmd == "ignore" || cmd == "materialize" || cmd == "bind-response" || cmd == "memory-search" || cmd == "memory-put" || cmd == "memory-get" || cmd == "memory-forget") && claim == "" {
		output(map[string]string{"error": "claim_required"})
		return 2
	}
	var result any
	mutated := false
	switch cmd {
	case "worker-register", "worker-observe", "worker-bind", "worker-status", "worker-cancellations", "worker-cancel-ack":
		result, e = runWorkerControl(cmd, pos, f, store)
		mutated = e == nil && cmd != "worker-status" && cmd != "worker-cancellations"
	case "catchup-status":
		result, e = store.CatchupStatus()
	case "read-message", "read-history", "read-pins", "search-messages":
		var rest *bridge.RESTClient
		rest, e = bridge.NewRESTClient(settings)
		if e != nil {
			break
		}
		defer rest.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		limit, err := integer("limit", 25)
		if err != nil {
			e = errors.New("invalid_limit")
			break
		}
		switch cmd {
		case "read-message":
			result, e = rest.ReadMessage(ctx, store, pos[0], pos[1])
		case "read-history":
			result, e = rest.ReadHistory(ctx, store, pos[0], f["before"], limit)
		case "read-pins":
			result, e = rest.ReadPins(ctx, store, pos[0], f["before"], limit)
		case "search-messages":
			offset, err := integer("offset", 0)
			if err != nil {
				e = errors.New("invalid_offset")
				break
			}
			result, e = rest.SearchMessages(ctx, store, pos[0], f["query"], offset, limit)
		}
	case "abandon-message-operation":
		result, e = store.AbandonMessageOperation(pos[0], claim, f["operation-id"])
	case "message-operation-status":
		result, e = store.MessageOperation(pos[0])
	case "message-operation", "reconcile-message-operation":
		result, e = runMessageOperation(cmd, pos[0], claim, f, settings, store)
		mutated = e == nil
	case "memory-export":
		result, e = store.ExportMemory(pos[0])
	case "memory-import":
		result, e = store.ImportMemory(pos[0], f["sha256"])
	case "memory-search":
		result, e = store.RecallMemory(pos[0], claim, f["query"])
	case "memory-get":
		result, e = store.MemoryFactState(pos[0], claim, f["key"])
	case "memory-forget":
		var revision int64
		revision, e = strconv.ParseInt(f["revision"], 10, 64)
		if e == nil && revision >= 0 {
			e = store.ForgetMemory(pos[0], claim, bridge.MemoryRef{DocumentID: f["document"], SourceRevision: revision})
		} else {
			e = errors.New("invalid_memory_revision")
		}
		result = map[string]bool{"forgotten": e == nil}
	case "memory-backfill":
		var limit int
		limit, e = integer("limit", 50)
		if e == nil {
			result, e = store.BackfillMemory(limit)
		}
	case "memory-put":
		var file *os.File
		file, e = os.Open(f["json-file"])
		if e != nil {
			e = errors.New("memory_read_failed")
			break
		}
		var data []byte
		data, e = io.ReadAll(io.LimitReader(file, 16385))
		file.Close()
		if e != nil || len(data) > 16384 {
			e = errors.New("memory_read_failed")
			break
		}
		var mutation bridge.MemoryMutation
		decoder := json.NewDecoder(strings.NewReader(string(data)))
		decoder.DisallowUnknownFields()
		if e = decoder.Decode(&mutation); e != nil {
			e = errors.New("invalid_memory_json")
			break
		}
		var extra any
		if decoder.Decode(&extra) != io.EOF {
			e = errors.New("invalid_memory_json")
			break
		}
		result, e = store.PutMemory(pos[0], claim, mutation)
	case "materialize":
		if !bridge.Snowflake(f["attachment"]) {
			e = errors.New("attachment_required")
			break
		}
		var rest *bridge.RESTClient
		rest, e = bridge.NewRESTClient(settings)
		if e != nil {
			break
		}
		defer rest.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		result, e = rest.MaterializeAttachment(ctx, store, pos[0], claim, f["attachment"])
	case "register-commands":
		if f["dry-run"] == "true" && f["fetch"] == "true" && f["apply-reviewed"] == "" && f["plan-file"] == "" && f["snapshot-file"] == "" {
			var rest *bridge.RESTClient
			rest, e = bridge.NewRESTClient(settings)
			if e != nil {
				break
			}
			defer rest.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			var snapshot bridge.CommandSnapshot
			snapshot, e = rest.CommandSnapshot(ctx, store)
			if e == nil {
				var plan bridge.CommandPlan
				plan, e = bridge.PlanGuildCommands(settings, snapshot)
				plan.NetworkUsed = true
				result = plan
			}
		} else if f["apply-reviewed"] == "true" && f["plan-file"] != "" && f["dry-run"] == "" && f["fetch"] == "" && f["snapshot-file"] == "" {
			var file *os.File
			file, e = os.Open(f["plan-file"])
			if e != nil {
				e = errors.New("plan_read_failed")
				break
			}
			raw, err := io.ReadAll(io.LimitReader(file, 1048577))
			file.Close()
			if err != nil || len(raw) > 1048576 {
				e = errors.New("invalid_plan_file")
				break
			}
			var plan bridge.CommandPlan
			if json.Unmarshal(raw, &plan) != nil {
				e = errors.New("invalid_plan_file")
				break
			}
			var rest *bridge.RESTClient
			rest, e = bridge.NewRESTClient(settings)
			if e != nil {
				break
			}
			defer rest.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			result, e = rest.ApplyGuildCommandPlan(ctx, store, plan)
		} else {
			e = errors.New("invalid_registration_mode")
		}
	case "bind-response":
		var binding string
		binding, e = store.BindPendingResponse(pos[0], claim, f["message-id"])
		result = map[string]string{"pending_response_id": binding}
	case "recover-thread-message":
		var rest *bridge.RESTClient
		rest, e = bridge.NewRESTClient(settings)
		if e != nil {
			break
		}
		defer rest.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		var outcome string
		outcome, e = rest.RecoverThreadMessage(ctx, store, pos[0], pos[1])
		mutated = e == nil && outcome == "validation_staged"
		result = map[string]string{"recovery": outcome}
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
		claimStarted := time.Now()
		item, err := store.ClaimNextForConsumer(lease, begin, consumer)
		claimReturned := time.Now()
		if err != nil {
			e = err
			break
		}
		if item != nil {
			trace.markAt("claim_call_started", claimStarted)
			trace.markAt("claim_returned", claimReturned)
			trace.setInbound(item.InboundID)
			result = map[string]any{"message": item}
			mutated = true
			break
		}
		if !time.Now().Before(deadline) {
			trace.markAt("claim_call_started", claimStarted)
			trace.markAt("claim_returned", claimReturned)
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
		watch := bridge.WatchWake(settings.DBPath+".sock", deadline)
		defer watch.Close()
		for {
			if pollHealth != nil {
				_ = pollHealth.Touch()
			}
			var item *bridge.Claim
			claimStarted = time.Now()
			item, e = store.ClaimNextForConsumer(lease, begin, consumer)
			claimReturned = time.Now()
			if e != nil {
				break
			}
			if item != nil {
				trace.markAt("claim_call_started", claimStarted)
				trace.markAt("claim_returned", claimReturned)
				trace.setInbound(item.InboundID)
				result = map[string]any{"message": item}
				mutated = true
				break
			}
			if !time.Now().Before(deadline) {
				trace.markAt("claim_call_started", claimStarted)
				trace.markAt("claim_returned", claimReturned)
				result = map[string]any{"message": nil}
				break
			}
			remaining := time.Until(deadline)
			recovery := time.Second
			if !watch.Connected() {
				recovery = 250 * time.Millisecond
			}
			if remaining > recovery {
				remaining = recovery
			}
			timer := time.NewTimer(remaining)
			select {
			case <-watch.Events():
				timer.Stop()
			case <-timer.C:
			}

		}
		if pollHealth != nil {
			_ = pollHealth.Close()
		}
	case "prune-reply-spool":
		var count int
		count, e = store.PruneReplySpool()
		result = map[string]int{"removed_files": count}
	case "reconcile-reply":
		if f["verified-in-discord"] != "true" || !bridge.Snowflake(f["message-id"]) {
			e = errors.New("verification_required")
			break
		}
		var index int
		index, e = integer("chunk", -1)
		if e != nil || index < 0 {
			e = errors.New("invalid_chunk")
			break
		}
		var rest *bridge.RESTClient
		rest, e = bridge.NewRESTClient(settings)
		if e != nil {
			break
		}
		defer rest.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, e = rest.ReconcileReply(ctx, store, pos[0], index, f["message-id"])
		if e == nil {
			result, e = store.Delivery(pos[0])
			mutated = true
		}
	case "verify-reply":
		var index int
		index, e = integer("chunk", 0)
		if e != nil || index < 0 {
			e = errors.New("invalid_chunk")
			break
		}
		var c bridge.Chunk
		var receipt bridge.SendResult
		c, receipt, e = store.ReplyReadback(pos[0], index)
		if e != nil {
			break
		}
		var rest *bridge.RESTClient
		rest, e = bridge.NewRESTClient(settings)
		if e != nil {
			break
		}
		defer rest.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		result, e = rest.VerifyStoredReply(ctx, c, receipt)
	case "reply-output-dir":
		var dir string
		dir, e = store.ReplyOutputDir()
		result = map[string]string{"directory": dir}
	case "reply", "followup":
		if _, manifest := f["manifest-file"]; manifest {
			if _, text := f["text-file"]; text {
				e = errors.New("reply_input_conflict")
				break
			}
		}
		trace.mark("reply_input_open_started")
		var reader io.Reader = os.Stdin
		inputPath, hasPath := f["text-file"]
		_, manifest := f["manifest-file"]
		if manifest {
			inputPath, hasPath = f["manifest-file"]
		}
		if manifest && hasPath && inputPath != "-" {
			var data []byte
			data, e = bridge.ReadReplyManifestFile(inputPath)
			if e != nil {
				break
			}
			reader = strings.NewReader(string(data))
		} else if p, ok := inputPath, hasPath; ok && p != "-" {
			var file *os.File
			file, e = os.Open(p)
			if e != nil {
				e = errors.New("reply_file_failed")
				break
			}
			defer file.Close()
			reader = file
		}
		trace.mark("reply_input_open_finished")
		var data []byte
		trace.mark("reply_input_read_started")
		limit := int64(64005)
		if manifest {
			limit = bridge.MaxReplyManifestBytes + 1
		}
		data, e = io.ReadAll(io.LimitReader(reader, limit))
		trace.mark("reply_input_read_finished")
		if e != nil {
			e = errors.New("reply_read_failed")
			break
		}
		var id string
		trace.mark("queue_call_started")
		if cmd == "followup" && manifest {
			id, e = store.QueueFollowupManifest(pos[0], claim, f["key"], data)
		} else if cmd == "followup" {
			id, e = store.QueueFollowup(pos[0], claim, f["key"], string(data))
		} else if manifest {
			id, e = store.QueueReplyManifest(pos[0], claim, data)
		} else {
			id, e = store.QueueReply(pos[0], claim, string(data))
		}
		trace.mark("queue_returned")
		if e == nil {
			trace.setInbound(pos[0])
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
	case "cancel-reply":
		result, e = store.CancelReply(pos[0])
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
		trace.mark("notify_started")
		bridge.NotifyIPC(settings.DBPath + ".sock")
		trace.mark("notify_finished")
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
