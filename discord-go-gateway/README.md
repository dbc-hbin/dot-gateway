# Go Discord gateway

A single compiled Linux binary replaces the Python gateway and queue CLI. The
current assistant remains the only author of replies: there is no model/API
client, execution of incoming text, canned reply, or bot-self conversation.

For reconnect-safe consumption, use a stable `next --consumer-id` per logical
consumer. See [claim recovery and deployment](CLAIM_RECOVERY.md) before switching
existing anonymous pollers. Legacy `next` without an identity remains supported.

## Response generation and readiness

This ports the adjacent Python **bridge**, not the full upstream Hermes runner.
The Python bridge imports Hermes reaction/typing hooks but deliberately does not
start its message handler or model runner. The Go daemon likewise receives and
stores messages, shows lifecycle feedback, and sends already-authored replies.
Neither daemon creates substantive replies by itself.

The current assistant must own a running consumer for the active session:

1. Run `next --consumer-id dot-discord-reasoning-v1 --wait 300` directly in the
   responding assistant's execution context. Confirm the call actually started;
   scheduling a listener is not evidence that one is running.
2. On a claim, call `begin` when the assistant starts real work, then author and
   queue the reply. `--begin` is appropriate only when the caller will immediately
   do that work. Renew claim/processing leases separately when needed.
3. If output is lost, recover with the same consumer identity. A recovered claim
   does not renew its claim or processing lease; inspect the returned expiry and
   call `begin` explicitly when resuming work.
4. Check delivery, then keep the next poll running. A daemon restart does not
   restart this assistant-side consumer or resume response generation.

The native supervisor manages the gateway process only. Process uptime, a fresh
gateway heartbeat, an open Discord connection, and a typing lease do not establish
that an assistant is generating an answer. Consumer status exposes waiting-CLI
observations and backlog separately; it explicitly does not prove stdout receipt
or reasoning liveness. The optional [worker-control protocol](WORKER_CONTROL.md)
adds controller-attested worker incarnations, fenced recovery, and durable
interrupt requests/acknowledgements. A controller with access to the actual
reasoning runtime must perform those operations; the Go gateway cannot restart
or interrupt the external native assistant by itself. There is no supported
inactive-session Discord wake in this integration, and no reboot auto-start
guarantee.

## Verified child threads

Owner messages in public and existing-member private threads under the pinned
conversation channel now pass durable quarantine and fresh parent/permission
validation. Replies and feedback stay in the thread. See [thread routing and
point-message recovery](THREAD_ROUTING.md) for exact scope, lifecycle restrictions
and the unavoidable Discord final-GET-to-POST archive race.

## Inbound media

Owner attachments, native voice messages, stickers, embeds, polls and forwards now
reach `next` as bounded, explicitly untrusted metadata. Media-only messages pass
the existing exact owner/route gate. Supported original attachments can be fetched
with the explicit live claim-bound `materialize` command; the consumer must open
the returned private file to interpret it. See [the media contract](MEDIA_CONTRACT.md)
for limits, security boundaries, invocation and unsupported types.

## Source edits, deletion and context

Message updates revoke stale claims and are refreshed from their exact authorized
source. Deletions leave durable tombstones and retire definitely unsent work;
ambiguous sends remain held. Bounded quoted and thread-starter context carries
explicit untrusted provenance. See [source revisions](SOURCE_REVISIONS.md).

## Authored file and rich output

Use the versioned manifest workflow for files, images, ordinary audio files and
bounded explicit embeds. The private outbox/spool, exact consumer commands,
receipt verification, limits and deployment gates are in [rich output](RICH_OUTPUT.md).
Plain text remains the default, with link previews suppressed.
Confirmed-delivery task continuations can use the same manifest for files and
images; see [idempotent follow-ups](FOLLOWUPS.md).

## Request-bound message operations

Explicit text edits, the bot’s own requested reactions, and exact-message pins
use a separate durable one-attempt ledger. Existing delivery records remain
immutable. See [message operations](MESSAGE_OPERATIONS.md) for owner-request
authority, current permissions, revision/memory safeguards and uncertainty
reconciliation. Installing the APIs does not perform any live action.

## Components

- DiscordGo handles Gateway protocol/state only. Its REST send/reconnect helpers
  are not used for message delivery. A cancellable supervisor owns reconnects.
- One SQLite actor owns one pinned connection per process. Transactions preserve
  the existing Python inbox, claims, replies, chunks, nonces, feedback and runtime.
- Owner/type/policy-filtered arrivals enter a durable validation quarantine before
  any channel GET. Consumers and feedback cannot see them until exact route
  validation atomically promotes the original envelope, ID and receipt time.
  Transient GET failures retry with 2/4/8/16/30-second backoff and survive restart.
  Permission/missing-route/mismatch failures remain blocked and visible until a
  validated reconnect; authentication failure still latches the supervisor.
  Quarantined plus pending/claimed inputs share the original 1,000-item capacity.
- The native gateway holds the **same `.lock` flock** as Python for its entire
  lifetime. Only one process can dispatch. The REST sender serializes POSTs too.
- Private owner-only Unix IPC and a private inotify wake file signal committed
  queue changes immediately. File signaling supports an exec sandbox that cannot
  create sockets. A one-second maintenance check covers a writer crashing between
  commit and signal; ready chunks otherwise drain without a fixed sleep.
- Identity/channel preflight GETs run concurrently. A pooled TLS-verifying HTTP/1
  client sends each message POST once, with no redirects or replayable body.
  Known bucket/global limits pace *future* requests; a 429 is not automatically
  resent. Lost/ambiguous acknowledgements are held as `uncertain` for review.
- Receipt 👀, lease-bound eight-second typing refresh and failure ❌ are real
  lifecycle feedback. Successful delivery clears 👀 without adding a checkmark
  reaction; the reply itself is completion feedback. Disconnect, expiry, reply and
  shutdown cancel typing. Symbolic errors avoid logging bodies, credentials or message content.
- A bounded 2,048-row content-free timing ledger records lifecycle stages using
  stable persisted timestamps. It distinguishes consumer/model wait from network
  delivery; changing language cannot remove the external reasoning/tool boundary.
- Content-free ingress counters expose accepted/duplicate/rejected events, failed
  route lookup/validation, staging/retry/blocking and a full durable queue.
  Validation backlog/age appears separately in status. A blocked conversation
  preserves its order without blocking other conversations' due validation work.
- Reconnect has one 60-second deadline covering socket Open and validated
  readiness. READY's securely validated resume URL is used with the original
  session/sequence. Invalid-session handshake retries share that deadline and
  never re-lock Open's session mutex. Owner DMs become identity-ready independently
  of guild availability; exact guild routes retain a separate fail-closed gate
  and 30-second read-only recovery checks. Authentication/identity failures remain
  global. See [session resilience](SESSION_RESILIENCE.md) for the pinned SDK patch,
  endpoint rules and offline coverage. Daemon recovery still does not start or
  restart the assistant-side responder.

## Build and checks

Go 1.24 or later is required; verification used official Go 1.27.1. In this
workspace `/usr/bin/go` is an unrelated executable: use the verified toolchain.

```sh
export PATH=/workspace/shared/go-toolchain/go1.27.1/bin:$PATH
export GOPATH=/workspace/shared/go-path GOCACHE=/workspace/shared/go-cache
go test -race ./...
go vet ./...
CGO_ENABLED=0 go build -buildvcs=false -trimpath -o bin/dot-gateway ./cmd/dot-gateway
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -buildvcs=false -trimpath -o bin/dot-gateway-linux-arm64 ./cmd/dot-gateway
```

The only test skipped in this exec sandbox is Unix-socket creation (`EPERM`);
run that test on the native host. File/inotify, fake HTTP/WebSocket, migration,
CLI subprocess, race, and cancellation tests are offline. No test uses a real
token, live database or Discord endpoint.

## Configuration and CLI

The original `DISCORD_OWNER_ID`, `DISCORD_ALLOWED_DM_IDS`, pinned bot/guild/channel,
guild mode, approved Message Content, `BRIDGE_DB`, HTTP keepalive and explicit
credential-free proxy environment settings are retained. Only live `gateway`
(alias `run-discord`), explicit `recover-thread-message`, and `materialize` load
`DISCORD_BOT_TOKEN_FILE`. Offline CLI commands never
need credentials or connect to Discord. The token loader rejects symlinks,
nonregular files, foreign ownership and any group/other permission.

```sh
bin/dot-gateway check
bin/dot-gateway gateway
bin/dot-gateway next --wait 30 --begin --processing-seconds 60
bin/dot-gateway reply INBOUND_ID --claim CLAIM --text-file response.txt
bin/dot-gateway begin INBOUND_ID --claim CLAIM --lease-seconds 60
bin/dot-gateway renew INBOUND_ID --claim CLAIM --lease-seconds 300
bin/dot-gateway delivery REPLY_ID
bin/dot-gateway status
```

`ignore`, `retry-failed`, `resolve-sent --verified-in-discord`, and read-only
`test-send-status` retain the legacy contracts. Flags may follow positional IDs.
Only explicit `retry-failed` requeues a known failed ordinary chunk; uncertain
chunks cannot be retried. The old one-shot test-send ledger remains untouched.

## Explicit migration transport tests

`diagnostic-send --index 1` queues one of exactly three lifetime slots (1–3).
`diagnostic-status --index 1` returns its durable state and timing. These commands
do not load a token or directly POST: the authorized running gateway dispatches.
Repeated commands retain the same immutable intent/nonce and cannot reset an
attempted slot, including failed or uncertain results. Diagnostic messages are
clearly labeled transport tests, have no fabricated inbound or reply reference,
and only target the pinned guild channel. No automatic test is sent at startup.

Measures: intent creation, local queue delay, send start, parallel preflight,
POST through complete ACK, local ACK time and remote snowflake creation time.
These are transport measurements, **not** human-to-model response latency.

## Provenance

This is a Go behavioral port, not an import of the Python Hermes runtime.
The adjacent `hermes-dot-gateway` and its full pinned MIT Hermes source remain
reference/provenance. Receipt/processing/completion behavior follows the narrow
Hermes lifecycle hooks; unrelated model, voice, command and profile code is not
included. See `THIRD_PARTY_NOTICES.md`.

## Bounded latency improvements

See [LATENCY_IMPROVEMENTS.md](LATENCY_IMPROVEMENTS.md) for claim paging, combined
file/socket wakeups, opt-in content-free CLI phase traces, immutable per-send
metrics, synthetic benchmarks, and measurement limitations. No model/provider
or live runtime change is implied by these source improvements.

## Cancelling an undeliverable reply

`cancel-reply REPLY_ID` durably abandons a queued or known-failed reply's unsent
remainder and releases later replies in that conversation. Inspect `delivery`
first: cancellation preserves sent/failed chunk evidence, including remote IDs,
error codes and attempt counts. It records a timestamped cancellation reason,
marks pending suffix chunks `cancelled`, and reports the whole reply as
`cancelled`, never `sent`. Status exposes `cancelled_replies`; lifecycle feedback
uses the existing failure path and does not restart typing. Cancellation is
idempotent, survives restart and cannot be undone by `retry-failed` or submitting
the same reply again.

Sending and uncertain chunks cannot be cancelled. Reconcile an ambiguous send
using the existing verified-remote-message workflow first. Known HTTP failures
are not automatically abandoned because generic HTTP 400/404 does not prove
that the original reply reference was deleted. `retry-failed` remains available
for an uncancelled reply when its actual cause has been corrected. No migration
or startup pass cancels existing failures or replays historical work.

Replies are split losslessly at at most 1,900 UTF-16 units. Splitting prefers late
line/word boundaries, keeps short fenced blocks together, protects common
combining/emoji/Hangul sequences, and redistributes short trailing whitespace
instead of rejecting valid boundary-length replies. A cluster larger than one
message or a whitespace run that cannot be divided into nonblank messages is
rejected explicitly. This is a conservative boundary parser, not full Unicode
or Markdown segmentation. Long code fences remain verbatim and can render across
Discord messages without a closing/reopening fence; no synthetic text is added.

## Private conversation memory


Claims now include bounded, exact-conversation historical recall. Owner text and
actually sent reply chunks are indexed incrementally in the existing private
SQLite ledger, with Korean bigram lookup and optional claim/source-bound curated
facts. Recalled material never supplies authorization. See
[conversation memory](CONVERSATION_MEMORY.md) for privacy boundaries, optional
single-call note/reply workflow, explicit legacy backfill and sanitized private
backup/restore. Memory archives are never public source artifacts.

## Scoped history and prospective recovery

See [SCOPED_HISTORY.md](SCOPED_HISTORY.md) for bounded exact-route message/history,
owner search and timestamp-paginated pin reads, plus prospective reconnect
catch-up. Read results are untrusted context and never become tasks. Automatic
recovery arms from now on an intact live ledger; backup restores stay disarmed.
