# Request-bound message operations

This module implements only explicit, synchronous operations on one exact Discord
message. Installing it does not authorize or perform any live operation. It has
no automatic dispatcher, target discovery, HTTP passthrough, message deletion,
other-user reaction removal, membership expansion, or permission changes.

## Authority and supported scope

The authoring assistant must have a real owner request for the exact action,
destination, target and consequential content. A live claim is a structural
ownership/source-currentness proof, **not** natural-language authorization. An
emoji or remembered fact is never sufficient authority. The assistant must apply
its confirmation policy before invoking a mutation.

The request must be an ordinary owner message with an active current claim.
Interaction `/ask` and reaction-control envelopes are deliberately unsupported as
mutation authority. A destination in another conversation is allowed only if it
was explicitly requested and is separately allowed by the configured parent or
an already-verified owner DM/thread route in the ledger. No DM is opened and no
thread is joined. Archived threads are not mutated. Existing-member private
threads must still prove current bot membership. No route or permission is
expanded by this API.

Supported actions:

- `edit_text`: replace nonempty text (maximum 2000 UTF-16 units) on one known
  confirmed-sent ordinary bot message. A terminal LF is rejected, because Discord
  can remove it and the edit projection intentionally has an exact-text contract
- `add_reaction`: add one specified normal reaction as the bot
- `remove_own_reaction`: remove only that bot's own normal reaction
- `pin`, `unpin`: change one exact message's pin state

Normal type0/type19 messages are supported. Webhook/interaction replies, voice
messages and Components V2 edits are rejected. Edits retain every attachment ID
in its existing array order and all existing flags. Embeds and components are
omitted from PATCH, so they are retained; their semantic digests are verified.
Every edit explicitly sends empty allowed-mentions arrays and replied_user=false.
Only known Discord proxy fields and attachment `ex/is/hm` URL signature parameters
are excluded from embed digests. Custom reaction identity is its immutable emoji
ID, so renaming a custom emoji does not break readback.

## Commands and typed contract

`message-operation REQUEST --claim CLAIM --json-file operation.json`

The strict JSON object has version=1, a caller-chosen stable `key`, `action`,
`channel_id`, `message_id`, and exactly the relevant `text` or `emoji` field.
Keys use 1–80 ASCII letters, digits, underscores or hyphens. Custom emoji use
`name:id`; ordinary emoji use Unicode. Unknown JSON fields are rejected.

Example edit input:

```json
{"version":1,"key":"correct-date-1","action":"edit_text","channel_id":"123","message_id":"456","text":"The meeting is on Friday"}
```

Additional explicit commands:

- `message-operation-status OPERATION_ID`: local evidence only, no network
- `reconcile-message-operation REQUEST --claim CLAIM --operation-id ID --verified-in-discord`:
  perform fresh exact readback to reconcile an uncertain/verified operation
- `abandon-message-operation REQUEST --claim CLAIM --operation-id ID`:
  cancel only provably unattempted `prepared` work; never release an uncertain
  attempt or reset its attempt counter

Library entry points are `ExecuteMessageOperation`, `MessageOperation`,
`ReconcileMessageOperation`, `AbandonMessageOperation`, and local-only
`CurrentEditedMessage`. Merely reading status or a local projection is not fresh
remote verification. Reconciliation requires an active owner request too; its
flag requests the exact readback procedure and is not a substitute for evidence.

## Durable execution and uncertainty

An operation ID is SHA256 of the canonical JSON array [request ID, key]. The
request, key, action, target, payload, route and before/desired snapshots are
immutable. Reusing a key with different content fails. Reusing an attempted key
only returns its stored outcome. A prepared operation may be explicitly resumed
with the same still-active claim, or abandoned using a current owner request.

One transaction reserves the operation as `uncertain`, consumes its single
attempt, and invalidates old reaction/question bindings **before** network I/O.
`attempts=1` means its single attempt was reserved, not proof bytes reached
Discord. Process death anywhere after that boundary cannot cause replay. There
is a unique per-target hold for prepared/uncertain work. Concurrent CLI clients
use SQL compare-and-swap; a losing preflight cannot overwrite a winner's real
lost-ACK state. A known rejection/zero-attempt failure is terminal; an uncertain
operation can only be resolved by exact observed desired state, never inferred
non-application or a blind retry.

Fresh checks cover bot identity, request source, exact authorized route, current
permissions, target author/content/attachments/flags and the intended pin or own
reaction state. A durable per-target Gateway generation is captured before the
first target read and checked at reservation and immediately before HTTP.
Observed target update/delete events revoke the proof even for non-bot targets.
All write-budget waits repeat the complete source/route/target proof. The final
claim and target-generation guard runs after rate waits immediately before Do.

Mutation HTTP is called once. 400/401/403/404/405/429 are definitive failures;
transport loss, unknown response statuses, incomplete ACK/readback, and database
commit failure remain uncertain. Even successful HTTP requires an exact fresh
readback. Readback is generation-CAS-bound before making a verified projection.
Discord does not offer atomic conditional message mutation, so an **unobserved**
remote change between the final GETs and mutation remains an unavoidable race.
This is not an exactly-once remote execution guarantee.

## Edit revisions, reactions and memory

Original `replies`, `chunks`, nonces and output receipts never change. Revision0
is an immutable verified baseline. Each confirmed edit appends an immutable
revision and advances a separate current projection. Before PATCH the projection
is unknown, which blocks dependent operations and memory assumptions. Failed
edits restore the previous projection only if no target invalidation intervened;
old approval bindings stay retired even when the edit fails.

Fresh reaction/question bindings use a digest including the verified edit
revision. Old bindings cannot authorize or even masquerade as current context.
A delayed Gateway update after successful readback conservatively marks the
projection unknown and revokes bindings; explicit reconciliation of the known
edit is required before creating a new binding. An external mismatching edit
stays unknown and fails closed. No automatic rewrite or adoption is attempted.

Assistant memory uses `assistant-edit:<operation ID>` only after exact verified
readback. Every memory query and fact-source dependency checks the live current
projection, so index failures cannot expose the original stale reply or an
unverified replacement. Recalled edited content remains untrusted historical
evidence, never permission. Original delivery evidence remains independently
available for audit.

## Recovery integration

Content-free recovery preserves each operation ID as an inert
`message_operation_recovery_fences` row; prepared/uncertain rows retain target
holds. Every edit projection becomes an unknown
`message_edit_recovery_projections` marker. Admission, dispatch and reconciliation
consult these fences. Restored operations never become runnable, and no
credentials, request/spec bodies or signed URLs are restored. A recovered unknown
edit target remains blocked; these APIs intentionally provide no unsafe
"clear fence and retry" shortcut.

The private plaintext memory archive may preserve an edited document, but its
canonical operation ID must match verified edit evidence or an inert recovery
fence. Restored projection markers suppress both assistant snippets and their
facts, including when an older pre-edit memory archive is imported after a newer
metadata snapshot. This is tested using the actual recovery executable.

## Discord reference

Verified against Discord's official Message and Permissions references:

- https://docs.discord.com/developers/resources/message#edit-message
- https://docs.discord.com/developers/resources/message#create-reaction
- https://docs.discord.com/developers/resources/message#pin-message
- https://docs.discord.com/developers/topics/permissions

Pins use PUT/DELETE `/channels/{channel}/messages/pins/{message}` and
`PIN_MESSAGES` (bit51), never the deprecated `/channels/{channel}/pins/...`
endpoint or MANAGE_MESSAGES as a replacement. Add-reaction requires
ADD_REACTIONS even if another user already used the emoji (conservative).
Threads additionally require ordinary current thread write eligibility.

## Offline verification

All fixtures use temporary SQLite ledgers and local fake HTTP transports, with
no real tokens, live message reads/writes, service restarts or Git remote changes.
Run `go test -race ./...`, `go vet ./...` and build `./cmd/dot-gateway`.
Set `DOT_MEMORY_RECOVERY_SOURCE` to the operation-aware recovery module to enable
actual snapshot → restore → OpenStore → memory-import/no-replay fixtures.

Coverage includes immutable payloads/revisions/original receipts; exact current
permissions and bit51; source/target/claim races; concurrent prepared clients;
known failure, lost ACK, interrupted restart, reservation rollback and failed
verification commits; prepared abandonment; delayed Gateway invalidation and
new reaction binding; mention/attachment-order/flags preservation; custom emoji
rename and refreshed embed signatures; unknown memory/fact suppression; and
actual recovery with both pre-edit and current memory archives.
