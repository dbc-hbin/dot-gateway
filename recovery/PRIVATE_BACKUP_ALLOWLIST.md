# Private backup allowlist and restore limits

This document describes the source-reviewed export contract. The actual state
snapshot, memory archive, private target configuration and their trusted hashes
must remain in an explicitly authorized private backup location, never public
GitHub or a source ZIP. No backup is uploaded by these tools.

## Existing transport and memory data

- Main snapshot: exact event/reply/chunk IDs, stable nonces, delivery states,
  attempts, confirmed remote IDs and approved owner/bot/guild/channel configuration
- Memory archive: separately redacted, exact-scope conversation evidence, curated
  note versions and content-free forget markers; never claims or credentials
- Main snapshot memory fences: identity, source revision/state/scope hash, deleted
  document keys and curated versions. The separately verified memory archive must
  match this ledger; newer fences override older archived content
- Restore creates blocked empty-envelope historical input and sent/uncertain
  transport receipts. It does not reconstruct raw conversation/output, claims,
  unacknowledged work, model context or a runnable sender

## Phase3 metadata extension

The optional `phase3` block is versioned and bound to the snapshot's exact
application, configured guild and owner. Older snapshots without it remain valid.
Unknown JSON fields and duplicate keys are rejected by the existing strict reader.

- Up to three registered command mappings: exact `ask`, `status`, `cancel` names
  and command IDs for the configured guild. They are restored as ownership
  metadata, allowing a fresh read-only registration plan to recognize unchanged
  commands instead of silently adopting or colliding with another command
- Up to 4,096 registration attempt records: exact SHA-256 plan digest + command
  name + create/update action key, acknowledgement ID where known, timestamp and
  bounded state. Attempted work restores uncertain; every attempt key remains
  occupied. Restore never POSTs/PATCHes commands or approves a new registration
- Tokenless interaction-ID deduplication: exact owner/channel/action/time and an
  optional ask-control inbound link. Every restored interaction is unavailable;
  no token, active modal/button binding or pending approval/question survives
- Reaction present/sequence tuples and target update/delete invalidations.
  Actual reaction event keys are validated structurally, including bounded
  Unicode emoji/custom emoji IDs. Historical rows never become user approvals
- Exact reply cancellation evidence and separate inert request-cancellation
  markers. Main historical input remains blocked; cancellation does not turn an
  unknown delivery into a verified send or a permission to retry
- Rich reply metadata: only returned attachment ID and byte size, keyed to an
  acknowledged reply/chunk. These records go to a separate inert recovery table,
  never the live `OutputReceipt`/output-manifest tables. They do not prove exact
  filename, description, content, embeds or bytes and cannot authorize readback,
  editing or resending an old artifact

The complete main snapshot must fit the existing 64 MiB restore limit; oversized
exports fail before creating an output file. Large historical control groups are
capped at 100,000 records each; a receipt has
at most ten attachments. Exceeding a cap fails the snapshot instead of dropping
no-replay evidence. Scope/name/ID/state mismatches also fail closed. Re-backing up
a restored ledger carries these inert metadata records forward.

## Intentionally excluded

- Discord/GitHub/API credentials, interaction/webhook tokens, authorization data,
  cookies, browser profiles, authentication sessions and signed URLs
- Inbox/outbox bodies, control target text, message operation payloads/snapshots,
  command descriptions/options, file names/descriptions/MIME strings, file bytes,
  rich output manifests and active reply spools
- Processing/claim/consumer leases; executable tasks; old buttons, modals, pending
  reaction-question bindings or any remembered authorization
- Catchup route registry, tracked/requested routes, cursors, history origins,
  startup witness files and transport resume/session state. Every restored ledger,
  including empty/legacy snapshots, receives only the fixed catchup
  `disarmed_restore` marker. Restored history never proves a live read route

## Verification and limitations

Use one consistent private SQLite snapshot for the main metadata and memory
exports, and record both whole-file hashes separately. Retain the reviewed source
commit/manifest hash alongside them. A hash inside its own archive is not an
independent trust anchor. Restore into a new private destination and verify every
source/state/memory hash before separate activation review.

Fresh remote identity, command state, current delivery history, permission and
consumer-liveness checks are still required after restore. Restored command IDs
reflect the verified backup, not a guarantee that Discord has not changed since.
An uncertain registration requires explicit remote review; tombstones must not be
removed to force a retry. Missing/renamed commands remain a collision/missing-ID
review problem instead of being silently recreated.

These exports intentionally cannot reconstruct old file/rich response content or
finish an interrupted ephemeral answer. Ask again in a new request when needed.
They do not install software, start a service, create credentials, reconnect an
account, schedule a job or send any external message. Local fsync does not prove
VM-loss durability; obtain a separately authorized off-VM private copy. A wholly
old backup cannot know about deletions or operations performed after its capture.

## Scoped message-operation fences

The optional `message_operations` block is also versioned and bound to the exact
snapshot application/guild/owner. It exports only operation ID, action enum,
channel/message IDs, state, attempt count and creation time. Operation IDs are
existing deterministic SHA-256 identities; no raw request keys, specs, routes,
message/reaction bodies or before/after snapshots are exported.

Restore creates only `message_operation_recovery_fences` and
`message_edit_recovery_projections`, never runnable `message_operations` rows or
edit revision payloads. Every old operation ID rejects replay. Prepared/uncertain
operations hold their target conservatively; all restored edit projections remain
unknown and suppress stale assistant memory and its dependent facts. The gateway
operation module must consume these exact fences at admission/dispatch/reconcile;
a file containing unused fences alone is not a replay guard. Re-backing up a
restored ledger carries the fences forward. Existing assistant-edit memory keys
are recognized only in their exact bounded `assistant-edit:<64 hex digits>` form.

Old edited content cannot be reverified from these metadata alone. Unknown
projection/target holds must not be cleared merely to make a mutation proceed.
A new read/verification and an explicitly supported recovery workflow are needed;
there is no automatic execution, reconstruction or reissue of historical edits,
reactions, pins or unpins. The reviewed combined gateway/recovery integration
must pass its no-replay and edited-memory suppression tests before deployment.

The snapshot/verify-state/restore-state result includes content-free state_counts
for registered commands, attempt/interaction/reaction/cancellation fences, inert
rich receipts, held operation targets, edit projections and memory fences. These
counts report the verified archive, not running-service or remote-state health.

## Worker-control reconciliation metadata

The optional, versioned `worker_control` block is bound to the exact application,
guild and owner. It retains only inbound IDs for unresolved executions and
worker-recovery quarantine, plus cancellation state and request/acknowledgement
timestamps. Source-revoked NULL queue claims and expired observations do not
remove an execution marker. Current terminal observations and explicitly retired
incarnations are not exported as fresh execution authority.

Restore uses only `recovery_worker_execution_fences` and
`recovery_worker_cancellation_fences`; it never recreates `worker_runtime`,
`worker_incarnations`, `worker_bindings` or live `worker_cancellations` rows.
Worker/controller names, incarnation IDs, claim/revision tokens, evidence text,
leases and prompt bodies are excluded. Pending cancellations stay pending and
survive re-backup. Aggregate state_counts expose unresolved requests, recovery
quarantine and pending cancellations without identities or text.

These are inert reconciliation records, not a live worker-control protocol.
The unchanged gateway does not consume these new tables as execution gates.
Historical inbound is still blocked and claimless, preventing historical output
or follow-up replay. Before starting a replacement native worker, the actual
controller must reconcile any unresolved execution or cancellation in its own
supported environment. A reset, transport restart, archive restore or expired
lease does not prove that a cloud-native reasoning turn stopped. This export
cannot reconstruct that controller or its evidence and must never fabricate ACKs.

Rich follow-ups use the existing per-chunk receipt allowlist, including nonzero
chunk indexes. Their keys, text, manifests and spool bytes are intentionally
excluded. Existing deterministic message-operation IDs remain the operation-key
replay fence; raw operation keys never need to leave the source database.
