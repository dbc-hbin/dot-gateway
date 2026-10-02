# Consumer liveness and backlog observations

## Additive status change

The existing `gateway` status remains transport-only. `status` now also returns:

- `consumer.state`: `waiting_cli`, `claim_committed`, `stale_cli`, `absent`, or
  `unknown`
- Fresh/stale waiting invocation counts, invalid observation count, and freshness
  thresholds
- Active claim and expired claim counts
- Active processing lease count, explicitly not evidence of model liveness
- Pending count, oldest pending age, and a 15-second overdue-backlog flag
- Last durable reply-queue timestamp from `replies.created`, retained even when
  the bounded timing samples are pruned
- Validation-quarantine pending/blocked counts, oldest age, and overdue flag
- `reasoning_liveness` and `result_receipt`, both `not_observable`
- `response_path.state`: `gateway_unavailable`, `degraded_backlog`,
  `consumer_unavailable`, `blocked_ingress`, `validating_ingress`, or `unverified`

`response_path.end_to_end_ready` is deliberately false, with readiness marked
`not_verifiable_from_gateway`. A fresh transport, waiting subprocess, processing
lease, or emitted stdout cannot establish that the assistant received the result.
`claim_committed` means durable work exists; it does not mean model processing.
A terminal old processing row is excluded once its inbound is replied or ignored.
An overdue pending backlog or expired claim remains degraded even with fresh
transport and CLI observations. Pending route validation is shown as
`validating_ingress` until it becomes overdue; blocked route validation is always
`blocked_ingress`. Validation rows cannot be claimed or sent until the gateway
validates and atomically promotes them.

## Observation lifecycle and safety

Waiting `next` invocations use unique `runtime` keys under `consumer_poll:`.
Each stores only a technical consumer name, observation time, and poll deadline.
There are no message texts, route IDs, claim tokens, authentication credentials,
or response contents. These CLI observations do not grant or extend claims.
The separate opt-in worker binding protocol described below adds explicit
recovery fencing for bound claims; legacy unbound recovery remains unchanged.

- A ready or recovered fast-path claim does not create a waiting observation
- A waiting invocation writes its start, then at most once every 10 seconds
- An observation is stale after 25 seconds or the poll deadline
- Normal timeout, claim, or failure cleans up only its own observation key
- A hard-killed process naturally becomes stale; a later poll prunes expired
  observations under this reserved prefix only
- At most 128 live observation rows are retained
- Touching a deleted/pruned instance does not recreate it
- Failed diagnostic writes never prevent claim acquisition or recovery
- Concurrent polls retain independent observations; an old invocation cannot
  delete or overwrite a newer invocation's health record

This does not relax the single-logical-consumer requirement. Concurrent polls
using the same recovery identity can receive the same outstanding claim, exactly
as documented in CLAIM_RECOVERY.md. Status observations do not grant ownership.

No mandatory acknowledgement roundtrip, automatic response, automatic POST,
model API, hidden platform hook, or model restart mechanism was added. A gateway
supervisor can keep transport alive; a supported live controller must still
observe and recover a stopped reasoning consumer.

## Verification

Focused and full offline tests cover absent/stale/invalid/deadline-expired CLI
observations, overdue backlog even with a fresh waiting CLI, processing leases
and recovered claims, terminal reply cleanup, bounded write frequency/capacity,
scoped stale-row pruning, overlapping invocation cleanup, timing-sample eviction
without losing the durable last-reply time, staged ingress visibility, and actual subprocess
wait/timeout/claim/overlap lifecycles. All ledgers are disposable.


## Native-worker controller attestations

`status.worker_control` and `worker-status` expose a separate, opt-in durable
controller protocol. It records the actual native task, a turn-scoped incarnation,
an exact claim binding, a bounded controller-attested observation lease, and
pending execution-cancellation acknowledgements. Stale observations fail closed
to `unknown`. They never change CLI telemetry into model-liveness proof, and
`response_path.end_to_end_ready` remains false.

The gateway has no native start/interrupt API and reports that limitation in
status. Only the actual controller can observe/interrupt/resume the native turn
and attest its result. Bound expired claims require verified stop and explicit
incarnation recovery, rather than silent automatic reuse.

See [WORKER_CONTROL.md](WORKER_CONTROL.md) for commands, recovery fencing, the
normal `completed` transition, and why `/cancel` remains `cancel_requested` until
a real controller interruption is acknowledged. Recovery-pending claims and
unbound pending cancellations remain visible in worker-control status; absence
of a waiting CLI does not mean those requests finished.
