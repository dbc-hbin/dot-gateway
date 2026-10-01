# Bridge/runtime parity review

2026-09-30–2026-10-01 UTC. Source review of this Go gateway, the adjacent Python
`hermes-dot-gateway`, its pinned upstream Hermes tree, and the native gateway
supervisor. Tests use temporary ledgers and local fake transports. No credentials,
live ledger changes, service restarts, or Discord sends were used for this review.

## P0: an active transport is not an active responder

The coordinating task reported approximately 132 seconds between a durable
receive and claim, with no reply from the designated listener. That listener had
not completed initialization; a subsequent direct call by the responding
assistant claimed immediately and produced one reply. Those are supplied incident
observations, not an independently repeated live test in this review.

The source explains the boundary: Go `RunGateway` runs receive, dispatch, feedback,
typing and health loops. Python `run_service` runs the same bridge duties. Python
`HermesLifecycle` imports upstream processing hooks but explicitly does not start
the upstream message runner. Full Hermes separately wires `_message_handler` and
calls it during `_process_message_background`; its runner can invoke
`agent.run_conversation`. None of those response-generation paths run here.

Therefore the inactive listener was an integration failure, not proof that a Go
queue operation took 132 seconds. The Python README already required an active
root-owned consumer. Keep the current responding assistant's poll running,
recover lost results with the stable consumer ID, and distinguish gateway health,
consumer-call health, claim ownership, processing lease, reply queueing and actual
delivery. The native supervisor restarts only the gateway process. This correction
does not create supported unattended wake-up or import a new model/API.

## Verified Go regressions corrected in source

### P1: missing ingress failure visibility

`receiveMessage` previously returned success (`nil`) after a channel GET failure
or route-validation failure, and discarded the store's `queue_full` result.
Python reports symbolic owner-route lookup warnings and its dispatcher emits a
capacity warning. The Go daemon could therefore remain visibly healthy after
dropping work before durable admission.

The receiver now returns a bounded outcome. Runtime ingress counters and symbolic
warning counts retain lookup, validation and capacity failures without message
text, response bodies, tokens, or route IDs. Health persistence preserves those
warnings instead of replacing them with an empty map. The subsequent durable
validation change below also repairs loss during channel lookup; the initial
diagnostics-only patch by itself did not.

### P2: permission-denied typing retried when work lease changed

Go formerly latched a typing 401/403 only inside one typing job. A renewed
processing deadline cancelled that job and created a new one, retrying the denied
endpoint before reconnect. Python retains a channel-level denial until validated
reconnect. Go also missed 401/403 from the preceding channel-validation GET.

The deny latch now outlives processing jobs for the connection epoch. Both channel
preflight and typing 401/403 are recognized. Lease renewal and disconnect alone
do not retry; a newly validated connection permits a new attempt. Reaction
permission handling uses the same classification for preflight denials.

## Additional availability gaps corrected before deployment

### P1: durable quarantine before channel lookup

Go previously did a channel GET before every durable inbound commit, expanding
Python's partial-channel lookup loss exposure to every event. It now persists an
owner/type/policy-filtered envelope in `ingress_validation` before network work.
This private table is never queried by claim or feedback. Validation promotes the
identical envelope, ID and original creation/receipt time in one transaction after
exact route checks and a fresh local policy check. Forged or changed promotion
inputs are rejected. Deduplication spans staging and ordinary inbox atomically.

Staged, blocked, pending and claimed work shares the original 1,000-item active
limit. Read-only transient validation errors retry at 2/4/8/16/30-second intervals;
the 20-second validation budget includes rate-limit waiting. Cancellation/restart
retains staged input. Channel 401/403/404 or route mismatch stays visible and
blocked; validated reconnect permits revalidation, while 401 still terminates with
the permanent authentication code. Later messages cannot overtake an earlier
unvalidated message in the same conversation. Due work in other conversations
can progress during that conversation's backoff/block. One in-flight GET can
occupy the single validator for at most its operation budget.

Tests cover durable restart, transient GET recovery, cancellation during a learned
rate budget, permission/missing-route recovery, exact identity/time preservation,
changed-envelope rejection, policy revocation, shared capacity, same-conversation
ordering and independent-conversation eligibility. No cache was introduced and no
route validation was bypassed. This protects events after the staging commit; it
does not guarantee replay if the process dies before that first durable write.

### P2: bounded post-Open reconnect readiness

The same 60-second attempt deadline now spans Open and validated readiness for
both startup and reconnect. An epoch-fenced coherent readiness snapshot rejects
stale validation. Fatal and cancelled/deadline states take precedence over late
readiness. Incomplete cache state causes a transient exit handled by the existing
bounded native supervisor, rather than an indefinitely alive/not-ready process.
Classification preserves permanent auth, identity, route and permission codes
even when a validation failure is already symbolic.

A fake WebSocket sends Hello and READY with an unavailable guild but no usable
channel/member state: Open returns successfully, then the scaled readiness
deadline produces the expected transient exit. Additional tests cover permanent
codes, already-expired deadlines, wrong epochs and a disconnect during lookup.
These are offline reproductions; no new live restart was performed here.

## Remaining limits and existing behavior

- The native daemon still cannot start assistant reasoning or wake an inactive
  conversation. An actual responding-assistant consumer is required.
- A full 1,000-item active queue still refuses new input visibly rather than
  consuming unbounded storage. Blocked validation requires its underlying access,
  authentication or route issue to be corrected and a validated reconnect.
- Older Python/Go builds do not process the new quarantine table. A rollback must
  preserve it and explicitly account for its pending work; never promote rows
  without validation merely to clear a backlog.
- Existing baseline behavior: failed or uncertain older replies intentionally
  block later sends in the same conversation. Known-failed chunks require explicit
  retry; uncertain chunks require verified resolution. Do not automatically
  bypass that ordering or retry a possibly accepted POST.
- Existing baseline limitation: processing leases survive a gateway restart and
  can keep typing until their bounded expiry even if the assistant disappeared.
  A processing lease is permission for feedback, not proof of model liveness.
- Go's rate-limit pacing, nonce binding and one-attempt POST logic have offline
  tests. These are not end-to-end response-time guarantees. Parallel ancillary
  requests do not reserve remaining bucket capacity in advance; future work can
  improve pacing without retrying attempted message POSTs.

## Checked parity and verification limits

Source and tests preserve immutable reply binding, one claim per conversation,
expired-lease reclaim and stale-token rejection, explicit processing leases,
per-chunk ordering, uncertain crash recovery, no uncertain-send retry, owner-only
route policy, shutdown cancellation, transient typing retry, and disconnect
typing cancellation. Named-consumer recovery is an improvement over Python's
anonymous claim protocol and deliberately does not renew a recovered claim.

The available tests cover transport/queue helpers and selected lifecycle pieces;
they do not prove the complete responding-assistant scheduling path or every
Gateway reconnect sequence. Historical transport tests, CLI microbenchmarks and
native daemon uptime cannot substitute for that proof. See `TEST_REPORT.md` for
the exact checks run against the revised source.
