# Reasoning-worker lifecycle and cancellation

## Boundary

This is a durable controller protocol, not a model API or process supervisor.
The Go gateway cannot start, observe, resume, or interrupt a native reasoning
agent. For the current native-agent deployment, the parent controller must use
its supported collaboration tools, then record the actual result. No process,
transport, CLI polling, typing, or claim heartbeat is treated as model health.

`worker-status` and `status.worker_control` explicitly report
`native_control_available: false` and `automatic_restart: false`. An attestation
is the trusted local controller's assertion; the gateway cannot independently
verify that a referenced native tool call happened. Never manufacture an
observation or interruption ACK to make status look healthy.

The implementation is original Go, informed by Hermes's separation of real
interrupts, generation fencing, and restart-resume ownership. See
[THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md) for source and license details.

## Identity and lifecycle

- `worker` identifies the native task, for example `/root/worker`.
- `incarnation` identifies one execution generation / turn, not the reusable
  native-agent identity. Choose a new identifier for each subsequent turn.
- `controller` identifies the parent allowed to control that native task.
- Each incarnation may bind exactly one immutable inbound claim. Even after an
  early reply is delivered, it cannot bind a different request. This prevents a
  late `/cancel` for the first request from interrupting unrelated later work.
- These identities are fencing and audit data within the existing trusted
  OS-owner boundary. They do not authenticate a different OS user.
- Evidence references are short non-secret opaque identifiers of observations,
  ownership results, or interrupt results. Do not put prompts, tool-output bodies,
  credentials, email addresses, or other private content in them.

A controller observation is valid for 1–300 seconds (default 60). Effective
`state` becomes `unknown` when its lease expires, while `attested_state` remains
available as historical evidence. Registration retries do not renew the lease;
only an explicit `worker-observe` can record a new observation. A controller must
actually inspect the native run before renewing it. No periodic CLI heartbeat
should invoke `worker-observe` automatically.

States are `running`, `idle`, `completed`, `interrupted`, and `failed`.
Registration starts at `running` (default) or `idle`; binding requires a fresh
`running` observation. `completed` is an assertion that the native turn really
finished. It is rejected while the bound inbound remains claimed or a
cancellation is awaiting ACK. Completion fences the retained continuation claim,
so an old turn cannot append another follow-up after completion.

`completed`, `interrupted`, and `failed` are terminal for that incarnation.
Returning to work requires an explicit successor registration. The controller
must observe that a failed/interrupted native turn is actually stopped before
recording either terminal state; a timeout is not such an observation.

## Commands

All commands use the configured private `BRIDGE_DB`. They perform no Discord or
model request. Flags take values, including `--state` and `--lease-seconds`.

Initial registration after the native worker has really started:

```sh
dot-gateway worker-register /root/worker \
  --incarnation turn-1 --controller /root --state running \
  --lease-seconds 60 --evidence-ref native:start-1
```

After `next` returns an exact claim, the controller binds the actual run before
it performs work:

```sh
dot-gateway worker-bind INBOUND_ID --claim CLAIM \
  --worker-id /root/worker --incarnation turn-1 --controller /root \
  --evidence-ref native:claim-1
```

Record a new real observation, inspect status, or list pending cancellations:

```sh
dot-gateway worker-observe /root/worker --incarnation turn-1 \
  --controller /root --state running --lease-seconds 60 \
  --evidence-ref native:observation-2
dot-gateway worker-status
dot-gateway worker-cancellations /root/worker \
  --incarnation turn-1 --controller /root
```

`worker-cancellations` returns an object containing a `cancellations` array.
It is a durable read, not a dequeue: a lost result can be read again. Wrong
controller or stale incarnation arguments fail rather than returning or changing
a newer turn's work.

## Cancellation

Owner route and immutable source revision are checked by `/cancel` before any
change. Queue suppression and the durable execution-cancellation record commit
atomically:

1. Revoke the request's claim, consumer recovery mapping, and processing lease.
2. Suppress pending output and invalidate response controls.
3. If a claim may be executing, persist `cancel_requested` for the exact bound
   worker, incarnation, and controller. No “stopped” result is reported yet.
4. The real controller reads the request and interrupts that exact native turn.
5. Only after the real interruption completes, acknowledge it:

```sh
dot-gateway worker-cancel-ack INBOUND_ID \
  --worker-id /root/worker --incarnation turn-1 --controller /root \
  --evidence-ref native:interrupt-result-1
```

ACK records `acknowledged` / `interrupted_controller_attested` and a terminal
worker observation. Exact repeated ACKs are idempotent; different evidence,
controllers, targets, or retired incarnations are rejected. No lease expiration,
transport disconnect, delivery success, or observation alone acknowledges a
cancellation. Recovery cannot discard a still-pending cancellation.

The inbound queue's internal `cancelled` state is a delivery fence, not proof
that a native worker stopped. The separate durable `worker_cancellations` row
remains `cancel_requested` until ACK. Request status exposes both execution
cancellation and delivery. Already delivered early answers remain delivered;
sending/uncertain chunks remain unresolved. Neither ACK nor cancellation reverses
external actions or retries a potentially completed POST.

A pending request with no acquired claim can be cancelled locally with
`not_required` / `no_active_claim`: no execution claim needs interrupting. A
previously queued reply without a worker binding also only has its local output
cancelled; this does not attest that any model execution stopped.

### Unbound worker

Cancelling an active legacy/unbound claim records `cancel_requested` with
`execution_state: worker_unbound`. It stays pending through restarts and repeated
`/cancel`; it is never silently upgraded to successful interruption.

If the controller can establish which exact native turn owned the original
claim, it may register that real incarnation and call `worker-bind` using the
preserved original claim receipt and an ownership evidence reference. Binding a
pending cancellation does not reactivate the cancelled claim. The controller
then performs the real interrupt and ACKs it normally. A wrong historical claim
or different existing binding is rejected.

If the native turn or original claim receipt cannot be established, leave the
request pending and report the need for controller reconciliation. This protocol
has no “force stopped” or unverified unbound ACK command.

## Explicit recovery

Expired bound claims are not automatically reclaimed by `next`, even if worker
observations are stale. This avoids running a duplicate native turn merely
because a heartbeat was missed. The immutable execution binding survives source
refreshes that revoke or clear the queue claim: cancellation still targets its
original claim and native incarnation. While that execution remains unresolved,
later work in the same conversation also stays fenced, including after an early
reply. A controller-attested completion or stop releases the execution fence;
any explicit-recovery quarantine still applies. Legacy unbound claim recovery
is unchanged.

After the controller really interrupts or otherwise verifies the old native
turn stopped, it records `interrupted` or `failed` with `worker-observe`.
Outstanding exact claims become `worker_recovery_pending`; their tokens are
revoked and later work in the same conversation stays fenced. These rows remain
visible in status and count toward queue capacity. Unresolved delivery evidence
and already queued output are preserved.

Resolve pending cancellation ACKs first. After the replacement/resumed native
turn actually starts, register a new incarnation with the exact predecessor:

```sh
dot-gateway worker-register /root/worker \
  --incarnation turn-2 --controller /root --state running \
  --previous-incarnation turn-1 --evidence-ref native:resume-2
```

This compare-and-swap registration releases only the predecessor's quarantined
claims. `next` returns a new claim token, which must be bound to the new turn.
Source deletion/supersession still wins; recovery cannot resurrect that work.
A completed turn uses the same next-incarnation registration path after a real
`completed` observation, without falsely claiming it was interrupted.

Retired incarnation identifiers are retained durably and cannot be reused.
Neither registration nor database reopening launches any work. A stopped native
controller still needs a supported external controller or an operator; a Go
transport supervisor does not provide that capability.

## Verification

Disposable-store and subprocess tests cover stale observations independent of
CLI health; lost-registration-result retries; exact owner/revision cancellation;
claim suppression; unbound reconciliation; pending cancellation persistence
across reopen; wrong and retired incarnation rejection; idempotent ACKs; explicit
recovery; old-claim fencing; queue capacity and conversation order during
recovery; source deletion; normal completion; delivered/uncertain early replies;
and prevention of cross-request binding within one incarnation. Native
interrupts are represented by controller attestations in these tests, not
performed against a live agent.
