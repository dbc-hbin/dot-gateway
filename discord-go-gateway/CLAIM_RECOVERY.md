# Recovering a lost consumer result

## Contract

`next --consumer-id <stable-name>` (or `BRIDGE_CONSUMER_ID`) identifies one
logical consumer across reconnects, process exits and restarts.
An explicit flag takes precedence over the environment. Names are 1–128 ASCII
letters/digits or `_`, `-`, `.`, `:`. Unset/empty environment retains legacy
behavior; an explicitly empty flag is rejected.

The store atomically returns the consumer's one current, unexpired claim before
looking for new work. Replays return its current lease and exact token without
renewing it, changing processing feedback, or inserting another claim timing.
Expired ownership is never revived: ordinary acquisition assigns a fresh token.
Replied/ignored/revoked or superseded claims cannot be replayed. Existing
policy checks and per-conversation admission ordering remain in effect.

This is recovery of outstanding work, not permanent request-response caching.
After a reply or ignore, `next` may return new work. Two overlapping polls with
the same key can both receive the same claim, so only use a key for one logical
consumer. Independent consumers must use distinct keys. Existing immutable
reply binding prevents different second replies; existing chunk state and
single-attempt POST handling prevent duplicate sends. Consumer names are an
idempotency namespace under the existing private-file OS-owner trust boundary,
not a new authentication mechanism or public endpoint.

On a detected stdout error the CLI exits nonzero and writes only
`result_output_failed` to stderr. The committed claim is deliberately retained.
A transport can still drop output after a successful write; the same recovery
path handles that case. Never blindly resend a reply or release its claim based
on missing output: recover `next` with the same key or inspect delivery state.

## Coordinated deployment

1. Stop/quiesce old anonymous `next` subprocesses from the execution contexts
   that launched them. Inspecting an unrelated process namespace is insufficient.
2. Check for an outstanding anonymous claim. It cannot be adopted by name;
   finish it with its known token or let it expire. Never replay already replied
   incident inbound `62e871ba22ed44e2944862dd03b616a8`.
3. Deploy the candidate binary through the normal supervisor cutover; do not
   overwrite an executing binary. The gateway protocol/schema stays compatible.
4. Set `BRIDGE_CONSUMER_ID=dot-discord-reasoning-v1` in the dedicated consumer
   wrapper, or pass that explicit flag for every `next`. Do not reuse it for
   another independent consumer. Direct legacy CLI invocations remain supported.
5. Reconnect/retry using the identical stable key whenever a poll result is lost.

The failure regression uses disposable ledgers and no live Discord messages.
The deployment preserved all 11 existing replies and all three diagnostic send
records. No incident message was replayed.
