# Discord session and owner-DM resilience

## Independent route readiness

A connected, freshly identity-verified Gateway session may process owner DMs even
before GUILD_CREATE arrives or when the pinned guild parent cannot be used. DMs
still require owner-only policy plus a fresh type-1 channel response containing
exactly the configured owner as its sole recipient. Group DMs and other users
remain rejected; this change does not authorize broader routes.

Guild messages and threads have a separate epoch-fenced readiness gate. The
configured guild must be available with valid cached view/history and either
parent-send or thread-send rights, and a fresh REST GET must match the exact
configured guild/type-0 parent. Route-specific parent versus thread permissions,
thread parent/type/membership/archive checks, and preflight validation still run.

Guild permission loss and guild lookup 403/404/429/5xx or mismatches degrade only
the guild route. Status includes content-free `guild_ready`, `guild_failure` and
`configured_guild_unavailable`. Validation runs on relevant readiness events and
at most once per 30-second recovery tick; no message POST is retried. A restored
guild gate makes quarantined blocked work eligible for fresh validation. Invalid
credentials and bot identity mismatch remain global failures. Disconnect clears
both readiness gates; queued old-epoch checks cannot reopen either one.

## Secure Gateway resume

DiscordGo is pinned to official upstream commit
`98dc1334978683bb0bec263fe809a70e3ec00e91`, which implements READY's
`resume_gateway_url`. READY resume endpoints are validated before readiness:
WSS only, a valid Discord-owned `*.discord.gg` hostname, default/443 port,
no credentials, fragments or query, and the same configured proxy route. Every
WebSocket dial validates its endpoint again. WebSocket redirects are not followed.
The SDK adds the API version and JSON encoding on both original and resume dials.

A normal reconnect uses the supplied resume URL and original session ID/sequence.
Opcode 9 true and opcode 7 retain that session. Opcode 9 false clears resume URL,
session ID and sequence so the next connection uses the original gateway and
identifies afresh. Credentials and session values are never exposed in diagnostics.

The upstream revision can deadlock if opcode 7/9 arrives during Open, which already
holds the session mutex. A narrow checked-in `third_party/discordgo` replacement
returns a typed handshake-restart outcome to Open's deferred socket cleanup and
handles unexpected handshake ACKs without recursive locking. Normal invalid-
session state is reset before the disconnect callback becomes visible. The exact
upstream license, checksum manifest and one-file diff are preserved there.

The bridge remains the sole reconnect owner. Handshake restarts use 1–5-second
jitter under the existing 60-second attempt deadline, after the previous Open has
returned and closed its socket. Cancelling interrupts network reads or retry wait;
there is no concurrent second session, identification spin or blind message replay.
A failed overall reconnect remains a transient exit for the process supervisor.

## Verification and limits

Offline fixtures exercise independent DM quarantine/claim/one-POST delivery with
no guild permission, missing/unavailable guild state, guild 403/404/429/503 recovery,
auth/identity refusal, stale validation fences, READY endpoint/session/sequence
preservation, invalid endpoint/proxy refusal before network access, no redirects,
opcode 7, opcode 9 true/false, unexpected ACK, cancellation, and bounded retries.
All bridge tests use fake credentials, temporary SQLite and local transports.

The upstream package's own tests are retained. They contain credential-gated
integration tests; run only with `DGB_TOKEN`, `DGU_TOKEN`, `DG_OAUTH2_TOKEN` and
`DG_*` route variables unset, as documented in `third_party/discordgo/PATCHES.md`.

No live outage, reconnect, permission change, Discord mutation, deployment or
assistant-consumer restart is part of this offline verification. A full process
restart still cannot recover never-received messages without explicit recovery;
this patch does not implement history backfill or an inactive-session wake.

Official references:
- https://docs.discord.com/developers/events/gateway#resuming
- https://github.com/bwmarrin/discordgo/commit/98dc1334978683bb0bec263fe809a70e3ec00e91
