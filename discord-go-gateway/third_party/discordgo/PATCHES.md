# Narrow DiscordGo Gateway patch

Upstream: https://github.com/bwmarrin/discordgo
Commit: `98dc1334978683bb0bec263fe809a70e3ec00e91`
Module version: `v0.29.1-0.20250705141350-98dc13349786`
Module checksum: `h1:gavjaVdaVKKrJKJNEeIaIXErLNra15sLydd8mgQRNbw=`
Module go.mod checksum: `h1:NJZpH+1AfhIcyQsPeuBKsUtYrRnjkyu0kIVMCHkZtRY=`
License: BSD-3-Clause; original LICENSE retained verbatim

This directory contains all upstream root Go sources and package tests, plus
upstream go.mod, go.sum, README.md and LICENSE. No examples, build products,
credentials, repository metadata or unrelated transitive modules are included.
The root module replaces only DiscordGo with this checked-in directory.

Upstream is three commits after v0.29.0. The delta adds READY resume gateway URL
support plus two unrelated minor changes: a component JSON tag and webhook flags.
The exact local delta is LOCAL_PATCH.diff. Every unmodified source can be checked
against UPSTREAM_SHA256SUMS; wsapi.go is the only intentionally modified file.

## Why the local patch is needed

The upstream resume fix handles opcode 9 by calling CloseWithCode. When opcode 9
arrives during the initial/reconnect handshake, Open already holds Session's
mutex. CloseWithCode attempts to lock it again and deadlocks. Closing TCP alone
cannot unblock this mutex wait. Opcode 7 has the same handshake issue, as does an
unexpected opcode 11 lock. A fake local-server reproducer confirmed the stack.

The patch explicitly identifies event handling under Open's existing lock.
During that phase opcodes 7 and 9 return ErrGatewayReconnect to Open's deferred
socket cleanup; opcode 9 false first clears session ID, resume URL and sequence.
Opcode 9 true and opcode 7 retain session identity. For a normal listener event,
nonresumable state is cleared under the mutex before Disconnect is emitted.
Heartbeat ACK state does not recursively re-lock during the handshake.

The bridge supervisor, with SDK autonomous reconnect disabled, handles the typed
restart outcome using 1–5-second jitter under its existing total attempt deadline.
It does not send another identify or resume until the old Open has returned and
closed its socket. Cancellation interrupts TCP/TLS/handshake reads and retry wait.
Production READY URLs and every dial remain subject to the bridge's WSS Discord
host/proxy validation and no redirect-following behavior.

Regression tests are in ../../internal/bridge/session_handshake_test.go and
session_resilience_test.go. They use fake tokens and local WebSockets only.
Upstream package tests are preserved; when running them unset DGB_TOKEN,
DGU_TOKEN, DG_OAUTH2_TOKEN and DG_* route variables so credential-gated integration
tests skip. Never run those integration tests with live credentials.

Remove this replacement only after an official upstream revision fixes these
locked-handshake cases and the bridge's complete offline race suite still passes.
