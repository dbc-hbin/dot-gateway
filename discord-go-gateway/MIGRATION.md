# Controlled adoption and rollback

Deployment is coordinated through the native supervisor, which remains Python
infrastructure. Gateway and consumer CLI are Go; no claim is made that every
host infrastructure process is Go.

1. Finish offline race/vet/security/contract tests and reviewer approval.
2. Use the existing native supervisor stop flow. Confirm its gateway child exited
   and the legacy DB `.lock` is released. Never run two senders.
3. Create a consistent SQLite backup using SQLite's backup API. Do not copy only
   a WAL-mode main database while writes are active. Retain the original files.
4. Update the supervised child launch to the compiled binary with `gateway`,
   existing exact environment scope and existing secure token-file path. Use the
   native-host credential-free proxy configuration, not the exec environment's
   proxy. Do not read or copy the token. No permissions or auth grants change.
5. Start exactly one child. Existing tables are adopted in an immediate
   transaction; additive tables/indexes include timing, consumer observations,
   per-attempt `send_measurements`, and the private `ingress_validation` quarantine. Historical terminal
   feedback is marked `history` only when upgrading a DB without feedback.
6. Startup recovers previously `sending` ordinary/diagnostic records as
   `uncertain`; these must never be blind-resubmitted. Check content-free status,
   pinned identity/permissions, heartbeat and actual supervisor child lifetime.
7. Switch consumer wrapper to the Go CLI only after ready. Verify an ordinary
   real inbound can be claimed, processing typing starts, authored text is
   queued, private file wake dispatches it, ACK is persisted and 👀 is removed
   without adding a completion checkmark reaction.
8. Only after reviewed authorization, queue diagnostic slots one at a time.
   Observe each terminal result; uncertain means stop/review, not rerun.

Rollback: stop Go via the supervisor and verify its lock and process released.
Then count **all** `ingress_validation` rows using the current shared ledger.
Only a verified zero count permits restarting an older Go or Python runtime.
Older versions cannot validate/promote quarantined work and do not account for
its shared capacity. A nonzero or unknown count prohibits a legacy restart;
preserve the current ledger and forward-fix using a quarantine-aware build.
Never delete or blindly promote quarantine rows to make rollback possible.

When the zero-count guard passes, use the **current shared ledger**. Python
ignores additive tables and sees ordinary replies/attempts.
Do not restore an older backup after any live send: that would forget attempt
history and could duplicate delivery. Already attempted diagnostics remain
immutable in their own table even if the old runtime is temporarily restored.

Quarantined plus pending/claimed work shares the existing 1,000-item capacity.
Staged input is durable but not yet accepted for reasoning: no claim, receipt
reaction or processing lease is issued before exact route validation. Transient
GET failure retries are read-only; blocked 401/403/404 or route mismatch remains
visible and preserves same-conversation order. A validated reconnect permits
revalidation; authentication failures retain the permanent supervisor latch.
Deployment checks must include validation pending/blocked counts and oldest age,
not merely gateway process uptime and heartbeat.

The legacy network-only `test-send-discord` / `verify-test-send` commands are not
new Go CLI commands. Their read-only status is supported and their ledger is
preserved. New tests use the separate three-slot diagnostic path.


