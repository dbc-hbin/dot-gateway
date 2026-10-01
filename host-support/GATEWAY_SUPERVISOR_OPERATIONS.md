# Gateway supervisor operations

Source-only operational guide. Historical deployment observations are omitted. The launcher provides host-lifetime supervision, not reboot auto-start. Configure an explicitly approved credential-free proxy for the destination environment.

## Offline checks

`python test_gateway_supervisor.py` (14 tests)

`python test_gateway_daemon.py` (4 tests)

Tests use temporary directories and fake Python children only. They do not read
or write the live SQLite ledger, open the bot-token file, or access the network.

## Explicit operations (only after authorized cutover)

Use absolute paths, e.g. ROOT=/opt/assistant-project.

Run these operations in the native host terminal. The sandbox exec PID namespace
cannot inspect or stop the native daemon. `bash $ROOT/launch_gateway_native.sh`
is the reviewed detached launcher. `gateway_daemon.py status` and `stop` use
the native host identity. Native status snapshot is `gateway_native_status.json`;
its contents are historical, so rerun `inspect_gateway_native.py` to refresh it.

- Run: `$ROOT/.hermes-venv/bin/python $ROOT/gateway_supervisor.py run --runtime $ROOT/.gateway-supervisor`
- Status: `python $ROOT/gateway_supervisor.py status --runtime $ROOT/.gateway-supervisor`
- Request graceful stop: `python $ROOT/gateway_supervisor.py stop --runtime $ROOT/.gateway-supervisor`

The run command remains foreground. A proven authorized native launcher or actual
service manager must own its lifetime and capture its stdout to a private log.
Do not start it while another gateway is active. Existing wrapper and dispatcher
locks still guard the original and recovered ledgers; they are not removed.

Status `running` means supervised process exists, **not Discord readiness**.
After stop, check status becomes `inactive` and last_status is `stopped`.
`orphan_locked`/`locked_unknown` are fail-closed: do not delete the lock, launch a
second sender, or signal a stored PID. Investigate and validate the child identity.

## Routing guard

Inherited environment is preserved. Optional `--proxy-config PATH` reads only a
private, owner-owned regular JSON file with exactly `BRIDGE_HTTPS_PROXY` and
`BRIDGE_NO_PROXY`. The bridge's local validator rejects credentials, malformed
URLs, unsupported proxy protocols, inconsistent Discord routes and direct-route
bypasses. No token variables are copied into files.

`proxy-fingerprint` prints a SHA-256 hash of the routing environment, never its
values. With `--proxy-config PATH`, it hashes the effective environment after those
two overrides. Pass that exact hash via `--expected-proxy-sha256 HASH` on `run` to
reject changed routing environment before any child launches. Fingerprints are
not connectivity checks. No fallback routes are implemented.

## Failure and shutdown behavior

- At most 6 child starts per invocation, delays 2, 4, 8, 16, 32 seconds by default
- Every failure counts against the lifetime budget; it never silently resets
- Clean child exit stops; known auth/permission/identity/config failures latch
- Generic failures exhaust a finite budget; no indefinite authentication retries
- Restart after a latch requires a deliberate new invocation after diagnosis
- SIGTERM/SIGINT to supervisor sends SIGINT to its owned child, allowing existing
  asyncio cleanup to finish; default drain 30 seconds, then forced child kill
- No retry of individual uncertain message sends; existing ledger logic is intact
- Child stderr is discarded; stdout is bounded-parsed only for allowlisted fatal
  JSON enum codes, with all raw text discarded; logs contain static events, enums,
  numeric timings/exit codes, and process identities, never captured environment
- Supervisor lock is inherited by child, so an orphan prevents replacements
- Stop requires held lock, matching boot ID/process start ticks/non-zombie state,
  command identity/runtime, and Linux pidfd signalling; no stale PID fallback

This does not install a boot hook, persist a service in host config, or promise
availability after reboot. The original exec-session gateway was gracefully stopped before this native
launch; never start a second gateway alongside it.

