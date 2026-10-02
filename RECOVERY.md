# Recovery and deployment boundaries

The complete staged VM-reset path is in [recovery/VM_RESET_RUNBOOK.ko.md](recovery/VM_RESET_RUNBOOK.ko.md): pinned source/state recovery, separately authorized opaque token-file installation, receive-only bootstrap, native host-lifetime launch, single-consumer activation and no-replay checks. A fresh source commit does not silently rebind older private snapshots. Keep each pinned source/snapshot pair and any reviewed private upgrade overlay together.

## Normal gateway startup

Normal `activation-env --component gateway` validates an existing private bridge
DB/schema, secure token-file metadata, and explicit credential-free proxy. It no
longer requires activation attestations, a reviewed binary hash, consumer/history
paperwork, or clearing `RECOVERY_BLOCK.json`. Gateway transport still validates
expected bot identity and exact configured routes. Startup does not create a new
DB or alter historical blocked events, receipts, or disarmed catch-up state.

The separately requested `gateway-receive-only` and `headed` modes retain their
staged recovery gates. The staged runbook describes those stricter workflows;
its activation requirements do not apply to normal gateway startup.

## What can be recovered

This tree can rebuild source binaries after official tools/dependencies are installed. Go modules are pinned with `go.mod`/`go.sum`; Playwright 1.63.0 is pinned with its npm lockfile. The recorded build toolchain was Go 1.27.1, Node 24.19.0, npm 11.9.0 and Python 3.12.14. A real headed Linux Chromium 151.0.7922.173 was used previously. Toolchain/browser binaries and package caches are not in Git.

Install Go from https://go.dev/dl/, Node from https://nodejs.org/ and Chromium through a trusted official OS/package source. Preserve TLS validation, Chromium sandboxing and the real graphical desktop requirement. Do not silently substitute headless/Xvfb or reuse a user's browser profile.

## Source and offline checks

From `insane-search-migration/` with the approved Go on PATH:

```sh
export GO_BIN=$(command -v go)
mkdir -p bin
./tools/go.sh mod download
./tools/go.sh test -race ./... -count=1 -timeout 180s
./tools/go.sh vet ./...
CGO_ENABLED=0 ./tools/go.sh build -trimpath -o bin/insane ./cmd/insane
CGO_ENABLED=0 ./tools/go.sh build -trimpath -o bin/headed-broker ./cmd/headed-broker
CGO_ENABLED=0 ./tools/go.sh build -trimpath -o bin/discord-report ./cmd/discord-report
npm ci --prefix runtime/browser --ignore-scripts
node --test runtime/browser/test_adapter.cjs
```

From `discord-go-gateway/`:

```sh
mkdir -p bin
go mod download
go test -mod=readonly -race ./...
go vet -mod=readonly ./...
CGO_ENABLED=0 go build -mod=readonly -buildvcs=false -trimpath -o bin/dot-gateway ./cmd/dot-gateway
cp bin/dot-gateway bin/dot-bridge-cli
```

Host supervisor tests use temporary fake children only:

```sh
cd host-support
python3 test_gateway_supervisor.py
python3 test_gateway_daemon.py
python3 -m unittest -v test_recovery_control
```

Optional historical Python oracle tools are source references; they are not production prerequisites or part of the default native test path. Native `vendor/` is a provenance snapshot, not Go module vendoring. Original path/hash export records, the personal cron instance and historical runtime evidence are intentionally omitted, so this sanitized tree is not a byte-identical copy of the original migration export.

## Inert configuration examples

These values are placeholders and confer no authority or service access:

- `100000000000000001`: example bot
- `100000000000000002`: example guild
- `100000000000000003`: example report channel
- `100000000000000004`: example conversational gateway channel
- `100000000000000005`: example owner
- Other `10000000000000000x` IDs in documentation are examples

The report publisher's exact live target tuple in `internal/reporting/rest.go` and its config must be changed together only for the approved destination, then rebuilt and reviewed. Live credential and workspace paths are generalized under `/opt/assistant-project` and `/opt/assistant-shared`; they must be deliberately configured. The proxy example uses an `.invalid` hostname. The gateway launcher has a deliberate SHA placeholder and refuses an unreviewed build. Do not remove these safety gates to get a quick launch.

No token is stored. Reconnect credentials only with the user's approval through the official secure workflow, respecting user-entry requirements. Never place credentials in Git, command arguments, logs or chat. New security permissions or persistent access require their own authorization.

The optional `host-support/install_token_file.py` copies an existing private input file as opaque bytes only after the operator explicitly invokes `--apply` with current authorization. It never emits the credential or its hash, never replaces an existing destination, and does not confer approval. The Linux recovery controller requires Python 3.12+ with pidfd support. Go's offline recovery tool still never reads or transfers credential bytes.

## Delivery history and schedules

Restore and verify the newest separate private publisher outbox before sending. It contains exact run/payload/chunk hashes, nonces and receipt IDs but no report text. Never replace a newer ledger with this source repository or an older backup. Do not delete state, change run IDs or retry uncertain sends to bypass replay protection. Without a current ledger, pause delivery and reconcile against verified service history.

No live schedule ID or cron instance is in this repository. Inspect the existing authorized cloud scheduler first; do not create a duplicate. The disabled imported policy template must remain paused unless an explicit new deployment scope authorizes otherwise. A source checkout does not register or restore a scheduler.

The gateway SQLite database is intentionally absent because it contains real conversation and send history. Code alone cannot restore claims, quarantine, prior send attempts or deduplication state. Never silently initialize a new DB to replay old work. Preserve a current consistent SQLite backup under a separately authorized sensitive-data workflow when available; do not copy only a WAL-mode main DB or revert post-send history.

## No automatic recovery guarantee

The supervisor and browser launcher provide host-lifetime process management. No VM reboot or blank-VM full restore has been tested by this backup task, and no boot service is installed here. A connected gateway does not establish that an assistant-side consumer is active or reasoning. Revalidate the real display, credentials, permissions, process identity, consumer and ledger separately before authorized live use.

Never persist an execution-scoped localhost proxy into a detached service. Use only the current environment's approved, verified native host-lifetime route; the recovery launcher preserves explicit private proxy configuration and does not guess a replacement or bypass network policy. PID/boot/start-time/executable and dispatcher-lock checks plus a fresh post-launch DB heartbeat are required. `end_to_end_ready` remains false because transport health cannot prove model reasoning or receipt of CLI output.

No service restart, token reading, live message sending or new scheduler registration is part of the backup verification. `VERIFICATION.json` states exactly what was tested and its limits.

Worker-control observations and task bindings are runtime state, not proof that
an external reasoning process survived a restore. The content-free recovery
snapshot does not export the new worker protocol or follow-up key/output maps.
Do not reconstruct continuation keys, claim tokens, worker readiness, or stop
acknowledgements from it. A restored deployment must reconcile outstanding
external work and establish a fresh controller-attested incarnation before
binding new work; source publication alone does not activate that controller.
