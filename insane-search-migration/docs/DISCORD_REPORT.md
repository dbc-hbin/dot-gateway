# Bounded native Discord report publisher

`bin/discord-report` is a standalone Go publisher for prepared reports. It does
not start the gateway, impersonate an inbound user, run a model, collect data,
change permissions, create credentials, or activate any scheduler. It does not
modify `bin/insane` or the live conversational gateway.

The live-client boundary permits exactly bot `100000000000000001`, guild
`100000000000000002`, and text channel `100000000000000003`. The same exact tuple
must be provided in `config/discord-report.json`. Changing that authorized live
tuple requires an intentional code/config update and rebuild, not a CLI flag.

## Build and offline verification

```sh
./tools/go.sh test -race ./... -count=1 -timeout 180s
./tools/go.sh vet ./...
CGO_ENABLED=0 ./tools/go.sh build -trimpath -o bin/discord-report ./cmd/discord-report
sha256sum bin/discord-report
```

All tests use fake tokens and in-memory transports, temporary state, and no live
Discord traffic. This publisher adds no dependencies and needs no Python runtime.

## Read-only inspect

```sh
bin/discord-report inspect --config config/discord-report.json
```

Omitting `--proxy-config` selects the existing `HTTPS_PROXY` (or lowercase
`https_proxy`) environment URL. The URL must be credential-free HTTP; no direct
fallback exists. A NO_PROXY bypass for Discord is rejected. Alternatively use
the existing native desktop route explicitly:

```sh
bin/discord-report inspect --config config/discord-report.json \
  --proxy-config /opt/assistant-project/gateway_native_proxy_config.json
```

Inspect only performs GETs: authenticated bot identity, exact channel/guild/type,
guild owner, bot member, and guild roles. It calculates everyone, role and member
overwrites, rejects pending/timed-out membership, and requires View Channel,
Send Messages and Read Message History. It creates no ledger or messages.

TLS certificate checks remain enabled, TLS 1.2 is the minimum, the API origin is
fixed at `https://discord.com/api/v10`, and redirects are never followed. The sole
credential source is `/opt/assistant-shared/.discord-private/bot-token`, read internally
with O_NOFOLLOW, regular-file, same-owner, private-mode and single-link checks.
Credentials and HTTP response bodies are never printed. Do not print, copy, or
supply the token through CLI arguments or environment variables.

## Publish a prepared report

```sh
bin/discord-report publish --config config/discord-report.json \
  --state-dir /opt/assistant-shared/ai-benefits-cron/outbox \
  --run-id first-test-20261001T023000Z --file /absolute/path/report.txt
```

Use `--file -` (the default) for stdin. The input must be UTF-8, at most 64 KiB,
nonempty, and free of unsupported control characters. Splitting prefers newlines
or spaces, never breaks a rune, and produces at most 40 messages of at most 1900
UTF-16 units. Message-boundary whitespace is trimmed because Discord normalizes
terminal newlines; internal message whitespace remains intact. The full original
input and each normalized chunk have separate immutable SHA-256 bindings.
The overall deadline defaults to five minutes (configurable from 1 second to
15 minutes), including waiting for input. Report files cannot be symlinks or FIFOs.

The active token and high-confidence OpenAI/Google/GitHub/Discord/private-key or
labeled credential patterns are rejected before state/network activity. This is
not a comprehensive secret scanner. Do not pass account pools, keys, or obfuscated
credentials; prepare public, verified offer content upstream. Public coupon codes
without credential labels remain permitted.

Each chunk uses `allowed_mentions.parse: []`, empty user/role mention lists,
`replied_user: false`, suppressed embeds, a stable 24-hex nonce and
`enforce_nonce: true`. It is an ordinary standalone message, not a gateway reply.

## Durability and retry rules

- Keep the same private state directory across all runs. Its target pin is
  immutable; all directory components reject symlinks. The final directory must
  be same-owner and private, and files must be private same-owner regular files
  with one link. The publisher creates missing directories with mode 0700
- An exclusive flock allows one publisher per state directory
- Each stable run ID binds its original payload, exact target, ordered chunk
  hashes and nonces. Reusing a run ID with different bytes is rejected
- Before each chunk's single POST, fresh identity/scope/permission preflight
  passes, then an attempted marker is atomically persisted and fsynced
- No POST retry exists, including on 429, transport failure, malformed response,
  crash or uncertain acknowledgement. The Go POST body is non-replayable
- Acknowledgements must match author, channel, exact sent content and message
  type. A nonce, if present in the optional REST field, must also match
- The acknowledged message ID is persisted before GET read-back. Read-back must
  match author/channel/ID/exact content before a confirmed receipt is saved
- The same run returns existing confirmed receipts without sending again. If an
  acknowledged ID exists but its read-back failed, retrying the identical run can
  perform GET-only reconciliation and then send only previously unattempted chunks
- A failed, attempted-without-ID or uncertain-without-ID chunk blocks the run.
  Inspect Discord before taking manual recovery action. Do not manufacture a new
  run ID or delete state to retry an ambiguous send: that can duplicate content
- Discord's nonce deduplication only covers recent minutes; durable local state,
  not nonce alone, provides long-term replay prevention

A crash can conservatively block a message that never reached Discord. A partial
multi-chunk report can remain partially delivered. The ledger is local; deleting
or replacing it defeats replay protection. Concurrent processes must share the
same state directory. Permission changes can still race between GETs and a POST;
Discord remains the final authorization authority. Confirmed receipts record a
verified historical delivery, not a claim the message can never later be edited
or deleted. There is no automatic ledger pruning.

Reference: [Discord messages](https://docs.discord.com/developers/resources/message)
and [permission hierarchy](https://docs.discord.com/developers/topics/permissions).
