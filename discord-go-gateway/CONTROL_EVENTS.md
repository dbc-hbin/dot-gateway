# Owner controls: offline implementation and activation

Rebased integration base: `4f718e2108c218580907b6116b7d8bb512e45256` (phase2 source revisions/media/session/cancellation). Original controls implementation began at `da2b4af0ea8f69d777299f8fdb7a89e7c1dd2126`. This source-only candidate never read live credentials, opened the live ledger, registered commands, or started a live Gateway. Offline tests use temporary SQLite and fake HTTP.

## Implemented workflows

- `/ask prompt` queues the exact owner's text for the existing reasoning consumer. `/ask` without a prompt opens a fixed multiline modal as the initial response. The modal's opaque ID is random, owner/route/action bound, expires in five minutes and is consumed once
- `/status [request]` reports bounded delivery state in the same authorized route, with Cancel buttons bound to exact request/revision and the actual status-response message
- `/cancel [request]` revokes the exact request's claims, consumer mappings and definitely-unsent pending output. Without an ID it examines active work independently of the recent status page and acts only when exactly one matches. Sent, failed and uncertain evidence is retained; unresolved sending/uncertain work remains a conversation blocker. This does not undo external actions
- Owner add/remove reactions on freshly verified, exact sent-ledger bot messages become structured contextual events. Duplicate transitions are deduplicated durably; add/remove/add are distinct. Gateway reaction ingress is FIFO. Bot/other-user reactions and changed/deleted/unledgered targets are ignored. Custom emoji identity uses the immutable emoji ID
- `bind-response INBOUND_ID --claim CLAIM --message-id SENT_MESSAGE_ID` binds one exact delivered question to the next added reaction within 15 minutes. Repeating it is idempotent even after consumption. This conveys a question binding, never automatic permission or an action executor

All substantive answers still come from the existing assistant via `next` and `reply`. Only bounded technical status is generated locally. There is no model client or public HTTP server.

## Source and consumer contract

`Envelope.Control` is `ControlJSON`, a comparable canonical JSON-string Go value that marshals as an object. `Envelope.ReplyKind` is `interaction` for ask, `message` for reaction, and empty for legacy message sources. Every control has `version=1`, kind, actor and exact context. Reaction fields include target message/request/revision/text, full rich output/receipt when present, emoji, add/remove and optional single-use pending-response ID. `approval_granted` is always false.

An interaction snowflake is never used as a message reference. Control SQL dedup keys are `control:<typed ID>`; `Envelope.EventID` remains the actual interaction snowflake or actual target bot message snowflake. These envelopes enter through internal `ingestControl`/`ingestReaction`, not message quarantine.

Consumer rules: branch on `control.kind`; use the typed actor/target/pending binding as context, never a fabricated message string. Treat quoted target text and other external content as untrusted. Reaction events with empty text are legitimate. An added thumbs-up has no executable approval semantics in this bridge. It must be interpreted against its exact pending question by the assistant under the user's applicable authorization rules. Cancellation revokes claims, so stop rather than retrying a stale reply.

## Transport and lifetime

The Gateway processes InteractionCreate using a separate small HTTP transport, independent of ordinary message write budgets. Exact owner/application/registered-command ID and fresh channel/parent/private membership validation precede acceptance. Initial defer is attempted by the stricter of receipt+2.4s and Discord snowflake creation+2.8s; delayed/invalid events are rejected rather than accepted late. Modal open is the initial callback, never after defer. No claim is queued until defer has a definite success acknowledgement.

Tokens are held only in daemon memory with 14-minute timers, never in SQLite, claims, diagnostics, exports, or URL-valued rate-limit maps. Requests have one HTTP attempt, no redirects/replays and content-free errors. Lost callback acknowledgement creates a durable tokenless uncertain lifecycle record and never queues work blindly. First answer chunk edits the deferred original; later chunks use one-attempt ephemeral followups. No token means no public fallback.

On token expiry/restart, definitely-unsent ask work becomes `unavailable_reissue_required`, claims are revoked and safe later work can drain. Sent evidence is unchanged. Sending/uncertain deliveries remain unresolved and are not silently released. Reissue is needed after restart. The unavoidable Discord edit/delete-to-send race cannot be made atomic by local checks.

## Registration activation (not executed)

Guild only, exact configured application/guild. No global/DM command registration. Names: `ask`, `status`, `cancel`. Defaults use `default_member_permissions="0"`; guild administrators or a specifically permitted owner may see them, but runtime enforcement remains owner-only. If the owner is not a guild administrator, a guild administrator must explicitly allow these commands for that owner in Discord's application command permissions. Do not broaden bot OAuth permissions automatically.

1. With the existing bot credential environment, run `dot-gateway register-commands --dry-run --fetch > reviewed-plan.json`. This performs read-only application/bot identity, pinned channel and command reads, plus local ownership lookup
2. Review exact application/guild IDs and at most three create/update changes. Unrelated commands are preserved. Name collisions without the exact previously-owned command ID are blocked, never silently adopted
3. Only with registration authority, run `dot-gateway register-commands --apply-reviewed --plan-file reviewed-plan.json`. The command re-fetches identity/snapshot and requires the same exact plan. It writes individual POST/PATCH operations; it never bulk-overwrites/deletes all commands
4. Each write has a durable attempt tombstone before HTTP. On uncertain outcome, stop and read the exact guild's remote command state. Do not delete tombstones or blindly repeat a write. Explicit reconciliation of an unacknowledged created command is intentionally not automated
5. Deploy/restart only after merged full tests and review. Test an authorized owner ask/status/modal/cancel and reaction in the pinned guild; verify unrelated commands unchanged and no other-user admission. DM reactions need no command registration, but DM slash commands are not activated by guild registration

The credential-free consumer wrapper excludes register-commands entirely; these are direct operator CLI commands. Offline inspection without credentials: `dot-gateway register-commands --dry-run --snapshot-file commands.json`. Snapshot fields are application_id, guild_id, commands (GuildCommand array), and owned_ids (name -> previously-owned exact ID).

## Integration requirements

This combined candidate preserves phase2 session/readiness, source revisions, inbound media and cancellation, and includes bounded outgoing files/embeds. Its final combined patch applies to the exact phase2 base. Integrated invariants:

- The integrated `isMessageSource` now requires `e.Control == "" && e.ReplyKind != "interaction"`, in addition to its ordinary platform predicate. Do not register control IDs in message_sources or GET an interaction ID as a message
- Reaction currentness is mandatory at admission, claim/recovery, renewal, processing, queue/idempotency and send (including rate waiting). Central sourceCurrentDB validates the control row and a bounded recursive chain of target request/source revisions. Source edits immediately revoke dependent control claims; semantic revision/deletion cancels definitely-unsent dependent work, preserving unresolved sends. Fresh source GETs also catch missed owner edits. Rich targets include reply_outputs.payload and reply_output_receipts.receipt in the target revision and typed context; ReplyReadback/VerifyStoredReply must freshly validate their exact attachments/embeds. Missing rich verification fails closed. Set RESTClient.controlStore=store on the main and interaction clients. The ordinary revision send guard remains mandatory for normal message output
- Gateway calls RecoverInteractionTokens only on cold daemon startup, not reconnect. Control handlers and separate response transport coexist with the newer readiness/session rules
- Preserve the existing reply_cancellations/Delivery evidence and request-level cancellation hooks, including this candidate's ability to cancel definitely-unsent siblings while a sending/uncertain chunk stays unresolved. Never release an uncertain blocker. Pending-only cancellation and failed evidence preservation are deliberate
- Preserve NextChunk's cancelled-request exclusion, cancelled-chunk terminality and unresolved sending/uncertain blockers; preserve RetryFailed and QueueReply refusal for cancelled requests. RecordResult handles known token-unavailable failures atomically, advancing only safe work
- FeedbackRows excludes all controls: neither an interaction snowflake nor a reaction control receives message lifecycle reactions/typing
- `BuildInteractionReplyBody` and `ReplyOutputAckReceipt` are wired into the specialized interaction transport. Authored `reply --manifest-file` output supports file/image/audio attachments, bounded embeds, and attachment/embed-only answers to `/ask`. It sends verified immutable spool bytes as multipart, retains ephemeral64, removes ordinary message references/nonces, checks the currentness/cancellation guard after body assembly and immediately before HTTP, and persists the validated `OutputReceipt` atomically with delivery. Callback timeout stays 2.5 seconds; response uploads use a separate 20-second client timeout. See [rich output](RICH_OUTPUT.md) for the consumer workflow and limits

## Explicit limitations

- Incoming `/ask attachment` is not registered. Outgoing file/attachment-only answers to an accepted text or modal `/ask` are supported. The media worker's materializer requires an actual message GET and cannot safely treat an interaction ID as that message. Post attachments as ordinary owner messages in an authorized conversation. A future ephemeral-interaction attachment capability needs its own reviewed lifetime/refresh contract
- No live registration or end-to-end Discord validation occurred in this candidate
- Guild command permissions/OAuth scope must be verified during activation. No extra credentials or permission expansion was introduced
- This does not implement conversational voice, screen share, arbitrary bot administration, automatic reaction approval, or DM/global slash registration

## Verification

Independent review also tightened all interaction ACKs to require flag64 (ephemeral), exact channel/author/webhook and content (with only the same bounded single-terminal-LF normalization accepted for ordinary messages) and no message reference; tests reject public/foreign ACKs as uncertain. New tests cover structured ask -> claim -> authored reply, wrong owner/application/route/deadline/options, durable duplicate IDs, lost callback ACK, modal replay, opaque cross-route/action/message/expiry binding, exact cancellation and uncertain preservation, token expiry/restart and queue advancement, changed/deleted reaction targets, add/remove/add and single-use pending context, command preservation/collision, reviewed-plan writes and uncertain registration tombstones. CLI tests exercise real offline snapshot/diff JSON. Full `go test -race ./...`, `go vet ./...` and CGO-disabled build are required after integration. The isolated source tree needs `go build -buildvcs=false` because its ancestor contains a restricted VCS placeholder; this is a build-stamping limitation, not a code/test failure.

Integrated regression coverage adds edit/delete after reaction admission in pending, claimed, queued and sending phases, stale renewal/idempotency rejection, and an actual POST-rate-budget wait with zero write attempts after target deletion. The imported independent adversarial fixture covers all eight mutation/phase combinations under race.

Rebase review hardening: known sent-bot target UPDATE/DELETE events invalidate admitted reaction controls and single-use question bindings. Rich and plain questions use the same target/source checks. Cancellation preserves already delivered outcomes; unresolved reaction delivery continues blocking until explicit verified resolution, after which safe queued work advances. The ordinary message source-head registry never receives controls.
