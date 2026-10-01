# Message source revisions and bounded context

This layer adds source lifecycle and context to cancellation, media metadata/materialization, and the DiscordGo/session-resilience patch. Regression tests use fake REST and temporary SQLite, never live credentials or Discord mutations.

## Invariants

- The real Discord identity is always `Envelope.EventID`, `ConversationID`, `GuildID`, owner and verified parent. Old inbound envelopes are never rewritten.
- `message_sources` has a unique actual `(platform,event_id)` head, source revision, independent refresh generation, exact route, durable state and canonical snapshot. A deletion is a durable tombstone.
- Existing inbound/quarantine `UNIQUE(platform,event_id)` is retained without rebuilding FK-linked tables. Revision zero keeps the original SQL event key. Later SQL keys use `sourceLedgerKey`: `realID:revision:N`. The envelope still carries the original snowflake, so message reference, API path, attribution, owner checks and nonce binding remain exact. No API uses the SQL synthetic key.
- Each meaningful edit creates a new inbound ID and therefore a new claim/reply/nonce namespace. Old replies keep their original source via their immutable old inbound row. Duplicate create/recovery reads the actual head and cannot revive an old revision or deleted message.
- Create/update/delete/bulk-delete share one FIFO. An update payload supplies no authoritative partial content; it only invalidates a known exact route immediately. The refresh worker retrieves that single source via route-validated REST. Missing guild fields use the already stored exact channel/message binding; an explicitly wrong guild is rejected.
- Generation compare-and-swap rejects out-of-order refresh completion. An observed-send GET cannot steal a newer queued update generation. A 404 on the exact message endpoint is deletion; permission/transport errors are not inferred to be deletion.
- Authored text/media/reference fingerprints exclude signed URLs, autoembeds, poll vote counts and edit timestamps. Cosmetic updates do not make another user task. Legacy text-only snapshots have a conservative unchanged-text/reference path only while the fresh message has no authored media.
- Known deleted or superseded work revokes claims/processing/consumer ownership and cancels definitely-unsent reply remainders using the shared cancellation primitive. Sent rows and remote IDs remain forensic evidence. Sending/uncertain rows are never cancelled while ambiguous; after a real failed/sent result or explicit uncertain resolution, cancellation is retried safely.
- The source-current guard runs before acquisition, queueing, materialization, dispatch selection and after REST rate waits immediately before `client.Do`. Real gateway dispatch additionally fetches the exact source before sending. Discord offers no atomic conditional message POST, so a final remote edit/delete-to-POST race still exists; a POST that crossed the boundary remains subject to ordinary uncertain-send handling and is never replayed automatically.
- `source_not_current` is a dedicated proof of zero POST attempts. If an unchanged refresh and that failed result race, either completion order safely restores the same pending chunk. Generic transport failures and uncertain outcomes are not restored this way.
- A replacement active revision reuses its active-capacity slot. Editing a historical/ignored source at 1000 active items defers with `source_queue_full` and a bounded retry, never admits item 1001.
- Source backfill is a one-time transactional migration with `message_sources_migration_v1`. Concurrent openers observe all-or-nothing results; subsequent CLI opens do not scan/unmarshal message history.

## Bounded context contract

`Envelope.Context` is immutable canonical JSON, comparable like `MediaSnapshot`. Up to two one-hop `ContextMessage` values carry relation, bounded status/provenance enums, exact message/channel IDs, author ID when present, up to 2000 UTF-16 units of untrusted text, bounded media metadata and truncation flag. No signed attachment URL is persisted.

- A normal quoted reply can use Discord's resolved same-channel referenced message; otherwise it performs one exact same-channel message GET.
- A verified public thread can fetch only its exact starter ID. Only a type21 starter with an explicit reference to the configured parent permits a subsequent exact parent-message GET.
- No sibling/foreign channel, recursive quote, thread history or guild history is fetched. Private/no-parent threads do not pretend a parent context exists.
- Missing/deleted/out-of-scope context is labelled and does not block the user question. Context is `untrusted_external_context`, never an instruction or an independently authored user task.

## Integration

Source lifecycle, media materialization, cancellation and session readiness must retain their independent guards when adding new delivery kinds.

- Preserve root's `gatewayRoutePermissions`, patched DiscordGo and session-resilience code; the candidate has been locally rebased onto them.
- Preserve `Policy.Accepts(... && validEnvelopeMedia(e))`, phase1 cancellation and chunking. No replacement of those files is needed from the initial pre-rebase candidate.
- When control/interaction fields are merged, extend the single `isMessageSource` hook to `e.Platform == "discord" && e.Control == "" && e.ReplyKind != "interaction"`. Control rows are not Discord message sources and must never cause a message GET. All registration/current-check/pre-send paths use this hook.
- For outbound manifests, apply the same sourceCurrentDB guard before QueueReplyManifest idempotent lookup; source snapshots remain immutable automatically because revisions have separate inbound IDs.
- Ordinary media metadata is included by `projectGatewayMessage`; refresh and ingress must share this projection. ClaimedEnvelope includes the transaction-local source-current guard before CDN materialization.

## Verification

Focused fixtures cover claim invalidation, exact partial fetch, edit/delete restart, create replay, immutable old source, sending/uncertain boundaries, explicit resolution, generation ordering, identical/cosmetic updates, both unchanged/result completion orders, rate-wait final guard, 404 versus403, FIFO quarantine deletion, wrong explicit guild, capacity reuse/defer, one-time migration with 2001 rows and poison-history reopen, concurrent openers, rollback marker, materialization invalidation, rejected sibling cleanup, bounded quote/media context, missing context availability, exact starter parent scope, no private/history/recursive fetch and forged context labels.

Final rebased `go test -race ./...` passed: CLI 5.650s; bridge 56.074s. `go vet ./...` and `go build ./cmd/dot-gateway` also passed with `-buildvcs=false` in the source-only candidate. Independent review copied the frozen tree and separately passed full race, vet, build and both reviewer-found regression fixtures.

## Future incremental memory hooks (documentation only)

A later memory layer should use a transactional outbox or equivalent idempotent change records at these existing lifecycle points. This candidate does not implement conversation-memory indexing or curated memory.

- `promoteValidation`: index a validated owner message only after promotion, using actual Envelope.EventID + SourceRevision + inbound ID. Quarantined/rejected inputs never become searchable user memory.
- `InvalidateSourceUpdate`: immediately mark the current indexed revision temporarily stale while an exact refresh is pending. `ApplySourceRefresh` unchanged can restore it; a changed revision retires the old index and the replacement is indexed only after its own validated promotion.
- `retireSourceDB` / `DeleteSource`: withdraw stale/deleted source retrieval in the same logical generation. Persist the tombstone/provenance so restart or old create replay cannot resurrect deleted evidence.
- `recordResult`: index assistant text as delivered only from confirmed sent chunks, preserving real remote IDs. QueueReply is authored-but-unsent content, not a delivered assistant memory. Entire replies can be labelled complete only after all required chunks are sent; a partially sent cancelled reply must remain explicitly partial.
- `ResolveSent`: an operator-verified uncertain chunk may join the confirmed-sent index after resolution, keyed idempotently by reply ID + chunk index + remote ID. Unknown/uncertain chunks never become asserted delivered history.
- Use session scope `(platform, guild_id, conversation_id)` so threads remain distinct. The SQL event_id revision key is never the external source identity; index the envelope's actual event ID and revision.
- Key index/outbox changes by lifecycle event and revision, not repeated next/claim calls. Consumer claim recovery must not duplicate memories. Keep bounded curated user notes separate from verbatim session search; context metadata remains untrusted external evidence with provenance.
- Incremental indexing must not reintroduce full envelope/history scans on every CLI open. The migration marker pattern and 2001-row reopen regression demonstrate the startup requirement. Korean two-character queries need a compatible token/bigram strategy rather than a trigram-only index with a three-character minimum.
