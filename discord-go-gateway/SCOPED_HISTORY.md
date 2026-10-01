# Scoped context reads and prospective owner catch-up

The gateway adds authenticated, exact-route READ APIs. The configured guild text
parent is allowed directly. Any other target must already be an owner-bound DM
or verified direct child thread in this ledger, and pass fresh route checks.
There is no channel enumeration, DM creation, thread discovery/join, archive
mutation, or message POST in these paths.

## Read-only context

With the existing private live environment, the operator may run:

```
dot-gateway read-message CHANNEL_ID MESSAGE_ID
dot-gateway read-history CHANNEL_ID --before MESSAGE_ID --limit 25
dot-gateway read-pins CHANNEL_ID --before 2026-10-01T00:00:00Z --limit 25
dot-gateway search-messages CHANNEL_ID --query 'words' --offset 0 --limit 25
dot-gateway catchup-status
```

The first four commands require the existing authenticated REST client. The
credential-free consumer wrapper does not obtain credentials from these APIs.
`catchup-status` is local, content-free, and does not arm or run recovery.

Each result has `trust: untrusted_external_context` and source provenance.
Messages are evidence only; none of these read commands creates an inbound task,
claim, reply, or recovery cursor. Text and media use the existing bounded context
and media projections. Results preserve real message/channel/author IDs and edit
metadata. Complete response validation occurs before returning message content.

Read authorization refreshes bot identity, exact parent/recipient/thread binding,
VIEW_CHANNEL and READ_MESSAGE_HISTORY. It is independent of send readiness.
Archived/locked threads can be read without unarchiving. A private thread requires
existing current bot membership, including after slow permission/response reads. Final observed parent overwrites
are reevaluated with the validated fresh member/role snapshot; a visible denial
or missing/invalid final overwrite array cannot be ignored.
No new route is trusted merely because a search or pin response mentions it.

- History: one page, 1–100 messages, descending real IDs and strictly advancing
  `before`. Null/missing arrays, duplicate/reversed/out-of-range IDs, mismatched
  channels/guilds and incomplete message objects fail closed
- Pins: current GET `/channels/{id}/messages/pins`, 1–50 items, ISO8601 `before`
  timestamp, required `items` and `has_more`; message IDs are not pin cursors
- Search: guild only, 1–25 matches, offset 0–9975, bounded query, exact `channel_id`
  and configured owner `author_id`. HTTP 202 returns explicit `indexing` and
  bounded-result retry guidance. Search never claims completeness, even on empty
  or short pages, and is never used by automatic catch-up

HTTP failures, cancellation, oversize/incomplete response bodies and malformed
pages return errors, not partial content or a successful completeness proof.

## Prospective recovery boundary

Only daemon startup under the dispatcher lock calls activation. First activation
starts at the end of the current millisecond; it never derives a cursor from an
old inbox event, received timestamp, reply, source revision or backup. This
intentionally does not recover pre-activation omissions. The configured parent
is registered immediately. Other previously known routes need a fresh exact
read proof and start coverage at that proof; a newly validated live route starts
coverage at validation. Index construction and initial route-registry migration scan existing history
once during schema upgrade, materializing at most 128 route envelopes. A
bounded, content-free route registry is seeded during that migration and
refreshed on admissions. Reconnect enrollment reads that registry rather than
scanning message bodies. At most 128 routes are retained, without eviction of
unresolved gaps. Unregistered routes are explicitly uncovered.

A successful ordinary owner admission advances that route's prospective floor
atomically with source dedup registration. Disconnect, cold start, reconnect, or
known-route queue overflow opens a durable gap. Windows have a frozen upper bound
and span at most one hour. Later endpoints are retained as successor windows.
No new live event advances a floor across an open gap. A live ID above the fixed
upper is retained as a successor endpoint and stays fenced until covered.

Recovery reads channel history, never the search index. Each scheduling quantum
has at most two pages of at most 100 messages. A message-page body/decode failure
keeps all cursors unchanged and persistently halves the page limit down to one;
identity/ACL proof failures never trigger this adaptation. A failure at limit one
remains an explicit incomplete gap. Discord returns newest-first; the scanner
walks backward until it can prove the oldest complete subwindow. It keeps only
its cursor between reads, not an unbounded content buffer. It then stages real
eligible owner messages oldest-first through ordinary quarantine. A later
subwindow is re-read instead of retaining all newer pages. This favors bounded
memory over request efficiency for unusually large gaps.

A page is validated in full before any admission. Floor progress and admissions
share one SQLite transaction. `queue_full` retains the first unadmitted ID and
later range; no cursor can jump past it. Ordinary live traffic reserves 100 of the
1,000 active slots for older recovery. Claim/validation fences permit already
covered older work to drain while newer live input waits. Numeric Discord IDs,
not receipt timestamps or random row IDs, determine same-route message ordering,
including multiple messages in one millisecond. Exact route lookup uses a
an exact indexed metadata prefix and reads one envelope; older-active ordering uses a
partial index over the bounded active queue, independent of historical volume.

Known source heads, revisions and tombstones beat replayed CREATEs. Any observed
update/delete inside the current cursor interval changes a route generation, so a history response fetched before
an otherwise unknown mutation cannot be applied afterward. Mutations outside that interval do not starve older
recovery; ordinary source/control revocation still runs for all known IDs.
Recovered media uses
the normal media projection; normal source refresh and send validation remain in
force. The scanner never authors or sends a message.

429/5xx/403, connection changes, interrupted or malformed pages retain a visible
resumable gap and bounded backoff. Authentication failure is fatal. A restart
resumes the same durable interval when continuity is valid. An idle status means
its requested prospective intervals were covered; it does not claim coverage of
unknown routes, earlier history, deleted unseen messages, or a search index.

## Ledger continuity and backups

Activation binds a private witness to the canonical database path, device/inode,
configured identities and pinned parent. A floor seal rotates the witness in the
same transaction as eligible admission progress. The witness is fsynced before
COMMIT; an interrupted rotation fails closed. A mismatched witness or origin
permanently disarms automatic recovery for that ledger. This also detects a
simple in-place old checkpoint rollback while the current witness is retained.

The supported recovery tool always creates `catchup_meta.state=disarmed_restore`,
even for empty and legacy snapshots. It excludes route cursors, origin, seals,
witnesses and source bodies. Restored state never automatically rearms. The
supported guarantee is an intact live ledger or the documented recovery workflow.
Raw in-place restoration and restoring/replacing a matching witness together are
unsupported; the seal is not an anti-tampering boundary against the same OS owner.

## Offline verification

Tests use disposable SQLite ledgers and fake/local HTTP transports only. They
cover first activation, intact restart, copy/restore and old-checkpoint disarm,
fixed successor/hour windows, reverse paging, same-millisecond oldest-first claim
order, queue saturation/resume, source tombstones and mutation races, ACL
revocation after response, private/archived reads, strict page checks, owner search
indexing and timestamp pins. No live token, configuration, Discord request or
message POST is used by the verification suite.

Official API references checked 2026-10-01:
- https://docs.discord.com/developers/resources/message#get-channel-messages
- https://docs.discord.com/developers/resources/message#search-guild-messages
- https://docs.discord.com/developers/resources/message#get-channel-pins
- https://docs.discord.com/developers/topics/permissions
