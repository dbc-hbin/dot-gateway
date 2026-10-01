# Verified child-thread routing

The existing configured guild text channel remains the sole conversation parent.
Only messages authored by the configured owner are eligible, with the existing
mention/all-mode rule. Public (`11`) and private (`12`) thread IDs must be verified
as direct children of that exact parent in that exact guild. Announcement threads
(`10`) are recognized but are not compatible with this pinned GUILD_TEXT parent.
Forum/media channels, sibling channels, report-only channels and threads beneath
other parents never become consumable routes. No broader role permissions or
Discord settings are changed.

## Admission and immutable routing

Unknown owner messages in the configured guild are staged as
`guild_thread_candidate` in the durable quarantine before network reads. The
candidate cannot be claimed, replied to, or shown lifecycle feedback. A fresh
thread GET, parent GET, bot-member GET and role GET establish the binding and
permissions. A final thread GET follows potentially slow permission reads.
Promotion preserves event/channel/sender/content/receipt identity while adding
verified parent and thread type. The thread title is included as `thread_name`
for conversation context; it remains untrusted content, never instructions.
Caller-supplied `guild_thread` envelopes are downgraded to candidates even through
`Ingest`, so a claimed parent/type cannot bypass REST validation.

The validated thread ID, not its parent ID, remains the conversation, reply URL,
message-reference channel, feedback target and acknowledgement binding. Every
send, typing operation and reaction operation refreshes the actual thread and
parent. Reconnect epochs and the durable sender still prevent uncertain replay.
Ordinary parent-channel and owner-DM envelopes remain compatible.

## Permission and lifecycle rules

Thread permission calculation uses fresh parent overwrites plus the authenticated
bot's roles. It requires VIEW_CHANNEL, READ_MESSAGE_HISTORY and
SEND_MESSAGES_IN_THREADS; ordinary SEND_MESSAGES does not substitute for the
thread permission. Add-reaction also requires ADD_REACTIONS. Explicit
Administrator handling avoids the older DiscordGo PermissionAll mask's omission
of the thread-send bit. Guild-route readiness accepts either valid parent-send or
thread-send capability, then route-specific guards keep a thread-only bot from
claiming or posting ordinary parent-channel work. Owner DMs have an independent
identity-ready gate: missing/unavailable guild state, revoked guild send rights,
and a forbidden/deleted parent do not stop an otherwise valid owner DM. Guild
traffic remains blocked until the exact parent is validated again. See
[session resilience](SESSION_RESILIENCE.md).

Missing/null required arrays or types, malformed permission bits, unknown assigned
roles, incomplete thread metadata and mismatched bot identity fail closed. Active
member timeouts are respected. Private threads additionally require current bot
membership from GET Channel, including matching thread and bot IDs, even for an
administrator. No join, invite, permission-change or archive-edit endpoint exists
in this path. Active locked threads require MANAGE_THREADS. Observed archived
threads are never written to.

Discord message POST can automatically unarchive a thread. Validation therefore
rechecks thread state after slow identity/permission reads and after rate-budget
waits. These checks narrow the race, but Discord provides no atomic conditional
"send only if still active" operation: an archive occurring between the final GET
and POST cannot be eliminated by this client. This is not an absolute guarantee
against implicit unarchive during that external race.

Transient failures and reversible local thread conditions use bounded read-only
retry (2/4/8/16/30 seconds); they remain hidden until validated. HTTP 401/403/404
retain the existing blocked-until-validated-reconnect behavior; 401 remains fatal.
A proven wrong type/parent/guild creates a bounded content-free rejected tombstone,
not a capacity-consuming blocked message. At most 1,000 such dedup tombstones are
retained, outside the shared 1,000 active quarantine/inbound capacity.

## Point recovery of previously dropped messages

After deploying the new gateway, an operator can identify a specific original
thread/message pair and run the binary with the existing secure live environment:

```sh
bin/dot-gateway recover-thread-message THREAD_ID MESSAGE_ID
```

This authenticated operation retrieves exactly one real Discord message after
fresh identity/thread/parent/permission checks, verifies its IDs/owner/type, and
stages it through normal quarantine. It does not author or send a reply, scan
history, fabricate user content, join a thread, or change archive state. The
credential-free `dot-bridge` consumer wrapper intentionally does not expose this
live-only command. Run once per explicitly identified original message in the
original chronological order; duplicate IDs are harmless. The gateway validates
again before promotion. No schema deletion or live ledger reset is needed.

A daemon restart does not restart the assistant-side reasoning consumer. Verify
that consumer separately and inspect delivery rather than equating gateway uptime
with successful response generation.

## Verification

Offline local-transport tests cover owner/mention/type filtering, public/private
membership, wrong parent/guild/report-like channels, malformed metadata, effective
permissions and admin inheritance, archive/lock handling, rate-wait and slow-GET
archive changes, no feedback/claim before validation, tombstone capacity, duplicate
and restart behavior, immutable send/ack routing, and exact-message recovery.
Tests use disposable ledgers and fake transports, without live tokens or Discord.

Official references (checked 2026-10-01):
- https://docs.discord.com/developers/topics/threads
- https://docs.discord.com/developers/resources/channel
- https://docs.discord.com/developers/topics/permissions
