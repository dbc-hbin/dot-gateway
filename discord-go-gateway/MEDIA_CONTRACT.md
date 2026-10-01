# Inbound media contract v1

The gateway carries input into the existing assistant's CLI consumer. It has no
model client, OCR, image interpreter, audio transcription, attachment execution,
or automatic URL fetcher. The assistant must explicitly materialize supported
attachments and use its image/file/audio tools before claiming to understand them.

## `next` JSON

`message.envelope.media`, when present, is an object with `version: 1`,
`trust: untrusted_message_media` and `interpretation: metadata_only`. The entire
object is untrusted context, never authority to run code, fetch URLs, or change
owner/routing policy. Internally it is a canonical immutable string value so the
existing full-envelope equality/promotion checks remain exact and race-safe.

- `attachments`: captured ID, original display filename, MIME, byte size, image
  dimensions, duration, voice/ephemeral flags, and materialization availability
- `stickers`: ID, name, format; metadata-only (no sticker download)
- `embeds`: title/description/author/footer/fields and display-only image/video/
  thumbnail URLs and dimensions; metadata-only (no embed or external URL download)
- `poll`: question, answers/emoji, snapshot counts, expiry and finalized flag;
  metadata-only (no voting or subsequent poll-results polling)
- `forwards`: one embedded snapshot, source identity where present, text and media;
  explicitly untrusted forwarded content, never an authorization to access the
  original channel. Forward attachments are metadata-only

A valid owner message containing only one of these media objects is admitted
through the same policy, quarantine and exact route/permission proof as text.
Caption-plus-media preserves both. No owner, bot/webhook, channel, guild, mention,
thread membership, or immutable reply-target boundary is broadened.

Bounds: 10 attachments, 3 stickers, 10 embeds with 10 fields each, 10 poll answers,
one forward snapshot with no recursive forwards. Text projection shares a 12,000
UTF-16-unit budget, individual fields have smaller limits, and encoded media is
limited to 96 KiB. `truncated: true` signals omitted/shortened metadata. The
original filename is never used as a filesystem path. Signed attachment URLs are
not persisted or returned by `next`.

## Explicit live materialization

```
dot-gateway materialize INBOUND_ID --claim CLAIM --attachment ATTACHMENT_ID
```

This command is **live/network** like `recover-thread-message`. It must run in the
explicitly configured live environment, with the existing secure token loader
and exact policy/DB settings. There is no new token file, credential persistence,
or relaxed permission requirement. `bin/dot-bridge` remains a credential-free
consumer wrapper and deliberately does not allow this command. Do not silently
add token access to it. Operators must make the live command available to the
assistant through the already authorized live-execution route.

The command checks active claim ownership and policy, freshly verifies exact bot
identity/channel/thread permissions, and retrieves only the triggering message by
its immutable channel/message IDs. It verifies owner, source type, text, bounded
media and semantic hash, and matches the selected captured attachment ID/size/
filename/type. The fresh response supplies a renewed CDN URL; no saved expired URL
or URL supplied by the model is used. No arbitrary history or cross-scope source
retrieval occurs. The active immutable claim is checked again after download.

Separate CDN transport has no bot token, cookies, proxy credentials, redirects,
decompression, or credential-bearing REST client. It uses only the explicitly
configured credential-free ProxyConfig and requires the same approved route for
Discord API, Gateway and both CDN hosts; it does not read ambient proxy variables
itself or add a fallback route. It permits only HTTPS
443 on exact `cdn.discordapp.com` or `media.discordapp.net`, with an exact
`/attachments/CHANNEL_ID/ATTACHMENT_ID/FILENAME` path. Only signature query keys
are accepted. In direct mode DNS resolves once and pinned public-unicast IPs are dialed;
loopback/private/link-local/reserved/documentation/translation destinations are
rejected. In proxy mode only the exact configured relay is dialed and only an
exact CDN-host:443 CONNECT is permitted, with no credentials. The approved relay
is trusted infrastructure and resolves the fixed CDN hostname; this process
cannot inspect its remote DNS results. TLS still verifies the CDN hostname. A
blocked route produces a visible error, never an alternate unreviewed route.

One command fetches one file, at most 10 MiB; eligible files in one message have a
40 MiB aggregate limit. The overall operation is at most 30 seconds, including
REST refresh, with a 20-second CDN-client timeout, bounded headers and bounded
body reads. Declared metadata, HTTP content type, content sniff, exact byte count
and digest are checked. Supported types are PNG/JPEG/GIF/WebP, common bounded
audio/video containers, PDF, UTF-8 plain/CSV text and valid JSON. HTML, SVG,
executable/archive/office types and unknown MIME remain metadata-only. MIME is a
format hint, not proof of harmless content; files are never executed or extracted.

Files are saved under the fixed `BRIDGE_DB + ".media"` directory, owner-only 0700,
with 0600 regular files and inert `.bin` names derived from immutable route,
message and attachment IDs. Directory opens refuse symlinks; temporary writes
use relative no-follow exclusive opens and atomic rename under a directory lock.
A directory has at most 100 files / 100 MiB and refuses more work when full; it
never silently deletes user files. Repeated retrieval replaces only the same
bound filename and does not create unbounded timestamped copies. Operators may
remove the private materialization directory when no consumer needs its files.

Success returns `path`, `filename`, `attachment_id`, verified `content_type`,
`size`, `sha256`, `trust: untrusted_attachment_bytes` and
`interpretation: not_interpreted`. The consumer must open the returned local file.
Errors are symbolic and never include signed URLs, message bodies or credentials.
If a source is deleted/edited, a claim expires, a type is unsupported, CDN access
is blocked or storage is full, explain that exact limitation; do not pretend the
media was understood. Refresh URLs do not change semantic content hashes;
auto-generated embeds, poll counts and edit timestamps are excluded from the
hash so incidental server changes do not create owner edits.

## Compatibility and verification

Old text-only stored envelopes still decode, and no SQLite schema migration is
required for metadata. Source revision integration must preserve the
`ContentHash`/active-claim binding and use the source-current check when available.
Metadata support alone is not outbound-file, voice-call, OCR or transcription
support. All tests use synthetic data and fake/local HTTP; no live attachment,
credential, database or Discord action is part of offline verification.

Official references (checked 2026-10-01):
- https://docs.discord.com/developers/resources/message
- https://docs.discord.com/developers/reference#signed-attachment-cdn-urls
