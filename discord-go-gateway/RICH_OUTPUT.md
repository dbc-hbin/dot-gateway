# Authored files, images, audio files, and rich embeds

This is an opt-in outgoing delivery contract. The current assistant still writes
all substantive replies. There is no new model client, transcription service,
speech synthesis, arbitrary file reader, voice-channel transport, or video call.

## Consumer commands

Ordinary text remains unchanged:

```sh
bin/dot-bridge reply INBOUND_ID --claim CLAIM --text-file answer.txt
```

For files, first create/find the private outbox. Deliberately save only the
approved, user-requested output artifacts there. A returned path is local to the
consumer/gateway filesystem, not an upload of any arbitrary path on a computer.

```sh
bin/dot-bridge reply-output-dir
# JSON: {"directory":".../reply-outbox"}
# Write the approved artifact report.txt into that directory, with mode 0600.
bin/dot-bridge reply INBOUND_ID --claim CLAIM --manifest-file reply.json
bin/dot-bridge delivery REPLY_ID
```

`reply.json`:

```json
{
  "version": 1,
  "text": "The report is attached",
  "attachments": [
    {"path": "report.txt", "filename": "report.txt", "description": "Requested report"}
  ],
  "embeds": [
    {"title": "Report", "description": "Summary authored by the current assistant"}
  ]
}
```

The manifest can also be supplied on stdin with `--manifest-file -`.
`--text-file` and `--manifest-file` are mutually exclusive. Attachment-only and
embed-only replies are allowed; rich output accompanies only the first durable
text chunk. An attachment may be an image, audio file, document, or other regular
file. Its MIME type is detected from bytes; an extension does not override it.
An ordinary audio attachment is not a native Discord voice-message bubble and
does not imply that the assistant can listen to or transcribe speech.

To display an attached image inside a rich embed, select the same filename:

```json
{
  "version": 1,
  "attachments": [{"path": "chart.png", "description": "Requested chart"}],
  "embeds": [{"title": "Chart", "image": {"url": "attachment://chart.png"}}]
}
```

Allowed embed fields are title, description, HTTPS link URL, color, fields,
footer text, author name/HTTPS URL, timestamp, and attachment-backed image or
thumbnail. External image/icon fetching, video/provider HTML, arbitrary flags,
components, destinations, message references, mentions, and credentials cannot
be specified by the manifest. Ordinary replies keep link previews suppressed.
An explicit rich embed lifts suppression for that entire Discord message, so
links in the authored text can also be eligible for Discord previews. Mention
parsing remains disabled in both modes.

## Bounds and security

- Manifest: 64 KiB UTF-8 JSON, version 1, no unknown/duplicate keys, no excessive
  nesting; text up to 16,000 UTF-16 units, split into existing 1,900-unit chunks
- Attachments: at most 10; nonempty, at most 8 MiB each and 16 MiB total
- Rich embeds: at most 10, 6,000 combined text units; Discord's individual
  title/description/field/author/footer limits are also checked
- Attachment `path`: a simple visible basename in the private outbox. Absolute
  paths, subdirectories, traversal, hidden names, symlinked ancestors/files,
  hardlinks, devices, FIFOs, directories, foreign ownership, and group/world
  writable files are rejected. Existing credentials/configuration outside the
  outbox cannot be selected by their original paths
- Staging: database-specific `BRIDGE_DB.reply/` root with private owner-owned `reply-spool`, mode-0400 SHA-256 addressed blobs,
  fsync and atomic no-replace publication before queue commit, bounded at 256
  files and 128 MiB. Interrupted unpublished staging temps are cleaned under
  the spool lock before later staging. The database stores
  filenames, sizes, detected MIME, descriptions and hashes, not arbitrary source
  paths. Original outbox edits after queueing cannot change transmitted bytes
- Sending: reopens only the digest-selected private blob, verifies ownership,
  regular-file status, link count, size, MIME and hash, and snapshots a bounded
  multipart body before attempting the request. Metadata tampering or missing
  bytes fails before any POST

Directory and source permissions are checked with no-follow file descriptors,
not merely a string prefix or `realpath` followed by a separate vulnerable open.
This protects against incidental path substitution, not a hostile process with
the same OS identity that can rewrite the gateway's own code or database.

The outbox itself is deliberately managed by the consumer. Do not copy private
configuration, authentication material, unrelated user files, or untrusted
attachment-selected paths into it. Files are not inferred from message text.

## Receipts, uncertainty, restart, and cleanup

The existing source binding, source revision gates after integration, nonce,
`enforce_nonce`, exact channel/reference, empty allowed mentions, and one-attempt
HTTP behavior remain in force. Multipart bodies have no replayable `GetBody`.
Redirects, 5xx responses, malformed/lost ACKs, and interrupted `sending` rows do
not authorize a resend. `retry-failed` never requeues `uncertain` output.

A successful ACK must match bot, channel, source message, nonce when present,
text, attachment count, distinct returned attachment IDs, filename, size, MIME,
description, and all authored rich embed fields. Both ordinary and interaction
ACKs require an explicit string content field and permit only the existing
single-terminal-LF removal after a nonspace rune; no broad trimming is allowed. Attachment-backed image
references must bind to the returned channel/attachment identity. Expiring CDN
URL equality is not required. Remote bytes are not claimed to be hash-verified:
the digest proves local staged/uploaded bytes, while Discord's receipt proves
returned attachment identity and metadata.

Validated attachment identities are committed atomically alongside the chunk's
`sent` result, and appear in `delivery` as `output_receipt`. A receipt persistence
failure rolls back the sent transition; restart leaves it uncertain. An
operator with the already configured scoped credentials can read back a known
sent message without changing the ledger:

```sh
bin/dot-gateway verify-reply REPLY_ID --chunk 0
```

This authenticated GET command is intentionally excluded from the
credential-free `dot-bridge` wrapper. It checks the exact recorded message and
attachment IDs again and never silently resolves or retries uncertain output.
For a lost ACK, visually identify the exact Discord message, then use the
operator-only reconciliation command. It issues only GETs, validates the entire
output again, and commits the receipt plus uncertain-to-sent transition together:

```sh
bin/dot-gateway reconcile-reply REPLY_ID --chunk 0 --message-id MESSAGE_ID --verified-in-discord
```

`resolve-sent` refuses structured output because a message ID alone omits the
attachment receipt. `reconcile-reply` is also excluded from the credential-free
wrapper and does not post, resend, or weaken uncertainty automatically.

An interaction response needs its interaction-token transport for verification;
the ordinary message readback path is not an interaction-token fallback.

The credential-free cleanup command deletes only unreferenced or fully-sent
spool blobs older than seven days:

```sh
bin/dot-bridge prune-reply-spool
```

All pending, sending, failed, uncertain, and cancelled-unsent blobs are protected.
Quota exhaustion is explicit and fail-closed; uncertain files are never removed
merely to make room. Outbox source artifacts are never removed by cleanup.

## Activation and validation

Deploy a reviewed binary and its matching wrapper together using the existing
coordinated deployment process. No Gateway intents, OAuth grants, bot
permissions, external model API, new service, or public listener is needed by
this outgoing implementation. Existing channel permissions and Discord upload
limits still apply. Do not restart or deploy the live gateway during offline
work.

Offline coverage uses temporary stores/outboxes and local fake HTTP transports:
real CLI manifest input, multipart bytes, immutable source mutation, image /
audio / file-only output, bounded embeds, invalid paths/ownership/link/device
cases, quota cleanup, integrity corruption, ACK mismatches, lost ACK, redirects,
429/5xx, restart uncertainty, idempotence, exact attachment readback and the
credential-free wrapper allowlist. Run full race tests, vet and both ordinary
and `CGO_ENABLED=0` builds before integration. These checks do not claim live
Discord end-to-end delivery; a separately authorized scoped live test remains a
deployment gate.
