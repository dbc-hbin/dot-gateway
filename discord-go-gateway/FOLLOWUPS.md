# Same-conversation task follow-ups

```sh
dot-gateway followup INBOUND_ID --claim ORIGINAL_CLAIM --key STABLE_KEY --text-file FILE
```

Send an authorized task result after its initial answer was confirmed delivered.
Keep the original inbound ID and claim for the task. Text may also come from
stdin. Keys use 1–80 ASCII letters, digits, underscores or hyphens; reuse the same
key and identical content after uncertain CLI output, never a fresh key as a retry.

## Files, images and embeds

Follow-ups accept the same version-1 manifest as initial rich replies:

```sh
dot-bridge reply-output-dir
# Save the approved result into the returned private outbox with mode 0600.
dot-bridge followup INBOUND_ID --claim ORIGINAL_CLAIM --key completed-report --manifest-file result.json
dot-bridge delivery REPLY_ID
```

```json
{
  "version": 1,
  "text": "The completed chart is attached",
  "attachments": [{"path": "chart.png", "description": "Requested chart"}],
  "embeds": [{"title": "Result", "image": {"url": "attachment://chart.png"}}]
}
```

`--manifest-file -` reads JSON from stdin. `--text-file` and `--manifest-file`
are mutually exclusive. Attachment-only and embed-only follow-ups are supported.
Files and embeds accompany only the first chunk of that follow-up. They reuse
all [rich output](RICH_OUTPUT.md) bounds, private outbox rules, MIME checks,
immutable SHA-256 spool, multipart sender, mention suppression and receipt checks.
No arbitrary paths, URL downloads or new destinations are allowed.

The command appends chunks to the original reply, leaving original text, chunks
and receipts unchanged. Delivery returns the original reply ID and all chunks.
A key/content conflict fails; content includes file bytes, filenames, descriptions
and embeds. Equivalent JSON formatting and omitted default filenames do not
create new output. Keep source artifacts unchanged for same-key manifest replay;
if they have been removed, inspect `delivery` using the original reply ID instead
of recreating the task with a new key. Once queued, changing an outbox file cannot
change transmitted bytes.

A new follow-up requires every earlier chunk to be confirmed sent; sending,
failed, uncertain and cancelled work blocks it. Same-key replay only reports
existing state and never resends. Interrupted rich delivery remains uncertain;
`resolve-sent` cannot bypass its attachment receipt. Use operator-only
`reconcile-reply REPLY_ID --chunk INDEX --message-id MESSAGE_ID --verified-in-discord`
for full GET-only verification. `verify-reply` also checks rich follow-up chunks.
Cleanup protects every pending, sending, failed, uncertain or cancelled-unsent
follow-up attachment, even when the original reply was already sent.

The retained claim must match an ordinary, still-current authorized owner source
in replied state. Source revocation and cancellation still apply, including
same-key replay, before artifacts are read. No destination argument, forged
inbound, interaction/control event or fence reset is supported. Caller
authorization remains required; a claim alone does not interpret consent.
The existing dispatcher's source, route, rate, nonce and uncertainty checks apply.

## Deployment and recovery

Rich follow-ups require the upgraded dispatcher and CLI together. Deploy/restart
the dispatcher through the normal separately authorized coordinated process
before queueing rich follow-ups. An older text-only dispatcher cannot read their
per-chunk output and may send only the caption or reject an empty caption. A CLI
upgrade alone is sufficient only for text-only follow-ups with the existing
compatible dispatcher. No live process is restarted by these commands.

The additive `reply_followup_outputs` table binds structured output to the first
appended chunk; the original `reply_outputs` and `reply_followups` rows keep their
meaning. Receipt readback, reaction context and explicit message-operation
verification include the follow-up's structured output and attachment identity.

The source manifest authenticates source files only, not built binaries.
The existing content-free recovery tool does not export the key map or rich
follow-up output. Recovered inbound claims are not continuation authority; do not
recreate old follow-ups using new keys after restore.

This is an original Go adaptation of the document/image delivery behavior
reviewed in Hermes `adapter_media.py` at commit
`10c6188de188871f64a88dd95bc6b262adb0c307`. It reuses the gateway's existing safe
manifest pipeline rather than copying Hermes' arbitrary-path transport.
Hermes is MIT-licensed; see [third-party notices](THIRD_PARTY_NOTICES.md).
