# Same-conversation task follow-ups

`dot-gateway followup INBOUND_ID --claim ORIGINAL_CLAIM --key STABLE_KEY --text-file FILE`

Send an authorized task result after its initial answer was confirmed delivered.
Keep the original inbound ID and claim for the task. Text may also come from
stdin. Keys use 1–80 ASCII letters, digits, underscores or hyphens; reuse the same
key and identical text after uncertain CLI output, never a fresh key as a retry.

This text-only command appends chunks to the original reply, leaving original
text, chunks and receipts unchanged. Delivery returns the original reply ID and
all chunks. A key/content conflict fails. A new follow-up requires every earlier
chunk to be confirmed sent; sending, failed, uncertain and cancelled work blocks
it. Same-key replay only reports existing state and never resends.

The retained claim must match an ordinary, still-current authorized owner source
in replied state. Source revocation and cancellation still apply. No destination
argument, forged inbound, interaction/control event or fence reset is supported.
Caller authorization remains required; a claim alone does not interpret consent.
The existing dispatcher's source, route, rate, nonce and uncertainty checks apply.

The additive reply_followups table is created by the new CLI. The running
same-version dispatcher can consume its ordinary appended chunks without restart.
Build the updated CLI before using followup commands.

The source manifest authenticates source files only, not built binaries.
The existing content-free recovery tool does not
export the new key map. Recovered inbound claims are not continuation authority;
do not recreate old follow-ups using new keys after restore.
