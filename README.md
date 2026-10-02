# Assistant source recovery

Source-only backup of the native Go public-source search/collector, guarded headed Playwright browser adapter, bounded Discord report publisher, and Go Discord conversational gateway.

This repository deliberately contains no credentials, real messages, raw community captures, browser profiles/cookies, SQLite data, private delivery/review ledgers, active schedule configuration, or installed executables. The separate private recovery bundle holds the non-message delivery metadata needed for duplicate-send prevention.

Start with [RECOVERY.md](RECOVERY.md) and the [VM reset runbook](recovery/VM_RESET_RUNBOOK.ko.md). Live Discord IDs are replaced by documented numeric examples and operational paths are generalized. The source builds and offline tests run without live credentials. Restoring an existing separately held token file is an explicit, current-authorization operator step; source cannot recreate lost credentials or message history.

- `insane-search-migration/`: Go collector/search/extraction, report publisher, browser client, Playwright layer, synthetic fixtures and pinned dependencies
- `discord-go-gateway/`: Go gateway/CLI, durable-state and transport safeguards, offline tests and consumer wrappers
- `host-support/`: Python host-lifetime supervisor/launcher, idempotent recovery controller, credential-free consumer adapter, opaque operator token-file installer and reviewed source-overlay tool; no boot registration
- `SANITIZATION.json`: transformations applied for source-only storage
- `SOURCE_MANIFEST.json`: SHA-256 file inventory for this sanitized source tree

The original search reference code and its MIT license remain under `insane-search-migration/vendor/insane-search/`. Gateway dependency provenance is documented in `discord-go-gateway/THIRD_PARTY_NOTICES.md`. Third-party terms remain applicable; this backup does not invent a new project-wide license.

See [THIRD_PARTY.md](THIRD_PARTY.md) and `third-party-notices/INDEX.json` for retained attribution, license texts, and source-only redistribution scope.
