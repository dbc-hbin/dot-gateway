#!/usr/bin/env python3
"""Offline, idempotent source-only upgrade of a private configured recovery tree.

Verify all candidates before any writes. A failed/interrupted apply can be
repeated with the same reviewed manifest. Never point at an active deployment.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import sys
from recovery_files import directory, read_bytes, read_json, unique_object, write_bytes, write_json


def digest(data):
    return hashlib.sha256(data).hexdigest()


def apply(source, overlay, pin, mutate=False):
    source, overlay = Path(source), Path(overlay)
    raw = read_bytes(overlay / 'OVERLAY_MANIFEST.json', limit=1 << 20)
    if not re.fullmatch('[a-f0-9]{64}', pin) or digest(raw) != pin:
        raise ValueError('trusted_overlay_manifest_mismatch')
    manifest = json.loads(raw, object_pairs_hook=unique_object)
    if (set(manifest) != {'schema', 'base_source_manifest_sha256', 'files'} or
            manifest['schema'] != 1 or not isinstance(manifest['files'], list) or
            not 1 <= len(manifest['files']) <= 10000):
        raise ValueError('invalid_overlay_manifest')
    base = read_json(source / 'PRIVATE_DERIVATION.json')
    if base['base_manifest_sha256'] != manifest['base_source_manifest_sha256']:
        raise ValueError('overlay_base_provenance_mismatch')
    seen, changes = set(), []
    for entry in manifest['files']:
        if set(entry) != {'path', 'sha256', 'configured_base_sha256'}:
            raise ValueError('invalid_overlay_entry')
        relative = PurePosixPath(entry['path'])
        if (relative.is_absolute() or str(relative) != entry['path'] or
                any(c in entry['path'] for c in '\\\x00\r\n') or
                any(x in ('..', '.', 'secrets', 'input', '.git', 'bin') for x in relative.parts) or
                relative.suffix not in ('.go', '.py', '.sh', '.md', '.json', '.cjs', '.mod', '.sum') or
                relative.name in ('SOURCE_MANIFEST.json', 'PRIVATE_DERIVATION.json') or
                str(relative) in seen or not re.fullmatch('[a-f0-9]{64}', entry['sha256']) or
                entry['configured_base_sha256'] is not None and not re.fullmatch('[a-f0-9]{64}', entry['configured_base_sha256'])):
            raise ValueError('unsafe_overlay_entry')
        seen.add(str(relative))
        candidate = read_bytes(overlay / relative, limit=64 << 20)
        if digest(candidate) != entry['sha256']:
            raise ValueError('overlay_content_mismatch')
        destination = source / relative
        with directory(destination.parent):
            pass
        try:
            current = read_bytes(destination, limit=64 << 20)
        except FileNotFoundError:
            current = None
        if current == candidate:
            continue
        old = entry['configured_base_sha256']
        if (current is None and old is not None or
                current is not None and (old is None or digest(current) != old)):
            raise ValueError('unreviewed_destination_change')
        changes.append((destination, candidate, current is not None))
    if mutate:
        for destination, candidate, exists in changes:
            write_bytes(destination, candidate, replace=exists)
        write_json(source / 'UPGRADE_APPLIED.json', {
            'schema': 1, 'base_source_manifest_sha256': manifest['base_source_manifest_sha256'],
            'overlay_manifest_sha256': pin, 'files': manifest['files'],
            'binaries_rebuilt': False, 'activation_authorized': False})
    return {'verified': True, 'applied': mutate, 'changed_files': len(changes),
            'services_started': False, 'requires_rebuild_and_fresh_activation': True}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', required=True)
    parser.add_argument('--overlay', required=True)
    parser.add_argument('--manifest-sha', required=True)
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    os.umask(0o077)
    try:
        print(json.dumps(apply(args.source, args.overlay, args.manifest_sha, args.apply)))
        return 0
    except Exception:
        print(json.dumps({'error': 'source_overlay_failed', 'activation_authorized': False}))
        return 1


if __name__ == '__main__':
    sys.exit(main())
