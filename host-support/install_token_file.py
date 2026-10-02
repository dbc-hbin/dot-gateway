#!/usr/bin/env python3
"""Operator-only, opaque local credential installation; never run on import.

An assistant must obtain any required action-time authorization before invoking
--apply. This flag is an execution switch, not evidence of approval. No token is
accepted in arguments, environment variables or JSON. No hashes are produced.
"""
import argparse
import json
import os
from pathlib import Path
import sys
from recovery_files import directory, read_bytes, write_bytes


def install(source, destination, apply=False):
    source, destination = Path(source), Path(destination)
    # Validation-only mode never opens the source credential.
    with directory(source.parent), directory(destination.parent):
        pass
    if not apply:
        return {'applied': False, 'credential_read': False,
                'requires_operator_authorization': True}
    data = read_bytes(source, limit=16384)
    if not data or not data.strip():
        raise ValueError('empty_credential_file')
    try:
        write_bytes(destination, data, replace=False)
    finally:
        # Best-effort lifetime reduction, not a Python memory-erasure guarantee.
        del data
    return {'applied': True, 'credential_output': False, 'existing_files_replaced': False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input-file', required=True)
    parser.add_argument('--destination-file', required=True)
    parser.add_argument('--apply', action='store_true', help='Run only after current authorization')
    args = parser.parse_args()
    os.umask(0o077)
    try:
        print(json.dumps(install(args.input_file, args.destination_file, args.apply)))
        return 0
    except Exception:
        # Never render exceptions that could carry input bytes, paths or contents.
        print(json.dumps({'error': 'credential_install_failed', 'applied_not_assumed': True}))
        return 1


if __name__ == '__main__':
    sys.exit(main())
