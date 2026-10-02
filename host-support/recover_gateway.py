#!/usr/bin/env python3
"""Explicit Linux recovery controller; no boot hook, token parsing or model API.

Use only from a supported native, host-lifetime execution context. A successful
launch is not consumer readiness. All paths are explicit private operator input.
"""
import argparse
import fcntl
import json
import os
from pathlib import Path
import re
import shlex
import signal
import sqlite3
import subprocess
import sys
import time
from recovery_files import (directory, make_private_directory, metadata,
                            private_lock, read_json, write_json)


def identity(pid):
    if not isinstance(pid, int) or pid < 1:
        return None
    try:
        proc = Path('/proc') / str(pid)
        fields = (proc / 'stat').read_text().rsplit(') ', 1)[1].split()
        if fields[0] == 'Z':
            return None
        return {'pid': pid, 'start_ticks': fields[19],
                'boot_id': Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
                'exe': os.readlink(proc / 'exe')}
    except (OSError, ValueError, IndexError):
        return None


def clean_environment():
    env = os.environ.copy()
    for key in list(env):
        if (key.startswith(('DISCORD_', 'BRIDGE_')) or
                key.lower() in ('http_proxy', 'https_proxy', 'all_proxy', 'no_proxy')):
            del env[key]
    return env


def policy_environment(operation, db):
    mapping = {'DISCORD_OWNER_ID': 'owner_id', 'DISCORD_ALLOWED_DM_IDS': 'owner_id',
               'DISCORD_EXPECTED_BOT_ID': 'bot_id', 'DISCORD_GUILD_ID': 'guild_id',
               'DISCORD_GUILD_CHANNEL_ID': 'gateway_channel_id', 'DISCORD_GUILD_MODE': 'guild_mode'}
    env = {key: operation[value] for key, value in mapping.items()}
    env.update(DISCORD_ENABLE_MESSAGE_CONTENT=str(operation['message_content_approved']).lower(),
               BRIDGE_DB=str(db), BRIDGE_KEEP_CATCHUP_DISARMED='true')
    return env


class Controller:
    def __init__(self, source, state_root):
        self.source, self.root = Path(source), Path(state_root)
        with directory(self.source, private=False), directory(self.root):
            pass
        self.db = self.root / 'bridge/bridge.sqlite3'
        self.runtime = self.root / 'daemon'
        self.binary = self.source / 'discord-go-gateway/bin/dot-gateway'

    def lock_held(self):
        with directory(self.db.parent) as parent:
            try:
                fd = os.open(self.db.name + '.lock', os.O_RDWR | os.O_NOFOLLOW |
                             os.O_NONBLOCK | os.O_CLOEXEC, dir_fd=parent)
            except FileNotFoundError:
                return False
            try:
                metadata(os.fstat(fd))
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    return False
                except BlockingIOError:
                    return True
            finally:
                os.close(fd)

    def record(self):
        try:
            return read_json(self.runtime / 'process.json')
        except FileNotFoundError:
            return {}

    def process_state(self):
        record = self.record()
        actual = identity(record.get('identity', {}).get('pid'))
        matches = bool(actual and actual == record.get('identity') and
                       actual['exe'] == str(self.binary))
        held = self.lock_held()
        return {'running': matches and held, 'tracked_process_alive': matches,
                'dispatcher_lock_held': held, 'untracked_dispatcher': held and not matches,
                'identity': actual if matches else None, 'mode': record.get('mode'),
                'end_to_end_ready': False, 'consumer_started_by_launcher': False,
                'boot_autostart': False}

    def status(self):
        process = self.process_state()
        record = self.record()
        # Opening the existing private DB read-only must never initialize a new DB.
        with directory(self.db.parent) as parent:
            metadata(os.stat(self.db.name, dir_fd=parent, follow_symlinks=False))
            for suffix in ('-wal', '-shm'):
                try:
                    metadata(os.stat(self.db.name + suffix, dir_fd=parent,
                                     follow_symlinks=False))
                except FileNotFoundError:
                    pass
            con = sqlite3.connect(self.db.as_uri() + '?mode=ro', uri=True)
            try:
                row = con.execute("select value from runtime where key='gateway'").fetchone()
                gateway = json.loads(row[0]) if row else {}
                row = con.execute("select value from catchup_meta where key='state'").fetchone()
                catchup = row[0] if row else None
            finally:
                con.close()
        age = time.time() - gateway.get('updated_at', 0)
        fresh = bool(process['running'] and gateway.get('state') == 'connected' and -5 <= age <= 15
                     and gateway.get('updated_at', 0) >= record.get('launched_at', float('inf')))
        return {**process,
                'gateway_state': gateway.get('state'), 'heartbeat_age_seconds': round(age, 3),
                'connected_fresh': fresh, 'receive_only': gateway.get('receive_only'),
                'catchup_state': catchup}

    def environment(self, mode):
        provenance = read_json(self.root / 'RECOVERY_STATE.json')
        args = [str(self.source / 'recovery/dot-recovery'), 'activation-env',
                '--state-root', str(self.root), '--source', str(self.source),
                '--snapshot-sha', provenance['snapshot_sha256'],
                '--manifest-sha', provenance['source_manifest_sha256'],
                '--component', 'gateway-receive-only' if mode == 'receive-only' else 'gateway']
        result = subprocess.run(args, capture_output=True, text=True, env=clean_environment(), timeout=30)
        if result.returncode:
            raise ValueError('activation_gate_failed')
        env = clean_environment()
        allowed = {'BRIDGE_HTTPS_PROXY', 'BRIDGE_NO_PROXY', 'https_proxy', 'no_proxy',
                   'BRIDGE_RECEIVE_ONLY', 'BRIDGE_KEEP_CATCHUP_DISARMED'}
        for line in result.stdout.splitlines():
            words = shlex.split(line)
            if words and words[0] == 'unset':
                for key in words[1:]:
                    env.pop(key, None)
            elif len(words) == 2 and words[0] == 'export' and '=' in words[1]:
                key, value = words[1].split('=', 1)
                if key not in allowed:
                    raise ValueError('unexpected_activation_output')
                env[key] = value
            else:
                raise ValueError('unexpected_activation_output')
        env.update(policy_environment(read_json(self.root / 'operation.json'), self.db))
        env['DISCORD_BOT_TOKEN_FILE'] = str(self.root / 'secrets/bot-token')
        env['BRIDGE_RECEIVE_ONLY'] = str(mode == 'receive-only').lower()
        return env

    def consumer_record(self):
        record = read_json(self.root / 'CONSUMER_READY.json')
        expected = {'schema', 'consumer_id', 'boot_id', 'controller_verified',
                    'exclusive_consumer_verified', 'paused'}
        boot = Path('/proc/sys/kernel/random/boot_id').read_text().strip()
        if (set(record) != expected or record['schema'] != 1 or
                not isinstance(record['consumer_id'], str) or
                not re.fullmatch(r'[A-Za-z0-9_.:-]{1,128}', record['consumer_id']) or
                record['boot_id'] != boot or record['controller_verified'] is not True or
                record['exclusive_consumer_verified'] is not True or
                not isinstance(record['paused'], bool)):
            raise ValueError('consumer_readiness_not_verified')
        return record

    def start(self, mode):
        current = self.status()
        if current['tracked_process_alive']:
            if current['mode'] != mode:
                raise ValueError('existing_gateway_different_mode_stop_first')
            return dict(current, already_running=True)
        if current['dispatcher_lock_held']:
            raise ValueError('unknown_dispatcher_lock_held')
        if current['catchup_state'] != 'disarmed_restore':
            raise ValueError('catchup_not_disarmed')
        env = self.environment(mode)
        if mode == 'active':
            if not self.consumer_record()['paused']:
                raise ValueError('consumer_must_be_paused_for_start')
        with directory(self.runtime) as runtime:
            fd = os.open('gateway.log', os.O_WRONLY | os.O_CREAT | os.O_APPEND |
                         os.O_NOFOLLOW | os.O_CLOEXEC, 0o600, dir_fd=runtime)
            try:
                metadata(os.fstat(fd))
                launched_at = time.time()
                child = subprocess.Popen([str(self.binary), 'gateway'], env=env,
                         stdin=subprocess.DEVNULL, stdout=fd, stderr=subprocess.DEVNULL,
                         start_new_session=True, close_fds=True)
            finally:
                os.close(fd)
        until = time.monotonic() + 5
        while time.monotonic() < until:
            actual = identity(child.pid)
            if actual and actual['exe'] == str(self.binary):
                # Record before waiting for the lock/heartbeat. Repeated starts
                # recognize this child even during its connection handshake.
                write_json(self.runtime / 'process.json', {'schema': 1, 'mode': mode,
                           'launched_at': launched_at, 'identity': actual})
                return dict(self.status(), launch_requested=True)
            if child.poll() is not None:
                raise ValueError('gateway_early_exit')
            time.sleep(.05)
        raise ValueError('gateway_identity_unconfirmed_do_not_retry_blindly')

    def stop(self):
        # Safe termination must not depend on healthy SQLite/heartbeat telemetry.
        current = self.process_state()
        if current['untracked_dispatcher']:
            raise ValueError('unknown_dispatcher_lock_held')
        if not current['tracked_process_alive']:
            return dict(current, already_stopped=True)
        actual = current['identity']
        fd = os.pidfd_open(actual['pid'])
        try:
            if identity(actual['pid']) != actual:
                raise ValueError('process_identity_changed')
            signal.pidfd_send_signal(fd, signal.SIGINT)
        finally:
            os.close(fd)
        until = time.monotonic() + 15
        while time.monotonic() < until:
            if identity(actual['pid']) != actual and not self.lock_held():
                return dict(self.process_state(), stopped=True)
            time.sleep(.1)
        raise ValueError('graceful_stop_unconfirmed')

    def consumer(self, args):
        # Only local ledger commands. Network credential-bearing commands and
        # diagnostics are intentionally not part of this recovery adapter.
        allowed = {'status', 'catchup-status', 'check', 'next', 'reply', 'renew',
                   'begin', 'ignore', 'bind-response', 'delivery', 'cancel-reply'}
        if not args or args[0] not in allowed or any(x.startswith('--consumer-id') for x in args):
            raise ValueError('unsupported_consumer_command')
        current = self.status()
        record = self.consumer_record()
        maintenance = args[0] in {'status', 'catchup-status', 'delivery', 'cancel-reply', 'renew'}
        if not maintenance and (record['paused'] or current['mode'] != 'active' or
                not current['connected_fresh'] or current['receive_only'] is not False or
                current['catchup_state'] != 'disarmed_restore'):
            raise ValueError('reply_path_not_activated')
        # Recheck full historical tombstones/binary/activation binding before
        # allowing the consumer to mutate its durable claim/reply ledger.
        self.environment('receive-only' if current['mode'] == 'receive-only' else 'active')
        env = clean_environment()
        env.update(policy_environment(read_json(self.root / 'operation.json'), self.db))
        env['BRIDGE_CONSUMER_ID'] = record['consumer_id']
        with private_lock(self.runtime / 'consumer-command.lock') as fd:
            # The child retains the flock if its Python wrapper is interrupted.
            return subprocess.run([str(self.binary)] + args, env=env, check=False,
                                  pass_fds=(fd,)).returncode


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', required=True)
    parser.add_argument('--state-root', required=True)
    parser.add_argument('--mode', choices=['receive-only', 'active'], default='receive-only')
    parser.add_argument('--wait-connected', type=float, default=0)
    parser.add_argument('action', choices=['status', 'start', 'stop', 'consumer'])
    parser.add_argument('consumer_args', nargs=argparse.REMAINDER)
    args = parser.parse_args()
    os.umask(0o077)
    try:
        if not 0 <= args.wait_connected <= 120:
            raise ValueError('invalid_wait')
        controller = Controller(args.source, args.state_root)
        if args.action == 'consumer':
            rest = args.consumer_args
            return controller.consumer(rest[1:] if rest[:1] == ['--'] else rest)
        if args.consumer_args:
            raise ValueError('unexpected_arguments')
        if args.action == 'status':
            result = controller.status()
        else:
            make_private_directory(controller.runtime)
            with private_lock(controller.runtime / 'launch.lock'):
                result = controller.start(args.mode) if args.action == 'start' else controller.stop()
                until = time.monotonic() + args.wait_connected
                while args.action == 'start' and time.monotonic() < until:
                    result = controller.status()
                    if result['connected_fresh'] or not result['tracked_process_alive']:
                        break
                    time.sleep(.25)
                write_json(controller.runtime / 'STATUS.json', result)
        print(json.dumps(result))
        return 0 if not args.wait_connected or result.get('connected_fresh') else 1
    except ValueError as error:
        print(json.dumps({'error': str(error), 'ready_not_assumed': True}))
        return 1
    except Exception:
        print(json.dumps({'error': 'recovery_control_failed', 'ready_not_assumed': True}))
        return 1


if __name__ == '__main__':
    sys.exit(main())
