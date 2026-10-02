"""Offline tests: synthetic credentials/DBs and temporary subprocesses only."""
import contextlib
import fcntl
import io
import json
import os
from pathlib import Path
import signal
import sqlite3
import subprocess
import tempfile
import time
import unittest
from unittest.mock import patch

import install_token_file
import apply_source_overlay as overlay
import recover_gateway as recovery
from recovery_files import private_lock, read_bytes, read_json, write_bytes, write_json


class RecoveryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.root.chmod(0o700)

    def tearDown(self):
        self.temp.cleanup()

    def file(self, name, content=b'opaque-synthetic-fixture\n'):
        path = self.root / name
        write_bytes(path, content)
        return path

    def controller(self):
        source = self.root / 'source'
        state = self.root / 'state'
        for path in (source, state, state / 'bridge', state / 'daemon'):
            path.mkdir(mode=0o700)
        db = state / 'bridge/bridge.sqlite3'
        con = sqlite3.connect(db)
        con.executescript("create table runtime(key text primary key, value text);"
                          "create table catchup_meta(key text primary key, value text);"
                          "insert into catchup_meta values('state','disarmed_restore');")
        con.commit()
        con.close()
        db.chmod(0o600)
        return recovery.Controller(source, state)

    def test_opaque_install_and_no_output(self):
        source = self.file('input', b'\xff\x00opaque\n')
        destination = self.root / 'installed'
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            result = install_token_file.install(source, destination)
        self.assertFalse(result['credential_read'])
        self.assertFalse(destination.exists())
        self.assertEqual(output.getvalue(), '')
        self.assertTrue(install_token_file.install(source, destination, True)['applied'])
        self.assertEqual(destination.read_bytes(), b'\xff\x00opaque\n')
        self.assertEqual(destination.stat().st_mode & 0o777, 0o600)
        with self.assertRaises(FileExistsError):
            install_token_file.install(source, destination, True)
        self.assertEqual(destination.read_bytes(), b'\xff\x00opaque\n')

    def test_install_rejects_unsafe_sources_and_destinations(self):
        source = self.file('input')
        destination = self.root / 'dest'
        source.chmod(0o644)
        with self.assertRaises(ValueError):
            install_token_file.install(source, destination, True)
        source.chmod(0o600)
        alias = self.root / 'alias'
        alias.symlink_to(source)
        with self.assertRaises(OSError):
            install_token_file.install(alias, destination, True)
        alias.unlink()
        os.link(source, alias)
        with self.assertRaises(ValueError):
            install_token_file.install(source, destination, True)
        alias.unlink()
        destination.symlink_to(source)
        with self.assertRaises(FileExistsError):
            install_token_file.install(source, destination, True)
        self.assertEqual(source.read_bytes(), b'opaque-synthetic-fixture\n')
        destination.unlink()
        fifo = self.root / 'fifo'
        os.mkfifo(fifo, 0o600)
        with self.assertRaises(ValueError):
            install_token_file.install(fifo, destination, True)
        source.write_bytes(b'x' * 16385)
        with self.assertRaises(ValueError):
            install_token_file.install(source, destination, True)
        self.assertFalse(destination.exists())

    def test_symlink_parent_and_duplicate_json_rejected(self):
        actual = self.root / 'actual'
        actual.mkdir(mode=0o700)
        alias = self.root / 'alias'
        alias.symlink_to(actual, target_is_directory=True)
        with self.assertRaises(OSError):
            write_bytes(alias / 'output', b'no')
        with self.assertRaises(ValueError):
            read_json(self.file('duplicate.json', b'{"a":true,"a":false}'))

    def test_status_never_creates_db_and_unknown_lock_blocks(self):
        c = self.controller()
        self.assertFalse(c.status()['running'])
        fd = os.open(str(c.db) + '.lock', os.O_CREAT | os.O_RDWR, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.assertTrue(c.status()['untracked_dispatcher'])
            with self.assertRaisesRegex(ValueError, 'unknown_dispatcher'):
                c.start('receive-only')
            with self.assertRaisesRegex(ValueError, 'unknown_dispatcher'):
                c.stop()
        finally:
            os.close(fd)
        c.db.unlink()
        with self.assertRaises(FileNotFoundError):
            c.status()
        self.assertFalse(c.db.exists())

    def test_start_idempotence_before_lock_and_mode_conflict(self):
        c = self.controller()
        status = {'tracked_process_alive': True, 'mode': 'receive-only'}
        with patch.object(c, 'status', return_value=status), patch.object(c, 'environment') as env:
            self.assertTrue(c.start('receive-only')['already_running'])
            env.assert_not_called()
            with self.assertRaisesRegex(ValueError, 'different_mode'):
                c.start('active')

    def test_stale_catchup_and_unpaused_consumer_block_start(self):
        c = self.controller()
        status = dict(c.status(), catchup_state='armed')
        with patch.object(c, 'status', return_value=status):
            with self.assertRaisesRegex(ValueError, 'catchup_not_disarmed'):
                c.start('active')
        with patch.object(c, 'environment', return_value={}), patch.object(c, 'consumer_record', return_value={'paused': False}):
            with self.assertRaisesRegex(ValueError, 'must_be_paused'):
                c.start('active')

    def test_pidfd_only_signals_verified_child(self):
        c = self.controller()
        child = subprocess.Popen(['/usr/bin/sleep', '60'])
        try:
            actual = recovery.identity(child.pid)
            c.binary = Path(actual['exe'])
            write_json(c.runtime / 'process.json', {'identity': actual, 'mode': 'receive-only'})
            self.assertTrue(c.status()['tracked_process_alive'])
            c.db.unlink()  # Telemetry failure must not prevent a verified pidfd stop.
            self.assertTrue(c.stop()['stopped'])
            child.wait(timeout=2)
            self.assertEqual(child.returncode, -signal.SIGINT)
        finally:
            if child.poll() is None:
                child.terminate()
                child.wait()

    def test_mismatched_identity_never_signals(self):
        c = self.controller()
        actual = recovery.identity(os.getpid())
        actual['start_ticks'] = 'invalid'
        write_json(c.runtime / 'process.json', {'identity': actual, 'mode': 'active'})
        with patch('os.pidfd_open') as pidfd:
            self.assertTrue(c.stop()['already_stopped'])
            pidfd.assert_not_called()

    def test_consumer_record_boot_and_exclusivity(self):
        c = self.controller()
        record = {'schema': 1, 'consumer_id': 'synthetic-consumer',
                  'boot_id': recovery.identity(os.getpid())['boot_id'],
                  'controller_verified': True, 'exclusive_consumer_verified': True, 'paused': True}
        write_json(c.root / 'CONSUMER_READY.json', record)
        self.assertTrue(c.consumer_record()['paused'])
        for field, value in [('boot_id', 'old-boot'), ('exclusive_consumer_verified', False),
                             ('consumer_id', ''), ('paused', 'false')]:
            write_json(c.root / 'CONSUMER_READY.json', dict(record, **{field: value}))
            with self.assertRaises(ValueError):
                c.consumer_record()

    def test_consumer_commands_cannot_request_network_or_override_owner(self):
        c = self.controller()
        for args in (['diagnostic-send'], ['read-message'], ['next', '--consumer-id', 'other']):
            with self.assertRaisesRegex(ValueError, 'unsupported_consumer'):
                c.consumer(args)

    def test_environment_removes_inherited_credentials_and_routes(self):
        with patch.dict(os.environ, {'DISCORD_BOT_TOKEN': 'synthetic', 'DISCORD_BOT_TOKEN_FILE': '/invalid',
                                   'BRIDGE_DB': '/invalid', 'HTTPS_PROXY': 'http://invalid'}):
            env = recovery.clean_environment()
        self.assertFalse(any(k.startswith(('DISCORD_', 'BRIDGE_')) for k in env))
        self.assertNotIn('HTTPS_PROXY', env)

    def test_overlay_verify_apply_repeat_and_destination_conflict(self):
        source, incoming = self.root / 'source', self.root / 'overlay'
        source.mkdir(mode=0o700)
        incoming.mkdir(mode=0o700)
        base = overlay.digest(b'synthetic-base')
        write_json(source / 'PRIVATE_DERIVATION.json', {'base_manifest_sha256': base})
        write_bytes(source / 'example.py', b'old source')
        write_bytes(incoming / 'example.py', b'new source')
        manifest = {'schema': 1, 'base_source_manifest_sha256': base, 'files': [
            {'path': 'example.py', 'sha256': overlay.digest(b'new source'),
             'configured_base_sha256': overlay.digest(b'old source')}]}
        write_json(incoming / 'OVERLAY_MANIFEST.json', manifest)
        pin = overlay.digest((incoming / 'OVERLAY_MANIFEST.json').read_bytes())
        self.assertEqual(overlay.apply(source, incoming, pin)['changed_files'], 1)
        self.assertEqual((source / 'example.py').read_bytes(), b'old source')
        self.assertEqual(overlay.apply(source, incoming, pin, True)['changed_files'], 1)
        self.assertEqual(overlay.apply(source, incoming, pin, True)['changed_files'], 0)
        self.assertFalse(read_json(source / 'UPGRADE_APPLIED.json')['activation_authorized'])
        (source / 'example.py').write_bytes(b'unreviewed')
        with self.assertRaisesRegex(ValueError, 'unreviewed_destination'):
            overlay.apply(source, incoming, pin, True)
        with self.assertRaisesRegex(ValueError, 'manifest_mismatch'):
            overlay.apply(source, incoming, '0' * 64, True)

    def test_status_rejects_old_heartbeat_even_when_process_matches(self):
        c = self.controller()
        actual = recovery.identity(os.getpid())
        c.binary = Path(actual['exe'])
        now = time.time()
        write_json(c.runtime / 'process.json', {'identity': actual, 'mode': 'active', 'launched_at': now})
        def heartbeat(stamp):
            con = sqlite3.connect(c.db)
            con.execute("insert or replace into runtime values('gateway',?)",
                        (json.dumps({'state': 'connected', 'updated_at': stamp, 'receive_only': False}),))
            con.commit()
            con.close()
        with patch.object(c, 'lock_held', return_value=True):
            heartbeat(now - 1)
            self.assertFalse(c.status()['connected_fresh'])
            heartbeat(now + .01)
            self.assertTrue(c.status()['connected_fresh'])
            self.assertFalse(c.status()['end_to_end_ready'])

    def test_consumer_child_keeps_lock_after_parent_fd_closes(self):
        lock = self.root / 'consumer.lock'
        with private_lock(lock) as fd:
            child = subprocess.Popen(['/usr/bin/sleep', '60'], pass_fds=(fd,))
        try:
            with self.assertRaises(BlockingIOError):
                with private_lock(lock):
                    pass
        finally:
            child.terminate()
            child.wait(timeout=2)
        with private_lock(lock):
            pass

    def test_disconnected_maintenance_works_but_polling_stays_gated(self):
        c = self.controller()
        current = dict(c.status(), mode='active', connected_fresh=False)
        record = {'paused': True, 'consumer_id': 'synthetic-consumer'}
        write_json(c.root / 'operation.json', {})
        seen = []
        def run(args, **kwargs):
            self.assertEqual(len(kwargs['pass_fds']), 1)
            self.assertGreater(os.fstat(kwargs['pass_fds'][0]).st_ino, 0)
            seen.append(args[1])
            return subprocess.CompletedProcess(args, 0)
        with patch.object(c, 'status', return_value=current), \
             patch.object(c, 'consumer_record', return_value=record), \
             patch.object(c, 'environment', return_value={}), \
             patch.object(recovery, 'policy_environment', return_value={}), \
             patch.object(recovery.subprocess, 'run', side_effect=run):
            self.assertEqual(c.consumer(['delivery', 'synthetic-reply']), 0)
            self.assertEqual(seen, ['delivery'])
            with self.assertRaisesRegex(ValueError, 'reply_path_not_activated'):
                c.consumer(['next'])


if __name__ == '__main__':
    unittest.main()
