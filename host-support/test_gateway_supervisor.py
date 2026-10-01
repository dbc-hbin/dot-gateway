"""Offline-only supervisor integration tests: no token, DB or network access."""
import contextlib
import fcntl
import importlib.util
import io
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest

SCRIPT = Path(__file__).with_name('gateway_supervisor.py').resolve()
spec = importlib.util.spec_from_file_location('supervisor', SCRIPT)
supervisor = importlib.util.module_from_spec(spec)
spec.loader.exec_module(supervisor)


class SupervisorTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.runtime = self.root / 'runtime'
        self.child = self.root / 'fake.py'
        self.processes = []

    def tearDown(self):
        for proc in self.processes:
            if proc.poll() is None:
                proc.terminate()
            try:
                proc.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.communicate()
        self.tmp.cleanup()

    def args(self, action='run', *extra):
        return [sys.executable, str(SCRIPT), action, '--runtime', str(self.runtime),
                '--attempts', '3', '--delay', '0.04', '--max-delay', '0.08', '--drain', '0.3', *extra]

    def launch(self, code):
        self.child.write_text(code)
        proc = subprocess.Popen(self.args('run', '--offline-child', str(self.child)),
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        self.processes.append(proc)
        return proc

    def terminal(self, code):
        proc = self.launch(code)
        out, err = proc.communicate(timeout=10)
        self.assertEqual(err, '')
        return proc.returncode, [json.loads(line) for line in out.splitlines()], out

    def wait_running(self, proc):
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                self.fail('supervisor exited before running')
            try:
                state = supervisor.inspect(self.runtime)
                if state.get('status') == 'running':
                    return state
            except (OSError, ValueError):
                pass
            time.sleep(0.01)
        self.fail('no running state')

    def test_clean_exit_not_restarted(self):
        rc, events, _ = self.terminal('pass\n')
        self.assertEqual(rc, 0)
        self.assertEqual(sum(e['event'] == 'running' for e in events), 1)
        self.assertEqual(events[-1]['reason'], 'clean_exit')

    def test_failure_budget_backoff(self):
        rc, events, _ = self.terminal('raise SystemExit(1)\n')
        self.assertEqual(rc, 1)
        self.assertEqual(sum(e['event'] == 'running' for e in events), 3)
        self.assertEqual([e['delay_seconds'] for e in events if e['event'] == 'backoff'], [0.04, 0.08])
        self.assertEqual(events[-1]['reason'], 'failure_budget_exhausted')

    def test_auth_and_permission_latch(self):
        for code in ('authentication_failed', 'configured_channel_permissions_missing'):
            rc, events, out = self.terminal('print(\'{"error":"' + code + '"}\', flush=True)\nraise SystemExit(1)\n')
            self.assertEqual(rc, 1)
            self.assertEqual(events[-1]['reason'], code)
            self.assertEqual(sum(e['event'] == 'running' for e in events), 1)

    def test_config_exit_latches(self):
        rc, events, _ = self.terminal('raise SystemExit(2)\n')
        self.assertEqual(events[-1]['reason'], 'configuration_or_permission_exit')
        self.assertEqual(sum(e['event'] == 'running' for e in events), 1)

    def test_output_is_not_logged(self):
        rc, events, out = self.terminal("import sys\nprint('SECRET_CANARY' * 10000)\nprint('SECRET_CANARY', file=sys.stderr)\nprint('{\"error\":\"SECRET_CANARY\"}')\n")
        self.assertEqual(rc, 0)
        self.assertNotIn('SECRET_CANARY', out)

    def test_graceful_signal_and_validated_stop(self):
        marker = self.root / 'ready'
        drained = self.root / 'drained'
        proc = self.launch(f"import signal,time\nfrom pathlib import Path\ndef stop(*_):\n time.sleep(.05)\n Path({str(drained)!r}).write_text('drained')\n raise SystemExit(0)\nsignal.signal(signal.SIGINT, stop)\nPath({str(marker)!r}).touch()\nwhile True: time.sleep(.01)\n")
        self.wait_running(proc)
        deadline = time.monotonic()+3
        while not marker.exists() and time.monotonic()<deadline:
            time.sleep(.01)
        result = subprocess.run(self.args('stop'), capture_output=True, text=True, timeout=5)
        self.assertEqual(result.returncode, 0, result.stdout)
        out, err = proc.communicate(timeout=5)
        self.assertTrue(drained.exists())
        self.assertEqual(proc.returncode, 0)
        self.assertIn('operator_stop', out)
        self.assertEqual(supervisor.inspect(self.runtime)['status'], 'inactive')

    def test_duplicate_lock(self):
        proc = self.launch('import time\ntime.sleep(30)\n')
        self.wait_running(proc)
        second = subprocess.run(self.args('run', '--offline-child', str(self.child)), capture_output=True, text=True, timeout=5)
        self.assertEqual(second.returncode, 4)
        self.assertIn('duplicate_refused', second.stdout)

    def test_stale_identity_never_signals(self):
        supervisor.private_dir(self.runtime)
        fd = supervisor.lock_open(self.runtime)
        fcntl.flock(fd, fcntl.LOCK_EX)
        ident = supervisor.proc_identity(os.getpid())
        ident['start_ticks'] += 1
        supervisor.write_state(self.runtime, {'identity': ident, 'status': 'running'})
        try:
            self.assertEqual(supervisor.inspect(self.runtime)['status'], 'orphan_locked')
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(supervisor.validated_stop(self.runtime), 3)
        finally:
            os.close(fd)

    def test_command_identity_never_signals_unrelated_process(self):
        supervisor.private_dir(self.runtime)
        fd = supervisor.lock_open(self.runtime)
        fcntl.flock(fd, fcntl.LOCK_EX)
        supervisor.write_state(self.runtime, {'identity': supervisor.proc_identity(os.getpid()), 'status': 'running'})
        try:
            with contextlib.redirect_stdout(io.StringIO()) as output:
                self.assertEqual(supervisor.validated_stop(self.runtime), 3)
            self.assertIn('command_identity_mismatch', output.getvalue())
        finally:
            os.close(fd)

    def test_proxy_fingerprint_guard(self):
        self.child.write_text('raise RuntimeError("must not start")\n')
        result = subprocess.run(self.args('run', '--offline-child', str(self.child), '--expected-proxy-sha256', '0'*64), capture_output=True, text=True)
        self.assertEqual(result.returncode, 5)
        self.assertFalse(self.runtime.exists())

    def test_proxy_config_validated_and_credential_free(self):
        path = self.root / 'proxy.json'
        def save(value):
            path.write_text(json.dumps(value))
            path.chmod(0o600)
        valid = {'BRIDGE_HTTPS_PROXY': 'http://127.0.0.1:1234', 'BRIDGE_NO_PROXY': 'localhost,127.0.0.1'}
        save(valid)
        self.assertEqual(supervisor.load_proxy_config(path), valid)
        invalid = [dict(valid, DISCORD_BOT_TOKEN='CANARY'),
                   dict(valid, BRIDGE_HTTPS_PROXY='http://user:CANARY@localhost:1234'),
                   dict(valid, BRIDGE_NO_PROXY='*'),
                   dict(valid, BRIDGE_HTTPS_PROXY='https://localhost:1234'),
                   dict(valid, BRIDGE_NO_PROXY='discord.com')]
        for value in invalid:
            save(value)
            with self.assertRaises(ValueError):
                supervisor.load_proxy_config(path)

    def test_lock_inherited_until_child_exits(self):
        supervisor.private_dir(self.runtime)
        fd = supervisor.lock_open(self.runtime)
        fcntl.flock(fd, fcntl.LOCK_EX)
        child = subprocess.Popen([sys.executable, '-c', 'import time;time.sleep(.25)'], pass_fds=(fd,))
        os.close(fd)
        probe = supervisor.lock_open(self.runtime)
        try:
            with self.assertRaises(BlockingIOError):
                fcntl.flock(probe, fcntl.LOCK_EX | fcntl.LOCK_NB)
            child.wait(timeout=3)
            fcntl.flock(probe, fcntl.LOCK_EX | fcntl.LOCK_NB)
        finally:
            child.wait(timeout=3)
            os.close(probe)

    def test_zombie_identity_not_alive(self):
        child = subprocess.Popen([sys.executable, '-c', 'pass'])
        time.sleep(.15)  # deliberately do not poll/reap before identity observation
        self.assertIsNone(supervisor.proc_identity(child.pid))
        child.wait()

    def test_drain_timeout_escalation(self):
        marker = self.root / 'ready'
        proc = self.launch(f"import signal,time\nfrom pathlib import Path\nsignal.signal(signal.SIGINT, signal.SIG_IGN)\nPath({str(marker)!r}).touch()\nwhile True: time.sleep(.01)\n")
        self.wait_running(proc)
        deadline = time.monotonic()+3
        while not marker.exists() and time.monotonic()<deadline:
            time.sleep(.01)
        proc.terminate()
        out, _ = proc.communicate(timeout=5)
        self.assertIn('drain_timeout', out)
        self.assertEqual(proc.returncode, 0)


if __name__ == '__main__':
    unittest.main(verbosity=2)
