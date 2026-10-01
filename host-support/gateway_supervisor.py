#!/usr/bin/env python3
"""Linux host-lifetime supervisor. Does not provide host/reboot autostart.

Run explicitly after stopping any existing gateway; original wrapper retains both
sender locks. Never reads token/ledger. Child output is discarded, except bounded
allowlisted terminal JSON codes. No environment or arbitrary exception logging.
"""
from __future__ import annotations
import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import signal
import stat
import subprocess
import sys
import threading
import time

ROOT = Path(__file__).resolve().parent
DEFAULT_COMMAND = [str(ROOT / '.hermes-venv/bin/python'), str(ROOT / 'start_go_gateway.py')]
FATAL_CODES = frozenset({'authentication_failed', 'privileged_intents_required',
    'bot_identity_mismatch', 'guild_channel_invalid', 'guild_permissions_missing',
    'gateway_proxy_route_or_endpoint_invalid', 'route_recipient_mismatch',
    'invalid_input_or_local_io', 'guild_channel_unavailable', 'guild_permission_denied',
    'configured_channel_lookup_failed', 'configured_channel_permissions_missing',
    'configured_channel_invalid', 'configured_channel_mismatch'})


def proxy_fingerprint(env=None):
    env = os.environ if env is None else env
    keys = ('HTTP_PROXY', 'HTTPS_PROXY', 'ALL_PROXY', 'NO_PROXY',
            'http_proxy', 'https_proxy', 'all_proxy', 'no_proxy', 'BRIDGE_HTTPS_PROXY', 'BRIDGE_NO_PROXY')
    values = {key: env.get(key) for key in keys}
    return hashlib.sha256(json.dumps(values, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def load_proxy_config(path):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd) as handle:
        info = os.fstat(handle.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
            raise ValueError('unsafe_proxy_config')
        raw = handle.read(8193)
    if len(raw) > 8192:
        raise ValueError('oversized_proxy_config')
    obj = json.loads(raw)
    if not isinstance(obj, dict) or set(obj) != {'BRIDGE_HTTPS_PROXY', 'BRIDGE_NO_PROXY'}:
        raise ValueError('invalid_proxy_config_keys')
    # Existing project validator rejects credentials, controls, invalid URLs,
    # malformed bypasses and inconsistent REST/Gateway routing. Pure local parse.
    sys.path.insert(0, str(ROOT / 'hermes-dot-gateway'))
    try:
        from dot_bridge.proxy import ProxyConfig
        config = ProxyConfig.load(obj)
        if config.discord_route() is None:
            raise ValueError('explicit_proxy_config_cannot_select_direct_route')
    finally:
        sys.path.pop(0)
    return obj


def emit(event, **fields):
    print(json.dumps({'time': round(time.time(), 3), 'event': event, **fields}, sort_keys=True), flush=True)


def proc_identity(pid):
    try:
        text = Path(f'/proc/{pid}/stat').read_text()
        fields = text[text.rfind(')') + 2:].split()
        if fields[0] == 'Z':
            return None
        return {'pid': pid, 'start_ticks': int(fields[19]),
                'boot_id': Path('/proc/sys/kernel/random/boot_id').read_text().strip()}
    except (OSError, ValueError, IndexError):
        return None


def private_dir(path):
    path.mkdir(mode=0o700, parents=False, exist_ok=True)
    info = path.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
        raise ValueError('unsafe_runtime_directory')


def lock_open(runtime):
    fd = os.open(runtime / 'supervisor.lock', os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
    info = os.fstat(fd)
    if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
        os.close(fd)
        raise ValueError('unsafe_supervisor_lock')
    return fd


def read_state(runtime):
    fd = os.open(runtime / 'state.json', os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    info = os.fstat(fd)
    if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
        os.close(fd)
        raise ValueError('unsafe_state_file')
    with os.fdopen(fd) as handle:
        return json.loads(handle.read(8192))


def write_state(runtime, state):
    temp = runtime / f'.state-{os.getpid()}'
    fd = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        with os.fdopen(fd, 'w') as handle:
            json.dump(state, handle, sort_keys=True)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp, runtime / 'state.json')
    finally:
        temp.unlink(missing_ok=True)


def inspect(runtime):
    if not runtime.exists():
        return {'status': 'not_started'}
    private_dir(runtime)
    fd = lock_open(runtime)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            locked = False
        except BlockingIOError:
            locked = True
        try:
            state = read_state(runtime)
        except (OSError, ValueError):
            return {'status': 'locked_unknown' if locked else 'not_started'}
        ident = state.get('identity', {})
        valid = proc_identity(ident.get('pid', -1)) == ident and bool(ident)
        if locked and valid:
            return state
        return {'status': 'orphan_locked' if locked else 'inactive',
                'last_status': state.get('status'), 'identity_valid': valid}
    finally:
        os.close(fd)


def validated_stop(runtime):
    state = inspect(runtime)
    ident = state.get('identity')
    if not ident:
        emit('stop_refused', status=state['status'])
        return 3
    # pidfd prevents PID-reuse races. Fail closed if platform cannot provide it.
    try:
        fd = os.pidfd_open(ident['pid'])
    except (OSError, AttributeError):
        emit('stop_refused', status='pidfd_unavailable')
        return 3
    try:
        if proc_identity(ident['pid']) != ident:
            emit('stop_refused', status='identity_changed')
            return 3
        cmdline = Path(f"/proc/{ident['pid']}/cmdline").read_bytes().split(b'\0')
        expected = [str(Path(__file__).resolve()).encode(), b'run']
        runtime_matches = runtime == ROOT / '.gateway-supervisor'
        if b'--runtime' in cmdline:
            index = cmdline.index(b'--runtime')
            runtime_matches = index + 1 < len(cmdline) and cmdline[index + 1] == str(runtime).encode()
        if not runtime_matches or not all(value in cmdline for value in expected):
            emit('stop_refused', status='command_identity_mismatch')
            return 3
        signal.pidfd_send_signal(fd, signal.SIGTERM)
        emit('stop_requested')
        return 0
    finally:
        os.close(fd)


class SafeOutput:
    """Bounded parser: only a fixed fatal enum survives; never stores raw logs."""
    def __init__(self, stream):
        self.stream = stream
        self.fatal = None
        self.thread = threading.Thread(target=self.consume, daemon=True)
        self.thread.start()

    def consume(self):
        line = bytearray()
        oversized = False
        while True:
            chunk = self.stream.read(1024)
            if not chunk:
                break
            for value in chunk:
                if value == 10:
                    if not oversized:
                        try:
                            obj = json.loads(line)
                            code = obj.get('error') if isinstance(obj, dict) else None
                            if isinstance(code, str) and code in FATAL_CODES:
                                self.fatal = code
                        except (ValueError, UnicodeDecodeError):
                            pass
                    line.clear()
                    oversized = False
                elif len(line) < 4096 and not oversized:
                    line.append(value)
                else:
                    line.clear()
                    oversized = True
        self.stream.close()


def run(runtime, command, attempts=6, delay=2.0, max_delay=60.0, drain=30.0, env=None):
    private_dir(runtime)
    lock = lock_open(runtime)
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        os.close(lock)
        emit('duplicate_refused')
        return 4
    stopping = threading.Event()
    old_handlers = {}
    for sig in (signal.SIGTERM, signal.SIGINT):
        old_handlers[sig] = signal.signal(sig, lambda *_: stopping.set())
    state = {'identity': proc_identity(os.getpid()), 'status': 'starting', 'attempt': 0}
    child = None
    def update(status, **fields):
        state.update(status=status, **fields)
        write_state(runtime, state)
        emit(status, **fields)
    try:
        update('starting')
        for attempt in range(1, attempts + 1):
            if stopping.is_set():
                break
            state['attempt'] = attempt
            try:
                # Inherit lock: an orphan gateway blocks replacements too. Preserve
                # inherited environment unchanged; wrapper handles token lookup.
                child = subprocess.Popen(command, stdin=subprocess.DEVNULL,
                    stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                    start_new_session=True, pass_fds=(lock,), bufsize=0, env=env)
            except OSError:
                update('latched', reason='child_launch_failed')
                return 1
            output = SafeOutput(child.stdout)
            update('running', child_identity=proc_identity(child.pid), attempt=attempt)
            while child.poll() is None and not stopping.wait(0.1):
                pass
            if stopping.is_set() and child.poll() is None:
                update('draining')
                # Direct owned child only: never signal a stale PID or process group.
                child.send_signal(signal.SIGINT)
                try:
                    child.wait(timeout=drain)
                except subprocess.TimeoutExpired:
                    update('drain_timeout')
                    child.kill()
                    child.wait()
            rc = child.wait()
            output.thread.join(timeout=2)
            state.pop('child_identity', None)
            if stopping.is_set():
                update('stopped', reason='operator_stop', returncode=rc)
                return 0
            if rc == 0:
                update('stopped', reason='clean_exit', returncode=rc)
                return 0
            if output.fatal or rc in {2, 64, 77, 78}:
                update('latched', reason=output.fatal or 'configuration_or_permission_exit', returncode=rc)
                return 1
            if attempt == attempts:
                update('latched', reason='failure_budget_exhausted', returncode=rc)
                return 1
            pause = min(max_delay, delay * 2 ** min(attempt - 1, 20))
            update('backoff', returncode=rc, delay_seconds=pause)
            if stopping.wait(pause):
                break
        update('stopped', reason='operator_stop')
        return 0
    finally:
        # Exceptions in supervisor must not leak an untracked active sender.
        if child is not None and child.poll() is None:
            child.send_signal(signal.SIGINT)
            try:
                child.wait(timeout=drain)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait()
        for sig, handler in old_handlers.items():
            signal.signal(sig, handler)
        os.close(lock)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('run', 'status', 'stop', 'proxy-fingerprint'))
    parser.add_argument('--runtime', type=Path, default=ROOT / '.gateway-supervisor')
    parser.add_argument('--attempts', type=int, default=6)
    parser.add_argument('--delay', type=float, default=2)
    parser.add_argument('--max-delay', type=float, default=60)
    parser.add_argument('--drain', type=float, default=30)
    parser.add_argument('--proxy-config', type=Path, help='Validated credential-free BRIDGE proxy overrides only')
    parser.add_argument('--expected-proxy-sha256', help='Fail closed if inherited routing env fingerprint differs')
    parser.add_argument('--offline-child', type=Path, help='Offline testing only: Python fake child script')
    args = parser.parse_args(argv)
    if not 1 <= args.attempts <= 100 or not 0 <= args.delay <= args.max_delay <= 3600 or not 0 < args.drain <= 300:
        parser.error('invalid bounded supervisor policy')
    runtime = args.runtime.absolute()
    child_env = None
    if args.proxy_config and args.action in {'run', 'proxy-fingerprint'}:
        child_env = os.environ.copy()
        child_env.update(load_proxy_config(args.proxy_config))
    if args.action == 'proxy-fingerprint':
        print(proxy_fingerprint(child_env))
        return 0
    if args.action == 'run' and args.expected_proxy_sha256 and args.expected_proxy_sha256 != proxy_fingerprint(child_env):
        emit('launch_refused', reason='proxy_environment_changed')
        return 5
    if args.action == 'status':
        print(json.dumps(inspect(runtime), sort_keys=True))
        return 0
    if args.action == 'stop':
        return validated_stop(runtime)
    command = [sys.executable, str(args.offline_child.absolute())] if args.offline_child else DEFAULT_COMMAND
    return run(runtime, command, args.attempts, args.delay, args.max_delay, args.drain, env=child_env)


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except (OSError, ValueError):
        emit('supervisor_error', reason='local_io_or_state_invalid')
        raise SystemExit(1)
