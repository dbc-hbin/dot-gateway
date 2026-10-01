#!/usr/bin/env python3
"""Benchmark reviewed native CLI binaries using a temporary fixture environment."""

import argparse
import json
import os
import sqlite3
import statistics
import subprocess
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path


def stats(samples):
    ordered = sorted(samples)
    return {
        "n": len(ordered),
        "median_ms": round(statistics.median(ordered), 3),
        "p95_ms": round(ordered[int(0.95 * (len(ordered) - 1))], 3),
    }


@contextmanager
def fixture(binary, count):
    with tempfile.TemporaryDirectory(prefix="gateway-latency-offline-") as directory:
        os.chmod(directory, 0o700)
        database = str(Path(directory) / "queue.db")
        # Intentionally do not inherit credentials, policy, or BRIDGE_DB.
        environment = {
            "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
            "DISCORD_OWNER_ID": "1",
            "DISCORD_ALLOWED_DM_IDS": "1",
            "DISCORD_EXPECTED_BOT_ID": "4",
            "BRIDGE_DB": database,
        }
        subprocess.run(
            [binary, "status"], env=environment,
            stdout=subprocess.DEVNULL, check=True,
        )
        connection = sqlite3.connect(database)
        try:
            for index in range(count):
                event = {
                    "platform": "discord",
                    "event_id": str(index + 3),
                    "conversation_id": "2",
                    "sender_id": "1",
                    "text": "x" * 8000,
                    "received_at": 1,
                    "route_kind": "dm",
                    "sender_is_bot": False,
                    "bot_mentioned": False,
                }
                connection.execute(
                    "INSERT INTO inbound(id,platform,event_id,envelope,created) "
                    "VALUES(?,?,?,?,?)",
                    (f"{index:032x}", "discord", str(index + 3),
                     json.dumps(event), index + 1),
                )
            connection.commit()
            yield connection, environment
        finally:
            connection.close()


def measure(binary, count, samples, trace=False):
    elapsed = []
    with fixture(binary, count) as (connection, environment):
        environment["BRIDGE_PHASE_TRACE"] = "1" if trace else "0"
        for iteration in range(samples + 5):
            # Reset only the synthetic claim outside the measured interval.
            connection.execute(
                "UPDATE inbound SET state='pending',claim=NULL,lease_until=NULL "
                "WHERE state='claimed'"
            )
            connection.commit()
            started = time.perf_counter()
            result = subprocess.run(
                [binary, "next", "--begin"], env=environment,
                capture_output=True, check=True,
            )
            duration = (time.perf_counter() - started) * 1000
            if not json.loads(result.stdout)["message"]:
                raise RuntimeError("synthetic next returned no message")
            if iteration >= 5:
                elapsed.append(duration)
    return stats(elapsed)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--before", required=True, type=Path)
    parser.add_argument("--after", required=True, type=Path)
    parser.add_argument("--samples", type=int, default=60)
    parser.add_argument("--trace-samples", type=int, default=100)
    args = parser.parse_args()
    if args.samples < 1 or args.trace_samples < 1:
        parser.error("sample counts must be positive")
    binaries = {
        "before": str(args.before.resolve()),
        "after": str(args.after.resolve()),
    }
    # A deployed shell wrapper can override the clean environment with its live
    # ledger path. Reject all scripts before invoking either candidate. ELF is a
    # format check, not authenticity: supply only reviewed native CLI builds.
    for version, binary in binaries.items():
        try:
            with open(binary, "rb") as candidate:
                magic = candidate.read(4)
        except OSError as error:
            parser.error(f"cannot read {version} binary: {error}")
        if magic != b"\x7fELF":
            parser.error(f"{version} must be a compiled ELF CLI, never a wrapper")
    results = []
    for count in (1, 1000):
        for version, binary in binaries.items():
            results.append({
                "case": f"next_{count}_maxsize",
                "version": version,
                **measure(binary, count, args.samples),
            })
    for enabled in (False, True):
        results.append({
            "case": "next_trace_on" if enabled else "next_trace_off",
            **measure(binaries["after"], 1, args.trace_samples, trace=enabled),
        })
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
