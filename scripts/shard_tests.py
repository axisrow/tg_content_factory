#!/usr/bin/env python3
"""Deterministic file-atomic sharding of the parallel-safe suite for CI.

Prints the test files of one shard, one per line, for use as pytest args:

    pytest -q -m "not aiosqlite_serial" -n auto \\
        $(python scripts/shard_tests.py --num-shards 2 --shard-id 0)

File-atomic by construction: the ``--dist=loadfile`` invariant (tests in one
file must stay on one worker) holds because whole files are assigned to
shards. Balancing is greedy LPT by collected test count, heaviest file first.
Deterministic for a given test tree (sorted input, index tie-breaks), so both
matrix jobs of one CI run split the same tree the same way.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from collections import defaultdict

MARKER = "not aiosqlite_serial"  # keep in sync with the CI shard legs


def collect_files(marker: str) -> list[tuple[str, int]]:
    """(file, selected-test-count) per file, sorted by filename.

    Runs the same ``-m`` filter as the shard run legs, so serial files never
    enter the shard lists.
    """
    proc = subprocess.run(
        [sys.executable, "-m", "pytest", "tests", "--collect-only", "-q", "-m", marker],
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        sys.exit(f"collection failed (exit {proc.returncode}):\n{proc.stdout[-2000:]}\n{proc.stderr[-2000:]}")
    counts: dict[str, int] = defaultdict(int)
    for line in proc.stdout.splitlines():
        if "::" in line:  # summary lines ("N tests collected...") carry no "::"
            counts[line.split("::", 1)[0]] += 1
    return sorted(counts.items())


def lpt(files: list[tuple[str, int]], num_shards: int) -> list[list[str]]:
    """Assign whole files to shards, heaviest first, to the least-loaded shard."""
    shards: list[list[str]] = [[] for _ in range(num_shards)]
    loads = [0] * num_shards
    for path, count in sorted(files, key=lambda item: (-item[1], item[0])):
        least = min(range(num_shards), key=lambda k: (loads[k], k))
        shards[least].append(path)
        loads[least] += count
    return shards


def main() -> None:
    parser = argparse.ArgumentParser(description="Print the test files of one CI shard, one per line.")
    parser.add_argument("--num-shards", type=int, required=True)
    parser.add_argument("--shard-id", type=int, required=True)
    parser.add_argument("--marker", default=MARKER)
    args = parser.parse_args()
    if not 0 <= args.shard_id < args.num_shards:
        sys.exit(f"--shard-id {args.shard_id} out of range for --num-shards {args.num_shards}")
    shards = lpt(collect_files(args.marker), args.num_shards)
    shard = shards[args.shard_id]
    if not shard:
        # An empty arg list would make pytest collect the whole rootdir — a
        # silent full duplicate run. Fail loud instead.
        sys.exit(f"shard {args.shard_id}/{args.num_shards} is empty — refusing to emit no file args")
    print("\n".join(shard))


if __name__ == "__main__":
    main()
