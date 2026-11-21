"""OS process creation identity, independent of the CLI import graph."""

import os
import re
import subprocess
import sys
from pathlib import Path

_NUMBERS = re.compile(r"\d+")


def process_executable(pid: int) -> str | None:
    try:
        if sys.platform.startswith("linux"):
            value = os.readlink(Path("/proc") / str(pid) / "exe").removesuffix(" (deleted)")
        else:
            result = subprocess.run(["ps", "-o", "comm=", "-p", str(pid)],
                                    capture_output=True, text=True, check=False)
            value = result.stdout.strip() if result.returncode == 0 else ""
        return str(Path(value).resolve()) if value else None
    except OSError:
        return None


def process_start_identity(pid: int) -> str | None:
    if sys.platform.startswith("linux"):
        try:
            fields = (Path("/proc") / str(pid) / "stat").read_text().rsplit(")", 1)[1].split()
            return None if fields[0] == "Z" else fields[19]
        except (OSError, IndexError):
            return None
    # `ps` spells the start time in the caller's locale; a fixed one keeps the
    # identity comparable between processes started from different shells.
    result = subprocess.run(["ps", "-o", "lstart=", "-p", str(pid)], capture_output=True, text=True, check=False,
                            env={**os.environ, "LC_ALL": "C"})
    return result.stdout.strip() or None


def same_start_identity(recorded: object, current: str | None) -> bool:
    """Whether a recorded start identity names the process observed now.

    A record written before the identity was read in a fixed locale holds the
    start time as the writer's locale spelled it. Its numbers (day, year,
    clock) are the same in every spelling, so those are compared.
    """
    if not recorded or not current:
        return False
    if recorded == current:
        return True
    numbers = _NUMBERS.findall(str(recorded))
    return bool(numbers) and sorted(numbers) == sorted(_NUMBERS.findall(current))
