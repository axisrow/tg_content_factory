"""OS process creation identity, independent of the CLI import graph."""

import os
import subprocess
import sys
from pathlib import Path


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
    result = subprocess.run(["ps", "-o", "lstart=", "-p", str(pid)], capture_output=True, text=True, check=False)
    return result.stdout.strip() or None
