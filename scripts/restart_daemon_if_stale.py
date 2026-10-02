#!/usr/bin/env python3
# pyright: reportMissingImports=false
"""PostToolUse (Bash) hook entry: restart the daemon when src/ is newer.

Wiring lives in .claude/settings.local.json; decision logic and gates are in
src/cli/daemon_autorestart.py. Silent no-op unless every gate holds.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.cli.daemon_autorestart import main  # noqa: E402

if __name__ == "__main__":
    main()
