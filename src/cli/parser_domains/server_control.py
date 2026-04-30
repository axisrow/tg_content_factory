from __future__ import annotations

import argparse


def register(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser | None:
    subparsers.add_parser("stop", help="Stop web server started by this app")

    restart_parser = subparsers.add_parser("restart", help="Restart web server")
    restart_parser.add_argument("--web-pass", help="Web panel password (overrides config)")
