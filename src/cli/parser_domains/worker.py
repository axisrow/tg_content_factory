from __future__ import annotations

import argparse


def register(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser | None:
    subparsers.add_parser("worker", help="Start Telegram worker runtime")
