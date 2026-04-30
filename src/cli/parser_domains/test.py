from __future__ import annotations

import argparse


def register(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser | None:
    test_parser = subparsers.add_parser("test", help="Run diagnostic tests")
    test_sub = test_parser.add_subparsers(dest="test_action")
    test_sub.add_parser("all", help="Run all test sections (read + write + telegram)")
    test_sub.add_parser("read", help="Read-only DB checks")
    test_sub.add_parser("write", help="Write DB checks on a temporary DB copy")
    test_sub.add_parser("telegram", help="Live Telegram API tests on a temporary DB copy")
    test_sub.add_parser(
        "benchmark",
        help="Benchmark serial pytest run against the safe mixed parallel test workflow",
    )
