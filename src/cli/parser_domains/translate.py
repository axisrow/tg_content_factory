from __future__ import annotations

import argparse


def register(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser | None:
    translate_parser = subparsers.add_parser("translate", help="Language detection and translation")
    translate_sub = translate_parser.add_subparsers(dest="translate_action")
    translate_sub.add_parser("stats", help="Show language distribution")
    detect_parser = translate_sub.add_parser("detect", help="Backfill language detection")
    detect_parser.add_argument("--batch-size", type=int, default=5000)
    run_parser = translate_sub.add_parser("run", help="Run translation batch")
    run_parser.add_argument("--target", default="en", help="Target language code")
    run_parser.add_argument("--source-filter", default="", help="Comma-separated source languages")
    run_parser.add_argument("--limit", type=int, default=100, help="Max messages to translate")

    translate_msg = translate_sub.add_parser("message", help="Translate a single message")
    translate_msg.add_argument("message_id", type=int, help="Message DB id")
    translate_msg.add_argument("--target", default="en", help="Target language code")
