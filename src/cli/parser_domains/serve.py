from __future__ import annotations

import argparse


def register(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser | None:
    serve_parser = subparsers.add_parser("serve", help="Start web server")
    serve_parser.add_argument("--web-pass", help="Web panel password (overrides config)")
    serve_parser.add_argument(
        "--no-worker",
        action="store_true",
        help=(
            "Do not spawn the embedded Telegram worker inside this process. "
            "Use this when you run `python -m src.main worker` in a separate "
            "process / container (Docker, k8s). Without this flag the serve "
            "command runs both the web app and the worker in one process — "
            "clicking 'Collect' in the UI immediately triggers collection."
        ),
    )
