from __future__ import annotations

import argparse


def register(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser | None:
    p = subparsers.add_parser(
        "mcp-server",
        help="Expose the agent tool registry as a stdio MCP server (for external agents like Codex)",
    )
    p.add_argument(
        "--no-pool",
        action="store_true",
        help="Skip Telegram client pool init; pool-dependent tools return an error message",
    )
    return p
