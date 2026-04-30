from __future__ import annotations

import argparse


def register(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser | None:
    # ── provider ──
    provider_parser = subparsers.add_parser("provider", help="LLM provider management")
    provider_sub = provider_parser.add_subparsers(dest="provider_action")
    provider_sub.add_parser("list", help="List configured providers with models and status")
    provider_add = provider_sub.add_parser("add", help="Add or update a provider")
    provider_add.add_argument("name", help="Provider name (e.g. openai, groq, anthropic)")
    provider_add.add_argument("--api-key", required=True, dest="api_key", help="API key")
    provider_add.add_argument("--base-url", default=None, dest="base_url", help="Custom base URL")
    provider_del = provider_sub.add_parser("delete", help="Delete a provider")
    provider_del.add_argument("name", help="Provider name")
    provider_probe = provider_sub.add_parser("probe", help="Test provider connection")
    provider_probe.add_argument("name", help="Provider name")
    provider_refresh = provider_sub.add_parser("refresh", help="Refresh provider models")
    provider_refresh.add_argument("name", nargs="?", default=None, help="Provider name (default: all)")
    provider_sub.add_parser("test-all", help="Test all configured providers")
