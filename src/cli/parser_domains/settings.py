from __future__ import annotations

import argparse


def register(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser | None:
    settings_parser = subparsers.add_parser("settings", help="System settings management")
    settings_sub = settings_parser.add_subparsers(dest="settings_action")
    settings_get = settings_sub.add_parser("get", help="Show settings")
    settings_get.add_argument("--key", default=None, help="Specific setting key (default: show all)")
    settings_set = settings_sub.add_parser("set", help="Set a setting value")
    settings_set.add_argument("key", help="Setting key")
    settings_set.add_argument("value", help="Setting value")
    settings_sub.add_parser("info", help="Show system diagnostics")

    settings_agent = settings_sub.add_parser("agent", help="Configure agent backend and defaults")
    settings_agent.add_argument("--backend", default=None, help="Agent backend (claude-agent-sdk, deepagents)")
    settings_agent.add_argument("--prompt-template", default=None, dest="prompt_template",
                                help="Default prompt template")

    settings_filter = settings_sub.add_parser("filter-criteria", help="Configure filter thresholds")
    settings_filter.add_argument("--min-uniqueness", type=float, default=None, dest="min_uniqueness")
    settings_filter.add_argument("--min-sub-ratio", type=float, default=None, dest="min_sub_ratio")
    settings_filter.add_argument("--max-cross-dupe", type=float, default=None, dest="max_cross_dupe")
    settings_filter.add_argument("--min-cyrillic", type=float, default=None, dest="min_cyrillic")

    settings_semantic = settings_sub.add_parser("semantic", help="Configure semantic search")
    settings_semantic.add_argument("--provider", default=None, help="Embedding provider")
    settings_semantic.add_argument("--model", default=None, help="Embedding model")
    settings_semantic.add_argument("--api-key", default=None, dest="api_key", help="Embedding API key")
