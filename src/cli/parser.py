from __future__ import annotations

import argparse

from src import __version__
from src.cli.parser_domains import register_all


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="TG Post Search")
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    parser.add_argument("--config", default="config.yaml", help="Path to config file")
    sub = parser.add_subparsers(dest="command")
    register_all(sub)
    return parser
