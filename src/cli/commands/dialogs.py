from __future__ import annotations

import argparse

from src.cli.commands import my_telegram


def run(args: argparse.Namespace) -> None:
    """'dialogs' is the new name for 'my-telegram'. Delegates to my_telegram.run()."""
    my_telegram.run(args)
