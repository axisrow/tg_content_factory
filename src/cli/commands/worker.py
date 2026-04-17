from __future__ import annotations

import argparse

from src.config import load_config
from src.runtime.worker import run_worker


def run(args: argparse.Namespace) -> None:
    config = load_config(args.config)
    run_worker(config)
