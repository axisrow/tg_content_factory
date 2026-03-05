from __future__ import annotations

import argparse
import logging
import sys

import uvicorn

from src.config import load_config
from src.web.app import create_app


def run(args: argparse.Namespace) -> None:
    config = load_config(args.config)
    if args.web_pass:
        config.web.password = args.web_pass
    if not config.web.password:
        logging.error("WEB_PASS must be set for web panel authentication")
        sys.exit(1)
    app = create_app(config)
    uvicorn.run(app, host=config.web.host, port=config.web.port)
