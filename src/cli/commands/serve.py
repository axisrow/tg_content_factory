from __future__ import annotations

import argparse
import logging
import sys

import uvicorn

from src.cli.process_control import pid_file_path, register_current_process, unregister_current_process
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
    pid_path = pid_file_path(config)
    try:
        register_current_process(pid_path)
    except RuntimeError as exc:
        logging.error(str(exc))
        sys.exit(1)

    try:
        uvicorn.run(app, host=config.web.host, port=config.web.port)
    finally:
        unregister_current_process(pid_path)
