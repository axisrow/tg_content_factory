from __future__ import annotations

import argparse

from src.cli.parser_domains import (
    account,
    agent,
    analytics,
    channel,
    collect,
    debug,
    dialogs,
    export,
    filter,
    image,
    messages,
    notification,
    photo_loader,
    pipeline,
    provider,
    scheduler,
    search,
    search_query,
    serve,
    server_control,
    settings,
    test,
    translate,
    worker,
)

_REGISTRARS = (
    serve.register,
    worker.register,
    server_control.register,
    collect.register,
    search.register,
    messages.register,
    channel.register,
    filter.register,
    search_query.register,
    pipeline.register,
    image.register,
    account.register,
    scheduler.register,
    dialogs.register,
    notification.register,
    agent.register,
    photo_loader.register,
    test.register,
    analytics.register,
    provider.register,
    export.register,
    translate.register,
    settings.register,
    debug.register,
)


def register_all(subparsers: argparse._SubParsersAction) -> None:
    for register in _REGISTRARS:
        register(subparsers)
