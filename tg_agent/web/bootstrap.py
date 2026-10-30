"""Compatibility entrypoints and presentation assembly for the legacy web app."""

from __future__ import annotations

from dataclasses import fields
from typing import cast

from fastapi.templating import Jinja2Templates

from tg_agent.config import AppConfig
from tg_agent.runtime.bootstrap import (
    _cancel_bg_tasks as _cancel_bg_tasks,
)
from tg_agent.runtime.bootstrap import (
    _log_task_exception as _log_task_exception,
)
from tg_agent.runtime.bootstrap import (
    _persist_incoming_dm as _persist_incoming_dm,
)
from tg_agent.runtime.bootstrap import (
    build_runtime_container,
)
from tg_agent.runtime.bootstrap import (
    build_worker_container as build_worker_container,
)
from tg_agent.runtime.bootstrap import (
    load_telegram_credentials as load_telegram_credentials,
)
from tg_agent.runtime.bootstrap import (
    start_container as start_container,
)
from tg_agent.runtime.bootstrap import (
    stop_container as stop_container,
)
from tg_agent.runtime.container import RuntimeContainer
from tg_agent.runtime.log_handler import LogBuffer
from tg_agent.web.container import AppContainer
from tg_agent.web.paths import TEMPLATES_DIR
from tg_agent.web.template_globals import configure_template_globals
from tg_agent.web.timing import TimingBuffer


async def build_container(config: AppConfig, *, log_buffer: LogBuffer) -> AppContainer:
    return await build_web_container(config, log_buffer=log_buffer)


async def build_web_container(config: AppConfig, *, log_buffer: LogBuffer) -> AppContainer:
    return cast(AppContainer, await build_container_with_templates(
        config, log_buffer=log_buffer, templates=None, runtime_mode="web"
    ))


async def build_container_with_templates(
    config: AppConfig,
    *,
    log_buffer: LogBuffer,
    timing_buffer: TimingBuffer | None = None,
    templates: Jinja2Templates | None,
    runtime_mode: str = "web",
) -> AppContainer | RuntimeContainer:
    runtime = await build_runtime_container(config, log_buffer=log_buffer, runtime_mode=runtime_mode)
    if runtime_mode == "worker":
        return runtime
    presentation = configure_template_globals(
        templates or Jinja2Templates(directory=str(TEMPLATES_DIR)), config
    )
    return AppContainer(
        **{field.name: getattr(runtime, field.name) for field in fields(runtime)},
        templates=presentation,
        timing_buffer=timing_buffer,
    )
