"""Web-side container builders: the core runtime container plus panel templates.

The runtime container itself (services, pool, dispatchers, start/stop) lives in
the core (``tg_agent.runtime.bootstrap``) so the worker never depends on the
web surface (#1720); this module only attaches the Jinja2 templates.
"""

from __future__ import annotations

from fastapi.templating import Jinja2Templates

from tg_agent.config import AppConfig
from tg_agent.runtime.bootstrap import build_runtime_container
from tg_agent.runtime.container import AppContainer
from tg_agent.runtime.log_buffer import LogBuffer
from tg_agent.utils.timing import TimingBuffer
from tg_agent.web.paths import TEMPLATES_DIR
from tg_agent.web.template_globals import configure_template_globals


async def build_container(config: AppConfig, *, log_buffer: LogBuffer) -> AppContainer:
    return await build_web_container(config, log_buffer=log_buffer)


async def build_web_container(config: AppConfig, *, log_buffer: LogBuffer) -> AppContainer:
    return await build_container_with_templates(
        config,
        log_buffer=log_buffer,
        templates=None,
        runtime_mode="web",
    )


async def build_container_with_templates(
    config: AppConfig,
    *,
    log_buffer: LogBuffer,
    timing_buffer: TimingBuffer | None = None,
    templates: Jinja2Templates | None,
    runtime_mode: str = "web",
) -> AppContainer:
    container = await build_runtime_container(
        config,
        log_buffer=log_buffer,
        timing_buffer=timing_buffer,
        runtime_mode=runtime_mode,
    )
    container.templates = configure_template_globals(
        templates or Jinja2Templates(directory=str(TEMPLATES_DIR)),
        config,
    )
    return container
