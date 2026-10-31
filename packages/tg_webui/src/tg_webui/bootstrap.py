"""Presentation assembly for the legacy web app: the core runtime container plus templates."""

from __future__ import annotations

from dataclasses import fields

from fastapi.templating import Jinja2Templates

from tg_agent.config import AppConfig
from tg_agent.runtime.bootstrap import build_runtime_container
from tg_agent.runtime.log_handler import LogBuffer
from tg_agent.utils.timing import TimingBuffer
from tg_webui.container import AppContainer
from tg_webui.paths import TEMPLATES_DIR
from tg_webui.template_globals import configure_template_globals


async def build_container(config: AppConfig, *, log_buffer: LogBuffer) -> AppContainer:
    return await build_web_container(config, log_buffer=log_buffer)


async def build_web_container(config: AppConfig, *, log_buffer: LogBuffer) -> AppContainer:
    return await build_container_with_templates(config, log_buffer=log_buffer, templates=None)


async def build_container_with_templates(
    config: AppConfig,
    *,
    log_buffer: LogBuffer,
    timing_buffer: TimingBuffer | None = None,
    templates: Jinja2Templates | None,
) -> AppContainer:
    runtime = await build_runtime_container(config, log_buffer=log_buffer, runtime_mode="web")
    presentation = configure_template_globals(
        templates or Jinja2Templates(directory=str(TEMPLATES_DIR)), config
    )
    return AppContainer(
        **{field.name: getattr(runtime, field.name) for field in fields(runtime)},
        templates=presentation,
        timing_buffer=timing_buffer,
    )
