"""Web presentation state layered on the shared runtime container."""

from __future__ import annotations

from dataclasses import dataclass

from fastapi.templating import Jinja2Templates

from tg_agent.runtime.container import (
    RuntimeClientPool as WebClientPool,
)
from tg_agent.runtime.container import (
    RuntimeCollector as WebCollector,
)
from tg_agent.runtime.container import (
    RuntimeContainer,
)
from tg_agent.runtime.container import (
    RuntimeScheduler as WebScheduler,
)
from tg_agent.web.timing import TimingBuffer

__all__ = ["AppContainer", "WebClientPool", "WebCollector", "WebScheduler"]


@dataclass(slots=True, kw_only=True)
class AppContainer(RuntimeContainer):
    templates: Jinja2Templates
    timing_buffer: TimingBuffer | None
