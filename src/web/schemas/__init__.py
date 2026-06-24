"""Pydantic response models for the FastAPI REST surface (#1070).

These models describe the JSON shapes returned by the ``/api/*``-style REST
endpoints so that ``/openapi.json`` (and therefore Swagger ``/docs`` + ReDoc)
carries concrete request/response schemas. They are declared as ``response_model``
on the corresponding routes.

The routes themselves still return ``JSONResponse`` (raw dict/list) for backward
compatibility and to avoid re-validating hand-built payloads on the hot path — so
these models drive the *documentation*, not runtime serialization. They are kept
faithful to the actual payload shapes (mostly snake_case, nullable where the
underlying query/service can produce ``None``).

The shared ``OPENAPI_TAGS`` list groups operations in Swagger.
"""

from __future__ import annotations

# OpenAPI tag groups — surfaced in the Swagger sidebar. Keeping them here keeps
# the tag vocabulary consistent across routers.
OPENAPI_TAGS: list[dict[str, str]] = [
    {"name": "health", "description": "Liveness/readiness probe."},
    {"name": "accounts", "description": "Telegram account status and diagnostics (REST/JSON)."},
    {"name": "agent", "description": "Agent threads and channel pickers (REST/JSON)."},
    {"name": "search-queries", "description": "Saved search queries (REST/JSON)."},
    {"name": "analytics", "description": "Content/channel/trend analytics (REST/JSON)."},
    {"name": "calendar", "description": "Content publication calendar (REST/JSON)."},
    {"name": "pipelines", "description": "Content pipelines: details, runs, queue (REST/JSON)."},
    {"name": "dialogs", "description": "Telegram dialog data: participants, broadcast stats (REST/JSON)."},
]
