"""OpenAPI completeness contract for the FastAPI REST surface (#1070).

The FastAPI surface (`/api/*`-style JSON/REST routes) is meant for *programmatic*
consumers (interop with other programs), so its `/openapi.json` schema must
describe request/response shapes — otherwise `/docs` (Swagger) shows empty
models and the spec is useless to a client generator.

This module pins, per annotated REST endpoint, that:

1. The operation declares a ``200`` response whose ``application/json`` body
   references a concrete schema component (``$ref``), not an empty/implicit
   ``{}``. FastAPI only emits that ``$ref`` when the route carries a
   ``response_model``; before #1070 none of these routes had one, so the schema
   was empty and these assertions were red.
2. The operation carries a human ``summary`` and is grouped under a ``tags``
   entry, so Swagger lists it sensibly.

HTML/HTMX routes (the human-facing Web surface, ``response_class=HTMLResponse``)
are intentionally excluded — they need no JSON schema.
"""

from __future__ import annotations

import pytest

from src.config import AppConfig
from src.web.app import create_app

# (method, path) of every REST/JSON endpoint annotated under #1070. Paths are the
# fully-mounted forms (router prefix + route path) as they appear in openapi.json.
ANNOTATED_REST_ENDPOINTS: list[tuple[str, str]] = [
    # built-in
    ("get", "/health"),
    # accounts (mounted under /settings)
    ("get", "/settings/flood-status"),
    ("get", "/settings/{account_id}/info"),
    # agent
    ("get", "/agent/threads/{thread_id}/messages"),
    ("get", "/agent/channels-json"),
    # search-queries
    ("get", "/search-queries/{sq_id}"),
    ("get", "/search-queries/{sq_id}/stats"),
    # analytics — content
    ("get", "/analytics/content/api/summary"),
    ("get", "/analytics/content/api/types"),
    ("get", "/analytics/content/api/pipelines"),
    ("get", "/analytics/content/api/daily"),
    # analytics — trends
    ("get", "/analytics/trends/topics"),
    ("get", "/analytics/trends/channels"),
    ("get", "/analytics/trends/emojis"),
    # analytics — channels
    ("get", "/analytics/channels/api/overview"),
    ("get", "/analytics/channels/api/ratings"),
    # analytics — messages / misc
    ("get", "/analytics/messages/top"),
    ("get", "/analytics/messages/hourly"),
    ("get", "/analytics/pipelines/stats"),
    ("get", "/analytics/messages/velocity"),
    ("get", "/analytics/peak-hours"),
    # calendar
    ("get", "/calendar/api/calendar"),
    ("get", "/calendar/api/upcoming"),
    ("get", "/calendar/api/stats"),
    # pipelines (JSON-for-programs)
    ("get", "/pipelines/api/channels/search"),
    ("get", "/pipelines/{pipeline_id}/show"),
    ("get", "/pipelines/{pipeline_id}/runs"),
    ("get", "/pipelines/{pipeline_id}/runs/{run_id}"),
    ("get", "/pipelines/{pipeline_id}/queue"),
    ("get", "/pipelines/templates/json"),
    # dialogs
    ("get", "/dialogs/participants"),
    ("get", "/dialogs/broadcast-stats"),
]


@pytest.fixture(scope="module")
def openapi_spec() -> dict:
    """Generate the OpenAPI document from route declarations alone (no DB/pool)."""
    app = create_app(AppConfig())
    return app.openapi()


def _operation(spec: dict, method: str, path: str) -> dict:
    paths = spec.get("paths", {})
    assert path in paths, f"path {path!r} missing from openapi.json"
    item = paths[path]
    assert method in item, f"{method.upper()} {path} missing from openapi.json"
    return item[method]


@pytest.mark.parametrize("method,path", ANNOTATED_REST_ENDPOINTS)
def test_rest_endpoint_has_response_schema(openapi_spec: dict, method: str, path: str) -> None:
    """Every annotated REST endpoint exposes a concrete 200 JSON response schema."""
    op = _operation(openapi_spec, method, path)
    responses = op.get("responses", {})
    assert "200" in responses, f"{method.upper()} {path}: no 200 response declared"
    content = responses["200"].get("content", {})
    assert "application/json" in content, f"{method.upper()} {path}: 200 is not application/json"
    schema = content["application/json"].get("schema", {})
    # A response_model produces a $ref (object/list models) or a typed schema.
    # An un-annotated JSONResponse route produces an empty/absent schema — red.
    has_concrete_schema = bool(schema) and (
        "$ref" in schema
        or schema.get("type") in {"array", "object"}
        or "items" in schema
        or "anyOf" in schema
        or "allOf" in schema
    )
    assert has_concrete_schema, (
        f"{method.upper()} {path}: 200 response has no concrete schema "
        f"(response_model missing). Got: {schema!r}"
    )


@pytest.mark.parametrize("method,path", ANNOTATED_REST_ENDPOINTS)
def test_rest_endpoint_has_summary_and_tags(openapi_spec: dict, method: str, path: str) -> None:
    """Every annotated REST endpoint carries a Swagger summary and a tag group."""
    op = _operation(openapi_spec, method, path)
    summary = op.get("summary") or ""
    assert summary.strip(), f"{method.upper()} {path}: missing summary"
    tags = op.get("tags") or []
    assert tags, f"{method.upper()} {path}: missing tags"
