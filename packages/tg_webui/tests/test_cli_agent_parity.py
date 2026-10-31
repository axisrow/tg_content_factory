"""Guard the documented CLI / Web / Agent parity contract."""
# Web-panel tests split out of tests/test_cli_agent_parity.py (#1720 wave 3).


from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]  # repo root (packages/tg_webui/tests/…)


def _actual_web_routes() -> set[tuple[str, str]]:
    # Introspect via OpenAPI instead of app.routes: newer Starlette nests
    # feature routes inside an unnamed _IncludedRouter per include_router(), so a
    # flat walk of app.routes misses them. app.openapi()["paths"] expands every
    # included router, exposing the documented (in-schema) endpoints with methods.
    from tg_webui.app import create_app

    routes: set[tuple[str, str]] = set()
    for path, operations in create_app().openapi().get("paths", {}).items():
        for method in operations:
            routes.add((method.upper(), path))
    return routes


def _documented_web_routes() -> list[tuple[str, str, str]]:
    routes: list[tuple[str, str, str]] = []
    for line in (ROOT / "docs/reference/parity.md").read_text(encoding="utf-8").splitlines():
        if not line.startswith("|") or line.startswith("|---") or "Операция" in line:
            continue
        cells = [cell.strip() for cell in line.strip("|").split("|")]
        if len(cells) < 4:
            continue
        operation = cells[0]
        web_cell = cells[2]
        if "исключение" in web_cell:
            continue
        for endpoint in re.findall(r"`([^`]+)`", web_cell):
            for method, path in _expand_web_endpoint(endpoint):
                if "*" not in path:
                    routes.append((operation, method, path.split("?", 1)[0]))
    return routes


def _expand_web_endpoint(endpoint: str) -> list[tuple[str, str]]:
    if endpoint.startswith("GET/POST "):
        path = endpoint.removeprefix("GET/POST ").strip()
        return [("GET", path), ("POST", path)]
    match = re.match(r"^(GET|POST|DELETE|PUT|PATCH)\s+(.+)$", endpoint)
    if match is None:
        return []
    return [(match.group(1), match.group(2).strip())]


def _route_exists(actual_routes: set[tuple[str, str]], method: str, documented_path: str) -> bool:
    return any(
        actual_method == method and _paths_match(actual_path, documented_path)
        for actual_method, actual_path in actual_routes
    )


def _paths_match(actual_path: str, documented_path: str) -> bool:
    actual_parts = actual_path.strip("/").split("/") if actual_path.strip("/") else []
    documented_parts = documented_path.strip("/").split("/") if documented_path.strip("/") else []
    if len(actual_parts) != len(documented_parts):
        return False
    # A segment matches only when it is literally equal, or BOTH sides are path
    # params. Treating a single param segment as a wildcard (e.g. documented
    # {id} vs actual static "notifications") would let an unrelated static
    # sibling satisfy a removed dynamic route, false-greening the guard.
    return all(
        actual == documented or (_is_path_param(actual) and _is_path_param(documented))
        for actual, documented in zip(actual_parts, documented_parts, strict=True)
    )


def _is_path_param(part: str) -> bool:
    return part.startswith("{") and part.endswith("}")


def test_documented_web_endpoints_exist_in_app_routes():
    actual_routes = _actual_web_routes()
    missing_routes = [
        (operation, f"{method} {path}")
        for operation, method, path in _documented_web_routes()
        if not _route_exists(actual_routes, method, path)
    ]
    assert missing_routes == []


def test_configure_app_tolerates_unnamed_included_routers():
    """Starlette >= 1.x adds an unnamed _IncludedRouter to app.routes per include_router(),
    which has no .name attribute. configure_app must not crash reading route names while
    deciding whether the static / data-image mounts already exist (FastAPI/Starlette compat)."""
    from fastapi import APIRouter, FastAPI

    from tg_webui.assembly import configure_app

    app = FastAPI()
    extra = APIRouter()

    @extra.get("/_compat_probe")
    def _probe():
        return {}

    app.include_router(extra)  # produces a _IncludedRouter on new Starlette

    # Must not raise AttributeError: '_IncludedRouter' object has no attribute 'name'.
    configure_app(app, None)

    # The static mount is still added (its name is reachable via getattr default).
    assert any(getattr(r, "name", None) == "static" for r in app.routes)
