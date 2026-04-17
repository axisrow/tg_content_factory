from __future__ import annotations

from pathlib import Path


def test_web_routes_do_not_call_live_pool_methods() -> None:
    routes_dir = Path("src/web/routes")
    disallowed = (
        "request.app.state.pool.add_client",
        "request.app.state.pool.get_client_by_phone",
        "request.app.state.pool.get_native_client_by_phone",
        "pool.add_client(",
        "pool.get_client_by_phone(",
        "pool.get_native_client_by_phone(",
    )

    offenders: list[str] = []
    for path in sorted(routes_dir.glob("*.py")):
        if path.name == "__init__.py":
            continue
        text = path.read_text(encoding="utf-8")
        for token in disallowed:
            if token in text:
                offenders.append(f"{path}: {token}")

    assert offenders == []
