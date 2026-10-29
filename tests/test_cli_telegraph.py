"""Offline CLI tests for the ``telegraph`` group (#1703).

No network: every test drives the production Typer app through ``CliRunner``
and swaps the service's ``aiohttp.ClientSession`` for a fake transport. The
account round-trip goes through a real file-backed SQLite in ``tmp_path`` —
``login`` stores the token via ``set_secret`` (config carries a test
``session_encryption_key``), ``create`` reads it back with ``get_secret``.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from tg_agent.cli.typer_app import app
from tg_agent.services import telegraph_service

runner = CliRunner()


class _FakeResponse:
    """Minimal aiohttp response stand-in (see tests/test_telegraph_service.py)."""

    def __init__(self, *, status: int = 200, payload: dict | None = None):
        self.status = status
        self._payload = payload if payload is not None else {}

    async def text(self) -> str:
        return json.dumps(self._payload)

    async def json(self, content_type: str | None = None) -> dict:
        return self._payload

    async def __aenter__(self) -> "_FakeResponse":
        return self

    async def __aexit__(self, *exc_info: object) -> bool:
        return False


class _FakeSession:
    """Returns the queued responses in order; records every POST form."""

    def __init__(self, responses: list[_FakeResponse]):
        self._responses = list(responses)
        self.posts: list[dict] = []

    def post(self, url: str, *, data: dict | None = None, **kwargs: object) -> _FakeResponse:
        self.posts.append({"url": url, "data": data})
        return self._responses.pop(0)

    async def __aenter__(self) -> "_FakeSession":
        return self

    async def __aexit__(self, *exc_info: object) -> bool:
        return False


def _isolate(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "config.yaml").write_text(
        "database:\n"
        f"  path: {tmp_path / 'local.db'}\n"
        "session_encryption_key: test-key\n",
        encoding="utf-8",
    )


def _patch(monkeypatch: pytest.MonkeyPatch, *responses: _FakeResponse) -> _FakeSession:
    session = _FakeSession(list(responses))
    monkeypatch.setattr(
        telegraph_service.aiohttp, "ClientSession", lambda timeout=None: session
    )
    return session


def _ok_page(path: str = "Test-10-30") -> _FakeResponse:
    return _FakeResponse(
        payload={
            "ok": True,
            "result": {"path": path, "url": f"https://telegra.ph/{path}"},
        }
    )


def _seed_token_via_settings(tmp_path: Path, token: str = "tok-1") -> None:
    """Store the token the documented way — plain ``settings set`` (the repo
    reads it back with ``get_secret``, legacy plaintext passes through)."""
    result = runner.invoke(
        app, ["--config", str(tmp_path / "config.yaml"), "settings", "set",
              telegraph_service.ACCESS_TOKEN_SETTING, token]
    )
    assert result.exit_code == 0


# --------------------------------------------------------------------------- #
# login — createAccount + token storage
# --------------------------------------------------------------------------- #


def test_login_stores_token_and_create_roundtrip(tmp_path, monkeypatch):
    _isolate(tmp_path, monkeypatch)
    session = _patch(
        monkeypatch,
        _FakeResponse(
            payload={"ok": True, "result": {"access_token": "tok-1", "short_name": "Клуб"}}
        ),
        _ok_page(),
    )

    login = runner.invoke(
        app,
        ["telegraph", "login", "--short-name", "Клуб", "--author-url", "https://t.me/x"],
    )
    assert login.exit_code == 0, login.output
    assert "telegraph_access_token" in login.stdout
    assert "tok-1" not in login.stdout  # the token is never echoed

    create = runner.invoke(
        app, ["telegraph", "create", "--title", "Т"], input="текст статьи"
    )
    assert create.exit_code == 0, create.output
    assert "https://telegra.ph/Test-10-30" in create.stdout

    account_form, page_form = session.posts[0]["data"], session.posts[1]["data"]
    assert account_form["short_name"] == "Клуб"
    assert page_form["access_token"] == "tok-1"  # stored by login, used by create


def test_login_without_encryption_key_fails(tmp_path, monkeypatch):
    _isolate(tmp_path, monkeypatch)
    (tmp_path / "config.yaml").write_text(
        f"database:\n  path: {tmp_path / 'local.db'}\n", encoding="utf-8"
    )
    # conftest.py sets a shared test key for every test — remove it explicitly.
    monkeypatch.delenv("SESSION_ENCRYPTION_KEY", raising=False)
    _patch(
        monkeypatch,
        _FakeResponse(payload={"ok": True, "result": {"access_token": "tok-1"}}),
    )

    result = runner.invoke(app, ["telegraph", "login", "--short-name", "Клуб"])
    assert result.exit_code == 1
    assert "SESSION_ENCRYPTION_KEY" in result.stderr


def test_login_rejects_oversized_short_name(tmp_path, monkeypatch):
    _isolate(tmp_path, monkeypatch)
    session = _patch(monkeypatch)

    result = runner.invoke(app, ["telegraph", "login", "--short-name", "И" * 33])
    assert result.exit_code == 1
    assert "32" in result.stderr
    assert session.posts == []


# --------------------------------------------------------------------------- #
# create — token required, field limits, error paths
# --------------------------------------------------------------------------- #


def test_create_without_account_hints_login(tmp_path, monkeypatch):
    _isolate(tmp_path, monkeypatch)
    session = _patch(monkeypatch, _ok_page())

    result = runner.invoke(app, ["telegraph", "create", "--title", "Т"], input="текст")
    assert result.exit_code == 1
    assert "telegraph login" in result.stderr
    assert session.posts == []  # no network before the account exists


def test_create_from_stdin_prints_url(tmp_path, monkeypatch):
    _isolate(tmp_path, monkeypatch)
    session = _patch(monkeypatch, _ok_page())
    _seed_token_via_settings(tmp_path)

    result = runner.invoke(
        app, ["telegraph", "create", "--title", "Заголовок"], input="текст"
    )
    assert result.exit_code == 0
    assert "https://telegra.ph/Test-10-30" in result.stdout

    form = session.posts[0]["data"]
    assert form["title"] == "Заголовок"
    assert json.loads(form["content"])[0]["children"] == ["текст"]


def test_create_from_file_with_json_format(tmp_path, monkeypatch):
    _isolate(tmp_path, monkeypatch)
    _patch(monkeypatch, _ok_page())
    _seed_token_via_settings(tmp_path)
    article = tmp_path / "article.md"
    article.write_text("# Статья\n\nТекст со ссылкой: https://example.com\n", encoding="utf-8")

    result = runner.invoke(
        app,
        [
            "telegraph",
            "create",
            "--title",
            "Статья",
            "--file",
            str(article),
            "--author-name",
            "Клуб",
            "--format",
            "json",
        ],
    )
    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["ok"] is True
    assert payload["result"]["url"] == "https://telegra.ph/Test-10-30"
    assert payload["result"]["path"] == "Test-10-30"


def test_create_stdin_empty_body_fails(tmp_path, monkeypatch):
    _isolate(tmp_path, monkeypatch)
    _patch(monkeypatch, _ok_page())
    _seed_token_via_settings(tmp_path)

    result = runner.invoke(app, ["telegraph", "create", "--title", "Т"], input="  \n ")
    assert result.exit_code == 1
    assert "empty" in result.stderr


def test_create_missing_file_fails(tmp_path, monkeypatch):
    _isolate(tmp_path, monkeypatch)
    _patch(monkeypatch, _ok_page())
    _seed_token_via_settings(tmp_path)

    result = runner.invoke(
        app,
        ["telegraph", "create", "--title", "Т", "--file", str(tmp_path / "nope.md")],
    )
    assert result.exit_code == 1
    assert "cannot read" in result.stderr


def test_create_rejects_oversized_title(tmp_path, monkeypatch):
    _isolate(tmp_path, monkeypatch)
    session = _patch(monkeypatch, _ok_page())
    _seed_token_via_settings(tmp_path)

    result = runner.invoke(
        app, ["telegraph", "create", "--title", "Т" * 257], input="текст"
    )
    assert result.exit_code == 1
    assert "256" in result.stderr
    assert session.posts == []  # rejected before the network call


def test_create_rejects_oversized_author_fields(tmp_path, monkeypatch):
    _isolate(tmp_path, monkeypatch)
    _patch(monkeypatch, _ok_page())
    _seed_token_via_settings(tmp_path)

    result = runner.invoke(
        app,
        ["telegraph", "create", "--title", "Т", "--author-name", "И" * 129],
        input="текст",
    )
    assert result.exit_code == 1
    assert "128" in result.stderr

    result = runner.invoke(
        app,
        ["telegraph", "create", "--title", "Т", "--author-url", "https://" + "u" * 512],
        input="текст",
    )
    assert result.exit_code == 1
    assert "512" in result.stderr


def test_create_api_error_exits_nonzero(tmp_path, monkeypatch):
    _isolate(tmp_path, monkeypatch)
    _patch(
        monkeypatch,
        _FakeResponse(payload={"ok": False, "error": "ACCESS_TOKEN_INVALID"}),
    )
    _seed_token_via_settings(tmp_path)

    result = runner.invoke(app, ["telegraph", "create", "--title", "Т"], input="текст")
    assert result.exit_code == 1
    assert "ACCESS_TOKEN_INVALID" in result.stderr


def test_create_http_error_exits_nonzero(tmp_path, monkeypatch):
    _isolate(tmp_path, monkeypatch)
    _patch(monkeypatch, _FakeResponse(status=500, payload={}))
    _seed_token_via_settings(tmp_path)

    result = runner.invoke(app, ["telegraph", "create", "--title", "Т"], input="текст")
    assert result.exit_code == 1
    assert "500" in result.stderr
