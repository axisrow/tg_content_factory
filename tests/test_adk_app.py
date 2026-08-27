"""Tests for the native Google ADK dev UI/eval entrypoint."""

from __future__ import annotations

import importlib.util
import os
import sqlite3
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace

from src.agent.prompt_template import DEFAULT_AGENT_PROMPT_TEMPLATE


def test_adk_app_exports_root_agent(monkeypatch):
    captured: dict = {}

    def fake_load_config(path):
        captured["config_path"] = path
        return SimpleNamespace(database=SimpleNamespace(path=":memory:"))

    def fake_build(config, *, config_path, client_pool=None, system_prompt=""):
        captured.update(
            config=config,
            config_path=config_path,
            client_pool=client_pool,
            system_prompt=system_prompt,
        )
        return object()

    config_module = ModuleType("src.config")
    config_module.AppConfig = object
    config_module.load_config = fake_load_config
    backend_module = ModuleType("src.agent.adk_backend")
    backend_module.build_adk_agent = fake_build
    monkeypatch.setitem(sys.modules, "src.config", config_module)
    monkeypatch.setitem(sys.modules, "src.agent.adk_backend", backend_module)
    monkeypatch.setenv("TG_CONFIG_PATH", "/tmp/tg-agent-test.yaml")

    entrypoint = Path(__file__).parents[1] / "adk" / "tg_content_factory" / "agent.py"
    spec = importlib.util.spec_from_file_location("tg_content_factory_adk_agent", entrypoint)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    assert module.root_agent is not None
    assert captured["config_path"] == "/tmp/tg-agent-test.yaml"
    assert captured["config"].database.path == ":memory:"
    assert captured["client_pool"] is None
    assert captured["system_prompt"] == DEFAULT_AGENT_PROMPT_TEMPLATE


def test_adk_app_loads_saved_prompt_from_sqlite(tmp_path, monkeypatch):
    config_module = ModuleType("src.config")
    config_module.AppConfig = object
    config_module.load_config = lambda _path: SimpleNamespace(
        database=SimpleNamespace(path=str(tmp_path / "agent.db"))
    )
    backend_module = ModuleType("src.agent.adk_backend")
    backend_module.build_adk_agent = lambda config, **kwargs: object()
    monkeypatch.setitem(sys.modules, "src.config", config_module)
    monkeypatch.setitem(sys.modules, "src.agent.adk_backend", backend_module)
    monkeypatch.setenv("TG_CONFIG_PATH", str(tmp_path / "config.yaml"))

    db_path = tmp_path / "agent.db"
    with sqlite3.connect(db_path) as connection:
        connection.execute("CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
        connection.execute(
            "INSERT INTO settings (key, value) VALUES (?, ?)",
            ("agent_prompt_template", "Custom {date} {source_messages}"),
        )

    entrypoint = Path(__file__).parents[1] / "adk" / "tg_content_factory" / "agent.py"
    spec = importlib.util.spec_from_file_location("tg_content_factory_adk_agent_prompt", entrypoint)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    assert module.root_agent is not None
    assert module._configured_prompt(config_module.load_config("unused")).startswith("Custom ")
    assert "{source_messages}" not in module._configured_prompt(config_module.load_config("unused"))


def test_adk_app_loads_dotenv_before_config(tmp_path, monkeypatch):
    config_path = tmp_path / "config.yaml"
    config_path.write_text("agent:\n  model: ${ADK_TEST_MODEL}\n", encoding="utf-8")
    (tmp_path / ".env").write_text("ADK_TEST_MODEL=from-dotenv\n", encoding="utf-8")
    monkeypatch.delenv("ADK_TEST_MODEL", raising=False)
    monkeypatch.setenv("TG_CONFIG_PATH", str(config_path))

    captured: dict[str, str] = {}
    config_module = ModuleType("src.config")
    config_module.AppConfig = object

    def fake_load_config(path):
        captured["path"] = path
        captured["model"] = os.environ.get("ADK_TEST_MODEL", "")
        return SimpleNamespace(database=SimpleNamespace(path=":memory:"))

    config_module.load_config = fake_load_config
    backend_module = ModuleType("src.agent.adk_backend")
    backend_module.build_adk_agent = lambda config, **kwargs: object()
    monkeypatch.setitem(sys.modules, "src.config", config_module)
    monkeypatch.setitem(sys.modules, "src.agent.adk_backend", backend_module)

    entrypoint = Path(__file__).parents[1] / "adk" / "tg_content_factory" / "agent.py"
    spec = importlib.util.spec_from_file_location("tg_content_factory_adk_agent_dotenv", entrypoint)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    assert captured == {"path": str(config_path), "model": "from-dotenv"}
