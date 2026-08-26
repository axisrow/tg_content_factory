"""Tests for the native Google ADK dev UI/eval entrypoint."""

from __future__ import annotations

import importlib
import sys
from types import ModuleType

from src.agent.prompt_template import DEFAULT_AGENT_PROMPT_TEMPLATE


def test_adk_app_exports_root_agent(monkeypatch):
    captured: dict = {}

    def fake_load_config(path):
        captured["config_path"] = path
        return "config"

    def fake_build(config, *, config_path, client_pool=None, system_prompt=""):
        captured.update(
            config=config,
            config_path=config_path,
            client_pool=client_pool,
            system_prompt=system_prompt,
        )
        return object()

    config_module = ModuleType("src.config")
    config_module.load_config = fake_load_config
    backend_module = ModuleType("src.agent.adk_backend")
    backend_module.build_adk_agent = fake_build
    monkeypatch.setitem(sys.modules, "src.config", config_module)
    monkeypatch.setitem(sys.modules, "src.agent.adk_backend", backend_module)
    monkeypatch.setenv("TG_CONFIG_PATH", "/tmp/tg-agent-test.yaml")
    sys.modules.pop("src.agent.agent", None)

    module = importlib.import_module("src.agent.agent")

    assert module.root_agent is not None
    assert captured == {
        "config_path": "/tmp/tg-agent-test.yaml",
        "config": "config",
        "client_pool": None,
        "system_prompt": DEFAULT_AGENT_PROMPT_TEMPLATE,
    }
