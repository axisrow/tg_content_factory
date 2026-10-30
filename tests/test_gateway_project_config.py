"""Gateway enrollment keeps project database paths independent."""

import logging
from uuid import uuid4

import pytest

from tg_agent.config import load_config
from tg_agent.gateway.project import ProjectBinding, write_private_json


def test_v2_markers_keep_each_project_database_local(tmp_path, monkeypatch):
    paths = []
    for name in ("a", "b"):
        project = tmp_path / name
        project.mkdir()
        monkeypatch.chdir(project)
        (project / "config.yaml").write_text(f"database:\n  path: {name}.db\n")
        binding = ProjectBinding(project_id=uuid4(), store_root=str(tmp_path / "shared"),
                                 token_file=str(project / "data" / "gateway-token.json"))
        write_private_json(project / ".tg-agent-global", binding.model_dump(mode="json"))
        config = load_config()
        paths.append(config.database.path)
        assert config.gateway["root"] == str(tmp_path / "shared")
    assert paths == ["a.db", "b.db"]


def test_broken_json_marker_is_ignored_with_warning(tmp_path, monkeypatch, caplog):
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".tg-agent-global").write_text("{broken")
    with caplog.at_level(logging.WARNING):
        config = load_config()
    assert config.gateway == {}
    assert config.database.path == "data/tg_search.db"
    assert "not valid JSON" in caplog.text


def test_unknown_marker_version_is_refused(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".tg-agent-global").write_text('{"version":3}')
    with pytest.raises(ValueError):
        load_config()


def test_enrolled_project_cannot_construct_local_telegram_pool(tmp_path, monkeypatch):
    from tg_agent.telegram.client_pool import ClientPool

    monkeypatch.chdir(tmp_path)
    binding = ProjectBinding(project_id=uuid4(), store_root=str(tmp_path / "gateway"),
                             token_file=str(tmp_path / "data" / "gateway.json"))
    write_private_json(tmp_path / ".tg-agent-global", binding.model_dump(mode="json"))
    with pytest.raises(RuntimeError, match="local Telegram ClientPool is forbidden"):
        ClientPool(object(), object())
