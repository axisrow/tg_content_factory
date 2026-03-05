
from src.config import AppConfig, load_config


def test_default_config():
    config = AppConfig()
    assert config.web.port == 8080
    assert config.scheduler.collect_interval_minutes == 30
    assert config.database.path == "data/tg_search.db"
    assert config.llm.enabled is False


def test_load_config_missing_file(tmp_path):
    config = load_config(tmp_path / "nonexistent.yaml")
    assert config.web.port == 8080


def test_load_config_with_env_substitution(tmp_path, monkeypatch):
    monkeypatch.setenv("TG_API_ID", "12345")
    monkeypatch.setenv("TG_API_HASH", "abcdef")
    config_file = tmp_path / "test_config.yaml"
    config_file.write_text(
        "telegram:\n  api_id: ${TG_API_ID}\n  api_hash: ${TG_API_HASH}\n"
    )
    config = load_config(config_file)
    assert config.telegram.api_id == 12345
    assert config.telegram.api_hash == "abcdef"


def test_load_config_with_empty_env(tmp_path, monkeypatch):
    """Empty env vars should fall back to Pydantic defaults, not crash."""
    monkeypatch.delenv("TG_API_ID", raising=False)
    monkeypatch.delenv("TG_API_HASH", raising=False)
    monkeypatch.delenv("LLM_API_KEY", raising=False)
    config_file = tmp_path / "test_config.yaml"
    config_file.write_text(
        "telegram:\n  api_id: ${TG_API_ID}\n  api_hash: ${TG_API_HASH}\n"
        "llm:\n  api_key: ${LLM_API_KEY}\n"
    )
    config = load_config(config_file)
    assert config.telegram.api_id == 0
    assert config.telegram.api_hash == ""
    assert config.llm.api_key == ""
