from src.config import AppConfig, load_config, resolve_session_encryption_secret


def test_default_config():
    config = AppConfig()
    assert config.web.port == 8080
    assert config.scheduler.collect_interval_minutes == 60
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
    assert config.agent.fallback_model == ""
    assert config.agent.fallback_api_key == ""


def test_load_config_reads_telegram_credentials_directly_from_env_without_placeholders(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("TG_API_ID", "77777")
    monkeypatch.setenv("TG_API_HASH", "hash-from-env")
    config_file = tmp_path / "test_config.yaml"
    config_file.write_text("web:\n  port: 9090\n")

    config = load_config(config_file)

    assert config.web.port == 9090
    assert config.telegram.api_id == 77777
    assert config.telegram.api_hash == "hash-from-env"


def test_load_config_reads_telegram_credentials_from_env_when_config_missing(monkeypatch, tmp_path):
    monkeypatch.setenv("TG_API_ID", "88888")
    monkeypatch.setenv("TG_API_HASH", "missing-file-hash")

    config = load_config(tmp_path / "missing.yaml")

    assert config.telegram.api_id == 88888
    assert config.telegram.api_hash == "missing-file-hash"


def test_load_config_reads_agent_fallback_directly_from_env_without_placeholders(monkeypatch, tmp_path):
    monkeypatch.setenv("AGENT_MODEL", "claude-sonnet-4-5")
    monkeypatch.setenv("AGENT_FALLBACK_MODEL", "openai:gpt-4.1-mini")
    monkeypatch.setenv("AGENT_FALLBACK_API_KEY", "fallback-key")

    config = load_config(tmp_path / "missing.yaml")

    assert config.agent.model == "claude-sonnet-4-5"
    assert config.agent.fallback_model == "openai:gpt-4.1-mini"
    assert config.agent.fallback_api_key == "fallback-key"


def test_resolve_session_encryption_secret_prefers_explicit_key():
    config = AppConfig()
    config.security.session_encryption_key = "explicit-session-key"
    config.web.password = "web-pass"
    assert resolve_session_encryption_secret(config) == "explicit-session-key"


def test_resolve_session_encryption_secret_does_not_fallback_to_web_pass():
    config = AppConfig()
    config.web.password = "web-pass"
    assert resolve_session_encryption_secret(config) is None
