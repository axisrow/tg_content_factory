from pathlib import Path

import pytest
import yaml

from src.config import AppConfig, load_config, parse_proxy_url, resolve_session_encryption_secret


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
    config_file.write_text("telegram:\n  api_id: ${TG_API_ID}\n  api_hash: ${TG_API_HASH}\n")
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


# --- TG_PROXY parsing (parse_proxy_url / load_config override) ---


def test_parse_proxy_url_socks5_with_auth_unquotes_credentials():
    proxy = parse_proxy_url("socks5://tg:p%40ss@203.0.113.10:1080")
    assert proxy == {
        "proxy_type": "socks5",
        "addr": "203.0.113.10",
        "port": 1080,
        "rdns": True,
        "username": "tg",
        "password": "p@ss",
    }


def test_parse_proxy_url_socks5_no_auth_omits_credentials():
    proxy = parse_proxy_url("socks5://127.0.0.1:9050")
    assert proxy == {"proxy_type": "socks5", "addr": "127.0.0.1", "port": 9050, "rdns": True}


def test_parse_proxy_url_one_sided_credentials_set_only_present_field():
    # A blank username must not reach python-socks as username="" — that would
    # start a doomed SOCKS5 auth instead of passing just the password.
    assert parse_proxy_url("socks5://:s3cret@h:1080") == {
        "proxy_type": "socks5",
        "addr": "h",
        "port": 1080,
        "rdns": True,
        "password": "s3cret",
    }
    assert parse_proxy_url("socks5://uer@h:1080") == {
        "proxy_type": "socks5",
        "addr": "h",
        "port": 1080,
        "rdns": True,
        "username": "uer",
    }


def test_parse_proxy_url_http_defaults_port():
    assert parse_proxy_url("http://proxy.local") == {
        "proxy_type": "http",
        "addr": "proxy.local",
        "port": 8080,
        "rdns": True,
    }


def test_parse_proxy_url_empty_returns_none():
    assert parse_proxy_url("") is None
    assert parse_proxy_url("   ") is None


def test_parse_proxy_url_rejects_bad_scheme_and_missing_host():
    with pytest.raises(ValueError, match="TG_PROXY"):
        parse_proxy_url("ftp://h:1")
    with pytest.raises(ValueError, match="TG_PROXY"):
        parse_proxy_url("socks5://")


def test_load_config_tg_proxy_env_override(tmp_path, monkeypatch):
    monkeypatch.setenv("TG_PROXY", "socks5://u:p@h:1080")
    config = load_config(tmp_path / "missing.yaml")
    assert config.telegram_runtime.proxy == {
        "proxy_type": "socks5",
        "addr": "h",
        "port": 1080,
        "rdns": True,
        "username": "u",
        "password": "p",
    }


def test_load_config_tg_proxy_unset_is_none(tmp_path, monkeypatch):
    monkeypatch.delenv("TG_PROXY", raising=False)
    config = load_config(tmp_path / "missing.yaml")
    assert config.telegram_runtime.proxy is None


def test_load_config_tg_proxy_malformed_fails_fast(tmp_path, monkeypatch):
    monkeypatch.setenv("TG_PROXY", "ftp://h:1")
    with pytest.raises(ValueError, match="TG_PROXY"):
        load_config(tmp_path / "missing.yaml")


def test_load_config_reads_telegram_credentials_from_env_when_config_missing(monkeypatch, tmp_path):
    monkeypatch.setenv("TG_API_ID", "88888")
    monkeypatch.setenv("TG_API_HASH", "missing-file-hash")

    config = load_config(tmp_path / "missing.yaml")

    assert config.telegram.api_id == 88888
    assert config.telegram.api_hash == "missing-file-hash"


def test_load_config_reads_agent_fallback_directly_from_env_without_placeholders(
    monkeypatch, tmp_path
):
    monkeypatch.setenv("AGENT_MODEL", "claude-sonnet-4-6")
    monkeypatch.setenv("AGENT_FALLBACK_MODEL", "openai:gpt-4.1-mini")
    monkeypatch.setenv("AGENT_FALLBACK_API_KEY", "fallback-key")

    config = load_config(tmp_path / "missing.yaml")

    assert config.agent.model == "claude-sonnet-4-6"
    assert config.agent.fallback_model == "openai:gpt-4.1-mini"
    assert config.agent.fallback_api_key == "fallback-key"


def test_load_config_warns_on_invalid_agent_fallback_model(monkeypatch, tmp_path, caplog):
    monkeypatch.setenv("AGENT_FALLBACK_MODEL", "llama3")
    caplog.set_level("WARNING")

    config = load_config(tmp_path / "missing.yaml")

    assert config.agent.fallback_model == "llama3"
    assert "Invalid AGENT_FALLBACK_MODEL" in caplog.text


def test_shipped_config_yaml_web_host_overridable_via_env(monkeypatch):
    """The repo's own config.yaml must let WEB_HOST override the loopback
    default, or Docker (which bind-mounts this exact file) can never expose
    the panel outside its network namespace (#1305 follow-up)."""
    monkeypatch.setenv("WEB_HOST", "0.0.0.0")
    config = load_config("config.yaml")
    assert config.web.host == "0.0.0.0"


def test_shipped_config_yaml_web_host_defaults_to_loopback(monkeypatch):
    """Without WEB_HOST set, native execution keeps the safe loopback default."""
    monkeypatch.delenv("WEB_HOST", raising=False)
    config = load_config("config.yaml")
    assert config.web.host == "127.0.0.1"


def test_docker_compose_overrides_web_host_for_container_network():
    """docker-compose.yml must set WEB_HOST=0.0.0.0 — otherwise the shared
    config.yaml's loopback default (#1303) makes the panel unreachable via
    the published port, while the in-container healthcheck still passes and
    masks the outage (#1305 follow-up)."""
    compose_path = Path(__file__).resolve().parent.parent / "docker-compose.yml"
    compose = yaml.safe_load(compose_path.read_text())
    service = next(iter(compose["services"].values()))
    env = service.get("environment", [])
    assert "WEB_HOST=0.0.0.0" in env, (
        "docker-compose.yml must export WEB_HOST=0.0.0.0 or the mounted "
        "config.yaml's loopback default breaks the published port mapping"
    )


def test_resolve_session_encryption_secret_prefers_explicit_key():
    config = AppConfig()
    config.security.session_encryption_key = "explicit-session-key"
    config.web.password = "web-pass"
    assert resolve_session_encryption_secret(config) == "explicit-session-key"


def test_resolve_session_encryption_secret_does_not_fallback_to_web_pass():
    config = AppConfig()
    config.web.password = "web-pass"
    assert resolve_session_encryption_secret(config) is None
