"""Integration tests for the TG Post Search application.

Tests cover full request cycles through the web layer including
auth, CRUD for channels/keywords, search, scheduler, and collector logic.
"""

from __future__ import annotations

from tg_agent.config import load_config

# ===================================================================
# Priority 2 — Important
# ===================================================================


class TestConfigEnvChain:
    """Full chain: .env → config.yaml → AppConfig."""

    def test_env_substitution_full_chain(self, tmp_path, monkeypatch):
        monkeypatch.setenv("TG_API_ID", "99999")
        monkeypatch.setenv("TG_API_HASH", "abc123hash")
        monkeypatch.setenv("WEB_PASS", "secret123")

        config_file = tmp_path / "config.yaml"
        config_file.write_text(
            "telegram:\n"
            "  api_id: ${TG_API_ID}\n"
            "  api_hash: ${TG_API_HASH}\n"
            "web:\n"
            "  password: ${WEB_PASS}\n"
        )
        config = load_config(config_file)

        assert config.telegram.api_id == 99999
        assert config.telegram.api_hash == "abc123hash"
        assert config.web.password == "secret123"

    def test_empty_env_drops_keys_uses_defaults(self, tmp_path, monkeypatch):
        monkeypatch.delenv("TG_API_ID", raising=False)
        monkeypatch.delenv("TG_API_HASH", raising=False)
        monkeypatch.delenv("WEB_PASS", raising=False)

        config_file = tmp_path / "config.yaml"
        config_file.write_text(
            "telegram:\n"
            "  api_id: ${TG_API_ID}\n"
            "  api_hash: ${TG_API_HASH}\n"
            "web:\n"
            "  password: ${WEB_PASS}\n"
        )
        config = load_config(config_file)

        assert config.telegram.api_id == 0
        assert config.telegram.api_hash == ""
        assert config.web.password == ""

    def test_partial_env_mix(self, tmp_path, monkeypatch):
        monkeypatch.setenv("TG_API_ID", "11111")
        monkeypatch.delenv("TG_API_HASH", raising=False)

        config_file = tmp_path / "config.yaml"
        config_file.write_text(
            "telegram:\n" "  api_id: ${TG_API_ID}\n" "  api_hash: ${TG_API_HASH}\n"
        )
        config = load_config(config_file)

        assert config.telegram.api_id == 11111
        assert config.telegram.api_hash == ""
