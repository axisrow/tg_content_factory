"""Integration tests for configuration loading: .env → config.yaml → AppConfig."""

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
        monkeypatch.setenv("SESSION_ENCRYPTION_KEY", "secret123")

        config_file = tmp_path / "config.yaml"
        config_file.write_text(
            "telegram:\n"
            "  api_id: ${TG_API_ID}\n"
            "  api_hash: ${TG_API_HASH}\n"
            "security:\n"
            "  session_encryption_key: ${SESSION_ENCRYPTION_KEY}\n"
        )
        config = load_config(config_file)

        assert config.telegram.api_id == 99999
        assert config.telegram.api_hash == "abc123hash"
        assert config.security.session_encryption_key == "secret123"

    def test_empty_env_drops_keys_uses_defaults(self, tmp_path, monkeypatch):
        monkeypatch.delenv("TG_API_ID", raising=False)
        monkeypatch.delenv("TG_API_HASH", raising=False)
        monkeypatch.delenv("SESSION_ENCRYPTION_KEY", raising=False)

        config_file = tmp_path / "config.yaml"
        config_file.write_text(
            "telegram:\n"
            "  api_id: ${TG_API_ID}\n"
            "  api_hash: ${TG_API_HASH}\n"
            "security:\n"
            "  session_encryption_key: ${SESSION_ENCRYPTION_KEY}\n"
        )
        config = load_config(config_file)

        assert config.telegram.api_id == 0
        assert config.telegram.api_hash == ""
        assert config.security.session_encryption_key == ""

    def test_a_leftover_web_block_from_the_removed_panel_is_ignored(self, tmp_path, monkeypatch):
        """Existing config.yaml files keep loading after the web panel removal (#1868)."""
        monkeypatch.setenv("TG_API_ID", "99999")
        config_file = tmp_path / "config.yaml"
        config_file.write_text(
            "telegram:\n"
            "  api_id: ${TG_API_ID}\n"
            "web:\n"
            "  host: 0.0.0.0\n"
            "  port: 8080\n"
        )
        config = load_config(config_file)

        assert config.telegram.api_id == 99999
        assert not hasattr(config, "web")

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
