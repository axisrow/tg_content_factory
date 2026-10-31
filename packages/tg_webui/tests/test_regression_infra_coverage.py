"""Coverage tests for infra: auth, collector, web routes, client pool, backends, session."""
# Web-panel tests split out of tests/test_regression_infra_coverage.py (#1720 wave 3).


from __future__ import annotations

from unittest.mock import MagicMock, patch

# ===========================================================================
# 9. web/routes/settings.py — dev mode guard
# ===========================================================================




class TestSettingsRouteHelpers:
    async def test_require_agent_dev_mode_disabled(self, db):
        from tg_webui.settings.handlers import _require_agent_dev_mode

        request = MagicMock()
        # Patch deps.get_db to return our real db
        with patch("tg_webui.settings.handlers.deps.get_db", return_value=db):
            await db.set_setting("agent_dev_mode_enabled", "0")
            result = await _require_agent_dev_mode(request)
            assert result is not None  # returns redirect

    async def test_require_agent_dev_mode_enabled(self, db):
        from tg_webui.settings.handlers import _require_agent_dev_mode

        request = MagicMock()
        with patch("tg_webui.settings.handlers.deps.get_db", return_value=db):
            await db.set_setting("agent_dev_mode_enabled", "1")
            result = await _require_agent_dev_mode(request)
            assert result is None




class TestWebSchedulerRoutesCoverage:
    """Cover remaining lines in web/routes/scheduler.py."""

    def test_job_label_known(self):
        """Lines 23-32: job_label for known and pattern-based job ids."""
        from tg_webui.routes.scheduler import _job_label

        assert _job_label("collect_all") == "Сбор всех каналов"
        assert "sq_" not in _job_label("sq_42")  # should show "Стат. запроса #42"
        assert _job_label("sq_42") == "Стат. запроса #42"
        assert _job_label("pipeline_run_5") == "Пайплайн #5"
        assert _job_label("content_generate_3") == "Генерация #3"
        assert _job_label("unknown_job") == "unknown_job"


# ---- web/routes/pipelines.py coverage ----




class TestWebPipelinesRoutesCoverage:
    """Cover remaining lines in web/routes/pipelines.py."""

    def test_pipeline_redirect_with_error(self):
        """Test the redirect helper."""
        from tg_webui.pipelines.responses import PipelineRedirect, pipeline_redirect_response

        resp = pipeline_redirect_response(PipelineRedirect("test_msg"))
        assert resp.status_code == 303
        assert "msg=test_msg" in str(resp.headers.get("location", ""))

    def test_pipeline_redirect_with_error_flag(self):
        from tg_webui.pipelines.responses import PipelineRedirect, pipeline_redirect_response

        resp = pipeline_redirect_response(PipelineRedirect("err", error=True))
        assert "error=err" in str(resp.headers.get("location", ""))


# ---- web/routes/channel_collection.py coverage ----




class TestWebChannelCollectionCoverage:
    """Cover remaining lines in web/routes/channel_collection.py."""

    def test_bulk_enqueue_msg_empty(self):
        """Lines 53-57: bulk enqueue message mapping."""
        from tg_agent.services.collection_service import BulkEnqueueResult
        from tg_webui.routes.channel_collection import bulk_enqueue_msg

        result = BulkEnqueueResult(
            queued_count=0,
            skipped_existing_count=0,
            total_candidates=0,
        )
        assert bulk_enqueue_msg(result) == "collect_all_empty"

    def test_bulk_enqueue_msg_queued(self):
        from tg_agent.services.collection_service import BulkEnqueueResult
        from tg_webui.routes.channel_collection import bulk_enqueue_msg

        result = BulkEnqueueResult(
            queued_count=3,
            skipped_existing_count=0,
            total_candidates=3,
        )
        assert bulk_enqueue_msg(result) == "collect_all_queued"

    def test_bulk_enqueue_msg_noop(self):
        from tg_agent.services.collection_service import BulkEnqueueResult
        from tg_webui.routes.channel_collection import bulk_enqueue_msg

        result = BulkEnqueueResult(
            queued_count=0,
            skipped_existing_count=3,
            total_candidates=3,
        )
        assert bulk_enqueue_msg(result) == "collect_all_noop"


# ---- web/routes misc coverage ----




class TestWebRoutesExtraBatch3:
    """Cover a few more web lines to push to 90%."""

    def test_scheduler_job_label_photo(self):
        """Cover photo_due and photo_auto job labels."""
        from tg_webui.routes.scheduler import _job_label

        assert _job_label("photo_due") == "Фото по расписанию"
        assert _job_label("photo_auto") == "Автозагрузка фото"

    def test_channel_collection_redirect_url(self):
        """Cover _collect_all_redirect_url."""
        from tg_agent.services.collection_service import BulkEnqueueResult
        from tg_webui.routes.channel_collection import _collect_all_redirect_url

        result = BulkEnqueueResult(queued_count=1, skipped_existing_count=0, total_candidates=1)
        url = _collect_all_redirect_url(result)
        assert "collect_all_queued" in url

    def test_pipeline_target_refs_parsing(self):
        """Cover _target_refs helper."""
        from tg_webui.pipelines.forms import parse_target_refs as _target_refs

        refs = _target_refs(["+1|123", "+2|456"])
        assert len(refs) == 2


# ---- CLI pipeline additional coverage ----




class TestWebSessionCoverage:
    """Cover remaining web session lines."""

    def test_verify_token_invalid_format(self):
        """Line 39: invalid token format (no dot)."""
        from tg_webui.session import verify_session_token

        result = verify_session_token("no_dot_here", "secret")
        assert result is None

    def test_verify_token_invalid_signature(self):
        """Lines 42-43: invalid signature."""
        from tg_webui.session import create_session_token, verify_session_token

        token = create_session_token("admin", "secret1")
        result = verify_session_token(token, "wrong_secret")
        assert result is None

    def test_verify_token_expired(self):
        """Lines 48-49: expired token."""
        # Create a token with TTL=0 (immediately expired)
        import json

        from tg_webui.session import _b64url_encode, _signer, verify_session_token

        payload = json.dumps({"user": "admin", "exp": 0})
        payload_b64 = _b64url_encode(payload.encode())
        token = _signer("secret").sign(payload_b64).decode()
        result = verify_session_token(token, "secret")
        assert result is None

    def test_verify_token_invalid_json(self):
        """Lines 46-47: invalid JSON payload."""
        from tg_webui.session import _b64url_encode, _signer, verify_session_token

        payload_b64 = _b64url_encode(b"not_json{{{")
        token = _signer("secret").sign(payload_b64).decode()
        result = verify_session_token(token, "secret")
        assert result is None

    def test_b64url_decode_padding(self):
        """Line 20: padding in b64url decode."""
        from tg_webui.session import _b64url_decode, _b64url_encode

        data = b"hello"
        encoded = _b64url_encode(data)
        decoded = _b64url_decode(encoded)
        assert decoded == data

    def test_create_and_verify_token(self):
        """Full round trip."""
        from tg_webui.session import create_session_token, verify_session_token

        token = create_session_token("admin", "secret")
        user = verify_session_token(token, "secret")
        assert user == "admin"


# ---- web/template_globals.py coverage ----




class TestWebTemplateGlobalsCoverage:
    """Cover remaining web template_globals lines."""

    def test_template_globals_basic(self):
        """Lines 31-45: template globals configuration."""
        from unittest.mock import MagicMock

        from tg_webui.template_globals import configure_template_globals

        templates = MagicMock()
        templates.env = MagicMock()
        templates.env.globals = {}
        templates.env.filters = {}

        result = configure_template_globals(templates, None)
        assert result is templates
