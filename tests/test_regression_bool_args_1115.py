"""Regression: agent-tool bool args arrive as JSON strings (#1115).

LLM backends routinely serialize a boolean tool argument as the JSON *string*
``"false"`` / ``"true"`` instead of a real bool. A handler that reads such a flag
with a bare ``args.get("flag")`` — or ``bool(args.get("flag"))`` — silently breaks,
because ``bool("false") is True``: every non-empty string is truthy. The flag then
reaches the service with the *wrong* (often opposite) meaning, even though the
service itself is correct. These tests drive the tools exactly as the LLM does
(handler called with a JSON-arg dict) and assert the real bool that reaches the
backend.

The shared fix is :func:`src.agent.tools._registry.arg_bool` /
:func:`is_affirmative`, which already exists for precisely this case (audit
#837): it maps the strings ``"false"``/``"0"``/``"no"`` to ``False``.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from src.models import Account, PhotoAutoUploadJob, PhotoSendMode
from tests.agent_tools_helpers import _get_tool_handlers, _text


def _make_account(phone: str = "+79001234567") -> MagicMock:
    acc = MagicMock(spec=Account)
    acc.id = 1
    acc.phone = phone
    acc.is_active = True
    acc.is_primary = True
    acc.session_string = "fake"
    return acc


def _make_mock_pool() -> tuple[MagicMock, AsyncMock]:
    mock_client = AsyncMock()
    mock_client.get_entity = AsyncMock(return_value=MagicMock(id=123456))
    mock_client.edit_admin = AsyncMock()
    mock_client.edit_permissions = AsyncMock()
    mock_client.pin_message = AsyncMock()

    mock_session = MagicMock()
    mock_pool = MagicMock()
    mock_pool.get_native_client_by_phone = AsyncMock(return_value=(mock_client, None))
    mock_pool.get_client_by_phone = AsyncMock(return_value=(mock_session, None))
    mock_pool.resolve_dialog_entity = AsyncMock(return_value=MagicMock(id=123456))
    return mock_pool, mock_client


# ---------------------------------------------------------------------------
# edit_admin: is_admin="false" must DEMOTE, not promote.
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_edit_admin_string_false_demotes(mock_db):
    """is_admin="false" (LLM JSON string) must reach Telethon as is_admin=False."""
    mock_pool, mock_client = _make_mock_pool()
    mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
    handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)

    result = await handlers["edit_admin"](
        {
            "phone": "+79001234567",
            "chat_id": "chat",
            "user_id": "111",
            "is_admin": "false",
            "confirm": "true",
        }
    )

    assert "обновлены" in _text(result)
    mock_client.edit_admin.assert_awaited_once()
    # The actual privilege flag that hit Telethon.
    assert mock_client.edit_admin.await_args.kwargs.get("is_admin") is False


@pytest.mark.anyio
async def test_edit_admin_string_false_confirmation_says_demote(mock_db):
    """The confirmation prompt must describe a demotion, not a promotion."""
    mock_pool, _ = _make_mock_pool()
    mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
    handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)

    # No confirm → gate text is returned; it must reflect the real action.
    result = await handlers["edit_admin"](
        {"phone": "+79001234567", "chat_id": "chat", "user_id": "111", "is_admin": "false"}
    )
    text = _text(result)
    assert "понизит" in text
    assert "повысит" not in text


# ---------------------------------------------------------------------------
# edit_permissions: send_messages="false" must RESTRICT, not allow.
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_edit_permissions_string_false_restricts(mock_db):
    """send_messages="false" must reach Telethon as the bool False (a restriction)."""
    mock_pool, mock_client = _make_mock_pool()
    mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
    handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)

    result = await handlers["edit_permissions"](
        {
            "phone": "+79001234567",
            "chat_id": "chat",
            "user_id": "111",
            "send_messages": "false",
            "confirm": "true",
        }
    )

    assert "обновлены" in _text(result)
    mock_client.edit_permissions.assert_awaited_once()
    assert mock_client.edit_permissions.await_args.kwargs.get("send_messages") is False


@pytest.mark.anyio
async def test_edit_permissions_omitted_flag_stays_none(mock_db):
    """An omitted flag must NOT be coerced to False — it stays None (three-state)."""
    mock_pool, mock_client = _make_mock_pool()
    mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
    handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)

    result = await handlers["edit_permissions"](
        {
            "phone": "+79001234567",
            "chat_id": "chat",
            "user_id": "111",
            "send_media": "true",
            "confirm": "true",
        }
    )

    assert "обновлены" in _text(result)
    kwargs = mock_client.edit_permissions.await_args.kwargs
    # send_messages was not passed → must not appear (service drops None flags).
    assert "send_messages" not in kwargs
    assert kwargs.get("send_media") is True


# ---------------------------------------------------------------------------
# pin_message: notify="false" must pin silently.
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_pin_message_string_false_notify_is_silent(mock_db):
    """notify="false" must reach Telethon as notify=False (no member ping)."""
    mock_pool, mock_client = _make_mock_pool()
    mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
    handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)

    result = await handlers["pin_message"](
        {
            "phone": "+79001234567",
            "chat_id": "chat",
            "message_id": 10,
            "notify": "false",
            "confirm": "true",
        }
    )

    assert "закреплено" in _text(result)
    mock_client.pin_message.assert_awaited_once()
    assert mock_client.pin_message.await_args.kwargs.get("notify") is False


# ---------------------------------------------------------------------------
# update_auto_upload: is_active="false" must DEACTIVATE the job.
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_update_auto_upload_string_false_deactivates(db):
    """is_active="false" must persist is_active=False, not leave the job active."""
    pool = MagicMock()
    job_id = await db.repos.photo_loader.create_auto_job(
        PhotoAutoUploadJob(
            phone="+79001234567",
            target_dialog_id=1,
            folder_path="/tmp",
            send_mode=PhotoSendMode("album"),
            caption=None,
            interval_minutes=60,
            is_active=True,
        )
    )
    handlers = _get_tool_handlers(db, client_pool=pool)

    result = await handlers["update_auto_upload"](
        {"job_id": job_id, "is_active": "false", "confirm": "true"}
    )

    assert "обновлена" in _text(result)
    job = await db.repos.photo_loader.get_auto_job(job_id)
    assert job.is_active is False


async def _seed_auto_job(db) -> int:
    return await db.repos.photo_loader.create_auto_job(
        PhotoAutoUploadJob(
            phone="+79001234567",
            target_dialog_id=1,
            folder_path="/tmp",
            send_mode=PhotoSendMode("album"),
            caption=None,
            interval_minutes=60,
            is_active=True,
        )
    )


@pytest.mark.anyio
@pytest.mark.parametrize("bad_interval", ["abc", 0, -5, "0", "-3"])
async def test_update_auto_upload_invalid_interval_rejected(db, bad_interval):
    """A non-numeric or sub-1 interval must be rejected with a safe tool error.

    Writing 0/-5 would violate PhotoAutoUploadJob.interval_minutes (Field ge=1) and
    poison every later get/list_auto_jobs read; a non-numeric value must surface a
    friendly error, not an unhandled ValueError escaping the handler (#1115 review).
    """
    job_id = await _seed_auto_job(db)
    handlers = _get_tool_handlers(db, client_pool=MagicMock())

    result = await handlers["update_auto_upload"](
        {"job_id": job_id, "interval_minutes": bad_interval, "confirm": "true"}
    )

    text = _text(result)
    assert "Ошибка" in text
    # The job must remain readable with its original, valid interval — not poisoned.
    job = await db.repos.photo_loader.get_auto_job(job_id)
    assert job.interval_minutes == 60


@pytest.mark.anyio
async def test_update_auto_upload_empty_interval_leaves_unchanged(db):
    """An empty-string interval means "not supplied" → the interval stays unchanged."""
    job_id = await _seed_auto_job(db)
    handlers = _get_tool_handlers(db, client_pool=MagicMock())

    result = await handlers["update_auto_upload"](
        {"job_id": job_id, "interval_minutes": "", "confirm": "true"}
    )

    assert "обновлена" in _text(result)
    job = await db.repos.photo_loader.get_auto_job(job_id)
    assert job.interval_minutes == 60


@pytest.mark.anyio
async def test_update_auto_upload_valid_string_interval_persists(db):
    """A valid numeric string interval must still be coerced and persisted."""
    job_id = await _seed_auto_job(db)
    handlers = _get_tool_handlers(db, client_pool=MagicMock())

    result = await handlers["update_auto_upload"](
        {"job_id": job_id, "interval_minutes": "30", "confirm": "true"}
    )

    assert "обновлена" in _text(result)
    job = await db.repos.photo_loader.get_auto_job(job_id)
    assert job.interval_minutes == 30


# ---------------------------------------------------------------------------
# add_search_query: is_regex="false" must NOT enable regex mode.
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_add_search_query_string_false_flags_disabled(db):
    """is_regex/is_fts/notify_on_collect="false" must persist as False, not True."""
    handlers = _get_tool_handlers(db, client_pool=None)

    result = await handlers["add_search_query"](
        {
            "query": "needle",
            "is_regex": "false",
            "is_fts": "false",
            "notify_on_collect": "false",
            "confirm": "true",
        }
    )
    assert "создан" in _text(result)

    from src.services.search_query_service import SearchQueryService

    queries = await SearchQueryService(db).list_queries()
    assert len(queries) == 1
    sq = queries[0]
    assert sq.is_regex is False
    assert sq.is_fts is False
    assert sq.notify_on_collect is False


# ---------------------------------------------------------------------------
# collect_channel: force="false" must NOT force-collect a filtered channel.
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_collect_channel_string_false_force_respects_filter(db):
    """force="false" must keep the filtered-channel guard active."""
    from src.models import Channel

    await db.add_channel(Channel(channel_id=2002, title="Filtered", username="filtered"))
    ch = (await db.get_channels(include_filtered=True))[0]
    await db.set_channel_filtered(ch.id, True)

    pool = MagicMock()
    handlers = _get_tool_handlers(db, client_pool=pool)

    result = await handlers["collect_channel"]({"pk": ch.id, "force": "false"})
    # force="false" must be respected → the filtered guard fires.
    assert "отфильтрован" in _text(result)


# ---------------------------------------------------------------------------
# add_pipeline: ab_num_variants=0 must be rejected, not silently coerced to 1.
# ---------------------------------------------------------------------------


@pytest.mark.anyio
@pytest.mark.parametrize("bad_variants", [0, "0", -2])
async def test_add_pipeline_invalid_ab_num_variants_rejected(mock_db, bad_variants):
    """A sub-1 ab_num_variants must surface an error instead of `... or 1` → 1.

    Pipeline.ab_num_variants is Field ge=1; silently turning a real 0 into 1 hides
    an invalid request (#1115 review).
    """
    from unittest.mock import patch

    handlers = _get_tool_handlers(mock_db)
    with patch("src.services.pipeline_service.PipelineService") as mock_svc:
        mock_svc.return_value.add = AsyncMock(return_value=10)
        result = await handlers["add_pipeline"](
            {
                "confirm": "true",
                "name": "P",
                "prompt_template": "t",
                "source_channel_ids": "1",
                "target_refs": "+7123456|789",
                "ab_num_variants": bad_variants,
            }
        )

    assert "ab_num_variants" in _text(result)
