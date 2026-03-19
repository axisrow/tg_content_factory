from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from telethon.errors import FloodWaitError

from src.models import PhotoSendMode
from src.services.photo_publish_service import PhotoPublishService
from src.telegram.flood_wait import HandledFloodWaitError
from tests.helpers import FakeCliTelethonClient


@pytest.mark.asyncio
async def test_photo_publish_service_sends_album_via_transport_session(
    real_pool_harness_factory,
):
    harness = real_pool_harness_factory()
    client = harness.queue_cli_client(
        phone="+7000",
        client=FakeCliTelethonClient(
            input_entity_resolver=lambda peer: SimpleNamespace(
                id=getattr(peer, "channel_id", getattr(peer, "user_id", peer))
            ),
            send_file_side_effect=lambda *args, **kwargs: [
                SimpleNamespace(id=101),
                SimpleNamespace(id=102),
            ],
        ),
    )
    await harness.add_account("+7000", session_string="photo-session", is_primary=True)
    await harness.initialize_connected_accounts()

    service = PhotoPublishService(harness.pool)
    result = await service.send_now(
        phone="+7000",
        target_dialog_id=-1001,
        target_type="channel",
        file_paths=["/tmp/one.jpg", "/tmp/two.jpg"],
        send_mode=PhotoSendMode.ALBUM,
        caption="hello",
    )

    assert result == [101, 102]
    client.send_file.assert_awaited_once()


@pytest.mark.asyncio
async def test_photo_publish_service_reports_flood_from_transport_session(
    real_pool_harness_factory,
):
    flood = FloodWaitError(request=None, capture=0)
    flood.seconds = 33

    harness = real_pool_harness_factory()
    harness.queue_cli_client(
        phone="+7000",
        client=FakeCliTelethonClient(
            input_entity_resolver=lambda peer: SimpleNamespace(
                id=getattr(peer, "channel_id", getattr(peer, "user_id", peer))
            ),
            send_file_side_effect=lambda *args, **kwargs: flood,
        ),
    )
    await harness.add_account("+7000", session_string="photo-session", is_primary=True)
    await harness.initialize_connected_accounts()

    service = PhotoPublishService(harness.pool)
    with pytest.raises(HandledFloodWaitError) as exc_info:
        await service.send_now(
            phone="+7000",
            target_dialog_id=-1001,
            target_type="channel",
            file_paths=["/tmp/one.jpg"],
            send_mode=PhotoSendMode.SEPARATE,
        )

    assert exc_info.value.info.wait_seconds == 33
    accounts = await harness.db.get_accounts()
    assert accounts[0].flood_wait_until is not None


@pytest.mark.asyncio
async def test_photo_publish_service_does_not_wrap_dialog_resolver_with_short_timeout(
    real_pool_harness_factory,
    monkeypatch,
):
    import src.services.photo_publish_service as photo_publish_module

    harness = real_pool_harness_factory()
    await harness.add_account("+7000", session_string="photo-session", is_primary=True)
    await harness.initialize_connected_accounts()

    service = PhotoPublishService(harness.pool)

    async def _resolve_dialog_entity(*_args, **_kwargs):
        return SimpleNamespace(id=4242)

    harness.pool.resolve_dialog_entity = _resolve_dialog_entity

    original_run_with_flood_wait = photo_publish_module.run_with_flood_wait

    async def _guarded_run_with_flood_wait(awaitable, **kwargs):
        if kwargs.get("operation") == "photo_publish_resolve_dialog_entity":
            raise AssertionError("dialog resolver should not be wrapped by service timeout")
        return await original_run_with_flood_wait(awaitable, **kwargs)

    monkeypatch.setattr(
        photo_publish_module,
        "run_with_flood_wait",
        _guarded_run_with_flood_wait,
    )

    raw_client = FakeCliTelethonClient(
        send_file_side_effect=lambda *args, **kwargs: SimpleNamespace(id=101),
    )
    monkeypatch.setattr(
        harness.pool,
        "get_client_by_phone",
        AsyncMock(return_value=(raw_client, "+7000")),
    )

    result = await service.send_now(
        phone="+7000",
        target_dialog_id=-1001,
        target_type="channel",
        file_paths=["/tmp/one.jpg"],
        send_mode=PhotoSendMode.SEPARATE,
    )

    assert result == [101]
