from __future__ import annotations

from types import SimpleNamespace

import pytest
from telethon.errors import FloodWaitError

from src.models import PhotoSendMode
from src.services.photo_publish_service import PhotoPublishService
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
    with pytest.raises(FloodWaitError):
        await service.send_now(
            phone="+7000",
            target_dialog_id=-1001,
            target_type="channel",
            file_paths=["/tmp/one.jpg"],
            send_mode=PhotoSendMode.SEPARATE,
        )

    accounts = await harness.db.get_accounts()
    assert accounts[0].flood_wait_until is not None
