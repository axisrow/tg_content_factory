"""Size-aware media download for export (issue #834, PR-2)."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.services.telegram_actions import (
    TelegramActionMessageNotFoundError,
    TelegramActionNoMediaError,
    TelegramActionService,
    classify_media_kind,
    media_subdir,
)


def _pool_with_client(client):
    pool = MagicMock()
    pool.get_native_client_by_phone = AsyncMock(return_value=(client, "+1"))
    pool.get_available_client = AsyncMock(return_value=(client, "+1"))
    pool.get_client_by_phone = AsyncMock(return_value=(client, "+1"))
    pool.release_client = AsyncMock()
    return pool


def _client_for(message, *, download_path=None):
    client = AsyncMock()
    client.get_entity = AsyncMock(return_value="entity")

    async def _iter(entity, ids):
        yield message

    client.iter_messages = MagicMock(return_value=_iter("entity", message.id if message else 0))
    client.download_media = AsyncMock(return_value=download_path)
    return client


def _photo_message(size=1000):
    return SimpleNamespace(id=7, media=object(), photo=True, file=SimpleNamespace(size=size, mime_type="image/jpeg"))


# ── pure helpers ──────────────────────────────────────────────────────────


def test_classify_media_kind():
    assert classify_media_kind(_photo_message()) == "photo"
    assert classify_media_kind(SimpleNamespace(voice=True, file=None)) == "voice"
    assert classify_media_kind(SimpleNamespace(video=True, file=None)) == "video"
    # Round video notes are a distinct kind (TG Desktop video_messages/).
    assert classify_media_kind(SimpleNamespace(video=True, video_note=True, file=None)) == "video_note"
    assert classify_media_kind(SimpleNamespace(file=SimpleNamespace(mime_type="video/mp4"))) == "video"
    assert classify_media_kind(SimpleNamespace(file=SimpleNamespace(mime_type="application/pdf"))) == "file"


def test_media_subdir_mapping():
    assert media_subdir("photo") == "photos"
    assert media_subdir("video") == "video_files"
    assert media_subdir("video_note") == "video_messages"
    assert media_subdir("voice") == "voice_messages"
    assert media_subdir("file") == "files"
    assert media_subdir("unknown") == "files"


# ── download_media_sized ──────────────────────────────────────────────────


@pytest.mark.anyio
async def test_downloads_under_threshold_into_typed_subdir(tmp_path):
    msg = _photo_message(size=1000)
    download_path = str(tmp_path / "photos" / "img.jpg")
    client = _client_for(msg, download_path=download_path)
    svc = TelegramActionService(_pool_with_client(client))

    outcome = await svc.download_media_sized(
        phone="+1", chat_id="@c", message_id=7, output_dir=tmp_path, max_size_bytes=3 * 1024 * 1024
    )

    assert outcome.skipped is False
    assert outcome.kind == "photo"
    assert outcome.subdir == "photos"
    assert outcome.rel_path == "photos/img.jpg"
    assert outcome.size_bytes == 1000
    client.download_media.assert_awaited_once()
    # downloaded into the photos/ subdir, not the export root
    _, kwargs = client.download_media.call_args
    assert kwargs["file"].endswith("photos")


@pytest.mark.anyio
async def test_skips_over_threshold_without_downloading(tmp_path):
    msg = _photo_message(size=9_000_000)
    client = _client_for(msg, download_path=str(tmp_path / "photos" / "big.jpg"))
    svc = TelegramActionService(_pool_with_client(client))

    outcome = await svc.download_media_sized(
        phone="+1", chat_id="@c", message_id=7, output_dir=tmp_path, max_size_bytes=3 * 1024 * 1024
    )

    assert outcome.skipped is True
    assert outcome.reason == "exceeds_max_size"
    assert outcome.size_bytes == 9_000_000
    assert outcome.path is None
    client.download_media.assert_not_awaited()


@pytest.mark.anyio
async def test_unknown_size_skipped_when_limit_set(tmp_path):
    # Telegram omits size for some media → must not bypass the limit (#938).
    msg = SimpleNamespace(id=7, media=object(), photo=True, file=SimpleNamespace(size=None, mime_type="image/jpeg"))
    client = _client_for(msg, download_path=str(tmp_path / "photos" / "x.jpg"))
    svc = TelegramActionService(_pool_with_client(client))

    outcome = await svc.download_media_sized(
        phone="+1", chat_id="@c", message_id=7, output_dir=tmp_path, max_size_bytes=3 * 1024 * 1024
    )
    assert outcome.skipped is True
    assert outcome.reason == "size_unknown"
    client.download_media.assert_not_awaited()


@pytest.mark.anyio
async def test_unknown_size_downloads_when_no_limit(tmp_path):
    msg = SimpleNamespace(id=7, media=object(), photo=True, file=SimpleNamespace(size=None, mime_type="image/jpeg"))
    client = _client_for(msg, download_path=str(tmp_path / "photos" / "x.jpg"))
    svc = TelegramActionService(_pool_with_client(client))

    outcome = await svc.download_media_sized(
        phone="+1", chat_id="@c", message_id=7, output_dir=tmp_path, max_size_bytes=None
    )
    assert outcome.skipped is False
    client.download_media.assert_awaited_once()


@pytest.mark.anyio
async def test_no_limit_always_downloads(tmp_path):
    msg = _photo_message(size=50_000_000)
    client = _client_for(msg, download_path=str(tmp_path / "photos" / "huge.jpg"))
    svc = TelegramActionService(_pool_with_client(client))

    outcome = await svc.download_media_sized(
        phone="+1", chat_id="@c", message_id=7, output_dir=tmp_path, max_size_bytes=None
    )

    assert outcome.skipped is False
    client.download_media.assert_awaited_once()


@pytest.mark.anyio
async def test_document_goes_to_files_subdir(tmp_path):
    msg = SimpleNamespace(
        id=8, media=object(), file=SimpleNamespace(size=100, mime_type="application/pdf")
    )
    client = _client_for(msg, download_path=str(tmp_path / "files" / "doc.pdf"))
    svc = TelegramActionService(_pool_with_client(client))

    outcome = await svc.download_media_sized(
        phone="+1", chat_id="@c", message_id=8, output_dir=tmp_path, max_size_bytes=None
    )
    assert outcome.kind == "file"
    assert outcome.subdir == "files"
    assert outcome.rel_path == "files/doc.pdf"


@pytest.mark.anyio
async def test_no_media_raises(tmp_path):
    msg = SimpleNamespace(id=9, media=None)
    client = _client_for(msg, download_path=None)
    svc = TelegramActionService(_pool_with_client(client))

    with pytest.raises(TelegramActionNoMediaError):
        await svc.download_media_sized(phone="+1", chat_id="@c", message_id=9, output_dir=tmp_path)


@pytest.mark.anyio
async def test_message_not_found_raises(tmp_path):
    client = _client_for(None, download_path=None)

    async def _empty(entity, ids):
        return
        yield  # pragma: no cover

    client.iter_messages = MagicMock(return_value=_empty("entity", 0))
    svc = TelegramActionService(_pool_with_client(client))

    with pytest.raises(TelegramActionMessageNotFoundError):
        await svc.download_media_sized(phone="+1", chat_id="@c", message_id=99, output_dir=tmp_path)
