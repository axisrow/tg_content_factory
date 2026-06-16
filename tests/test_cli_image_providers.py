"""Tests for CLI image service building from DB providers (audit #838/6)."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.cli.commands.image import _build_image_service


@pytest.mark.anyio
async def test_build_image_service_uses_db_configured_providers():
    db = MagicMock()
    config = MagicMock()
    adapters = {"replicate": AsyncMock()}
    provider_svc = MagicMock()
    provider_svc.load_provider_configs = AsyncMock(return_value=["cfg"])
    provider_svc.build_adapters = MagicMock(return_value=adapters)

    with patch(
        "src.services.image_provider_service.ImageProviderService", return_value=provider_svc
    ):
        svc = await _build_image_service(db, config)

    assert "replicate" in svc.adapter_names


@pytest.mark.anyio
async def test_build_image_service_falls_back_to_env_when_no_db_providers():
    db = MagicMock()
    config = MagicMock()
    provider_svc = MagicMock()
    provider_svc.load_provider_configs = AsyncMock(return_value=[])
    provider_svc.build_adapters = MagicMock(return_value={})

    with patch(
        "src.services.image_provider_service.ImageProviderService", return_value=provider_svc
    ):
        svc = await _build_image_service(db, config)

    # Falls back to env-based ImageGenerationService() (no DB adapters available).
    assert svc is not None
