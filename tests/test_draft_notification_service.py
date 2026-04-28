"""Tests for DraftNotificationService."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from src.models import ContentPipeline, GenerationRun, PipelinePublishMode
from src.services.draft_notification_service import DraftNotificationService


@pytest.fixture
def mock_db():
    """Mock database."""
    return MagicMock()


@pytest.fixture
def mock_notifier():
    """Mock notifier that succeeds."""
    notifier = MagicMock()
    notifier.notify = AsyncMock(return_value=True)
    return notifier


@pytest.fixture
def mock_run():
    """Sample generation run."""
    run = MagicMock(spec=GenerationRun)
    run.id = 123
    run.generated_text = "This is a test generated content for moderation."
    return run


@pytest.fixture
def mock_short_run():
    """Short generation run (< 200 chars)."""
    run = MagicMock(spec=GenerationRun)
    run.id = 124
    run.generated_text = "Short"
    return run


@pytest.fixture
def mock_long_run():
    """Long generation run (> 200 chars)."""
    run = MagicMock(spec=GenerationRun)
    run.id = 125
    run.generated_text = "A" * 500
    return run


@pytest.fixture
def mock_moderated_pipeline():
    """Pipeline in moderated mode."""
    pipeline = MagicMock(spec=ContentPipeline)
    pipeline.id = 1
    pipeline.name = "Test Pipeline"
    pipeline.publish_mode = PipelinePublishMode.MODERATED
    return pipeline


@pytest.fixture
def mock_auto_pipeline():
    """Pipeline in auto mode."""
    pipeline = MagicMock(spec=ContentPipeline)
    pipeline.id = 2
    pipeline.name = "Auto Pipeline"
    pipeline.publish_mode = PipelinePublishMode.AUTO
    return pipeline


# === notify_new_draft tests ===


@pytest.mark.anyio
async def test_notify_new_draft_without_notifier(mock_db, mock_run, mock_moderated_pipeline):
    """Returns False when notifier is None."""
    service = DraftNotificationService(mock_db, notifier=None)
    result = await service.notify_new_draft(mock_run, mock_moderated_pipeline)
    assert result is False


@pytest.mark.anyio
async def test_notify_new_draft_auto_mode(
    mock_db, mock_notifier, mock_run, mock_auto_pipeline
):
    """Returns False for auto mode pipelines."""
    service = DraftNotificationService(mock_db, notifier=mock_notifier)
    result = await service.notify_new_draft(mock_run, mock_auto_pipeline)
    assert result is False
    mock_notifier.notify.assert_not_called()


@pytest.mark.anyio
async def test_notify_new_draft_moderated_success(
    mock_db, mock_notifier, mock_run, mock_moderated_pipeline
):
    """Sends notification for moderated pipeline."""
    service = DraftNotificationService(mock_db, notifier=mock_notifier)
    result = await service.notify_new_draft(mock_run, mock_moderated_pipeline)
    assert result is True
    mock_notifier.notify.assert_called_once()
    call_args = mock_notifier.notify.call_args[0][0]
    assert "Новый черновик" in call_args
    assert "Test Pipeline" in call_args
    assert "#123" in call_args


@pytest.mark.anyio
async def test_notify_new_draft_truncates_long_content(
    mock_db, mock_notifier, mock_long_run, mock_moderated_pipeline
):
    """Truncates content longer than 200 chars."""
    service = DraftNotificationService(mock_db, notifier=mock_notifier)
    result = await service.notify_new_draft(mock_long_run, mock_moderated_pipeline)
    assert result is True
    call_args = mock_notifier.notify.call_args[0][0]
    assert "..." in call_args
    assert "A" * 200 in call_args


@pytest.mark.anyio
async def test_notify_new_draft_short_content(
    mock_db, mock_notifier, mock_short_run, mock_moderated_pipeline
):
    """Doesn't add ellipsis for short content."""
    service = DraftNotificationService(mock_db, notifier=mock_notifier)
    result = await service.notify_new_draft(mock_short_run, mock_moderated_pipeline)
    assert result is True
    call_args = mock_notifier.notify.call_args[0][0]
    assert "Short" in call_args
    # No ellipsis for short content
    assert call_args.count("...") == 0


@pytest.mark.anyio
async def test_notify_new_draft_notifier_exception(
    mock_db, mock_notifier, mock_run, mock_moderated_pipeline
):
    """Returns False when notifier raises exception."""
    mock_notifier.notify = AsyncMock(side_effect=Exception("Connection error"))
    service = DraftNotificationService(mock_db, notifier=mock_notifier)
    result = await service.notify_new_draft(mock_run, mock_moderated_pipeline)
    assert result is False


@pytest.mark.anyio
async def test_notify_new_draft_notifier_returns_false(
    mock_db, mock_notifier, mock_run, mock_moderated_pipeline
):
    """Returns False when notifier.notify returns False."""
    mock_notifier.notify = AsyncMock(return_value=False)
    service = DraftNotificationService(mock_db, notifier=mock_notifier)
    result = await service.notify_new_draft(mock_run, mock_moderated_pipeline)
    assert result is False


@pytest.mark.anyio
async def test_notify_new_draft_with_none_generated_text(
    mock_db, mock_notifier, mock_moderated_pipeline
):
    """Handles None generated_text gracefully."""
    run = MagicMock(spec=GenerationRun)
    run.id = 126
    run.generated_text = None
    service = DraftNotificationService(mock_db, notifier=mock_notifier)
    result = await service.notify_new_draft(run, mock_moderated_pipeline)
    assert result is True


# === notify_bulk_drafts tests ===


@pytest.mark.anyio
async def test_notify_bulk_drafts_without_notifier(mock_db, mock_moderated_pipeline):
    """Returns 0 when notifier is None."""
    service = DraftNotificationService(mock_db, notifier=None)
    runs = [MagicMock(spec=GenerationRun, id=i) for i in range(3)]
    result = await service.notify_bulk_drafts(runs, mock_moderated_pipeline)
    assert result == 0


@pytest.mark.anyio
async def test_notify_bulk_drafts_auto_mode(mock_db, mock_notifier, mock_auto_pipeline):
    """Returns 0 for auto mode pipelines."""
    service = DraftNotificationService(mock_db, notifier=mock_notifier)
    runs = [MagicMock(spec=GenerationRun, id=i) for i in range(3)]
    result = await service.notify_bulk_drafts(runs, mock_auto_pipeline)
    assert result == 0


@pytest.mark.anyio
async def test_notify_bulk_drafts_single_run(
    mock_db, mock_notifier, mock_run, mock_moderated_pipeline
):
    """Delegates to notify_new_draft for single run."""
    service = DraftNotificationService(mock_db, notifier=mock_notifier)
    result = await service.notify_bulk_drafts([mock_run], mock_moderated_pipeline)
    assert result == 1
    mock_notifier.notify.assert_called_once()


@pytest.mark.anyio
async def test_notify_bulk_drafts_multiple_runs(
    mock_db, mock_notifier, mock_moderated_pipeline
):
    """Sends bulk notification for multiple runs."""
    service = DraftNotificationService(mock_db, notifier=mock_notifier)
    runs = [MagicMock(spec=GenerationRun, id=i) for i in range(1, 6)]
    result = await service.notify_bulk_drafts(runs, mock_moderated_pipeline)
    assert result == 5
    mock_notifier.notify.assert_called_once()
    call_args = mock_notifier.notify.call_args[0][0]
    assert "5 новых черновиков" in call_args


@pytest.mark.anyio
async def test_notify_bulk_drafts_limits_displayed_runs(
    mock_db, mock_notifier, mock_moderated_pipeline
):
    """Shows only first 10 runs when more than 10."""
    service = DraftNotificationService(mock_db, notifier=mock_notifier)
    runs = [MagicMock(spec=GenerationRun, id=i) for i in range(1, 16)]
    result = await service.notify_bulk_drafts(runs, mock_moderated_pipeline)
    assert result == 15
    call_args = mock_notifier.notify.call_args[0][0]
    assert "и ещё 5" in call_args


@pytest.mark.anyio
async def test_notify_bulk_drafts_notifier_exception(
    mock_db, mock_notifier, mock_moderated_pipeline
):
    """Returns 0 when notifier raises exception."""
    mock_notifier.notify = AsyncMock(side_effect=Exception("Failed"))
    service = DraftNotificationService(mock_db, notifier=mock_notifier)
    runs = [MagicMock(spec=GenerationRun, id=i) for i in range(3)]
    result = await service.notify_bulk_drafts(runs, mock_moderated_pipeline)
    assert result == 0


@pytest.mark.anyio
async def test_notify_bulk_drafts_notifier_returns_false(
    mock_db, mock_notifier, mock_moderated_pipeline
):
    """Returns 0 when notifier returns False."""
    mock_notifier.notify = AsyncMock(return_value=False)
    service = DraftNotificationService(mock_db, notifier=mock_notifier)
    runs = [MagicMock(spec=GenerationRun, id=i) for i in range(3)]
    result = await service.notify_bulk_drafts(runs, mock_moderated_pipeline)
    assert result == 0


@pytest.mark.anyio
async def test_notify_bulk_drafts_empty_list(mock_db, mock_notifier, mock_moderated_pipeline):
    """Handles empty list gracefully."""
    service = DraftNotificationService(mock_db, notifier=mock_notifier)
    result = await service.notify_bulk_drafts([], mock_moderated_pipeline)
    # Empty list - single run path not triggered, bulk message with 0 runs
    assert result == 0
