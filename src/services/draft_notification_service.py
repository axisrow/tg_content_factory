from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from src.models import ContentPipeline, GenerationRun, PipelinePublishMode

if TYPE_CHECKING:
    from src.telegram.notifier import Notifier
    from src.database import Database

logger = logging.getLogger(__name__)


class DraftNotificationService:
    """Service for sending notifications about new generated drafts.
    
    Sends push notifications via Notifier when new content is generated
    for pipelines with publish_mode=MODERATED.
    """

    def __init__(self, db: Database, notifier: Notifier | None):
        self._db = db
        self._notifier = notifier

    async def notify_new_draft(
        self,
        run: GenerationRun,
        pipeline: ContentPipeline,
    ) -> bool:
        """Send notification about a new draft that needs moderation.
        
        Args:
            run: The generation run with new content
            pipeline: The pipeline that generated this content
            
        Returns:
            True if notification was sent successfully, False otherwise
        """
        if self._notifier is None:
            logger.debug("Notifier not configured, skipping draft notification")
            return False

        if pipeline.publish_mode != PipelinePublishMode.MODERATED:
            logger.debug(
                "Pipeline %s is not in moderated mode, skipping notification",
                pipeline.id,
            )
            return False

        preview = ""
        if run.generated_text:
            preview = run.generated_text[:200]
            if len(run.generated_text) > 200:
                preview += "..."

        message = (
            f"📝 Новый черновик для модерации\n\n"
            f"Pipeline: {pipeline.name} (#{pipeline.id})\n"
            f"Run: #{run.id}\n\n"
            f"Превью:\n{preview}\n\n"
            f"Проверить: /moderation/{run.id}/view"
        )

        try:
            success = await self._notifier.notify(message)
            if success:
                logger.info("Sent draft notification for run %s", run.id)
            return success
        except Exception:
            logger.exception("Failed to send draft notification for run %s", run.id)
            return False

    async def notify_bulk_drafts(
        self,
        runs: list[GenerationRun],
        pipeline: ContentPipeline,
    ) -> int:
        """Send notification about multiple new drafts.
        
        Args:
            runs: List of generation runs
            pipeline: The pipeline that generated these runs
            
        Returns:
            Number of successful notifications
        """
        if self._notifier is None:
            return 0

        if pipeline.publish_mode != PipelinePublishMode.MODERATED:
            return 0

        if len(runs) == 1:
            success = await self.notify_new_draft(runs[0], pipeline)
            return 1 if success else 0

        message = (
            f"📝 {len(runs)} новых черновиков для модерации\n\n"
            f"Pipeline: {pipeline.name} (#{pipeline.id})\n"
            f"Runs: {', '.join(f'#{r.id}' for r in runs[:10])}"
        )
        if len(runs) > 10:
            message += f"\n... и ещё {len(runs) - 10}"

        message += f"\n\nПроверить: /moderation"

        try:
            success = await self._notifier.notify(message)
            return len(runs) if success else 0
        except Exception:
            logger.exception("Failed to send bulk draft notification")
            return 0
