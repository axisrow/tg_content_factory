from __future__ import annotations

import asyncio
from dataclasses import dataclass

from src.agent.prompt_template import PromptTemplateError, validate_prompt_template
from src.database import Database
from src.database.bundles import PipelineBundle
from src.models import (
    ContentPipeline,
    PipelineGenerationBackend,
    PipelinePublishMode,
    PipelineSource,
    PipelineTarget,
)


@dataclass(frozen=True)
class PipelineTargetRef:
    phone: str
    dialog_id: int


class PipelineValidationError(ValueError):
    """Raised when pipeline configuration is invalid."""


class PipelineService:
    def __init__(self, bundle: PipelineBundle | Database):
        if isinstance(bundle, Database):
            bundle = PipelineBundle.from_database(bundle)
        self._bundle = bundle

    async def add(
        self,
        *,
        name: str,
        prompt_template: str,
        source_channel_ids: list[int],
        target_refs: list[PipelineTargetRef],
        llm_model: str | None = None,
        image_model: str | None = None,
        publish_mode: PipelinePublishMode | str = PipelinePublishMode.MODERATED,
        generation_backend: PipelineGenerationBackend | str = PipelineGenerationBackend.CHAIN,
        generate_interval_minutes: int = 60,
        is_active: bool = True,
    ) -> int:
        pipeline = await self._build_pipeline(
            name=name,
            prompt_template=prompt_template,
            llm_model=llm_model,
            image_model=image_model,
            publish_mode=publish_mode,
            generation_backend=generation_backend,
            generate_interval_minutes=generate_interval_minutes,
            is_active=is_active,
        )
        sources = await self._normalize_sources(source_channel_ids)
        targets = await self._normalize_targets(target_refs)
        return await self._bundle.add(pipeline, sources, targets)

    async def update(
        self,
        pipeline_id: int,
        *,
        name: str,
        prompt_template: str,
        source_channel_ids: list[int],
        target_refs: list[PipelineTargetRef],
        llm_model: str | None = None,
        image_model: str | None = None,
        publish_mode: PipelinePublishMode | str = PipelinePublishMode.MODERATED,
        generation_backend: PipelineGenerationBackend | str = PipelineGenerationBackend.CHAIN,
        generate_interval_minutes: int = 60,
        is_active: bool = True,
    ) -> bool:
        pipeline = await self._build_pipeline(
            name=name,
            prompt_template=prompt_template,
            llm_model=llm_model,
            image_model=image_model,
            publish_mode=publish_mode,
            generation_backend=generation_backend,
            generate_interval_minutes=generate_interval_minutes,
            is_active=is_active,
        )
        sources = await self._normalize_sources(source_channel_ids)
        targets = await self._normalize_targets(target_refs)
        return await self._bundle.update(pipeline_id, pipeline, sources, targets)

    async def list(self, active_only: bool = False) -> list[ContentPipeline]:
        return await self._bundle.get_all(active_only)

    async def get(self, pipeline_id: int) -> ContentPipeline | None:
        return await self._bundle.get_by_id(pipeline_id)

    async def delete(self, pipeline_id: int) -> None:
        await self._bundle.delete(pipeline_id)

    async def toggle(self, pipeline_id: int) -> bool:
        pipeline = await self._bundle.get_by_id(pipeline_id)
        if pipeline is None:
            return False
        await self._bundle.set_active(pipeline_id, not pipeline.is_active)
        return True

    async def get_sources(self, pipeline_id: int) -> list[PipelineSource]:
        return await self._bundle.list_sources(pipeline_id)

    async def get_targets(self, pipeline_id: int) -> list[PipelineTarget]:
        return await self._bundle.list_targets(pipeline_id)

    async def get_detail(self, pipeline_id: int) -> dict | None:
        pipeline = await self._bundle.get_by_id(pipeline_id)
        if pipeline is None:
            return None
        sources, targets, channels = await asyncio.gather(
            self._bundle.list_sources(pipeline_id),
            self._bundle.list_targets(pipeline_id),
            self._bundle.list_channels(include_filtered=True),
        )
        channels_by_id = {channel.channel_id: channel for channel in channels}
        return {
            "pipeline": pipeline,
            "sources": sources,
            "targets": targets,
            "source_ids": [source.channel_id for source in sources],
            "target_refs": [f"{target.phone}|{target.dialog_id}" for target in targets],
            "source_titles": [
                (ch.title or str(source.channel_id))
                if (ch := channels_by_id.get(source.channel_id))
                else str(source.channel_id)
                for source in sources
            ],
        }

    async def get_with_relations(self, active_only: bool = False) -> list[dict]:
        pipelines = await self._bundle.get_all(active_only)
        channels = await self._bundle.list_channels(include_filtered=True)
        channels_by_id = {channel.channel_id: channel for channel in channels}
        relation_rows = await asyncio.gather(
            *[
                self._get_relation_row(pipeline, channels_by_id)
                for pipeline in pipelines
            ]
        )
        return relation_rows

    async def list_cached_dialogs_by_phone(
        self,
        active_only: bool = False,
    ) -> dict[str, list[dict]]:
        accounts = await self._bundle.list_accounts(active_only=active_only)
        dialogs = await asyncio.gather(
            *[self._bundle.list_cached_dialogs(account.phone) for account in accounts]
        )
        return {
            account.phone: rows
            for account, rows in zip(accounts, dialogs, strict=False)
        }

    async def _get_relation_row(
        self,
        pipeline: ContentPipeline,
        channels_by_id: dict[int, object],
    ) -> dict:
        assert pipeline.id is not None
        sources, targets = await asyncio.gather(
            self._bundle.list_sources(pipeline.id),
            self._bundle.list_targets(pipeline.id),
        )
        return {
            "pipeline": pipeline,
            "sources": sources,
            "targets": targets,
            "source_ids": [source.channel_id for source in sources],
            "target_refs": [f"{target.phone}|{target.dialog_id}" for target in targets],
            "source_titles": [
                (ch.title or str(source.channel_id))
                if (ch := channels_by_id.get(source.channel_id))
                else str(source.channel_id)
                for source in sources
            ],
        }

    async def _build_pipeline(
        self,
        *,
        name: str,
        prompt_template: str,
        llm_model: str | None,
        image_model: str | None,
        publish_mode: PipelinePublishMode | str,
        generation_backend: PipelineGenerationBackend | str,
        generate_interval_minutes: int,
        is_active: bool,
        last_generated_id: int = 0,
    ) -> ContentPipeline:
        cleaned_name = name.strip()
        if not cleaned_name:
            raise PipelineValidationError("Название pipeline не может быть пустым.")
        cleaned_template = prompt_template.strip()
        if not cleaned_template:
            raise PipelineValidationError("Шаблон промпта не может быть пустым.")
        try:
            validate_prompt_template(cleaned_template)
        except PromptTemplateError as exc:
            raise PipelineValidationError(str(exc)) from exc
        try:
            publish_mode_enum = PipelinePublishMode(publish_mode)
            backend_enum = PipelineGenerationBackend(generation_backend)
        except ValueError as exc:
            raise PipelineValidationError("Указан неизвестный режим pipeline.") from exc
        if generate_interval_minutes < 1:
            raise PipelineValidationError("Интервал генерации должен быть не меньше 1 минуты.")
        return ContentPipeline(
            name=cleaned_name,
            prompt_template=cleaned_template,
            llm_model=(llm_model or "").strip() or None,
            image_model=(image_model or "").strip() or None,
            publish_mode=publish_mode_enum,
            generation_backend=backend_enum,
            is_active=is_active,
            last_generated_id=last_generated_id,
            generate_interval_minutes=generate_interval_minutes,
        )

    async def _normalize_sources(self, source_channel_ids: list[int]) -> list[int]:
        cleaned = sorted({int(channel_id) for channel_id in source_channel_ids})
        if not cleaned:
            raise PipelineValidationError("Выберите хотя бы один источник.")
        channels = await self._bundle.list_channels(include_filtered=True)
        known_ids = {channel.channel_id for channel in channels}
        missing = [channel_id for channel_id in cleaned if channel_id not in known_ids]
        if missing:
            missing_values = ", ".join(map(str, missing))
            raise PipelineValidationError(f"Неизвестные source channels: {missing_values}")
        return cleaned

    async def _normalize_targets(
        self,
        target_refs: list[PipelineTargetRef],
    ) -> list[PipelineTarget]:
        if not target_refs:
            raise PipelineValidationError("Выберите хотя бы одну цель публикации.")
        accounts = await self._bundle.list_accounts()
        known_phones = {account.phone for account in accounts}
        result: list[PipelineTarget] = []
        seen: set[tuple[str, int]] = set()
        for ref in target_refs:
            if ref.phone not in known_phones:
                raise PipelineValidationError(f"Аккаунт {ref.phone} не найден.")
            dialog = await self._bundle.get_cached_dialog(ref.phone, ref.dialog_id)
            if dialog is None:
                raise PipelineValidationError(
                    f"Диалог {ref.dialog_id} для {ref.phone} не найден в кеше. "
                    "Сначала откройте 'Мой Телеграм' для этого аккаунта."
                )
            if str(dialog.get("channel_type") or "").strip() == "bot":
                raise PipelineValidationError("Боты не поддерживаются как pipeline targets.")
            key = (ref.phone, ref.dialog_id)
            if key in seen:
                continue
            seen.add(key)
            result.append(
                PipelineTarget(
                    pipeline_id=0,
                    phone=ref.phone,
                    dialog_id=ref.dialog_id,
                    title=dialog.get("title"),
                    dialog_type=dialog.get("channel_type"),
                )
            )
        return result
