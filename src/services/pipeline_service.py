from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from typing import Any

from src.agent.prompt_template import PromptTemplateError, validate_prompt_template
from src.database import Database
from src.database.bundles import PipelineBundle
from src.models import (
    ContentPipeline,
    PipelineGenerationBackend,
    PipelineGraph,
    PipelineNodeType,
    PipelinePublishMode,
    PipelineSource,
    PipelineTarget,
    PipelineTemplate,
)
from src.services.pipeline_llm_requirements import pipeline_needs_llm

logger = logging.getLogger(__name__)


def _group_by_pipeline(items: list) -> dict[int, list]:
    """Group a list of source/target objects by their pipeline_id."""
    result: dict[int, list] = {}
    for item in items:
        result.setdefault(item.pipeline_id, []).append(item)
    return result


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
        react_emoji: str | None = None,
        dag_source_channel_ids: list[int] | None = None,
    ) -> bool:
        existing = await self.get(pipeline_id)
        existing_pipeline_json = existing.pipeline_json if existing else None
        is_dag = existing_pipeline_json is not None

        # Determine if the updated pipeline will need LLM based on new backend + existing pipeline_json
        probe = ContentPipeline(
            name="x",
            generation_backend=generation_backend,
            pipeline_json=existing_pipeline_json,
        )
        skip_llm_validation = not pipeline_needs_llm(probe)
        pipeline = await self._build_pipeline(
            name=name,
            prompt_template=prompt_template,
            llm_model=llm_model,
            image_model=image_model,
            publish_mode=publish_mode,
            generation_backend=generation_backend,
            generate_interval_minutes=generate_interval_minutes,
            is_active=is_active,
            skip_llm_validation=skip_llm_validation,
        )

        # Preserve existing pipeline_json and apply node-level config updates
        updated_graph = existing_pipeline_json
        if updated_graph is not None:
            if dag_source_channel_ids is not None:
                for node in updated_graph.nodes:
                    if node.type == PipelineNodeType.SOURCE:
                        node.config["channel_ids"] = dag_source_channel_ids
            if react_emoji is not None:
                emojis = [e.strip() for e in react_emoji.split(",") if e.strip()]
                for node in updated_graph.nodes:
                    if node.type == PipelineNodeType.REACT:
                        if len(emojis) > 1:
                            node.config["emoji"] = emojis[0]
                            node.config["random_emojis"] = emojis
                        else:
                            node.config["emoji"] = emojis[0] if emojis else "👍"
                            node.config["random_emojis"] = []
        pipeline = pipeline.model_copy(update={"pipeline_json": updated_graph})

        # For DAG pipelines, source/target are managed via pipeline_json nodes, not DB tables
        if is_dag:
            sources: list[int] = []
            targets: list[PipelineTarget] = []
        else:
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
                (
                    channels_by_id.get(source.channel_id).title or str(source.channel_id)
                    if channels_by_id.get(source.channel_id)
                    else str(source.channel_id)
                )
                for source in sources
            ],
        }

    async def get_with_relations(self, active_only: bool = False) -> list[dict]:
        pipelines = await self._bundle.get_all(active_only)
        if not pipelines:
            return []
        pipeline_ids = [p.id for p in pipelines if p.id is not None]
        channels, all_sources, all_targets = await asyncio.gather(
            self._bundle.list_channels(include_filtered=True),
            self._batch_sources(pipeline_ids),
            self._batch_targets(pipeline_ids),
        )
        channels_by_id = {channel.channel_id: channel for channel in channels}
        sources_by_pid = _group_by_pipeline(all_sources)
        targets_by_pid = _group_by_pipeline(all_targets)
        return [
            {
                "pipeline": p,
                "sources": sources_by_pid.get(p.id, []),
                "targets": targets_by_pid.get(p.id, []),
                "source_ids": [s.channel_id for s in sources_by_pid.get(p.id, [])],
                "target_refs": [
                    f"{t.phone}|{t.dialog_id}" for t in targets_by_pid.get(p.id, [])
                ],
                "source_titles": [
                    (
                        channels_by_id.get(s.channel_id).title or str(s.channel_id)
                        if channels_by_id.get(s.channel_id)
                        else str(s.channel_id)
                    )
                    for s in sources_by_pid.get(p.id, [])
                ],
            }
            for p in pipelines
        ]

    async def list_cached_dialogs_by_phone(
        self,
        active_only: bool = False,
    ) -> dict[str, list[dict]]:
        accounts = await self._bundle.list_accounts(active_only=active_only)
        dialogs = await asyncio.gather(
            *[self._bundle.list_cached_dialogs(account.phone) for account in accounts]
        )
        return {account.phone: rows for account, rows in zip(accounts, dialogs, strict=False)}

    async def _batch_sources(self, pipeline_ids: list[int]) -> list[PipelineSource]:
        """Load sources for all given pipeline IDs in a single query."""
        return await self._bundle.content_pipelines.batch_sources(pipeline_ids)

    async def _batch_targets(self, pipeline_ids: list[int]) -> list[PipelineTarget]:
        """Load targets for all given pipeline IDs in a single query."""
        return await self._bundle.content_pipelines.batch_targets(pipeline_ids)

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
        skip_llm_validation: bool = False,
    ) -> ContentPipeline:
        cleaned_name = name.strip()
        if not cleaned_name:
            raise PipelineValidationError("Название pipeline не может быть пустым.")
        cleaned_template = prompt_template.strip()
        if not skip_llm_validation:
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

    # ------------------------------------------------------------------
    # JSON import / export
    # ------------------------------------------------------------------

    async def export_json(self, pipeline_id: int) -> dict | None:
        """Export a pipeline as a JSON-serialisable dict."""
        detail = await self.get_detail(pipeline_id)
        if detail is None:
            return None
        pipeline: ContentPipeline = detail["pipeline"]
        data: dict[str, Any] = {
            "name": pipeline.name,
            "prompt_template": pipeline.prompt_template,
            "llm_model": pipeline.llm_model,
            "image_model": pipeline.image_model,
            "publish_mode": pipeline.publish_mode.value,
            "generation_backend": pipeline.generation_backend.value,
            "generate_interval_minutes": pipeline.generate_interval_minutes,
            "publish_times": pipeline.publish_times,
            "refinement_steps": pipeline.refinement_steps,
            "source_ids": detail["source_ids"],
            "target_refs": detail["target_refs"],
        }
        if pipeline.pipeline_json:
            data["pipeline_json"] = json.loads(pipeline.pipeline_json.to_json())
        return data

    async def import_json(
        self,
        data: dict | str,
        *,
        name_override: str | None = None,
    ) -> int:
        """Create a pipeline from a JSON export dict. Returns the new pipeline ID."""
        if isinstance(data, str):
            data = json.loads(data)
        name = name_override or data.get("name", "Imported pipeline")
        prompt_template = data.get("prompt_template", "")
        source_ids = [int(x) for x in data.get("source_ids", [])]
        target_refs_raw = data.get("target_refs", [])
        target_refs = []
        for ref in target_refs_raw:
            if isinstance(ref, str) and "|" in ref:
                phone, _, dialog_id = ref.partition("|")
                target_refs.append(PipelineTargetRef(phone=phone, dialog_id=int(dialog_id)))

        pipeline_json: PipelineGraph | None = None
        if "pipeline_json" in data:
            try:
                pipeline_json = PipelineGraph.from_json(data["pipeline_json"])
            except Exception:
                logger.warning("import_json: failed to parse pipeline_json field, ignoring")

        pipeline = await self._build_pipeline(
            name=name,
            prompt_template=prompt_template or ".",
            llm_model=data.get("llm_model"),
            image_model=data.get("image_model"),
            publish_mode=data.get("publish_mode", PipelinePublishMode.MODERATED),
            generation_backend=data.get("generation_backend", PipelineGenerationBackend.CHAIN),
            generate_interval_minutes=int(data.get("generate_interval_minutes", 60)),
            is_active=False,
        )
        pipeline = pipeline.model_copy(update={"pipeline_json": pipeline_json})

        # Imported pipelines may not include runtime data (source/target IDs valid in
        # the target environment); allow empty and create as inactive for later config.
        sources = await self._normalize_sources(source_ids) if source_ids else []
        targets = await self._normalize_targets(target_refs) if target_refs else []

        return await self._bundle.add(pipeline, sources, targets)

    # ------------------------------------------------------------------
    # Template operations
    # ------------------------------------------------------------------

    async def list_templates(self, category: str | None = None) -> list[PipelineTemplate]:
        """List all available pipeline templates."""
        if self._bundle.pipeline_templates is None:
            return []
        return await self._bundle.pipeline_templates.list_all(category)

    async def create_from_template(
        self,
        template_id: int,
        *,
        name: str,
        source_ids: list[int],
        target_refs: list[PipelineTargetRef],
        overrides: dict | None = None,
    ) -> int:
        """Create a new pipeline from a template. Returns pipeline ID."""
        if self._bundle.pipeline_templates is None:
            raise PipelineValidationError("Репозиторий шаблонов недоступен.")
        tpl = await self._bundle.pipeline_templates.get_by_id(template_id)
        if tpl is None:
            raise PipelineValidationError(f"Шаблон id={template_id} не найден.")

        graph = tpl.template_json
        overrides = overrides or {}

        # Extract legacy fields from the graph nodes for backward compat
        prompt_template = overrides.get("prompt_template", "")
        llm_model = overrides.get("llm_model")
        image_model = overrides.get("image_model")
        publish_mode = overrides.get("publish_mode", PipelinePublishMode.MODERATED)
        generation_backend = overrides.get("generation_backend", PipelineGenerationBackend.CHAIN)
        interval = int(overrides.get("generate_interval_minutes", 60))

        # Try to extract prompt_template from llm_generate node if not provided
        if not prompt_template:
            for node in graph.nodes:
                if node.type.value in ("llm_generate", "llm_refine"):
                    prompt_template = node.config.get("prompt_template") or node.config.get("prompt", "")
                    break
        if not prompt_template:
            prompt_template = name

        pipeline = await self._build_pipeline(
            name=name,
            prompt_template=prompt_template,
            llm_model=llm_model,
            image_model=image_model,
            publish_mode=publish_mode,
            generation_backend=generation_backend,
            generate_interval_minutes=interval,
            is_active=False,
        )
        pipeline = pipeline.model_copy(update={"pipeline_json": graph})

        # Inject source channel IDs into source nodes and target refs into publish/forward nodes
        if source_ids:
            clean_ids = sorted(set(source_ids))
            for node in graph.nodes:
                if node.type == PipelineNodeType.SOURCE and not node.config.get("channel_ids"):
                    node.config["channel_ids"] = clean_ids
        if target_refs:
            seen: set[tuple[str, int]] = set()
            target_dicts = []
            for t in target_refs:
                key = (t.phone, t.dialog_id)
                if key not in seen:
                    seen.add(key)
                    target_dicts.append({"phone": t.phone, "dialog_id": t.dialog_id})
            for node in graph.nodes:
                if node.type in (PipelineNodeType.PUBLISH, PipelineNodeType.FORWARD) and not node.config.get("targets"):
                    node.config["targets"] = target_dicts

        # Templates are created inactive; sources/targets are optional at creation time
        sources = await self._normalize_sources(source_ids) if source_ids else []
        targets = await self._normalize_targets(target_refs) if target_refs else []
        return await self._bundle.add(pipeline, sources, targets)

    async def edit_via_llm(self, pipeline_id: int, instruction: str, db: Database) -> dict:
        """Edit a pipeline's JSON config via LLM instruction.

        Returns {"ok": True, "pipeline_json": {...}} or {"ok": False, "error": "..."}.
        """
        pipeline = await self.get(pipeline_id)
        if pipeline is None:
            return {"ok": False, "error": f"Пайплайн id={pipeline_id} не найден."}

        import json as _json

        current_graph_json = (
            pipeline.pipeline_json.to_json() if pipeline.pipeline_json
            else _json.dumps({"nodes": [], "edges": []})
        )

        prompt = (
            "You are a pipeline configuration assistant. "
            "You receive the current pipeline JSON and a user instruction. "
            "Return ONLY the updated pipeline JSON object (no explanations, no markdown fences). "
            "Keep all existing nodes unless explicitly asked to remove them. "
            "Valid node types: source, retrieve_context, llm_generate, llm_refine, "
            "image_generate, publish, notify, filter, delay, react, forward, delete_message, "
            "condition, search_query_trigger.\n\n"
            f"Current pipeline JSON:\n{current_graph_json}\n\n"
            f"Instruction: {instruction}\n\n"
            "Return the updated JSON:"
        )

        try:
            from src.services.provider_service import AgentProviderService
            provider_service = AgentProviderService(db)
            provider_callable = provider_service.get_provider_callable(pipeline.llm_model)
            result = await provider_callable(prompt, model=pipeline.llm_model or "", max_tokens=4096, temperature=0.2)
            raw = result if isinstance(result, str) else (result.get("text") or result.get("generated_text") or "")
            # Strip markdown fences if present
            raw = raw.strip()
            if raw.startswith("```"):
                raw = raw.split("```", 2)[1]
                if raw.startswith("json"):
                    raw = raw[4:]
                raw = raw.split("```")[0].strip()
            new_graph = PipelineGraph.from_json(raw)
            # Save the updated graph
            await db.repos.content_pipelines.set_pipeline_json(pipeline_id, new_graph)
            return {"ok": True, "pipeline_json": _json.loads(new_graph.to_json())}
        except Exception as exc:
            logger.warning("edit_via_llm failed for pipeline_id=%s: %s", pipeline_id, exc, exc_info=True)
            return {"ok": False, "error": str(exc)}

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
                    "Сначала откройте 'Диалоги' для этого аккаунта."
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
