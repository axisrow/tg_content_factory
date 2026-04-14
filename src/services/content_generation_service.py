from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from src.database import Database
from src.models import ContentPipeline, GenerationRun, PipelineGenerationBackend, PipelinePublishMode
from src.search.engine import SearchEngine
from src.services.generation_service import GenerationService

if TYPE_CHECKING:
    from src.services.draft_notification_service import DraftNotificationService
    from src.services.quality_scoring_service import QualityScoringService

logger = logging.getLogger(__name__)


class ContentGenerationService:
    """Orchestrates content generation from pipeline configuration.

    This service coordinates:
    - Creating generation_runs records
    - Invoking GenerationService (RAG) or AgentManager (deep agents)
    - Handling image generation stub
    - Updating generation_runs with results
    - Setting initial moderation_status
    """

    def __init__(
        self,
        db: Database,
        search_engine: SearchEngine,
        agent_manager: Any | None = None,
        image_service: Any | None = None,
        notification_service: "DraftNotificationService | None" = None,
        quality_service: "QualityScoringService | None" = None,
        client_pool: Any | None = None,
        provider_service: Any | None = None,
    ) -> None:
        self._db = db
        self._search = search_engine
        self._agent_manager = agent_manager
        self._image_service = image_service
        self._notification_service = notification_service
        self._quality_service = quality_service
        self._client_pool = client_pool
        self._provider_service = provider_service

    async def generate(
        self,
        pipeline: ContentPipeline,
        model: str | None = None,
        max_tokens: int = 512,
        temperature: float = 0.7,
        dry_run: bool = False,
        since_hours: float = 24.0,
    ) -> GenerationRun:
        """Generate content for a pipeline and return the generation run.

        Steps:
        1. Create generation_runs record (status='running')
        2. Retrieve context messages from pipeline sources
        3. Render prompt template with source messages
        4. Call backend (GenerationService RAG or AgentManager)
        5. Handle image generation if image_model is set (skipped for dry_run)
        6. Save result with moderation_status='pending'
        7. Return the GenerationRun

        If dry_run=True: skips image generation and draft notifications; marks
        metadata with dry_run=True so callers can distinguish test runs.
        """
        run_id = await self._db.repos.generation_runs.create_run(
            pipeline_id=pipeline.id,
            prompt=pipeline.prompt_template,
        )
        try:
            await self._db.repos.generation_runs.set_status(run_id, "running")
        except Exception:
            await self._db.repos.generation_runs.set_status(run_id, "failed")
            raise

        try:
            result = await self._run_generation(
                pipeline=pipeline,
                model=model,
                max_tokens=max_tokens,
                temperature=temperature,
                since_hours=since_hours,
            )
            generated_text = result.get("generated_text", "")
            effective_publish_mode = result.get("publish_mode") or pipeline.publish_mode.value
            metadata: dict[str, Any] = {
                "citations": result.get("citations", []),
                "effective_publish_mode": effective_publish_mode,
            }
            if dry_run:
                metadata["dry_run"] = True
            if result.get("publish_reply"):
                metadata["publish_reply"] = True
            if result.get("reply_to_message_id") is not None:
                metadata["reply_to_message_id"] = result["reply_to_message_id"]

            if pipeline.refinement_steps and generated_text and pipeline.pipeline_json is None:
                # Refinement steps are only applied for legacy pipelines (graph-based ones encode them as nodes)
                generated_text = await self._apply_refinement_steps(
                    generated_text, pipeline, model, max_tokens, temperature
                )
                metadata["refinement_steps_applied"] = len(pipeline.refinement_steps)

            # Use image_url from graph executor if available, otherwise fall back to legacy image gen
            # Image generation is skipped for dry runs to save time and cost
            if not dry_run:
                image_url_from_graph = result.get("image_url")
                if image_url_from_graph:
                    await self._db.execute(
                        "UPDATE generation_runs SET image_url = ? WHERE id = ?",
                        (image_url_from_graph, run_id),
                    )
                else:
                    image_model = pipeline.image_model
                    if not image_model:
                        image_model = await self._db.get_setting("default_image_model") or ""
                    if image_model and pipeline.pipeline_json is None:
                        image_url = await self._generate_image(pipeline, generated_text, model=image_model)
                        if image_url:
                            await self._db.execute(
                                "UPDATE generation_runs SET image_url = ? WHERE id = ?",
                                (image_url, run_id),
                            )

            await self._db.repos.generation_runs.save_result(run_id, generated_text, metadata)
            if self._quality_service and generated_text:
                quality = await self._quality_service.score_content(
                    generated_text,
                    model=pipeline.llm_model,
                )
                await self._db.repos.generation_runs.set_quality_score(
                    run_id,
                    quality.overall,
                    quality.issues,
                )
            run = await self._db.repos.generation_runs.get(run_id)
            if run is None:
                raise RuntimeError(f"Generation run {run_id} not found after save")

            if (
                not dry_run
                and self._notification_service
                and run.moderation_status == "pending"
                and effective_publish_mode == PipelinePublishMode.MODERATED.value
            ):
                try:
                    await self._notification_service.notify_new_draft(run, pipeline)
                except Exception:
                    logger.warning("Failed to send draft notification", exc_info=True)
            return run
        except Exception:
            logger.exception(
                "Content generation failed for pipeline_id=%s run_id=%s",
                pipeline.id,
                run_id,
            )
            await self._db.repos.generation_runs.set_status(run_id, "failed")
            raise

    async def _run_generation(
        self,
        pipeline: ContentPipeline,
        model: str | None,
        max_tokens: int,
        temperature: float,
        since_hours: float = 24.0,
    ) -> dict[str, Any]:
        """Execute the generation backend."""
        if pipeline.pipeline_json is not None:
            return await self._run_graph(pipeline, model, max_tokens, temperature, since_hours)

        if pipeline.generation_backend == PipelineGenerationBackend.DEEP_AGENTS:
            return await self._run_deep_agents(pipeline, model, max_tokens, temperature)

        return await self._run_rag(pipeline, model, max_tokens, temperature)

    async def _run_graph(
        self,
        pipeline: ContentPipeline,
        model: str | None,
        max_tokens: int,
        temperature: float,
        since_hours: float = 24.0,
    ) -> dict[str, Any]:
        """Execute the pipeline using the node-based DAG executor."""
        from src.services.pipeline_executor import PipelineExecutor

        provider_callable = self._get_provider_callable(pipeline.llm_model)

        default_image_model = await self._db.get_setting("default_image_model") or ""

        services = {
            "search_engine": self._search,
            "provider_callable": provider_callable,
            "image_service": self._image_service,
            "notification_service": self._notification_service,
            "client_pool": self._client_pool,
            "default_model": model or pipeline.llm_model or "",
            "default_image_model": pipeline.image_model or default_image_model,
            "db": self._db,
            "since_hours": since_hours,
        }

        # Inject read-only agent tools for AgentLoopHandler
        try:
            from src.agent.tools import build_agent_tools_dict

            services["agent_tools"] = build_agent_tools_dict(
                db=self._db,
                client_pool=self._client_pool,
                search_engine=self._search,
            )
        except Exception:
            logger.debug("agent_tools unavailable for pipeline execution", exc_info=True)

        executor = PipelineExecutor()
        result = await executor.execute(pipeline, pipeline.pipeline_json, services)

        return {
            "generated_text": result.get("generated_text", ""),
            "image_url": result.get("image_url"),
            "citations": result.get("citations", []),
            "publish_mode": result.get("publish_mode"),
            "publish_reply": result.get("publish_reply", False),
            "reply_to_message_id": result.get("reply_to_message_id"),
        }

    async def _run_rag(
        self,
        pipeline: ContentPipeline,
        model: str | None,
        max_tokens: int,
        temperature: float,
    ) -> dict[str, Any]:
        """Run RAG-based generation using GenerationService."""
        provider_callable = self._get_provider_callable(pipeline.llm_model)

        gen = GenerationService(self._search, provider_callable=provider_callable)

        # Use pipeline name as search query; constrain to first source channel if available
        query = pipeline.name or ""
        channel_id: int | None = None
        try:
            sources = await self._db.repos.content_pipelines.list_sources(pipeline.id)
            if len(sources) == 1:
                channel_id = sources[0].channel_id
            # For multi-source pipelines keep channel_id=None to retrieve from all channels
        except Exception:
            logger.warning(
                "Failed to load pipeline sources for %s, continuing without channel scoping",
                pipeline.id,
                exc_info=True,
            )

        return await gen.generate(
            query=query,
            prompt_template=pipeline.prompt_template,
            model=(model or pipeline.llm_model),
            max_tokens=max_tokens,
            temperature=temperature,
            channel_id=channel_id,
        )

    async def _run_deep_agents(
        self,
        pipeline: ContentPipeline,
        model: str | None,
        max_tokens: int,
        temperature: float,
    ) -> dict[str, Any]:
        """Run generation using AgentManager deep agents backend."""
        if self._agent_manager is None:
            raise RuntimeError("AgentManager not configured for deep_agents generation")

        import json

        prompt = pipeline.prompt_template or pipeline.name or ""
        full_text = ""

        async def collect():
            nonlocal full_text
            async for chunk in self._agent_manager.chat_stream(
                thread_id=0,
                message=prompt,
                model=model,
            ):
                if chunk.startswith("data: "):
                    try:
                        data = json.loads(chunk[6:].strip())
                        if "text" in data:
                            full_text = data["text"]
                        elif "full_text" in data:
                            full_text = data["full_text"]
                    except json.JSONDecodeError:
                        pass

        await collect()

        return {
            "generated_text": full_text,
            "citations": [],
        }

    async def _apply_refinement_steps(
        self,
        text: str,
        pipeline: ContentPipeline,
        model: str | None,
        max_tokens: int,
        temperature: float,
    ) -> str:
        """Apply each refinement step sequentially, replacing {text} with current output."""
        provider_callable = self._get_provider_callable(pipeline.llm_model)

        for step in pipeline.refinement_steps:
            step_prompt = step.get("prompt", "")
            if not step_prompt:
                continue
            rendered = step_prompt.replace("{text}", text)
            try:
                result = await provider_callable(
                    rendered,
                    model=model or pipeline.llm_model or "",
                    max_tokens=max_tokens,
                    temperature=temperature,
                )
                refined = (
                    result if isinstance(result, str)
                    else (result.get("text") or result.get("generated_text") or "")
                )
                if refined:
                    text = refined
            except Exception:
                logger.warning(
                    "Refinement step %r failed for pipeline_id=%s; keeping previous text",
                    step.get("name", "unnamed"),
                    pipeline.id,
                    exc_info=True,
                )
        return text

    async def _generate_image(
        self, pipeline: ContentPipeline, text: str, *, model: str | None = None
    ) -> str | None:
        """Generate image for the content.

        Until the real image-generation service is wired, image generation should
        degrade gracefully instead of failing an otherwise valid text run.
        """
        if self._image_service is None:
            logger.info(
                "Skipping image generation for pipeline_id=%s because no image service is configured",
                pipeline.id,
            )
            return None
        return await self._image_service.generate(model or pipeline.image_model, text)

    def _get_provider_callable(self, model: str | None) -> Any:
        """Resolve provider callable — prefer injected shared service, fall back to local."""
        if self._provider_service is not None:
            return self._provider_service.get_provider_callable(model)
        from src.services.provider_service import AgentProviderService

        return AgentProviderService(self._db).get_provider_callable(model)
