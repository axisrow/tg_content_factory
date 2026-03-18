from __future__ import annotations

import logging
from typing import Any

from src.database import Database
from src.models import ContentPipeline, GenerationRun, PipelineGenerationBackend
from src.search.engine import SearchEngine
from src.services.generation_service import GenerationService

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
    ) -> None:
        self._db = db
        self._search = search_engine
        self._agent_manager = agent_manager
        self._image_service = image_service

    async def generate(
        self,
        pipeline: ContentPipeline,
        model: str | None = None,
        max_tokens: int = 512,
        temperature: float = 0.7,
    ) -> GenerationRun:
        """Generate content for a pipeline and return the generation run.

        Steps:
        1. Create generation_runs record (status='running')
        2. Retrieve context messages from pipeline sources
        3. Render prompt template with source messages
        4. Call backend (GenerationService RAG or AgentManager)
        5. Handle image generation if image_model is set (stub: NotImplementedError)
        6. Save result with moderation_status='pending'
        7. Return the GenerationRun
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
            )
            generated_text = result.get("generated_text", "")
            metadata: dict[str, Any] = {"citations": result.get("citations", [])}

            if pipeline.image_model:
                image_url = await self._generate_image(pipeline, generated_text)
                if image_url:
                    await self._db.execute(
                        "UPDATE generation_runs SET image_url = ? WHERE id = ?",
                        (image_url, run_id),
                    )

            await self._db.repos.generation_runs.save_result(run_id, generated_text, metadata)
            run = await self._db.repos.generation_runs.get(run_id)
            if run is None:
                raise RuntimeError(f"Generation run {run_id} not found after save")
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
    ) -> dict[str, Any]:
        """Execute the generation backend."""
        if pipeline.generation_backend == PipelineGenerationBackend.DEEP_AGENTS:
            return await self._run_deep_agents(pipeline, model, max_tokens, temperature)

        return await self._run_rag(pipeline, model, max_tokens, temperature)

    async def _run_rag(
        self,
        pipeline: ContentPipeline,
        model: str | None,
        max_tokens: int,
        temperature: float,
    ) -> dict[str, Any]:
        """Run RAG-based generation using GenerationService."""
        from src.services.provider_service import AgentProviderService

        provider_service = AgentProviderService(self._db)
        provider_callable = provider_service.get_provider_callable(pipeline.llm_model)

        gen = GenerationService(self._search, provider_callable=provider_callable)

        query = pipeline.prompt_template or pipeline.name or ""

        return await gen.generate(
            query=query,
            prompt_template=pipeline.prompt_template,
            model=(model or pipeline.llm_model),
            max_tokens=max_tokens,
            temperature=temperature,
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

    async def _generate_image(self, pipeline: ContentPipeline, text: str) -> str | None:
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

        return await self._image_service.generate(pipeline.image_model, text)
