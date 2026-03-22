from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass

from src.config import AppConfig
from src.database import Database
from src.database.bundles import SearchBundle
from src.settings_utils import parse_int_setting

logger = logging.getLogger(__name__)

LAST_EMBEDDED_ID_SETTING = "semantic_last_embedded_id"
EMBEDDINGS_PROVIDER_SETTING = "semantic_embeddings_provider"
EMBEDDINGS_MODEL_SETTING = "semantic_embeddings_model"
EMBEDDINGS_API_KEY_SETTING = "semantic_embeddings_api_key"
EMBEDDINGS_BASE_URL_SETTING = "semantic_embeddings_base_url"
EMBEDDINGS_BATCH_SIZE_SETTING = "semantic_embeddings_batch_size"
DEFAULT_EMBEDDINGS_PROVIDER = "openai"
DEFAULT_EMBEDDINGS_MODEL = "text-embedding-3-small"
DEFAULT_EMBEDDINGS_BATCH_SIZE = 64


@dataclass(slots=True, frozen=True)
class EmbeddingRuntimeConfig:
    provider: str
    model: str
    api_key: str
    base_url: str
    batch_size: int

    @property
    def model_ref(self) -> str:
        if ":" in self.model:
            return self.model
        return f"{self.provider}:{self.model}"


class EmbeddingService:
    def __init__(self, search: SearchBundle | Database, config: AppConfig | None = None):
        if isinstance(search, Database):
            search = SearchBundle.from_database(search)
        self._search = search
        self._config = config
        self._embeddings = None
        self._embeddings_key: tuple[str, str, str, str] | None = None

    async def _runtime_config(self) -> EmbeddingRuntimeConfig:
        provider = (
            await self._search.get_setting(EMBEDDINGS_PROVIDER_SETTING)
            or (self._config.llm.provider if self._config else "")
            or DEFAULT_EMBEDDINGS_PROVIDER
        ).strip()
        model = (
            await self._search.get_setting(EMBEDDINGS_MODEL_SETTING) or DEFAULT_EMBEDDINGS_MODEL
        ).strip()
        api_key = (
            await self._search.get_setting(EMBEDDINGS_API_KEY_SETTING)
            or (self._config.llm.api_key if self._config else "")
        ).strip()
        base_url = (await self._search.get_setting(EMBEDDINGS_BASE_URL_SETTING) or "").strip()
        batch_size = parse_int_setting(
            await self._search.get_setting(EMBEDDINGS_BATCH_SIZE_SETTING),
            setting_name=EMBEDDINGS_BATCH_SIZE_SETTING,
            default=DEFAULT_EMBEDDINGS_BATCH_SIZE,
            logger=logger,
        )
        return EmbeddingRuntimeConfig(
            provider=provider,
            model=model,
            api_key=api_key,
            base_url=base_url,
            batch_size=max(1, batch_size),
        )

    async def _get_embeddings(self):
        if not self._search.vec_available and not self._search.numpy_available:
            raise RuntimeError(
                "Semantic search is unavailable: sqlite-vec extension is not loaded "
                "and numpy fallback index is not initialised."
            )
        cfg = await self._runtime_config()
        cache_key = (cfg.provider, cfg.model, cfg.api_key, cfg.base_url)
        if self._embeddings is not None and self._embeddings_key == cache_key:
            return self._embeddings
        try:
            from langchain.embeddings import init_embeddings
        except ImportError as exc:
            raise RuntimeError("LangChain embeddings support is not installed.") from exc

        kwargs: dict[str, str] = {}
        if cfg.api_key:
            kwargs["api_key"] = cfg.api_key
        if cfg.base_url:
            kwargs["base_url"] = cfg.base_url
        try:
            embeddings = init_embeddings(cfg.model_ref, **kwargs)
        except ImportError as exc:
            package = f"langchain-{cfg.provider.replace('_', '-')}"
            raise RuntimeError(
                f"Embedding provider '{cfg.provider}' is unavailable. Install '{package}'."
            ) from exc
        except Exception as exc:
            raise RuntimeError(f"Failed to initialize embeddings: {exc}") from exc
        self._embeddings = embeddings
        self._embeddings_key = cache_key
        return embeddings

    async def _embed_documents(self, texts: list[str]) -> list[list[float]]:
        embeddings = await self._get_embeddings()
        if hasattr(embeddings, "aembed_documents"):
            return await embeddings.aembed_documents(texts)
        return await asyncio.to_thread(embeddings.embed_documents, texts)

    async def embed_query(self, query: str) -> list[float]:
        embeddings = await self._get_embeddings()
        if hasattr(embeddings, "aembed_query"):
            return await embeddings.aembed_query(query)
        return await asyncio.to_thread(embeddings.embed_query, query)

    async def index_pending_messages(
        self,
        *,
        batch_size: int | None = None,
        max_batches: int | None = None,
    ) -> int:
        cfg = await self._runtime_config()
        current_batch_size = max(1, batch_size or cfg.batch_size)
        last_embedded_id = parse_int_setting(
            await self._search.get_setting(LAST_EMBEDDED_ID_SETTING),
            setting_name=LAST_EMBEDDED_ID_SETTING,
            default=0,
            logger=logger,
        )
        indexed = 0
        batches = 0
        while max_batches is None or batches < max_batches:
            pending = await self._search.messages.get_messages_for_embedding(
                after_id=last_embedded_id,
                limit=current_batch_size,
            )
            if not pending:
                break
            message_ids = [message_id for message_id, _text in pending]
            texts = [text for _message_id, text in pending]
            vectors = await self._embed_documents(texts)
            await self._search.messages.upsert_message_embeddings(
                list(zip(message_ids, vectors))
            )
            await self._search.messages.upsert_message_embedding_json(
                list(zip(message_ids, vectors))
            )
            last_embedded_id = message_ids[-1]
            await self._search.set_setting(LAST_EMBEDDED_ID_SETTING, str(last_embedded_id))
            indexed += len(message_ids)
            batches += 1
        return indexed
