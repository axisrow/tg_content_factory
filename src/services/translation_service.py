from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from src.database import Database
    from src.models import Message
    from src.services.provider_service import AgentProviderService

# DB settings keys
TRANSLATION_PROVIDER = "translation_provider"
TRANSLATION_MODEL = "translation_model"
TRANSLATION_TARGET_LANG = "translation_target_lang"
TRANSLATION_SOURCE_FILTER = "translation_source_filter"
TRANSLATION_AUTO_ON_COLLECT = "translation_auto_on_collect"


class TranslationService:
    """Language detection and LLM-powered translation for messages."""

    def __init__(
        self,
        db: Database,
        provider_service: AgentProviderService | None = None,
    ) -> None:
        self._db = db
        self._provider_service = provider_service

    # ── language detection ───────────────────────────────────────────

    @staticmethod
    def detect_language(text: str | None) -> str | None:
        """Detect language using langdetect. Returns ISO 639-1 code or None."""
        if not text or len(text.strip()) < 8:
            return None
        try:
            from langdetect import detect

            return detect(text)
        except Exception:
            return None

    # ── single message translation ──────────────────────────────────

    async def translate_message(
        self,
        text: str,
        source_lang: str,
        target_lang: str,
        provider_name: str | None = None,
        model: str | None = None,
    ) -> str | None:
        """Translate text using LLM provider. Returns translated text or None.

        Skips translation when source_lang == target_lang.
        """
        if source_lang == target_lang:
            return None
        if not self._provider_service:
            logger.warning("No provider service configured for translation")
            return None

        provider = self._provider_service.get_provider_callable(provider_name)
        prompt = (
            f"Translate the following text from {source_lang} to {target_lang}. "
            "Return ONLY the translation, no explanations.\n\n"
            f"{text}"
        )
        try:
            result = await provider(prompt=prompt, model=model, max_tokens=2048, temperature=0.1)
            return result.strip() if result else None
        except Exception:
            logger.exception("Translation failed for text (%.40s...)", text[:40] if text else "")
            return None

    # ── batch translation ───────────────────────────────────────────

    async def translate_batch(
        self,
        messages: list[Message],
        target_lang: str,
        provider_name: str | None = None,
        model: str | None = None,
    ) -> list[tuple[int, str]]:
        """Translate multiple messages in a single LLM call.

        Returns list of (message_db_id, translated_text).
        """
        if not messages or not self._provider_service:
            return []

        # Filter out messages where source == target
        to_translate = [
            m for m in messages
            if m.detected_lang and m.detected_lang != target_lang and m.text
        ]
        if not to_translate:
            return []

        # Build numbered prompt
        lines = []
        for i, m in enumerate(to_translate, 1):
            # Truncate very long messages to avoid token limits
            text = m.text[:2000] if m.text else ""
            lines.append(f"{i}: {text}")

        numbered_block = "\n".join(lines)
        prompt = (
            f"Translate the following numbered messages to {target_lang}. "
            "Return ONLY the translations, one per line, keeping the number prefix.\n\n"
            f"{numbered_block}"
        )

        provider = self._provider_service.get_provider_callable(provider_name)
        try:
            result = await provider(prompt=prompt, model=model, max_tokens=4096, temperature=0.1)
        except Exception:
            logger.exception("Batch translation failed")
            return []

        if not result:
            return []

        # Parse numbered response
        translations = self._parse_numbered_response(result, len(to_translate))
        output: list[tuple[int, str]] = []
        for i, m in enumerate(to_translate):
            if i in translations and m.id is not None:
                output.append((m.id, translations[i]))

        return output

    @staticmethod
    def _parse_numbered_response(response: str, expected_count: int) -> dict[int, str]:
        """Parse '1: translated text' format into {0-based-index: text} dict."""
        result: dict[int, str] = {}
        for line in response.strip().splitlines():
            line = line.strip()
            if not line:
                continue
            match = re.match(r"^(\d+)\s*[:\.]\s*(.+)$", line)
            if match:
                num = int(match.group(1))
                text = match.group(2).strip()
                if 1 <= num <= expected_count and text:
                    result[num - 1] = text  # 0-based
        return result

    # ── settings ────────────────────────────────────────────────────

    async def get_settings(self) -> dict[str, str | None]:
        """Load translation settings from DB."""
        keys = [
            TRANSLATION_PROVIDER,
            TRANSLATION_MODEL,
            TRANSLATION_TARGET_LANG,
            TRANSLATION_SOURCE_FILTER,
            TRANSLATION_AUTO_ON_COLLECT,
        ]
        settings: dict[str, str | None] = {}
        for key in keys:
            settings[key] = await self._db.get_setting(key)
        return settings

    def get_source_filter(self, raw: str | None) -> list[str]:
        """Parse comma-separated source filter string into list of lang codes."""
        if not raw:
            return []
        return [lang.strip().lower() for lang in raw.split(",") if lang.strip()]
