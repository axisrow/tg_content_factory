"""Concrete handlers for all pipeline node types."""
from __future__ import annotations

import asyncio
import logging
import random
import re

from src.services.pipeline_nodes.base import BaseNodeHandler, NodeContext

logger = logging.getLogger(__name__)


class SourceHandler(BaseNodeHandler):
    """Source node: declares which channel IDs to use as data sources."""

    async def execute(self, node_config: dict, context: NodeContext, services: dict) -> None:
        channel_ids = node_config.get("channel_ids", [])
        context.set_global("source_channel_ids", channel_ids)


class FetchMessagesHandler(BaseNodeHandler):
    """Fetch recent messages from source channels into context."""

    async def execute(self, node_config: dict, context: NodeContext, services: dict) -> None:
        db = services.get("db")
        if db is None:
            context.set_global("context_messages", [])
            return
        channel_ids = context.get_global("source_channel_ids", [])
        since_hours = float(services.get("since_hours", context.get_global("since_hours", 24.0)))
        messages = await db.repos.messages.get_recent_for_channels(channel_ids, since_hours)
        context.set_global("context_messages", messages)


class RetrieveContextHandler(BaseNodeHandler):
    """Retrieve context from search engine."""

    async def execute(self, node_config: dict, context: NodeContext, services: dict) -> None:
        search_engine = services.get("search_engine")
        if search_engine is None:
            logger.warning("RetrieveContextHandler: no search_engine in services, skipping")
            context.set_global("context_messages", [])
            return

        query = context.get_global("generation_query", "") or context.get_global("prompt_template", "")
        limit = int(node_config.get("limit", 8))
        method = node_config.get("method", "hybrid")

        try:
            if method == "hybrid" and search_engine.semantic_available:
                result = await search_engine.search_hybrid(query, limit=limit)
            elif method == "semantic" and search_engine.semantic_available:
                result = await search_engine.search_semantic(query, limit=limit)
            else:
                result = await search_engine.search_local(query, limit=limit)
            context.set_global("context_messages", result.messages)
        except Exception:
            logger.warning("RetrieveContextHandler: search failed, using empty context", exc_info=True)
            context.set_global("context_messages", [])


class LlmGenerateHandler(BaseNodeHandler):
    """Generate text via LLM using retrieved context."""

    async def execute(self, node_config: dict, context: NodeContext, services: dict) -> None:
        provider_callable = services.get("provider_callable")
        if provider_callable is None:
            raise RuntimeError("LlmGenerateHandler: no provider_callable in services")

        from datetime import datetime

        from src.agent.prompt_template import render_prompt_template

        prompt_template = node_config.get("prompt_template") or context.get_global("prompt_template", "")
        max_tokens = int(node_config.get("max_tokens", 2000))
        temperature = float(node_config.get("temperature", 0.7))
        model = node_config.get("model") or services.get("default_model") or ""

        # Build source messages string from context
        messages = context.get_global("context_messages", [])
        source_parts = []
        for m in messages:
            text = (m.text or "").strip()
            if not text:
                continue
            header = m.channel_title or m.channel_username or ""
            when = m.date.isoformat() if isinstance(m.date, datetime) else str(m.date)
            source_parts.append(f"[{header}] {text} (id:{m.message_id} date:{when})")
        source_messages = "\n\n".join(source_parts)

        rendered = render_prompt_template(
            prompt_template,
            {
                "source_messages": source_messages,
                "query": context.get_global("generation_query", ""),
            },
        )

        result = await provider_callable(
            rendered,
            model=model,
            max_tokens=max_tokens,
            temperature=temperature,
        )
        if isinstance(result, str):
            generated_text = result
            citations: list = []
        else:
            generated_text = result.get("text") or result.get("generated_text") or ""
            citations = result.get("citations", [])
        context.set_global("generated_text", generated_text)
        context.set_global("citations", citations)


class LlmRefineHandler(BaseNodeHandler):
    """Refine/rewrite text using LLM."""

    async def execute(self, node_config: dict, context: NodeContext, services: dict) -> None:
        provider_callable = services.get("provider_callable")
        if provider_callable is None:
            raise RuntimeError("LlmRefineHandler: no provider_callable in services")

        text = context.get_global("generated_text", "") or ""
        # If no text generated yet, try to use source messages as input
        if not text:
            messages = context.get_global("context_messages", [])
            parts = []
            for m in messages:
                t = (m.text or "").strip()
                if t:
                    parts.append(t)
            text = "\n\n".join(parts[:3])

        prompt = node_config.get("prompt", "Перепиши следующий текст:\n\n{text}")
        rendered = prompt.replace("{text}", text)
        max_tokens = int(node_config.get("max_tokens", 1000))
        temperature = float(node_config.get("temperature", 0.7))
        model = node_config.get("model") or services.get("default_model") or ""

        result = await provider_callable(
            rendered,
            model=model,
            max_tokens=max_tokens,
            temperature=temperature,
        )
        refined = result if isinstance(result, str) else (result.get("text") or result.get("generated_text") or "")
        if refined:
            context.set_global("generated_text", refined)


class ImageGenerateHandler(BaseNodeHandler):
    """Generate an image based on generated text."""

    async def execute(self, node_config: dict, context: NodeContext, services: dict) -> None:
        image_service = services.get("image_service")
        if image_service is None:
            logger.info("ImageGenerateHandler: no image_service configured, skipping")
            return

        text = context.get_global("generated_text", "") or ""
        model = node_config.get("model") or services.get("default_image_model") or ""
        if not model:
            logger.info("ImageGenerateHandler: no image model configured, skipping")
            return

        try:
            image_url = await image_service.generate(model, text)
            if image_url:
                context.set_global("image_url", image_url)
        except Exception:
            logger.warning("ImageGenerateHandler: image generation failed", exc_info=True)


class PublishHandler(BaseNodeHandler):
    """Publish generated content to target dialogs."""

    async def execute(self, node_config: dict, context: NodeContext, services: dict) -> None:
        # Actual publishing is handled by PublishService post-execution
        # Store publish config in context for the executor to use
        targets = node_config.get("targets", [])
        mode = node_config.get("mode", "moderated")
        reply = bool(node_config.get("reply", False))
        context.set_global("publish_targets", targets)
        context.set_global("publish_mode", mode)
        context.set_global("publish_reply", reply)
        # When reply=True, capture the first matched message ID as the reply target
        if reply:
            messages = context.get_global("context_messages", [])
            if messages:
                first = messages[0]
                context.set_global("reply_to_message_id", getattr(first, "message_id", None))


class NotifyHandler(BaseNodeHandler):
    """Send a notification via the notification bot."""

    async def execute(self, node_config: dict, context: NodeContext, services: dict) -> None:
        notification_service = services.get("notification_service")
        if notification_service is None:
            logger.info("NotifyHandler: no notification_service configured, skipping")
            return

        text = context.get_global("generated_text", "") or context.get_global("trigger_text", "") or ""
        template = node_config.get("message_template", "{text}")
        channel_title = context.get_global("trigger_channel_title", "")
        message = template.replace("{text}", text).replace("{channel_title}", channel_title)

        try:
            await notification_service.send_text(message)
        except Exception:
            logger.warning("NotifyHandler: failed to send notification", exc_info=True)


class FilterHandler(BaseNodeHandler):
    """Filter messages by various criteria."""

    async def execute(self, node_config: dict, context: NodeContext, services: dict) -> None:
        filter_type = node_config.get("type", "keywords")
        messages = context.get_global("context_messages", [])
        filtered = []

        if filter_type == "keywords":
            keywords = [k.lower() for k in node_config.get("keywords", []) if k]
            match_links = bool(node_config.get("match_links", False))
            for m in messages:
                text_lower = (m.text or "").lower()
                if match_links and re.search(r"https?://\S+|t\.me/\S+", m.text or ""):
                    filtered.append(m)
                elif any(kw in text_lower for kw in keywords):
                    filtered.append(m)

        elif filter_type == "service_message":
            service_types = node_config.get("service_types", ["user_joined", "user_left"])
            for m in messages:
                text_lower = (m.text or "").lower()
                if any(st in text_lower for st in service_types):
                    filtered.append(m)

        elif filter_type == "anonymous_sender":
            for m in messages:
                if m.sender_id is None or m.sender_name is None:
                    filtered.append(m)

        elif filter_type == "regex":
            pattern_str = node_config.get("pattern", "")
            if pattern_str:
                try:
                    pattern = re.compile(pattern_str, re.IGNORECASE)
                    for m in messages:
                        if pattern.search(m.text or ""):
                            filtered.append(m)
                except re.error:
                    logger.warning("FilterHandler: invalid regex pattern: %s", pattern_str)

        else:
            filtered = messages

        context.set_global("filtered_messages", filtered)
        context.set_global("context_messages", filtered)


class DelayHandler(BaseNodeHandler):
    """Wait for a random or fixed delay."""

    async def execute(self, node_config: dict, context: NodeContext, services: dict) -> None:
        min_sec = float(node_config.get("min_seconds", 0))
        max_sec = float(node_config.get("max_seconds", 0))
        if max_sec > min_sec:
            delay = random.uniform(min_sec, max_sec)
        else:
            delay = min_sec
        if delay > 0:
            await asyncio.sleep(delay)


class ReactHandler(BaseNodeHandler):
    """Put a reaction on messages (requires Telegram client)."""

    async def execute(self, node_config: dict, context: NodeContext, services: dict) -> None:
        client_pool = services.get("client_pool")
        if client_pool is None:
            logger.warning("ReactHandler: no client_pool, skipping")
            return

        messages = context.get_global("context_messages", [])
        emoji = node_config.get("emoji", "👍")
        random_emoji_list = node_config.get("random_emojis", [])

        for message in messages:
            acquired_phone: str | None = None
            try:
                result = await client_pool.get_available_client()
                if result is None:
                    break
                session, acquired_phone = result
                chosen_emoji = random.choice(random_emoji_list) if random_emoji_list else emoji
                await session.send_reaction(message.channel_id, message.message_id, chosen_emoji)
            except Exception:
                logger.warning("ReactHandler: failed to react to message %s", message.message_id, exc_info=True)
            finally:
                if acquired_phone is not None:
                    await client_pool.release_client(acquired_phone)


class ForwardHandler(BaseNodeHandler):
    """Forward messages to target dialogs (requires Telegram client)."""

    async def execute(self, node_config: dict, context: NodeContext, services: dict) -> None:
        client_pool = services.get("client_pool")
        if client_pool is None:
            logger.warning("ForwardHandler: no client_pool, skipping")
            return

        messages = context.get_global("context_messages", [])
        targets = node_config.get("targets", [])

        for target in targets:
            phone = target.get("phone", "")
            dialog_id = target.get("dialog_id")
            if not phone or not dialog_id:
                continue
            acquired_phone: str | None = None
            try:
                result = await client_pool.get_client_by_phone(phone)
                if result is None:
                    continue
                session, acquired_phone = result
                for message in messages:
                    await session.forward_messages(dialog_id, message.message_id, message.channel_id)
            except Exception:
                logger.warning("ForwardHandler: failed to forward to %s", dialog_id, exc_info=True)
            finally:
                if acquired_phone is not None:
                    await client_pool.release_client(acquired_phone)


class DeleteMessageHandler(BaseNodeHandler):
    """Delete filtered messages (requires Telegram client)."""

    async def execute(self, node_config: dict, context: NodeContext, services: dict) -> None:
        client_pool = services.get("client_pool")
        if client_pool is None:
            logger.warning("DeleteMessageHandler: no client_pool, skipping")
            return

        messages = context.get_global("context_messages", [])

        for message in messages:
            acquired_phone: str | None = None
            try:
                result = await client_pool.get_available_client()
                if result is None:
                    break
                session, acquired_phone = result
                await session.delete_messages(message.channel_id, [message.message_id])
            except Exception:
                logger.warning(
                    "DeleteMessageHandler: failed to delete message %s", message.message_id, exc_info=True
                )
            finally:
                if acquired_phone is not None:
                    await client_pool.release_client(acquired_phone)


class ConditionHandler(BaseNodeHandler):
    """Branch execution based on a condition."""

    async def execute(self, node_config: dict, context: NodeContext, services: dict) -> None:
        field = node_config.get("field", "generated_text")
        operator = node_config.get("operator", "not_empty")
        value = node_config.get("value", "")

        actual = context.get_global(field, "")
        result = False

        if operator == "not_empty":
            result = bool(actual)
        elif operator == "empty":
            result = not bool(actual)
        elif operator == "contains":
            result = str(value).lower() in str(actual).lower()
        elif operator == "eq":
            result = str(actual) == str(value)
        elif operator == "gt":
            try:
                result = float(actual) > float(value)
            except (TypeError, ValueError):
                result = False

        context.set_global("condition_result", result)


class SearchQueryTriggerHandler(BaseNodeHandler):
    """Trigger based on a search query matching collected messages."""

    async def execute(self, node_config: dict, context: NodeContext, services: dict) -> None:
        search_engine = services.get("search_engine")
        if search_engine is None:
            logger.warning("SearchQueryTriggerHandler: no search_engine, skipping")
            return

        query = node_config.get("query", "")
        limit = int(node_config.get("limit", 10))
        if not query:
            return

        try:
            result = await search_engine.search_local(query, limit=limit)
            if result.messages:
                m = result.messages[0]
                context.set_global("trigger_text", m.text or "")
                context.set_global("trigger_channel_title", m.channel_title or "")
                context.set_global("context_messages", result.messages)
                context.set_global("trigger_matched", True)
            else:
                context.set_global("trigger_matched", False)
        except Exception:
            logger.warning("SearchQueryTriggerHandler: search failed", exc_info=True)
            context.set_global("trigger_matched", False)


class AgentLoopHandler(BaseNodeHandler):
    """Agent loop node: sends messages to an LLM with a system prompt and writes the result to context.

    Config keys:
        system_prompt (str): System prompt for the agent.
        model (str): LLM model override.
        max_tokens (int): Max response tokens.
        temperature (float): Response temperature.
    """

    async def execute(self, node_config: dict, context: NodeContext, services: dict) -> None:
        provider_callable = services.get("provider_callable")
        if provider_callable is None:
            raise RuntimeError("AgentLoopHandler: no provider_callable in services")

        from datetime import datetime

        system_prompt = node_config.get("system_prompt", "Ты полезный ассистент.")
        model = node_config.get("model") or services.get("default_model") or ""
        max_tokens = int(node_config.get("max_tokens", 2000))
        temperature = float(node_config.get("temperature", 0.7))

        # Build source messages string from context
        messages = context.get_global("context_messages", [])
        source_parts = []
        for m in messages:
            text = (m.text or "").strip()
            if not text:
                continue
            header = m.channel_title or m.channel_username or ""
            when = m.date.isoformat() if isinstance(m.date, datetime) else str(m.date)
            source_parts.append(f"[{header}] {text} (id:{m.message_id} date:{when})")
        source_messages = "\n\n".join(source_parts)

        full_prompt = f"{system_prompt}\n\n---\nСообщения для анализа:\n\n{source_messages}"

        logger.info("AgentLoop: calling provider with %d source messages", len(source_parts))
        result = await provider_callable(
            prompt=full_prompt,
            model=model,
            max_tokens=max_tokens,
            temperature=temperature,
        )

        context.set_global("generated_text", result)
        logger.info("AgentLoop: completed, %d chars generated", len(result or ""))
