"""Concrete handlers for all pipeline node types."""
from __future__ import annotations

import asyncio
import logging
import random
import re

from src.services.pipeline_filters import filter_messages
from src.services.pipeline_nodes.base import BaseNodeHandler, NodeContext
from src.services.pipeline_result import increment_action_count

try:  # telethon is an optional dependency at test-time
    from telethon.errors import (
        ChatWriteForbiddenError,
        FloodWaitError,
        ReactionInvalidError,
    )
except ImportError:  # pragma: no cover — handlers import path used without telethon
    class FloodWaitError(Exception):  # type: ignore[no-redef]
        seconds = 0

    class ChatWriteForbiddenError(Exception):  # type: ignore[no-redef]
        pass

    class ReactionInvalidError(Exception):  # type: ignore[no-redef]
        pass


logger = logging.getLogger(__name__)


def _current_node_id(services: dict, default: str = "?") -> str:
    """Return the node id for the currently-executing handler.

    PipelineExecutor.execute injects ``_current_node_id`` into services right
    before dispatching each handler so that node-level error records carry the
    source-of-truth id (issue #463).
    """
    value = services.get("_current_node_id")
    return str(value) if value else default


def _resolve_account_phone(account_phone: str | None, services: dict, context: NodeContext) -> str | None:
    """Return explicit account_phone, or discover one from source channel access map."""
    if account_phone:
        return account_phone
    client_pool = services.get("client_pool")
    if client_pool is None:
        return None
    source_ids = context.get_global("source_channel_ids") or []
    for cid in source_ids:
        phone = client_pool.get_phone_for_channel(cid)
        if phone:
            return phone
    return None


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
        limit = int(node_config["limit"]) if "limit" in node_config else None
        if limit is not None and len(messages) > limit:
            messages = messages[:limit]
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
        source_channel_ids = context.get_global("source_channel_ids", [])
        channel_id = source_channel_ids[0] if len(source_channel_ids) == 1 else context.get_global("channel_id")

        try:
            if method == "hybrid" and search_engine.semantic_available:
                result = await search_engine.search_hybrid(query, channel_id=channel_id, limit=limit)
            elif method == "semantic" and search_engine.semantic_available:
                result = await search_engine.search_semantic(query, channel_id=channel_id, limit=limit)
            else:
                result = await search_engine.search_local(query, channel_id=channel_id, limit=limit)
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
        messages = context.get_global("context_messages", [])
        filtered = filter_messages(messages, node_config)
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
        node_id = _current_node_id(services, default="react")
        client_pool = services.get("client_pool")
        if client_pool is None:
            context.record_error(
                node_id=node_id,
                code="no_client_pool",
                detail="client_pool not available in services",
            )
            logger.warning("ReactHandler[%s]: no client_pool, skipping", node_id)
            return

        messages = context.get_global("context_messages", [])
        emoji = node_config.get("emoji") or "👍"
        random_emoji_list = node_config.get("random_emojis", [])
        resolved_phone = _resolve_account_phone(services.get("account_phone"), services, context)

        for message in messages:
            acquired_phone: str | None = None
            try:
                if resolved_phone:
                    result = await client_pool.get_client_by_phone(resolved_phone)
                else:
                    result = await client_pool.get_available_client()
                if result is None:
                    context.record_error(
                        node_id=node_id,
                        code="no_available_client",
                        detail=(
                            f"no client_pool slot available for phone={resolved_phone}"
                            if resolved_phone
                            else "all accounts are flood-waited or disconnected"
                        ),
                    )
                    break
                session, acquired_phone = result
                chosen_emoji = random.choice(random_emoji_list) if random_emoji_list else emoji
                await session.send_reaction(message.channel_id, message.message_id, chosen_emoji)
                increment_action_count(context, "react")
            except FloodWaitError as exc:
                context.record_error(
                    node_id=node_id,
                    code="flood_wait",
                    detail=f"FloodWaitError on message {message.message_id}: {exc}",
                    retry_after=int(getattr(exc, "seconds", 0) or 0),
                )
            except ChatWriteForbiddenError as exc:
                context.record_error(
                    node_id=node_id,
                    code="chat_write_forbidden",
                    detail=(
                        f"ChatWriteForbiddenError on message {message.message_id}: {exc}"
                    ),
                )
            except ReactionInvalidError as exc:
                context.record_error(
                    node_id=node_id,
                    code="reaction_invalid",
                    detail=f"ReactionInvalidError on message {message.message_id}: {exc}",
                )
            except Exception as exc:
                context.record_error(
                    node_id=node_id,
                    code="unexpected_error",
                    detail=f"{type(exc).__name__}: {exc}",
                )
                logger.warning(
                    "ReactHandler[%s]: failed to react to message %s",
                    node_id,
                    message.message_id,
                    exc_info=True,
                )
            finally:
                if acquired_phone is not None:
                    await client_pool.release_client(acquired_phone)


class ForwardHandler(BaseNodeHandler):
    """Forward messages to target dialogs (requires Telegram client)."""

    async def execute(self, node_config: dict, context: NodeContext, services: dict) -> None:
        node_id = _current_node_id(services, default="forward")
        client_pool = services.get("client_pool")
        if client_pool is None:
            context.record_error(
                node_id=node_id,
                code="no_client_pool",
                detail="client_pool not available in services",
            )
            logger.warning("ForwardHandler[%s]: no client_pool, skipping", node_id)
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
                    context.record_error(
                        node_id=node_id,
                        code="no_available_client",
                        detail=(
                            f"no client_pool slot available for phone={phone}"
                        ),
                    )
                    continue
                session, acquired_phone = result
                for message in messages:
                    await session.forward_messages(dialog_id, message.message_id, message.channel_id)
                    increment_action_count(context, "forward")
            except FloodWaitError as exc:
                context.record_error(
                    node_id=node_id,
                    code="flood_wait",
                    detail=f"FloodWaitError forwarding to {dialog_id}: {exc}",
                    retry_after=int(getattr(exc, "seconds", 0) or 0),
                )
            except ChatWriteForbiddenError as exc:
                context.record_error(
                    node_id=node_id,
                    code="chat_write_forbidden",
                    detail=f"ChatWriteForbiddenError forwarding to {dialog_id}: {exc}",
                )
            except Exception as exc:
                context.record_error(
                    node_id=node_id,
                    code="unexpected_error",
                    detail=f"{type(exc).__name__}: {exc}",
                )
                logger.warning(
                    "ForwardHandler[%s]: failed to forward to %s",
                    node_id,
                    dialog_id,
                    exc_info=True,
                )
            finally:
                if acquired_phone is not None:
                    await client_pool.release_client(acquired_phone)


class DeleteMessageHandler(BaseNodeHandler):
    """Delete filtered messages (requires Telegram client)."""

    async def execute(self, node_config: dict, context: NodeContext, services: dict) -> None:
        node_id = _current_node_id(services, default="delete_message")
        client_pool = services.get("client_pool")
        if client_pool is None:
            context.record_error(
                node_id=node_id,
                code="no_client_pool",
                detail="client_pool not available in services",
            )
            logger.warning("DeleteMessageHandler[%s]: no client_pool, skipping", node_id)
            return

        messages = context.get_global("context_messages", [])
        resolved_phone = _resolve_account_phone(services.get("account_phone"), services, context)

        for message in messages:
            acquired_phone: str | None = None
            try:
                if resolved_phone:
                    result = await client_pool.get_client_by_phone(resolved_phone)
                else:
                    result = await client_pool.get_available_client()
                if result is None:
                    context.record_error(
                        node_id=node_id,
                        code="no_available_client",
                        detail=(
                            f"no client_pool slot available for phone={resolved_phone}"
                            if resolved_phone
                            else "all accounts are flood-waited or disconnected"
                        ),
                    )
                    break
                session, acquired_phone = result
                await session.delete_messages(message.channel_id, [message.message_id])
                increment_action_count(context, "delete_message")
            except FloodWaitError as exc:
                context.record_error(
                    node_id=node_id,
                    code="flood_wait",
                    detail=f"FloodWaitError deleting message {message.message_id}: {exc}",
                    retry_after=int(getattr(exc, "seconds", 0) or 0),
                )
            except ChatWriteForbiddenError as exc:
                context.record_error(
                    node_id=node_id,
                    code="chat_write_forbidden",
                    detail=(
                        f"ChatWriteForbiddenError deleting message {message.message_id}: {exc}"
                    ),
                )
            except Exception as exc:
                context.record_error(
                    node_id=node_id,
                    code="unexpected_error",
                    detail=f"{type(exc).__name__}: {exc}",
                )
                logger.warning(
                    "DeleteMessageHandler[%s]: failed to delete message %s",
                    node_id,
                    message.message_id,
                    exc_info=True,
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
    """Agent loop node: multi-step agent with tool access (ReAct pattern).

    Unlike a single-shot LLM call, this handler:
    1. Sends system prompt + messages to LLM
    2. Parses tool call requests from the response
    3. Executes approved tools from services["agent_tools"]
    4. Feeds results back to LLM
    5. Repeats until final answer or max_steps reached

    Config keys:
        system_prompt (str): System prompt for the agent.
        model (str): LLM model override.
        max_tokens (int): Max response tokens per step.
        temperature (float): Response temperature.
        max_steps (int): Maximum agent loop iterations (default 5).
    """

    _TOOL_CALL_RE = re.compile(
        r"```json\s*(\{[^`]+\})\s*```",
        re.DOTALL,
    )

    _REACT_SUFFIX = """

Если тебе нужно вызвать инструмент (tool), используй следующий формат:
```json
{"tool": "<tool_name>", "args": {<arguments>}}
```
Система выполнит инструмент и вернёт результат. Ты можешь вызывать инструменты несколько раз.
Когда ты получил все необходимые данные, дай финальный ответ без JSON-блока."""

    async def execute(self, node_config: dict, context: NodeContext, services: dict) -> None:
        provider_callable = services.get("provider_callable")
        if provider_callable is None:
            raise RuntimeError("AgentLoopHandler: no provider_callable in services")

        from datetime import datetime

        system_prompt = node_config.get("system_prompt", "Ты полезный ассистент.")
        model = node_config.get("model") or services.get("default_model") or ""
        max_tokens = int(node_config.get("max_tokens", 2000))
        temperature = float(node_config.get("temperature", 0.7))
        max_steps = max(1, int(node_config.get("max_steps", 5)))
        agent_tools = services.get("agent_tools", {})

        # Build tool descriptions for system prompt
        tool_desc = ""
        if agent_tools:
            import inspect
            parts = []
            for name, fn in agent_tools.items():
                doc = inspect.getdoc(fn) or ""
                parts.append(f"- {name}: {doc}")
            tool_desc = "\n\nДоступные инструменты:\n" + "\n".join(parts)

        full_system = system_prompt + tool_desc + self._REACT_SUFFIX

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

        user_message = f"Сообщения для анализа:\n\n{source_messages}" if source_parts else "Нет сообщений для анализа."

        conversation = [
            {"role": "system", "content": full_system},
            {"role": "user", "content": user_message},
        ]

        import json as _json

        def _serialize_conversation(conv: list[dict]) -> str:
            """Flatten multi-turn conversation into a single prompt string."""
            role_labels = {"system": "SYSTEM", "user": "USER", "assistant": "ASSISTANT"}
            parts = []
            for msg in conv:
                role = msg.get("role", "")
                content = msg.get("content", "")
                label = role_labels.get(role, role.upper())
                parts.append(f"{label}:\n{content}")
            return "\n\n".join(parts)

        response_text = ""
        for step in range(max_steps):
            logger.info("AgentLoop step %d/%d", step + 1, max_steps)
            prompt_text = _serialize_conversation(conversation)
            result = await provider_callable(
                prompt=prompt_text,
                model=model,
                max_tokens=max_tokens,
                temperature=temperature,
            )
            if isinstance(result, str):
                response_text = result
            else:
                response_text = result.get("text") or result.get("generated_text") or str(result)

            # Check for tool calls
            match = self._TOOL_CALL_RE.search(response_text)
            if not match:
                # Final answer — no tool call pattern found
                break

            json_str = match.group(1)
            try:
                call = _json.loads(json_str)
                tool_name = call.get("tool", "")
                tool_args = call.get("args") or {}
            except (_json.JSONDecodeError, AttributeError):
                break

            # Execute tool
            fn = agent_tools.get(tool_name)
            if fn is None:
                tool_result = f"[Unknown tool: {tool_name}]"
            else:
                try:
                    import inspect as _inspect
                    retval = fn(**tool_args)
                    if _inspect.isawaitable(retval):
                        retval = await retval
                    tool_result = str(retval)
                except Exception as exc:
                    tool_result = f"[Tool error: {exc}]"

            logger.info("AgentLoop tool %r → %s", tool_name, tool_result[:100])

            conversation.append({"role": "assistant", "content": response_text})
            conversation.append({
                "role": "user",
                "content": f"Результат инструмента `{tool_name}`:\n{tool_result}",
            })

        # If max_steps exhausted and last response is still a tool call, discard it
        if self._TOOL_CALL_RE.search(response_text):
            logger.warning(
                "AgentLoop: max_steps exhausted without final answer, discarding tool-call output"
            )
            response_text = ""

        context.set_global("generated_text", response_text)
        logger.info(
            "AgentLoop: completed in %d steps, %d chars generated",
            min(step + 1, max_steps),
            len(response_text or ""),
        )
