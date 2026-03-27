"""ReAct-style agent for Ollama models that lack native function calling.

Instead of tool binding (which some models ignore), this agent:
1. Embeds tool descriptions in the system prompt
2. Instructs the model to emit JSON tool call blocks
3. Parses and executes the calls, feeding results back
4. Loops until the model produces a final answer or max_steps is reached
"""
from __future__ import annotations

import inspect
import json
import logging
import re
import urllib.error
import urllib.request
from collections.abc import Callable
from typing import Any

logger = logging.getLogger(__name__)

# Pattern: ```json\n{...}\n``` or <tool_call>{...}</tool_call>
_TOOL_CALL_RE = re.compile(
    r"```json\s*(\{[^`]+\})\s*```|<tool_call>\s*(\{.*?\})\s*</tool_call>",
    re.DOTALL,
)

_REACT_SUFFIX = """

Если тебе нужно вызвать инструмент (tool), используй следующий формат:
```json
{"tool": "<название_инструмента>", "args": {<аргументы>}}
```
Система выполнит инструмент и вернёт результат. Ты можешь вызывать инструменты несколько раз.
Когда ты получил все необходимые данные, дай финальный ответ без JSON-блока."""


def _describe_tools(tools: list[Callable]) -> str:
    parts: list[str] = []
    for fn in tools:
        doc = inspect.getdoc(fn) or ""
        sig = inspect.signature(fn)
        params = [
            f"{p}" for p in sig.parameters if p not in ("self",)
        ]
        parts.append(f"- {fn.__name__}({', '.join(params)}): {doc}")
    return "\n".join(parts)


def _try_call_tool(name: str, args: dict, tools: list[Callable]) -> str:
    for fn in tools:
        if fn.__name__ == name:
            try:
                result = fn(**args)
                return str(result)
            except Exception as exc:
                return f"[Tool error: {exc}]"
    return f"[Unknown tool: {name}]"


def _chat_sync(base_url: str, model: str, messages: list[dict], api_key: str = "") -> str:
    """Synchronous HTTP call to ollama /api/chat."""
    url = base_url.rstrip("/") + "/api/chat"
    payload = json.dumps({"model": model, "messages": messages, "stream": False}).encode()
    headers: dict[str, str] = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    req = urllib.request.Request(url, data=payload, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            data = json.loads(resp.read())
            return data.get("message", {}).get("content", "") or str(data)
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"Ollama HTTP {exc.code}: {exc.read().decode()}") from exc


class OllamaReActAgent:
    """Synchronous ReAct agent — compatible with the invoke({"messages": [...]}) interface."""

    def __init__(
        self,
        base_url: str,
        model: str,
        tools: list[Callable],
        system_prompt: str,
        *,
        api_key: str = "",
        max_steps: int = 8,
    ) -> None:
        self._base_url = base_url
        self._model = model
        self._tools = tools
        self._api_key = api_key
        self._max_steps = max_steps

        tool_block = _describe_tools(tools)
        self._system = (
            system_prompt
            + (f"\n\nДоступные инструменты:\n{tool_block}" if tool_block else "")
            + _REACT_SUFFIX
        )

    def invoke(self, input_dict: dict[str, Any]) -> dict[str, Any]:
        messages: list[dict] = [{"role": "system", "content": self._system}]
        for msg in input_dict.get("messages", []):
            messages.append({"role": msg["role"], "content": msg["content"]})

        for step in range(self._max_steps):
            response = _chat_sync(self._base_url, self._model, messages, self._api_key)
            logger.debug("ReAct step %d response: %r", step, response[:200])

            match = _TOOL_CALL_RE.search(response)
            if not match:
                return {"messages": [_MockMessage(response)]}

            json_str = match.group(1) or match.group(2)
            try:
                call = json.loads(json_str)
                tool_name = call.get("tool", "")
                tool_args = call.get("args") or {}
            except (json.JSONDecodeError, AttributeError):
                return {"messages": [_MockMessage(response)]}

            tool_result = _try_call_tool(tool_name, tool_args, self._tools)
            logger.debug("ReAct tool %r → %r", tool_name, tool_result[:100])

            messages.append({"role": "assistant", "content": response})
            messages.append({
                "role": "user",
                "content": f"Результат инструмента `{tool_name}`:\n{tool_result}",
            })

        last_resp = _chat_sync(self._base_url, self._model, messages, self._api_key)
        return {"messages": [_MockMessage(last_resp)]}


class _MockMessage:
    """Minimal message-like object for _extract_result_text compatibility."""
    def __init__(self, content: str) -> None:
        self.content = content
