from __future__ import annotations

import os
from typing import Any, Awaitable, Callable, Dict, Optional

import aiohttp


async def _parse_json_for_text(data: Any) -> str:
    # Try common response shapes
    if data is None:
        return ""
    if isinstance(data, str):
        return data
    if isinstance(data, dict):
        # OpenAI style
        if "choices" in data:
            try:
                c = data["choices"][0]
                if isinstance(c, dict):
                    if "message" in c:
                        return c["message"].get("content", "")
                    return c.get("text", "")
            except Exception:
                pass
        # Cohere style
        if "generations" in data:
            try:
                return data["generations"][0].get("text", "")
            except Exception:
                pass
        # HuggingFace / other style
        if "generated_text" in data:
            return data.get("generated_text", "")
        if "outputs" in data:
            try:
                out = data["outputs"][0]
                if isinstance(out, dict):
                    return out.get("content", out.get("text", ""))
                return str(out)
            except Exception:
                pass
        if "result" in data:
            r = data["result"]
            if isinstance(r, str):
                return r
            if isinstance(r, dict):
                for k in ("text", "content", "generated_text"):
                    if k in r:
                        return r[k]
                return str(r)
        # Ollama / local shapes
        if "results" in data:
            try:
                r0 = data["results"][0]
                if isinstance(r0, dict):
                    # nested content
                    if "content" in r0 and isinstance(r0["content"], dict):
                        return r0["content"].get("text", "")
                    return r0.get("text", "")
            except Exception:
                pass
        # fallback: try first stringy field
        for v in data.values():
            if isinstance(v, str):
                return v
        return str(data)
    if isinstance(data, list):
        # list of items
        try:
            first = data[0]
            return await _parse_json_for_text(first)
        except Exception:
            return str(data)
    return str(data)


def make_cohere_adapter(api_key: str, base_url: Optional[str] = None) -> Callable[..., Awaitable[str]]:
    base = base_url or os.environ.get("COHERE_API_BASE", "https://api.cohere.ai/v1/generate")

    async def provider(
        prompt: str = "",
        model: Optional[str] = None,
        max_tokens: int = 256,
        temperature: float = 0.0,
        stream: bool = False,
        **kwargs: Any,
    ) -> str:
        url = base
        headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
        payload = {"prompt": prompt, "max_tokens": int(max_tokens or 256)}
        if model:
            payload["model"] = model
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload, headers=headers) as resp:
                    text = await resp.text()
                    if resp.status != 200:
                        raise RuntimeError(f"Cohere error {resp.status}: {text}")
                    data = await resp.json()
                    return await _parse_json_for_text(data)
        except Exception as ex:
            raise

    return provider


def make_ollama_adapter(base_url: Optional[str] = None, api_key: Optional[str] = None) -> Callable[..., Awaitable[str]]:
    base = base_url or os.environ.get("OLLAMA_BASE", "http://localhost:11434")
    endpoint = base.rstrip("/") + "/api/generate"

    async def provider(
        prompt: str = "",
        model: Optional[str] = None,
        max_tokens: int = 256,
        temperature: float = 0.0,
        stream: bool = False,
        **kwargs: Any,
    ) -> str:
        payload: Dict[str, Any] = {"model": model or os.environ.get("OLLAMA_DEFAULT_MODEL", "llama3.2"), "prompt": prompt}
        headers = {"Content-Type": "application/json"}
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(endpoint, json=payload, headers=headers) as resp:
                    text = await resp.text()
                    if resp.status != 200:
                        raise RuntimeError(f"Ollama error {resp.status}: {text}")
                    data = await resp.json()
                    return await _parse_json_for_text(data)
        except Exception:
            raise

    return provider


def make_huggingface_adapter(api_token: str, base_url: Optional[str] = None) -> Callable[..., Awaitable[str]]:
    # Use inference endpoint when model is provided in model arg; otherwise use base_url
    base = base_url or os.environ.get("HUGGINGFACE_API_BASE", "https://api-inference.huggingface.co/models")

    async def provider(
        prompt: str = "",
        model: Optional[str] = None,
        max_tokens: int = 256,
        temperature: float = 0.0,
        stream: bool = False,
        **kwargs: Any,
    ) -> str:
        if model:
            url = f"{base.rstrip('/')}/{model}"
        else:
            # fallback generic route
            url = base
        headers = {"Authorization": f"Bearer {api_token}"}
        payload = {"inputs": prompt, "parameters": {"max_new_tokens": int(max_tokens or 256)}}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload, headers=headers) as resp:
                    text = await resp.text()
                    if resp.status != 200:
                        raise RuntimeError(f"HuggingFace error {resp.status}: {text}")
                    data = await resp.json()
                    return await _parse_json_for_text(data)
        except Exception:
            raise

    return provider


def make_generic_http_adapter(base_url: str, api_key: Optional[str] = None, api_key_header: str = "Authorization") -> Callable[..., Awaitable[str]]:
    endpoint = base_url

    async def provider(
        prompt: str = "",
        model: Optional[str] = None,
        max_tokens: int = 256,
        temperature: float = 0.0,
        stream: bool = False,
        **kwargs: Any,
    ) -> str:
        payload: Dict[str, Any] = {"prompt": prompt}
        if model:
            payload["model"] = model
        headers = {"Content-Type": "application/json"}
        if api_key:
            headers[api_key_header] = f"Bearer {api_key}"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(endpoint, json=payload, headers=headers) as resp:
                    text = await resp.text()
                    if resp.status != 200:
                        raise RuntimeError(f"Provider error {resp.status}: {text}")
                    data = await resp.json()
                    return await _parse_json_for_text(data)
        except Exception:
            raise

    return provider


def make_context7_adapter(api_key: str, base_url: Optional[str] = None) -> Callable[..., Awaitable[str]]:
    base = base_url or os.environ.get("CONTEXT7_API_BASE", "https://api.context7.com/v1/generate")
    return make_generic_http_adapter(base, api_key)


# Convenience shims
def make_cohere(api_key: str) -> Callable[..., Awaitable[str]]:
    return make_cohere_adapter(api_key)


def make_ollama(base_url: Optional[str] = None, api_key: Optional[str] = None) -> Callable[..., Awaitable[str]]:
    return make_ollama_adapter(base_url, api_key)


def make_huggingface(api_token: str) -> Callable[..., Awaitable[str]]:
    return make_huggingface_adapter(api_token)
