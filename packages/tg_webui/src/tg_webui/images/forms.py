"""Request/form parsing for the image-generation web domain."""

from __future__ import annotations

from dataclasses import dataclass

from fastapi import Request


@dataclass(frozen=True)
class GenerateImageForm:
    prompt: str
    model: str


async def parse_generate_form(request: Request) -> GenerateImageForm:
    form = await request.form()
    return GenerateImageForm(
        prompt=str(form.get("prompt", "")).strip(),
        model=str(form.get("model", "")).strip(),
    )


@dataclass(frozen=True)
class ModelsSearchQuery:
    provider: str
    query: str
    refresh: bool


def parse_models_search(request: Request) -> ModelsSearchQuery:
    return ModelsSearchQuery(
        provider=request.query_params.get("provider", "").strip(),
        query=request.query_params.get("q", "").strip(),
        refresh=request.query_params.get("refresh", "").strip() in ("1", "true", "yes"),
    )
