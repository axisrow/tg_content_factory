import asyncio

from src.services.provider_service import AgentProviderService


def test_default_provider_returns_draft():
    svc = AgentProviderService()
    provider = svc.get_provider_callable(None)
    result = asyncio.run(provider(prompt="hello world"))
    assert result.startswith("DRAFT (default provider): hello world")
