# z.ai Provider Support

## Summary
Add Z.AI as a new LLM provider option for the deepagents backend. Z.AI provides an Anthropic-compatible API endpoint that works with langchain-anthropic.

## Changes

### 1. src/agent/provider_registry.py
- Added `ProviderSpec` for z.ai provider
- Static models: `glm-5`, `glm-5-turbo`, `glm-4.7`
- Secret field: `api_key` (required)
- Plain field: `base_url` (placeholder: `https://api.z.ai/api/anthropic`)

### 2. src/services/agent_provider_service.py
- Added `zai` to `_OPENAI_STYLE_DEFAULT_BASE_URLS` dict
- Added `_fetch_zai_models` method for dynamic model fetching
- Added z.ai case in `_fetch_live_models` method

### 3. src/agent/manager.py
- Added import of `_OPENAI_STYLE_DEFAULT_BASE_URLS` from agent_provider_service
- Added z.ai case in `_build_agent` to initialize ChatAnthropic with custom `anthropic_api_url`

## Usage

1. Add Z.AI provider in Settings UI or via CLI
2. Configure API key (and optionally base URL, defaults to https://api.z.ai/api/anthropic)
3. Select model (glm-5, glm-5-turbo, or glm-4.7)
4. Start chat with the agent

## Related Issue
- Closes #339
