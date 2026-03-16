Provider adapters and environment variables

This project includes lightweight HTTP adapters for common LLM providers and a mechanism to register adapters automatically from environment variables. Use the web UI (Settings → Providers) to manage provider configs for production/long-term use.

Supported env vars (examples)

- OPENAI_API_KEY — OpenAI REST API key (OpenAI-style adapters)
- COHERE_API_KEY — Cohere API key (cohere.generate endpoint)
- OLLAMA_BASE or OLLAMA_URL — Ollama server base URL (e.g., http://localhost:11434)
- HUGGINGFACE_API_KEY or HUGGINGFACE_TOKEN — Hugging Face Inference API token
- FIREWORKS_BASE, FIREWORKS_API_KEY — Fireworks inference endpoint
- DEEPSEEK_BASE, DEEPSEEK_API_KEY — DeepSeek endpoint
- TOGETHER_BASE, TOGETHER_API_KEY — Together API base

Registration behavior

- When the application starts, src/services/provider_service.py attempts to auto-register lightweight adapters for providers whose env vars are present.
- For operator-managed provider configs (multiple providers, per-provider models, encrypted secrets), use the web Settings UI which stores configs via src/services/agent_provider_service.py. That service requires SESSION_ENCRYPTION_KEY to encrypt provider secrets.

Fallbacks

- If langchain is enabled (USE_LANGCHAIN=1) and LangChain + provider client packages are installed, LangChain adapters are preferred for providers supported by LangChain.
- If LangChain is not enabled or not available, the lightweight HTTP adapters are used.

Testing

- Tests for adapters live in tests/test_provider_adapters.py and mock aiohttp sessions.
- For integration tests, mock HTTP responses (aioresponses or monkeypatching aiohttp.ClientSession) so tests do not require real service network calls.
