Provider adapters and environment variables

This project includes lightweight HTTP adapters for common LLM providers and a mechanism to register adapters automatically from environment variables. For operator‑managed provider configurations (per-provider models, encrypted secrets), use the web UI: Settings → Developer → Providers.

Supported env vars (examples)

- OPENAI_API_KEY — OpenAI REST API key
- COHERE_API_KEY — Cohere API key (Cohere generate endpoint)
- OLLAMA_BASE or OLLAMA_URL — Ollama server base URL (e.g., http://localhost:11434)
- HUGGINGFACE_API_KEY or HUGGINGFACE_TOKEN — Hugging Face Inference API token
- CONTEXT7_API_KEY or CTX7_API_KEY — Context7 API key (uses the generic shim)
- FIREWORKS_BASE, FIREWORKS_API_KEY — Fireworks inference endpoint
- DEEPSEEK_BASE, DEEPSEEK_API_KEY — DeepSeek endpoint
- TOGETHER_BASE, TOGETHER_API_KEY — Together API base

Env-based registration

- On startup, src/services/provider_service.py auto-registers lightweight adapters for providers with appropriate env vars set. This is convenient for single-provider deployments or CI testing.

Settings UI

- Use the Settings → Developer → Providers form to add, edit or remove provider configurations. Secret fields are encrypted before storage using SESSION_ENCRYPTION_KEY; when this key is not set the UI is read-only for provider secrets.
- The provider registry uses canonical provider names (e.g. "openai", "cohere", "ollama", "huggingface", "context7").
- For local Ollama testing, set OLLAMA_BASE to your server URL and optionally OLLAMA_DEFAULT_MODEL.

Context7 & operator caution

- A Context7 shim adapter exists (provider name "context7") but operator consent is required before routing production traffic to third-party MSPs. Prefer local or operator-controlled providers.

Testing

- Adapter unit tests mock aiohttp.ClientSession (see tests/test_provider_adapters.py).
- For integration tests, mock HTTP responses with aioresponses or monkeypatch aiohttp.ClientSession; LangChain-specific tests should be gated behind USE_LANGCHAIN or a dedicated CI job.

Security

- Do NOT commit API keys or the SESSION_ENCRYPTION_KEY into source control. Prefer operator-managed secret stores and keep the DB encryption key rotated and out of the repo.
