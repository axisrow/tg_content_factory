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
- ZAI_API_KEY — Z.AI API key (OpenAI-compatible chat completions)

Env-based registration

- On startup, src/services/provider_service.py auto-registers lightweight adapters for providers with appropriate env vars set. This is convenient for single-provider deployments or CI testing.

Settings UI

- Use the Settings → Developer → Providers form to add, edit or remove provider configurations. Secret fields are encrypted before storage using SESSION_ENCRYPTION_KEY; when this key is not set the UI is read-only for provider secrets.
- The provider registry uses canonical provider names (e.g. "openai", "cohere", "ollama", "huggingface", "context7").
- For local Ollama testing, set OLLAMA_BASE to your server URL and optionally OLLAMA_DEFAULT_MODEL.

Z.AI endpoints

- The `zai` provider uses the OpenAI-compatible Z.AI API. Empty Base URL defaults to the general endpoint: `https://api.z.ai/api/paas/v4`.
- GLM Coding Plan users can opt in by setting Base URL to `https://api.z.ai/api/coding/paas/v4`; model refresh and chat runtime both use the configured endpoint.
- Do not use `https://api.z.ai/api/anthropic` or `/anthropic/v1` with the `zai` provider. That URL is the Anthropic-compatible proxy for tools such as Claude Code; configure the `anthropic` provider for that endpoint instead.
- References: https://docs.z.ai/api-reference/introduction and https://docs.z.ai/devpack/tool/claude

Context7 & operator caution

- A Context7 shim adapter exists (provider name "context7") but operator consent is required before routing production traffic to third-party MSPs. Prefer local or operator-controlled providers.

Image adapters (issue #958)

- The OpenAI, Together and Replicate image adapters use their official SDKs
  (`openai`, `replicate`) rather than raw aiohttp: OpenAI via `AsyncOpenAI.images.generate`,
  Together via the same SDK pointed at Together's OpenAI-compatible `base_url`
  (so no heavy `together` SDK), and Replicate via `replicate` `async_run`
  (which replaces the hand-rolled create-then-poll loop).
- The `openai` SDK was already in the tree transitively via `langchain-openai`;
  it is now a direct dependency. `replicate` is lightweight (httpx + pydantic).
- HuggingFace deliberately stays on raw aiohttp: its SDK's `text_to_image`
  returns a `PIL.Image` (a Pillow dependency) whereas the raw path saves the
  returned `image/*` bytes directly.

Testing

- Image-adapter unit tests mock the SDK client (`openai.AsyncOpenAI`,
  `replicate.client.Client`); the HuggingFace adapter test still mocks
  `aiohttp.ClientSession`. See tests/test_image_adapters.py and tests/test_provider_adapters.py.
- The text/LLM adapters still mock aiohttp.ClientSession.
- For integration tests, mock HTTP responses with aioresponses or monkeypatch aiohttp.ClientSession.

Security

- Do NOT commit API keys or the SESSION_ENCRYPTION_KEY into source control. Prefer operator-managed secret stores and keep the DB encryption key rotated and out of the repo.
