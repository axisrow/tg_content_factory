LangChain integration (v0.1.16)

Overview

This project supports an optional LangChain-backed provider mode to simplify LLM provider integration. LangChain is strictly opt-in (to avoid adding heavy runtime dependencies) and is enabled via configuration.

Quick start

1. Install LangChain and the provider client(s) you intend to use (example for OpenAI):

   python -m pip install "langchain" openai

2. Enable LangChain usage (environment or operator):

   export USE_LANGCHAIN=1

3. Provide provider credentials either via environment variables (e.g. OPENAI_API_KEY) or via the web UI (Settings → Developer → Providers). To persist secrets in the DB you must set SESSION_ENCRYPTION_KEY (do NOT commit this key).

Example (env-based OpenAI):

    export OPENAI_API_KEY="your-openai-key"
    export USE_LANGCHAIN=1

Notes

- When USE_LANGCHAIN=1 and the required provider client packages are installed, the system prefers LangChain adapters for supported providers.
- If LangChain is not available or disabled, lightweight HTTP adapters in src/services/provider_adapters.py are used as fallbacks.
- LangChain adapters are imported lazily at runtime; missing packages produce runtime errors only when a LangChain adapter is invoked.

Developer / testing

- LangChain-related unit tests mock langchain imports (see tests/test_langchain_adapters.py).
- CI should gate LangChain-specific tests behind an explicit job or USE_LANGCHAIN flag to avoid requiring heavy deps in default runs.

Security

- Never commit API keys or SESSION_ENCRYPTION_KEY to source control. Use an operator secrets vault when possible.
- To store secrets in the UI, set SESSION_ENCRYPTION_KEY (see src/config.py and startup docs). If the key is changed, previously encrypted secrets become unreadable and must be re-entered.
