LangChain integration (v0.1.16)

Overview

This project supports an optional LangChain-backed provider mode to simplify LLM provider integration. LangChain is optional and gated by configuration to avoid adding heavy runtime dependencies by default.

Quick start

1. Install LangChain and the provider client(s) you intend to use (example for OpenAI):

   python -m pip install langchain openai

2. Enable LangChain usage via environment variable (or operator UI):

   export USE_LANGCHAIN=1

3. Configure provider credentials either via the web UI (Settings → Providers) or via environment variables (e.g., OPENAI_API_KEY).

Notes

- The system prefers LangChain adapters when USE_LANGCHAIN=1 and the langchain package plus the provider client packages are installed.
- If LangChain isn't available, the system falls back to lightweight HTTP adapters implemented in src/services/provider_adapters.py.
- Storing provider secrets in the web UI requires SESSION_ENCRYPTION_KEY to be configured so secrets can be encrypted at rest.

Developer/testing

- Unit tests for LangChain adapters mock langchain imports (see tests/test_langchain_adapters.py).
- CI runs adapter tests without requiring langchain; enable LangChain-specific tests separately when appropriate.

Security

- Do NOT commit API keys or session encryption keys into source control.
- For production, prefer operator-managed secrets vaults or environment variables and enable SESSION_ENCRYPTION_KEY to store secrets safely in the DB.
