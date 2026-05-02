from __future__ import annotations

from dataclasses import dataclass, field
from urllib.parse import urlsplit, urlunsplit

from src.agent.models import CLAUDE_MODELS

ZAI_GENERAL_BASE_URL = "https://api.z.ai/api/paas/v4"
ZAI_CODING_BASE_URL = "https://api.z.ai/api/coding/paas/v4"
# Default to the subscription/Coding Plan endpoint when a user only provides
# ZAI_API_KEY. The pay-per-token endpoint can still be selected explicitly.
ZAI_DEFAULT_BASE_URL = ZAI_CODING_BASE_URL
ZAI_LEGACY_ANTHROPIC_BASE_URLS = {
    "https://api.z.ai/api/anthropic",
    "https://api.z.ai/api/anthropic/v1",
}
ZAI_BASE_URL_REQUIRED_HINT = (
    "Z.AI Base URL is optional. Empty value defaults to "
    "https://api.z.ai/api/coding/paas/v4 for the GLM Coding Plan; use "
    "https://api.z.ai/api/paas/v4 only for pay-per-token PaaS access."
)


def is_zai_legacy_anthropic_base_url(base_url: str = "") -> bool:
    normalized = (base_url or "").strip().rstrip("/")
    return normalized in ZAI_LEGACY_ANTHROPIC_BASE_URLS


def normalize_zai_base_url(base_url: str = "") -> str:
    """Strip whitespace and trailing slash from a Z.AI base URL.

    Empty value means the subscription/Coding Plan endpoint.
    """
    return (base_url or "").strip().rstrip("/") or ZAI_DEFAULT_BASE_URL


@dataclass(frozen=True, slots=True)
class ProviderFieldSpec:
    name: str
    label: str
    required: bool = False
    secret: bool = False
    placeholder: str = ""
    help_text: str = ""


@dataclass(frozen=True, slots=True)
class ProviderSpec:
    name: str
    display_name: str
    package_name: str
    static_models: tuple[str, ...]
    plain_fields: tuple[ProviderFieldSpec, ...] = ()
    secret_fields: tuple[ProviderFieldSpec, ...] = ()
    default_base_url: str = ""
    runtime_provider: str = ""
    openai_compatible: bool = False
    supports_lightweight_adapter: bool = False
    canonical_export_mode: str = "provider_default"

    @property
    def all_fields(self) -> tuple[ProviderFieldSpec, ...]:
        return self.plain_fields + self.secret_fields

    @property
    def resolved_runtime_provider(self) -> str:
        return self.runtime_provider or self.name


def _field(
    name: str,
    label: str,
    *,
    required: bool = False,
    secret: bool = False,
    placeholder: str = "",
    help_text: str = "",
) -> ProviderFieldSpec:
    return ProviderFieldSpec(
        name=name,
        label=label,
        required=required,
        secret=secret,
        placeholder=placeholder,
        help_text=help_text,
    )


PROVIDER_SPECS: dict[str, ProviderSpec] = {
    "openai": ProviderSpec(
        name="openai",
        display_name="OpenAI",
        package_name="langchain-openai",
        static_models=("gpt-4.1", "gpt-4.1-mini", "gpt-4o-mini", "gpt-5-nano"),
        secret_fields=(_field("api_key", "API key", required=True, secret=True),),
        plain_fields=(_field("base_url", "Base URL", placeholder="https://api.openai.com/v1"),),
        default_base_url="https://api.openai.com/v1",
        openai_compatible=True,
        supports_lightweight_adapter=True,
        canonical_export_mode="default_base_url",
    ),
    "anthropic": ProviderSpec(
        name="anthropic",
        display_name="Anthropic",
        package_name="langchain-anthropic",
        static_models=tuple(m[0] for m in CLAUDE_MODELS),
        secret_fields=(_field("api_key", "API key", required=True, secret=True),),
        supports_lightweight_adapter=True,
    ),
    "azure_openai": ProviderSpec(
        name="azure_openai",
        display_name="Azure OpenAI",
        package_name="langchain-openai",
        static_models=("gpt-4.1", "gpt-4o", "gpt-4o-mini"),
        secret_fields=(_field("api_key", "API key", required=True, secret=True),),
        plain_fields=(
            _field("azure_endpoint", "Azure endpoint", required=True),
            _field("azure_deployment", "Deployment", required=True),
            _field("api_version", "API version", placeholder="2024-10-21"),
        ),
        canonical_export_mode="none",
    ),
    "azure_ai": ProviderSpec(
        name="azure_ai",
        display_name="Azure AI",
        package_name="langchain-azure-ai",
        static_models=("gpt-4.1", "gpt-4o", "Phi-4-mini-instruct"),
        secret_fields=(_field("api_key", "API key", required=True, secret=True),),
        plain_fields=(
            _field("endpoint", "Endpoint", required=True),
            _field("project_name", "Project name"),
        ),
        canonical_export_mode="none",
    ),
    "google_vertexai": ProviderSpec(
        name="google_vertexai",
        display_name="Google Vertex AI",
        package_name="langchain-google-vertexai",
        static_models=("gemini-2.5-pro", "gemini-2.5-flash", "gemini-2.0-flash"),
        plain_fields=(
            _field("project", "Project", required=True),
            _field("location", "Location", required=True, placeholder="us-central1"),
        ),
        canonical_export_mode="none",
    ),
    "google_genai": ProviderSpec(
        name="google_genai",
        display_name="Google GenAI",
        package_name="langchain-google-genai",
        static_models=("gemini-2.5-pro", "gemini-2.5-flash", "gemini-2.0-flash"),
        secret_fields=(_field("api_key", "API key", required=True, secret=True),),
    ),
    "bedrock": ProviderSpec(
        name="bedrock",
        display_name="AWS Bedrock",
        package_name="langchain-aws",
        static_models=("anthropic.claude-3-5-sonnet-20241022-v2:0", "amazon.nova-pro-v1:0"),
        secret_fields=(
            _field("aws_access_key_id", "AWS access key ID", required=True, secret=True),
            _field("aws_secret_access_key", "AWS secret access key", required=True, secret=True),
            _field("aws_session_token", "AWS session token", secret=True),
        ),
        plain_fields=(_field("region_name", "Region", required=True, placeholder="us-east-1"),),
        canonical_export_mode="none",
    ),
    "bedrock_converse": ProviderSpec(
        name="bedrock_converse",
        display_name="AWS Bedrock Converse",
        package_name="langchain-aws",
        static_models=("anthropic.claude-3-7-sonnet-20250219-v1:0", "amazon.nova-lite-v1:0"),
        secret_fields=(
            _field("aws_access_key_id", "AWS access key ID", required=True, secret=True),
            _field("aws_secret_access_key", "AWS secret access key", required=True, secret=True),
            _field("aws_session_token", "AWS session token", secret=True),
        ),
        plain_fields=(_field("region_name", "Region", required=True, placeholder="us-east-1"),),
        canonical_export_mode="none",
    ),
    "cohere": ProviderSpec(
        name="cohere",
        display_name="Cohere",
        package_name="langchain-cohere",
        static_models=("command-r-plus", "command-r", "command-a-03-2025"),
        secret_fields=(_field("api_key", "API key", required=True, secret=True),),
        supports_lightweight_adapter=True,
    ),
    "fireworks": ProviderSpec(
        name="fireworks",
        display_name="Fireworks",
        package_name="langchain-fireworks",
        static_models=(
            "accounts/fireworks/models/llama-v3p1-8b-instruct",
            "accounts/fireworks/models/qwen3-235b-a22b",
        ),
        secret_fields=(_field("api_key", "API key", required=True, secret=True),),
        plain_fields=(
            _field("base_url", "Base URL", placeholder="https://api.fireworks.ai/inference/v1"),
        ),
        default_base_url="https://api.fireworks.ai/inference/v1",
        openai_compatible=True,
        supports_lightweight_adapter=True,
        canonical_export_mode="default_base_url",
    ),
    "together": ProviderSpec(
        name="together",
        display_name="Together",
        package_name="langchain-together",
        static_models=(
            "meta-llama/Llama-3.3-70B-Instruct-Turbo",
            "Qwen/Qwen2.5-72B-Instruct-Turbo",
        ),
        secret_fields=(_field("api_key", "API key", required=True, secret=True),),
        default_base_url="https://api.together.xyz/v1",
        openai_compatible=True,
        supports_lightweight_adapter=True,
        canonical_export_mode="default_base_url",
    ),
    "mistralai": ProviderSpec(
        name="mistralai",
        display_name="Mistral",
        package_name="langchain-mistralai",
        static_models=("mistral-large-latest", "mistral-medium-latest", "ministral-8b-latest"),
        secret_fields=(_field("api_key", "API key", required=True, secret=True),),
        default_base_url="https://api.mistral.ai/v1",
        openai_compatible=True,
        supports_lightweight_adapter=True,
        canonical_export_mode="default_base_url",
    ),
    "huggingface": ProviderSpec(
        name="huggingface",
        display_name="HuggingFace",
        package_name="langchain-huggingface",
        static_models=("microsoft/Phi-3-mini-4k-instruct", "meta-llama/Llama-3.1-8B-Instruct"),
        secret_fields=(_field("api_key", "API token", secret=True),),
        supports_lightweight_adapter=True,
    ),
    "groq": ProviderSpec(
        name="groq",
        display_name="Groq",
        package_name="langchain-groq",
        static_models=("llama-3.3-70b-versatile", "llama-3.1-8b-instant", "mixtral-8x7b-32768"),
        secret_fields=(_field("api_key", "API key", required=True, secret=True),),
        default_base_url="https://api.groq.com/openai/v1",
        openai_compatible=True,
        supports_lightweight_adapter=True,
        canonical_export_mode="default_base_url",
    ),
    "ollama": ProviderSpec(
        name="ollama",
        display_name="Ollama",
        package_name="langchain-ollama",
        static_models=("qwen3", "deepseek-v3.2", "gemma3", "mistral-large3", "glm4.7"),
        plain_fields=(
            _field(
                "base_url",
                "Base URL",
                placeholder=(
                    "http://localhost:11434, http://localhost:11434/api, "
                    "https://ollama.com, or https://ollama.com/api"
                ),
                help_text=(
                    "Accepts both host and /api forms. Empty uses local Ollama by "
                    "default, or Ollama Cloud when API key is set."
                ),
            ),
        ),
        secret_fields=(
            _field(
                "api_key",
                "API key",
                secret=True,
                placeholder="ollama_...",
                help_text="Optional for local Ollama. Required for direct Ollama Cloud API access.",
            ),
        ),
        supports_lightweight_adapter=True,
        canonical_export_mode="ollama_cloud",
    ),
    "google_anthropic_vertex": ProviderSpec(
        name="google_anthropic_vertex",
        display_name="Google Vertex Anthropic",
        package_name="langchain-google-vertexai",
        static_models=("claude-3-5-sonnet-v2@20241022", "claude-3-7-sonnet@20250219"),
        plain_fields=(
            _field("project", "Project", required=True),
            _field("location", "Location", required=True, placeholder="us-east5"),
        ),
        canonical_export_mode="none",
    ),
    "deepseek": ProviderSpec(
        name="deepseek",
        display_name="DeepSeek",
        package_name="langchain-deepseek",
        static_models=("deepseek-chat", "deepseek-reasoner"),
        secret_fields=(_field("api_key", "API key", required=True, secret=True),),
        default_base_url="https://api.deepseek.com/v1",
        openai_compatible=True,
        supports_lightweight_adapter=True,
        canonical_export_mode="default_base_url",
    ),
    "ibm": ProviderSpec(
        name="ibm",
        display_name="IBM watsonx",
        package_name="langchain-ibm",
        static_models=("meta-llama/llama-3-3-70b-instruct", "ibm/granite-3-8b-instruct"),
        secret_fields=(_field("api_key", "API key", required=True, secret=True),),
        plain_fields=(
            _field("url", "API URL", required=True),
            _field("project_id", "Project ID"),
        ),
        canonical_export_mode="none",
    ),
    "nvidia": ProviderSpec(
        name="nvidia",
        display_name="NVIDIA",
        package_name="langchain-nvidia-ai-endpoints",
        static_models=("meta/llama-3.1-70b-instruct", "nvidia/llama-3.1-nemotron-70b-instruct"),
        secret_fields=(_field("api_key", "API key", required=True, secret=True),),
    ),
    "xai": ProviderSpec(
        name="xai",
        display_name="xAI",
        package_name="langchain-xai",
        static_models=("grok-2-1212", "grok-3-mini", "grok-3"),
        secret_fields=(_field("api_key", "API key", required=True, secret=True),),
        default_base_url="https://api.x.ai/v1",
        openai_compatible=True,
        supports_lightweight_adapter=True,
        canonical_export_mode="default_base_url",
    ),
    "perplexity": ProviderSpec(
        name="perplexity",
        display_name="Perplexity",
        package_name="langchain-perplexity",
        static_models=("sonar", "sonar-pro", "sonar-reasoning"),
        secret_fields=(_field("api_key", "API key", required=True, secret=True),),
        default_base_url="https://api.perplexity.ai",
        openai_compatible=True,
        supports_lightweight_adapter=True,
        canonical_export_mode="default_base_url",
    ),
    "zai": ProviderSpec(
        name="zai",
        display_name="Z.AI",
        package_name="langchain-openai",
        static_models=("glm-5", "glm-5-turbo", "glm-4.7"),
        secret_fields=(_field("api_key", "API key", required=True, secret=True),),
        plain_fields=(
            _field(
                "base_url",
                "Base URL",
                placeholder=(
                    "https://api.z.ai/api/coding/paas/v4 (Coding Plan) or "
                    "https://api.z.ai/api/paas/v4 (pay-per-token PaaS)"
                ),
                help_text=(
                    "GLM Coding Plan subscribers must use the coding endpoint. "
                    "See https://docs.z.ai/devpack/overview for the list of "
                    "supported tools and restrictions."
                ),
            ),
        ),
        default_base_url=ZAI_DEFAULT_BASE_URL,
        runtime_provider="openai",
        openai_compatible=True,
        supports_lightweight_adapter=True,
        canonical_export_mode="zai_base_url",
    ),
}

PROVIDER_ORDER = tuple(PROVIDER_SPECS.keys())


def provider_spec(name: str) -> ProviderSpec | None:
    return PROVIDER_SPECS.get(name)


def default_base_url_for(provider_name: str) -> str:
    spec = provider_spec(provider_name)
    return spec.default_base_url if spec is not None else ""


def normalize_urlish(raw: str) -> str:
    value = raw.strip()
    if not value:
        return ""
    parsed = urlsplit(value)
    if not parsed.scheme or not parsed.netloc:
        return value.rstrip("/")
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path.rstrip("/"), "", "")).rstrip("/")


def normalize_ollama_base_url(base_url: str, api_key: str = "") -> str:
    raw = base_url.strip()
    if not raw:
        return "https://ollama.com" if api_key.strip() else "http://localhost:11434"

    parsed = urlsplit(raw)
    if not parsed.scheme or not parsed.netloc:
        return raw.rstrip("/")

    normalized_path = parsed.path.rstrip("/")
    if normalized_path == "/api":
        normalized_path = ""
    return urlunsplit((parsed.scheme, parsed.netloc, normalized_path, "", "")).rstrip("/")


def normalize_provider_plain_fields(
    provider_name: str,
    plain_fields: dict[str, str],
    secret_fields: dict[str, str] | None = None,
) -> dict[str, str]:
    normalized: dict[str, str] = {}
    spec = provider_spec(provider_name)
    if spec is None:
        return normalized
    secret_fields = secret_fields or {}
    for spec_field in spec.plain_fields:
        key = spec_field.name
        value = plain_fields.get(key, "").strip()
        if key == "base_url" and provider_name == "ollama":
            normalized[key] = normalize_ollama_base_url(value, secret_fields.get("api_key", ""))
            continue
        if key == "base_url" and provider_name == "zai":
            if is_zai_legacy_anthropic_base_url(value):
                normalized[key] = normalize_urlish(value)
            else:
                normalized[key] = normalize_urlish(normalize_zai_base_url(value))
            continue
        if key == "base_url" and spec.default_base_url:
            normalized[key] = normalize_urlish(value or spec.default_base_url)
            continue
        if key in {"base_url", "endpoint", "azure_endpoint", "url"}:
            normalized_value = normalize_urlish(value)
            if normalized_value:
                normalized[key] = normalized_value
            continue
        if value:
            normalized[key] = value
    return normalized


def canonical_endpoint_fingerprint_for_config(cfg: ProviderRuntimeConfig) -> str | None:
    spec = provider_spec(cfg.provider)
    if spec is None:
        return None
    mode = spec.canonical_export_mode
    if mode == "none":
        return None
    if mode == "ollama_cloud":
        api_key = cfg.secret_fields.get("api_key", "").strip()
        base_url = normalize_ollama_base_url(cfg.plain_fields.get("base_url", ""), api_key)
        if api_key and base_url == "https://ollama.com":
            return "ollama://cloud"
        return None
    if mode == "zai_base_url":
        raw_base_url = cfg.plain_fields.get("base_url", "").strip()
        if is_zai_legacy_anthropic_base_url(raw_base_url):
            return None
        normalized = normalize_urlish(normalize_zai_base_url(raw_base_url))
        if normalized in {ZAI_GENERAL_BASE_URL, ZAI_CODING_BASE_URL}:
            return normalized
        return None
    if mode == "default_base_url":
        default_base_url = spec.default_base_url
        normalized = normalize_urlish(cfg.plain_fields.get("base_url", "").strip() or default_base_url)
        if normalized == normalize_urlish(default_base_url):
            return normalized
        return None
    return f"{cfg.provider}://default"


@dataclass(slots=True)
class ProviderRuntimeConfig:
    provider: str
    enabled: bool
    priority: int
    selected_model: str
    plain_fields: dict[str, str] = field(default_factory=dict)
    secret_fields: dict[str, str] = field(default_factory=dict)
    last_validation_error: str = ""
    secret_status: str = "ok"
    secret_fields_enc_preserved: dict[str, str] = field(default_factory=dict)

    @property
    def model_name(self) -> str:
        if ":" in self.selected_model:
            return self.selected_model
        return f"{self.provider}:{self.selected_model}"
