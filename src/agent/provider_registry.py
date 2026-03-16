from __future__ import annotations

from dataclasses import dataclass, field

from src.agent.models import CLAUDE_MODEL_IDS


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

    @property
    def all_fields(self) -> tuple[ProviderFieldSpec, ...]:
        return self.plain_fields + self.secret_fields


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
    ),
    "anthropic": ProviderSpec(
        name="anthropic",
        display_name="Anthropic",
        package_name="langchain-anthropic",
        static_models=tuple(CLAUDE_MODEL_IDS),
        secret_fields=(_field("api_key", "API key", required=True, secret=True),),
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
    ),
    "cohere": ProviderSpec(
        name="cohere",
        display_name="Cohere",
        package_name="langchain-cohere",
        static_models=("command-r-plus", "command-r", "command-a-03-2025"),
        secret_fields=(_field("api_key", "API key", required=True, secret=True),),
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
    ),
    "mistralai": ProviderSpec(
        name="mistralai",
        display_name="Mistral",
        package_name="langchain-mistralai",
        static_models=("mistral-large-latest", "mistral-medium-latest", "ministral-8b-latest"),
        secret_fields=(_field("api_key", "API key", required=True, secret=True),),
    ),
    "huggingface": ProviderSpec(
        name="huggingface",
        display_name="HuggingFace",
        package_name="langchain-huggingface",
        static_models=("microsoft/Phi-3-mini-4k-instruct", "meta-llama/Llama-3.1-8B-Instruct"),
        secret_fields=(_field("api_key", "API token", secret=True),),
    ),
    "groq": ProviderSpec(
        name="groq",
        display_name="Groq",
        package_name="langchain-groq",
        static_models=("llama-3.3-70b-versatile", "llama-3.1-8b-instant", "mixtral-8x7b-32768"),
        secret_fields=(_field("api_key", "API key", required=True, secret=True),),
    ),
    "ollama": ProviderSpec(
        name="ollama",
        display_name="Ollama",
        package_name="langchain-ollama",
        static_models=("llama3.2", "qwen2.5", "deepseek-r1", "gpt-oss:120b"),
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
    ),
    "deepseek": ProviderSpec(
        name="deepseek",
        display_name="DeepSeek",
        package_name="langchain-deepseek",
        static_models=("deepseek-chat", "deepseek-reasoner"),
        secret_fields=(_field("api_key", "API key", required=True, secret=True),),
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
    ),
    "perplexity": ProviderSpec(
        name="perplexity",
        display_name="Perplexity",
        package_name="langchain-perplexity",
        static_models=("sonar", "sonar-pro", "sonar-reasoning"),
        secret_fields=(_field("api_key", "API key", required=True, secret=True),),
    ),
}

PROVIDER_ORDER = tuple(PROVIDER_SPECS.keys())


def provider_spec(name: str) -> ProviderSpec | None:
    return PROVIDER_SPECS.get(name)


@dataclass(slots=True)
class ProviderRuntimeConfig:
    provider: str
    enabled: bool
    priority: int
    selected_model: str
    plain_fields: dict[str, str] = field(default_factory=dict)
    secret_fields: dict[str, str] = field(default_factory=dict)
    last_validation_error: str = ""

    @property
    def model_name(self) -> str:
        if ":" in self.selected_model:
            return self.selected_model
        return f"{self.provider}:{self.selected_model}"
