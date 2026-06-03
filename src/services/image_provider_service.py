"""Settings-driven image generation provider management.

Stores provider API keys (encrypted) in the DB settings table and builds
image adapters from them, falling back to environment variables when no
DB config exists for a given provider.
"""

from __future__ import annotations

import json
import logging
import os
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from src.config import AppConfig, resolve_session_encryption_secret
from src.database import Database
from src.security import SessionCipher, decrypt_failure_status, log_expected_decrypt_failure
from src.utils.json import safe_json_dumps

if TYPE_CHECKING:
    from src.services.provider_adapters import ImageAdapter

logger = logging.getLogger(__name__)

SETTINGS_KEY = "image_providers_v1"
_IMAGE_SECRET_ACTION = "restore_key_or_reenter_secret"


def _pa():  # noqa: ANN202 - tiny lazy-import shim
    """Lazy access to provider_adapters (avoids an import cycle at module load)."""
    import src.services.provider_adapters as provider_adapters

    return provider_adapters


def _m(mid: str, provider: str, desc: str) -> dict:
    """Build one static-catalog model entry (``provider:model_id`` convention)."""
    return {"id": mid, "model_string": f"{provider}:{mid}", "description": desc, "run_count": 0}


@dataclass(slots=True)
class ImageProviderSpec:
    """Declarative description of one image provider.

    Every per-provider behaviour is data here, so registration / refresh / UI
    are one uniform loop rather than per-provider branches. A provider is
    *keyless* iff it carries a ``detect`` callable (auth comes from elsewhere,
    e.g. the Codex CLI); otherwise it is *keyed* and registered from a DB key or
    an env var listed in ``env_vars``.
    """

    name: str
    display_name: str
    env_vars: list[str]
    # keyed providers: build from an API key/token (first positional arg)
    keyed_factory: Callable[[str], "ImageAdapter"] | None = None
    # keyless providers: build with no key; registered iff ``detect()`` is True
    keyless_factory: Callable[[], "ImageAdapter"] | None = None
    detect: Callable[[], bool] | None = None
    # search_models() fallback catalog (providers without a live search API)
    static_catalog: list[dict] = field(default_factory=list)
    # search_models(refresh=True) live fetch; receives the service so instance
    # monkeypatching of the underlying _fetch_* methods is honoured
    fetch_models: Callable[..., Awaitable[list[dict]]] | None = None
    # When True, never chosen as the implicit default adapter — only on explicit
    # ``provider:model`` selection. Set for adapters that are slow/expensive to
    # invoke (Codex spawns a blocking subprocess), so an unqualified generate()
    # never silently triggers them.
    explicit_only: bool = False

    @property
    def keyless(self) -> bool:
        return self.detect is not None


IMAGE_PROVIDER_SPECS: dict[str, ImageProviderSpec] = {
    "together": ImageProviderSpec(
        "together",
        "Together AI",
        ["TOGETHER_API_KEY"],
        keyed_factory=lambda key: _pa().make_together_image_adapter(key),
        static_catalog=[
            _m("black-forest-labs/FLUX.1-schnell", "together", "FLUX.1 Schnell — fast"),
            _m("black-forest-labs/FLUX.1-dev", "together", "FLUX.1 Dev — high quality"),
        ],
    ),
    "huggingface": ImageProviderSpec(
        "huggingface",
        "HuggingFace",
        ["HUGGINGFACE_API_KEY", "HUGGINGFACE_TOKEN"],
        keyed_factory=lambda key: _pa().make_huggingface_image_adapter(key),
    ),
    "openai": ImageProviderSpec(
        "openai",
        "OpenAI",
        ["OPENAI_API_KEY"],
        keyed_factory=lambda key: _pa().make_openai_image_adapter(key),
        # Only the confirmed model id goes in the static fallback; other
        # gpt-image-* variants surface via the live `refresh` path if/when OpenAI
        # actually reports them (offering a model that 400s would make generate()
        # fail silently). dall-e-* kept as legacy so saved selections keep
        # resolving in the UI.
        static_catalog=[
            _m("gpt-image-1", "openai", "GPT Image 1 — OpenAI image generation"),
            _m("dall-e-3", "openai", "DALL-E 3 — legacy"),
            _m("dall-e-2", "openai", "DALL-E 2 — legacy"),
        ],
        # openai resolves its token from the DB key or env; the fetcher returns
        # [] without a network call when neither is present.
        fetch_models=lambda svc, api_key="": svc._fetch_openai_image_models(
            api_key or os.environ.get("OPENAI_API_KEY", "")
        ),
    ),
    "replicate": ImageProviderSpec(
        "replicate",
        "Replicate",
        ["REPLICATE_API_TOKEN"],
        keyed_factory=lambda key: _pa().make_replicate_image_adapter(key),
    ),
    # Codex is keyless — auth comes from the Codex CLI (~/.codex/auth.json), so
    # there are no env vars to configure; it is registered on detection and its
    # live model list is pulled from the SDK on refresh.
    "codex": ImageProviderSpec(
        "codex",
        "Codex SDK",
        [],
        keyless_factory=lambda: _pa().make_codex_image_adapter(),
        detect=lambda: _pa().codex_available(),
        static_catalog=[_m("gpt-5.4", "codex", "Codex gpt-5.4 — image via $imagegen")],
        fetch_models=lambda svc, api_key="": svc._fetch_codex_models(),
        explicit_only=True,
    ),
}

IMAGE_PROVIDER_ORDER: list[str] = ["together", "huggingface", "openai", "replicate", "codex"]


@dataclass(slots=True)
class ImageProviderConfig:
    provider: str
    enabled: bool = True
    api_key: str = ""
    _api_key_enc_preserved: str = ""  # raw encrypted value kept on decrypt failure
    secret_status: str = "ok"


def image_provider_spec(name: str) -> ImageProviderSpec | None:
    return IMAGE_PROVIDER_SPECS.get(name)


def _env_key(env_vars: list[str]) -> str:
    """First non-empty environment value among *env_vars*, or empty string."""
    return next((os.environ.get(v, "").strip() for v in env_vars if os.environ.get(v, "").strip()), "")


class ImageProviderService:
    def __init__(self, db: Database, config: AppConfig) -> None:
        self._db = db
        secret = resolve_session_encryption_secret(config)
        self._cipher = SessionCipher(secret) if secret else None

    @property
    def writes_enabled(self) -> bool:
        return self._cipher is not None

    # ── load / save ──

    async def load_provider_configs(self) -> list[ImageProviderConfig]:
        raw = await self._db.get_setting(SETTINGS_KEY)
        if not raw:
            return []
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            logger.warning("Invalid image provider JSON in settings", exc_info=True)
            return []
        if not isinstance(parsed, list):
            return []

        configs: list[ImageProviderConfig] = []
        for item in parsed:
            if not isinstance(item, dict):
                continue
            provider = str(item.get("provider", "")).strip()
            if provider not in IMAGE_PROVIDER_SPECS:
                continue
            api_key = ""
            preserved_enc = ""
            secret_status = "ok"
            enc_key = str(item.get("api_key_enc", "")).strip()
            if enc_key:
                if self._cipher is None:
                    preserved_enc = enc_key
                    secret_status = "missing_key"
                    log_expected_decrypt_failure(
                        logger,
                        resource="image_provider",
                        identifier=provider,
                        status=secret_status,
                        action=_IMAGE_SECRET_ACTION,
                        level=logging.DEBUG,
                    )
                else:
                    try:
                        api_key = self._cipher.decrypt(enc_key)
                    except ValueError as exc:
                        preserved_enc = enc_key  # keep raw so save doesn't destroy it
                        status = decrypt_failure_status(exc)
                        secret_status = "decrypt_failed" if status == "key_mismatch" else status
                        log_expected_decrypt_failure(
                            logger,
                            resource="image_provider",
                            identifier=provider,
                            status=status,
                            action=_IMAGE_SECRET_ACTION,
                            level=logging.DEBUG,
                        )
            configs.append(
                ImageProviderConfig(
                    provider=provider,
                    enabled=bool(item.get("enabled", True)),
                    api_key=api_key,
                    _api_key_enc_preserved=preserved_enc,
                    secret_status=secret_status,
                )
            )
        return configs

    async def save_provider_configs(self, configs: list[ImageProviderConfig]) -> None:
        if not self.writes_enabled:
            raise RuntimeError("SESSION_ENCRYPTION_KEY is required to manage image providers.")
        payload = []
        for cfg in configs:
            entry: dict[str, Any] = {
                "provider": cfg.provider,
                "enabled": cfg.enabled,
            }
            if cfg.api_key.strip():
                assert self._cipher is not None
                entry["api_key_enc"] = self._cipher.encrypt(cfg.api_key.strip())
            elif cfg._api_key_enc_preserved:
                entry["api_key_enc"] = cfg._api_key_enc_preserved
            payload.append(entry)
        await self._db.set_setting(SETTINGS_KEY, safe_json_dumps(payload, ensure_ascii=False))

    # ── UI helpers ──

    def create_empty_config(self, provider_name: str) -> ImageProviderConfig:
        return ImageProviderConfig(provider=provider_name, enabled=True, api_key="")

    def build_provider_views(
        self, configs: list[ImageProviderConfig]
    ) -> list[dict[str, Any]]:
        views = []
        for cfg in configs:
            spec = IMAGE_PROVIDER_SPECS.get(cfg.provider)
            if spec is None:
                continue
            env_var_set = any(os.environ.get(v, "").strip() for v in spec.env_vars)
            views.append(
                {
                    "provider": cfg.provider,
                    "display_name": spec.display_name,
                    "enabled": cfg.enabled,
                    "keyless": spec.keyless,
                    "has_key": bool(cfg.api_key.strip()),
                    "secret_status": cfg.secret_status,
                    "requires_secret_reentry": cfg.secret_status != "ok",
                    "env_var_set": env_var_set,
                    "env_var_names": ", ".join(spec.env_vars),
                }
            )
        return views

    def parse_provider_form(
        self,
        form: Any,
        existing: list[ImageProviderConfig],
    ) -> list[ImageProviderConfig]:
        existing_map = {cfg.provider: cfg for cfg in existing}
        configs: list[ImageProviderConfig] = []
        for name in IMAGE_PROVIDER_ORDER:
            if not form.get(f"img_provider_present__{name}"):
                continue
            enabled = str(form.get(f"img_provider_enabled__{name}", "")).strip() == "1"
            new_key = str(form.get(f"img_provider_secret__{name}__api_key", "")).strip()
            old_cfg = existing_map.get(name)
            preserved = ""
            if new_key:
                api_key = new_key
                secret_status = "ok"
            elif old_cfg:
                api_key = old_cfg.api_key
                preserved = old_cfg._api_key_enc_preserved
                secret_status = old_cfg.secret_status if preserved else "ok"
            else:
                api_key = ""
                secret_status = "ok"
            configs.append(
                ImageProviderConfig(
                    provider=name,
                    enabled=enabled,
                    api_key=api_key,
                    _api_key_enc_preserved=preserved,
                    secret_status=secret_status,
                )
            )
        return configs

    # ── adapter construction ──

    def build_adapters(self, configs: list[ImageProviderConfig]) -> dict[str, "ImageAdapter"]:
        """Build image adapters from the provider table.

        One uniform loop over :data:`IMAGE_PROVIDER_ORDER`: a disabled DB config
        skips the provider (keyed or keyless alike); keyed providers register
        from a DB key falling back to env; keyless providers register on
        ``detect()``. Adding a provider is one :class:`ImageProviderSpec` entry.
        """
        cfg_by_provider = {cfg.provider: cfg for cfg in configs}
        adapters: dict[str, "ImageAdapter"] = {}

        for name in IMAGE_PROVIDER_ORDER:
            spec = IMAGE_PROVIDER_SPECS[name]
            cfg = cfg_by_provider.get(name)
            if cfg is not None and not cfg.enabled:
                continue
            if spec.keyless:
                if spec.detect() and spec.keyless_factory is not None:
                    adapters[name] = spec.keyless_factory()
            else:
                key = (cfg.api_key.strip() if cfg else "") or _env_key(spec.env_vars)
                if key and spec.keyed_factory is not None:
                    adapters[name] = spec.keyed_factory(key)

        return adapters
