from __future__ import annotations

import argparse
import asyncio

from src.agent.provider_registry import PROVIDER_SPECS, ProviderRuntimeConfig, provider_spec
from src.cli import runtime
from src.services.agent_provider_service import ProviderConfigService


def run(args: argparse.Namespace) -> None:
    async def _run() -> None:
        config, db = await runtime.init_db(args.config)
        svc = ProviderConfigService(db, config)
        try:
            if args.provider_action == "list":
                configs = await svc.load_provider_configs()
                cache = await svc.load_model_cache()
                if not configs:
                    print("No providers configured.")
                    print(f"Available providers: {', '.join(PROVIDER_SPECS.keys())}")
                    return
                fmt = "{:<15} {:<8} {:<8} {:<25} {:<6} {:<30}"
                print(fmt.format("Provider", "Enabled", "Priority", "Selected model", "Models", "Error"))
                print("-" * 100)
                for cfg in configs:
                    entry = cache.get(cfg.provider)
                    model_count = str(len(entry.models)) if entry else "—"
                    error = (cfg.last_validation_error or "")[:30]
                    print(fmt.format(
                        cfg.provider,
                        "Yes" if cfg.enabled else "No",
                        str(cfg.priority),
                        (cfg.selected_model or "—")[:25],
                        model_count,
                        error,
                    ))

            elif args.provider_action == "add":
                name = args.name.lower()
                spec = provider_spec(name)
                if spec is None:
                    print(f"Unknown provider: {name}")
                    print(f"Available: {', '.join(PROVIDER_SPECS.keys())}")
                    return
                if not svc.writes_enabled:
                    print("ERROR: SESSION_ENCRYPTION_KEY is required to manage providers.")
                    return

                configs = await svc.load_provider_configs()
                existing = next((c for c in configs if c.provider == name), None)

                secret_fields = {}
                plain_fields = {}
                for field in spec.secret_fields:
                    if field.name == "api_key":
                        secret_fields["api_key"] = args.api_key
                    elif existing:
                        secret_fields[field.name] = existing.secret_fields.get(field.name, "")
                for field in spec.plain_fields:
                    if field.name == "base_url" and args.base_url:
                        plain_fields["base_url"] = args.base_url
                    elif existing:
                        plain_fields[field.name] = existing.plain_fields.get(field.name, "")

                new_cfg = ProviderRuntimeConfig(
                    provider=name,
                    enabled=True,
                    priority=existing.priority if existing else 0,
                    selected_model=existing.selected_model if existing else "",
                    plain_fields=plain_fields,
                    secret_fields=secret_fields,
                )

                if existing:
                    configs = [new_cfg if c.provider == name else c for c in configs]
                else:
                    configs.append(new_cfg)

                await svc.save_provider_configs(configs)
                verb = "Updated" if existing else "Added"
                print(f"{verb} provider: {name}")

            elif args.provider_action == "delete":
                name = args.name.lower()
                if not svc.writes_enabled:
                    print("ERROR: SESSION_ENCRYPTION_KEY is required to manage providers.")
                    return
                configs = await svc.load_provider_configs()
                new_configs = [c for c in configs if c.provider != name]
                if len(new_configs) == len(configs):
                    print(f"Provider '{name}' not found.")
                    return
                await svc.save_provider_configs(new_configs)
                print(f"Deleted provider: {name}")

            elif args.provider_action == "probe":
                name = args.name.lower()
                configs = await svc.load_provider_configs()
                cfg = next((c for c in configs if c.provider == name), None)
                if cfg is None:
                    print(f"Provider '{name}' not configured. Add it first.")
                    return
                print(f"Probing {name}...")
                try:
                    entry = await svc.refresh_models_for_provider(name, cfg)
                    if entry.error:
                        print(f"WARN: {entry.error}")
                    print(f"OK: {len(entry.models)} models available (source: {entry.source})")
                    if entry.models:
                        for m in entry.models[:10]:
                            print(f"  {m}")
                        if len(entry.models) > 10:
                            print(f"  ... and {len(entry.models) - 10} more")
                except Exception as exc:
                    print(f"FAIL: {exc}")

            elif args.provider_action == "refresh":
                name = args.name
                if name:
                    name = name.lower()
                    print(f"Refreshing models for {name}...")
                    try:
                        entry = await svc.refresh_models_for_provider(name)
                        print(f"OK: {len(entry.models)} models (source: {entry.source})")
                    except Exception as exc:
                        print(f"FAIL: {exc}")
                else:
                    print("Refreshing all providers...")
                    results = await svc.refresh_all_models()
                    for prov, entry in results.items():
                        status = "OK" if not entry.error else f"WARN: {entry.error}"
                        print(f"  {prov}: {len(entry.models)} models — {status}")

            elif args.provider_action == "test-all":
                configs = await svc.load_provider_configs()
                if not configs:
                    print("No providers configured.")
                    return
                print(f"Testing {len(configs)} provider(s)...\n")
                for cfg in configs:
                    print(f"  {cfg.provider}...", end=" ", flush=True)
                    try:
                        entry = await svc.refresh_models_for_provider(cfg.provider, cfg)
                        if entry.error:
                            print(f"WARN ({entry.error})")
                        else:
                            print(f"OK ({len(entry.models)} models)")
                    except Exception as exc:
                        print(f"FAIL ({exc})")
        finally:
            await db.close()

    asyncio.run(_run())
