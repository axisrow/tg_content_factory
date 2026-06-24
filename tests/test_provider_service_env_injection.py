"""Env injection isolation for RuntimeProviderRegistry (#1050).

Root-cause fix for the provider parallel flake under ``-n auto``: the registry
used to read the real ``os.environ`` directly inside ``_register_env_providers``,
so a neighbouring xdist worker mutating ``OPENAI_API_KEY`` (and friends) could
"infect" an unrelated test's provider set non-deterministically (one such test
even reached the live OpenAI API).

These tests prove the registry is isolated *by design*: env arrives explicitly
and the registry NEVER touches the process environment. They must stay green
under ``-n auto`` without any autouse env-cleaner fixture, because the registry
no longer depends on global process state.
"""

from __future__ import annotations

import os

from src.services.provider_service import RuntimeProviderRegistry, build_provider_service


def test_explicit_env_registers_openai_provider():
    """env={OPENAI_API_KEY: ...} → openai provider is registered."""
    svc = RuntimeProviderRegistry(env={"OPENAI_API_KEY": "sk-explicit"})
    assert "openai" in svc._registry
    assert svc.has_providers()


def test_empty_env_yields_clean_registry():
    """env={} → only the 'default' stub, regardless of the real os.environ.

    This is the crux of the isolation fix: even if a neighbouring xdist worker
    has set OPENAI_API_KEY in the shared process env, an explicitly empty env
    must produce a clean registry.
    """
    svc = RuntimeProviderRegistry(env={})
    assert svc._registry == {"default": svc._registry["default"]}
    assert not svc.has_providers()


def test_default_env_none_does_not_read_os_environ(monkeypatch):
    """env=None (default) → registry ignores os.environ entirely.

    The registry must be clean by design; reading the process environment is the
    factory/call-site's job, not the registry's.
    """
    monkeypatch.setenv("OPENAI_API_KEY", "sk-from-process-env")
    svc = RuntimeProviderRegistry()
    assert "openai" not in svc._registry
    assert not svc.has_providers()


def test_explicit_env_does_not_leak_real_keys(monkeypatch):
    """An explicit env mapping fully overrides the real os.environ.

    Real OPENAI_API_KEY present in the process, but the explicit env supplies a
    *different* provider only → no openai provider leaks in.
    """
    monkeypatch.setenv("OPENAI_API_KEY", "sk-leak-attempt")
    svc = RuntimeProviderRegistry(env={"COHERE_API_KEY": "co-explicit"})
    assert "openai" not in svc._registry
    assert "cohere" in svc._registry


def test_openai_compat_env_provider_via_explicit_env():
    """OpenAI-compatible providers (together/deepseek/fireworks) come from env."""
    svc = RuntimeProviderRegistry(
        env={"TOGETHER_API_KEY": "tk", "TOGETHER_BASE": "https://api.together.xyz/v1"}
    )
    assert "together" in svc._registry


async def test_build_provider_service_snapshots_os_environ(monkeypatch):
    """build_provider_service(env=None) snapshots the *real* os.environ once.

    Prod call-sites rely on the factory to pull env from the process; this keeps
    env-based providers working in production while the registry stays pure.
    """
    monkeypatch.setenv("OPENAI_API_KEY", "sk-prod-snapshot")
    svc = await build_provider_service()
    assert "openai" in svc._registry


async def test_build_provider_service_explicit_env_overrides_process(monkeypatch):
    """build_provider_service(env=...) uses the explicit env, not os.environ."""
    monkeypatch.setenv("OPENAI_API_KEY", "sk-should-be-ignored")
    svc = await build_provider_service(env={})
    assert "openai" not in svc._registry
    assert not svc.has_providers()


def test_registry_body_has_no_os_environ_reads():
    """Guard: the registry source must not read os.environ in its body.

    Reading the process environment inside the registry is exactly the global
    state coupling that caused the #1050 flake. This static guard parses the
    registry's AST and fails if any ``os.environ`` *access* (attribute, call, or
    subscript) is reintroduced — comments/docstrings that merely mention the name
    are ignored.
    """
    import ast
    import inspect
    import textwrap

    tree = ast.parse(textwrap.dedent(inspect.getsource(RuntimeProviderRegistry)))
    offenders: list[int] = []
    for node in ast.walk(tree):
        # match `os.environ` attribute access regardless of how it's used
        if (
            isinstance(node, ast.Attribute)
            and node.attr == "environ"
            and isinstance(node.value, ast.Name)
            and node.value.id == "os"
        ):
            offenders.append(node.lineno)
    assert not offenders, (
        "RuntimeProviderRegistry must not access os.environ directly "
        f"(found at relative lines {offenders}); inject env explicitly (see #1050)."
    )


def test_make_service_module_keeps_os_import_for_factory():
    """Sanity: the factory still imports os to snapshot the process env."""
    import src.services.provider_service as mod

    assert hasattr(mod, "os")
    assert mod.os is os
