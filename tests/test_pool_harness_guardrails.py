from __future__ import annotations

import re
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
_FILES = [
    _REPO_ROOT / "tests/test_client_pool.py",
    _REPO_ROOT / "tests/test_client_pool_runtime.py",
    _REPO_ROOT / "tests/test_notification.py",
    _REPO_ROOT / "tests/test_search.py",
    _REPO_ROOT / "tests/test_import_web.py",
    _REPO_ROOT / "tests/test_collector_runtime.py",
    _REPO_ROOT / "tests/test_photo_loader.py",
]
_FORBIDDEN_PATTERNS = {
    r"\bpool\s*=\s*AsyncMock\b": "use RealPoolHarness instead of AsyncMock pool doubles",
    r"\bpool\s*=\s*MagicMock\b": "use RealPoolHarness instead of MagicMock pool doubles",
    r"make_mock_pool\(": "avoid fake pool helpers in runtime-sensitive tests",
    r"make_cli_pool\(": "seed connected accounts through RealPoolHarness",
    r"pool\.clients\[[^\]]+\]\s*=": "do not mutate pool.clients directly in rewritten tests",
}


def test_runtime_sensitive_tests_do_not_use_fake_pool_shortcuts():
    violations: list[str] = []

    for path in _FILES:
        content = path.read_text(encoding="utf-8")
        for pattern, message in _FORBIDDEN_PATTERNS.items():
            if re.search(pattern, content):
                violations.append(f"{path.name}: {message}")

    assert violations == []
