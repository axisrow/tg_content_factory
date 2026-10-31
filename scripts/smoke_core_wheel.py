"""Smoke-test the core wheel installed WITHOUT the legacy UI packages (#1720).

CI builds every wheel into ``dist/``; this script installs only the core
(``tg_agent``) and ``tg_shared`` wheels into a throwaway virtualenv and checks:

* none of the UI packages or their UI-only third-party deps got pulled in;
* the CLI starts (``--help``);
* ``serve`` exits 1 with the tg-webui install hint (no traceback).

Runs on Linux, macOS and Windows (CI build matrix). Usage::

    python scripts/smoke_core_wheel.py dist
"""

from __future__ import annotations

import subprocess
import sys
import tempfile
import venv
from pathlib import Path

# python-multipart is absent on purpose: it still reaches the core transitively
# (claude-agent-sdk -> mcp), which is fine — the core itself neither declares
# nor imports it (tests/test_core_without_surfaces.py hides it too).
UI_MODULES = ("tg_webui", "tg_tui", "jinja2", "textual", "qrcode", "itsdangerous")


def _core_wheels(dist: Path) -> list[str]:
    wheels = sorted(dist.glob("*.whl"))
    picked = [str(w) for w in wheels if w.name.startswith(("tg_agent-", "tg_shared-"))]
    if len(picked) != 2:
        raise SystemExit(f"expected the tg_agent and tg_shared wheels in {dist}, got {picked} of {wheels}")
    return picked


def main() -> int:
    dist = Path(sys.argv[1] if len(sys.argv) > 1 else "dist").resolve()
    wheels = _core_wheels(dist)
    with tempfile.TemporaryDirectory() as tmp:
        env_dir = Path(tmp) / "venv"
        venv.EnvBuilder(with_pip=True).create(env_dir)
        bin_dir = env_dir / ("Scripts" if sys.platform == "win32" else "bin")
        python = str(bin_dir / ("python.exe" if sys.platform == "win32" else "python"))

        subprocess.check_call([python, "-m", "pip", "install", "--quiet", *wheels])  # noqa: S603
        subprocess.check_call([python, "-m", "pip", "check"])  # noqa: S603

        probe = (
            "import importlib.util, sys; "
            f"leaked = [m for m in {UI_MODULES!r} if importlib.util.find_spec(m) is not None]; "
            "sys.exit(f'UI packages installed with the core wheel: {leaked}' if leaked else 0)"
        )
        subprocess.check_call([python, "-c", probe], cwd=tmp)  # noqa: S603
        subprocess.check_call([python, "-m", "tg_agent.main", "--help"], cwd=tmp, stdout=subprocess.DEVNULL)  # noqa: S603

        serve = subprocess.run(  # noqa: S603
            [python, "-m", "tg_agent.main", "serve", "--web-pass", "unused"],
            cwd=tmp,
            capture_output=True,
            text=True,
            check=False,
        )
        output = serve.stdout + serve.stderr
        if serve.returncode != 1 or "tg_webui" not in output or "Traceback" in output:
            print(output)
            raise SystemExit(f"serve without tg-webui: expected exit 1 with the install hint, got {serve.returncode}")
    print("core wheel OK without tg-webui/tg-tui")
    return 0


if __name__ == "__main__":
    sys.exit(main())
