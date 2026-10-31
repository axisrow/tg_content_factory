"""Smoke-test the core wheel installed WITHOUT the legacy UI packages (#1720).

CI builds every wheel into ``dist/``; this script installs only the core
(``tg_agent``) and ``tg_shared`` wheels into the CURRENT interpreter and checks:

* none of the UI packages or their UI-only third-party deps got pulled in;
* the CLI starts (``--help``);
* ``serve`` exits 1 with the tg-webui install hint (no traceback).

It installs into the running environment so the CI build job pays for the
core dependency closure once; the job installs the UI wheels afterwards. Run it
locally only from a throwaway venv. Works on Linux, macOS and Windows::

    python scripts/smoke_core_wheel.py dist
"""

from __future__ import annotations

import subprocess
import sys
import tempfile
from pathlib import Path

# The one list of UI-only modules: tests/test_core_without_surfaces.py hides
# exactly these. python-multipart is absent on purpose: it still reaches the
# core transitively (claude-agent-sdk -> mcp), which is fine — the core itself
# neither declares nor imports it.
UI_ONLY_MODULES = ("tg_webui", "tg_tui", "jinja2", "textual", "qrcode", "itsdangerous")

SERVE_HINT = "pip install -e packages/tg_webui"


def _core_wheels(dist: Path) -> list[str]:
    wheels = sorted(dist.glob("*.whl"))
    picked = [str(w) for w in wheels if w.name.startswith(("tg_agent-", "tg_shared-"))]
    if len(picked) != 2:
        raise SystemExit(f"expected the tg_agent and tg_shared wheels in {dist}, got {picked} of {wheels}")
    return picked


def main() -> int:
    dist = Path(sys.argv[1] if len(sys.argv) > 1 else "dist").resolve()
    python = sys.executable
    subprocess.check_call([python, "-m", "pip", "install", "--quiet", *_core_wheels(dist)])  # noqa: S603
    subprocess.check_call([python, "-m", "pip", "check"])  # noqa: S603

    # Outside the checkout so local sources cannot mask a broken wheel.
    with tempfile.TemporaryDirectory() as tmp:
        probe = (
            "import importlib.util, sys; "
            f"leaked = [m for m in {UI_ONLY_MODULES!r} if importlib.util.find_spec(m) is not None]; "
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
    if serve.returncode != 1 or SERVE_HINT not in output or "Traceback" in output:
        print(output)
        raise SystemExit(f"serve without tg-webui: expected exit 1 with the install hint, got {serve.returncode}")
    print("core wheel OK without tg-webui/tg-tui")
    return 0


if __name__ == "__main__":
    sys.exit(main())
