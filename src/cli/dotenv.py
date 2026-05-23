from __future__ import annotations

from pathlib import Path

from dotenv import load_dotenv


def load_cli_dotenv(config_path: str | Path = "config.yaml") -> None:
    path = Path(config_path).expanduser()
    if not path.is_absolute():
        path = (Path.cwd() / path).resolve()
    load_dotenv(path.parent / ".env", override=False)
