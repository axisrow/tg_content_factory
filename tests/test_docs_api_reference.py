"""Тесты автодоков API-reference через mkdocstrings (#1071, эпик #1022).

mkdocstrings подключён к mkdocs, чтобы генерировать API-страницы из Python
docstring для не-FastAPI поверхностей (сервисы, agent-tools, CLI-internals).
Эти тесты — TDD-якорь конфиг-задачи: они фиксируют, что

  1. mkdocs.yml включает handler mkdocstrings и nav-секцию «API Reference»;
  2. reference-страницы существуют и ссылаются на ожидаемые модули через
     `::: src.<module>` синтаксис;
  3. `mkdocs build` проходит без ошибок и реально рендерит ожидаемые символы
     (классы/функции) в собранный site/ — то есть autodoc-pipeline жив.

Сборку (шаг 3) гоняем один раз в самом тяжёлом тесте: он помечен медленным и
изолированным, потому что поднимает griffe-обход всего пакета src/.
"""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

_ROOT = Path(__file__).resolve().parents[1]
_MKDOCS_YML = _ROOT / "mkdocs.yml"
_API_DIR = _ROOT / "docs" / "reference" / "api"

# Страницы фасадов и символы, которые на них обязаны отрендериться. Опираемся на
# хорошо документированные поверхности (agent-tools, сервисы), чтобы autodoc
# имел что показывать; конкретные имена — публичные точки входа, которые вряд ли
# исчезнут без сознательного рефакторинга (и тогда тест справедливо упадёт).
_EXPECTED_PAGES = {
    "services.md": ["src.services.collection_service", "src.services.pipeline_service"],
    "agent-tools-api.md": ["src.agent.tools.channels", "src.agent.tools.search"],
    "cli-internals.md": ["src.cli.runtime", "src.cli.commands.channel"],
}

# Символы, которые должны попасть в собранный HTML (доказывает, что griffe
# действительно прошёл модули, а не просто срендерил пустой заголовок).
_EXPECTED_RENDERED_SYMBOLS = [
    "CollectionService",
    "PipelineService",
    "list_channels",
]


def _load_mkdocs_config() -> dict:
    # mkdocs.yml содержит !!python/name-теги material — грузим небезопасным
    # загрузчиком, как это делает сам mkdocs.
    with _MKDOCS_YML.open(encoding="utf-8") as fh:
        return yaml.unsafe_load(fh)


def _plugins_list(config: dict) -> list:
    return config.get("plugins") or []


def _mkdocstrings_entry(config: dict):
    for plugin in _plugins_list(config):
        if isinstance(plugin, dict) and "mkdocstrings" in plugin:
            return plugin["mkdocstrings"]
        if plugin == "mkdocstrings":
            return {}
    return None


# --------------------------------------------------------------------------- #
# mkdocs.yml — плагин и nav
# --------------------------------------------------------------------------- #


def test_mkdocstrings_plugin_enabled() -> None:
    config = _load_mkdocs_config()
    assert _mkdocstrings_entry(config) is not None, "mkdocstrings не подключён в mkdocs.yml plugins"


def test_mkdocstrings_uses_python_handler() -> None:
    config = _load_mkdocs_config()
    entry = _mkdocstrings_entry(config)
    assert isinstance(entry, dict)
    handlers = entry.get("handlers", {})
    assert "python" in handlers, "не настроен python-handler mkdocstrings"


def test_mkdocstrings_tolerates_missing_docstrings() -> None:
    """show_if_no_docstring=true — autodoc рендерит символы и без docstring.

    Без этого `mkdocs build` упал бы на сотнях недокументированных символов;
    дописывание docstring до 100% — отдельная задача #1072 (interrogate).
    """
    config = _load_mkdocs_config()
    entry = _mkdocstrings_entry(config)
    assert isinstance(entry, dict)
    options = entry["handlers"]["python"].get("options", {})
    assert options.get("show_if_no_docstring") is True


def test_api_reference_section_in_nav() -> None:
    config = _load_mkdocs_config()

    def _walk(node) -> bool:
        if isinstance(node, dict):
            for key, value in node.items():
                if key == "API Reference":
                    return True
                if _walk(value):
                    return True
        elif isinstance(node, list):
            return any(_walk(item) for item in node)
        return False

    assert _walk(config.get("nav", [])), "в nav нет секции «API Reference»"


# --------------------------------------------------------------------------- #
# reference-страницы существуют и ссылаются на модули
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("page", sorted(_EXPECTED_PAGES))
def test_api_page_exists(page: str) -> None:
    assert (_API_DIR / page).is_file(), f"нет страницы автодоков docs/reference/api/{page}"


@pytest.mark.parametrize(("page", "modules"), sorted(_EXPECTED_PAGES.items()))
def test_api_page_references_modules(page: str, modules: list[str]) -> None:
    text = (_API_DIR / page).read_text(encoding="utf-8")
    for module in modules:
        assert f"::: {module}" in text, f"в {page} нет директивы `::: {module}`"


def test_api_pages_wired_into_nav() -> None:
    """Каждая api-страница присутствует в nav (иначе она не попадёт в сборку)."""
    config = _load_mkdocs_config()
    nav_text = yaml.safe_dump(config.get("nav", []), allow_unicode=True)
    for page in _EXPECTED_PAGES:
        assert f"reference/api/{page}" in nav_text, f"{page} не подключена в nav"


# --------------------------------------------------------------------------- #
# end-to-end: mkdocs build зелёный и рендерит символы
# --------------------------------------------------------------------------- #


@pytest.mark.integration
def test_mkdocs_build_renders_api_symbols(tmp_path: Path) -> None:
    """`mkdocs build --strict` проходит и autodoc рендерит ожидаемые символы.

    --strict — критерий приёмки #1071: сборка обязана быть чистой от warning
    (битые ссылки, нерезолвленные autoref'ы). Дорогой тест: griffe обходит весь
    пакет src/. Гоняем в изолированный site-dir внутри tmp_path, чтобы не трогать
    рабочую site/.
    """
    pytest.importorskip("mkdocstrings")
    pytest.importorskip("mkdocstrings_handlers.python", reason="нужен python-handler mkdocstrings")

    if shutil.which("mkdocs") is None:
        pytest.skip("mkdocs CLI недоступен")

    site_dir = tmp_path / "site"
    result = subprocess.run(
        [sys.executable, "-m", "mkdocs", "build", "--strict", "--site-dir", str(site_dir)],
        cwd=_ROOT,
        capture_output=True,
        text=True,
        timeout=300,
    )
    assert result.returncode == 0, f"mkdocs build --strict упал:\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"

    # Собранные api-страницы существуют и содержат отрендеренные символы.
    rendered = ""
    for page in _EXPECTED_PAGES:
        html = site_dir / "reference" / "api" / page.replace(".md", "") / "index.html"
        assert html.is_file(), f"страница {page} не собралась в site/"
        rendered += html.read_text(encoding="utf-8")

    for symbol in _EXPECTED_RENDERED_SYMBOLS:
        assert symbol in rendered, f"символ {symbol} не отрендерился в собранных api-страницах"
