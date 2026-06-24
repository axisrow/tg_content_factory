"""Тесты для scripts/doc_coverage.py — отчёта doc-coverage (interrogate, #1072).

Логика отчёта (агрегация худших файлов, список недокументированного, гейт
`--fail-under`) проверяется на лёгких фейк-объектах, повторяющих форму
`interrogate.coverage.InterrogateResults`, — реальный interrogate в горячий
путь тестов не тащим (детерминированность + скорость, по образцу того, как
scripts/code_health.py опирается на формат radon, а не запускает его в тестах).
"""

from __future__ import annotations

import importlib.util
from dataclasses import dataclass, field
from pathlib import Path

import pytest

# scripts/ — не пакет; грузим модуль по пути, как это делают другие
# тесты-обёртки над вспомогательными скриптами.
_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "doc_coverage.py"
_spec = importlib.util.spec_from_file_location("doc_coverage", _SCRIPT)
assert _spec and _spec.loader
doc_coverage = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(doc_coverage)


# --------------------------------------------------------------------------- #
# Лёгкие фейки формы interrogate.coverage.*
# --------------------------------------------------------------------------- #


@dataclass
class FakeNode:
    """Зеркалит interrogate CovNode: документируемый узел (модуль/класс/функция)."""

    name: str
    covered: bool
    node_type: str = "FuncDef"
    lineno: int | None = 1


@dataclass
class FakeFileResult:
    """Зеркалит interrogate InterrogateFileResult."""

    filename: str
    total: int
    covered: int
    nodes: list[FakeNode] = field(default_factory=list)

    @property
    def missing(self) -> int:
        return self.total - self.covered

    @property
    def perc_covered(self) -> float:
        return 100.0 * self.covered / self.total if self.total else 100.0


@dataclass
class FakeResults:
    """Зеркалит interrogate InterrogateResults."""

    total: int
    covered: int
    file_results: list[FakeFileResult] = field(default_factory=list)

    @property
    def missing(self) -> int:
        return self.total - self.covered

    @property
    def perc_covered(self) -> float:
        return 100.0 * self.covered / self.total if self.total else 100.0


def _sample_results() -> FakeResults:
    files = [
        FakeFileResult(
            "src/good.py",
            total=2,
            covered=2,
            nodes=[FakeNode("a", True), FakeNode("b", True)],
        ),
        FakeFileResult(
            "src/bad.py",
            total=4,
            covered=1,
            nodes=[
                FakeNode("Module", True, node_type="Module", lineno=None),
                FakeNode("undoc_one", False, lineno=10),
                FakeNode("undoc_two", False, lineno=20),
                FakeNode("undoc_three", False, lineno=30),
            ],
        ),
        FakeFileResult(
            "src/mid.py",
            total=2,
            covered=1,
            nodes=[FakeNode("ok", True), FakeNode("nope", False, lineno=5)],
        ),
    ]
    total = sum(f.total for f in files)
    covered = sum(f.covered for f in files)
    return FakeResults(total=total, covered=covered, file_results=files)


# --------------------------------------------------------------------------- #
# worst_files — худшие по покрытию файлы
# --------------------------------------------------------------------------- #


def test_worst_files_orders_by_coverage_ascending() -> None:
    worst = doc_coverage.worst_files(_sample_results(), top=10)
    # bad.py (25%) хуже mid.py (50%); полностью покрытый good.py не попадает.
    assert [f.filename for f in worst] == ["src/bad.py", "src/mid.py"]


def test_worst_files_excludes_fully_documented() -> None:
    worst = doc_coverage.worst_files(_sample_results(), top=10)
    assert all(f.perc_covered < 100.0 for f in worst)


def test_worst_files_respects_top_limit() -> None:
    assert len(doc_coverage.worst_files(_sample_results(), top=1)) == 1


# --------------------------------------------------------------------------- #
# undocumented_nodes — плоский список недокументированного
# --------------------------------------------------------------------------- #


def test_undocumented_nodes_lists_only_missing() -> None:
    items = doc_coverage.undocumented_nodes(_sample_results())
    names = {name for _, _, name, _ in items}
    assert names == {"undoc_one", "undoc_two", "undoc_three", "nope"}


def test_undocumented_nodes_carries_location() -> None:
    items = doc_coverage.undocumented_nodes(_sample_results())
    by_name = {name: (filename, lineno) for filename, lineno, name, _ in items}
    assert by_name["undoc_one"] == ("src/bad.py", 10)
    assert by_name["nope"] == ("src/mid.py", 5)


# --------------------------------------------------------------------------- #
# Гейт --fail-under
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    ("perc", "threshold", "expected"),
    [
        (23.83, 20.0, 0),  # выше порога → ок
        (23.83, 23.83, 0),  # ровно на пороге → ок (>=)
        (23.83, 25.0, 1),  # ниже порога → провал
        (100.0, 80.0, 0),
    ],
)
def test_gate_exit_code(perc: float, threshold: float, expected: int) -> None:
    assert doc_coverage.gate_exit_code(perc, threshold) == expected


def test_gate_none_threshold_is_informational() -> None:
    # Без --fail-under прогон информационный: всегда exit 0.
    assert doc_coverage.gate_exit_code(0.0, None) == 0


# --------------------------------------------------------------------------- #
# split_pyproject_config — отделить служебные ключи от полей InterrogateConfig
# --------------------------------------------------------------------------- #


def test_split_pyproject_config_separates_service_keys() -> None:
    raw = {
        "fail_under": 23,
        "ignore_magic": True,
        "ignore_private": True,
        "exclude": ["tests", "scripts"],
        "verbose": 1,
    }
    conf_kwargs, excluded, fail_under = doc_coverage.split_pyproject_config(raw)
    # Служебные ключи не должны протечь в kwargs InterrogateConfig.
    assert conf_kwargs == {"ignore_magic": True, "ignore_private": True}
    assert excluded == ["tests", "scripts"]
    assert fail_under == 23.0


def test_split_pyproject_config_handles_empty() -> None:
    conf_kwargs, excluded, fail_under = doc_coverage.split_pyproject_config({})
    assert conf_kwargs == {}
    assert excluded == []
    assert fail_under is None


def test_known_config_kwargs_drops_unknown_keys() -> None:
    """Неизвестный для InterrogateConfig ключ отсеивается, а не роняет конструктор."""
    pytest.importorskip("interrogate.config")
    kept, dropped = doc_coverage.known_config_kwargs(
        {"ignore_magic": True, "generate_badge": "x", "output": "y"}
    )
    assert kept == {"ignore_magic": True}
    assert dropped == ["generate_badge", "output"]


def test_known_config_kwargs_keeps_all_valid() -> None:
    pytest.importorskip("interrogate.config")
    kept, dropped = doc_coverage.known_config_kwargs({"ignore_magic": True, "ignore_private": True})
    assert kept == {"ignore_magic": True, "ignore_private": True}
    assert dropped == []


def test_known_config_kwargs_result_constructs_config() -> None:
    """То, что прошло фильтр, гарантированно конструирует InterrogateConfig."""
    cfg = pytest.importorskip("interrogate.config")
    raw = cfg.parse_pyproject_toml("pyproject.toml") or {}
    conf_kwargs, _, _ = doc_coverage.split_pyproject_config(raw)
    # Подсунем заведомо чужой ключ — он не должен дойти до конструктора.
    conf_kwargs["definitely_not_a_field"] = 123
    kept, dropped = doc_coverage.known_config_kwargs(conf_kwargs)
    assert "definitely_not_a_field" in dropped
    cfg.InterrogateConfig(**kept)  # не должно бросить TypeError


def test_split_pyproject_config_matches_real_pyproject() -> None:
    """Реальный [tool.interrogate] раскладывается без лишних ключей.

    Это смоук на согласованность скрипта с pyproject: если в конфиг добавят
    ключ, который не является полем InterrogateConfig, конструирование conf в
    main() упадёт — здесь ловим заранее.
    """
    cfg = pytest.importorskip("interrogate.config")
    raw = cfg.parse_pyproject_toml("pyproject.toml") or {}
    if not raw:
        pytest.skip("в pyproject.toml нет [tool.interrogate]")
    conf_kwargs, _, fail_under = doc_coverage.split_pyproject_config(raw)
    # Все оставшиеся ключи должны быть валидными полями InterrogateConfig.
    cfg.InterrogateConfig(**conf_kwargs)
    assert fail_under is not None


# --------------------------------------------------------------------------- #
# format_percent — покраска/формат процента
# --------------------------------------------------------------------------- #


def test_format_percent_has_one_decimal() -> None:
    # Без TTY цвета нет — проверяем чистую числовую часть.
    assert "23.8%" in doc_coverage.format_percent(23.83)
