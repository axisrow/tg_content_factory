"""Регресс-гард: warm_all_dialogs()/leave_channels() — НЕ самописный retry.

Детектор «изобретённого велосипеда» (`scripts/detect_reinvented.py`, эпик #1083)
помечает обе функции med-confidence находкой «самописный retry/backoff-цикл →
tenacity.retry / aiolimiter» (issue #1114). Это **false-positive** эвристики
`_detect_retry_loop`: она ловит «цикл + try/except + asyncio.sleep» и по своему
же docstring предупреждает про «легитимные poll-циклы».

Семантически оба цикла — НЕ повторная попытка одной операции с backoff:

* `warm_all_dialogs()` итерирует по РАЗНЫМ аккаунтам (`_connected_phones()`),
  греет кэш каждого ровно один раз; `asyncio.sleep(WARM_STAGGER_DELAY_SEC)` —
  фиксированный inter-account стаггер (rate-limit вежливость), а не backoff. На
  ошибке аккаунт пропускается — не повторяется. FloodWait делегирован
  `run_with_flood_wait` (single-shot, намеренно fail-fast — см. docstring).
* `leave_channels()` итерирует по РАЗНЫМ диалогам, покидает каждый один раз;
  `asyncio.sleep(0.3)` — стаггер на success-пути. На FloodWait — `break`
  (мутирующие leave-флоу НЕ авто-ретраятся, правило проекта). Тоже делегирует
  FloodWait в `run_with_flood_wait`.

Замена этих циклов на `tenacity.retry` была бы поведенческой ошибкой: tenacity
повторно вызывает ОДИН callable на ошибке, а здесь на ошибке нужно перейти к
СЛЕДУЮЩЕМУ элементу. Реальный retry/backoff в проекте уже централизован в
`src/telegram/flood_wait.py::run_with_flood_wait_retry`.

Гард фиксирует инвариант «делегирование single-shot, а не велосипед», чтобы
будущий рефактор не превратил эти методы в настоящий hand-rolled retry-цикл.

Что именно стережём (после cycle-review #1149, Codex + Claude):
1. метод делегирует FloodWait ТОЛЬКО `run_with_flood_wait` (single-shot) и НЕ
   `run_with_flood_wait_retry` — последний делает `while True`+sleep+повтор
   callable (flood_wait.py:190), т.е. настоящий retry; для warm/leave это
   противоречит заявленной fail-fast-семантике (Codex п.2);
2. нет backoff-`sleep` внутри `except`-хендлера и нет вложенного retry-цикла —
   ровно та «плоская» мутация (`except ...: await asyncio.sleep(30)`), что
   проходила прежний nested-only гард, но это уже velosiped (Claude #1).

Оба инварианта мутационно проверены ниже (`test_*_mutation_*`): подмена
делегирования или внедрение backoff-в-except краснит гард. Решение по находке
#1114 («оставить» как false-positive) — за владельцем (#782).
"""

from __future__ import annotations

import ast
import inspect
import textwrap

import pytest

from src.telegram.pool_dialogs import DialogsMixin

# Единственный допустимый способ обработки FloodWait в этих двух методах:
# single-shot helper. ``run_with_flood_wait_retry`` (петля повторов) здесь
# запрещён намеренно — warm/leave должны fail-fast (Codex-ревью #1149).
_ALLOWED_FLOOD_HELPER = "run_with_flood_wait"
_FORBIDDEN_FLOOD_HELPER = "run_with_flood_wait_retry"

_GUARDED_METHODS = ["warm_all_dialogs", "leave_channels"]


def _method_source(name: str) -> str:
    """Исходник метода миксина по имени, dedent'нутый под парсинг."""
    func = getattr(DialogsMixin, name)
    return textwrap.dedent(inspect.getsource(func))


def _method_ast(name: str) -> ast.AsyncFunctionDef:
    node = ast.parse(_method_source(name)).body[0]
    assert isinstance(node, ast.AsyncFunctionDef), f"{name} должен быть async def"
    return node


def _called_names(node: ast.AST) -> set[str]:
    """Имена вызываемых функций (по `.id`/`.attr`) во всём поддереве."""
    names: set[str] = set()
    for call in ast.walk(node):
        if not isinstance(call, ast.Call):
            continue
        func = call.func
        if isinstance(func, ast.Name):
            names.add(func.id)
        elif isinstance(func, ast.Attribute):
            names.add(func.attr)
    return names


def _sleep_inside_except(node: ast.AST) -> ast.Call | None:
    """Первый `*.sleep(...)`/`sleep(...)` внутри любого except-хендлера, или None.

    Backoff-после-перехвата — характерная подпись самописного retry: «поймали
    ошибку → подождали → (повторяем)». На легитимном пути этих методов sleep
    стоит на success-ветке / между итерациями, НЕ в `except`. Это устойчивый
    различитель велосипеда от стаггера (закрывает «плоскую» мутацию из ревью,
    которую nested-only проверка пропускала).
    """
    for handler in ast.walk(node):
        if not isinstance(handler, ast.ExceptHandler):
            continue
        for call in ast.walk(handler):
            if isinstance(call, ast.Call):
                fn = call.func
                attr = fn.attr if isinstance(fn, ast.Attribute) else None
                ident = fn.id if isinstance(fn, ast.Name) else None
                if attr == "sleep" or ident == "sleep":
                    return call
    return None


def _nested_loop_with_sleep(node: ast.AST) -> ast.AST | None:
    """Вложенный цикл, в теле которого есть sleep, или None.

    Классический retry — «цикл попыток ВОКРУГ одной операции». Внешний цикл по
    коллекции легитимен; вложенный цикл со sleep — backoff.
    """
    loops = [n for n in ast.walk(node) if isinstance(n, (ast.For, ast.While))]
    for outer in loops:
        for inner in ast.walk(outer):
            if inner is outer or not isinstance(inner, (ast.For, ast.While)):
                continue
            for call in ast.walk(inner):
                if isinstance(call, ast.Call):
                    fn = call.func
                    if (isinstance(fn, ast.Attribute) and fn.attr == "sleep") or (
                        isinstance(fn, ast.Name) and fn.id == "sleep"
                    ):
                        return inner
    return None


@pytest.mark.parametrize("method_name", _GUARDED_METHODS)
def test_method_delegates_only_to_single_shot_helper(method_name: str) -> None:
    """FloodWait делегирован ТОЛЬКО `run_with_flood_wait`, без retry-петли.

    Делегирование single-shot helper'у = «не велосипед». Появление ручного
    `while attempt<N: ... sleep` (нет вызова helper) ИЛИ переход на
    `run_with_flood_wait_retry` (петля повторов внутри метода) краснит гард.
    """
    called = _called_names(_method_ast(method_name))
    assert _ALLOWED_FLOOD_HELPER in called, (
        f"{method_name}() обязан делегировать FloodWait в "
        f"`{_ALLOWED_FLOOD_HELPER}`; найдено вызовов: {sorted(called)}. "
        "Самописный retry-цикл здесь запрещён — см. модульный docstring и #1114."
    )
    assert _FORBIDDEN_FLOOD_HELPER not in called, (
        f"{method_name}() не должен звать `{_FORBIDDEN_FLOOD_HELPER}` "
        "(петля повторов): warm/leave намеренно fail-fast single-shot. "
        "Это превратило бы метод в retry — см. cycle-review #1149 (Codex п.2)."
    )


@pytest.mark.parametrize("method_name", _GUARDED_METHODS)
def test_method_has_no_handrolled_backoff(method_name: str) -> None:
    """Нет backoff-`sleep` в `except` и нет вложенного retry-цикла со `sleep`.

    Две формы hand-rolled retry: «поймал-подождал-повторил» (sleep в except) и
    «цикл попыток вокруг операции» (вложенный цикл со sleep). Обе запрещены;
    легитимный inter-item / success-path стаггер этих методов под них не подходит.
    """
    node = _method_ast(method_name)

    except_sleep = _sleep_inside_except(node)
    assert except_sleep is None, (
        f"{method_name}(): `sleep` внутри except-хендлера на строке "
        f"{except_sleep.lineno} — это backoff-после-ошибки (самописный retry). "
        "Делегируй повтор в run_with_flood_wait* вместо ручного цикла (#1114)."
    )

    nested = _nested_loop_with_sleep(node)
    assert nested is None, (
        f"{method_name}(): вложенный цикл со sleep на строке "
        f"{getattr(nested, 'lineno', '?')} — самописный retry/backoff (#1114)."
    )


# --------------------------------------------------------------------------- #
# Мутационные страховки: гард обязан КРАСНЕТЬ на каждой форме настоящего retry,
# иначе он вакуумен (находки cycle-review #1149: прежний nested-only гард
# пропускал плоский backoff-в-except и retry через разрешённый helper).
# Проверяем на синтетических телах функций, чтобы не трогать продкод.
# --------------------------------------------------------------------------- #

# (1) Плоский backoff в except — ровно мутация, что проходила старый гард зелёной.
_FLAT_BACKOFF_RETRY = '''
    import asyncio
    async def leave_channels(self, phone, dialogs):
        for cid, ctype in dialogs:
            try:
                await run_with_flood_wait(self._remove(cid), operation="x")
                outcomes[cid] = True
            except Exception:
                await asyncio.sleep(30)
'''

# (2) Настоящий retry через РАЗРЕШЁННЫЙ-на-вид helper-повтор (Codex п.2).
_RETRY_VIA_RETRY_HELPER = '''
    async def warm_all_dialogs(self):
        for phone in self._connected_phones():
            await run_with_flood_wait_retry(
                lambda: self._warm(phone), operation="x",
            )
'''

# (3) Вложенный retry-цикл со sleep вокруг одной операции.
_NESTED_RETRY_LOOP = '''
    import asyncio
    async def leave_channels(self, phone, dialogs):
        for cid, ctype in dialogs:
            attempt = 0
            while attempt < 3:
                try:
                    await run_with_flood_wait(self._remove(cid), operation="x")
                    break
                except Exception:
                    attempt += 1
                    await asyncio.sleep(2 ** attempt)
'''


def _synthetic_ast(body: str) -> ast.AsyncFunctionDef:
    node = ast.parse(textwrap.dedent(body)).body[-1]
    assert isinstance(node, ast.AsyncFunctionDef)
    return node


def test_mutation_flat_backoff_in_except_is_red() -> None:
    """Плоский `except: await asyncio.sleep(30)` (с разрешённым helper) — RED."""
    node = _synthetic_ast(_FLAT_BACKOFF_RETRY)
    # Делегирование на месте (helper зовётся) — старый гард был бы зелёным…
    assert _ALLOWED_FLOOD_HELPER in _called_names(node)
    # …но backoff-в-except теперь ловится:
    assert _sleep_inside_except(node) is not None


def test_mutation_retry_helper_in_loop_is_red() -> None:
    """Повтор через `run_with_flood_wait_retry` в цикле метода — RED (Codex п.2)."""
    node = _synthetic_ast(_RETRY_VIA_RETRY_HELPER)
    assert _FORBIDDEN_FLOOD_HELPER in _called_names(node)


def test_mutation_nested_retry_loop_is_red() -> None:
    """Вложенный `while attempt<N: ... sleep(2**attempt)` — RED."""
    node = _synthetic_ast(_NESTED_RETRY_LOOP)
    assert _nested_loop_with_sleep(node) is not None
