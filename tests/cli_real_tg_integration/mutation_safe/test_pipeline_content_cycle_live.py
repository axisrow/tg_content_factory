"""Live sквозной прогон контентного цикла: реальный LLM-провайдер + Saved Messages (#1040).

Тест доказывает весь цикл живьём: generate (реальный провайдер) → MODERATED
очередь → approve → publish в Saved Messages → сообщение реально доставлено.

Тройной гейт:
  - RUN_CLI_REAL_TG_LIVE=1          — живая Telegram сессия
  - RUN_REAL_TELEGRAM_MUTATION_SAFE=1 — разрешение на мутации (marker real_tg_mutation_safe)
  - RUN_REAL_PROVIDER_SMOKE=1       — реальный LLM-провайдер (тратит кредиты)

Без всех трёх гейтов тест скипается — никогда не запускается автоматически.
"""
from __future__ import annotations

import re
import sqlite3
import sys

import pytest

from tests.cli_real_tg_integration._live_readiness import _gate_enabled
from tests.cli_real_tg_integration.conftest import (
    cleanup_verified_messages,
    distinctive_text_fragment,
    fetch_pipeline_run_text,
    force_delete_messages_by_id,
    make_cli_nonce,
    resolve_saved_messages_dialog_id,
)

pytestmark = pytest.mark.real_tg_mutation_safe

_PROVIDER_SMOKE_GATE = "RUN_REAL_PROVIDER_SMOKE"

_RUN_ID_RE = re.compile(r"Created generation run id=(\d+)")
_PUBLISHED_MSG_RE = re.compile(r"published_message_id=(\d+)\s+phone=(\S+)\s+dialog_id=(-?\d+)")


def _skip_if_no_provider_gate() -> None:
    if not _gate_enabled(_PROVIDER_SMOKE_GATE):
        pytest.skip(
            f"live provider spend disabled; set {_PROVIDER_SMOKE_GATE}=1 to run "
            "— opt-in only, tратит provider-кредиты"
        )


def _create_live_cycle_pipeline(
    db_path,
    *,
    phone: str,
    dialog_id: int,
    nonce: str,
) -> int:
    """Создаёт pipeline + target в Saved Messages; возвращает pipeline_id."""
    pipeline_name = f"live-cycle-test-{nonce}"
    with sqlite3.connect(str(db_path)) as conn:
        cur = conn.execute(
            """
            INSERT INTO content_pipelines (
                name, prompt_template, publish_mode, generation_backend,
                is_active, generate_interval_minutes, account_phone
            )
            VALUES (?, ?, 'moderated', 'chain', 0, 60, ?)
            """,
            (
                pipeline_name,
                # Требуем ДЛИННОЕ предложение (>= 60 символов в одну строку), чтобы
                # distinctive_text_fragment() гарантированно нашёл маркер >= 40 симв.
                # и cleanup точно сработал — иначе короткий ответ дал бы None и пост
                # остался бы в Saved Messages (leak). Длина — первая линия защиты;
                # вторая (fail при nonce_marker=None) ниже в finally.
                "Напиши одно позитивное предложение о природе — не короче 60 символов, "
                f"одной строкой, без переносов. Nonce: {nonce}",
                phone,
            ),
        )
        pipeline_id = int(cur.lastrowid or 0)
        conn.execute(
            """
            INSERT INTO pipeline_targets (
                pipeline_id, phone, target_dialog_id, target_title, target_type
            )
            VALUES (?, ?, ?, 'Saved Messages', 'saved')
            """,
            (pipeline_id, phone, int(dialog_id)),
        )
    return pipeline_id


def _delete_live_cycle_pipeline(db_path, *, pipeline_id: int) -> None:
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("DELETE FROM generation_runs WHERE pipeline_id = ?", (int(pipeline_id),))
        conn.execute("DELETE FROM pipeline_targets WHERE pipeline_id = ?", (int(pipeline_id),))
        conn.execute(
            "DELETE FROM content_pipelines WHERE id = ? AND name LIKE 'live-cycle-test-%'",
            (int(pipeline_id),),
        )


# pytest-timeout — это hard deadlock-guard, поэтому считаем ПОЛНЫЙ worst-case всех
# bounded CLI-вызовов на критическом пути (в норме каждый отрабатывает за секунды):
#   resolve Saved Messages 60
#   + run 180 + moderation-list 30 + approve 30 + publish 120            (= 360, тело)
#   + finally: run-show 30 + cleanup (verify 2×60 + delete 60 = 180)     (= 210, cleanup)
#   ≈ 630s суммарно.
# Берём 660 (>= 630), чтобы pytest-timeout не убил тест ВНУТРИ finally и не оставил
# реальный пост неудалённым даже при медленном провайдере/повторных live-reads.
@pytest.mark.timeout(660)
def test_pipeline_content_cycle_live(run_cli, assert_cli_ok, cli_real_cli_env, live_scratch_message_dialog):
    """Сквозной живой прогон: LLM-generate → moderated → approve → publish → Saved Messages.

    Доказывает issue #1040: контентный цикл реально работает с живым провайдером
    и реальной Telegram-доставкой. Зафиксировано как боевой прогон.
    """
    _skip_if_no_provider_gate()

    phone = live_scratch_message_dialog.phone
    saved_dialog_id = resolve_saved_messages_dialog_id(cli_real_cli_env, phone=phone)
    if saved_dialog_id is None:
        pytest.skip("could not resolve Saved Messages dialog id")

    nonce = make_cli_nonce()
    pipeline_id = _create_live_cycle_pipeline(
        cli_real_cli_env.db_path,
        phone=phone,
        dialog_id=saved_dialog_id,
        nonce=nonce,
    )

    published_entries: list[tuple[str, str, str]] = []
    run_id: str | None = None
    leak_msg: str | None = None

    try:
        # === Step 1: pipeline run — реальный LLM-провайдер ===
        # --preview лишь ДОПЕЧАТЫВАЕт черновик в stdout; сам run всё равно
        # создаётся и пишется в generation_runs (см. pipeline.py: generate()
        # вызывается до проверки args.preview), поэтому дальше он реально
        # попадает в очередь модерации и публикуется. Флаг тут — для удобства
        # отладки вывода, он НЕ делает прогон «сухим».
        result = run_cli("pipeline", "run", str(pipeline_id), "--preview", timeout=180)
        assert_cli_ok(result)
        combined = f"{result.stdout}\n{result.stderr}"

        match = _RUN_ID_RE.search(combined)
        assert match, f"pipeline run did not print 'Created generation run id=': {combined!r}"
        run_id = match.group(1)

        # === Step 2: run находится в очереди модерации (MODERATED) ===
        modlist = run_cli("pipeline", "moderation-list", timeout=30)
        assert_cli_ok(modlist)
        assert run_id in modlist.stdout, (
            f"run id={run_id} not found in moderation-list output: {modlist.stdout!r}"
        )

        # === Step 3: approve ===
        approve = run_cli("pipeline", "approve", run_id, timeout=30)
        assert_cli_ok(approve)
        approve_combined = f"{approve.stdout}\n{approve.stderr}"
        assert f"Approved run id={run_id}" in approve_combined, (
            f"approve did not confirm: {approve_combined!r}"
        )

        # === Step 4: publish → реальная TG-доставка ===
        publish = run_cli("pipeline", "publish", run_id, timeout=120)
        assert_cli_ok(publish)
        pub_combined = f"{publish.stdout}\n{publish.stderr}"
        assert f"Published run id={run_id}" in pub_combined, (
            f"publish did not confirm: {pub_combined!r}"
        )

        for m in _PUBLISHED_MSG_RE.finditer(pub_combined):
            published_entries.append((m.group(1), m.group(2), m.group(3)))
        assert published_entries, f"publish output has no published_message_id: {pub_combined!r}"

        # === Step 5: один run, один пост — нет дублей ===
        assert len(published_entries) == 1, (
            f"Expected exactly 1 published entry (no duplicate billing), got {len(published_entries)}: "
            f"{published_entries}"
        )

    finally:
        # Cleanup: удаляем опубликованное сообщение из Saved Messages
        run_text: str | None = None
        if run_id is not None:
            run_text = fetch_pipeline_run_text(cli_real_cli_env, run_id)

        nonce_marker = distinctive_text_fragment(run_text) if run_text else None
        for message_id, published_phone, _ in published_entries:
            if nonce_marker is not None:
                current_leak = cleanup_verified_messages(
                    cli_real_cli_env,
                    phone=published_phone,
                    chat_ref="me",
                    candidates=[int(message_id)],
                    nonce=nonce_marker,
                )
                if current_leak and sys.exc_info()[0] is None:
                    leak_msg = current_leak
            else:
                # Маркер недоступен (короткий run text ИЛИ run-show упал/сменил
                # формат → run_text=None). Текстовая верификация невозможна, но
                # сообщение РЕАЛЬНО опубликовано → молча оставить = leak. Удаляем
                # best-effort по ТОЧНОМУ message_id из нашего же publish-вывода
                # (это id, который мы сами создали, не угаданный — безопасно
                # удалять без nonce-проверки). Если удаление не подтвердилось —
                # фиксируем leak, чтобы не «пройти» молча с реальным постом.
                force_leak = force_delete_messages_by_id(
                    cli_real_cli_env,
                    phone=published_phone,
                    chat_ref="me",
                    message_ids=[int(message_id)],
                )
                if force_leak and sys.exc_info()[0] is None:
                    leak_msg = (
                        f"LEAK: published message_id={message_id} мог остаться в "
                        f"Saved Messages (phone={published_phone}) — маркер недоступен "
                        f"(run_text={run_text!r}) и forced cleanup не подтвердился: "
                        f"{force_leak}"
                    )

        _delete_live_cycle_pipeline(
            cli_real_cli_env.db_path,
            pipeline_id=pipeline_id,
        )

        if leak_msg and sys.exc_info()[0] is None:
            pytest.fail(leak_msg, pytrace=False)
