"""Every access-layer gate operation must carry a deliberate budget category.

#1784: the gateway's first boot log showed ``telegram_fetch_me`` falling back
to the permissive ``default`` category with the floodgate "not registered in
_OPERATION_CATEGORIES" warning. These tests pin the fix at the registration
site (``_GATE_OPERATION_CATEGORIES`` in the access layer) and turn the audit
the issue asked for into a permanent guard: a new operation tag without a
deliberate category fails here instead of surfacing as a production log line.
"""

import ast
import logging
import re
from pathlib import Path
from types import SimpleNamespace

import pytest
from telethon import functions, types
from telethon_floodgate import RpcDispatchPolicy, TelegramRateLimitGate
from telethon_floodgate import rate_limit_gate as _fg_rate_limit_gate

from tg_agent.telegram.backends import (
    _GATE_OPERATION_CATEGORIES,
    TelegramTransportSession,
)

BACKENDS_PATH = (
    Path(__file__).resolve().parents[1] / "tg_agent" / "telegram" / "backends.py"
)

#: White-box on purpose: the floodgate warning names this exact table, so the
#: guard reads it instead of inferring coverage from ``category_for`` output
#: (which returns ``"default"`` both for a deliberate row and for a fallback).
PACKAGE_OPERATION_CATEGORIES = _fg_rate_limit_gate._OPERATION_CATEGORIES

_TAG_PATTERN = re.compile(r"telegram_[a-z_]+")


def _docstring_node_ids(tree: ast.AST) -> set[int]:
    """ids of string constants that are module/class/function docstrings."""
    ids: set[int] = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            body = getattr(node, "body", [])
            if (
                body
                and isinstance(body[0], ast.Expr)
                and isinstance(body[0].value, ast.Constant)
                and isinstance(body[0].value.value, str)
            ):
                ids.add(id(body[0].value))
    return ids


def _access_layer_operation_tags() -> set[str]:
    """Operation-tag string literals in the gateway telegram access layer."""
    tree = ast.parse(BACKENDS_PATH.read_text(encoding="utf-8"))
    docstrings = _docstring_node_ids(tree)
    tags: set[str] = set()
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Constant)
            and isinstance(node.value, str)
            and id(node) not in docstrings
            and _TAG_PATTERN.fullmatch(node.value)
        ):
            tags.add(node.value)
    return tags


def _has_deliberate_category(tag: str) -> bool:
    if tag in _GATE_OPERATION_CATEGORIES or tag in PACKAGE_OPERATION_CATEGORIES:
        return True
    # Decorated warm/sweep tags resolve through the floodgate suffix rules
    # (``_category_for_operation``), so they are covered without table rows.
    return tag.endswith("_stream_dialogs") or tag.endswith("_warm_dialog_cache")


async def test_fetch_me_reservation_is_deliberate_and_silent(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The #1784 symptom end-to-end: no unknown-op warning, deliberate pricing.

    Since floodgate 0.1.8 absorbed the row, ``fetch_me`` prices at the
    ``resolve`` pass-through family (``try_acquire`` short-circuits it — the
    family owns no bucket). The guard inverts accordingly: the call must not
    warn and must not silently consume a ``default`` slot — a ``default``
    usage here would mean the pricing degraded back to the fallback.
    """
    gate = TelegramRateLimitGate()
    phone = "+15551230000"

    async def get_me() -> str:
        return "sentinel-me"

    client = SimpleNamespace(get_me=get_me)
    pool = SimpleNamespace(_rate_limit_gate=gate)
    session = TelegramTransportSession(
        client, disconnect_on_close=False, phone=phone, pool=pool
    )

    with caplog.at_level(logging.WARNING):
        result = await session.fetch_me()

    assert result == "sentinel-me"
    warnings = [
        record
        for record in caplog.records
        if "not registered in _OPERATION_CATEGORIES" in record.getMessage()
    ]
    assert not warnings
    assert gate.snapshot(phone)["categories"]["default"]["used"] == 0


def test_every_access_layer_operation_tag_has_deliberate_category() -> None:
    tags = _access_layer_operation_tags()
    assert tags, "scan found no operation tags — did the access layer move?"
    # The issue's subject stays under the guard even if the scan is refactored.
    assert "telegram_fetch_me" in tags
    uncovered = sorted(tag for tag in tags if not _has_deliberate_category(tag))
    assert not uncovered, (
        "operation tags without a deliberate gate category — add a row to "
        "_GATE_OPERATION_CATEGORIES (access layer) or register the tag in "
        "telethon-floodgate's _OPERATION_CATEGORIES: "
        f"{uncovered}"
    )


def test_consumer_rows_agree_with_package_table() -> None:
    """No silent drift: when floodgate registers a tag, categories must match.

    The floodgate table is authoritative; the access-layer table only fills
    its gaps until a floodgate release absorbs the row.
    """
    for tag, category in _GATE_OPERATION_CATEGORIES.items():
        package_category = PACKAGE_OPERATION_CATEGORIES.get(tag)
        if package_category is not None:
            assert package_category == category, (
                f"{tag}: access layer prices {category!r}, floodgate table "
                f"prices {package_category!r} — drop the access-layer row"
            )


def test_consumer_rows_use_real_gate_categories() -> None:
    """Each row's category must be a configured gate bucket.

    ``try_acquire`` never raises on an unknown category — it silently prices
    at ``_limiters["default"]`` — so a typo row must be caught by enumerating
    the real buckets through the public ``snapshot()`` instead.
    """
    gate = TelegramRateLimitGate()
    snapshot_categories = gate.snapshot("+15551230001")["categories"]
    assert isinstance(snapshot_categories, dict)
    for tag, category in _GATE_OPERATION_CATEGORIES.items():
        assert category in snapshot_categories, (
            f"{tag}: {category!r} is not a configured gate bucket — it would "
            "silently price at 'default'"
        )


# --- RPC dispatch boundary: transfers own their buckets (#1985) ----------------


def test_transfer_rpcs_are_priced_outside_the_default_bucket() -> None:
    """#1985: upload and download RPCs were priced in ``default`` — the bucket
    the account's connect path (``users.getUsers``) is priced in too. Its
    sustained tier then refused the sixth small photo of an album, and for
    minutes the account could not connect at all. floodgate 0.1.12 gives
    transfers buckets of their own (#1928's forum-topics row ships with it).
    """
    assert PACKAGE_OPERATION_CATEGORIES["rpc_upload"] == "upload"
    assert PACKAGE_OPERATION_CATEGORIES["rpc_download"] == "download"
    assert PACKAGE_OPERATION_CATEGORIES["rpc:GetForumTopicsRequest"] == "history"
    gate = TelegramRateLimitGate()
    configured = gate.snapshot("+15551230001")["categories"]
    assert isinstance(configured, dict)
    assert {"upload", "download"} <= set(configured)


def test_upload_media_request_is_classified_at_the_boundary() -> None:
    """``messages.uploadMedia`` closes every media upload. Without a row it took
    the ``rpc:UploadMediaRequest`` fallback: a warn-once line on the first
    album, and a refusal under ``strict_categories``."""
    request = functions.messages.UploadMediaRequest(
        peer=types.InputPeerSelf(),
        media=types.InputMediaUploadedPhoto(
            file=types.InputFile(id=1, parts=1, name="a.png", md5_checksum="abc")
        ),
    )

    operation = RpcDispatchPolicy().operation_for(request)

    assert operation == "rpc_upload"
    assert TelegramRateLimitGate(strict_categories=True).category_for(operation) == "upload"


def test_spent_upload_hour_leaves_the_connect_path_admitted() -> None:
    """The second half of #1985: a spent transfer budget must not lock the
    account out of the bucket its connect path is priced in."""
    clock = [1000.0]
    gate = TelegramRateLimitGate(time_func=lambda: clock[0], jitter_func=lambda low, high: 0.0)
    spec = TelegramRateLimitGate.TRANSFER_SPEC
    assert spec.sustained_max_calls is not None
    for _ in range(spec.sustained_max_calls // spec.max_calls):
        for _ in range(spec.max_calls):
            assert gate.try_acquire("+15551230001", "upload") == 0.0
        clock[0] += spec.window_sec

    assert gate.try_acquire("+15551230001", "upload") > 60.0
    assert gate.try_acquire("+15551230001", "default") == 0.0
