"""Interop task types (#960): external dm_reply/chat_answer/fetch_* must exist
as enum values + payload models, and must NOT be claimable by the factory's own
internal workers (they are executed by an external tg_messenger worker)."""

from __future__ import annotations

import pytest

from src.models import (
    EXTERNAL_INTEROP_TASK_TYPES,
    ChatAnswerTaskPayload,
    CollectionTaskType,
    DmReplyTaskPayload,
    FetchDialogsTaskPayload,
    FetchHistoryTaskPayload,
)


def test_external_interop_types_are_defined():
    assert {t.value for t in EXTERNAL_INTEROP_TASK_TYPES} == {
        "dm_reply",
        "chat_answer",
        "fetch_dialogs",
        "fetch_history",
    }


def test_external_types_absent_from_unified_dispatcher_pool():
    # HANDLED_TYPES drives claim_next_due_generic_task; if an external type leaked
    # in, the factory worker would steal a task meant for tg_messenger.
    from src.services.unified_dispatcher import HANDLED_TYPES

    for task_type in EXTERNAL_INTEROP_TASK_TYPES:
        assert task_type.value not in HANDLED_TYPES


def test_channel_collect_pull_is_unaffected():
    # CollectionQueue only pulls CHANNEL_COLLECT, so external types never enter it.
    assert CollectionTaskType.CHANNEL_COLLECT not in EXTERNAL_INTEROP_TASK_TYPES


@pytest.mark.parametrize(
    "model, kwargs, kind",
    [
        (DmReplyTaskPayload, {"peer": "@bob", "text": "hi"}, "dm_reply"),
        (ChatAnswerTaskPayload, {"chat_id": 42, "text": "hi"}, "chat_answer"),
        (FetchDialogsTaskPayload, {}, "fetch_dialogs"),
        (FetchHistoryTaskPayload, {"peer": "@chan"}, "fetch_history"),
    ],
)
def test_payload_models_default_to_v1(model, kwargs, kind):
    payload = model(**kwargs)
    assert payload.v == 1
    assert payload.task_kind == kind


def test_interop_payloads_reject_empty_peer_and_text():
    import pytest
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        DmReplyTaskPayload(peer="", text="hi")
    with pytest.raises(ValidationError):
        DmReplyTaskPayload(peer="@bob", text="")
    with pytest.raises(ValidationError):
        ChatAnswerTaskPayload(chat_id=1, text="")
    with pytest.raises(ValidationError):
        FetchHistoryTaskPayload(peer="")
