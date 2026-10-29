"""Stable intent identities shared by project task handlers and remote actions."""

from contextlib import contextmanager
from contextvars import ContextVar
from uuid import uuid4

_intent = ContextVar("gateway_intent", default=None)


@contextmanager
def gateway_intent(key, *, automatic=False):
    token = _intent.set([key, 0, automatic])
    try:
        yield
    finally:
        _intent.reset(token)


def next_intent_key():
    state = _intent.get()
    if state is None:
        return f"explicit-action:{uuid4()}"
    index = state[1]
    state[1] += 1
    return f"{state[0]}:step:{index}"


def automatic_intent():
    state = _intent.get()
    return bool(state and state[2])
