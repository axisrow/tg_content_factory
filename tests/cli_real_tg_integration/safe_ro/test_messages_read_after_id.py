import json

import pytest

pytestmark = pytest.mark.real_tg_safe


@pytest.mark.timeout(240)
def test_messages_read_after_id_json_forward_cursor(run_cli, assert_cli_ok, live_channel):
    """`--after-id` forward cursor over live JSON output (#1872).

    Read-only. The envelope must carry the cursor pair; a page bounded by a
    future id (2**40) is a clean empty read, never an error.
    """
    pk, _channel_id = live_channel
    first = run_cli(
        "messages", "read", pk, "--live", "--limit", "3",
        "--format", "json", timeout=180,
    )
    assert_cli_ok(first)
    envelope = json.loads(first.stdout)
    assert set(envelope) == {"messages", "newest_id", "has_more"}
    newest = envelope["newest_id"]
    assert newest is None or isinstance(newest, int)

    follow = run_cli(
        "messages", "read", pk, "--live", "--after-id", str(newest or 0),
        "--format", "json", timeout=180,
    )
    assert_cli_ok(follow)
    follow_envelope = json.loads(follow.stdout)
    ids = [m["id"] for m in follow_envelope["messages"]]
    assert ids == sorted(ids)
    assert all(m["id"] > (newest or 0) for m in follow_envelope["messages"])
    assert follow_envelope["has_more"] in (True, False)

    empty = run_cli(
        "messages", "read", pk, "--live", "--after-id", str(2**40),
        "--format", "json", timeout=180,
    )
    assert_cli_ok(empty)
    assert json.loads(empty.stdout) == {"messages": [], "newest_id": None, "has_more": False}
