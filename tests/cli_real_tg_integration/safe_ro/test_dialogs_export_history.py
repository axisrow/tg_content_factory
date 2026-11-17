import json

import pytest

pytestmark = pytest.mark.real_tg_safe


@pytest.mark.timeout(240)
def test_dialogs_export_history_bounded(run_cli, assert_cli_ok, live_channel, live_phone, tmp_path):
    """`dialogs export-history` — bounded live export, read-only, resumable file.

    Small --limit keeps the walk to one page; the export file must carry the
    Desktop-style chats map and the summary line must match it.
    """
    import re

    pk, _channel_id = live_channel
    out_file = tmp_path / "history.json"
    result = run_cli(
        "dialogs", "export-history", pk,
        "--phone", live_phone,
        "--limit", "5",
        "--output", str(out_file),
        timeout=180,
    )
    assert_cli_ok(result)
    match = re.search(r"Exported (\d+) chat\(s\), skipped (\d+).*?(\d+) messages", result.stdout)
    assert match, f"unexpected summary line: {result.stdout!r}"
    assert int(match.group(1)) == 1

    payload = json.loads(out_file.read_text(encoding="utf-8"))
    chats = payload["chats"]
    assert len(chats) == 1
    chat = next(iter(chats.values()))
    assert int(match.group(3)) == chat["messages_exported"]
    assert len(chat["messages"]) == chat["messages_exported"] <= 5
    for message in chat["messages"]:
        assert {"id", "dialog_id", "date", "text"} <= set(message)
