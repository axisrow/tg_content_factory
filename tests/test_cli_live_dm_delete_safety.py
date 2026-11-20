"""Offline checks for the live DM deletion test's target and cleanup guards."""

from subprocess import CompletedProcess
from types import SimpleNamespace

import pytest

from tests.cli_real_tg_integration import conftest as live
from tests.cli_real_tg_integration.mutation_safe import test_dialogs_delete_message_second_account as scenario


@pytest.mark.parametrize("case", ["third-profile", "alternate-sender", "wrong-message-body", "no-op-delete"])
def test_live_dm_deletion_uses_only_a_verified_peer_and_message(monkeypatch, case):
    sender, second, third = ("+10000002531", "+10000009509", "+10000002629")
    env = SimpleNamespace(phones=(sender, second, third), primary_phone=sender)
    nonce = "offline-test-marker"
    write_phone = second if case == "alternate-sender" else sender
    target = second if case == "wrong-message-body" else "@third_profile"
    target_id = 102 if target == second else 103
    stored = {}
    message_reads = []
    monkeypatch.delenv(scenario.CLI_REAL_TG_MUTATION_PHONE_ENV, raising=False)
    monkeypatch.setattr(scenario, "make_cli_nonce", lambda: nonce)

    def dm(argv, uid, username=None):
        tail = f"Username: {username}\n" if username else ""
        return CompletedProcess(argv, 0, stdout=f"Title: Test\nType: dm\nID: {uid}\n{tail}", stderr="")

    def capture(_env, *argv, timeout):
        if argv[:2] == ("dialogs", "resolve"):
            ref, phone = argv[2], argv[4]
            if ref == "me":
                uid, username = {
                    sender: (101, None), second: (102, "@second_profile"), third: (103, "@third_profile"),
                }[phone]
                return dm(argv, uid, username)
            if (phone, ref) == (write_phone, target):
                return dm(argv, target_id)
            if ref == "@second_profile":
                return dm(argv, 901, ref)
            missing = (
                '  File "/example/telethon_floodgate/flood_wait.py", line 235\n'
                "ValueError: Cannot find any entity\n"
            )
            return CompletedProcess(argv, 1, stdout="", stderr=missing)
        if argv[:2] == ("messages", "read"):
            message_reads.append(argv[2])
            body = stored.get(argv[2])
            output = "No messages found.\n" if body is None else f"[2026-11-20 01:00:00] #77 Test\n  {body}\n"
            return CompletedProcess(argv, 0, stdout=output, stderr="")
        if argv[:2] == ("dialogs", "delete-message"):
            return run_cli(*argv, timeout=timeout)
        raise AssertionError(f"Unexpected read command {argv[:2]}")

    def run_cli(*argv, timeout):
        ref = argv[5]
        assert (argv[4], ref) == (write_phone, target)
        if argv[:2] == ("dialogs", "send"):
            stored[ref] = "Existing unrelated message" if case == "wrong-message-body" else argv[6]
            return CompletedProcess(argv, 0, stdout=f"Message sent to {ref}. message_id=77\n", stderr="")
        if argv[:2] == ("dialogs", "delete-message"):
            if case != "no-op-delete":
                del stored[ref]
            return CompletedProcess(argv, 0, stdout="Deleted 1 message(s).\n", stderr="")
        raise AssertionError(f"Unexpected write command {argv[:2]}")

    monkeypatch.setattr(scenario, "_capture_cli", capture)
    monkeypatch.setattr(live, "_capture_cli", capture)
    if case == "wrong-message-body":
        with pytest.raises(AssertionError, match="does not contain the test nonce"):
            scenario.test_dialogs_delete_second_account_message(run_cli, live._assert_cli_result_ok, env)
        assert list(stored.values()) == ["Existing unrelated message"]
        assert message_reads == [target]
    elif case == "no-op-delete":
        with pytest.raises(AssertionError, match="is still present after deletion"):
            scenario.test_dialogs_delete_second_account_message(run_cli, live._assert_cli_result_ok, env)
        assert nonce in stored[target]
    else:
        try:
            scenario.test_dialogs_delete_second_account_message(run_cli, live._assert_cli_result_ok, env)
        except pytest.skip.Exception:
            pytest.fail("The verified peer is reachable")
        assert message_reads == [target, target]
        assert stored == {}


def test_recipient_lookup_stops_on_reported_flood_wait(monkeypatch):
    result = CompletedProcess([], 1, stdout="", stderr="Error resolving entity: FLOOD_WAIT_600")
    monkeypatch.setattr(scenario, "_capture_cli", lambda *_args, **_kwargs: result)

    with pytest.raises(pytest.skip.Exception, match="Telegram limited a profile lookup"):
        scenario._resolve_dm(SimpleNamespace(), "+10000002531", "@second_profile")
