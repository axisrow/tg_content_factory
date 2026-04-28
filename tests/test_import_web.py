from __future__ import annotations

import pytest

from tests.helpers import (
    FakeCliTelethonClient,
    build_web_app,
    make_auth_client,
    make_channel_entity,
    make_test_config,
)


def _resolved_channel_entity(identifier: object):
    if not isinstance(identifier, str):
        return make_channel_entity(abs(hash(str(identifier))) % 10**10)
    return make_channel_entity(identifier)


@pytest.fixture
async def client(tmp_path, real_pool_harness_factory):
    config = make_test_config(tmp_path)
    harness = real_pool_harness_factory()
    harness.queue_cli_client(
        phone="+7000",
        client=FakeCliTelethonClient(entity_resolver=_resolved_channel_entity),
    )
    await harness.connect_account("+7000", session_string="session", is_primary=True)
    app, db = await build_web_app(config, harness)

    async with make_auth_client(app) as c:
        yield c

    await db.close()


@pytest.mark.anyio
async def test_import_page_get(client):
    resp = await client.get("/channels/import")
    assert resp.status_code == 200
    assert "Импорт" in resp.text
    assert "Импортировать" in resp.text


@pytest.mark.anyio
async def test_import_textarea(client):
    resp = await client.post(
        "/channels/import",
        data={"text_input": "@channel1\n@channel2"},
    )
    assert resp.status_code == 200
    assert "Добавлен" in resp.text
    assert "channel1" in resp.text
    assert "channel2" in resp.text


@pytest.mark.anyio
async def test_import_empty_input(client):
    resp = await client.post(
        "/channels/import",
        data={"text_input": ""},
    )
    assert resp.status_code == 200
    # No errors, results block shows zeros
    assert "Всего" in resp.text


@pytest.mark.anyio
async def test_import_skips_duplicates(client):
    # First import
    await client.post(
        "/channels/import",
        data={"text_input": "@dupchan"},
    )
    # Second import of same channel
    resp = await client.post(
        "/channels/import",
        data={"text_input": "@dupchan"},
    )
    assert resp.status_code == 200
    assert "Пропущен" in resp.text


@pytest.mark.anyio
async def test_import_button_on_channels_page(client):
    resp = await client.get("/channels/")
    assert resp.status_code == 200
    assert "/channels/import" in resp.text
    assert "Импорт" in resp.text


@pytest.fixture
async def client_no_accounts(tmp_path, real_pool_harness_factory):
    """Client fixture with no connected Telegram accounts."""
    config = make_test_config(tmp_path, db_name="test_no_acc.db")
    harness = real_pool_harness_factory()
    app, db = await build_web_app(config, harness)

    async with make_auth_client(app) as c:
        yield c

    await db.close()


@pytest.mark.anyio
async def test_import_no_client_still_enqueues(client_no_accounts):
    """Under queued model, web just enqueues a channels.import_batch command.

    Account availability is checked by the worker; web path does not validate.
    """
    resp = await client_no_accounts.post(
        "/channels/import",
        data={"text_input": "@chan1\n@chan2\n@chan3"},
    )
    assert resp.status_code == 200
    # All 3 identifiers should appear as queued
    assert "chan1" in resp.text
    assert "chan2" in resp.text
    assert "chan3" in resp.text


@pytest.mark.anyio
async def test_import_file_upload(client):
    file_content = b"@filech1\n@filech2\n@filech3"
    resp = await client.post(
        "/channels/import",
        data={"text_input": ""},
        files={"file": ("channels.txt", file_content, "text/plain")},
    )
    assert resp.status_code == 200
    assert "filech1" in resp.text
    assert "filech2" in resp.text
    assert "filech3" in resp.text


@pytest.fixture
async def client_scam(tmp_path, real_pool_harness_factory):
    """Client fixture where resolve_channel returns a scam channel."""
    config = make_test_config(tmp_path, db_name="test_scam.db")
    harness = real_pool_harness_factory()
    app, db = await build_web_app(config, harness)

    async def _no_users(self):
        return []

    async def _resolve_scam(self, identifier):
        return {
            "channel_id": -1008888888,
            "title": "Scam Import Channel",
            "username": "scamimport",
            "channel_type": "scam",
            "deactivate": True,
        }

    async def _get_dialogs(self):
        return []

    app.state.pool.get_users_info = _no_users.__get__(app.state.pool, type(app.state.pool))
    app.state.pool.resolve_channel = _resolve_scam.__get__(app.state.pool, type(app.state.pool))
    app.state.pool.get_dialogs = _get_dialogs.__get__(app.state.pool, type(app.state.pool))

    async with make_auth_client(app) as c:
        yield c, db

    await db.close()


# removed: replaced by queued-command model — web only enqueues channels.import_batch;
# scam detection / is_active handling is now the worker's responsibility and exercised
# in telegram_command_dispatcher tests.
