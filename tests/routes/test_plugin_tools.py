"""Real plugin discovery → CLI/HTTP calls → persistence and settings UI."""
from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest
from httpx import ASGITransport, AsyncClient
from typer.testing import CliRunner

from tests.test_plugins import Entry
from tg_agent import plugin_api
from tg_agent.cli.typer_app import app as cli_app
from tg_agent.plugin_api import Plugin, start_plugins, text_response, tool

pytestmark = [pytest.mark.anyio, pytest.mark.aiosqlite_serial]


@pytest.fixture
async def digest_host(base_app, tmp_path, monkeypatch):
    app, db, _pool = base_app
    # Actual package metadata as installed by pip, not a mocked entry-point loader.
    root = Path(__file__).parent.parent.parent
    monkeypatch.syspath_prepend(str(root / 'examples' / 'plugin_digest'))
    distribution = tmp_path / 'digest_example-0.1.0.dist-info'
    distribution.mkdir()
    (distribution / 'METADATA').write_text('Name: digest-example\nVersion: 0.1.0\n')
    (distribution / 'entry_points.txt').write_text('[tg_agent.plugins]\nmy_digest = tg_plugin_digest:PLUGIN\n')
    monkeypatch.syspath_prepend(str(tmp_path))
    await start_plugins(db, app.state.config)
    return app, db, tmp_path


async def test_digest_cli_http_listing_settings_and_restart(route_client, digest_host):
    app, db, tmp_path = digest_host
    config_path = tmp_path / 'plugin-config.json'
    config_path.write_text(app.state.config.model_dump_json())
    # The CLI reads YAML, a superset of JSON; it owns its own DB/loop/cleanup.
    def invoke(*args):
        return CliRunner().invoke(cli_app, ['--config', str(config_path), 'plugin', *args])

    cli = await asyncio.to_thread(invoke, 'run', 'digest_today', '--limit', '5')
    assert cli.exit_code == 0, cli.output
    response = await route_client.post('/api/tools/digest_today', json={'limit': 5})
    assert response.status_code == 200, response.text
    assert json.loads(cli.stdout) == response.json() == text_response('Нет собранных сообщений за 24 часа.')
    assert len(await db.execute_fetchall('SELECT ts FROM plugin_my_digest_runs')) == 2
    # Independent subsequent CLI runtime can activate the same plugin and reuse data.
    repeated = await asyncio.to_thread(invoke, 'run', 'digest_today')
    assert repeated.exit_code == 0, repeated.output
    assert len(await db.execute_fetchall('SELECT ts FROM plugin_my_digest_runs')) == 3
    listing = await asyncio.to_thread(invoke, 'list', '--json')
    assert listing.exit_code == 0, listing.output
    record = next(item for item in json.loads(listing.stdout) if item['id'] == 'my_digest')
    assert record['status'] == 'active'
    assert record['tools'] == ['digest_today']
    settings = await route_client.get('/settings')
    assert settings.status_code == 200
    assert 'pane-plugins' in settings.text and 'my_digest' in settings.text and 'digest_today' in settings.text
    assert (await route_client.post('/api/tools/list_channels', json={})).status_code == 404
    for invalid in ({'limit': 0}, {'limit': '5'}, []):
        assert (await route_client.post('/api/tools/digest_today', json=invalid)).status_code == 422
    assert (await route_client.post('/api/tools/digest_today', content='{')).status_code == 422
    assert len(await db.execute_fetchall('SELECT ts FROM plugin_my_digest_runs')) == 3
    denied_cli = await asyncio.to_thread(invoke, 'run', 'digest_today', '--unknown', '5')
    assert denied_cli.exit_code != 0
    await db.set_setting('agent_tool_permissions', json.dumps({'digest_today': False}))
    assert (await route_client.post('/api/tools/digest_today', json={})).status_code == 403
    denied_cli = await asyncio.to_thread(invoke, 'run', 'digest_today')
    assert denied_cli.exit_code != 0 and 'not allowed' in denied_cli.output
    assert len(await db.execute_fetchall('SELECT ts FROM plugin_my_digest_runs')) == 3


async def test_authentication_and_csrf_protect_real_plugin(digest_host):
    app, db, _tmp = digest_host
    async with AsyncClient(transport=ASGITransport(app), base_url='http://test') as client:
        response = await client.post('/api/tools/digest_today', json={})
        assert response.status_code in (401, 303)
        await client.post('/login', data={'password': 'testpass'})
        response = await client.post('/api/tools/digest_today', json={}, headers={'Origin': 'http://evil.test'})
        assert response.status_code == 403
        response = await client.post('/api/tools/digest_today', json={}, headers={'Origin': 'http://test'})
        assert response.status_code == 200
    assert len(await db.execute_fetchall('SELECT ts FROM plugin_my_digest_runs')) == 1


async def test_errors_confirmation_and_runtime_mapping(base_app, route_client, monkeypatch):
    app, db, _pool = base_app

    def register(ctx):
        @tool('write_example', 'Write', {'confirm': bool}, category='write')
        async def write(args):
            return plugin_api.require_confirmation('изменит данные', args) or text_response('done')
        @tool('crash_example', 'Crash', {}, category='read')
        async def crash(args):
            raise ValueError('internal details')
        @tool('live_example', 'Live', {'phone': str}, category='read', phone_bound=True)
        async def live(args):
            pytest.fail('must not invoke without live runtime')
        return [write, crash, live]

    entries = [Entry(Plugin('example', '1', 1, register))]
    monkeypatch.setattr(plugin_api, 'entry_points', lambda **kw: entries)
    await start_plugins(db, app.state.config)
    response = await route_client.post('/api/tools/write_example', json={'confirm': False})
    assert response.status_code == 200 and 'Подтвердите' in response.json()['content'][0]['text']
    assert (await route_client.post('/api/tools/write_example', json={'confirm': True})).json() == text_response('done')
    response = await route_client.post('/api/tools/crash_example', json={})
    assert response.status_code == 500 and 'internal details' not in response.text
    assert (await route_client.post('/api/tools/live_example', json={'phone': '+1234567890'})).status_code == 503
    await db.set_setting('agent_tool_permissions', json.dumps({'write_example': None}))
    assert (await route_client.post('/api/tools/write_example', json={'confirm': True})).status_code == 403


async def test_composed_schema_cli_http_parity_and_rejected_inputs(base_app, route_client, monkeypatch, tmp_path):
    app, db, _pool = base_app
    calls = []
    schema = {'$ref': '#/$defs/Args', '$defs': {
        'Text': {'type': 'string', 'minLength': 2},
        'Flag': {'type': 'boolean'},
        'Args': {'type': 'object', 'allOf': [
            {'properties': {'title': {'$ref': '#/$defs/Text'}, 'enabled': {'$ref': '#/$defs/Flag'},
                            'limit': {'type': 'integer', 'minimum': 1}}},
            {'properties': {'limit': {'maximum': 5}}},
        ], 'required': ['title', 'enabled', 'limit']},
    }}

    def register(ctx):
        @tool('composed_echo', 'Echo', schema, category='read')
        async def echo(args):
            calls.append(args)
            return text_response(json.dumps(args, sort_keys=True))
        return [echo]

    monkeypatch.setattr(plugin_api, 'entry_points', lambda **kw: [Entry(Plugin('example', '1', 1, register))])
    await start_plugins(db, app.state.config)
    config_path = tmp_path / 'config.json'
    config_path.write_text(app.state.config.model_dump_json())

    def invoke(*flags):
        return CliRunner().invoke(cli_app, ['--config', str(config_path), 'plugin', 'run', 'composed_echo', *flags])

    expected = {'title': 'hello', 'enabled': False, 'limit': 5}
    cli = await asyncio.to_thread(invoke, '--title', 'hello', '--no-enabled', '--limit', '5')
    response = await route_client.post('/api/tools/composed_echo', json=expected)
    assert cli.exit_code == 0, cli.output
    assert response.status_code == 200, response.text
    assert json.loads(cli.stdout) == response.json() == text_response(json.dumps(expected, sort_keys=True))
    assert calls == [expected, expected]
    for title, limit in [('x', 5), ('hello', 0), ('hello', 6)]:
        rejected = await asyncio.to_thread(invoke, '--title', title, '--enabled', '--limit', str(limit))
        assert rejected.exit_code != 0 and not rejected.stdout
        response = await route_client.post('/api/tools/composed_echo', json={
            'title': title, 'enabled': True, 'limit': limit,
        })
        assert response.status_code == 422
    assert len(calls) == 2


@pytest.mark.parametrize('reference', ['#/$defs/Missing', 'https://example.invalid/schema'])
async def test_unresolvable_schema_http_and_cli_errors_before_handler(
    base_app, route_client, monkeypatch, tmp_path, reference,
):
    app, db, _pool = base_app
    calls = []

    def register(ctx):
        @tool('unresolvable', 'Bad schema', {'properties': {'value': {'$ref': reference}}}, category='read')
        async def echo(args):
            calls.append(args)
            return text_response('unexpected')
        return [echo]

    monkeypatch.setattr(plugin_api, 'entry_points', lambda **kw: [Entry(Plugin('example', '1', 1, register))])
    await start_plugins(db, app.state.config)
    response = await route_client.post('/api/tools/unresolvable', json={})
    assert response.status_code == 422 and 'Unresolvable plugin schema reference' in response.text
    config_path = tmp_path / 'config.json'
    config_path.write_text(app.state.config.model_dump_json())
    result = await asyncio.to_thread(CliRunner().invoke, cli_app, [
        '--config', str(config_path), 'plugin', 'run', 'unresolvable',
    ])
    assert result.exit_code != 0 and not result.stdout
    assert 'Unresolvable plugin schema reference' in result.stderr
    assert calls == []
