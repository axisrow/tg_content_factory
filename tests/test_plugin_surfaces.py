"""Shared schema/ACL execution and schema-generated command options."""
from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import click
import pytest
from typer.testing import CliRunner

from tests.test_plugins import Entry
from tg_agent import plugin_api
from tg_agent.cli.commands import plugin as cli
from tg_agent.config import AppConfig
from tg_agent.plugin_api import Plugin, start_plugins, text_response, tool
from tg_agent.services.plugin_tools import PluginToolError, PluginTools, normalize_schema

pytestmark = pytest.mark.anyio


@pytest.fixture
async def host(monkeypatch):
    entries = []
    monkeypatch.setattr(plugin_api, 'entry_points', lambda **kw: entries)
    db = SimpleNamespace(get_setting=AsyncMock(return_value=None), get_account_summaries=AsyncMock(return_value=[]))
    yield db, entries
    if getattr(db, 'plugins', None):
        await db.plugins.close()


async def service_for(host, register, pool=None):
    db, entries = host
    entries.append(Entry(Plugin('example', '1', 1, register)))
    return PluginTools(await start_plugins(db, AppConfig(), pool))


def test_schema_formats_and_no_mutation():
    assert normalize_schema({'count': int, 'enabled': bool}) == {
        'type': 'object', 'properties': {'count': {'type': 'integer'}, 'enabled': {'type': 'boolean'}},
        'required': ['count', 'enabled'],
    }
    schema = {'type': 'object', 'properties': {'limit': {'type': 'integer', 'default': 20}}}
    normalized = normalize_schema(schema)
    normalized['properties']['limit']['default'] = 99
    assert schema['properties']['limit']['default'] == 20


async def test_flags_and_payload_defaults_confirm_and_nested_values(host):
    calls = []
    schema = {'type': 'object', 'properties': {
        'limit': {'type': 'integer', 'minimum': 1, 'default': 20},
        'enabled': {'type': 'boolean'}, 'nested': {'type': 'object'}, 'items': {'type': 'array'},
        'confirm': {'type': 'boolean'},
    }, 'additionalProperties': False}

    def register(ctx):
        @tool('example_run', 'Example', schema, category='write')
        async def run(args):
            calls.append(args)
            gate = plugin_api.require_confirmation('изменит данные', args)
            return gate or text_response(json.dumps(args))
        return [run]

    service = await service_for(host, register)
    descriptor = service.describe('example_run')
    assert cli.parse_tool_flags(descriptor, []) == {}
    flags = ['--limit', '5', '--no-enabled', '--nested', '{"x":1}', '--items', '[1,2]', '--confirm']
    arguments = cli.parse_tool_flags(descriptor, flags)
    assert arguments == {'limit': 5, 'enabled': False, 'nested': {'x': 1}, 'items': [1, 2], 'confirm': True}
    assert (await service.invoke('example_run', arguments))['content'][0]['text'] == json.dumps(arguments)
    assert 'Подтвердите' in (await service.invoke('example_run', {}))['content'][0]['text']
    assert calls[-1] == {}
    for bad in ({'limit': 0}, {'limit': '5'}, {'unknown': 1}, []):
        with pytest.raises(PluginToolError) as error:
            await service.invoke('example_run', bad)
        assert error.value.status == 422
    assert len(calls) == 2
    with pytest.raises(click.ClickException):
        cli.parse_tool_flags(descriptor, ['--unknown', '5'])
    with pytest.raises(click.ClickException):
        cli.parse_tool_flags(descriptor, ['--nested', 'invalid'])


@pytest.mark.parametrize('value', [False, None])
async def test_denied_and_requestable_do_not_invoke(host, value):
    called = []

    def register(ctx):
        @tool('example_run', 'Example', {}, category='read')
        async def run(args):
            called.append(True)
            return text_response('called')
        return [run]

    service = await service_for(host, register)
    host[0].get_setting.return_value = json.dumps({'example_run': value})
    with pytest.raises(PluginToolError) as error:
        await service.invoke('example_run', {})
    assert error.value.status == 403
    assert called == []


async def test_phone_acl_runtime_and_normalization(host):
    def register(ctx):
        @tool('phone_tool', 'Phone', {'phone': str}, category='read', phone_bound=True)
        async def run(args):
            return text_response(args['phone'])
        return [run]

    service = await service_for(host, register, pool=SimpleNamespace())
    host[0].get_setting.return_value = json.dumps({'+111': {'phone_tool': True}, '+222': {'phone_tool': False}})
    assert await service.invoke('phone_tool', {'phone': '111'}) == text_response('+111')
    with pytest.raises(PluginToolError) as error:
        await service.invoke('phone_tool', {'phone': '222'})
    assert error.value.status == 403
    service.runtime.client_pool = None
    with pytest.raises(PluginToolError) as error:
        await service.invoke('phone_tool', {'phone': '111'})
    assert error.value.status == 503


async def test_inactive_and_failures_and_cancellation(host):
    entered = asyncio.Event()

    def register(ctx):
        @tool('broken', 'Broken', {}, category='read')
        async def broken(args):
            raise ValueError('do not disclose this')
        @tool('waiting', 'Waiting', {}, category='read')
        async def waiting(args):
            entered.set()
            await asyncio.Event().wait()
        return [broken, waiting]

    service = await service_for(host, register)
    for name, status in [('list_channels', 404), ('broken', 500)]:
        with pytest.raises(PluginToolError) as error:
            await service.invoke(name, {})
        assert error.value.status == status
        assert 'do not disclose' not in error.value.detail
    task = asyncio.create_task(service.invoke('waiting', {}))
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await service.runtime.close()
    with pytest.raises(PluginToolError) as error:
        await service.invoke('broken', {})
    assert error.value.status == 404


async def test_listing_all_activation_states_without_importing_disabled(host):
    db, entries = host
    disabled = Entry(Plugin('disabled', '1', 1, lambda ctx: []))
    entries.extend([disabled, Entry(Plugin('future', '2', 99, lambda ctx: [])),
                    Entry(RuntimeError('secret'), name='broken'), Entry(Plugin('ready', '3', 1, lambda ctx: []))])
    runtime = await start_plugins(db, AppConfig(plugins_disabled=['disabled']))
    records = {item['id']: item for item in PluginTools(runtime).list_plugins()}
    assert {key: item['status'] for key, item in records.items()} == {
        'disabled': 'disabled', 'future': 'incompatible', 'broken': 'failed', 'ready': 'active',
    }
    assert not disabled.loaded
    assert records['disabled']['version'] is None
    assert records['future']['version'] == '2'
    assert 'secret' not in records['broken']['reason']


async def test_cli_uses_generated_flags_and_shared_invocation(host, monkeypatch):
    def register(ctx):
        @tool('echo_plugin', 'Echo', {'limit': int}, category='read')
        async def echo(args):
            return text_response(str(args['limit']))
        return [echo]

    service = await service_for(host, register)

    @asynccontextmanager
    async def fake_host(*args, **kwargs):
        yield service

    monkeypatch.setattr(cli, 'plugin_host', fake_host)
    assert await cli.run_impl('unused', 'echo_plugin', ['--limit', '5']) == text_response('5')
    records = await cli.list_impl('unused')
    assert records[0]['tools'] == ['echo_plugin']


def test_cli_help_has_no_startup(monkeypatch):
    monkeypatch.setattr(cli, 'apply_startup', lambda ctx: pytest.fail('help must not start resources'))
    runner = CliRunner()
    for args in (['--help'], ['run', '--help'], ['run', 'any_tool', '--help'], ['list', '--help']):
        assert runner.invoke(cli.plugin_app, args).exit_code == 0


def test_generic_python_types_and_nullable_cli_values():
    from typing import Literal

    from tg_agent.services.plugin_tools import PluginToolDescriptor

    schema = normalize_schema({'items': list[int], 'mode': Literal['first', 'last'], 'optional': str | None})
    descriptor = PluginToolDescriptor('example', 'example', 'Example', schema, 'read', False)
    arguments = cli.parse_tool_flags(descriptor, ['--items', '[1,2]', '--mode', 'first', '--optional', 'null'])
    assert arguments == {'items': [1, 2], 'mode': 'first', 'optional': None}


def test_nullable_json_schema_and_flag_collisions():
    from tg_agent.services.plugin_tools import PluginToolDescriptor

    schema = {'type': 'object', 'properties': {'value': {'type': ['string', 'null']}}}
    descriptor = PluginToolDescriptor('example', 'example', 'Example', schema, 'read', False)
    assert cli.parse_tool_flags(descriptor, ['--value', 'null']) == {'value': None}
    for properties in (
        {'a_b': {'type': 'string'}, 'a-b': {'type': 'string'}},
        {'enabled': {'type': 'boolean'}, 'no_enabled': {'type': 'string'}},
        {'no_enabled': {'type': 'string'}, 'enabled': {'type': 'boolean'}},
    ):
        descriptor = PluginToolDescriptor('example', 'example', 'Example', {'properties': properties}, 'read', False)
        with pytest.raises(click.UsageError):
            cli.parse_tool_flags(descriptor, [])
