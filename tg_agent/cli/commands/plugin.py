"""Schema-driven plugin commands; help never initializes application resources."""
from __future__ import annotations

import json
import re
from contextlib import AsyncExitStack, asynccontextmanager

import click
import typer
from click.core import ParameterSource

from tg_agent.cli import runtime
from tg_agent.cli.typer_app import apply_startup, run_async
from tg_agent.plugin_api import start_plugins
from tg_agent.services.plugin_tools import PluginToolError, PluginTools

plugin_app = typer.Typer(no_args_is_help=True, help='Installed plugin tools')


@asynccontextmanager
async def plugin_host(config_path, with_pool=False):
    config, db = await runtime.init_db(config_path)
    async with AsyncExitStack() as cleanup:
        cleanup.push_async_callback(db.close)
        pool = None
        if with_pool:
            auth, pool = await runtime.init_pool(config, db)
            if auth is not None:
                cleanup.push_async_callback(auth.cleanup)
            cleanup.push_async_callback(pool.disconnect_all)
        plugins = await start_plugins(db, config, pool)
        cleanup.push_async_callback(plugins.close)
        yield PluginTools(plugins)


def parse_tool_flags(descriptor, flags):
    parameters = []
    names = set()
    source_names = {}
    for index, (name, schema) in enumerate(descriptor.input_schema.get('properties', {}).items()):
        option = name.replace('_', '-')
        if not re.fullmatch(r'[A-Za-z][A-Za-z0-9-]*', option) or option in names or option in ('help', 'with-pool'):
            raise click.UsageError(f'Unsupported or conflicting tool option: {name}')
        names.add(option)
        kind = schema.get('type') if isinstance(schema, dict) else None
        if not isinstance(kind, str):
            kind = None
        internal_name = f'arg_{index}'
        source_names[internal_name] = name
        kwargs = {'default': None, 'help': schema.get('description', '') if isinstance(schema, dict) else ''}
        declarations = [f'--{option}', internal_name]
        if kind == 'boolean':
            if f'no-{option}' in names:
                raise click.UsageError(f'Conflicting tool option: {name}')
            names.add(f'no-{option}')
            declarations = [f'--{option}/--no-{option}', internal_name]
        else:
            scalar_types = {'integer': click.INT, 'number': click.FLOAT, 'string': click.STRING}
            kwargs['type'] = scalar_types.get(kind, click.STRING)
            if kind != 'string' and kind not in ('integer', 'number'):
                def decode(ctx, param, value):
                    if value is None:
                        return None
                    try:
                        return json.loads(value)
                    except ValueError as exc:
                        raise click.BadParameter('Expected a JSON value', param=param) from exc
                kwargs['callback'] = decode
        parameters.append(click.Option(declarations, **kwargs))

    @click.pass_context
    def arguments(ctx, **values):
        return {source_names[name]: value for name, value in values.items()
                if ctx.get_parameter_source(name) == ParameterSource.COMMANDLINE}

    command = click.Command(descriptor.name, params=parameters, callback=arguments)
    return command.main(args=flags, standalone_mode=False)


async def run_impl(config_path, name, flags, with_pool=False):
    async with plugin_host(config_path, with_pool) as service:
        arguments = parse_tool_flags(service.describe(name), flags)
        return await service.invoke(name, arguments)


async def list_impl(config_path):
    async with plugin_host(config_path) as service:
        return service.list_plugins()


@plugin_app.command('run', context_settings={'allow_extra_args': True, 'ignore_unknown_options': True})
def plugin_run(
    ctx: typer.Context, tool: str = typer.Argument(...),
    with_pool: bool = typer.Option(False, '--with-pool'),
):
    """Run TOOL using flags generated from its schema; arrays/objects accept JSON."""
    apply_startup(ctx)
    try:
        result = run_async(run_impl(ctx.obj.config, tool, ctx.args, with_pool))
    except (PluginToolError, click.ClickException) as exc:
        detail = exc.detail if isinstance(exc, PluginToolError) else exc.format_message()
        typer.echo(f"Error: {detail}", err=True)
        raise typer.Exit(exc.exit_code if isinstance(exc, click.ClickException) else 1) from exc
    typer.echo(json.dumps(result, ensure_ascii=False))


@plugin_app.command('list')
def plugin_list(ctx: typer.Context, as_json: bool = typer.Option(False, '--json')):
    """List installed plugins and activation status."""
    apply_startup(ctx)
    records = run_async(list_impl(ctx.obj.config))
    if as_json:
        typer.echo(json.dumps(records, ensure_ascii=False))
    elif not records:
        typer.echo('No plugins installed')
    else:
        for record in records:
            typer.echo(f"{record['id']}\t{record['version'] or '-'}\t{record['status']}\t"
                       f"{', '.join(record['tools']) or '-'}\t{record['reason']}")
