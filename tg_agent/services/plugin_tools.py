"""Shared plugin-tool descriptors, validation and unattended execution."""
from __future__ import annotations

import copy
import logging
from dataclasses import asdict, dataclass
from typing import get_type_hints, is_typeddict

import jsonschema
from pydantic import TypeAdapter

from tg_agent.agent.runtime_context import AgentToolRuntimeError, detect_runtime_kind
from tg_agent.agent.tools._registry import resolve_phone
from tg_agent.agent.tools.permissions import ToolAccessState, get_tool_access_state
from tg_agent.plugin_api import PluginRuntime

logger = logging.getLogger(__name__)


class PluginToolError(Exception):
    def __init__(self, status: int, detail: str):
        super().__init__(detail)
        self.status = status
        self.detail = detail


def normalize_schema(schema) -> dict:
    """Accept the SDK's Python-type map or an object JSON Schema without mutation."""
    if is_typeddict(schema):
        hints = get_type_hints(schema)
        result = normalize_schema(hints)
        result['required'] = sorted(schema.__required_keys__)
        return result
    if not isinstance(schema, dict):
        raise PluginToolError(422, 'Unsupported plugin input schema')
    schema_keys = ('type', 'properties', '$ref', '$schema', '$defs', 'required', 'allOf', 'anyOf', 'oneOf')
    if any(key in schema and isinstance(schema[key], (str, dict, list, bool)) for key in schema_keys):
        result = copy.deepcopy(schema)
    else:
        properties = {}
        definitions = {}
        for name, kind in schema.items():
            try:
                properties[name] = TypeAdapter(kind).json_schema()
                for key, definition in properties[name].pop('$defs', {}).items():
                    if key in definitions and definitions[key] != definition:
                        raise PluginToolError(422, f'Conflicting schema definition: {key}')
                    definitions[key] = definition
            except Exception as exc:
                raise PluginToolError(422, f'Unsupported type for {name}') from exc
        result = {'type': 'object', 'properties': properties, 'required': list(properties)}
        if definitions:
            result['$defs'] = definitions
    try:
        jsonschema.validators.validator_for(result).check_schema(result)
    except jsonschema.SchemaError as exc:
        raise PluginToolError(422, 'Invalid plugin input schema') from exc
    return result


@dataclass(frozen=True)
class PluginToolDescriptor:
    name: str
    plugin_id: str
    description: str
    input_schema: dict
    category: str
    phone_bound: bool


class PluginTools:
    def __init__(self, runtime: PluginRuntime | None):
        self.runtime = runtime

    def _find(self, name):
        if isinstance(self.runtime, PluginRuntime) and not self.runtime._closed:
            for tool in self.runtime.tools:
                if tool.name == name:
                    return tool
        raise PluginToolError(404, f'Plugin tool not available: {name}')

    def describe(self, name: str) -> PluginToolDescriptor:
        tool = self._find(name)
        return PluginToolDescriptor(
            tool.name, self.runtime.tool_owners[tool.name], tool.description,
            normalize_schema(tool.input_schema), tool.plugin_meta.category.value, tool.plugin_meta.phone_bound,
        )

    def list_plugins(self) -> list[dict]:
        if not isinstance(self.runtime, PluginRuntime):
            return []
        return [asdict(record) for record in self.runtime.records]

    async def invoke(self, name: str, arguments: object) -> dict:
        tool = self._find(name)
        descriptor = self.describe(name)
        if not isinstance(arguments, dict):
            raise PluginToolError(422, 'Tool arguments must be a JSON object')
        try:
            jsonschema.validators.validator_for(descriptor.input_schema)(descriptor.input_schema).validate(arguments)
        except jsonschema.ValidationError as exc:
            raise PluginToolError(422, f'Invalid tool arguments: {exc.message}') from exc
        args = copy.deepcopy(arguments)
        phone = ''
        if descriptor.phone_bound:
            phone, error = await resolve_phone(self.runtime.db, args.get('phone', ''))
            if error:
                raise PluginToolError(503, 'No account available for plugin tool')
        state = await get_tool_access_state(self.runtime.db, name, phone=phone)
        if state != ToolAccessState.ALLOWED:
            raise PluginToolError(403, f'Tool access is not allowed: {name}')
        if descriptor.phone_bound:
            if detect_runtime_kind(self.runtime.client_pool) != 'live':
                raise PluginToolError(503, 'Live Telegram runtime is unavailable')
            args['phone'] = phone
        try:
            # No interactive permission context is installed here. Explicit ACL
            # checks above and the handler's confirmation/runtime guards remain authoritative.
            return await tool.handler(args)
        except AgentToolRuntimeError as exc:
            raise PluginToolError(503, str(exc)) from exc
        except Exception as exc:
            logger.exception('Plugin tool failed: %s', name)
            raise PluginToolError(500, f'Plugin tool failed: {name}') from exc
