"""Local-reference and composition behavior at the CLI/service boundary."""
from __future__ import annotations

import copy

import jsonschema
import pytest

from tg_agent.cli.commands.plugin import parse_tool_flags
from tg_agent.services.plugin_schema import PluginSchema, PluginSchemaError
from tg_agent.services.plugin_tools import PluginToolDescriptor


def parse(schema, *flags):
    return parse_tool_flags(PluginToolDescriptor('echo', 'example', 'Echo', schema, 'read', False), list(flags))


def test_reference_chains_property_pointer_and_description_without_mutation():
    schema = {'type': 'object', 'properties': {
        'original': {'type': 'string', 'description': 'A title'},
        'title': {'$ref': '#/$defs/A'},
        'ratio': {'$ref': '#/$defs/Ratio'},
    }, '$defs': {'A': {'$ref': '#/$defs/B'}, 'B': {'$ref': '#/properties/original'},
                'Ratio': {'type': 'number'}}}
    before = copy.deepcopy(schema)
    assert parse(schema, '--title', 'hello', '--ratio', '1.5') == {'title': 'hello', 'ratio': 1.5}
    assert PluginSchema(schema).arguments()['title'].description == 'A title'
    assert schema == before


def test_allof_keeps_all_constraints_and_does_not_supply_defaults():
    schema = {'type': 'object', 'allOf': [
        {'properties': {'limit': {'$ref': '#/$defs/Count'}}},
        {'properties': {'limit': {'maximum': 5}}},
    ], '$defs': {'Count': {'type': 'integer', 'minimum': 2, 'default': 3}}}
    assert parse(schema) == {}
    for value in (2, 5):
        args = parse(schema, '--limit', str(value))
        assert args == {'limit': value}
        PluginSchema(schema).validate(args)
    for value in (1, 6):
        with pytest.raises(jsonschema.ValidationError):
            PluginSchema(schema).validate(parse(schema, '--limit', str(value)))


@pytest.mark.parametrize('composition', ['anyOf', 'oneOf'])
def test_union_types_and_declared_names(composition):
    schema = {composition: [
        {'properties': {'value': {'type': 'string'}, 'same': {'type': 'string'}, 'left': {'type': 'boolean'}}},
        {'properties': {'value': {'type': 'integer'}, 'same': {'type': 'string'}, 'right': {'type': 'array'}}},
    ]}
    assert parse(schema, '--value', '3', '--same', 'hello', '--left', 'false', '--right', '[1]') == {
        'value': 3, 'same': 'hello', 'left': False, 'right': [1],
    }
    assert parse(schema, '--value', '"hello"') == {'value': 'hello'}
    # A branch that does not constrain a property must not force its scalar type.
    assert PluginSchema(schema).arguments()['left'].kind is None


@pytest.mark.parametrize('definition', [
    {'type': ['string', 'null']},
    {'anyOf': [{'type': 'string'}, {'type': 'null'}]},
    {'oneOf': [{'type': 'string'}, {'type': 'null'}]},
])
def test_nullable_reference_requires_json(definition):
    schema = {'properties': {'value': {'$ref': '#/$defs/Optional'}}, '$defs': {'Optional': definition}}
    assert parse(schema, '--value', 'null') == {'value': None}
    assert parse(schema, '--value', '"hello"') == {'value': 'hello'}


def test_recursive_object_is_a_single_json_argument():
    schema = {'type': 'object', 'properties': {'node': {'$ref': '#/$defs/Node'}}, '$defs': {
        'Node': {'type': 'object', 'properties': {
            'value': {'type': 'integer'}, 'child': {'$ref': '#/$defs/Node'}}, 'required': ['value']},
    }}
    args = parse(schema, '--node', '{"value":1,"child":{"value":2}}')
    assert args == {'node': {'value': 1, 'child': {'value': 2}}}
    PluginSchema(schema).validate(args)
    with pytest.raises(jsonschema.ValidationError):
        PluginSchema(schema).validate({'node': {'value': 1, 'child': {'value': 'wrong'}}})


def test_root_reference_and_local_resource_ids_and_anchors():
    schema = {'$id': 'https://example.invalid/root', '$ref': '#/$defs/Args', '$defs': {
        'Args': {'$id': 'args', 'type': 'object', 'properties': {'title': {'$ref': '#text'}},
                 '$defs': {'Text': {'$anchor': 'text', 'type': 'string'}}},
    }}
    args = parse(schema, '--title', 'hello')
    assert args == {'title': 'hello'}
    PluginSchema(schema).validate(args)


@pytest.mark.parametrize('ref', ['#/$defs/missing', '#missing', 'https://example.invalid/remote'])
def test_unresolvable_references_are_schema_errors_even_for_omitted_arguments(ref):
    schema = {'properties': {'value': {'$ref': ref}}}
    with pytest.raises(PluginSchemaError, match='Unresolvable plugin schema reference'):
        PluginSchema(schema)


def test_reference_only_cycle_does_not_loop():
    schema = {'$ref': '#/$defs/Loop', '$defs': {'Loop': {'$ref': '#'}}}
    assert PluginSchema(schema).arguments() == {}
    with pytest.raises(PluginSchemaError, match='cyclic'):
        PluginSchema(schema).validate({})


def test_draft7_reference_siblings_keep_original_dialect_semantics():
    schema = {'$schema': 'http://json-schema.org/draft-07/schema#',
              '$ref': '#/definitions/Args',
              'properties': {'ignored': {'type': 'boolean'}},
              'definitions': {'Args': {'properties': {
                  'title': {'$ref': '#/definitions/Text', 'type': 'integer'}}},
                  'Text': {'type': 'string'}}}
    assert set(PluginSchema(schema).arguments()) == {'title'}
    args = parse(schema, '--title', 'hello')
    assert args == {'title': 'hello'}
    PluginSchema(schema).validate(args)


def test_intersection_of_union_types_can_recover_unambiguous_scalar():
    schema = {'allOf': [
        {'properties': {'value': {'anyOf': [{'type': 'string'}, {'type': 'boolean'}]}}},
        {'properties': {'value': {'type': 'boolean'}}},
    ]}
    assert parse(schema, '--value') == {'value': True}
    PluginSchema(schema).validate({'value': True})
