"""Local JSON Schema references and a conservative projection for CLI flags.

The projection selects a decoder only. The original schema remains authoritative
for validation, including constraints on repeated properties and compositions.
"""
from __future__ import annotations

from dataclasses import dataclass

import jsonschema
from referencing import Registry, Resource
from referencing.exceptions import Unresolvable
from referencing.jsonschema import DRAFT202012, specification_with

_TYPES = frozenset(('string', 'integer', 'number', 'boolean', 'object', 'array', 'null'))


class PluginSchemaError(ValueError):
    """The plugin schema cannot be resolved using its own local resources."""


@dataclass(frozen=True)
class SchemaArgument:
    types: frozenset[str] = _TYPES
    description: str = ''

    @property
    def kind(self) -> str | None:
        if self.types == {'integer', 'number'}:
            return 'number'
        if len(self.types) == 1:
            return next(iter(self.types))
        return None


def _combine(left, right, *, union=False):
    result = {}
    for name in dict.fromkeys((*left, *right)):
        a, b = left.get(name, SchemaArgument()), right.get(name, SchemaArgument())
        types = a.types | b.types if union else a.types & b.types
        result[name] = SchemaArgument(types, a.description or b.description)
    return result


class PluginSchema:
    """Resolve references without retrieval; inspect schemas using public APIs."""

    def __init__(self, schema: dict):
        self.schema = schema
        validator = jsonschema.validators.validator_for(schema)
        dialect = validator.META_SCHEMA.get('$id', validator.META_SCHEMA.get('id', ''))
        self.specification = specification_with(dialect, default=DRAFT202012)
        self.reference_only = self.specification.name in ('draft-03', 'draft-04', 'draft-06', 'draft-07')
        resource = Resource.from_contents(schema, default_specification=self.specification)
        base = resource.id() or 'urn:tg-agent:plugin-schema'
        # Registry's default retrieve callback refuses all external retrieval.
        self.registry = Registry().with_resource(base, resource).crawl()
        self.resolver = self.registry.resolver(base)
        self._check_references(resource, self.resolver, set())

    def _resource(self, schema):
        return Resource.from_contents(schema, default_specification=self.specification)

    @staticmethod
    def _reference(schema, resolver):
        try:
            return resolver.lookup(schema['$ref'])
        except (Unresolvable, ValueError) as exc:
            raise PluginSchemaError(f'Unresolvable plugin schema reference: {schema["$ref"]}') from exc

    def _check_references(self, resource, resolver, seen):
        schema = resource.contents
        if id(schema) in seen:
            return
        seen.add(id(schema))
        if isinstance(schema, dict) and '$ref' in schema:
            resolved = self._reference(schema, resolver)
            self._check_references(self._resource(resolved.contents), resolved.resolver, seen)
        for child in resource.subresources():
            self._check_references(child, resolver.in_subresource(child), seen)

    def _types(self, schema, resolver, seen=frozenset()):
        if not isinstance(schema, dict) or id(schema) in seen:
            return _TYPES if schema is not False else frozenset()
        seen = seen | {id(schema)}
        if self.reference_only and '$ref' in schema:
            resolved = self._reference(schema, resolver)
            return self._types(resolved.contents, resolved.resolver, seen)
        declared = schema.get('type', _TYPES)
        types = frozenset((declared,)) if isinstance(declared, str) else frozenset(declared)
        if 'number' in types:
            types |= {'integer'}
        if '$ref' in schema:
            resolved = self._reference(schema, resolver)
            types &= self._types(resolved.contents, resolved.resolver, seen)
        for keyword in ('allOf', 'anyOf', 'oneOf'):
            branches = schema.get(keyword, [])
            if not branches:
                continue
            inferred = [self._types(child, resolver.in_subresource(self._resource(child)), seen) for child in branches]
            combined = _TYPES if keyword == 'allOf' else frozenset()
            for branch in inferred:
                combined = combined & branch if keyword == 'allOf' else combined | branch
            types &= combined
        return types

    def _description(self, schema, resolver, seen=frozenset()):
        if not isinstance(schema, dict) or id(schema) in seen:
            return ''
        if schema.get('description'):
            return schema['description']
        if '$ref' in schema:
            resolved = self._reference(schema, resolver)
            return self._description(resolved.contents, resolved.resolver, seen | {id(schema)})
        return ''

    def _arguments(self, schema, resolver, seen=frozenset()):
        if not isinstance(schema, dict) or id(schema) in seen:
            return {}
        seen = seen | {id(schema)}
        if self.reference_only and '$ref' in schema:
            resolved = self._reference(schema, resolver)
            return self._arguments(resolved.contents, resolved.resolver, seen)
        result = {}
        for name, child in schema.get('properties', {}).items():
            scoped = resolver.in_subresource(self._resource(child))
            result[name] = SchemaArgument(self._types(child, scoped), self._description(child, scoped))
        if '$ref' in schema:
            resolved = self._reference(schema, resolver)
            result = _combine(result, self._arguments(resolved.contents, resolved.resolver, seen))
        for child in schema.get('allOf', []):
            result = _combine(result, self._arguments(child, resolver.in_subresource(self._resource(child)), seen))
        for keyword in ('anyOf', 'oneOf'):
            branches = schema.get(keyword, [])
            union = None
            for child in branches:
                arguments = self._arguments(child, resolver.in_subresource(self._resource(child)), seen)
                union = arguments if union is None else _combine(union, arguments, union=True)
            if union is not None:
                result = _combine(result, union)
        return result

    def arguments(self) -> dict[str, SchemaArgument]:
        return self._arguments(self.schema, self.resolver)

    def validate(self, arguments):
        validator = jsonschema.validators.validator_for(self.schema)
        try:
            validator(self.schema, registry=self.registry).validate(arguments)
        except (Unresolvable, RecursionError) as exc:
            raise PluginSchemaError('Unresolvable or cyclic plugin input schema') from exc
