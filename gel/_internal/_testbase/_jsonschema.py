# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

"""
Simple JSON schema renderer for Pydantic model schemas.
Renders JSON schemas as Python-like class declarations for testing purposes.
"""

import json
from typing import Any


class SchemaRenderer:
    def __init__(self) -> None:
        # Will be populated during analysis
        self.inline_types: set[str] = set()

    def render(self, schema: dict[str, Any]) -> str:
        """Render a JSON schema as Python-like class declarations."""
        if not isinstance(schema, dict):
            raise ValueError("Schema must be a dictionary")

        defs = schema.get("$defs", {})

        # Analyze which types should be inlined
        self._analyze_inline_types(defs)

        output_lines: list[str] = []

        # Handle root object if it has properties
        if "properties" in schema:
            root_class = self._render_object_type(
                schema, schema.get("title", "Root"), defs
            )
            output_lines.extend([root_class, ""])

        # Process $defs in sorted order
        if defs:
            sorted_defs = sorted(defs.keys())

            for type_name in sorted_defs:
                if type_name in self.inline_types:
                    continue  # Skip inline types

                type_def = defs[type_name]
                rendered = self._render_type_def(type_name, type_def, defs)
                if rendered:
                    output_lines.extend([rendered, ""])

        return "\n".join(output_lines).rstrip()

    def _analyze_inline_types(self, defs: dict[str, Any]) -> None:
        """Analyze which types should be inlined based on simple heuristics."""
        self.inline_types = set()

        for type_name, type_def in defs.items():
            # Inline if it's a simple type that doesn't warrant its own
            # definition
            if self._should_inline_type(type_def):
                self.inline_types.add(type_name)

    def _should_inline_type(self, type_def: dict[str, Any]) -> bool:
        """Simple heuristics to determine if a type should be inlined."""

        # 1. Simple union types (anyOf with few options)
        if "anyOf" in type_def:
            any_of = type_def["anyOf"]
            if len(any_of) <= 3:  # Keep unions simple
                return True

        # 2. Simple array types
        if type_def.get("type") == "array":
            return True

        # 3. Simple primitive types with just metadata (default, format, etc.)
        if type_def.get("type") in {"string", "integer", "number", "boolean"}:
            return True

        # 4. Types with only a default value (like ComputedProperty)
        if (
            "default" in type_def and len(type_def) <= 2
        ):  # Just default + maybe one other field
            return True

        # 5. Don't inline object types (they should be classes)
        if type_def.get("type") == "object":
            return False

        return False

    def _render_type_def(
        self, name: str, type_def: dict[str, Any], defs: dict[str, Any]
    ) -> str:
        """Render a single type definition."""
        if type_def.get("type") == "object":
            return self._render_object_type(type_def, name, defs)
        elif type_def.get("type") == "array":
            return self._render_array_type(type_def, name, defs)
        elif "anyOf" in type_def:
            return self._render_union_type(type_def, name, defs)
        else:
            return f"class {name}:\n    pass"

    def _render_object_type(
        self,
        obj_def: dict[str, Any],
        class_name: str,
        defs: dict[str, Any] | None = None,
    ) -> str:
        """Render an object type as a class."""
        lines = [f"class {class_name}:"]

        properties = obj_def.get("properties", {})
        required = set(obj_def.get("required", []))

        if not properties:
            lines.append("    pass")
            return "\n".join(lines)

        # Sort properties for stability
        sorted_props = sorted(properties.keys())

        for prop_name in sorted_props:
            prop_def = properties[prop_name]
            is_required = prop_name in required
            is_readonly = prop_def.get("readOnly", False)

            type_annotation = self._get_type_annotation(prop_def, defs or {})

            # Build the field definition
            has_default = "default" in prop_def
            default_val = prop_def.get("default")

            # Handle different field annotation cases
            if not is_required and has_default and default_val is None:
                # Case: optional with default=None -> " = None"
                field_annotation = " = None"
            elif not is_required and has_default:
                # Case: optional with non-None default
                field_annotation = f" = {default_val!r}"
            elif not is_required:
                # Case: optional without default -> " = ..."
                field_annotation = " = ..."
            elif has_default:
                # Case: required with default (unusual but possible)
                field_annotation = f" = {default_val!r}"
            else:
                # Case: simple required field
                field_annotation = ""

            # Check for discriminator info and build comments
            comments = []
            if is_readonly:
                comments.append("readonly")

            # Check for discriminator in array items or direct property
            discriminator_info = self._extract_discriminator_info(prop_def)
            if discriminator_info:
                comments.append(f"discriminator: {discriminator_info}")

            comment_str = f"  # {', '.join(comments)}" if comments else ""

            lines.append(
                f"    {prop_name}: {type_annotation}{field_annotation}"
                f"{comment_str}"
            )

        return "\n".join(lines)

    def _extract_discriminator_info(self, prop_def: dict[str, Any]) -> str:
        """Extract discriminator property name if present."""
        # Check direct discriminator
        if "discriminator" in prop_def:
            res = prop_def["discriminator"].get("propertyName", "")
            assert isinstance(res, str)
            return res

        # Check discriminator in array items
        if prop_def.get("type") == "array":
            items = prop_def.get("items", {})
            assert isinstance(items, dict)
            if "discriminator" in items:
                res = items["discriminator"].get("propertyName", "")
                assert isinstance(res, str)
                return res

        return ""

    def _render_array_type(
        self, array_def: dict[str, Any], type_name: str, defs: dict[str, Any]
    ) -> str:
        """Render an array type."""
        items = array_def.get("items", {})
        item_type = self._get_type_annotation(items, defs)
        return f"{type_name} = list[{item_type}]"

    def _render_union_type(
        self, union_def: dict[str, Any], type_name: str, defs: dict[str, Any]
    ) -> str:
        """Render a union type (anyOf)."""
        any_of = union_def.get("anyOf", [])
        union_types = [
            self._get_type_annotation(option, defs) for option in any_of
        ]
        union_str = " | ".join(sorted(set(union_types)))
        return f"{type_name} = {union_str}"

    def _get_type_annotation(
        self, type_def: dict[str, Any], defs: dict[str, Any]
    ) -> str:
        """Get the type annotation for a type definition."""
        if "$ref" in type_def:
            ref = type_def["$ref"]
            assert isinstance(ref, str)
            if ref.startswith("#/$defs/"):
                ref_name = ref[8:]  # Remove '#/$defs/'
                return self._resolve_ref_type(ref_name, defs)
            return ref

        if "oneOf" in type_def:
            one_of = type_def["oneOf"]
            union_types = [
                self._get_type_annotation(option, defs) for option in one_of
            ]
            return " | ".join(sorted(set(union_types)))

        if "anyOf" in type_def:
            any_of = type_def["anyOf"]
            union_types = []
            for option in any_of:
                union_types.append(self._get_type_annotation(option, defs))
            return " | ".join(sorted(set(union_types)))

        type_name = type_def.get("type")
        if type_name == "string":
            if "const" in type_def:
                return f"Literal[{type_def['const']!r}]"
            elif type_def.get("format") == "uuid":
                return "UUID"
            return "str"
        elif type_name == "integer":
            return "int"
        elif type_name == "number":
            return "float"
        elif type_name == "boolean":
            return "bool"
        elif type_name == "null":
            return "None"
        elif type_name == "array":
            items = type_def.get("items", {})
            item_type = self._get_type_annotation(items, defs)
            return f"list[{item_type}]"
        elif type_name == "object":
            return "dict"
        elif type_name is None and "default" in type_def:
            # Handle types that only specify default value
            # (like ComputedProperty)
            return "Any"

        return "Any"

    def _resolve_ref_type(self, ref_name: str, defs: dict[str, Any]) -> str:
        """Resolve a reference type, inlining simple types."""
        if ref_name in self.inline_types:
            # Inline the type definition
            ref_def = defs.get(ref_name, {})
            if ref_def:
                return self._get_type_annotation(ref_def, defs)

        return ref_name


def render_schema(schema_dict: dict[str, Any]) -> str:
    """Render a JSON schema dictionary as Python-like class declarations."""
    renderer = SchemaRenderer()
    return renderer.render(schema_dict)


def render_schema_from_json(json_str: str) -> str:
    """Render a JSON schema string as Python-like class declarations."""
    schema_dict = json.loads(json_str)
    return render_schema(schema_dict)


def render_schema_from_file(file_path: str) -> str:
    """Render a JSON schema file as Python-like class declarations."""
    with open(file_path, encoding="utf8") as f:
        schema_dict = json.load(f)
    return render_schema(schema_dict)
