# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Generic,
    Protocol,
    TypeVar,
)
from typing_extensions import TypeAliasType

import dataclasses
import enum
import operator
import typing
from collections import defaultdict
from collections.abc import Mapping, MutableMapping

from . import _namespace
from . import _typing_eval
from . import _typing_inspect
from ._typecache import type_cache

if TYPE_CHECKING:
    import types


T = TypeVar("T", covariant=True)
_CastMap = TypeAliasType("_CastMap", Mapping[type[Any], tuple[type[Any], ...]])


def coerce_to_dataclass(
    cls: type[T] | types.UnionType,
    obj: Any,
    *,
    cast_map: _CastMap | None = None,
    replace: Mapping[str, Any] | None = None,
) -> T:
    """Reconstruct a dataclass from a dataclass-like object including
    all nested dataclass-like instances."""
    # Handle generic aliases directly (for recursive calls)
    if _typing_inspect.is_generic_alias(cls):
        return _coerce_generic_alias(cls, obj, cast_map=cast_map)  # type: ignore [no-any-return]

    target = _coerceable(cls)
    if target is None:
        raise TypeError(
            f"{cls!r} is not a dataclass or a "
            f"discriminated union of dataclasses"
        )

    return _coerce_to_dataclass(
        target, obj, cast_map=cast_map, replace=replace
    )


_FieldGetter = TypeAliasType(
    "_FieldGetter", "type[operator.itemgetter[str] | operator.attrgetter[str]]"
)


class _DataclassInstance(Protocol[T]):
    __dataclass_fields__: ClassVar[dict[str, dataclasses.Field[Any]]]


class _TaggedUnion(Generic[T]):
    def __init__(
        self, field: str, mapping: Mapping[object, type[_DataclassInstance[T]]]
    ) -> None:
        self._mapping = mapping
        self._field = field

    def discriminate(
        self, obj: Any, getter: _FieldGetter
    ) -> type[_DataclassInstance[T]]:
        val = getter(self._field)(obj)
        type_ = self._mapping.get(val)
        if type_ is None:
            raise LookupError(
                f"{obj} has unexpected value in {self._field}: {val}; "
                f"cannot discriminate"
            )
        return type_


_Coerceable = TypeAliasType(
    "_Coerceable",
    type[_DataclassInstance[T]] | _TaggedUnion[T],
    type_params=(T,),
)


def _coerceable(cls: type[T] | types.UnionType) -> _Coerceable[T] | None:
    if isinstance(cls, type) and dataclasses.is_dataclass(cls):
        return cls
    elif _typing_inspect.is_union_type(cls):
        discriminators: defaultdict[
            str, defaultdict[object, set[type[_DataclassInstance[T]]]]
        ] = defaultdict(lambda: defaultdict(set))
        members = typing.get_args(cls)
        for arg in members:
            if isinstance(arg, type) and dataclasses.is_dataclass(arg):
                module = _namespace.module_of(arg)
                for field in dataclasses.fields(arg):
                    field_type = _typing_eval.resolve_type(
                        field.type, owner=module
                    )
                    if _typing_inspect.is_literal(field_type):
                        literals = typing.get_args(field_type)
                        for literal in literals:
                            discriminators[field.name][literal].add(arg)

        for field_name, mapping in discriminators.items():
            if len(mapping) == len(members):
                val_to_cls = {k: next(iter(v)) for k, v in mapping.items()}
                return _TaggedUnion(field_name, val_to_cls)

        return None
    else:
        return None


@type_cache
def _dataclass_fields(
    cls: type[_DataclassInstance[Any]],
) -> tuple[tuple[dataclasses.Field[Any], type[Any]], ...]:
    return tuple(
        (field, _namespace.get_annotation_origin(cls, field.name))
        for field in dataclasses.fields(cls)
    )


def _coerce_value(
    target_type: Any,
    obj: Any,
    *,
    cast_map: _CastMap | None = None,
) -> Any:
    """Recursively coerce a value to match the target type."""
    # Handle None values first
    if obj is None:
        return None

    # Try direct dataclass coercion
    if (coerceable_target := _coerceable(target_type)) is not None:
        return _coerce_to_dataclass(coerceable_target, obj, cast_map=cast_map)

    # Handle union types
    if _typing_inspect.is_union_type(target_type):
        return _coerce_union_value(target_type, obj, cast_map=cast_map)

    # Handle generic aliases (containers)
    if _typing_inspect.is_generic_alias(target_type):
        return _coerce_generic_alias(target_type, obj, cast_map=cast_map)

    # Handle enum coercion
    if isinstance(target_type, type) and issubclass(target_type, enum.Enum):
        return target_type(obj)

    # Handle cast_map coercion
    if (
        isinstance(target_type, type)
        and cast_map is not None
        and (from_types := cast_map.get(target_type))
        and isinstance(obj, from_types)
    ):
        return target_type(obj)

    # Return as-is if no coercion needed
    return obj


def _coerce_union_value(
    union_type: Any,
    obj: Any,
    *,
    cast_map: _CastMap | None = None,
) -> Any:
    """Coerce a value to a union type by trying each component."""
    # Handle None values specially - if None is in the union, return None
    if obj is None and type(None) in typing.get_args(union_type):
        return None

    last_error = None
    for component in typing.get_args(union_type):
        # Skip None type - we already handled it above
        if component is type(None):
            continue

        try:
            return _coerce_value(component, obj, cast_map=cast_map)
        except (TypeError, ValueError, KeyError, AttributeError) as e:
            last_error = e

    # All components failed
    if last_error is not None:
        raise last_error

    # If no components were coerceable, return the object as-is
    return obj


def _coerce_generic_alias(
    target_type: Any,
    obj: Any,
    *,
    cast_map: _CastMap | None = None,
) -> Any:
    """Coerce a value to a generic alias type (list, dict, etc.)."""
    origin = typing.get_origin(target_type)

    # Handle sequence types (list, tuple, set)
    if origin in {list, tuple, set}:
        element_type = typing.get_args(target_type)[0]
        coerced_items = [
            _coerce_value(element_type, item, cast_map=cast_map)
            for item in obj
        ]
        return origin(coerced_items)

    # Handle mapping types (dict, Mapping, MutableMapping)
    elif origin in {dict, Mapping, MutableMapping}:
        args = typing.get_args(target_type)
        value_type = args[1]  # Key type is args[0], value type is args[1]
        coerced_dict = {
            k: _coerce_value(value_type, v, cast_map=cast_map)
            for k, v in obj.items()
        }
        return coerced_dict

    # For other generic aliases, return the object as-is
    return obj


def _coerce_to_dataclass(
    target: _Coerceable[T],
    obj: Any,
    *,
    cast_map: _CastMap | None = None,
    replace: Mapping[str, Any] | None = None,
) -> T:
    """Coerce an object to a dataclass instance."""
    # Determine field getter based on object type
    getter: _FieldGetter
    if isinstance(obj, dict):
        getter = operator.itemgetter
    else:
        getter = operator.attrgetter

    # Handle discriminated unions
    if isinstance(target, _TaggedUnion):
        target = target.discriminate(obj, getter)

    # Process each field in the dataclass
    new_kwargs: dict[str, Any] = {}
    for field, defined_in in _dataclass_fields(target):
        module = _namespace.module_of(defined_in)
        field_type = _typing_eval.resolve_type(field.type, owner=module)
        value_getter = getter(field.name)

        # Handle optional fields
        if _typing_inspect.is_optional_type(field_type):
            try:
                value = value_getter(obj)
            except (AttributeError, KeyError):
                value = None

            # Extract non-None types from the optional union
            opt_args = [
                arg
                for arg in typing.get_args(field_type)
                if arg is not type(None)
            ]
            field_type = opt_args[0]
            for opt_arg in opt_args[1:]:
                field_type |= opt_arg
        else:
            value = value_getter(obj)

        # Skip None values
        if value is None:
            new_kwargs[field.name] = value
            continue

        # Coerce the field value
        new_kwargs[field.name] = _coerce_value(
            field_type, value, cast_map=cast_map
        )

    # Apply any field replacements
    if replace is not None:
        new_kwargs.update(replace)

    return target(**new_kwargs)  # type: ignore [return-value]
