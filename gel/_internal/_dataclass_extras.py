from __future__ import annotations

from typing import (
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


T = TypeVar("T", covariant=True)

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


def _coerceable(cls: type[T]) -> _Coerceable[T] | None:
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


def coerce_to_dataclass(
    cls: type[T],
    obj: Any,
    *,
    cast_map: Mapping[type[Any], tuple[type[Any], ...]] | None = None,
    replace: Mapping[str, Any] | None = None,
) -> T:
    """Reconstruct a dataclass from a dataclass-like object including
    all nested dataclass-like instances."""
    target = _coerceable(cls)
    if target is None:
        raise TypeError(
            f"{cls!r} is not a dataclass or a "
            f"discriminated union of dataclasses"
        )

    return _coerce_to_dataclass(
        target, obj, cast_map=cast_map, replace=replace
    )


def _coerce_to_dataclass(
    target: _Coerceable[T],
    obj: Any,
    *,
    cast_map: Mapping[type[Any], tuple[type[Any], ...]] | None = None,
    replace: Mapping[str, Any] | None = None,
) -> T:
    getter: _FieldGetter
    if isinstance(obj, dict):
        getter = operator.itemgetter
    else:
        getter = operator.attrgetter

    if isinstance(target, _TaggedUnion):
        target = target.discriminate(obj, getter)

    new_kwargs: dict[str, Any] = {}
    for field, defined_in in _dataclass_fields(target):
        module = _namespace.module_of(defined_in)
        field_type = _typing_eval.resolve_type(field.type, owner=module)
        value_getter = getter(field.name)
        if _typing_inspect.is_optional_type(field_type):
            try:
                value = value_getter(obj)
            except (AttributeError, KeyError):
                value = None

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

        if value is None:
            new_kwargs[field.name] = value
            continue

        if (ft := _coerceable(field_type)) is not None:
            value = _coerce_to_dataclass(ft, value, cast_map=cast_map)
        elif _typing_inspect.is_generic_alias(field_type):
            origin = typing.get_origin(field_type)

            if origin in {list, tuple, set}:
                element_type = typing.get_args(field_type)[0]
                new_values = []
                for item in value:
                    if (elt := _coerceable(element_type)) is not None:
                        new_item = _coerce_to_dataclass(
                            elt, item, cast_map=cast_map
                        )
                    else:
                        new_item = item
                    new_values.append(new_item)

                value = origin(new_values)
            elif origin in {dict, Mapping, MutableMapping}:
                args = typing.get_args(field_type)
                element_type = args[1]
                new_value = {}
                for k, v in value.items():
                    if (elt := _coerceable(element_type)) is not None:
                        new_item = _coerce_to_dataclass(
                            elt, v, cast_map=cast_map
                        )
                    else:
                        new_item = v
                    new_value[k] = new_item

                value = new_value
        elif (
            isinstance(field_type, type)
            and cast_map is not None
            and (from_types := cast_map.get(field_type))
            and isinstance(value, from_types)
        ) or issubclass(field_type, enum.Enum):
            value = field_type(value)

        new_kwargs[field.name] = value

    if replace is not None:
        new_kwargs.update(replace)

    return target(**new_kwargs)  # type: ignore [return-value]
