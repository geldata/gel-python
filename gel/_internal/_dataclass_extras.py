from typing import (
    Any,
    TypeVar,
)

import dataclasses
import typing

from . import _typing_eval
from . import _typing_inspect
from . import _utils

T = TypeVar("T")


def coerce_to_dataclass(cls: type[T], obj: Any) -> T:
    """Reconstruct a dataclass from a dataclass-like object including
    all nested dataclass-like instances."""
    if not dataclasses.is_dataclass(cls):
        raise TypeError(f"{cls!r} is not a dataclass")
    if not dataclasses.is_dataclass(obj) or isinstance(obj, type):
        raise TypeError(f"{obj!r} is not a dataclass instance")

    new_kwargs = {}
    for field in dataclasses.fields(cls):
        field_type = _typing_eval.resolve_type(
            field.type, owner=_utils.module_of(cls)
        )
        if _typing_inspect.is_optional_type(field_type):
            value = getattr(obj, field.name, None)
        else:
            value = getattr(obj, field.name)

        if value is not None:
            if dataclasses.is_dataclass(field_type):
                assert isinstance(field_type, type)
                value = coerce_to_dataclass(field_type, value)
            elif _typing_inspect.is_generic_alias(field_type):
                origin = typing.get_origin(field_type)

                if origin is not None and origin in {list, tuple, set}:
                    element_type = typing.get_args(field_type)[0]
                    new_values = []
                    for item in value:
                        if dataclasses.is_dataclass(item):
                            new_item = coerce_to_dataclass(element_type, item)
                        else:
                            new_item = item
                        new_values.append(new_item)

                    value = origin(new_values)
                elif origin is dict:
                    args = typing.get_args(field_type)
                    element_type = args[1]
                    value = {}
                    for k, v in value.items():
                        if dataclasses.is_dataclass(v):
                            new_item = coerce_to_dataclass(element_type, v)
                        else:
                            new_item = v

                        value[k] = new_item

        new_kwargs[field.name] = value

    return cls(**new_kwargs)  # type: ignore [return-value]
