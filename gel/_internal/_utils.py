# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

"""Miscellaneous utilities."""

from typing import Any, TypeVar, final, overload

import sys
import types


@final
class UnspecifiedType:
    """A type used as a sentinel for unspecified values."""


Unspecified = UnspecifiedType()


def type_repr(t: type[Any]) -> str:
    if isinstance(t, type):
        if t.__module__ == "builtins":
            return t.__qualname__
        else:
            return f"{t.__module__}.{t.__qualname__}"
    else:
        return repr(t)


def is_dunder(attr: str) -> bool:
    return attr.startswith("__") and attr.endswith("__")


def module_ns_of(obj: object) -> dict[str, Any]:
    """Return the namespace of the module where *obj* is defined."""
    module_name = getattr(obj, "__module__", None)
    if module_name:
        module = sys.modules.get(module_name)
        if module is not None:
            return module.__dict__

    return {}


_T = TypeVar("_T")


@overload
def maybe_get_descriptor(
    cls: type,
    name: str,
    of_type: type[_T],
) -> _T | None: ...


@overload
def maybe_get_descriptor(
    cls: type,
    name: str,
    of_type: None = None,
) -> Any | None: ...


def maybe_get_descriptor(
    cls: type,
    name: str,
    of_type: type | None = None,
) -> Any | None:
    if of_type is None:
        of_type = types.MethodDescriptorType

    for ancestor in cls.__mro__:
        desc = ancestor.__dict__.get(name, Unspecified)
        if desc is not Unspecified and isinstance(desc, of_type):
            return desc

    return None
