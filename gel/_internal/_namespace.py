# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

from __future__ import annotations
from typing import Any

import functools
import keyword
import sys
import types

from gel._internal._typecache import type_cache


def is_dunder(s: str) -> bool:
    return s.startswith("__") and s.endswith("__")


_PYTHON_GLOBAL_DUNDERS = [name for name in globals() if is_dunder(name)]
"""List of dunders in module namespace."""


@functools.cache
def ident(s: str) -> str:
    if keyword.iskeyword(s):
        return f"{s}_"
    elif s.isidentifier():
        return s
    else:
        result = "".join(
            c if c.isidentifier() or c.isdigit() else "_" for c in s
        )
        if result and result[0].isdigit():
            result = f"_{result}"

        return result


@functools.cache
def dunder(s: str) -> str:
    sid = ident(s)
    dundered = f"__{sid}__"
    if dundered in _PYTHON_GLOBAL_DUNDERS:
        dundered = f"__{sid}___"

    return dundered


@type_cache
def get_annotation_origin(cls: type, name: str) -> type:
    """
    Return the class in cls.__mro__ that first declared a type annotation
    for `name`.  Raises NameError if no such annotation exists.
    """
    empty: dict[str, Any] = {}
    for base in cls.__mro__:
        if name in getattr(base, "__annotations__", empty):
            return base

    raise NameError(f"{name!r} is not annotated on any class in {cls} MRO")


def module_of(obj: object) -> types.ModuleType | None:
    if isinstance(obj, types.ModuleType):
        return obj
    else:
        module_name = getattr(obj, "__module__", None)
        if module_name:
            return sys.modules.get(module_name)
        else:
            return None


def module_ns_of(obj: object) -> dict[str, Any]:
    """Return the namespace of the module where *obj* is defined."""
    module = module_of(obj)
    return module.__dict__ if module is not None else {}
