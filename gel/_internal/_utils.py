# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

"""Miscellaneous utilities."""

from typing import Any, final

import sys


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
