# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

"""Miscellaneous utilities."""

from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Any,
    final,
    ParamSpec,
    TypeVar,
)


if TYPE_CHECKING:
    from collections.abc import Callable


@final
class UnspecifiedType:
    """A type used as a sentinel for unspecified values."""


Unspecified = UnspecifiedType()


def type_repr(t: Any) -> str:
    if isinstance(t, type):
        if t.__module__ == "builtins":
            return t.__qualname__
        else:
            return f"{t.__module__}.{t.__qualname__}"
    else:
        return repr(t)


P = ParamSpec("P")
R = TypeVar("R")


def inherit_signature(
    func: Callable[P, R],
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Decorator to make the wrapper have the same signature as the wrapped."""

    def decorator(wrapper: Callable[P, R]) -> Callable[P, R]:
        return wrapper

    return decorator
