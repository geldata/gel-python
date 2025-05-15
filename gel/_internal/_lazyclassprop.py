# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Generic, TypeVar

from gel._internal import _utils

if TYPE_CHECKING:
    from collections.abc import Callable


T = TypeVar("T")


class LazyClassProperty(Generic[T]):
    def __init__(
        self, meth: Callable[[type[Any]], T] | classmethod[Any, Any, T], /
    ) -> None:
        if isinstance(meth, classmethod):
            self._func = meth.__func__
        else:
            raise TypeError(
                f"{self.__class__.__name__} must be used to "
                f"decorate classmethods"
            )

        self._recursion_guard = False

    def __set_name__(self, owned: type[Any], name: str) -> None:
        self._name = name

    def __get__(self, instance: Any, owner: type[Any] | None = None) -> T:
        if instance is not None or owner is None:
            cls = type(self)
            raise AssertionError(
                f"{_utils.type_repr(cls)}: unexpected lazy class property "
                f"access on containing class instance (not class)"
            )

        fqname = f"{owner.__qualname__}.{self._name}"
        if self._recursion_guard:
            raise NameError(f"recursion while resolving {fqname}")

        self._recursion_guard = True

        try:
            value = self._func(owner)
        except AttributeError as e:
            raise NameError(f"cannot define {fqname} yet") from e
        finally:
            self._recursion_guard = False

        setattr(owner, self._name, value)
        return value
