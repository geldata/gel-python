# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Generic, TypeVar
from typing_extensions import TypeAliasType


from gel._internal import _utils

if TYPE_CHECKING:
    from collections.abc import Callable


T = TypeVar("T")


_MethodOrClassMethod = TypeAliasType(
    "_MethodOrClassMethod",
    "Callable[[Any], T] | classmethod[type[Any], [], T]",
    type_params=(T,),
)


class LazyHybridProperty(Generic[T]):
    def __init__(self, meth: _MethodOrClassMethod[T], /) -> None:
        if isinstance(meth, classmethod):
            meth = meth.__func__
        self._func: Callable[[Any], T] = meth
        self._recursion_guard = False

    def __set_name__(self, owner: type[Any], name: str) -> None:
        self._name = name

    def __get__(self, instance: Any, owner: type[Any] | None = None) -> T:
        if instance is not None:
            owner = type(instance)
            scope = instance
        else:
            assert owner is not None
            scope = owner

        fqname = f"{owner.__qualname__}.{self._name}"
        if self._recursion_guard:
            raise NameError(f"recursion while resolving {fqname}")

        self._recursion_guard = True

        try:
            value = self._func(scope)
        except AttributeError as e:
            raise NameError(f"cannot define {fqname} yet") from e
        finally:
            self._recursion_guard = False

        setattr(scope, self._name, value)
        return value


class LazyProperty(LazyHybridProperty[T]):
    def __init__(
        self,
        meth: Callable[[Any], T],
        /,
    ) -> None:
        if isinstance(meth, classmethod):
            raise TypeError(
                f"{self.__class__.__name__} cannot be used to "
                f"decorate classmethods"
            )

        super().__init__(meth)

    def __get__(self, instance: Any, owner: type[Any] | None = None) -> T:
        if instance is None:
            cls = type(self)
            raise AssertionError(
                f"{_utils.type_repr(cls)}: unexpected lazy property "
                f"access on containing class (not instance)"
            )

        return super().__get__(instance, owner)


_ClassMethod = TypeAliasType(
    "_ClassMethod",
    "Callable[[type[Any]], T] | classmethod[type[Any], [], T]",
    type_params=(T,),
)


class LazyClassProperty(LazyHybridProperty[T]):
    def __init__(self, meth: _ClassMethod[T], /) -> None:
        if not isinstance(meth, classmethod):
            raise TypeError(
                f"{self.__class__.__name__} must be used to "
                f"decorate classmethods"
            )

        super().__init__(meth)

    def __get__(self, instance: Any, owner: type[Any] | None = None) -> T:
        if instance is not None or owner is None:
            cls = type(self)
            raise AssertionError(
                f"{_utils.type_repr(cls)}: unexpected lazy class property "
                f"access on containing class instance (not class)"
            )

        return super().__get__(instance, owner)
