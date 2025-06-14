# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

from typing import (
    TYPE_CHECKING,
    Any,
    Concatenate,
    Generic,
    TypeVar,
    ParamSpec,
    overload,
)
from collections.abc import Callable
from typing_extensions import Never

import functools
import types

_T = TypeVar("_T")
_P = ParamSpec("_P")
_R_co = TypeVar("_R_co", covariant=True)


class hybridmethod(Generic[_P, _R_co]):  # noqa: N801
    """Transform a method in a hybrid class/instance method.

    A hybrid method would receive either the class or the instance
    as the first implicit argument depending on whether the method
    was called on a class or an instance of a class.
    """

    def __init__(self, f: Callable[_P, _R_co], /) -> None:
        self._func = f
        # Copy __doc__, __name__, __qualname__, __annotations__,
        # and set __wrapped__ so inspect.signature works
        functools.update_wrapper(self, f)

    if TYPE_CHECKING:

        def __call__(self, *args: _P.args, **kwargs: _P.kwargs) -> _R_co:
            # Make the descriptor itself a Callable[P, R],
            # satisfying update_wrapper's signature
            return self._func(*args, **kwargs)

    @overload
    def __get__(self, obj: None, cls: type[Any]) -> Callable[_P, _R_co]: ...

    @overload
    def __get__(self, obj: Any, cls: type[Any]) -> Callable[_P, _R_co]: ...

    def __get__(self, obj: Any, cls: type[Any]) -> Callable[_P, _R_co]:
        target = obj if obj is not None else cls
        # bind to either instance or class
        return types.MethodType(self._func, target)


class classonlymethod(Generic[_P, _R_co]):  # noqa: N801
    """Transform a method into a class-only method."""

    def __init__(
        self, cm: Callable[_P, _R_co], /
    ) -> None:
        self._cm = cm

    @overload
    def __get__(
        self,
        instance: None,
        owner: type[Any],
    ) -> Callable[_P, _R_co]: ...

    @overload
    def __get__(
        self, instance: Never, owner: type[Any] | None = None
    ) -> Callable[_P, _R_co]: ...

    def __get__(
        self,
        instance: None,
        owner: type[Any] | None = None,
    ) -> Callable[_P, _R_co]:
        if instance is not None:
            raise AttributeError(
                f"'{self._cm.__qualname__}' is a class-only method and "
                "cannot be accessed on instances"
            )
        else:
            return self._cm.__get__(None, owner)  # type: ignore [no-any-return]
