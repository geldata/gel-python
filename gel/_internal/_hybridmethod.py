# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

from typing import (
    Any,
    Callable,
    Generic,
    TypeVar,
    ParamSpec,
    overload,
)

import functools
import types

P = ParamSpec("P")
R = TypeVar("R")


class hybridmethod(Generic[P, R]):
    """Transform a method in a hybrid class/instance method.

    A hybrid method would receive either the class or the instance
    as the first implicit argument depending on whether the method
    was called on a class or an instance of a class.
    """
    def __init__(self, func: Callable[P, R]) -> None:
        self.func = func
        # Copy __doc__, __name__, __qualname__, __annotations__,
        # and set __wrapped__ so inspect.signature works
        functools.update_wrapper(self, func)

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R:
        # Make the descriptor itself a Callable[P, R],
        # satisfying update_wrapper's signature
        return self.func(*args, **kwargs)

    @overload
    def __get__(self, obj: None, cls: type[Any]) -> Callable[P, R]: ...

    @overload
    def __get__(self, obj: Any, cls: type[Any]) -> Callable[P, R]: ...

    def __get__(self, obj: Any, cls: type[Any]) -> Callable[P, R]:
        target = obj if obj is not None else cls
        # bind to either instance or class
        return types.MethodType(self.func, target)
