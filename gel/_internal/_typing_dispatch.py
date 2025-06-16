# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.


from typing import (
    Any,
    Generic,
    ParamSpec,
    TypeVar,
    overload,
)
from collections.abc import (
    Callable,
)
from typing_extensions import (
    get_overloads,
)

import inspect
import functools
import types
import typing

from gel._internal import _namespace
from gel._internal import _typing_inspect

_P = ParamSpec("_P")
_R_co = TypeVar("_R_co", covariant=True)


def _isinstance(obj: Any, tp: Any) -> bool:
    if isinstance(tp, type):
        return isinstance(obj, tp)
    elif isinstance(tp, tuple):
        return any(_isinstance(obj, el) for el in tp)
    elif _typing_inspect.is_union_type(tp):
        return any(_isinstance(obj, el) for el in typing.get_args(tp))
    elif _typing_inspect.is_generic_alias(tp):
        origin = typing.get_origin(tp)
        args = typing.get_args(tp)
        if origin is type:
            if isinstance(obj, type):
                return issubclass(obj, args[0])
            elif (mroent := getattr(obj, "__mro_entries__", None)) is not None:
                genalias_mro = mroent((obj,))
                return any(issubclass(c, args[0]) for c in genalias_mro)
            else:
                return False
        else:
            raise TypeError(f"_isinstance() argument 2 is {tp!r}")

    else:
        raise TypeError(f"_isinstance() argument 2 is {tp!r}")


class _OverloadDispatch(Generic[_P, _R_co]):
    def __init__(
        self,
        func: Callable[_P, _R_co],
    ) -> None:
        self._qname = func.__qualname__
        self._overloads = {
            fn: inspect.signature(fn) for fn in get_overloads(func)
        }
        self._type_hints: dict[Callable[..., Any], dict[str, Any]] = {}
        self._attr_name: str | None = None
        self._boundcall = functools.partial(self._call, boundmethod=True)
        functools.update_wrapper(self._boundcall, func)
        functools.update_wrapper(self, func)

    def __set_name__(self, name: str, owner: type[Any]) -> None:
        self._attr_name = name

    @overload
    def __get__(
        self,
        instance: None,
        owner: type[Any],
    ) -> Callable[_P, _R_co]: ...

    @overload
    def __get__(
        self,
        instance: Any,
        owner: type[Any] | None = None,
    ) -> Callable[_P, _R_co]: ...

    def __get__(
        self,
        instance: Any | None,
        owner: type[Any] | None = None,
    ) -> Callable[_P, _R_co]:
        if instance is None:
            return self
        else:
            return types.MethodType(self._boundcall, instance)

    def __call__(self, *args: _P.args, **kwargs: _P.kwargs) -> _R_co:
        return self._call(args, kwargs)

    def _call(
        self,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        *,
        boundmethod: bool = False,
    ) -> _R_co:
        for fn, sig in self._overloads.items():
            try:
                bound = sig.bind(*args, **kwargs)
            except TypeError:
                continue

            param_types = self._type_hints.get(fn)
            if param_types is None:
                param_types = typing.get_type_hints(
                    fn,
                    globalns=_namespace.module_ns_of(fn),
                )
                self._type_hints[fn] = param_types
            bound_args = iter(bound.arguments.items())
            if boundmethod or self._attr_name:
                next(bound_args)

            for pn, arg in bound_args:
                pt = param_types[pn]
                if not _isinstance(arg, pt):
                    break
            else:
                result = fn(*args, **kwargs)
                break
        else:
            raise TypeError(
                f"cannot dispatch to {self._qname}: no overload found for "
                f"args={args} and kwargs={kwargs}"
            )

        return result  # type: ignore [return-value]


def dispatch_overload(
    func: Callable[_P, _R_co],
) -> Callable[_P, _R_co]:
    return _OverloadDispatch(func)
