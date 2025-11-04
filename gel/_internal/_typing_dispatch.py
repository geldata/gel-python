# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

"""
Utility for runtime overload dispatch based on type hints.

This module provides a decorator ``dispatch_overload`` that transforms a
function with ``@overload`` definitions into a callable object which selects
the correct implementation at runtime by inspecting the arguments' types
against the type hints of each overload. It mimics the behaviour of static
overload resolution but works dynamically.
"""

from typing import (
    Any,
    Generic,
    ParamSpec,
    TypeVar,
    overload,
)
from collections.abc import (
    Callable,
    Collection,
    Mapping,
)
from typing_extensions import (
    get_overloads,
)

import inspect
import functools
import types
import typing

from gel._internal import _namespace
from gel._internal import _type_expression
from gel._internal import _typing_eval
from gel._internal import _typing_inspect
from gel._internal import _typing_parametric
from gel._internal._utils import type_repr

_P = ParamSpec("_P")
_R_co = TypeVar("_R_co", covariant=True)


def _resolve_to_bound(tp: Any, fn: Any) -> Any:
    if isinstance(tp, TypeVar):
        tp = tp.__bound__
        ns = _namespace.module_ns_of(fn)
        tp = _typing_eval.resolve_type(tp, globals=ns)

    return tp


def _issubclass(lhs: Any, tp: Any, fn: Any) -> bool:
    # NB: Much more limited than _isinstance below.

    # The only special case here is handling subtyping on
    # our ParametricTypes. ParametricType creates bona fide
    # subclasses when indexed with concrete types, but GenericAlias
    # when indexed with type variables.
    #
    # This handles the case where the RHS of issubclass is a
    # GenericAlias over one of our ParametricTypes by comparing the
    # types for equality and then checking that the concrete types are
    # subtypes of the variable bounds.
    # This lets us handle cases like:
    # std.array[Object] <: std.array[_T_anytype].

    if issubclass(lhs, _type_expression.Intersection):
        return any(_issubclass(c, tp, fn) for c in (lhs.lhs, lhs.rhs))
    elif issubclass(lhs, _type_expression.Union):
        return all(_issubclass(c, tp, fn) for c in (lhs.lhs, lhs.rhs))

    if _typing_inspect.is_generic_alias(tp):
        origin = typing.get_origin(tp)
        args = typing.get_args(tp)
        if issubclass(origin, _typing_parametric.ParametricType):
            if (
                not issubclass(lhs, _typing_parametric.ParametricType)
                or lhs.__parametric_origin__ is not origin
                or lhs.__parametric_type_args__ is None
            ):
                return False

            targs = lhs.__parametric_type_args__[origin]
            return all(
                _issubclass(l, _resolve_to_bound(r, fn), fn)
                for l, r in zip(targs, args, strict=True)
            )

    # In other cases,
    return issubclass(lhs, tp)  # pyright: ignore [reportArgumentType]


def _isinstance(obj: Any, tp: Any, fn: Any) -> bool:
    # Handle Any type - matches everything
    if tp is Any:
        return True

    # Handle basic types
    if _typing_inspect.is_valid_isinstance_arg(tp):
        return isinstance(obj, tp)

    elif _typing_inspect.is_union_type(tp):
        return any(_isinstance(obj, el, fn) for el in typing.get_args(tp))

    elif _typing_inspect.is_literal(tp):
        # For Literal types, check if obj is one of the literal values
        return obj in typing.get_args(tp)

    elif _typing_inspect.is_generic_alias(tp):
        origin = typing.get_origin(tp)
        args = typing.get_args(tp)
        if origin is type:
            atype = _resolve_to_bound(args[0], fn)

            if isinstance(obj, type):
                return issubclass(obj, atype)
            # NB: This is to handle the case where obj is something
            # like a qb BaseAlias, where it has some fictitious
            # associated type that isn't really its runtime type.
            elif (mroent := getattr(obj, "__mro_entries__", None)) is not None:
                genalias_mro = mroent((obj,))
                return any(_issubclass(c, atype, fn) for c in genalias_mro)
            else:
                return False

        elif not isinstance(origin, type):  # pragma: no cover
            raise TypeError(
                "_isinstance() argument 2 contains a generic with non-type "
                "origin"
            )

        elif issubclass(origin, Mapping):
            # Check the container type first
            if not isinstance(obj, origin):
                return False

            # If no type args or empty mapping, we're done
            if not args or len(obj) == 0:
                return True

            if len(args) != 2:
                raise TypeError(
                    f"_isinstance() argument 2 contains improperly typed "
                    f"{type_repr(origin)} generic"
                )

            # For Mapping[K, V], check first key and first value
            k, v = next(iter(obj.items()))
            return _isinstance(k, args[0], fn) and _isinstance(v, args[1], fn)

        elif issubclass(origin, tuple):
            # Check the container type first
            if not isinstance(obj, origin):
                return False

            num_args = len(args)
            num_elems = len(obj)

            # If no type args or empty container, we're done
            if num_args == 0 or num_elems == 0:
                return True

            # Tuples can be homogeneous tuple[T, ...] or
            # heterogeneous tuple[*T]
            if num_args == 2 and args[1] is ...:
                # Homogeneous tuple like tuple[int, ...]
                return _isinstance(next(iter(obj)), args[0], fn)
            elif num_args != num_elems:
                # Shape of tuple value does not match type definition
                return False
            else:
                for el_type, el_val in zip(args, obj, strict=True):
                    if not _isinstance(el_val, el_type, fn):
                        return False
                return True

        elif issubclass(origin, Collection):
            # Check the container type first
            if not isinstance(obj, origin):
                return False

            # If no type args or empty container, we're done
            if not args or len(obj) == 0:
                return True

            return _isinstance(next(iter(obj)), args[0], fn)

        else:
            # For other generic types, fall back to checking the origin
            return isinstance(obj, origin)

    elif isinstance(tp, TypeVar):
        return _isinstance(obj, _resolve_to_bound(tp, fn), fn)

    else:
        raise TypeError(f"_isinstance() argument 2 is {tp!r}")


class _OverloadDispatch(Generic[_P, _R_co]):
    def __init__(
        self,
        func: Callable[_P, _R_co],
    ) -> None:
        self._qname = func.__qualname__
        self._overloads: dict[Callable[..., _R_co], inspect.Signature] = {}
        for fn in get_overloads(func):
            real_fn: Callable[..., Any] = getattr(fn, "__func__", fn)
            self._overloads[real_fn] = inspect.signature(real_fn)
        self._param_types: dict[Callable[..., Any], dict[str, Any]] = {}
        self._is_classmethod = isinstance(func, classmethod)
        self._is_staticmethod = isinstance(func, staticmethod)
        self._is_method = False
        self._attr_name: str | None = None
        functools.update_wrapper(self, func)

    def __set_name__(self, owner: type[Any], name: str) -> None:
        self._attr_name = name
        self._is_method = (
            not self._is_classmethod and not self._is_staticmethod
        )

    @overload
    def __get__(
        self,
        instance: None,
        owner: type[Any],
        /,
    ) -> Callable[_P, _R_co]: ...

    @overload
    def __get__(
        self,
        instance: Any,
        owner: type[Any] | None = None,
        /,
    ) -> Callable[_P, _R_co]: ...

    def __get__(
        self,
        instance: Any | None,
        owner: type[Any] | None = None,
        /,
    ) -> Callable[_P, _R_co]:
        if instance is None:
            if self._is_classmethod:

                def closure(
                    cls: type[Any],
                    /,
                    *args: _P.args,
                    **kwargs: _P.kwargs,
                ) -> _R_co:
                    return self._call(cls, *args, **kwargs)

                functools.update_wrapper(closure, self)

                cm = classmethod(closure).__get__(None, owner)
                if self._attr_name is not None:
                    try:
                        setattr(owner, self._attr_name, cm)
                    except (TypeError, AttributeError):  # pragma: no cover
                        pass
                return cm
            else:
                return self
        else:

            def method(
                instance: Any,
                /,
                *args: _P.args,
                **kwargs: _P.kwargs,
            ) -> _R_co:
                return self._call(instance, *args, **kwargs)

            functools.update_wrapper(method, self)

            m = types.MethodType(method, instance)
            if self._attr_name is not None:
                try:
                    setattr(instance, self._attr_name, m)
                except (TypeError, AttributeError):  # pragma: no cover
                    pass
            return m

    def __call__(self, *args: _P.args, **kwargs: _P.kwargs) -> _R_co:
        return self._call(None, *args, **kwargs)

    def _call(
        self,
        bound_to: object | type[Any] | None = None,
        /,
        *args: _P.args,
        **kwargs: _P.kwargs,
    ) -> _R_co:
        for fn, sig in self._overloads.items():
            try:
                if bound_to is not None:
                    bound = sig.bind(bound_to, *args, **kwargs)
                else:
                    bound = sig.bind(*args, **kwargs)
                bound.apply_defaults()
            except TypeError:
                continue

            # Get the bound arguments, skipping 'self' for methods

            param_types = self._param_types.get(fn)
            if param_types is None:
                ns = _namespace.module_ns_of(fn)
                type_hints = typing.get_type_hints(fn, globalns=ns)
                param_types = {
                    n: _typing_eval.resolve_type(t, globals=ns)
                    for n, t in type_hints.items()
                }
                self._param_types[fn] = param_types

            bound_args = iter(bound.arguments.items())
            # Methods might be called in unbound mode,
            # e.g Class.method(obj, *args, **kwargs)
            if bound_to is not None or self._is_method:
                next(bound_args)  # skip cls/self
            for pn, arg in bound_args:
                pt = param_types.get(pn)
                if pt is None:  # pragma: no cover
                    raise TypeError(
                        f"cannot dispatch to {self._qname}: an overload "
                        f"is missing a type annotation on the {pn} parameter"
                    )
                if not _isinstance(arg, pt, fn):
                    break
            else:
                if bound_to is not None:
                    return fn(bound_to, *args, **kwargs)
                else:
                    return fn(*args, **kwargs)

        # No matching overload found
        raise TypeError(
            f"cannot dispatch to {self._qname}: no overload found for "
            f"args={args!r} kwargs={kwargs!r}"
        )


def dispatch_overload(
    func: Callable[_P, _R_co],
) -> Callable[_P, _R_co]:
    return _OverloadDispatch(func)
