# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

from __future__ import annotations
from typing import (
    Any,
    ClassVar,
    Generic,
    Tuple,
    Type,
    TypeVar,
    Dict,
    get_type_hints,
)

from typing_extensions import (
    Self,
)

import contextvars
import types
import sys
import typing
import weakref
from types import GenericAlias

from . import _typing_eval
from . import _typing_inspect


__all__ = [
    "ParametricType",
    "SingleParametricType",
]


ParametricTypesCacheKey = tuple[Any, Any, tuple[Any, ...]]
ParametricTypesCache = weakref.WeakValueDictionary[
    ParametricTypesCacheKey,
    "type[ParametricType]",
]

_PARAMETRIC_TYPES_CACHE: contextvars.ContextVar[
    ParametricTypesCache | None
] = contextvars.ContextVar("_GEL_PARAMETRIC_TYPES_CACHE", default=None)


def _get_cached_parametric_type(
    parent: type[ParametricType],
    typevars: Any,
) -> type[ParametricType] | None:
    parametric_types_cache = _PARAMETRIC_TYPES_CACHE.get()
    if parametric_types_cache is None:
        parametric_types_cache = ParametricTypesCache()
        _PARAMETRIC_TYPES_CACHE.set(parametric_types_cache)
    return parametric_types_cache.get(typevars)


def _set_cached_parametric_type(
    parent: type[ParametricType],
    typevars: Any,
    type_: type[ParametricType],
) -> None:
    parametric_types_cache = _PARAMETRIC_TYPES_CACHE.get()
    if parametric_types_cache is None:
        parametric_types_cache = ParametricTypesCache()
        _PARAMETRIC_TYPES_CACHE.set(parametric_types_cache)
    parametric_types_cache[typevars] = type_


T = TypeVar("T", covariant=True)


class ParametricType:
    __parametric_type_args__: ClassVar[tuple[type, ...] | None] = None
    __parametric_orig_args__: ClassVar[tuple[type, ...] | None] = None
    __parametric_forward_refs__: ClassVar[dict[str, tuple[int, str]]] = {}
    _type_param_map: ClassVar[dict[Any, str]] = {}
    _non_type_params: ClassVar[dict[int, type]] = {}

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()

        if cls.__parametric_type_args__ is not None:
            return
        elif ParametricType in cls.__bases__:
            cls._init_parametric_base()
        elif any(issubclass(b, ParametricType) for b in cls.__bases__):
            cls._init_parametric_user()

    @classmethod
    def _init_parametric_base(cls) -> None:
        """Initialize a direct subclass of ParametricType"""

        # Direct subclasses of ParametricType must declare
        # ClassVar attributes corresponding to the Generic type vars.
        # For example:
        #     class P(ParametricType, Generic[T, V]):
        #         t: ClassVar[Type[T]]
        #         v: ClassVar[Type[V]]

        params = getattr(cls, "__parameters__", None)

        if not params:
            raise TypeError(f"{cls} must be declared as Generic")

        mod = sys.modules[cls.__module__]
        annos = get_type_hints(cls, mod.__dict__)
        param_map = {}

        for attr, t in annos.items():
            if not _typing_inspect.is_classvar(t):
                continue

            args = typing.get_args(t)
            # ClassVar constructor should have the check, but be extra safe.
            assert len(args) == 1

            arg = args[0]
            if typing.get_origin(arg) is not type:
                continue

            arg_args = typing.get_args(arg)
            # Likewise, rely on Type checking its stuff in the constructor
            assert len(arg_args) == 1

            if type(arg_args[0]) is not TypeVar:
                continue

            if arg_args[0] in params:
                param_map[arg_args[0]] = attr

        for param in params:
            if param not in param_map:
                raise TypeError(
                    f"{cls.__name__}: missing ClassVar for"
                    f" generic parameter {param}"
                )

        cls._type_param_map = param_map

    @classmethod
    def _init_parametric_user(cls) -> None:
        """Initialize an indirect descendant of ParametricType."""

        # For ParametricType grandchildren we have to deal with possible
        # TypeVar remapping and generally check for type sanity.

        ob = getattr(cls, "__orig_bases__", ())
        generic_params: list[type] = []

        for b in ob:
            if (
                isinstance(b, type)
                and not isinstance(b, GenericAlias)
                and issubclass(b, ParametricType)
                and b is not ParametricType
            ):
                raise TypeError(
                    f"{cls.__name__}: missing one or more type arguments for"
                    f" base {b.__name__!r}"
                )

            org = typing.get_origin(b)
            if org is None:
                continue

            if not isinstance(org, type):
                continue
            if not issubclass(org, ParametricType):
                generic_params.extend(getattr(b, "__parameters__", ()))
                continue

            base_params = getattr(org, "__parameters__", ())
            base_non_type_params = getattr(org, "_non_type_params", {})
            args = typing.get_args(b)
            expected = len(base_params)
            if len(args) != expected:
                raise TypeError(
                    f"{b.__name__} expects {expected} type arguments"
                    f" got {len(args)}"
                )

            base_map = dict(cls._type_param_map)
            subclass_map = {}

            for i, arg in enumerate(args):
                if i in base_non_type_params:
                    continue
                if type(arg) is not TypeVar:
                    raise TypeError(
                        f"{b.__name__} expects all arguments to be TypeVars"
                    )

                base_typevar = base_params[i]
                attr = base_map.get(base_typevar)
                if attr is not None:
                    subclass_map[arg] = attr

            if len(subclass_map) != len(base_map):
                raise TypeError(
                    f"{cls.__name__}: missing one or more type arguments for"
                    f" base {org.__name__!r}"
                )

            cls._type_param_map = subclass_map

        cls._non_type_params = {
            i: p
            for i, p in enumerate(generic_params)
            if p not in cls._type_param_map
        }

    def __new__(cls, *args: Any, **kwargs: Any) -> Self:
        if cls.__parametric_forward_refs__:
            raise TypeError(
                f"{cls.__qualname__} has unresolved type parameters"
            )

        if cls.__parametric_type_args__ is None:
            raise TypeError(
                f"{cls.__qualname__} must be parametrized to instantiate"
            )

        if super().__new__ is object.__new__:
            return super().__new__(cls)
        else:
            return super().__new__(cls, *args, **kwargs)

    def __class_getitem__(
        cls,
        params: type[Any] | tuple[type[Any], ...],
    ) -> type[ParametricType]:
        """Return a dynamic subclass parametrized with `params`.

        We cannot use `_GenericAlias` provided by `Generic[T]` because the
        default `__class_getitem__` on `_GenericAlias` is not a real type and
        so it doesn't retain information on generics on the class.  Even on
        the object, it adds the relevant `__orig_class__` link too late, after
        `__init__()` is called.  That means we wouldn't be able to type-check
        in the initializer using built-in `Generic[T]`.
        """
        if cls.__parametric_type_args__ is not None:
            raise TypeError(f"{cls!r} is already parametrized")

        result = _get_cached_parametric_type(cls, params)
        if result is not None:
            return result

        if not isinstance(params, tuple):
            params = (params,)
        all_params = params
        type_params = []
        for i, param in enumerate(all_params):
            if i not in cls._non_type_params:
                type_params.append(param)
        params_str = ", ".join(_type_repr(a) for a in all_params)
        name = f"{cls.__name__}[{params_str}]"
        bases = (cls,)
        type_dict: Dict[str, Any] = {
            "__parametric_type_args__": tuple(type_params),
            "__parametric_orig_args__": all_params,
            "__module__": cls.__module__,
        }
        forward_refs: Dict[str, Tuple[int, str]] = {}
        tuple_to_attr: Dict[int, str] = {}

        if cls._type_param_map:
            gen_params = getattr(cls, "__parameters__", ())
            for i, gen_param in enumerate(gen_params):
                attr = cls._type_param_map.get(gen_param)
                if attr:
                    tuple_to_attr[i] = attr

            expected = len(gen_params)
            actual = len(params)
            if expected != actual:
                raise TypeError(
                    f"type {cls.__name__!r} expects {expected} type"
                    f" parameter{'s' if expected != 1 else ''},"
                    f" got {actual}"
                )

            for i, attr in tuple_to_attr.items():
                type_dict[attr] = all_params[i]

        if not all(isinstance(param, type) for param in type_params):
            if all(
                type(param) is TypeVar  # type: ignore[comparison-overlap]
                for param in type_params
            ):
                # All parameters are type variables: return the regular generic
                # alias to allow proper subclassing.
                generic = super(ParametricType, cls)
                return generic.__class_getitem__(all_params)  # type: ignore
            else:
                forward_refs = {}
                for i, param in enumerate(type_params):
                    if isinstance(param, str):
                        forward_refs[param] = (i, tuple_to_attr[i])

                if not forward_refs:
                    raise TypeError(
                        f"{cls!r} expects types as type parameters"
                    )

        result = type(name, bases, type_dict)
        assert issubclass(result, ParametricType)
        result.__parametric_forward_refs__ = forward_refs

        _set_cached_parametric_type(cls, params, result)

        return result

    @classmethod
    def is_fully_resolved(cls) -> bool:
        return not cls.__parametric_forward_refs__

    @classmethod
    def try_resolve_types(
        cls,
        globalns: dict[str, Any] | None = None,
    ) -> None:
        if cls.__parametric_type_args__ is None:
            raise TypeError(f"{cls!r} is not parametrized")

        if not cls.__parametric_forward_refs__:
            return

        types = list(cls.__parametric_type_args__)

        ns = {}
        try:
            module_dict = sys.modules[cls.__module__].__dict__
        except AttributeError:
            pass
        else:
            ns.update(module_dict)

        if globalns is not None:
            ns.update(globalns)

        for ut, (idx, attr) in tuple(cls.__parametric_forward_refs__.items()):
            t = _typing_eval.try_resolve_type(ut, globals=ns)
            if t is None:
                continue
            elif isinstance(t, type) and not isinstance(t, GenericAlias):
                types[idx] = t
                setattr(cls, attr, t)
                del cls.__parametric_forward_refs__[ut]
            else:
                raise TypeError(
                    f"{cls!r} expects types as type parameters, got {t!r:.100}"
                )

        cls.__parametric_type_args__ = tuple(types)

    @classmethod
    def is_anon_parametrized(cls) -> bool:
        return cls.__name__.endswith("]")

    def __reduce__(self) -> Tuple[Any, ...]:
        raise NotImplementedError(
            "must implement explicit __reduce__ for ParametricType subclass"
        )


class SingleParametricType(ParametricType, Generic[T]):
    type: ClassVar[Type[T]]  # type: ignore


def _type_repr(obj: Any) -> str:
    if isinstance(obj, type):
        if obj.__module__ == "builtins":
            return obj.__qualname__
        return f"{obj.__module__}.{obj.__qualname__}"
    if isinstance(obj, types.FunctionType):
        return obj.__name__
    return repr(obj)
