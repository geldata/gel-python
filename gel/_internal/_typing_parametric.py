# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

from __future__ import annotations
from typing import (
    Any,
    ClassVar,
    Generic,
    get_type_hints,
)

from typing_extensions import (
    Self,
    TypeVar,
    TypeAliasType,
)

import contextvars
import sys
import typing
import weakref
from collections import defaultdict

from . import _typing_eval
from . import _typing_inspect
from . import _utils


__all__ = [
    "ParametricType",
    "SingleParametricType",
]


class _WeakTypeProxy:
    def __init__(self, t: type[Any]) -> None:
        self._t = weakref.ref(t)
        self._t_id = id(t)
        self._t_repr = repr(t)

    def __hash__(self) -> int:
        if self._t() is None:
            raise TypeError(f"{self._t_repr} has been garbage-collected")
        return self._t_id

    def __eq__(self, other: object) -> bool:
        if self._t() is None:
            raise TypeError(f"{self._t_repr} has been garbage-collected")
        if not isinstance(other, _WeakTypeProxy):
            return NotImplemented
        else:
            return self._t_id == other._t_id

    def __repr__(self) -> str:
        return self._t_repr


ParametricTypesCacheKey = tuple[_WeakTypeProxy, tuple[Any, ...]]
ParametricTypesCache = weakref.WeakValueDictionary[
    ParametricTypesCacheKey,
    "type[ParametricType]",
]

_PARAMETRIC_TYPES_CACHE: contextvars.ContextVar[
    ParametricTypesCache | None
] = contextvars.ContextVar("_GEL_PARAMETRIC_TYPES_CACHE", default=None)


def _get_parametric_type_cache_key(
    parent: type[ParametricType],
    typevars: Any,
) -> ParametricTypesCacheKey:
    return (
        _WeakTypeProxy(parent),
        typevars if isinstance(typevars, tuple) else (typevars,),
    )


def _get_cached_parametric_type(
    parent: type[ParametricType],
    typevars: Any,
) -> type[ParametricType] | None:
    parametric_types_cache = _PARAMETRIC_TYPES_CACHE.get()
    if parametric_types_cache is None:
        parametric_types_cache = ParametricTypesCache()
        _PARAMETRIC_TYPES_CACHE.set(parametric_types_cache)
    cache_key = _get_parametric_type_cache_key(parent, typevars)
    return parametric_types_cache.get(cache_key)


def _set_cached_parametric_type(
    parent: type[ParametricType],
    typevars: Any,
    type_: type[ParametricType],
) -> None:
    parametric_types_cache = _PARAMETRIC_TYPES_CACHE.get()
    if parametric_types_cache is None:
        parametric_types_cache = ParametricTypesCache()
        _PARAMETRIC_TYPES_CACHE.set(parametric_types_cache)
    cache_key = _get_parametric_type_cache_key(parent, typevars)
    parametric_types_cache[cache_key] = type_


def _is_fully_parametrized_over(
    cls: type[ParametricType],
    base: type[ParametricType],
) -> bool:
    type_args = cls.__parametric_type_args__
    if type_args is not None and (
        base in type_args or base.__parametric_origin__ in type_args
    ):
        return True

    # Special case for diamond inheritance: if the base is already a
    # specialized parametric type (has __parametric_origin__), and it's
    # in the direct bases of cls, then it's already fully parametrized.
    base_origin = getattr(base, "__parametric_origin__", None)
    return base_origin is not None and base in cls.__bases__


def _is_fully_parametrized(cls: type[ParametricType]) -> bool:
    # A class is fully parametrized if:
    # 1. All its bases are either non-parametric or parametrized by cls, AND
    # 2. It has no remaining type parameters
    has_no_type_params = not getattr(cls, "__parameters__", None)
    all_bases_parametrized = all(
        (
            not issubclass(b, ParametricType)
            or _is_fully_parametrized_over(cls, b)
        )
        for b in cls.__bases__
    )
    return has_no_type_params and all_bases_parametrized


_ForwardRefs = TypeAliasType(
    "_ForwardRefs",
    dict[
        Any,
        tuple[type["ParametricType"], tuple[type["ParametricType"], int], str],
    ],
)

_TypeArgs = TypeAliasType(
    "_TypeArgs",
    dict[type["ParametricType"], tuple[type, ...]],
)

_MutTypeArgs = TypeAliasType(
    "_MutTypeArgs",
    dict[type["ParametricType"], list[type]],
)


def _try_resolve_forward_refs(refs: _ForwardRefs) -> None:
    updated_type_args: dict[type[ParametricType], _MutTypeArgs] = {}
    for ref, (cls, (base, idx), attr) in tuple(refs.items()):
        resolved = _typing_eval.try_resolve_type(ref, owner=cls)
        if resolved is not None:
            setattr(cls, attr, resolved)
            updated_cls = updated_type_args.setdefault(cls, {})
            try:
                updated_base = updated_cls[base]
            except KeyError:
                cls_type_args = cls.__parametric_type_args__
                assert cls_type_args is not None
                updated_base = updated_cls[base] = [*cls_type_args[base]]

            updated_base[idx] = resolved
            del refs[ref]

    for cls, updates in updated_type_args.items():
        cls_type_args = cls.__parametric_type_args__
        assert cls_type_args is not None
        for base, type_args in updates.items():
            cls_type_args[base] = tuple(type_args)


class ParametricType:
    """Runtime generic type.

    Unlike GenericAlias which is not a type and is ephemeral, ParametricType
    materializes its specialization as a proper subclass of itself and stores
    specific type arguments into designated class vars.
    """

    __parametric_origin__: ClassVar[type | None] = None
    __parametric_type_args__: ClassVar[_TypeArgs | None] = None
    __parametric_forward_refs__: ClassVar[_ForwardRefs] = {}
    _type_param_map: ClassVar[dict[Any, str]] = {}
    _non_type_params: ClassVar[dict[int, type]] = {}

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()

        if ParametricType in cls.__bases__:
            cls._init_parametric_base()
        else:
            for b in cls.__bases__:
                if issubclass(
                    b, ParametricType
                ) and not _is_fully_parametrized_over(cls, b):
                    cls._init_parametric_user(b)

        # Validate diamond inheritance compatibility
        cls._validate_diamond_inheritance()

    @classmethod
    def _init_parametric_base(cls) -> None:
        """Initialize a direct subclass of ParametricType"""

        # ruff: noqa: ERA001
        #
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

        # We require ParametricType users to declare ClassVars for their
        # in the body of the class that inherits from ParametricType.
        #
        # get_type_hints() attempts to traverse the MRO and will error
        # out of ancestor classes have different class parameter names.
        # But we don't need to resolve those or care about them, we
        # are only interested in the ClassVars declared in the current
        # class (that's the contract of using ParametricType anyway).
        #
        # So we create a new throwaway class with the same name and
        # annotations as the current class, but with an empty MRO, just
        # to extract resolved ClassVars that we care about.
        #
        # Besides, this will be faster as the entire MRO doesn't
        # need to be traversed (not that it's a big deal, but still
        # an ever so slightly faster import).
        top_anno_cls = type(
            cls.__name__, (), {"__annotations__": cls.__annotations__}
        )
        annos = get_type_hints(top_anno_cls, mod.__dict__, include_extras=True)
        param_map = {}

        for attr, t in annos.items():
            if not _typing_inspect.is_classvar(t):
                continue

            args = typing.get_args(t)
            # ClassVar constructor should have the check, but be extra safe.
            assert len(args) == 1

            arg = args[0]
            if _typing_inspect.is_annotated(arg):
                arg_meta = arg.__metadata__
                if not arg_meta:
                    continue
                arg_meta_0 = arg_meta[0]
                if not _typing_inspect.is_type_var_tuple_unpack(arg_meta_0):
                    continue
                arg_args = typing.get_args(arg_meta_0)
            else:
                if typing.get_origin(arg) is not type:
                    continue

                arg_args = typing.get_args(arg)

            # Likewise, rely on Type checking its stuff in the constructor
            assert len(arg_args) == 1
            arg_args_0 = arg_args[0]

            if not _typing_inspect.is_type_var_or_tuple(arg_args_0):
                continue

            if arg_args_0 in params:
                param_map[arg_args_0] = attr

        for param in params:
            if param not in param_map:
                raise TypeError(
                    f"{cls.__name__}: missing ClassVar for"
                    f" generic parameter {param}"
                )

        cls._type_param_map = param_map

    @classmethod
    def _init_parametric_user(
        cls, parametric_base: type[ParametricType]
    ) -> None:
        """Initialize an indirect descendant of ParametricType."""

        # For ParametricType grandchildren we have to deal with possible
        # TypeVar remapping and generally check for type sanity.

        # Check for unspecialized parametric types in inheritance
        # This catches cases like: class Child(Base) where Base is a parametric
        # type but Base is not parameterized in __orig_bases__
        orig_bases = getattr(cls, "__orig_bases__", ())
        orig_base_origins = set()
        for ob in orig_bases:
            org = typing.get_origin(ob)
            if org is not None:
                orig_base_origins.add(org)

        for b in cls.__bases__:
            if (
                _typing_inspect.is_valid_isinstance_arg(b)
                and issubclass(b, ParametricType)
                and bool(getattr(b, "__parameters__", ()))
                and b is not ParametricType
                and b not in orig_base_origins
            ):
                raise TypeError(
                    f"{cls.__name__}: missing one or more type arguments for"
                    f" base {b.__name__!r}"
                )

        ob = getattr(cls, "__orig_bases__", ())
        generic_params: list[type] = []

        for b in ob:
            if _typing_inspect.is_valid_isinstance_arg(b) and issubclass(
                b, parametric_base
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

            base_map = dict(b._type_param_map)
            subclass_map = {}

            for i, arg in enumerate(args):
                if i in base_non_type_params:
                    continue
                if not _typing_inspect.is_type_var_or_tuple_unpack(arg):
                    raise TypeError(
                        f"{b.__name__}: {arg} "
                        f"is not a TypeVar or *TypeVarTuple"
                    )
                if _typing_inspect.is_type_var_tuple_unpack(arg):
                    arg = typing.get_args(arg)[0]  # noqa: PLW2901
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

    @classmethod
    def _validate_diamond_inheritance(cls) -> None:
        """Validate that diamond inheritance doesn't have conflicting type
        arguments.

        When a class inherits from multiple specializations of the same
        parametric base, ensure they don't have incompatible type arguments.
        """
        # Collect all parametric base types in the MRO
        # with their type arguments
        parametric_bases: defaultdict[type, dict[tuple[Any, ...], bool]] = (
            defaultdict(dict)
        )

        for base in cls.__mro__:
            if (
                issubclass(base, ParametricType)
                and base.__parametric_origin__ is not None
            ):
                origin = base.__parametric_origin__
                type_args = base.__parametric_type_args__

                if type_args and (args := type_args.get(origin)) is not None:
                    parametric_bases[origin][
                        tuple(
                            _WeakTypeProxy(a) if isinstance(a, type) else a
                            for a in args
                        )
                    ] = True

        # Check for conflicts
        for origin, arg_sets in parametric_bases.items():
            if len(arg_sets) <= 1:
                continue

            # If we have multiple different type argument sets, it's invalid
            conflicting = " and ".join(
                f"[{' '.join(str(a))}]" for a in arg_sets
            )
            raise TypeError(
                f"Base classes of {cls.__name__} are mutually "
                f"incompatible: {origin.__name__} appears with "
                f"conflicting type arguments {conflicting}"
            )

    def __new__(cls, *args: Any, **kwargs: Any) -> Self:
        if not _is_fully_parametrized(cls):
            raise TypeError(
                f"{cls.__qualname__} must be parametrized to instantiate"
            )

        if cls.__parametric_forward_refs__:
            _try_resolve_forward_refs(cls.__parametric_forward_refs__)

        if cls.__parametric_forward_refs__:
            raise TypeError(
                f"{cls.__qualname__} has unresolved type parameters"
            )

        if super().__new__ is object.__new__:
            return super().__new__(cls)
        else:
            return super().__new__(cls, *args, **kwargs)

    def __class_getitem__(
        cls,
        params: Any,
        /,
    ) -> type[ParametricType]:
        """Return a dynamic subclass parametrized with `params`.

        We cannot use `_GenericAlias` provided by `Generic[T]` because the
        default `__class_getitem__` on `_GenericAlias` is not a real type and
        so it doesn't retain information on generics on the class.  Even on
        the object, it adds the relevant `__orig_class__` link too late, after
        `__init__()` is called.  That means we wouldn't be able to type-check
        in the initializer using built-in `Generic[T]`.
        """
        if _is_fully_parametrized(cls):
            raise TypeError(f"{cls!r} is already parametrized")

        result = _get_cached_parametric_type(cls, params)
        if result is not None:
            return result

        if not isinstance(params, tuple):
            params = (params,)
        all_params: tuple[Any, ...] = params
        type_args = []
        for i, param in enumerate(all_params):
            if i not in cls._non_type_params:
                type_args.append(param)
        params_str = ", ".join(_utils.type_repr(a) for a in all_params)
        name = f"{cls.__name__}[{params_str}]"
        type_args_dict: dict[type[ParametricType], tuple[type, ...]] = {
            **(getattr(cls, "__parametric_type_args__", {}) or {}),
            cls: tuple(type_args),
        }
        type_dict: dict[str, Any] = {
            "__parametric_origin__": cls,
            "__parametric_type_args__": type_args_dict,
            "__module__": cls.__module__,
        }
        tuple_to_attr: dict[int, str] = {}
        type_var_tuple_idx: int | None = None
        typevar_defaults: dict[int, Any] = {}
        args_hi_idx = len(all_params) - 1

        if cls._type_param_map:
            gen_params = getattr(cls, "__parameters__", ())
            for i, gen_param in enumerate(gen_params):
                attr = cls._type_param_map.get(gen_param)
                if attr:
                    tuple_to_attr[i] = attr
                    if _typing_inspect.is_type_var_tuple(gen_param):
                        type_var_tuple_idx = i
                    elif (
                        i > args_hi_idx
                        and _typing_inspect.is_type_var(gen_param)
                        and (
                            has_default := getattr(
                                gen_param, "has_default", None
                            )
                        )
                        is not None
                        and has_default()
                    ):
                        typevar_defaults[i] = gen_param.__default__

            expected = len(gen_params)
            actual = len(params)

            # Skip validation if there are TypeVarTuple parameters
            # (they accept variable args)
            has_type_var_tuple = any(
                _typing_inspect.is_type_var_tuple(p) for p in gen_params
            )

            if not has_type_var_tuple and expected != actual:
                # For too many arguments, always fail
                if actual > expected:
                    raise TypeError(
                        f"type {cls.__name__!r} expects {expected} type"
                        f" parameter{'s' if expected != 1 else ''},"
                        f" got {actual}"
                    )

                # For too few arguments, check if we have enough defaults
                elif actual < expected:
                    for i in range(actual, expected):
                        if i not in typevar_defaults:
                            raise TypeError(
                                f"type {cls.__name__!r} expects {expected} "
                                f"type parameter{'s' if expected != 1 else ''}"
                                f", got {actual}"
                            )

            for i, attr in tuple_to_attr.items():
                if i == type_var_tuple_idx:
                    type_dict[attr] = all_params[i:]
                    break
                elif i > args_hi_idx:
                    type_dict[attr] = typevar_defaults[i]
                else:
                    type_dict[attr] = all_params[i]

        forward_refs: dict[Any, tuple[int, str]] = {}
        num_type_args = len(type_args)
        num_type_vars = 0

        for i, arg in enumerate(type_args):
            if _typing_inspect.is_type_var_or_tuple_unpack(arg):
                num_type_vars += 1
            elif _typing_inspect.contains_forward_refs(arg):
                forward_refs[arg] = (i, tuple_to_attr[i])
            elif not _typing_inspect.is_valid_type_arg(arg):
                raise TypeError(
                    f"{cls!r} expects types as type parameters, got {arg!r}"
                )

        if num_type_vars == num_type_args:
            # All parameters are type variables: return the regular generic
            # alias to allow proper subclassing.
            generic = super(ParametricType, cls)
            return generic.__class_getitem__(all_params)  # type: ignore [attr-defined, no-any-return]
        else:
            result = type(name, (cls,), type_dict)
            assert issubclass(result, ParametricType)

            if forward_refs:
                result.__parametric_forward_refs__ = {
                    **cls.__parametric_forward_refs__,
                    **{
                        fref: (result, (cls, idx), attr_name)
                        for fref, (idx, attr_name) in forward_refs.items()
                    },
                }

            _set_cached_parametric_type(cls, params, result)

            return result

    def __reduce__(self) -> tuple[Any, ...]:
        raise NotImplementedError(
            f"{type(self)} must implement explicit __reduce__ "
            f"for ParametricType subclass"
        )


_T = TypeVar("_T", covariant=True)


class SingleParametricType(ParametricType, Generic[_T]):
    type: ClassVar[type[_T]]  # type: ignore [misc]
