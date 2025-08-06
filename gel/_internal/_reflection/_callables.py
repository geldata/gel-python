# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.
#

from __future__ import annotations
from typing import NamedTuple, TypeVar
from typing_extensions import Self, TypeAliasType

import dataclasses
import functools
import importlib
import operator
import typing
import uuid
from collections import defaultdict
from collections.abc import (
    Collection,
    Iterator,
    Iterable,
    Mapping,
    MutableMapping,
    Set as AbstractSet,
)

from ._base import sobject, struct, SchemaObject
from ._types import Indirection, TypeRef, Type, Schema, compare_type_generality
from ._enums import CallableParamKind, TypeModifier


CallableParamKey = TypeAliasType("CallableParamKey", int | str)
"""Ordinal (for positional params) or name (for named-only params)."""

PyTypeName = TypeAliasType("PyTypeName", tuple[str, str])
"""Fully-qualified name of a Python type."""

CallableParamTypeMap = TypeAliasType(
    "CallableParamTypeMap",
    MutableMapping[CallableParamKey, list[Type | PyTypeName]],
)


@functools.cache
def _get_py_type(name: PyTypeName) -> type:
    mod = importlib.import_module(name[0])
    return getattr(mod, name[1])  # type: ignore [no-any-return]


@struct
class CallableParam:
    name: str
    type: TypeRef | Type
    kind: CallableParamKind
    typemod: TypeModifier
    index: int
    default: str | None

    def __str__(self) -> str:
        if self.typemod is TypeModifier.Optional:
            typespec = f"optional {self.type}"
        elif self.typemod is TypeModifier.SetOf:
            typespec = f"set of {self.type}"
        else:
            typespec = f"{self.type}"

        if self.kind is CallableParamKind.NamedOnly:
            name = f"named only {self.name}"
        elif self.kind is CallableParamKind.Variadic:
            name = f"variadic {self.name}"
        else:
            name = self.name

        sig = f"{name}: {typespec}"
        if self.default is not None:
            sig = f"{sig} = {self.default}"

        return sig

    @functools.cached_property
    def key(self) -> CallableParamKey:
        """Key used to identify the parameter in a signature"""
        if self.kind is CallableParamKind.NamedOnly:
            return self.name
        else:
            return self.index

    @functools.cached_property
    def sort_key(self) -> str:
        return str(self.key)

    @functools.cached_property
    def is_variadic(self) -> bool:
        return self.kind is CallableParamKind.Variadic

    def get_type(self, schema: Schema) -> Type:
        t = self.type
        return schema[t.id] if isinstance(t, TypeRef) else t

    def specialize(
        self,
        spec: Mapping[Indirection, tuple[Type, Type]],
        *,
        schema: Mapping[str, Type],
    ) -> Self:
        return dataclasses.replace(
            self,
            type=self.get_type(schema).specialize(spec, schema=schema),
        )


CallableGenericPositions = TypeAliasType(
    "CallableGenericPositions",
    Mapping[
        CallableParamKey,
        Mapping[Type, AbstractSet[Indirection]],
    ],
)
"""Mapping type encoding positions of typevars in callable params.

The key is the param position (index for positional, name for named), and
the value is a mapping where a key is a generic type and a value is a set
of indirections within the param type where that generic type is located,
e.g for

    def fn[T: anytype, T2: anyint](
       bar: T,
       *,
       foo: tuple[T2, list[tuple[int, T, T2]]],
    )

the position mapping would be:

    {
        0: {
            <anytype>: {()},  # empty indirection indicates type itself
        },
        "foo": {
            # "__element_type__" indicates a homogeneous container
            # element type;
            # "__element_types__[N]" indicates a type within a
            # heterogeneous container at position N.
            <anytype>: {
                (
                    ("__element_types__", 1),
                    "__element_type__",
                    ("__element_types__", 1),
                ),
            },
            <anyint>: {
                (
                    ("__element_types__", 0),
                ),
                (
                    ("__element_types__", 1),
                    "__element_type__",
                    ("__element_types__", 1),
                ),
            },
        }
    }

"""


class CallableSignature(NamedTuple):
    """A 4-tuple type containing the callable's fully-qualified schema
    path, the number of positional parameters, a boolean indicating the
    presence if a variadic argument and a frozenset of names of named-only
    parameters.
    """

    name: str
    num_pos: int
    num_pos_with_defaults: int
    has_variadic: bool
    kwargs: frozenset[str]
    kwargs_with_defaults: frozenset[str]

    def contains(self, other: CallableSignature) -> bool:
        """Determine if this signature contains the *other* signature,
        i.e that any permissible combination of this signature's params
        overlaps with other's from the standpoint of typing.overload
        logic.

        For self to contain other, every call that matches other must also
        match self. This means self must be at least as permissive as other
        in all dimensions: positional args, variadic args, and named args.

        Examples:
            func(x, y=1) contains func(x)  # self accepts 1-2 pos args,
                                           # other needs 1
            func(x, *args) contains func(x, y, z)  # self accepts 1+ pos args,
                                                   # other needs 3
            func(*, x=1) contains func(*, x)  # self makes x optional,
                                              # other requires x
            func(*, x, y=1) contains func(*, x)  # self requires x,
                                                 # other also requires x

        Containment logic:

        1. Positional args: self's acceptable range must contain other's range.
        2. Named args: all of other's named args must be acceptable to self.
        3. Required args: self cannot require args that other doesn't require.

        Args:
            other: The signature to check for containment

        Returns:
            True if self can handle all calls that other can handle
        """
        if self.name != other.name:
            return False

        # Calculate the range of positional arguments each signature accepts
        self_min_pos = self.num_pos - self.num_pos_with_defaults
        self_max_pos = float("inf") if self.has_variadic else self.num_pos

        other_min_pos = other.num_pos - other.num_pos_with_defaults
        other_max_pos = float("inf") if other.has_variadic else other.num_pos

        # For containment, self's positional range must contain other's range
        if not (
            self_min_pos <= other_min_pos and other_max_pos <= self_max_pos
        ):
            return False

        # Check named-only arguments:
        # For self to contain other, self must be able to handle all call
        # patterns that other accepts. This means:
        # 1. All of other's named args must be acceptable to self
        if not other.kwargs.issubset(self.kwargs):
            return False

        # 2. If other requires fewer named args than self, self cannot contain
        # other because other accepts calls that self would reject
        self_required_kwargs = self.kwargs - self.kwargs_with_defaults
        other_required_kwargs = other.kwargs - other.kwargs_with_defaults

        # Self's required kwargs must be a subset of other's required kwargs
        # (self cannot require something that other doesn't require)
        return self_required_kwargs.issubset(other_required_kwargs)

    def overlaps(self, other: CallableSignature) -> bool:
        """Determine if this callable overlaps with *other*.

        Two signatures overlap if there exists at least one call that would
        match both signatures. This is different from containment where one
        signature handles ALL calls that the other handles.

        Examples:
            func(x) overlaps with func(x, y=1)  # both accept func(1)
            func(x, y) does not overlap with func(*, z)  # no common calls
            func(x, *, y) overlaps with func(x, *, y, z=1)  # both accept
                                                            # func(1, y=2)

        Args:
            other: The signature to check for overlap.

        Returns:
            True if self and other have overlapping calls.
        """
        sig1 = self
        sig2 = other

        if sig1.name != sig2.name:
            return False

        # Check if positional argument ranges intersect
        sig1_min_pos = sig1.num_pos - sig1.num_pos_with_defaults
        sig1_max_pos = float("inf") if sig1.has_variadic else sig1.num_pos

        sig2_min_pos = sig2.num_pos - sig2.num_pos_with_defaults
        sig2_max_pos = float("inf") if sig2.has_variadic else sig2.num_pos

        # Ranges intersect if max(min1, min2) <= min(max1, max2)
        pos_min = max(sig1_min_pos, sig2_min_pos)
        pos_max = min(sig1_max_pos, sig2_max_pos)
        if pos_min > pos_max:
            return False

        # Check if there exists a valid combination of named arguments
        # that both signatures accept
        sig1_required = sig1.kwargs - sig1.kwargs_with_defaults
        sig2_required = sig2.kwargs - sig2.kwargs_with_defaults

        # Combined required arguments that any overlapping call must include
        combined_required = sig1_required | sig2_required

        # Both signatures must accept all combined required arguments
        return combined_required.issubset(
            sig1.kwargs
        ) and combined_required.issubset(sig2.kwargs)

    def iter_param_keys(self) -> Iterator[CallableParamKey]:
        yield from range(self.num_pos + self.has_variadic)
        yield from self.kwargs


def compare_callable_generality(
    a: Callable, b: Callable, *, schema: Schema
) -> int:
    """Return 1 if a is more general than b, -1 if a is more specific
    than b, and 0 if a and b are considered of equal generality."""
    if a == b:
        return 0
    elif a.assignable_from(b, schema=schema):
        return 1
    elif b.assignable_from(a, schema=schema):
        return -1
    else:
        for a_param in a.params:
            b_param = b.param_map.get(a_param.key)
            if b_param is None:
                continue
            a_param_type = a_param.get_type(schema)
            b_param_type = b_param.get_type(schema)

            param_comparison = compare_type_generality(
                a_param_type, b_param_type, schema=schema
            )

            if param_comparison != 0:
                return param_comparison

        # If all compared parameters are equal, the one with fewer parameters
        # is more general (can accept more call patterns)
        if len(a.params) < len(b.params):
            return 1
        elif len(a.params) > len(b.params):
            return -1

    return 0


class _CallableSignatureMatcher:
    def __init__(
        self,
        *,
        left: _Callable_T,
        left_param_types: CallableParamTypeMap | None = None,
        right: _Callable_T,
        right_param_types: CallableParamTypeMap | None = None,
        schema: Schema,
    ) -> None:
        self._left = left
        self._right = right

        def _param_generics(
            param_key: CallableParamKey,
            *,
            genmap: CallableGenericPositions,
        ) -> dict[Indirection, Type] | None:
            if (pgenerics := genmap.get(param_key)) is not None:
                return {
                    path: gt
                    for gt, paths in pgenerics.items()
                    for path in paths
                }
            else:
                return None

        left_generics = left.generics(schema)
        right_generics = right.generics(schema)

        self._get_left_param_generics = functools.partial(
            _param_generics,
            genmap=left_generics,
        )
        self._get_right_param_generics = functools.partial(
            _param_generics,
            genmap=right_generics,
        )

        def _param_type(
            param: CallableParam,
            *,
            typemap: CallableParamTypeMap | None,
        ) -> Collection[Type | PyTypeName]:
            if typemap is not None:
                types = typemap.get(param.key)
                if types is None:
                    types = [param.get_type(schema)]
            else:
                types = [param.get_type(schema)]

            return types

        self._get_left_param_type = functools.partial(
            _param_type, typemap=left_param_types
        )
        self._get_right_param_type = functools.partial(
            _param_type, typemap=right_param_types
        )

        self._generic_bindings: dict[Type, Type] = {}
        self._schema = schema

    def is_param_compatible(
        self,
        key: CallableParamKey,
        *,
        two_way: bool = False,
        consider_py_inheritance: bool = False,
    ) -> bool:
        right_param = self._right.param_map[key]
        left_param = self._left.param_map.get(key)
        if left_param is None:
            return False
        left_types = self._get_left_param_type(left_param)
        right_types = self._get_right_param_type(right_param)
        left_generics = self._get_left_param_generics(left_param.key)

        return all(
            any(
                self._is_assignable_from(
                    left_type=left_type,
                    right_type=right_type,
                    left_generics=left_generics,
                    left_typemod=left_param.typemod,
                    right_typemod=right_param.typemod,
                    consider_py_inheritance=consider_py_inheritance,
                )
                for left_type in left_types
            )
            for right_type in right_types
        ) or (
            two_way
            and all(
                any(
                    self._is_assignable_from(
                        left_type=right_type,
                        right_type=left_type,
                        left_typemod=right_param.typemod,
                        right_typemod=left_param.typemod,
                        consider_py_inheritance=consider_py_inheritance,
                    )
                    for right_type in right_types
                )
                for left_type in left_types
            )
        )

    def is_param_overlapping(
        self,
        key: CallableParamKey,
        *,
        two_way: bool = False,
        consider_py_inheritance: bool = False,
        consider_optionality: bool = True,
    ) -> bool:
        right_param = self._right.param_map[key]
        left_param = self._left.param_map.get(key)
        if left_param is None:
            return False
        left_types = self._get_left_param_type(left_param)
        right_types = self._get_right_param_type(right_param)
        left_generics = self._get_left_param_generics(left_param.key)
        left_typemod = left_param.typemod
        right_typemod = right_param.typemod

        if (
            consider_optionality
            and left_typemod is TypeModifier.Optional
            and right_typemod is TypeModifier.Optional
        ):
            return True

        return any(
            self._is_assignable_from(
                left_type=left_type,
                right_type=right_type,
                left_generics=left_generics,
                left_typemod=left_typemod,
                right_typemod=right_typemod,
                consider_py_inheritance=consider_py_inheritance,
                proper_subtype=True,
            )
            for left_type in left_types
            for right_type in right_types
        ) or (
            two_way
            and any(
                self._is_assignable_from(
                    left_type=right_type,
                    right_type=left_type,
                    left_typemod=right_typemod,
                    right_typemod=left_typemod,
                    consider_py_inheritance=consider_py_inheritance,
                    proper_subtype=True,
                )
                for right_type in right_types
                for left_type in left_types
            )
        )

    def is_return_compatible(
        self,
        *,
        two_way: bool = False,
        consider_py_inheritance: bool = False,
    ) -> bool:
        return self._is_assignable_from(
            left_type=self._left.get_return_type(self._schema),
            right_type=self._right.get_return_type(self._schema),
            left_generics=self._get_left_param_generics("__return__"),
            left_typemod=self._left.return_typemod,
            right_typemod=self._right.return_typemod,
            two_way=two_way,
            consider_py_inheritance=consider_py_inheritance,
        )

    def _is_assignable_from(
        self,
        *,
        left_type: Type | PyTypeName,
        right_type: Type | PyTypeName,
        left_generics: dict[Indirection, Type] | None = None,
        left_typemod: TypeModifier,
        right_typemod: TypeModifier,
        two_way: bool = False,
        consider_py_inheritance: bool = False,
        proper_subtype: bool = False,
    ) -> bool:
        if isinstance(left_type, Type):
            if isinstance(right_type, Type):
                return left_type.assignable_from(
                    right_type,
                    schema=self._schema,
                    generics=left_generics,
                    generic_bindings=self._generic_bindings,
                    proper_subtype=proper_subtype,
                ) or (
                    two_way
                    and right_type.assignable_from(
                        left_type,
                        schema=self._schema,
                        proper_subtype=proper_subtype,
                    )
                )
            else:
                return False
        else:
            if isinstance(right_type, Type):
                return False
            elif consider_py_inheritance:
                return self._is_strict_py_sub_type(
                    left_type=left_type,
                    right_type=right_type,
                    left_typemod=left_typemod,
                    right_typemod=right_typemod,
                ) or (
                    two_way
                    and self._is_strict_py_sub_type(
                        left_type=right_type,
                        right_type=left_type,
                        left_typemod=right_typemod,
                        right_typemod=left_typemod,
                    )
                )
            else:
                return left_type == right_type

    def _is_strict_py_sub_type(
        self,
        *,
        left_type: PyTypeName,
        right_type: PyTypeName,
        left_typemod: TypeModifier,
        right_typemod: TypeModifier,
    ) -> bool:
        if right_type == left_type:
            return False
        else:
            return issubclass(
                _get_py_type(right_type), _get_py_type(left_type)
            ) or (
                # bytes is a subclass of Sequence[int]
                left_type == ("builtins", "int")
                and left_typemod is TypeModifier.SetOf
                and right_type == ("builtins", "bytes")
            )


@sobject
class Callable(SchemaObject):
    return_type: TypeRef | Type
    return_typemod: TypeModifier
    params: list[CallableParam]

    @functools.cached_property
    def edgeql_signature(self) -> str:
        named_only = []
        positional = []
        variadic = []
        for param in self.params:
            if param.kind is CallableParamKind.NamedOnly:
                named_only.append(str(param))
            elif param.kind is CallableParamKind.Variadic:
                variadic.append(str(param))
            else:
                positional.append(str(param))

        params = ", ".join(positional + variadic + named_only)
        if self.return_typemod is TypeModifier.SetOf:
            ret = f"set of {self.return_type}"
        elif self.return_typemod is TypeModifier.Optional:
            ret = f"optional {self.return_type}"
        else:
            ret = f"{self.return_type}"
        return f"{self.name}({params}) -> {ret}"

    @functools.cached_property
    def ident(self) -> str:
        return self.schemapath.name

    @functools.cached_property
    def param_map(self) -> Mapping[CallableParamKey, CallableParam]:
        return {p.key: p for p in self.params}

    @functools.cached_property
    def signature(self) -> CallableSignature:
        """Callable signature.

        Returns a 4-tuple containing the callable's loxal identifier, the
        number of positional parameters, a boolean indicating the presence if
        a variadic argument and a frozenset of names of named-only parameters.
        """
        if not self.params:
            # Short-circuit for parameterless functions.
            return CallableSignature(
                name=self.ident,
                num_pos=0,
                num_pos_with_defaults=0,
                has_variadic=False,
                kwargs=frozenset(),
                kwargs_with_defaults=frozenset(),
            )
        else:
            num_pos = 0
            num_pos_with_defaults = 0
            kwargs = set()
            kwargs_with_defaults = set()
            has_variadic = False

            for p in self.params:
                if p.kind is CallableParamKind.Positional:
                    num_pos += 1
                    if p.default is not None:
                        num_pos_with_defaults += 1
                elif p.kind is CallableParamKind.NamedOnly:
                    kwargs.add(p.name)
                    if p.default is not None:
                        kwargs_with_defaults.add(p.name)
                elif p.kind is CallableParamKind.Variadic:
                    has_variadic = True
                else:
                    raise AssertionError(f"unexpected param kind: {p.kind}")

            return CallableSignature(
                name=self.ident,
                num_pos=num_pos,
                num_pos_with_defaults=num_pos_with_defaults,
                has_variadic=has_variadic,
                kwargs=frozenset(kwargs),
                kwargs_with_defaults=frozenset(kwargs_with_defaults),
            )

    def get_return_type(self, schema: Schema) -> Type:
        t = self.return_type
        return schema[t.id] if isinstance(t, TypeRef) else t

    def generics(self, schema: Mapping[str, Type]) -> CallableGenericPositions:
        """Return generic types that appear in multiple positions across
        callable's parameters and return type.  See comment on
        CallableGenericPositions about the structure of the return type.
        """
        result = getattr(self, "_generic_params", None)
        if result is not None:
            return result  # type: ignore [no-any-return]

        # Track positions of each generic type
        type_positions: defaultdict[
            Type, dict[CallableParamKey, Indirection]
        ] = defaultdict(dict)

        ret_type = self.get_return_type(schema)
        for gt, paths in ret_type.contained_generics(schema).items():
            type_positions[gt]["__return__"] = min(paths, key=len)

        for param in self.params:
            param_type = param.get_type(schema)
            for gt, paths in param_type.contained_generics(schema).items():
                idx: CallableParamKey
                if param.kind is CallableParamKind.NamedOnly:
                    idx = param.name
                else:
                    idx = param.index
                type_positions[gt][idx] = min(paths, key=len)

        generic_params: defaultdict[
            int | str,
            defaultdict[Type, set[Indirection]],
        ] = defaultdict(lambda: defaultdict(set))
        for typ, positions in type_positions.items():
            if len(positions) > 1:
                for position, path in positions.items():
                    generic_params[position][typ].add(path)

        object.__setattr__(self, "_generic_params", generic_params)  # noqa: PLC2801

        return generic_params

    def specialize(
        self,
        spec: Mapping[
            CallableParamKey, Mapping[Indirection, tuple[Type, Type]]
        ],
        *,
        schema: Mapping[str, Type],
    ) -> Self:
        return dataclasses.replace(
            self,
            id=str(uuid.uuid4()),
            params=[
                (
                    param.specialize(param_spec, schema=schema)
                    if (param_spec := spec.get(param.key))
                    else param
                )
                for param in self.params
            ],
            return_type=(
                self.get_return_type(schema).specialize(rspec, schema=schema)
                if (rspec := spec.get("__return__"))
                else self.return_type
            ),
        )

    def assignable_from(
        self,
        other: Self,
        *,
        param_types: CallableParamTypeMap | None = None,
        other_param_types: CallableParamTypeMap | None = None,
        param_getter: CallableParamGetter[Self] = operator.attrgetter(
            "params"
        ),
        schema: Mapping[str, Type],
        ignore_return: bool = False,
    ) -> bool:
        """Check if this callable subsumes (is more general than)
        *other* function.
        """
        # Self signature must contain other's signature.
        if not self.signature.contains(other.signature):
            return False

        sig_matcher = _CallableSignatureMatcher(
            left=self,
            right=other,
            left_param_types=param_types,
            right_param_types=other_param_types,
            schema=schema,
        )

        # Check each parameter: self must accept all params of other...
        if not all(
            sig_matcher.is_param_compatible(p.key) for p in param_getter(other)
        ):
            return False

        # Check if return types are compatible
        return ignore_return or sig_matcher.is_return_compatible()

    def overlaps(
        self,
        other: Self,
        *,
        param_types: CallableParamTypeMap | None = None,
        other_param_types: CallableParamTypeMap | None = None,
        param_getter: CallableParamGetter[Self] = operator.attrgetter(
            "params"
        ),
        schema: Mapping[str, Type],
        consider_py_inheritance: bool = False,
        consider_optionality: bool = True,
    ) -> bool:
        """Check if this callable overlaps the *other* callable.  Assumes
        that *self* appears _after_ *other* in the list of overloads.
        """
        # First quick check for type agnostic signature overlap.
        if not self.signature.overlaps(other.signature):
            return False

        sig_matcher = _CallableSignatureMatcher(
            left=self,
            right=other,
            left_param_types=param_types,
            right_param_types=other_param_types,
            schema=schema,
        )

        if sig_matcher.is_return_compatible():
            # Overloads with compatible return types are not considered
            # to be overlapping by type checkers.
            return False

        return all(
            sig_matcher.is_param_overlapping(
                param.key,
                two_way=True,
                consider_py_inheritance=consider_py_inheritance,
                consider_optionality=consider_optionality,
            )
            for param in param_getter(self)
            if (
                param.kind is CallableParamKind.Positional
                or param.kind is CallableParamKind.NamedOnly
            )
            and param.default is None
        )


_Callable_T = TypeVar("_Callable_T", bound="Callable")
CallableParamGetter = TypeAliasType(
    "CallableParamGetter",
    typing.Callable[[_Callable_T], Iterable[CallableParam]],
    type_params=(_Callable_T,),
)
