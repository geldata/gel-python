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
    """Import and return a Python type from its fully-qualified name.

    Args:
        name: A tuple of (module_name, type_name) identifying the type.

    Returns:
        The imported Python type.
    """
    mod = importlib.import_module(name[0])
    return getattr(mod, name[1])  # type: ignore [no-any-return]


@struct
class CallableParam:
    """Represents a parameter in a callable's signature.

    Contains all metadata about a parameter including its name, type,
    kind (positional/named-only/variadic), type modifiers, position,
    and default value if any.
    """

    name: str
    type: TypeRef | Type
    kind: CallableParamKind
    typemod: TypeModifier
    index: int
    default: str | None

    def __str__(self) -> str:
        """Return a human-readable string representation of the parameter."""
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
        """String representation of the parameter key for sorting purposes."""
        return str(self.key)

    @functools.cached_property
    def is_variadic(self) -> bool:
        """Whether this parameter accepts a variable number of arguments."""
        return self.kind is CallableParamKind.Variadic

    def get_type(self, schema: Schema) -> Type:
        """Resolve the parameter's type using the provided schema.

        Args:
            schema: The schema to resolve TypeRef objects against.

        Returns:
            The resolved Type object.
        """
        t = self.type
        return schema[t.id] if isinstance(t, TypeRef) else t

    def specialize(
        self,
        spec: Mapping[Indirection, tuple[Type, Type]],
        *,
        schema: Mapping[str, Type],
    ) -> Self:
        """Create a specialized version with concrete types for generics.

        Args:
            spec: Mapping from type indirections to (generic, concrete) pairs.
            schema: The schema containing type definitions.

        Returns:
            A new CallableParam with specialized types.
        """
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
    """Represents a callable's signature for type checking and overload
    resolution.

    Contains the callable's name, positional parameter counts, variadic
    parameter presence, and named-only parameter information needed for
    signature comparison and overlap detection.
    """

    name: str
    num_pos: int
    num_pos_with_defaults: int
    has_variadic: bool
    kwargs: frozenset[str]
    kwargs_with_defaults: frozenset[str]

    def sort_key(self) -> tuple[object, ...]:
        """Produce something that is safe to sort on

        Sets < is set inclusion, which is not something you can sort
        an array with!
        """
        return (
            self.name,
            self.num_pos,
            self.num_pos_with_defaults,
            self.has_variadic,
            sorted(self.kwargs),
            sorted(self.kwargs_with_defaults),
        )

    def contains(self, other: CallableSignature) -> bool:
        """Determine if this signature contains the other signature.

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
            other: The signature to check for containment.

        Returns:
            True if self can handle all calls that other can handle.
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
        """Determine if this signature overlaps with the other signature.

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
        """Iterate over all parameter keys in this signature.

        Yields positional parameter indices first (including variadic if
        present), then named-only parameter names.

        Returns:
            Iterator over parameter keys (int for positional, str for named).
        """
        yield from range(self.num_pos + self.has_variadic)
        yield from self.kwargs


def compare_callable_generality(
    a: Callable, b: Callable, *, schema: Schema
) -> int:
    """Compare the generality of two callables for overload resolution.

    Args:
        a: First callable to compare.
        b: Second callable to compare.
        schema: Schema containing type definitions.

    Returns:
        1 if a is more general than b, -1 if a is more specific than b,
        and 0 if a and b are considered of equal generality.
    """
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
    """Helper class for comparing callable signatures and parameters.

    Provides methods to check parameter compatibility, overlap, and return
    type compatibility between two callables, with support for generic
    type matching and Python inheritance checking.
    """

    def __init__(
        self,
        *,
        left: _Callable_T,
        left_param_types: CallableParamTypeMap | None = None,
        right: _Callable_T,
        right_param_types: CallableParamTypeMap | None = None,
        schema: Schema,
    ) -> None:
        """Initialize signature matcher for two callables.

        Args:
            left: Left callable in comparison.
            left_param_types: Optional type map override for left callable.
            right: Right callable in comparison.
            right_param_types: Optional type map override for right callable.
            schema: Schema containing type definitions.
        """
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
        """Check if parameters at the given key are type-compatible.

        Args:
            key: Parameter key to check compatibility for.
            two_way: If True, check compatibility in both directions.
            consider_py_inheritance: If True, consider Python inheritance.

        Returns:
            True if the parameters are compatible.
        """
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
        """Check if parameters at the given key have overlapping types.

        Args:
            key: Parameter key to check overlap for.
            two_way: If True, check overlap in both directions.
            consider_py_inheritance: If True, consider Python inheritance.
            consider_optionality: If True, treat optional params as overlapping

        Returns:
            True if the parameters have overlapping types.
        """
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
        """Check if return types of both callables are compatible.

        Args:
            two_way: If True, check compatibility in both directions.
            consider_py_inheritance: If True, consider Python inheritance.

        Returns:
            True if return types are compatible.
        """
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
        """Check if left type can be assigned from right type.

        Args:
            left_type: The target type for assignment.
            right_type: The source type for assignment.
            left_generics: Generic type bindings for the left type.
            left_typemod: Type modifier for the left type.
            right_typemod: Type modifier for the right type.
            two_way: If True, also check assignability in reverse direction.
            consider_py_inheritance: If True, consider Python inheritance.
            proper_subtype: If True, require proper subtype relationship.

        Returns:
            True if assignment is valid.
        """
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
        """Check if right_type is a strict Python subtype of left_type.

        Args:
            left_type: The supertype to check against.
            right_type: The subtype candidate.
            left_typemod: Type modifier for the left type.
            right_typemod: Type modifier for the right type.

        Returns:
            True if right_type is a strict subtype of left_type.
        """
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
    """Represents a callable schema object (function, operator, etc.).

    Contains the complete signature including parameters, return type,
    and type modifiers. Provides methods for signature analysis,
    specialization, and compatibility checking.
    """

    return_type: TypeRef | Type
    return_typemod: TypeModifier
    params: list[CallableParam]

    @functools.cached_property
    def edgeql_signature(self) -> str:
        """Return the EdgeQL signature representation of this callable."""
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
        """Return the local identifier (name) of this callable."""
        return self.schemapath.name

    @functools.cached_property
    def param_map(self) -> Mapping[CallableParamKey, CallableParam]:
        """Return a mapping from parameter keys to CallableParam objects."""
        return {p.key: p for p in self.params}

    @functools.cached_property
    def signature(self) -> CallableSignature:
        """Extract and return the callable's signature information.

        Returns:
            CallableSignature containing the callable's local identifier,
            positional parameter counts, variadic parameter presence, and
            named-only parameter information.
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
        """Resolve and return the callable's return type.

        Args:
            schema: The schema to resolve TypeRef objects against.

        Returns:
            The resolved return Type object.
        """
        t = self.return_type
        return schema[t.id] if isinstance(t, TypeRef) else t

    def generics(self, schema: Mapping[str, Type]) -> CallableGenericPositions:
        """Extract generic type positions across callable parameters and return
        type.

        Analyzes the callable to find generic types that appear in multiple
        positions, which is essential for proper generic type inference and
        specialization.

        Args:
            schema: Mapping from type IDs to Type objects for resolution.

        Returns:
            Mapping from parameter keys to generic type positions within
            those parameters. See CallableGenericPositions documentation
            for detailed structure information.
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
        """Create a specialized version with concrete types for generics.

        Args:
            spec: Mapping from parameter keys to type specializations.
            schema: Schema containing type definitions.

        Returns:
            A new Callable with specialized parameter and return types.
        """
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
        """Check if this callable can be assigned from another callable.

        Determines if this callable is more general than the other callable,
        meaning it can handle all call patterns that the other callable
        accepts.

        Args:
            other: The callable to check assignability from.
            param_types: Optional type override map for this callable's params.
            other_param_types: Optional type override map for other's params.
            param_getter: Function to extract parameters from callables.
            schema: Schema containing type definitions for resolution.
            ignore_return: If True, skip return type compatibility checking.

        Returns:
            True if this callable can be assigned from the other callable.
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
        """Check if this callable overlaps with another callable.

        Determines if there exists at least one call pattern that would
        match both callables. Used for overload validation where overlapping
        overloads may cause ambiguity.

        Args:
            other: The callable to check for overlap with.
            param_types: Optional type override map for this callable's params.
            other_param_types: Optional type override map for other's params.
            param_getter: Function to extract parameters from callables.
            schema: Schema containing type definitions for resolution.
            consider_py_inheritance: If True, consider Python inheritance.
            consider_optionality: If True, treat optional parameters as
                                  overlapping.

        Returns:
            True if the callables have overlapping call patterns.
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
