# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.


import typing

from typing import (
    Annotated,
    Any,
    ClassVar,
    ForwardRef,
    Literal,
    TypeGuard,
    TypeVar,
    Union,
    get_args,
    get_origin,
)
from typing import _GenericAlias, _SpecialGenericAlias  # type: ignore [attr-defined]  # noqa: PLC2701
from typing_extensions import TypeAliasType, TypeVarTuple, Unpack
from types import GenericAlias, UnionType

from typing_inspection.introspection import (
    AnnotationSource,
    InspectedAnnotation,
    inspect_annotation,
)


def is_classvar(t: Any) -> bool:
    return t is ClassVar or (is_generic_alias(t) and get_origin(t) is ClassVar)  # type: ignore [comparison-overlap]


def is_generic_alias(t: Any) -> TypeGuard[GenericAlias]:
    return isinstance(t, (GenericAlias, _GenericAlias, _SpecialGenericAlias))


def is_valid_type_arg(t: Any) -> bool:
    return isinstance(t, type) or (
        is_generic_alias(t) and get_origin(t) is not Unpack  # type: ignore [comparison-overlap]
    )


def is_type_alias(t: Any) -> TypeGuard[TypeAliasType]:
    return isinstance(t, TypeAliasType) and not is_generic_alias(t)


def is_type_var(t: Any) -> bool:
    return type(t) is TypeVar


if (TypingTypeVarTuple := getattr(typing, "TypeVarTuple", None)) is not None:

    def is_type_var_tuple(t: Any) -> bool:
        tt = type(t)
        return tt is TypeVarTuple or tt is TypingTypeVarTuple

    def is_type_var_or_tuple(t: Any) -> bool:
        tt = type(t)
        return tt is TypeVar or tt is TypeVarTuple or tt is TypingTypeVarTuple
else:

    def is_type_var_tuple(t: Any) -> bool:
        return type(t) is TypeVarTuple

    def is_type_var_or_tuple(t: Any) -> bool:
        tt = type(t)
        return tt is TypeVar or tt is TypeVarTuple


def is_type_var_tuple_unpack(t: Any) -> TypeGuard[GenericAlias]:
    return (
        is_generic_alias(t)
        and get_origin(t) is Unpack  # type: ignore [comparison-overlap]
        and is_type_var_tuple(get_args(t)[0])
    )


def is_type_var_or_tuple_unpack(t: Any) -> bool:
    return is_type_var(t) or is_type_var_tuple_unpack(t)


def is_generic_type_alias(t: Any) -> TypeGuard[GenericAlias]:
    return is_generic_alias(t) and isinstance(get_origin(t), TypeAliasType)


def is_annotated(t: Any) -> TypeGuard[Annotated[Any, ...]]:
    return is_generic_alias(t) and get_origin(t) is Annotated  # type: ignore [comparison-overlap]


def is_forward_ref(t: Any) -> TypeGuard[ForwardRef]:
    return isinstance(t, ForwardRef)


def is_union_type(t: Any) -> TypeGuard[UnionType]:
    return (
        (is_generic_alias(t) and get_origin(t) is Union)  # type: ignore [comparison-overlap]
        or isinstance(t, UnionType)
    )


def is_optional_type(t: Any) -> TypeGuard[UnionType]:
    return is_union_type(t) and type(None) in get_args(t)


def is_literal(t: Any) -> bool:
    return is_generic_alias(t) and get_origin(t) is Literal  # type: ignore [comparison-overlap]


__all__ = (
    "AnnotationSource",
    "InspectedAnnotation",
    "inspect_annotation",
    "is_annotated",
    "is_classvar",
    "is_forward_ref",
    "is_generic_alias",
    "is_generic_type_alias",
    "is_literal",
    "is_optional_type",
    "is_type_alias",
    "is_union_type",
)
