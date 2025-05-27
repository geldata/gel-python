# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.


from typing import (
    Annotated,
    Any,
    ClassVar,
    ForwardRef,
    TypeGuard,
    Union,
    get_args,
    get_origin,
)
from typing import _GenericAlias, _SpecialGenericAlias  # type: ignore [attr-defined]  # noqa: PLC2701
from typing_extensions import TypeAliasType
from types import GenericAlias, UnionType


def is_classvar(t: Any) -> bool:
    return t is ClassVar or (is_generic_alias(t) and get_origin(t) is ClassVar)  # type: ignore [comparison-overlap]


def is_generic_alias(t: Any) -> TypeGuard[GenericAlias]:
    return isinstance(t, (GenericAlias, _GenericAlias, _SpecialGenericAlias))


def is_type_alias(t: Any) -> TypeGuard[TypeAliasType]:
    return isinstance(t, TypeAliasType)


def is_annotated(t: Any) -> TypeGuard[Annotated[Any, ...]]:
    return is_generic_alias(t) and get_origin(t) is Annotated  # type: ignore [comparison-overlap]


def is_forward_ref(t: Any) -> TypeGuard[ForwardRef]:
    return isinstance(t, ForwardRef)


def is_union_type(t: Any) -> bool:
    return (
        (is_generic_alias(t) and get_origin(t) is Union)  # type: ignore [comparison-overlap]
        or isinstance(t, UnionType)
    )


def is_optional_type(t: Any) -> bool:
    return is_union_type(t) and type(None) in get_args(t)
