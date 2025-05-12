# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.


from typing import (
    Annotated,
    Any,
    ClassVar,
    TypeGuard,
    Union,
    get_args,
    get_origin,
)
from typing import _GenericAlias  # type: ignore
from types import GenericAlias, UnionType

from typing_extensions import ForwardRef, TypeAliasType


def is_classvar(t: Any) -> bool:
    return t is ClassVar or (is_generic_alias(t) and get_origin(t) is ClassVar)


def is_generic_alias(t: Any) -> bool:
    return isinstance(t, (GenericAlias, _GenericAlias))


def is_type_alias(t: Any) -> TypeGuard[TypeAliasType]:
    return isinstance(t, TypeAliasType)


def is_annotated(t: Any) -> TypeGuard[Annotated[Any, ...]]:
    return is_generic_alias(t) and get_origin(t) is Annotated


def is_forward_ref(t: Any) -> TypeGuard[ForwardRef]:
    return isinstance(t, ForwardRef)


def is_union_type(t: Any) -> bool:
    return (is_generic_alias(t) and get_origin(t) is Union) or isinstance(
        t, UnionType
    )


def is_optional_type(t: Any) -> bool:
    return is_union_type(t) and type(None) in get_args(t)
