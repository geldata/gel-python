# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

from __future__ import annotations
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Generic,
    Final,
    TypeGuard,
    TypeVar,
    final,
)

from typing_extensions import Self

import dataclasses
import typing

from gel._internal import _edgeql
from gel._internal import _qb
from gel._internal._xmethod import hybridmethod

if TYPE_CHECKING:
    import types
    from collections.abc import Iterator


T = TypeVar("T")
T_co = TypeVar("T_co", covariant=True)


if TYPE_CHECKING:

    class GelTypeMeta(type):
        def __edgeql_qb_expr__(cls) -> _qb.Expr: ...

    class GelType(
        _qb.AbstractDescriptor,
        _qb.GelTypeMetadata,
        metaclass=GelTypeMeta,
    ):
        __gel_type_class__: ClassVar[type]

        def __edgeql_qb_expr__(self) -> _qb.Expr: ...

        @classmethod
        def __edgeql__(cls) -> tuple[type[Self], str]: ...

        @staticmethod
        def __edgeql_expr__() -> str: ...

else:
    GelTypeMeta = type

    class GelType(_qb.AbstractDescriptor, _qb.GelTypeMetadata):
        @hybridmethod
        def __edgeql_qb_expr__(self) -> _qb.Expr:
            if isinstance(self, type):
                return _qb.ExprPlaceholder()
            else:
                return self.__edgeql_literal__()

        def __edgeql_literal__(self) -> _qb.Literal:
            raise NotImplementedError(
                f"{type(self).__name__}.__edgeql_literal__"
            )

        @hybridmethod
        def __edgeql__(self) -> tuple[type, str]:
            if isinstance(self, type):
                raise NotImplementedError(f"{type(self).__name__}.__edgeql__")
            else:
                return type(self), _qb.toplevel_edgeql(self)


_GelType_T = TypeVar("_GelType_T", bound=GelType)


class GelTypeConstraint(Generic[_GelType_T]):
    pass


def is_gel_type(t: Any) -> TypeGuard[type[GelType]]:
    return isinstance(t, type) and issubclass(t, GelType)


if TYPE_CHECKING:

    class GelObjectTypeMeta(GelTypeMeta):
        __gel_pointer_infos__: ClassVar[dict[str, PointerInfo]]

        # Splat qb protocol
        def __iter__(cls) -> Iterator[_qb.ShapeElement]:  # noqa: N805
            ...
else:
    GelObjectTypeMeta = type


@dataclasses.dataclass(kw_only=True, frozen=True)
class PointerInfo:
    computed: bool = False
    readonly: bool = False
    has_props: bool = False
    cardinality: _edgeql.Cardinality = _edgeql.Cardinality.One
    annotation: type[Any] | None = None
    kind: _edgeql.PointerKind | None = None


class GelObjectType(
    GelType,
    _qb.GelObjectTypeMetadata,
    metaclass=GelObjectTypeMeta,
):
    __gel_variant__: ClassVar[str | None] = None
    """Auto-reflected model variant marker."""

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()
        cls.__gel_variant__ = None


def is_gel_object_type(t: Any) -> TypeGuard[type[GelObjectType]]:
    return isinstance(t, type) and issubclass(t, GelObjectType)


def maybe_collapse_object_type_variant_union(
    t: types.UnionType,
) -> type[GelObjectType] | None:
    """If *t* is a Union of GelObjectType reflections of the same object
    type, find and return the first union component that is a default
    variant."""
    default_variant: type[GelObjectType] | None = None
    typename = None
    for union_arg in typing.get_args(t):
        if not is_gel_object_type(union_arg):
            # Not an object type reflection union at all!
            return None
        if typename is None:
            typename = union_arg.__gel_reflection__.name
        elif typename != union_arg.__gel_reflection__.name:
            # Reflections of different object types, cannot collapse.
            return None
        if union_arg.__gel_variant__ == "Default" and default_variant is None:
            default_variant = union_arg

    return default_variant


@final
class DefaultValue:
    def __repr__(self) -> str:
        return "<DEFAULT_VALUE>"


DEFAULT_VALUE: Final = DefaultValue()
"""Sentinel value indicating that the object should use the default value
from the schema for a pointer on which this is set."""
