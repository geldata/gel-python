# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

from __future__ import annotations
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Final,
    TypeGuard,
    TypeVar,
    final,
)

import dataclasses

from gel._internal import _edgeql
from gel._internal import _qb
from gel._internal._hybridmethod import hybridmethod

if TYPE_CHECKING:
    from collections.abc import Iterator


T = TypeVar("T")
T_co = TypeVar("T_co", covariant=True)
GelType_T = TypeVar("GelType_T", bound="GelType")


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

        @staticmethod
        def __edgeql__() -> tuple[type, str]: ...

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
    pass


@final
class DefaultValue:
    def __repr__(self) -> str:
        return "<DEFAULT_VALUE>"


DEFAULT_VALUE: Final = DefaultValue()
"""Sentinel value indicating that the object should use the default value
from the schema for a pointer on which this is set."""
