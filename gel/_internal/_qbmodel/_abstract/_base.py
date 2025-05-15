# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

from __future__ import annotations
from typing import TYPE_CHECKING, ClassVar, TypeVar


from gel._internal import _qb
from gel._internal._hybridmethod import hybridmethod


T = TypeVar("T")
T_co = TypeVar("T_co", covariant=True)
GelType_T = TypeVar("GelType_T", bound="GelType")


if TYPE_CHECKING:

    class GelTypeMeta(type):
        def __edgeql_qb_expr__(cls) -> _qb.Expr: ...

    class GelType(_qb.AbstractDescriptor, metaclass=GelTypeMeta):
        __gel_type_class__: ClassVar[type]

        def __edgeql_qb_expr__(self) -> _qb.Expr: ...

        @staticmethod
        def __edgeql__() -> tuple[type, str]: ...

        @staticmethod
        def __edgeql_expr__() -> str: ...

else:
    GelTypeMeta = type

    class GelType(_qb.AbstractDescriptor):
        __gel_type_class__: ClassVar[type]

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
