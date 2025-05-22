# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

"""Abstract type definitions for the EdgeQL query builder"""

from __future__ import annotations
from typing import TYPE_CHECKING, Any
from typing_extensions import Self

import abc
from dataclasses import dataclass

from gel._internal import _edgeql
from gel._internal import _reflection

if TYPE_CHECKING:
    from collections.abc import Mapping, Set as AbstractSet


class Expr(abc.ABC):
    @abc.abstractproperty
    def precedence(self) -> _edgeql.Precedence: ...

    @abc.abstractproperty
    def type(self) -> _reflection.SchemaPath: ...

    @property
    def symbols(self) -> Mapping[str, Symbol]:
        return {}

    @property
    def symrefs(self) -> AbstractSet[Symbol]:
        return frozenset({})

    @abc.abstractmethod
    def __edgeql_expr__(self) -> str: ...

    def __edgeql_qb_expr__(self) -> Self:
        return self


@dataclass(kw_only=True, frozen=True)
class TypedExpr(Expr):
    type_: _reflection.SchemaPath

    @property
    def type(self) -> _reflection.SchemaPath:
        return self.type_


class IdentLikeExpr(TypedExpr):
    @property
    def precedence(self) -> _edgeql.Precedence:
        return _edgeql.PRECEDENCE[_edgeql.Token.IDENT]


class Symbol(IdentLikeExpr):
    pass


class AbstractDescriptor:
    pass


class AbstractFieldDescriptor(AbstractDescriptor):
    def get(self, owner: Any) -> Any:
        raise NotImplementedError(f"{type(self)}.get")
