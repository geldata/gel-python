# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

"""Abstract type definitions for the EdgeQL query builder"""

from __future__ import annotations
from typing import TYPE_CHECKING, Any, TypeVar, overload
from typing_extensions import Self

import abc
import weakref
from dataclasses import dataclass, field

from gel._internal import _edgeql
from gel._internal import _reflection

if TYPE_CHECKING:
    from collections.abc import Iterable


@dataclass(kw_only=True, frozen=True)
class Node(abc.ABC):
    symrefs: frozenset[Symbol] = field(
        default_factory=frozenset, init=False, compare=False
    )

    @abc.abstractmethod
    def compute_symrefs(self) -> frozenset[Symbol]: ...

    def __post_init__(self) -> None:
        object.__setattr__(self, "symrefs", self.compute_symrefs())


@dataclass(kw_only=True, frozen=True)
class Expr(Node):
    @abc.abstractproperty
    def precedence(self) -> _edgeql.Precedence: ...

    @abc.abstractproperty
    def type(self) -> _reflection.SchemaPath: ...

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

    def compute_symrefs(self) -> frozenset[Symbol]:
        return frozenset()


@dataclass(kw_only=True, frozen=True)
class Symbol(IdentLikeExpr):
    scope: Scope

    def compute_symrefs(self) -> frozenset[Symbol]:
        return frozenset((self,))


class Scope:
    stmt: weakref.ref[Stmt]

    def __init__(self, stmt: Stmt | None = None) -> None:
        if stmt is not None:
            self.stmt = weakref.ref(stmt)

    def __repr__(self) -> str:
        return f"<Scope at {id(self):0x}>"


DEFAULT_SCOPE = Scope()


_T = TypeVar("_T")


class ScopeDescriptor:
    def __set_name__(self, owner: type[Any], name: str) -> None:
        self._name = "_" + name

    @overload
    def __get__(self, instance: None, owner: type[_T]) -> Self: ...

    @overload
    def __get__(
        self, instance: _T, owner: type[_T] | None = None
    ) -> Scope: ...

    def __get__(
        self,
        instance: object | None,
        owner: type[Any] | None = None,
    ) -> Scope | Self:
        if instance is None:
            return self
        else:
            scope = getattr(instance, self._name, None)
            if scope is None:
                stmt = instance if isinstance(instance, Stmt) else None
                scope = Scope(stmt=stmt)
                object.__setattr__(instance, self._name, scope)
            return scope

    def __set__(
        self,
        obj: Any,
        value: Scope,
    ) -> None:
        if isinstance(value, Scope):
            object.__setattr__(obj, self._name, value)


@dataclass(kw_only=True, frozen=True)
class ScopedExpr(Expr):
    scope: ScopeDescriptor = ScopeDescriptor()

    def filter_refs(self, refs: Iterable[Symbol]) -> Iterable[Symbol]:
        """Remove refs that are in scope of this expression."""
        return (ref for ref in refs if ref.scope is not self.scope)

    def node_refs(self, node: Node | None) -> frozenset[Symbol]:
        """Remove refs that are in scope of this expression."""
        if node is None:
            return frozenset()
        else:
            return frozenset(
                ref for ref in node.symrefs if ref.scope is not self.scope
            )


@dataclass(kw_only=True, frozen=True)
class Stmt(ScopedExpr):
    stmt: _edgeql.Token
    aliases: dict[str, Expr] = field(default_factory=dict)

    @property
    def precedence(self) -> _edgeql.Precedence:
        return _edgeql.PRECEDENCE[self.stmt]


class AbstractDescriptor:
    pass


class AbstractFieldDescriptor(AbstractDescriptor):
    def get(self, owner: Any) -> Any:
        raise NotImplementedError(f"{type(self)}.get")
