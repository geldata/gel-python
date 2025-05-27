# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

"""EdgeQL query builder expressions"""

from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Any,
    TypeVar,
    overload,
)
from typing_extensions import (
    Self,
)

import abc
import copy
import textwrap
import weakref
from dataclasses import dataclass, field

from gel._internal import _edgeql
from gel._internal import _reflection
from gel._internal._reflection import SchemaPath

from ._abstract import Expr, IdentLikeExpr, Symbol, TypedExpr
from ._protocols import ExprCompatible, edgeql, edgeql_qb_expr

if TYPE_CHECKING:
    import decimal


class ExprPlaceholder(Expr):
    @property
    def precedence(self) -> _edgeql.Precedence:
        raise TypeError("unreplaced ExprPlaceholder")

    @property
    def type(self) -> SchemaPath:
        raise TypeError("unreplaced ExprPlaceholder")

    def __edgeql_expr__(self) -> str:
        raise TypeError("unreplaced ExprPlaceholder")


class ShapedExpr(Expr):
    @abc.abstractproperty
    def shape(self) -> Shape | None: ...


@dataclass(kw_only=True, frozen=True)
class Ident(IdentLikeExpr):
    name: str

    def __edgeql_expr__(self) -> str:
        return _edgeql.quote_ident(self.name)


@dataclass(kw_only=True, frozen=True)
class Variable(Symbol):
    name: str

    def __edgeql_expr__(self) -> str:
        return _edgeql.quote_ident(self.name)


@dataclass(kw_only=True, frozen=True)
class SchemaSet(IdentLikeExpr):
    type_: _reflection.SchemaPath

    def __edgeql_expr__(self) -> str:
        return "::".join(self.type.parts)


class Literal(IdentLikeExpr):
    pass


@dataclass(kw_only=True, frozen=True)
class BoolLiteral(Literal):
    val: bool
    type_: SchemaPath = field(default=SchemaPath("std", "bool"))

    def __edgeql_expr__(self) -> str:
        return "true" if self.val else "false"


@dataclass(kw_only=True, frozen=True)
class IntLiteral(Literal):
    val: int
    type_: SchemaPath = field(default=SchemaPath("std", "int64"))

    def __edgeql_expr__(self) -> str:
        return str(self.val)


@dataclass(kw_only=True, frozen=True)
class FloatLiteral(Literal):
    val: float
    type_: SchemaPath = field(default=SchemaPath("std", "float64"))

    def __edgeql_expr__(self) -> str:
        return str(self.val)


@dataclass(kw_only=True, frozen=True)
class BigIntLiteral(Literal):
    val: int
    type_: SchemaPath = field(default=SchemaPath("std", "bigint"))

    def __edgeql_expr__(self) -> str:
        return f"n{self.val}"


@dataclass(kw_only=True, frozen=True)
class DecimalLiteral(Literal):
    val: decimal.Decimal
    type_: SchemaPath = field(default=SchemaPath("std", "decimal"))

    def __edgeql_expr__(self) -> str:
        return f"n{self.val}"


@dataclass(kw_only=True, frozen=True)
class BytesLiteral(Literal):
    val: bytes
    type_: SchemaPath = field(default=SchemaPath("std", "bytes"))

    def __edgeql_expr__(self) -> str:
        v = _edgeql.quote_literal(repr(self.val)[2:-1])
        return f"b{v}"


@dataclass(kw_only=True, frozen=True)
class StringLiteral(Literal):
    val: str
    type_: SchemaPath = field(default=SchemaPath("std", "str"))

    def __edgeql_expr__(self) -> str:
        return _edgeql.quote_literal(self.val)


@dataclass(kw_only=True, frozen=True)
class SetLiteral(TypedExpr):
    items: list[Expr]

    @property
    def precedence(self) -> _edgeql.Precedence:
        return _edgeql.PRECEDENCE[_edgeql.Token.LBRACE]

    def __edgeql_expr__(self) -> str:
        exprs = []
        for item in self.items:
            item_edgeql = edgeql(item)
            if self._need_parens(item):
                item_edgeql = f"({item_edgeql})"
            exprs.append(item_edgeql)

        return "{" + ", ".join(exprs) + "}"

    def _need_parens(self, item: Expr) -> bool:
        if isinstance(item, IdentLikeExpr):
            return False
        comma_prec = _edgeql.PRECEDENCE[_edgeql.Token.COMMA]
        return item.precedence.value < comma_prec.value


@dataclass(kw_only=True, frozen=True)
class Path(ShapedExpr, TypedExpr):
    source: Expr
    name: str
    is_lprop: bool

    @property
    def shape(self) -> Shape | None:
        if isinstance(self.source, ShapedExpr):
            return self.source.shape
        else:
            return None

    @property
    def precedence(self) -> _edgeql.Precedence:
        return _edgeql.PRECEDENCE[_edgeql.Operation.PATH]

    def __edgeql_expr__(self) -> str:
        steps = []
        current: Expr = self
        while isinstance(current, Path):
            steps.append(current.name)
            current = current.source

        steps.append(current.__edgeql_expr__())

        return ".".join(reversed(steps))


@dataclass(kw_only=True, frozen=True)
class Op(TypedExpr):
    op: _edgeql.Token

    def __init__(
        self,
        /,
        *,
        op: _edgeql.Token | str,
        type_: SchemaPath,
    ) -> None:
        super().__init__(type_=type_)
        if isinstance(op, str):
            op = _edgeql.Token.from_str(op)
        object.__setattr__(self, "op", op)

    @property
    def precedence(self) -> _edgeql.Precedence:
        return _edgeql.PRECEDENCE[self.op]


@dataclass(kw_only=True, frozen=True)
class PrefixOp(Op):
    expr: Expr

    def __init__(
        self,
        *,
        expr: ExprCompatible,
        op: _edgeql.Token | str,
        type_: SchemaPath,
    ) -> None:
        super().__init__(op=op, type_=type_)
        object.__setattr__(self, "expr", edgeql_qb_expr(expr))

    def __edgeql_expr__(self) -> str:
        left = edgeql(self.expr)
        if _need_right_parens(self, self.expr):
            left = f"({left})"
        return f"{self.op} {left}"


@dataclass(kw_only=True, frozen=True)
class InfixOp(Op):
    lexpr: Expr
    rexpr: Expr

    def __init__(
        self,
        *,
        lexpr: ExprCompatible,
        rexpr: ExprCompatible,
        op: _edgeql.Token | str,
        type_: SchemaPath,
    ) -> None:
        super().__init__(op=op, type_=type_)
        object.__setattr__(self, "lexpr", edgeql_qb_expr(lexpr))
        object.__setattr__(self, "rexpr", edgeql_qb_expr(rexpr))

    def __edgeql_expr__(self) -> str:
        left = edgeql(self.lexpr)
        if _need_left_parens(self, self.lexpr):
            left = f"({left})"
        right = edgeql(self.rexpr)
        if _need_right_parens(self, self.rexpr):
            right = f"({right})"
        return f"{left} {self.op} {right}"


@dataclass(kw_only=True, frozen=True)
class FuncCall(TypedExpr):
    fname: str
    args: list[Expr]
    kwargs: dict[str, Expr]

    @property
    def precedence(self) -> _edgeql.Precedence:
        return _edgeql.PRECEDENCE[_edgeql.Operation.CALL]

    def __edgeql_expr__(self) -> str:
        args = ", ".join(
            [
                *(edgeql(arg) for arg in self.args),
                *(f"{n} := {edgeql(v)}" for n, v in self.kwargs.items()),
            ]
        )

        return f"{self.fname}({args})"


@dataclass(kw_only=True, frozen=True)
class ClauseExpr(ShapedExpr):
    expr: Expr

    @property
    def type(self) -> _reflection.SchemaPath:
        return self.expr.type

    @property
    def shape(self) -> Shape | None:
        if isinstance(self.expr, ShapedExpr):
            return self.expr.shape
        else:
            return None


@dataclass(kw_only=True, frozen=True)
class Filter:
    filters: list[Expr]

    @property
    def precedence(self) -> _edgeql.Precedence:
        return _edgeql.PRECEDENCE[_edgeql.Token.FILTER]

    def __edgeql_expr__(self) -> str:
        fexpr = self.filters[0]
        for item in self.filters[1:]:
            fexpr = InfixOp(
                lexpr=fexpr,
                op=_edgeql.Token.AND,
                rexpr=item,
                type_=SchemaPath("std", "bool"),
            )
        return f"FILTER {edgeql(fexpr)}"


@dataclass(kw_only=True, frozen=True)
class OrderBy:
    directions: list[Expr]

    @property
    def precedence(self) -> _edgeql.Precedence:
        return _edgeql.PRECEDENCE[_edgeql.Token.ORDER_BY]

    def __edgeql_expr__(self) -> str:
        dexpr = self.directions[0]
        for item in self.directions[1:]:
            dexpr = InfixOp(
                lexpr=dexpr,
                op=_edgeql.Token.THEN,
                rexpr=item,
                type_=SchemaPath("std", "bool"),
            )

        return f"ORDER BY {edgeql(dexpr)}"


@dataclass(kw_only=True, frozen=True)
class Limit:
    limit: Expr

    @property
    def precedence(self) -> _edgeql.Precedence:
        return _edgeql.PRECEDENCE[_edgeql.Token.LIMIT]

    def __edgeql_expr__(self) -> str:
        return f"LIMIT {edgeql(self.limit)}"


@dataclass(kw_only=True, frozen=True)
class Offset:
    offset: Expr

    @property
    def precedence(self) -> _edgeql.Precedence:
        return _edgeql.PRECEDENCE[_edgeql.Token.OFFSET]

    def __edgeql_expr__(self) -> str:
        return f"OFFSET {edgeql(self.offset)}"


class Scope:
    stmt: weakref.ref[Stmt]

    def __init__(self, stmt: Stmt | None = None) -> None:
        if stmt is not None:
            self.stmt = weakref.ref(stmt)


_T = TypeVar("_T")


class ScopeDescriptor:
    def __set_name__(self, owner: type[Any], name: str) -> None:
        self._name = "_" + name

    @overload
    def __get__(self, instance: None, owner: type[_T]) -> Self: ...

    @overload
    def __get__(self, instance: _T, owner: type[_T]) -> Scope: ...

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
        value: Scope | Self,
    ) -> None:
        if isinstance(value, Scope):
            object.__setattr__(obj, self._name, value)


@dataclass(kw_only=True, frozen=True)
class Stmt(Expr):
    stmt: _edgeql.Token
    scope: ScopeDescriptor = ScopeDescriptor()
    aliases: dict[str, Expr] = field(default_factory=dict)

    @property
    def precedence(self) -> _edgeql.Precedence:
        return _edgeql.PRECEDENCE[self.stmt]

    def derive(self) -> Self:
        return copy.copy(self)


@dataclass(kw_only=True, frozen=True)
class SelectStmt(ClauseExpr, Stmt):
    stmt: _edgeql.Token = _edgeql.Token.SELECT
    var: str | None = None
    implicit: bool = False
    filter: Filter | None = None
    order_by: OrderBy | None = None
    limit: Limit | None = None
    offset: Offset | None = None

    def __edgeql_expr__(self) -> str:
        expr = self.expr
        parts = [str(self.stmt)]
        expr_text = edgeql(expr)
        if _need_right_parens(self, expr):
            expr_text = f"({expr_text})"
        parts.append(expr_text)
        if self.filter is not None:
            parts.append(edgeql(self.filter))
        if self.order_by is not None:
            parts.append(edgeql(self.order_by))
        if self.limit is not None:
            parts.append(edgeql(self.limit))
        if self.offset is not None:
            parts.append(edgeql(self.offset))

        return " ".join(parts)


@dataclass(kw_only=True, frozen=True)
class InsertStmt(Stmt, TypedExpr):
    stmt: _edgeql.Token = _edgeql.Token.INSERT
    shape_: Shape | None = None

    @property
    def shape(self) -> Shape | None:
        return self.shape_

    def __edgeql_expr__(self) -> str:
        text = f"{self.stmt} {self.type.as_schema_name()}"
        if self.shape_:
            text = f"{text} {_render_shape(self.shape_, None)}"
        return text


@dataclass(kw_only=True, frozen=True)
class UpdateStmt(Stmt, ClauseExpr):
    stmt: _edgeql.Token = _edgeql.Token.UPDATE
    filter: Filter | None = None
    shape_: Shape

    @property
    def shape(self) -> Shape:
        return self.shape_

    def __edgeql_expr__(self) -> str:
        expr = self.expr
        parts = [str(self.stmt)]
        expr_text = edgeql(expr)
        if _need_right_parens(self, expr):
            expr_text = f"({expr_text})"
        parts.append(expr_text)
        if self.filter is not None:
            parts.append(edgeql(self.filter))
        parts.extend((" SET ", _render_shape(self.shape_, self.expr)))
        return " ".join(parts)


@dataclass(kw_only=True, frozen=True)
class DeleteStmt(Stmt, ClauseExpr):
    stmt: _edgeql.Token = _edgeql.Token.DELETE

    def __edgeql_expr__(self) -> str:
        expr = self.expr
        expr_text = edgeql(expr)
        if _need_right_parens(self, expr):
            expr_text = f"({expr_text})"
        return f"{self.stmt} {expr_text}"


@dataclass(kw_only=True, frozen=True)
class ForStmt(Stmt, ClauseExpr):
    stmt: _edgeql.Token = _edgeql.Token.FOR
    iter_expr: Expr
    var: Variable

    def __edgeql_expr__(self) -> str:
        return (
            f"{self.stmt} {edgeql(self.var)} IN ({edgeql(self.iter_expr)})\n"
            f"UNION ({edgeql(self.expr)})"
        )


@dataclass(kw_only=True, frozen=True)
class PathPrefix(Symbol):
    scope: Scope

    def __edgeql_expr__(self) -> str:
        return ""


@dataclass(kw_only=True, frozen=True)
class Shape:
    elements: dict[str, Expr] = field(default_factory=dict)
    star_splat: bool = False
    doublestar_splat: bool = False


def _render_shape(shape: Shape, source: Expr | None) -> str:
    els = []
    if shape.star_splat:
        els.append("*")
    if shape.doublestar_splat:
        els.append("**")
    for n, el in shape.elements.items():
        if (
            source is not None
            and isinstance(el, Path)
            and isinstance(el.source, (SchemaSet, PathPrefix))
            and el.source.type == source.type
            and el.name == n
        ):
            el_text = _edgeql.quote_ident(n)
        else:
            assign = InfixOp(
                lexpr=Ident(name=n, type_=el.type),
                op=_edgeql.Token.ASSIGN,
                rexpr=el,
                type_=el.type,
            )
            el_text = edgeql(assign)
        els.append(f"{el_text},")
    shape_text = "{\n" + textwrap.indent("\n".join(els), "  ") + "\n}"
    return shape_text


@dataclass(kw_only=True, frozen=True)
class ShapeOp(ShapedExpr):
    expr: Expr
    shape_: Shape
    scope: Scope = field(default_factory=Scope)

    @property
    def shape(self) -> Shape:
        return self.shape_

    @property
    def type(self) -> _reflection.SchemaPath:
        return self.expr.type

    @property
    def precedence(self) -> _edgeql.Precedence:
        return _edgeql.PRECEDENCE[_edgeql.Token.LBRACE]

    def __edgeql_expr__(self) -> str:
        subject = edgeql(self.expr)
        if _need_left_parens(self, self.expr):
            subject = f"({subject})"
        shape_text = _render_shape(self.shape, self.expr)
        return f"{subject} {shape_text}"


def _need_left_parens(prod: Expr, lexpr: Expr) -> bool:
    if isinstance(lexpr, IdentLikeExpr):
        return False
    left_prec = lexpr.precedence.value
    self_prec = prod.precedence.value
    self_assoc = prod.precedence.assoc

    return left_prec < self_prec or (
        left_prec == self_prec and self_assoc is not _edgeql.Assoc.RIGHT
    )


def _need_right_parens(prod: Expr, rexpr: Expr) -> bool:
    if isinstance(rexpr, IdentLikeExpr):
        return False
    right_prec = rexpr.precedence.value
    self_prec = prod.precedence.value
    self_assoc = prod.precedence.assoc

    return right_prec < self_prec or (
        right_prec == self_prec and self_assoc is _edgeql.Assoc.RIGHT
    )
