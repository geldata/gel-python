# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

"""EdgeQL query builder expressions"""

from __future__ import annotations

from typing import (
    TYPE_CHECKING,
)

import textwrap
from dataclasses import dataclass, field

from gel._internal import _edgeql
from gel._internal import _reflection
from gel._internal._reflection import SchemaPath

from ._abstract import (
    Expr,
    IdentLikeExpr,
    Node,
    ScopedExpr,
    Stmt,
    Symbol,
    TypedExpr,
)
from ._protocols import ExprCompatible, edgeql, edgeql_qb_expr

if TYPE_CHECKING:
    import decimal


class ExprPlaceholder(Expr):
    def compute_symrefs(self) -> frozenset[Symbol]:
        return frozenset()

    @property
    def precedence(self) -> _edgeql.Precedence:
        raise TypeError("unreplaced ExprPlaceholder")

    @property
    def type(self) -> SchemaPath:
        raise TypeError("unreplaced ExprPlaceholder")

    def __edgeql_expr__(self) -> str:
        raise TypeError("unreplaced ExprPlaceholder")


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

    def compute_symrefs(self) -> frozenset[Symbol]:
        symrefs: set[Symbol] = set()
        for item in self.items:
            symrefs.update(item.symrefs)
        return frozenset(symrefs)

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
class Path(TypedExpr):
    source: Expr
    name: str
    is_lprop: bool

    def compute_symrefs(self) -> frozenset[Symbol]:
        return self.source.symrefs

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
        object.__setattr__(self, "expr", edgeql_qb_expr(expr))
        super().__init__(op=op, type_=type_)

    def compute_symrefs(self) -> frozenset[Symbol]:
        return self.expr.symrefs

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
        object.__setattr__(self, "lexpr", edgeql_qb_expr(lexpr))
        object.__setattr__(self, "rexpr", edgeql_qb_expr(rexpr))
        super().__init__(op=op, type_=type_)

    def compute_symrefs(self) -> frozenset[Symbol]:
        return self.lexpr.symrefs | self.rexpr.symrefs

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

    def compute_symrefs(self) -> frozenset[Symbol]:
        symrefs: set[Symbol] = set()
        for arg in self.args:
            symrefs.update(arg.symrefs)
        for kwarg in self.kwargs.values():
            symrefs.update(kwarg.symrefs)
        return frozenset(symrefs)

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
class ClauseExpr(Expr):
    expr: Expr

    @property
    def type(self) -> _reflection.SchemaPath:
        return self.expr.type


class Clause(Node):
    pass


@dataclass(kw_only=True, frozen=True)
class Filter(Clause):
    filters: list[Expr]

    def compute_symrefs(self) -> frozenset[Symbol]:
        symrefs: set[Symbol] = set()
        for f in self.filters:
            symrefs.update(f.symrefs)
        return frozenset(symrefs)

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
class OrderBy(Clause):
    directions: list[Expr]

    def compute_symrefs(self) -> frozenset[Symbol]:
        symrefs: set[Symbol] = set()
        for d in self.directions:
            symrefs.update(d.symrefs)
        return frozenset(symrefs)

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
class Limit(Clause):
    limit: Expr

    def compute_symrefs(self) -> frozenset[Symbol]:
        return self.limit.symrefs

    @property
    def precedence(self) -> _edgeql.Precedence:
        return _edgeql.PRECEDENCE[_edgeql.Token.LIMIT]

    def __edgeql_expr__(self) -> str:
        return f"LIMIT {edgeql(self.limit)}"


@dataclass(kw_only=True, frozen=True)
class Offset(Clause):
    offset: Expr

    def compute_symrefs(self) -> frozenset[Symbol]:
        return self.offset.symrefs

    @property
    def precedence(self) -> _edgeql.Precedence:
        return _edgeql.PRECEDENCE[_edgeql.Token.OFFSET]

    def __edgeql_expr__(self) -> str:
        return f"OFFSET {edgeql(self.offset)}"


@dataclass(kw_only=True, frozen=True)
class SelectStmt(ClauseExpr, Stmt):
    stmt: _edgeql.Token = _edgeql.Token.SELECT
    var: str | None = None
    implicit: bool = False
    filter: Filter | None = None
    order_by: OrderBy | None = None
    limit: Limit | None = None
    offset: Offset | None = None

    def compute_symrefs(self) -> frozenset[Symbol]:
        symrefs: set[Symbol] = set()
        symrefs.update(self.node_refs(self.expr))
        symrefs.update(self.node_refs(self.filter))
        symrefs.update(self.node_refs(self.order_by))
        symrefs.update(self.node_refs(self.limit))
        symrefs.update(self.node_refs(self.offset))
        return frozenset(symrefs)

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
    shape: Shape | None = None

    def compute_symrefs(self) -> frozenset[Symbol]:
        return self.node_refs(self.shape)

    def __edgeql_expr__(self) -> str:
        text = f"{self.stmt} {self.type.as_schema_name()}"
        if self.shape is not None:
            text = f"{text} {_render_shape(self.shape, None)}"
        return text


@dataclass(kw_only=True, frozen=True)
class UpdateStmt(Stmt, ClauseExpr):
    stmt: _edgeql.Token = _edgeql.Token.UPDATE
    filter: Filter | None = None
    shape: Shape

    def compute_symrefs(self) -> frozenset[Symbol]:
        symrefs: set[Symbol] = set(self.node_refs(self.expr))
        symrefs.update(self.node_refs(self.filter))
        symrefs.update(self.node_refs(self.shape))
        return frozenset(symrefs)

    def __edgeql_expr__(self) -> str:
        expr = self.expr
        parts = [str(self.stmt)]
        expr_text = edgeql(expr)
        if _need_right_parens(self, expr):
            expr_text = f"({expr_text})"
        parts.append(expr_text)
        if self.filter is not None:
            parts.append(edgeql(self.filter))
        parts.extend((" SET ", _render_shape(self.shape, self.expr)))
        return " ".join(parts)


@dataclass(kw_only=True, frozen=True)
class DeleteStmt(Stmt, ClauseExpr):
    stmt: _edgeql.Token = _edgeql.Token.DELETE

    def compute_symrefs(self) -> frozenset[Symbol]:
        return self.node_refs(self.expr)

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

    def compute_symrefs(self) -> frozenset[Symbol]:
        symrefs: set[Symbol] = set(self.node_refs(self.expr))
        symrefs.update(self.node_refs(self.iter_expr))
        return frozenset(symrefs)

    def __edgeql_expr__(self) -> str:
        return (
            f"{self.stmt} {edgeql(self.var)} IN ({edgeql(self.iter_expr)})\n"
            f"UNION ({edgeql(self.expr)})"
        )


@dataclass(kw_only=True, frozen=True)
class PathPrefix(Symbol):
    def __edgeql_expr__(self) -> str:
        return ""


@dataclass(kw_only=True, frozen=True)
class Shape(Node):
    elements: dict[str, Expr] = field(default_factory=dict)
    star_splat: bool = False
    doublestar_splat: bool = False

    def compute_symrefs(self) -> frozenset[Symbol]:
        symrefs: set[Symbol] = set()
        for elem in self.elements.values():
            symrefs.update(elem.symrefs)
        return frozenset(symrefs)


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
class ShapeOp(ScopedExpr):
    expr: Expr
    shape: Shape

    def compute_symrefs(self) -> frozenset[Symbol]:
        return self.expr.symrefs | self.node_refs(self.shape)

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
