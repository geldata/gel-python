# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

"""EdgeQL query builder expressions"""

from __future__ import annotations

from typing import (
    TYPE_CHECKING,
)

import itertools
import textwrap
from dataclasses import dataclass, field

from gel._internal import _edgeql
from gel._internal import _reflection
from gel._internal._reflection import SchemaPath

from ._abstract import (
    Expr,
    IdentLikeExpr,
    ImplicitIteratorStmt,
    IteratorExpr,
    Node,
    PathPrefix,
    ScopeContext,
    Stmt,
    Symbol,
    TypedExpr,
)
from ._protocols import ExprCompatible, edgeql, edgeql_qb_expr

if TYPE_CHECKING:
    import decimal
    from collections.abc import Callable, Iterable


class ExprPlaceholder(Expr):
    def subnodes(self) -> Iterable[Node]:
        return ()

    @property
    def precedence(self) -> _edgeql.Precedence:
        raise TypeError("unreplaced ExprPlaceholder")

    @property
    def type(self) -> SchemaPath:
        raise TypeError("unreplaced ExprPlaceholder")

    def __edgeql_expr__(self, *, ctx: ScopeContext | None) -> str:
        raise TypeError("unreplaced ExprPlaceholder")


@dataclass(kw_only=True, frozen=True)
class Ident(IdentLikeExpr):
    name: str

    def __edgeql_expr__(self, *, ctx: ScopeContext | None) -> str:
        return _edgeql.quote_ident(self.name)


@dataclass(kw_only=True, frozen=True)
class Variable(Symbol):
    def __edgeql_expr__(self, *, ctx: ScopeContext | None) -> str:
        name = ctx.bindings.get(self) if ctx is not None else None
        if name is None:
            raise RuntimeError(f"unbound {self}")
        return _edgeql.quote_ident(name)


@dataclass(kw_only=True, frozen=True)
class SchemaSet(IdentLikeExpr):
    type_: _reflection.SchemaPath

    def __edgeql_expr__(self, *, ctx: ScopeContext | None) -> str:
        return "::".join(self.type.parts)


class Literal(IdentLikeExpr):
    pass


@dataclass(kw_only=True, frozen=True)
class BoolLiteral(Literal):
    val: bool
    type_: SchemaPath = field(default=SchemaPath("std", "bool"))

    def __edgeql_expr__(self, *, ctx: ScopeContext | None) -> str:
        return "true" if self.val else "false"


@dataclass(kw_only=True, frozen=True)
class IntLiteral(Literal):
    val: int
    type_: SchemaPath = field(default=SchemaPath("std", "int64"))

    def __edgeql_expr__(self, *, ctx: ScopeContext | None) -> str:
        return str(self.val)


@dataclass(kw_only=True, frozen=True)
class FloatLiteral(Literal):
    val: float
    type_: SchemaPath = field(default=SchemaPath("std", "float64"))

    def __edgeql_expr__(self, *, ctx: ScopeContext | None) -> str:
        return str(self.val)


@dataclass(kw_only=True, frozen=True)
class BigIntLiteral(Literal):
    val: int
    type_: SchemaPath = field(default=SchemaPath("std", "bigint"))

    def __edgeql_expr__(self, *, ctx: ScopeContext | None) -> str:
        return f"n{self.val}"


@dataclass(kw_only=True, frozen=True)
class DecimalLiteral(Literal):
    val: decimal.Decimal
    type_: SchemaPath = field(default=SchemaPath("std", "decimal"))

    def __edgeql_expr__(self, *, ctx: ScopeContext | None) -> str:
        return f"n{self.val}"


@dataclass(kw_only=True, frozen=True)
class BytesLiteral(Literal):
    val: bytes
    type_: SchemaPath = field(default=SchemaPath("std", "bytes"))

    def __edgeql_expr__(self, *, ctx: ScopeContext | None) -> str:
        v = _edgeql.quote_literal(repr(self.val)[2:-1])
        return f"b{v}"


@dataclass(kw_only=True, frozen=True)
class StringLiteral(Literal):
    val: str
    type_: SchemaPath = field(default=SchemaPath("std", "str"))

    def __edgeql_expr__(self, *, ctx: ScopeContext | None) -> str:
        return _edgeql.quote_literal(self.val)


@dataclass(kw_only=True, frozen=True)
class SetLiteral(TypedExpr):
    items: list[Expr]

    def subnodes(self) -> Iterable[Node]:
        return self.items

    @property
    def precedence(self) -> _edgeql.Precedence:
        return _edgeql.PRECEDENCE[_edgeql.Token.LBRACE]

    def __edgeql_expr__(self, *, ctx: ScopeContext | None) -> str:
        exprs = []
        for item in self.items:
            item_edgeql = edgeql(item, ctx=ctx)
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

    def subnodes(self) -> Iterable[Node]:
        return (self.source,)

    def compute_must_bind_refs(
        self, subnodes: Iterable[Node | None]
    ) -> Iterable[Symbol]:
        if isinstance(self.source, PathPrefix):
            return ()
        else:
            return self.source.visible_must_bind_refs

    @property
    def precedence(self) -> _edgeql.Precedence:
        return _edgeql.PRECEDENCE[_edgeql.Operation.PATH]

    def __edgeql_expr__(self, *, ctx: ScopeContext | None) -> str:
        steps = []
        current: Expr = self
        while isinstance(current, Path):
            steps.append(current.name)
            current = current.source

        steps.append(edgeql(current, ctx=ctx))

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

    def subnodes(self) -> Iterable[Node]:
        return (self.expr,)

    def __edgeql_expr__(self, *, ctx: ScopeContext | None) -> str:
        left = edgeql(self.expr, ctx=ctx)
        if _need_right_parens(self.precedence, self.expr):
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

    def subnodes(self) -> Iterable[Node]:
        return (self.lexpr, self.rexpr)

    def __edgeql_expr__(self, *, ctx: ScopeContext | None) -> str:
        left = edgeql(self.lexpr, ctx=ctx)
        if _need_left_parens(self.precedence, self.lexpr):
            left = f"({left})"
        right = edgeql(self.rexpr, ctx=ctx)
        if _need_right_parens(self.precedence, self.rexpr):
            right = f"({right})"
        return f"{left} {self.op} {right}"


@dataclass(kw_only=True, frozen=True)
class FuncCall(TypedExpr):
    fname: str
    args: list[Expr]
    kwargs: dict[str, Expr]

    def __init__(
        self,
        *,
        fname: str,
        args: list[ExprCompatible],
        kwargs: dict[str, ExprCompatible],
        type_: SchemaPath,
    ) -> None:
        object.__setattr__(self, "fname", fname)
        object.__setattr__(self, "args", [edgeql_qb_expr(a) for a in args])
        object.__setattr__(
            self, "kwargs", {k: edgeql_qb_expr(v) for k, v in kwargs.items()}
        )
        super().__init__(type_=type_)

    def subnodes(self) -> Iterable[Node]:
        return itertools.chain(self.args, self.kwargs.values())

    @property
    def precedence(self) -> _edgeql.Precedence:
        return _edgeql.PRECEDENCE[_edgeql.Operation.CALL]

    def __edgeql_expr__(self, *, ctx: ScopeContext | None) -> str:
        args = []
        comma_prec = _edgeql.PRECEDENCE[_edgeql.Token.COMMA]
        for arg in self.args:
            arg_text = edgeql(arg, ctx=ctx)
            if _need_left_parens(comma_prec, arg):
                arg_text = f"({arg_text})"
            args.append(arg_text)
        for n, arg in self.kwargs.items():
            arg_text = edgeql(arg, ctx=ctx)
            if _need_left_parens(comma_prec, arg):
                arg_text = f"({arg_text})"
            args.append(f"{n} := {arg_text}")

        return f"{self.fname}({' '.join(args)})"


class Clause(Node):
    pass


@dataclass(kw_only=True, frozen=True)
class Filter(Clause):
    filters: list[Expr]

    def subnodes(self) -> Iterable[Node]:
        return self.filters

    @property
    def precedence(self) -> _edgeql.Precedence:
        return _edgeql.PRECEDENCE[_edgeql.Token.FILTER]

    def __edgeql_expr__(self, *, ctx: ScopeContext | None) -> str:
        fexpr = self.filters[0]
        for item in self.filters[1:]:
            fexpr = InfixOp(
                lexpr=fexpr,
                op=_edgeql.Token.AND,
                rexpr=item,
                type_=SchemaPath("std", "bool"),
            )
        return f"FILTER {edgeql(fexpr, ctx=ctx)}"


@dataclass(kw_only=True, frozen=True)
class OrderBy(Clause):
    directions: list[Expr]

    def subnodes(self) -> Iterable[Node]:
        return self.directions

    @property
    def precedence(self) -> _edgeql.Precedence:
        return _edgeql.PRECEDENCE[_edgeql.Token.ORDER_BY]

    def __edgeql_expr__(self, *, ctx: ScopeContext | None) -> str:
        dexpr = self.directions[0]
        for item in self.directions[1:]:
            dexpr = InfixOp(
                lexpr=dexpr,
                op=_edgeql.Token.THEN,
                rexpr=item,
                type_=SchemaPath("std", "bool"),
            )

        return f"ORDER BY {edgeql(dexpr, ctx=ctx)}"


@dataclass(kw_only=True, frozen=True)
class Limit(Clause):
    limit: Expr

    def subnodes(self) -> Iterable[Node]:
        return (self.limit,)

    @property
    def precedence(self) -> _edgeql.Precedence:
        return _edgeql.PRECEDENCE[_edgeql.Token.LIMIT]

    def __edgeql_expr__(self, *, ctx: ScopeContext | None) -> str:
        return f"LIMIT {edgeql(self.limit, ctx=ctx)}"


@dataclass(kw_only=True, frozen=True)
class Offset(Clause):
    offset: Expr

    def subnodes(self) -> Iterable[Node]:
        return (self.offset,)

    @property
    def precedence(self) -> _edgeql.Precedence:
        return _edgeql.PRECEDENCE[_edgeql.Token.OFFSET]

    def __edgeql_expr__(self, *, ctx: ScopeContext | None) -> str:
        return f"OFFSET {edgeql(self.offset, ctx=ctx)}"


@dataclass(kw_only=True, frozen=True)
class InsertStmt(Stmt, TypedExpr):
    stmt: _edgeql.Token = _edgeql.Token.INSERT
    shape: Shape | None = None

    def subnodes(self) -> Iterable[Node | None]:
        return (self.shape,)

    def _edgeql(self, ctx: ScopeContext) -> str:
        text = f"{self.stmt} {self.type.as_schema_name()}"
        if self.shape is not None:
            text = f"{text} {_render_shape(self.shape, None, ctx)}"
        return text


class IteratorStmt(ImplicitIteratorStmt):
    @property
    def precedence(self) -> _edgeql.Precedence:
        token = _edgeql.Token.FOR if self.self_ref is not None else self.stmt
        return _edgeql.PRECEDENCE[token]

    def _iteration_edgeql(self, ctx: ScopeContext) -> str:
        expr = self.iter_expr
        if isinstance(expr, ShapeOp):
            expr = expr.iter_expr
        expr_text = edgeql(expr, ctx=ctx)
        token = _edgeql.Token.IN if self.self_ref is not None else self.stmt
        prec = _edgeql.PRECEDENCE[token]
        if _need_right_parens(self.precedence, expr, rprec=prec):
            expr_text = f"({expr_text})"
        return expr_text

    def _edgeql(self, ctx: ScopeContext) -> str:
        iterable, body = self._edgeql_parts(ctx)
        if self.self_ref is not None and self.self_ref_must_bind:
            var = ctx.bindings.get(self.self_ref)
            if var is None:
                raise AssertionError(f"{self.self_ref} in {self} is unbound")
            parts = [
                _edgeql.Token.FOR,
                var,
                _edgeql.Token.IN,
                iterable,
                self.stmt,
                var,
                body,
            ]
        else:
            parts = [self.stmt, iterable, body]

        return " ".join(parts)


@dataclass(kw_only=True, frozen=True)
class SelectStmt(IteratorStmt):
    stmt: _edgeql.Token = _edgeql.Token.SELECT
    implicit: bool = False
    filter: Filter | None = None
    order_by: OrderBy | None = None
    limit: Limit | None = None
    offset: Offset | None = None

    @classmethod
    def wrap(
        cls,
        expr: Expr,
        *,
        new_stmt_if: Callable[[SelectStmt], bool] | None = None,
    ) -> SelectStmt:
        if not isinstance(expr, SelectStmt) or (
            new_stmt_if is not None and new_stmt_if(expr)
        ):
            kwargs = {}
            if isinstance(expr, ShapeOp):
                kwargs["body_scope"] = expr.scope
            expr = SelectStmt(iter_expr=expr, **kwargs)  # type: ignore [arg-type]

        return expr

    def subnodes(self) -> Iterable[Node | None]:
        expr = self.iter_expr
        expr_nodes: tuple[Node, ...]
        if isinstance(expr, ShapeOp):
            expr_nodes = (expr.iter_expr, expr.shape)
        else:
            expr_nodes = (expr,)
        return (
            *expr_nodes,
            self.filter,
            self.order_by,
            self.limit,
            self.offset,
        )

    def _body_edgeql(self, ctx: ScopeContext) -> str:
        parts = []
        expr = self.iter_expr
        if isinstance(expr, ShapeOp):
            parts.append(_render_shape(expr.shape, expr.iter_expr, ctx))
        if self.filter is not None:
            parts.append(edgeql(self.filter, ctx=ctx))
        if self.order_by is not None:
            parts.append(edgeql(self.order_by, ctx=ctx))

        return " ".join(parts)

    def _edgeql(self, ctx: ScopeContext) -> str:
        text = super()._edgeql(ctx)
        if self.limit is not None or self.offset is not None:
            if self.self_ref is not None and self.self_ref_must_bind:
                text = f"SELECT ({text})"
            if self.limit is not None:
                text += "\n" + edgeql(self.limit, ctx=ctx)
            if self.offset is not None:
                text += "\n" + edgeql(self.offset, ctx=ctx)

        return text


@dataclass(kw_only=True, frozen=True)
class UpdateStmt(IteratorStmt):
    stmt: _edgeql.Token = _edgeql.Token.UPDATE
    filter: Filter | None = None
    shape: Shape

    def subnodes(self) -> Iterable[Node | None]:
        return (
            self.iter_expr,
            self.filter,
            self.shape,
        )

    def _body_edgeql(self, ctx: ScopeContext) -> str:
        parts = []
        if self.filter is not None:
            parts.append(edgeql(self.filter, ctx=ctx))
        parts.extend((" SET ", _render_shape(self.shape, self.iter_expr, ctx)))
        return " ".join(parts)


@dataclass(kw_only=True, frozen=True)
class DeleteStmt(IteratorStmt):
    stmt: _edgeql.Token = _edgeql.Token.DELETE

    def subnodes(self) -> Iterable[Node]:
        return (self.iter_expr,)

    def _body_edgeql(self, ctx: ScopeContext) -> str:
        return ""


@dataclass(kw_only=True, frozen=True)
class ForStmt(IteratorExpr):
    stmt: _edgeql.Token = _edgeql.Token.FOR
    iter_expr: Expr
    body: Expr
    var: Variable = field(init=False, compare=False)

    def __post_init__(self) -> None:
        super().__post_init__()
        var = Variable(type_=self.iter_expr.type, scope=self.scope)
        object.__setattr__(self, "var", var)

    @property
    def type(self) -> _reflection.SchemaPath:
        return self.body.type

    def subnodes(self) -> Iterable[Node]:
        return (self.iter_expr, self.body)

    def _edgeql(self, ctx: ScopeContext | None) -> str:
        return (
            f"{self.stmt} {edgeql(self.var, ctx=ctx)} IN "
            f"({edgeql(self.iter_expr, ctx=ctx)})\n"
            f"UNION ({edgeql(self.body, ctx=ctx)})"
        )


@dataclass(kw_only=True, frozen=True)
class Shape(Node):
    elements: dict[str, Expr] = field(default_factory=dict)
    star_splat: bool = False
    doublestar_splat: bool = False

    def subnodes(self) -> Iterable[Node]:
        return self.elements.values()


def _render_shape(
    shape: Shape,
    source: Expr | None,
    ctx: ScopeContext | None,
) -> str:
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
            el_text = edgeql(assign, ctx=ctx)
        els.append(f"{el_text},")
    shape_text = "{\n" + textwrap.indent("\n".join(els), "  ") + "\n}"
    return shape_text


@dataclass(kw_only=True, frozen=True)
class ShapeOp(IteratorExpr):
    shape: Shape

    def subnodes(self) -> Iterable[Node]:
        return (self.iter_expr, self.shape)

    @property
    def type(self) -> _reflection.SchemaPath:
        return self.iter_expr.type

    @property
    def precedence(self) -> _edgeql.Precedence:
        return _edgeql.PRECEDENCE[_edgeql.Token.LBRACE]

    def _iteration_edgeql(self, ctx: ScopeContext) -> str:
        iteration = edgeql(self.iter_expr, ctx=ctx)
        if _need_left_parens(self.precedence, self.iter_expr):
            iteration = f"({iteration})"
        return iteration

    def _body_edgeql(self, ctx: ScopeContext) -> str:
        return _render_shape(self.shape, self.iter_expr, ctx)


def _need_left_parens(
    prod_prec: _edgeql.Precedence,
    lexpr: Expr,
    lprec: _edgeql.Precedence | None = None,
) -> bool:
    if isinstance(lexpr, IdentLikeExpr):
        return False
    left_prec = lprec.value if lprec is not None else lexpr.precedence.value
    self_prec = prod_prec.value
    self_assoc = prod_prec.assoc

    return left_prec < self_prec or (
        left_prec == self_prec and self_assoc is not _edgeql.Assoc.RIGHT
    )


def _need_right_parens(
    prod_prec: _edgeql.Precedence,
    rexpr: Expr,
    rprec: _edgeql.Precedence | None = None,
) -> bool:
    if isinstance(rexpr, IdentLikeExpr):
        return False
    right_prec = rprec.value if rprec is not None else rexpr.precedence.value
    self_prec = prod_prec.value
    self_assoc = prod_prec.assoc

    return right_prec < self_prec or (
        right_prec == self_prec and self_assoc is _edgeql.Assoc.RIGHT
    )


def toplevel_edgeql(x: ExprCompatible) -> str:
    expr = edgeql_qb_expr(x)
    if not isinstance(expr, Stmt):
        expr = SelectStmt.wrap(expr)
    return edgeql(expr, ctx=None)
