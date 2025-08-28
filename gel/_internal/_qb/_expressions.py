# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

"""EdgeQL query builder expressions"""

from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    TypeVar,
)
from typing_extensions import Self

import textwrap
import typing
import weakref

from dataclasses import dataclass, field

from gel._internal import _edgeql
from gel._internal._polyfills import _strenum
from gel._internal._schemapath import SchemaPath

from ._abstract import (
    AtomicExpr,
    Expr,
    IdentLikeExpr,
    ImplicitIteratorStmt,
    IteratorExpr,
    Node,
    PathExpr,
    PathPrefix,
    Scope,
    ScopeContext,
    Stmt,
    Symbol,
    TypedExpr,
)
from ._protocols import (
    ExprCompatible,
    edgeql,
    edgeql_qb_expr,
)

if TYPE_CHECKING:
    import decimal
    from collections.abc import Callable, Iterable, Sequence

    from ._reflection import GelTypeMetadata


_T = TypeVar("_T")


class ExprPlaceholder(Expr):
    def subnodes(self) -> Iterable[Node]:
        return ()

    @property
    def precedence(self) -> _edgeql.Precedence:
        raise TypeError("unreplaced ExprPlaceholder")

    @property
    def type(self) -> SchemaPath:
        raise TypeError("unreplaced ExprPlaceholder")

    def __edgeql_expr__(self, *, ctx: ScopeContext) -> str:
        raise TypeError("unreplaced ExprPlaceholder")


@dataclass(kw_only=True, frozen=True)
class Ident(IdentLikeExpr):
    name: str

    def __edgeql_expr__(self, *, ctx: ScopeContext) -> str:
        return _edgeql.quote_ident(self.name)


@dataclass(kw_only=True, frozen=True)
class Variable(Symbol):
    def __edgeql_expr__(self, *, ctx: ScopeContext) -> str:
        name = ctx.bindings.get(self) if ctx is not None else None
        if name is None:
            raise RuntimeError(f"unbound {self}")
        return _edgeql.quote_ident(name)


@dataclass(kw_only=True, frozen=True)
class SchemaSet(IdentLikeExpr):
    type_: SchemaPath

    def __edgeql_expr__(self, *, ctx: ScopeContext) -> str:
        return "::".join(self.type.parts)


@dataclass(kw_only=True, frozen=True)
class Global(TypedExpr):
    name: SchemaPath
    type_: SchemaPath

    @property
    def precedence(self) -> _edgeql.Precedence:
        return _edgeql.PRECEDENCE[_edgeql.Token.GLOBAL]

    def subnodes(self) -> Iterable[Node]:
        return ()

    def __edgeql_expr__(self, *, ctx: ScopeContext) -> str:
        return f"global {self.name.as_quoted_schema_name()}"


@dataclass(kw_only=True, frozen=True)
class Literal(IdentLikeExpr):
    val: object

    def __edgeql_expr__(self, *, ctx: ScopeContext) -> str:
        var = ctx.bind_argument(self.val)
        return f"(<{self.type_}>${var})"


@dataclass(kw_only=True, frozen=True)
class BoolLiteral(Literal):
    val: bool
    type_: SchemaPath = field(default=SchemaPath("std", "bool"))

    def __edgeql_expr__(self, *, ctx: ScopeContext) -> str:
        # XXX: BoolLiteral uses our custom bool subtype??
        var = ctx.bind_argument(bool(self.val))
        return f"(<{self.type_}>${var})"


@dataclass(kw_only=True, frozen=True)
class IntLiteral(Literal):
    val: int
    type_: SchemaPath = field(default=SchemaPath("std", "int64"))


@dataclass(kw_only=True, frozen=True)
class FloatLiteral(Literal):
    val: float
    type_: SchemaPath = field(default=SchemaPath("std", "float64"))


@dataclass(kw_only=True, frozen=True)
class BigIntLiteral(Literal):
    val: int
    type_: SchemaPath = field(default=SchemaPath("std", "bigint"))


@dataclass(kw_only=True, frozen=True)
class DecimalLiteral(Literal):
    val: decimal.Decimal
    type_: SchemaPath = field(default=SchemaPath("std", "decimal"))


@dataclass(kw_only=True, frozen=True)
class BytesLiteral(Literal):
    val: bytes
    type_: SchemaPath = field(default=SchemaPath("std", "bytes"))


@dataclass(kw_only=True, frozen=True)
class StringLiteral(Literal):
    val: str
    type_: SchemaPath = field(default=SchemaPath("std", "str"))


@dataclass(kw_only=True, frozen=True)
class SetLiteral(AtomicExpr):
    items: tuple[Expr, ...]

    def __init__(
        self,
        /,
        *,
        items: Iterable[ExprCompatible],
        type_: SchemaPath,
    ) -> None:
        object.__setattr__(
            self,
            "items",
            tuple(edgeql_qb_expr(it) for it in items),
        )
        super().__init__(type_=type_)

    def subnodes(self) -> Iterable[Node]:
        return self.items

    @property
    def precedence(self) -> _edgeql.Precedence:
        return _edgeql.PRECEDENCE[_edgeql.Token.LBRACE]

    def __edgeql_expr__(self, *, ctx: ScopeContext) -> str:
        exprs = []
        for item in self.items:
            item_edgeql = edgeql(item, ctx=ctx)
            if self._need_parens(item):
                item_edgeql = f"({item_edgeql})"
            exprs.append(item_edgeql)

        return "{" + ", ".join(exprs) + "}"

    def _need_parens(self, item: Expr) -> bool:
        if isinstance(item, AtomicExpr):
            return False
        comma_prec = _edgeql.PRECEDENCE[_edgeql.Token.COMMA]
        return item.precedence.value < comma_prec.value


@dataclass(kw_only=True, frozen=True)
class Path(PathExpr):
    def __edgeql_expr__(self, *, ctx: ScopeContext) -> str:
        steps = []
        current: Expr = self
        while isinstance(current, Path):
            source = current.source
            if isinstance(source, PathPrefix) and source.lprop_pivot:
                step = f"@{_edgeql.quote_ident(current.name)}"
            else:
                step = f".{_edgeql.quote_ident(current.name)}"
            steps.append(step)
            current = source

        steps.append(edgeql(current, ctx=ctx))

        return "".join(reversed(steps))


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
        if not isinstance(op, _edgeql.Token):
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

    def __edgeql_expr__(self, *, ctx: ScopeContext) -> str:
        left = edgeql(self.expr, ctx=ctx)
        if _need_right_parens(self.precedence, self.expr):
            left = f"({left})"
        return f"{self.op} {left}"


@dataclass(kw_only=True, frozen=True)
class CastOp(PrefixOp):
    def __init__(
        self,
        *,
        expr: ExprCompatible,
        type_: SchemaPath,
    ) -> None:
        op = _edgeql.Token.RANGBRACKET
        super().__init__(expr=expr, op=op, type_=type_)

    @property
    def precedence(self) -> _edgeql.Precedence:
        return _edgeql.PRECEDENCE[_edgeql.Operation.RAW]

    def subnodes(self) -> Iterable[Node]:
        return (self.expr,)

    def __edgeql_expr__(self, *, ctx: ScopeContext) -> str:
        expr = edgeql(self.expr, ctx=ctx)
        if _need_right_parens(self.precedence, self.expr):
            expr = f"({expr})"
        return f"<{self.type.as_quoted_schema_name()}>{expr}"


def empty_set(type_: SchemaPath) -> CastOp:
    return CastOp(expr=SetLiteral(items=(), type_=type_), type_=type_)


def empty_set_if_none(val: _T | None, type_: SchemaPath) -> _T | CastOp:
    return empty_set(type_) if val is None else val


@dataclass(kw_only=True, frozen=True)
class BinaryOp(Op):
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


@dataclass(kw_only=True, frozen=True)
class InfixOp(BinaryOp):
    def __init__(
        self,
        *,
        lexpr: ExprCompatible,
        rexpr: ExprCompatible,
        op: _edgeql.Token | str,
        type_: SchemaPath,
    ) -> None:
        super().__init__(lexpr=lexpr, rexpr=rexpr, op=op, type_=type_)

    def __edgeql_expr__(self, *, ctx: ScopeContext) -> str:
        left = edgeql(self.lexpr, ctx=ctx)
        if _need_left_parens(self.precedence, self.lexpr):
            left = f"({left})"
        right = edgeql(self.rexpr, ctx=ctx)
        if _need_right_parens(self.precedence, self.rexpr):
            right = f"({right})"
        return f"{left} {self.op} {right}"


def construct_infix_op_chain(
    op_name: str, exprs: Sequence[ExprCompatible], type_: SchemaPath
) -> Expr:
    """
    Converts a sequence of n expressions into a chain of n-1 infix
    operations. Requires at least one expression.

    For example, `'AND', (a, b, c)` is converted into `a AND (b AND c)`.
    """
    rev = reversed(exprs)
    result: Expr = edgeql_qb_expr(next(rev))
    while True:
        e = next(rev, None)
        if e is None:
            break
        result = InfixOp(lexpr=e, op=op_name, rexpr=result, type_=type_)
    return result


@dataclass(kw_only=True, frozen=True)
class IndexOp(BinaryOp):
    def __init__(
        self,
        *,
        lexpr: ExprCompatible,
        rexpr: ExprCompatible,
        op: _edgeql.Token | str,
        type_: SchemaPath,
    ) -> None:
        super().__init__(
            lexpr=lexpr, rexpr=rexpr, op=_edgeql.Token.LBRACKET, type_=type_
        )

    def __edgeql_expr__(self, *, ctx: ScopeContext) -> str:
        left = edgeql(self.lexpr, ctx=ctx)
        if _need_left_parens(self.precedence, self.lexpr):
            left = f"({left})"
        right = edgeql(self.rexpr, ctx=ctx)
        if _need_right_parens(self.precedence, self.rexpr):
            right = f"({right})"
        return f"{left}[{right}]"


@dataclass(kw_only=True, frozen=True)
class FuncCall(TypedExpr):
    fname: str
    args: list[Expr] | None = None
    kwargs: dict[str, Expr] | None = None

    def __init__(
        self,
        *,
        fname: str,
        args: list[ExprCompatible] | None = None,
        kwargs: dict[str, ExprCompatible] | None = None,
        type_: SchemaPath,
    ) -> None:
        object.__setattr__(self, "fname", fname)
        if args is not None:
            object.__setattr__(self, "args", [edgeql_qb_expr(a) for a in args])
        else:
            object.__setattr__(self, "args", None)
        if kwargs is not None:
            object.__setattr__(
                self,
                "kwargs",
                {k: edgeql_qb_expr(v) for k, v in kwargs.items()},
            )
        else:
            object.__setattr__(self, "kwargs", None)
        super().__init__(type_=type_)

    def subnodes(self) -> Iterable[Node]:
        if self.args is not None:
            yield from self.args
        if self.kwargs is not None:
            yield from self.kwargs.values()

    @property
    def precedence(self) -> _edgeql.Precedence:
        return _edgeql.PRECEDENCE[_edgeql.Operation.CALL]

    def __edgeql_expr__(self, *, ctx: ScopeContext) -> str:
        args = []
        comma_prec = _edgeql.PRECEDENCE[_edgeql.Token.COMMA]
        if self.args is not None:
            for arg in self.args:
                arg_text = edgeql(arg, ctx=ctx)
                if _need_left_parens(comma_prec, arg):
                    arg_text = f"({arg_text})"
                args.append(arg_text)
        if self.kwargs is not None:
            for n, arg in self.kwargs.items():
                arg_text = edgeql(arg, ctx=ctx)
                if _need_left_parens(comma_prec, arg):
                    arg_text = f"({arg_text})"
                args.append(f"{n} := {arg_text}")

        return f"{self.fname}({', '.join(args)})"


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

    def __edgeql_expr__(self, *, ctx: ScopeContext) -> str:
        fexpr = self.filters[0]
        for item in self.filters[1:]:
            fexpr = InfixOp(
                lexpr=fexpr,
                op=_edgeql.Token.AND,
                rexpr=item,
                type_=SchemaPath("std", "bool"),
            )
        return f"FILTER {edgeql(fexpr, ctx=ctx)}"


class OrderDirection(_strenum.StrEnum):
    asc = "asc"
    desc = "desc"


class OrderEmptyDirection(_strenum.StrEnum):
    empty_first = "empty first"
    empty_last = "empty last"


Direction = typing.Literal["asc", "desc"]
EmptyDirection = typing.Literal["empty first", "empty last"]
OrderByExpr = (
    Expr
    | tuple[Expr, Direction | str]
    | tuple[Expr, Direction | str, EmptyDirection | str]
)


@dataclass(kw_only=True, frozen=True)
class OrderByElem(Expr):
    expr: Expr
    direction: OrderDirection | None = None
    empty_direction: OrderEmptyDirection | None = None

    def subnodes(self) -> Iterable[Node]:
        return (self.expr,)

    @property
    def type(self) -> SchemaPath:
        return self.expr.type

    @property
    def precedence(self) -> _edgeql.Precedence:
        if self.direction is None:
            return self.expr.precedence
        else:
            return _edgeql.PRECEDENCE[_edgeql.Token.ASC]

    def __edgeql_expr__(self, *, ctx: ScopeContext) -> str:
        text = f"{edgeql(self.expr, ctx=ctx)}"
        if self.direction is not None:
            text += f" {self.direction.upper()}"
        if self.empty_direction is not None:
            text += f" {self.empty_direction.upper()}"
        return text


@dataclass(kw_only=True, frozen=True)
class OrderBy(Clause):
    directions: list[OrderByElem]

    def subnodes(self) -> Iterable[Node]:
        return self.directions

    @property
    def precedence(self) -> _edgeql.Precedence:
        return _edgeql.PRECEDENCE[_edgeql.Token.ORDER_BY]

    def __edgeql_expr__(self, *, ctx: ScopeContext) -> str:
        dexpr: Expr = self.directions[0]
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

    def __edgeql_expr__(self, *, ctx: ScopeContext) -> str:
        # Can't do constant extraction for LIMIT 1, it has semantic meaning.
        if isinstance(self.limit, IntLiteral) and self.limit.val == 1:
            return "LIMIT 1"
        else:
            clause = edgeql(self.limit, ctx=ctx)
            return f"LIMIT {clause}"


@dataclass(kw_only=True, frozen=True)
class Offset(Clause):
    offset: Expr

    def subnodes(self) -> Iterable[Node]:
        return (self.offset,)

    @property
    def precedence(self) -> _edgeql.Precedence:
        return _edgeql.PRECEDENCE[_edgeql.Token.OFFSET]

    def __edgeql_expr__(self, *, ctx: ScopeContext) -> str:
        return f"OFFSET {edgeql(self.offset, ctx=ctx)}"


@dataclass(kw_only=True, frozen=True)
class InsertStmt(Stmt, TypedExpr):
    stmt: _edgeql.Token = _edgeql.Token.INSERT
    shape: Shape | None = None

    def subnodes(self) -> Iterable[Node | None]:
        return (self.shape,)

    def _edgeql(self, ctx: ScopeContext) -> str:
        text = f"{self.stmt} {self.type.as_quoted_schema_name()}"
        if self.shape is not None:
            source = SchemaSet(type_=self.type)
            text = f"{text} {_render_shape(self.shape, source, ctx)}"
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
        if not isinstance(expr, AtomicExpr):
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
        splat_cb: Callable[[], Shape] | None = None,
    ) -> SelectStmt:
        if not isinstance(expr, SelectStmt) or (
            new_stmt_if is not None and new_stmt_if(expr)
        ):
            kwargs = {}
            if isinstance(expr, ShapeOp):
                kwargs["body_scope"] = expr.scope
            elif isinstance(expr, SchemaSet):
                if splat_cb is not None:
                    shape = splat_cb()
                else:
                    shape = Shape.splat(source=expr.type)
                expr = ShapeOp(iter_expr=expr, shape=shape)
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
            if self.offset is not None:
                text += "\n" + edgeql(self.offset, ctx=ctx)
            if self.limit is not None:
                text += "\n" + edgeql(self.limit, ctx=ctx)

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
    def type(self) -> SchemaPath:
        return self.body.type

    def subnodes(self) -> Iterable[Node]:
        return (self.iter_expr, self.body)

    def _edgeql(self, ctx: ScopeContext) -> str:
        return (
            f"{self.stmt} {edgeql(self.var, ctx=ctx)} IN "
            f"({edgeql(self.iter_expr, ctx=ctx)})\n"
            f"UNION ({edgeql(self.body, ctx=ctx)})"
        )


class Splat(_strenum.StrEnum):
    STAR = "*"
    DOUBLESTAR = "**"


@dataclass(kw_only=True, frozen=True)
class ShapeElement(Node):
    name: str | Splat
    origin: SchemaPath
    expr: Expr | None = None

    def subnodes(self) -> Iterable[Node]:
        if self.expr is not None:
            return (self.expr,)
        else:
            return ()

    @classmethod
    def splat(
        cls,
        source: SchemaPath,
        *,
        kind: Splat = Splat.STAR,
    ) -> Self:
        return cls(name=kind, origin=source)


@dataclass(kw_only=True, frozen=True)
class Shape(Node):
    elements: list[ShapeElement]

    def subnodes(self) -> Iterable[Node]:
        return self.elements

    @classmethod
    def splat(
        cls,
        source: SchemaPath,
        *,
        kind: Splat = Splat.STAR,
    ) -> Self:
        elements = [ShapeElement.splat(source=source, kind=kind)]
        return cls(elements=elements)


def _render_shape(
    shape: Shape,
    source: Expr,
    ctx: ScopeContext,
) -> str:
    els = []
    for el in shape.elements:
        if isinstance(el.name, Splat):
            if source.type != el.origin:
                el_source = el.origin.as_quoted_schema_name()
                el_text = f"[IS {el_source}].{el.name}"
            else:
                el_text = str(el.name)
        else:
            el_name = _edgeql.quote_ident(el.name)
            el_expr = el.expr
            if el_expr is None:
                if source.type != el.origin:
                    el_source = el.origin.as_quoted_schema_name()
                    el_text = f"[IS {el_source}].{el_name}"
                else:
                    el_text = el_name
            elif (
                isinstance(el_expr, Path)
                and isinstance(el_expr.source, (SchemaSet, PathPrefix))
                and el_expr.source.type == source.type
                and el_expr.name == el.name
            ):
                el_text = _edgeql.quote_ident(el.name)
            else:
                assign = InfixOp(
                    lexpr=Ident(name=el.name, type_=el_expr.type),
                    op=_edgeql.Token.ASSIGN,
                    rexpr=el_expr,
                    type_=el_expr.type,
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
    def type(self) -> SchemaPath:
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
    if isinstance(lexpr, AtomicExpr):
        return False
    left_prec = lprec if lprec is not None else lexpr.precedence
    return _edgeql.need_left_parens(prod_prec, left_prec)


def _need_right_parens(
    prod_prec: _edgeql.Precedence,
    rexpr: Expr,
    rprec: _edgeql.Precedence | None = None,
) -> bool:
    if isinstance(rexpr, AtomicExpr):
        return False
    right_prec = rprec if rprec is not None else rexpr.precedence
    return _edgeql.need_right_parens(prod_prec, right_prec)


_type_splat_cache: weakref.WeakKeyDictionary[type[GelTypeMetadata], Shape] = (
    weakref.WeakKeyDictionary()
)


def get_object_type_splat(cls: type[GelTypeMetadata]) -> Shape:
    shape = _type_splat_cache.get(cls)
    if shape is None:
        reflection = cls.__gel_reflection__
        shape = Shape.splat(source=reflection.name)
        _type_splat_cache[cls] = shape
    return shape


def toplevel_edgeql(
    x: ExprCompatible,
    *,
    splat_cb: Callable[[], Shape] | None = None,
) -> tuple[str, dict[str, object]]:
    expr = edgeql_qb_expr(x)
    if not isinstance(expr, Stmt):
        expr = SelectStmt.wrap(expr, splat_cb=splat_cb)
    ctx = ScopeContext(Scope(expr))
    return edgeql(expr, ctx=ctx), ctx.get_arguments()
