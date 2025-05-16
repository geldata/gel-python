# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

"""EdgeQL query builder expressions"""

from __future__ import annotations

from typing import (
    TYPE_CHECKING,
)

import dataclasses
import textwrap

from gel._internal import _edgeql
from gel._internal import _reflection
from gel._internal._reflection import SchemaPath

from ._abstract import Expr
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


@dataclasses.dataclass(kw_only=True)
class TypedExpr(Expr):
    type_: SchemaPath

    @property
    def type(self) -> SchemaPath:
        return self.type_


@dataclasses.dataclass(kw_only=True)
class IdentLikeExpr(TypedExpr):
    @property
    def precedence(self) -> _edgeql.Precedence:
        return _edgeql.PRECEDENCE[_edgeql.Token.IDENT]


@dataclasses.dataclass(kw_only=True)
class Symbol(IdentLikeExpr):
    pass


@dataclasses.dataclass(kw_only=True)
class PathPrefix(Symbol):
    def __edgeql_expr__(self) -> str:
        return ""


@dataclasses.dataclass
class SchemaSet(IdentLikeExpr):
    type_: _reflection.SchemaPath

    def __edgeql_expr__(self) -> str:
        return "::".join(self.type.parts)


@dataclasses.dataclass(kw_only=True)
class Literal(IdentLikeExpr):
    pass


@dataclasses.dataclass(kw_only=True)
class BoolLiteral(Literal):
    val: bool
    type_: SchemaPath = dataclasses.field(default=SchemaPath("std", "bool"))

    def __edgeql_expr__(self) -> str:
        return "true" if self.val else "false"


@dataclasses.dataclass(kw_only=True)
class IntLiteral(Literal):
    val: int
    type_: SchemaPath = dataclasses.field(default=SchemaPath("std", "int64"))

    def __edgeql_expr__(self) -> str:
        return str(self.val)


@dataclasses.dataclass(kw_only=True)
class FloatLiteral(Literal):
    val: float
    type_: SchemaPath = dataclasses.field(default=SchemaPath("std", "float64"))

    def __edgeql_expr__(self) -> str:
        return str(self.val)


@dataclasses.dataclass(kw_only=True)
class BigIntLiteral(Literal):
    val: int
    type_: SchemaPath = dataclasses.field(default=SchemaPath("std", "bigint"))

    def __edgeql_expr__(self) -> str:
        return f"n{self.val}"


@dataclasses.dataclass(kw_only=True)
class DecimalLiteral(Literal):
    val: decimal.Decimal
    type_: SchemaPath = dataclasses.field(default=SchemaPath("std", "decimal"))

    def __edgeql_expr__(self) -> str:
        return f"n{self.val}"


@dataclasses.dataclass(kw_only=True)
class BytesLiteral(Literal):
    val: bytes
    type_: SchemaPath = dataclasses.field(default=SchemaPath("std", "bytes"))

    def __edgeql_expr__(self) -> str:
        v = _edgeql.quote_literal(repr(self.val)[2:-1])
        return f"b{v}"


@dataclasses.dataclass(kw_only=True)
class StringLiteral(Literal):
    val: str
    type_: SchemaPath = dataclasses.field(default=SchemaPath("std", "str"))

    def __edgeql_expr__(self) -> str:
        return _edgeql.quote_literal(self.val)


@dataclasses.dataclass(kw_only=True)
class Path(TypedExpr):
    source: Expr
    name: str
    is_lprop: bool

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


@dataclasses.dataclass(kw_only=True)
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
        self.op = op

    @property
    def precedence(self) -> _edgeql.Precedence:
        return _edgeql.PRECEDENCE[self.op]


@dataclasses.dataclass(kw_only=True)
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
        self.expr = edgeql_qb_expr(expr)

    def __edgeql_expr__(self) -> str:
        return f"{self.op} {edgeql(self.expr)}"


@dataclasses.dataclass(kw_only=True)
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
        self.lexpr = edgeql_qb_expr(lexpr)
        self.rexpr = edgeql_qb_expr(rexpr)

    def __edgeql_expr__(self) -> str:
        left = edgeql(self.lexpr)
        if self._need_left_parens():
            left = f"({left})"
        right = edgeql(self.rexpr)
        if self._need_right_parens():
            right = f"({right})"
        return f"{left} {self.op} {right}"

    def _need_left_parens(self) -> bool:
        lexpr = self.lexpr
        if isinstance(lexpr, IdentLikeExpr):
            return False
        left_prec = lexpr.precedence.value
        self_prec = self.precedence.value
        self_assoc = self.precedence.assoc

        return left_prec < self_prec or (
            left_prec == self_prec
            and self_assoc is not _edgeql.Assoc.RIGHT
            and (not isinstance(lexpr, InfixOp) or lexpr.op != self.op)
        )

    def _need_right_parens(self) -> bool:
        rexpr = self.rexpr
        if isinstance(rexpr, IdentLikeExpr):
            return False
        right_prec = rexpr.precedence.value
        self_prec = self.precedence.value
        self_assoc = self.precedence.assoc

        return right_prec < self_prec or (
            right_prec == self_prec and self_assoc is _edgeql.Assoc.RIGHT
        )


@dataclasses.dataclass(kw_only=True)
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


@dataclasses.dataclass(kw_only=True)
class Filter(Expr):
    expr: Expr
    filters: list[Expr]

    @property
    def type(self) -> _reflection.SchemaPath:
        return self.expr.type

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
        fexpr = InfixOp(
            lexpr=self.expr,
            op=_edgeql.Token.FILTER,
            rexpr=fexpr,
            type_=SchemaPath("std", "bool"),
        )
        return edgeql(fexpr)


@dataclasses.dataclass(kw_only=True)
class Limit(Expr):
    expr: Expr
    limit: Expr

    @property
    def type(self) -> _reflection.SchemaPath:
        return self.expr.type

    @property
    def precedence(self) -> _edgeql.Precedence:
        return _edgeql.PRECEDENCE[_edgeql.Token.LIMIT]

    def __edgeql_expr__(self) -> str:
        return f"{edgeql(self.expr)} LIMIT {edgeql(self.limit)}"


@dataclasses.dataclass(kw_only=True)
class Offset(Expr):
    expr: Expr
    offset: Expr

    @property
    def type(self) -> _reflection.SchemaPath:
        return self.expr.type

    @property
    def precedence(self) -> _edgeql.Precedence:
        return _edgeql.PRECEDENCE[_edgeql.Token.OFFSET]

    def __edgeql_expr__(self) -> str:
        return f"{edgeql(self.expr)} OFFSET {edgeql(self.offset)}"


@dataclasses.dataclass(kw_only=True)
class Shape(TypedExpr):
    expr: Expr
    elements: dict[str, Expr] = dataclasses.field(default_factory=dict)
    star_splat: bool = False
    doublestar_splat: bool = False

    @property
    def type(self) -> _reflection.SchemaPath:
        return self.expr.type

    @property
    def precedence(self) -> _edgeql.Precedence:
        return _edgeql.PRECEDENCE[_edgeql.Token.LBRACE]

    def __edgeql_expr__(self) -> str:
        els = []
        if self.star_splat:
            els.append("*")
        if self.doublestar_splat:
            els.append("**")
        for n, el in self.elements.items():
            if (
                isinstance(el, Path)
                and isinstance(el.source, (SchemaSet, PathPrefix))
                and el.source.type == self.expr.type
                and el.name == n
            ):
                els.append(f"{n},")
            else:
                els.append(f"{n} := {edgeql(el)},")
        shape = "{\n" + textwrap.indent("\n".join(els), "  ") + "\n}"
        return f"{edgeql(self.expr)} {shape}"


class Stmt(Expr):
    pass
