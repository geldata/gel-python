# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

"""EdgeQL query builder expressions"""

from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Any,
)

import dataclasses
import textwrap

from gel._internal import _edgeql
from gel._internal import _reflection

from ._abstract import Expr
from ._protocols import ExprCompatible, edgeql, edgeql_qb_expr

if TYPE_CHECKING:
    import decimal


class ExprPlaceholder(Expr):
    @property
    def precedence(self) -> _edgeql.Precedence:
        raise TypeError("unreplaced ExprPlaceholder")

    def __edgeql_expr__(self) -> str:
        raise TypeError("unreplaced ExprPlaceholder")


class IdentLikeExpr(Expr):
    @property
    def precedence(self) -> _edgeql.Precedence:
        return _edgeql.PRECEDENCE[_edgeql.Token.IDENT]


class Symbol(IdentLikeExpr):
    pass


class PathPrefix(Symbol):
    def __edgeql_expr__(self) -> str:
        return ""


@dataclasses.dataclass(frozen=True)
class SchemaSet(IdentLikeExpr):
    name: _reflection.SchemaPath

    def __edgeql_expr__(self) -> str:
        return "::".join(self.name.parts)


class Literal(IdentLikeExpr):
    pass


@dataclasses.dataclass(frozen=True)
class BoolLiteral(Literal):
    val: bool

    def __edgeql_expr__(self) -> str:
        return "true" if self.val else "false"


@dataclasses.dataclass(frozen=True)
class IntLiteral(Literal):
    val: int

    def __edgeql_expr__(self) -> str:
        return str(self.val)


@dataclasses.dataclass(frozen=True)
class FloatLiteral(Literal):
    val: float

    def __edgeql_expr__(self) -> str:
        return str(self.val)


@dataclasses.dataclass(frozen=True)
class BigIntLiteral(Literal):
    val: int

    def __edgeql_expr__(self) -> str:
        return f"n{self.val}"


@dataclasses.dataclass(frozen=True)
class DecimalLiteral(Literal):
    val: decimal.Decimal

    def __edgeql_expr__(self) -> str:
        return f"n{self.val}"


@dataclasses.dataclass(frozen=True)
class BytesLiteral(Literal):
    val: bytes

    def __edgeql_expr__(self) -> str:
        v = _edgeql.quote_literal(repr(self.val)[2:-1])
        return f"b{v}"


@dataclasses.dataclass(frozen=True)
class StringLiteral(Literal):
    val: str

    def __edgeql_expr__(self) -> str:
        return _edgeql.quote_literal(self.val)


@dataclasses.dataclass(kw_only=True)
class Path(Expr):
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

        if not isinstance(current, (SchemaSet, Symbol)):
            raise AssertionError(
                "Path does not start with a SourceSet or a Symbol"
            )
        steps.append(current.__edgeql_expr__())

        return ".".join(reversed(steps))


@dataclasses.dataclass(kw_only=True)
class Op(Expr):
    op: _edgeql.Token

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
    ) -> None:
        self.expr = edgeql_qb_expr(expr)
        if isinstance(op, str):
            op = _edgeql.Token.from_str(op)
        self.op = op

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
    ) -> None:
        self.lexpr = edgeql_qb_expr(lexpr)
        self.rexpr = edgeql_qb_expr(rexpr)
        if isinstance(op, str):
            op = _edgeql.Token.from_str(op)
        self.op = op

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


@dataclasses.dataclass(frozen=True, kw_only=True)
class FuncCall(Expr):
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


@dataclasses.dataclass(frozen=True, kw_only=True)
class Filter(Expr):
    expr: Any
    filters: list[Expr]

    @property
    def precedence(self) -> _edgeql.Precedence:
        return _edgeql.PRECEDENCE[_edgeql.Token.FILTER]

    def __edgeql_expr__(self) -> str:
        fexpr = self.filters[0]
        for item in self.filters[1:]:
            fexpr = InfixOp(lexpr=fexpr, op=_edgeql.Token.AND, rexpr=item)
        fexpr = InfixOp(lexpr=self.expr, op=_edgeql.Token.FILTER, rexpr=fexpr)
        return edgeql(fexpr)


@dataclasses.dataclass(frozen=True, kw_only=True)
class Shape(Expr):
    expr: Any
    elements: dict[str, Expr] = dataclasses.field(default_factory=dict)
    star_splat: bool = False
    doublestar_splat: bool = False

    @property
    def precedence(self) -> _edgeql.Precedence:
        return _edgeql.PRECEDENCE[_edgeql.Token.LBRACE]

    def __edgeql_expr__(self) -> str:
        els = []
        if self.star_splat:
            els.append("*")
        if self.doublestar_splat:
            els.append("**")
        els.extend(f"{n} := {edgeql(ex)}" for n, ex in self.elements.items())
        shape = "{\n" + textwrap.indent("\n".join(els), "  ") + "\n}"
        return f"{edgeql(self.expr)} {shape}"


class Stmt(Expr):
    pass
