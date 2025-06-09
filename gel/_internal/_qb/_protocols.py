# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

"""Protocols for the EdgeQL query builder"""

from __future__ import annotations

from typing import (
    Any,
    ClassVar,
    ParamSpec,
    Protocol,
    TypeGuard,
    TypeVar,
)
from typing_extensions import TypeAliasType
from collections.abc import Callable

from gel._internal import _utils

from ._abstract import Expr, ScopeContext


class TypeClassProto(Protocol):
    __gel_type_class__: ClassVar[type]


class InstanceSupportsEdgeQLExpr(Protocol):
    def __edgeql_expr__(self, *, ctx: ScopeContext | None) -> str: ...


class TypeSupportsEdgeQLExpr(Protocol):
    @classmethod
    def __edgeql_expr__(cls, *, ctx: ScopeContext | None) -> str: ...


SupportsEdgeQLExpr = TypeAliasType(
    "SupportsEdgeQLExpr",
    InstanceSupportsEdgeQLExpr | type[TypeSupportsEdgeQLExpr],
)


def supports_edgeql_expr(v: Any) -> TypeGuard[SupportsEdgeQLExpr]:
    return callable(getattr(v, "__edgeql_expr__", None))


class ExprCompatibleInstance(Protocol):
    def __edgeql_qb_expr__(self) -> Expr: ...


class ExprCompatibleType(Protocol):
    @classmethod
    def __edgeql_qb_expr__(cls) -> Expr: ...


ExprCompatible = TypeAliasType(
    "ExprCompatible",
    ExprCompatibleInstance | type[ExprCompatibleType],
)


def is_expr_compatible(v: Any) -> TypeGuard[ExprCompatible]:
    return callable(getattr(v, "__edgeql_qb_expr__", None))


ExprClosure = TypeAliasType(
    "ExprClosure",
    Callable[[ExprCompatible], ExprCompatible],
)


def is_expr_closure(v: Any) -> TypeGuard[ExprClosure]:
    return callable(v)


P = ParamSpec("P")
R = TypeVar("R")


def exprmethod(func: Callable[P, R]) -> Callable[P, R]:
    actual_func: Callable[P, R] = getattr(func, "__func__", func)
    actual_func.__gel_expr_method__ = True  # type: ignore [attr-defined]
    return func


def is_exprmethod(obj: Any) -> TypeGuard[Callable[..., Any]]:
    if hasattr(obj, "__gel_expr_method__"):
        return True
    func = getattr(obj, "__func__", None)
    if func is not None:
        return hasattr(func, "__gel_expr_method__")
    return False


def edgeql(
    source: SupportsEdgeQLExpr | ExprCompatible,
    *,
    ctx: ScopeContext | None,
) -> str:
    try:
        __edgeql_expr__ = source.__edgeql_expr__  # type: ignore [union-attr]
    except AttributeError:
        try:
            __edgeql_qb_expr__ = source.__edgeql_qb_expr__  # type: ignore [union-attr]
        except AttributeError:
            raise TypeError(
                f"{type(source)} does not support __edgeql_expr__ protocol"
            ) from None
        else:
            expr = __edgeql_qb_expr__()
            __edgeql_expr__ = expr.__edgeql_expr__

    if not callable(__edgeql_expr__):
        raise TypeError(f"{type(source)}.__edgeql_expr__ is not callable")

    value = __edgeql_expr__(ctx=ctx)
    if not isinstance(value, str):
        raise ValueError("{type(source)}.__edgeql_expr__()")
    return value


def edgeql_qb_expr(
    x: ExprCompatible | ExprClosure,
    *,
    var: ExprCompatible | None = None,
) -> Expr:
    if isinstance(x, Expr):
        return x

    as_expr = getattr(x, "__edgeql_qb_expr__", None)
    if as_expr is None or not callable(as_expr):
        if is_expr_closure(x):
            if var is None:
                raise ValueError(
                    "edgeql_qb_expr: must specify *var* when evaluating "
                    "expression closures"
                )
            x = x(var)
            as_expr = getattr(x, "__edgeql_qb_expr__", None)
            if as_expr is None or not callable(as_expr):
                as_expr = None
        else:
            as_expr = None

    if as_expr is None:
        raise TypeError(
            f"{_utils.type_repr(type(x))} cannot be converted to an Expr"
        )
    expr = as_expr()
    if not isinstance(expr, Expr):
        raise ValueError(
            f"{_utils.type_repr(type(x))}.__edgeql_qb_expr__ did not "
            f"return an Expr instance"
        )
    return expr


def assert_edgeql_qb_expr(x: Any) -> Expr:
    return edgeql_qb_expr(x)
