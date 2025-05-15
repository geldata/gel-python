# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

"""Protocols for the EdgeQL query builder"""

from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    ParamSpec,
    Protocol,
    TypeGuard,
    TypeVar,
    runtime_checkable,
)
from typing_extensions import TypeAliasType

from gel._internal import _utils

from ._abstract import Expr

if TYPE_CHECKING:
    from collections.abc import Callable


@runtime_checkable
class TypeClassProto(Protocol):
    __gel_type_class__: ClassVar[type]


@runtime_checkable
class InstanceSupportsEdgeQLExpr(Protocol):
    def __edgeql_expr__(self) -> str: ...


@runtime_checkable
class TypeSupportsEdgeQLExpr(Protocol):
    @classmethod
    def __edgeql_expr__(cls) -> str: ...


SupportsEdgeQLExpr = TypeAliasType(
    "SupportsEdgeQLExpr",
    InstanceSupportsEdgeQLExpr | type[TypeSupportsEdgeQLExpr],
)


@runtime_checkable
class ExprCompatibleInstance(Protocol):
    def __edgeql_qb_expr__(self) -> Expr: ...


@runtime_checkable
class ExprCompatibleType(Protocol):
    @classmethod
    def __edgeql_expr__(cls) -> str: ...


ExprCompatible = TypeAliasType(
    "ExprCompatible",
    ExprCompatibleInstance | type[ExprCompatibleType],
)


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


def edgeql(source: SupportsEdgeQLExpr | ExprCompatible) -> str:
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

    value = __edgeql_expr__()
    if not isinstance(value, str):
        raise ValueError("{type(source)}.__edgeql_expr__()")
    return value


def edgeql_qb_expr(x: ExprCompatible) -> Expr:
    if isinstance(x, Expr):
        return x

    as_expr = getattr(x, "__edgeql_qb_expr__", None)
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
