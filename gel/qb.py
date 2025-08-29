# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.


"""Query building constructs"""

from typing import TypeVar
from collections.abc import Callable

from gel._internal import _qb
from gel._internal._qb import edgeql
from gel._internal._qbmodel import _abstract


_T = TypeVar("_T", bound=_abstract.GelType)
_X = TypeVar("_X", bound=_abstract.GelType)


def for_in(iter: type[_T], body: Callable[[type[_T]], type[_X]]) -> type[_X]:
    """Evaluate the expression returned by *body* for each element in *iter*.

    This is the Pythonic representation of the EdgeQL FOR expression."""

    iter_expr = _qb.edgeql_qb_expr(iter)
    scope = _qb.Scope()
    var = _qb.Variable(type_=iter_expr.type, scope=scope)
    body_ = body(_qb.AnnotatedVar(iter.__gel_origin__, var))  # type: ignore [arg-type, attr-defined]
    return _qb.AnnotatedExpr(  # type: ignore [return-value]
        body_.__gel_origin__,  # type: ignore [attr-defined]
        _qb.ForStmt(
            iter_expr=iter_expr,
            body=_qb.edgeql_qb_expr(body_),
            scope=scope,
        ),
    )


__all__ = ("for_in",)
