# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.


"""Query building constructs"""

from typing import TypeVar
from collections.abc import Callable

from gel._internal import _qb
from gel._internal._qbmodel import _abstract


_T = TypeVar("_T", bound=_abstract.GelType)
_X = TypeVar("_X", bound=_abstract.GelType)


def foreach(iter: type[_T], body: Callable[[type[_T]], type[_X]]) -> type[_X]:
    """Evaluate the expression returned by *body* for each element in *iter*.

    This is the Pythonic representation of the EdgeQL FOR expression."""

    iter_expr = _qb.edgeql_qb_expr(iter)
    var = _qb.Variable(name="x", type_=iter_expr.type)
    type_ = body(_qb.AnnotatedVar(iter, var))  # type: ignore [arg-type]
    return _qb.AnnotatedExpr(  # type: ignore [return-type]
        type_,
        _qb.ForStmt(
            iter_expr=iter_expr,
            expr=_qb.edgeql_qb_expr(type_),
            var=var,
        ),
    )
