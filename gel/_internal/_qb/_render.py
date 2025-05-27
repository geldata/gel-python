# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

"""EdgeQL rendering helpers"""

from __future__ import annotations

from ._abstract import Stmt

from ._expressions import (
    SelectStmt,
    ShapeOp,
)

from ._protocols import (
    ExprCompatible,
    edgeql_qb_expr,
    edgeql,
)


def toplevel_edgeql(x: ExprCompatible) -> str:
    expr = edgeql_qb_expr(x)
    if not isinstance(expr, Stmt):
        kwargs = {}
        if isinstance(expr, ShapeOp):
            kwargs["scope"] = expr.scope
        expr = SelectStmt(expr=expr, **kwargs)  # type: ignore [arg-type]
    return edgeql(expr)
