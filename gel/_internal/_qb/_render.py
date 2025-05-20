# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

"""EdgeQL rendering helpers"""

from __future__ import annotations

from ._expressions import (
    SelectStmt,
    Stmt,
)

from ._protocols import (
    ExprCompatible,
    edgeql_qb_expr,
    edgeql,
)


def toplevel_edgeql(x: ExprCompatible) -> str:
    expr = edgeql_qb_expr(x)
    if not isinstance(expr, Stmt):
        expr = SelectStmt(expr=expr)
    return edgeql(expr)
