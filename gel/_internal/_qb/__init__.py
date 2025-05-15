# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

"""EdgeQL Query Builder"""


from __future__ import annotations

from ._abstract import (
    AbstractDescriptor,
    Expr,
)

from ._descriptors import (
    OptionalPointerDescriptor,
    PointerDescriptor,
    field_descriptor,
)

from ._expressions import (
    BoolLiteral,
    BytesLiteral,
    DecimalLiteral,
    ExprPlaceholder,
    Filter,
    FloatLiteral,
    FuncCall,
    InfixOp,
    IntLiteral,
    Literal,
    PathPrefix,
    PrefixOp,
    SchemaSet,
    Shape,
    Stmt,
    StringLiteral,
)

from ._generics import (
    AnnotatedExpr,
    ExprAlias,
    PathAlias,
)

from ._protocols import (
    ExprCompatible,
    edgeql_qb_expr,
    edgeql,
    exprmethod,
)

from ._render import (
    toplevel_edgeql,
)


__all__ = (
    "AbstractDescriptor",
    "AnnotatedExpr",
    "BoolLiteral",
    "BytesLiteral",
    "DecimalLiteral",
    "Expr",
    "ExprAlias",
    "ExprCompatible",
    "ExprPlaceholder",
    "Filter",
    "FloatLiteral",
    "FuncCall",
    "InfixOp",
    "IntLiteral",
    "Literal",
    "OptionalPointerDescriptor",
    "PathAlias",
    "PathPrefix",
    "PointerDescriptor",
    "PrefixOp",
    "SchemaSet",
    "Shape",
    "Stmt",
    "StringLiteral",
    "edgeql",
    "edgeql_qb_expr",
    "exprmethod",
    "field_descriptor",
    "toplevel_edgeql",
)
