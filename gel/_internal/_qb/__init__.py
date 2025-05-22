# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

"""EdgeQL Query Builder"""


from __future__ import annotations

from ._abstract import (
    AbstractDescriptor,
    AbstractFieldDescriptor,
    Expr,
)

from ._expressions import (
    BoolLiteral,
    BytesLiteral,
    DecimalLiteral,
    DeleteStmt,
    ExprPlaceholder,
    Filter,
    FloatLiteral,
    ForStmt,
    FuncCall,
    InfixOp,
    IntLiteral,
    Limit,
    Literal,
    OrderBy,
    Offset,
    Path,
    PathPrefix,
    PrefixOp,
    SchemaSet,
    Scope,
    SetLiteral,
    Shape,
    ShapeOp,
    Stmt,
    SelectStmt,
    StringLiteral,
    UpdateStmt,
    Variable,
)

from ._generics import (
    AnnotatedExpr,
    AnnotatedPath,
    AnnotatedVar,
    BaseAlias,
    ExprAlias,
    PathAlias,
    SortAlias,
    VarAlias,
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
    "AbstractFieldDescriptor",
    "AnnotatedExpr",
    "AnnotatedPath",
    "AnnotatedVar",
    "BaseAlias",
    "BoolLiteral",
    "BytesLiteral",
    "DecimalLiteral",
    "DeleteStmt",
    "Expr",
    "ExprAlias",
    "ExprCompatible",
    "ExprPlaceholder",
    "Filter",
    "FloatLiteral",
    "ForStmt",
    "FuncCall",
    "InfixOp",
    "IntLiteral",
    "Limit",
    "Literal",
    "Offset",
    "OrderBy",
    "Path",
    "PathAlias",
    "PathPrefix",
    "PrefixOp",
    "SchemaSet",
    "Scope",
    "SelectStmt",
    "SetLiteral",
    "Shape",
    "ShapeOp",
    "SortAlias",
    "Stmt",
    "StringLiteral",
    "UpdateStmt",
    "VarAlias",
    "Variable",
    "edgeql",
    "edgeql_qb_expr",
    "exprmethod",
    "toplevel_edgeql",
)
