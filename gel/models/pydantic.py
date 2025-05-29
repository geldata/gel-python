# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.


"""API exported to generated Pydantic-based models."""

from pydantic import (
    PrivateAttr,
    computed_field,
)

from gel._internal._lazyprop import LazyClassProperty
from gel._internal._reflection import SchemaPath
from gel._internal._typing_dispatch import dispatch_overload
from gel._internal._utils import UnspecifiedType, Unspecified

from gel._internal._qb import (
    AnnotatedExpr,
    ExprClosure,
    ExprCompatible,
    InfixOp,
    PrefixOp,
    FuncCall,
    PathAlias,
)

from gel._internal._qbmodel._abstract import (
    AnyEnum,
    AnyTuple,
    BaseScalar,
    GelType,
    GelType_T,
    GelTypeMeta,
    GelTypeMetadata,
    PyConstType,
)

from gel._internal._qbmodel._pydantic import (
    Array,
    ComputedProperty,
    GelLinkModel,
    GelModel,
    GelModelMeta,
    IdProperty,
    LinkClassNamespace,
    LinkPropsDescriptor,
    MultiLink,
    MultiLinkWithProps,
    MultiRange,
    OptionalLink,
    OptionalLinkWithProps,
    OptionalProperty,
    ProxyModel,
    PyTypeScalar,
    Range,
    Tuple,
)


__all__ = (
    "AnnotatedExpr",
    "AnyEnum",
    "AnyTuple",
    "Array",
    "BaseScalar",
    "ComputedProperty",
    "ExprClosure",
    "ExprCompatible",
    "FuncCall",
    "GelLinkModel",
    "GelModel",
    "GelModelMeta",
    "GelType",
    "GelTypeMeta",
    "GelTypeMetadata",
    "GelType_T",
    "IdProperty",
    "InfixOp",
    "LazyClassProperty",
    "LinkClassNamespace",
    "LinkPropsDescriptor",
    "MultiLink",
    "MultiLinkWithProps",
    "MultiRange",
    "OptionalLink",
    "OptionalLinkWithProps",
    "OptionalProperty",
    "PathAlias",
    "PrefixOp",
    "PrivateAttr",
    "ProxyModel",
    "PyConstType",
    "PyTypeScalar",
    "Range",
    "SchemaPath",
    "Tuple",
    "Unspecified",
    "UnspecifiedType",
    "computed_field",
    "dispatch_overload",
)
