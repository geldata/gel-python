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
    InfixOp,
    PrefixOp,
    FuncCall,
)

from gel._internal._qbmodel._abstract import (
    AnyEnum,
    AnyTuple,
    BaseScalar,
    GelType,
    GelType_T,
    GelTypeMeta,
    GelTypeMetadata,
)

from gel._internal._qbmodel._pydantic import (
    Array,
    Tuple,
    GelLinkModel,
    GelModel,
    GelModelMeta,
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
)


__all__ = (
    "AnnotatedExpr",
    "AnyEnum",
    "AnyTuple",
    "Array",
    "BaseScalar",
    "FuncCall",
    "GelLinkModel",
    "GelModel",
    "GelModelMeta",
    "GelType",
    "GelTypeMeta",
    "GelTypeMetadata",
    "GelType_T",
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
    "PrefixOp",
    "PrivateAttr",
    "ProxyModel",
    "PyTypeScalar",
    "Range",
    "SchemaPath",
    "Tuple",
    "Unspecified",
    "UnspecifiedType",
    "computed_field",
    "dispatch_overload",
)
