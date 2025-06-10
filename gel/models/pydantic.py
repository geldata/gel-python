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
    EmptyDirection,
    Direction,
    ExprClosure,
    ExprCompatible,
    IndexOp,
    InfixOp,
    PrefixOp,
    FuncCall,
    PathAlias,
)

from gel._internal._qbmodel._abstract import (
    AnyEnum,
    AnyTuple,
    BaseScalar,
    DateTimeLike,
    GelType,
    GelType_T,
    GelTypeMeta,
    GelTypeMetadata,
    PyConstType,
)

from gel._internal._qbmodel._pydantic import (
    Array,
    GelLinkModel,
    GelModel,
    GelModelMeta,
    IdProperty,
    LinkClassNamespace,
    LinkPropsDescriptor,
    ComputedMultiLink,
    ComputedMultiLinkWithProps,
    ComputedMultiProperty,
    ComputedProperty,
    MultiLink,
    RequiredMultiLink,
    RequiredMultiLinkWithProps,
    MultiLinkWithProps,
    MultiProperty,
    MultiRange,
    OptionalLink,
    OptionalLinkWithProps,
    OptionalProperty,
    OptionalComputedLink,
    OptionalComputedLinkWithProps,
    OptionalComputedProperty,
    ProxyModel,
    Property,
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
    "ComputedMultiLink",
    "ComputedMultiLinkWithProps",
    "ComputedMultiProperty",
    "ComputedProperty",
    "DateTimeLike",
    "Direction",
    "EmptyDirection",
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
    "IndexOp",
    "InfixOp",
    "LazyClassProperty",
    "LinkClassNamespace",
    "LinkPropsDescriptor",
    "MultiLink",
    "MultiLinkWithProps",
    "MultiProperty",
    "MultiRange",
    "OptionalComputedLink",
    "OptionalComputedLinkWithProps",
    "OptionalComputedProperty",
    "OptionalLink",
    "OptionalLinkWithProps",
    "OptionalProperty",
    "PathAlias",
    "PrefixOp",
    "PrivateAttr",
    "Property",
    "ProxyModel",
    "PyConstType",
    "PyTypeScalar",
    "Range",
    "RequiredMultiLink",
    "RequiredMultiLinkWithProps",
    "SchemaPath",
    "Tuple",
    "Unspecified",
    "UnspecifiedType",
    "computed_field",
    "dispatch_overload",
)
