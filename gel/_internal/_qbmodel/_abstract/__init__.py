# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

"""Base types used to implement class-based query builders."""

from __future__ import annotations

from ._base import (
    GelType,
    GelType_T,
    GelTypeMeta,
    GelTypeMetadata,
)

from ._descriptors import (
    AnyPropertyDescriptor,
    LinkDescriptor,
    ModelFieldDescriptor,
    OptionalLinkDescriptor,
    OptionalPointerDescriptor,
    OptionalPropertyDescriptor,
    PointerDescriptor,
    PointerInfo,
    PropertyDescriptor,
    field_descriptor,
)

from ._objects import (
    GelModel,
    GelModelMeta,
)

from ._primitive import (
    AnyEnum,
    AnyTuple,
    Array,
    BaseScalar,
    GelPrimitiveType,
    MultiRange,
    PyConstType,
    PyTypeScalar,
    Range,
    Tuple,
)


__all__ = (
    "AnyEnum",
    "AnyPropertyDescriptor",
    "AnyTuple",
    "Array",
    "BaseScalar",
    "GelModel",
    "GelModelMeta",
    "GelPrimitiveType",
    "GelType",
    "GelTypeMeta",
    "GelTypeMetadata",
    "GelType_T",
    "LinkDescriptor",
    "ModelFieldDescriptor",
    "MultiRange",
    "OptionalLinkDescriptor",
    "OptionalPointerDescriptor",
    "OptionalPropertyDescriptor",
    "PointerDescriptor",
    "PointerInfo",
    "PropertyDescriptor",
    "PyConstType",
    "PyTypeScalar",
    "Range",
    "Tuple",
    "field_descriptor",
)
