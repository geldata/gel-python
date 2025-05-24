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
    LinkDescriptor,
    ModelFieldDescriptor,
    OptionalLinkDescriptor,
    OptionalPointerDescriptor,
    OptionalPropertyDescriptor,
    PointerDescriptor,
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
    "PropertyDescriptor",
    "PyConstType",
    "PyTypeScalar",
    "Range",
    "Tuple",
    "field_descriptor",
)
