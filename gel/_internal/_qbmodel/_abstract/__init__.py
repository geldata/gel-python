# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

"""Base types used to implement class-based query builders."""

from __future__ import annotations

from ._base import (
    GelPointerReflection,
    GelType,
    GelType_T,
    GelTypeMeta,
    GelTypeMetadata,
    PointerInfo,
)

from ._descriptors import (
    AnyPropertyDescriptor,
    LinkDescriptor,
    ModelFieldDescriptor,
    OptionalLinkDescriptor,
    OptionalPointerDescriptor,
    OptionalPropertyDescriptor,
    PointerDescriptor,
    PropertyDescriptor,
    field_descriptor,
)

from ._expressions import (
    get_object_type_splat,
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
    DateTimeLike,
    GelPrimitiveType,
    MultiRange,
    PyConstType,
    PyTypeScalar,
    Range,
    Tuple,
    get_py_type_for_scalar,
    get_py_type_for_scalar_hierarchy,
    get_py_type_scalar_match_rank,
    maybe_get_protocol_for_py_type,
    maybe_get_zero_value_for_scalar_hierarchy,
)


__all__ = (
    "AnyEnum",
    "AnyPropertyDescriptor",
    "AnyTuple",
    "Array",
    "BaseScalar",
    "DateTimeLike",
    "GelModel",
    "GelModelMeta",
    "GelPointerReflection",
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
    "get_object_type_splat",
    "get_py_type_for_scalar",
    "get_py_type_for_scalar_hierarchy",
    "get_py_type_scalar_match_rank",
    "maybe_get_protocol_for_py_type",
    "maybe_get_zero_value_for_scalar_hierarchy",
)
