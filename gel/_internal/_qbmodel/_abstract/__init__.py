# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

"""Base types used to implement class-based query builders."""

from __future__ import annotations

from ._base import (
    GelType,
    GelType_T,
    GelTypeMeta,
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
    "GelPrimitiveType",
    "GelType",
    "GelTypeMeta",
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
    "get_py_type_for_scalar",
    "get_py_type_for_scalar_hierarchy",
    "get_py_type_scalar_match_rank",
    "maybe_get_protocol_for_py_type",
)
