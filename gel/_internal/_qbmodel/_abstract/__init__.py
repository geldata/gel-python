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
    GelLinkModel,
    GelModel,
    GelModelMeta,
    GelSourceModel,
)

from ._primitive import (
    MODEL_SUBSTRATE_MODULE,
    AnyEnum,
    AnyTuple,
    Array,
    BaseScalar,
    DateImpl,
    DateTimeLike,
    DateTimeImpl,
    GelPrimitiveType,
    MultiRange,
    PyConstType,
    PyTypeScalar,
    Range,
    TimeImpl,
    TimeDeltaImpl,
    Tuple,
    UUIDImpl,
    get_py_base_for_scalar,
    get_py_type_for_scalar,
    get_py_type_for_scalar_hierarchy,
    get_py_type_scalar_match_rank,
    maybe_get_protocol_for_py_type,
)


__all__ = (
    "MODEL_SUBSTRATE_MODULE",
    "AnyEnum",
    "AnyPropertyDescriptor",
    "AnyTuple",
    "Array",
    "BaseScalar",
    "DateImpl",
    "DateTimeImpl",
    "DateTimeLike",
    "GelLinkModel",
    "GelModel",
    "GelModelMeta",
    "GelPrimitiveType",
    "GelSourceModel",
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
    "TimeDeltaImpl",
    "TimeImpl",
    "Tuple",
    "UUIDImpl",
    "field_descriptor",
    "get_py_base_for_scalar",
    "get_py_type_for_scalar",
    "get_py_type_for_scalar_hierarchy",
    "get_py_type_scalar_match_rank",
    "maybe_get_protocol_for_py_type",
)
