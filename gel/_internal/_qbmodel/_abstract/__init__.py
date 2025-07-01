# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

"""Base types used to implement class-based query builders."""

from __future__ import annotations

from ._base import (
    DEFAULT_VALUE,
    DefaultValue,
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

from ._expressions import (
    empty_set_if_none,
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
    AnyNamedTuple,
    AnyTuple,
    Array,
    BaseScalar,
    DateImpl,
    DateTimeImpl,
    DateTimeLike,
    GelPrimitiveType,
    MultiRange,
    PyConstType,
    PyTypeScalar,
    Range,
    TimeDeltaImpl,
    TimeImpl,
    Tuple,
    UUIDImpl,
    get_py_base_for_scalar,
    get_py_type_for_scalar,
    get_py_type_for_scalar_hierarchy,
    get_py_type_scalar_match_rank,
    is_generic_type,
    maybe_get_protocol_for_py_type,
)


__all__ = (
    "DEFAULT_VALUE",
    "MODEL_SUBSTRATE_MODULE",
    "AnyEnum",
    "AnyNamedTuple",
    "AnyPropertyDescriptor",
    "AnyTuple",
    "Array",
    "BaseScalar",
    "DateImpl",
    "DateTimeImpl",
    "DateTimeLike",
    "DefaultValue",
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
    "empty_set_if_none",
    "field_descriptor",
    "get_py_base_for_scalar",
    "get_py_type_for_scalar",
    "get_py_type_for_scalar_hierarchy",
    "get_py_type_scalar_match_rank",
    "is_generic_type",
    "maybe_get_protocol_for_py_type",
)
