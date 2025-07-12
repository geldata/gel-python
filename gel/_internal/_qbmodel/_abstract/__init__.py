# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

"""Base types used to implement class-based query builders."""

from __future__ import annotations

from ._base import (
    DEFAULT_VALUE,
    DefaultValue,
    GelType,
    GelTypeMeta,
    PointerInfo,
)

from ._descriptors import (
    AnyLinkDescriptor,
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

from ._globals import (
    Global,
)

from ._objects import (
    GelLinkModel,
    GelLinkModelDescriptor,
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
    DateImpl,
    DateTimeImpl,
    DateTimeLike,
    GelPrimitiveType,
    GelScalarType,
    MultiRange,
    PyConstType,
    PyTypeScalar,
    PyTypeScalarConstraint,
    Range,
    JSONImpl,
    TimeDeltaImpl,
    TimeImpl,
    Tuple,
    UUIDImpl,
    get_base_scalars_backed_by_py_type,
    get_overlapping_py_types,
    get_scalar_type_disambiguation_for_mod,
    get_scalar_type_disambiguation_for_py_type,
    get_py_base_for_scalar,
    get_py_type_for_scalar,
    get_py_type_for_scalar_hierarchy,
    get_py_type_scalar_match_rank,
    get_py_type_typecheck_meta_bases,
    is_generic_type,
    maybe_get_protocol_for_py_type,
)


__all__ = (
    "DEFAULT_VALUE",
    "MODEL_SUBSTRATE_MODULE",
    "AnyEnum",
    "AnyLinkDescriptor",
    "AnyNamedTuple",
    "AnyPropertyDescriptor",
    "AnyTuple",
    "Array",
    "DateImpl",
    "DateTimeImpl",
    "DateTimeLike",
    "DefaultValue",
    "GelLinkModel",
    "GelLinkModelDescriptor",
    "GelModel",
    "GelModelMeta",
    "GelPrimitiveType",
    "GelScalarType",
    "GelSourceModel",
    "GelType",
    "GelTypeMeta",
    "Global",
    "JSONImpl",
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
    "PyTypeScalarConstraint",
    "Range",
    "TimeDeltaImpl",
    "TimeImpl",
    "Tuple",
    "UUIDImpl",
    "empty_set_if_none",
    "field_descriptor",
    "get_base_scalars_backed_by_py_type",
    "get_overlapping_py_types",
    "get_py_base_for_scalar",
    "get_py_type_for_scalar",
    "get_py_type_for_scalar_hierarchy",
    "get_py_type_scalar_match_rank",
    "get_py_type_typecheck_meta_bases",
    "get_scalar_type_disambiguation_for_mod",
    "get_scalar_type_disambiguation_for_py_type",
    "is_generic_type",
    "maybe_get_protocol_for_py_type",
)
