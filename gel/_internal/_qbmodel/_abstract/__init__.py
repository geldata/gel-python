# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

"""Base types used to implement class-based query builders."""

from __future__ import annotations

from ._base import (
    DEFAULT_VALUE,
    AbstractGelLinkModel,
    AbstractGelModel,
    AbstractGelModelMeta,
    AbstractGelSourceModel,
    DefaultValue,
    GelType,
    GelTypeMeta,
    PointerInfo,
)

from ._descriptors import (
    AbstractGelProxyModel,
    AnyLinkDescriptor,
    AnyPropertyDescriptor,
    ComputedMultiLinkDescriptor,
    ComputedMultiPropertyDescriptor,
    ComputedPropertyDescriptor,
    GelLinkModelDescriptor,
    LinkDescriptor,
    ModelFieldDescriptor,
    MultiLinkDescriptor,
    MultiLinkWithPropsDescriptor,
    MultiPropertyDescriptor,
    OptionalLinkDescriptor,
    OptionalPointerDescriptor,
    OptionalPropertyDescriptor,
    PointerDescriptor,
    PropertyDescriptor,
    field_descriptor,
)

from ._distinct_list import (
    AbstractDistinctList,
    DistinctList,
    ProxyDistinctList,
)

from ._expressions import (
    empty_set_if_none,
)

from ._globals import (
    Global,
)

from ._methods import (
    BaseGelModel,
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
    get_py_type_from_gel_type,
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
    "AbstractDistinctList",
    "AbstractGelLinkModel",
    "AbstractGelModel",
    "AbstractGelModelMeta",
    "AbstractGelProxyModel",
    "AbstractGelSourceModel",
    "AnyEnum",
    "AnyLinkDescriptor",
    "AnyNamedTuple",
    "AnyPropertyDescriptor",
    "AnyTuple",
    "Array",
    "BaseGelModel",
    "ComputedMultiLinkDescriptor",
    "ComputedMultiPropertyDescriptor",
    "ComputedPropertyDescriptor",
    "DateImpl",
    "DateTimeImpl",
    "DateTimeLike",
    "DefaultValue",
    "DistinctList",
    "GelLinkModelDescriptor",
    "GelPrimitiveType",
    "GelScalarType",
    "GelType",
    "GelTypeMeta",
    "Global",
    "JSONImpl",
    "LinkDescriptor",
    "ModelFieldDescriptor",
    "MultiLinkDescriptor",
    "MultiLinkWithPropsDescriptor",
    "MultiPropertyDescriptor",
    "MultiRange",
    "OptionalLinkDescriptor",
    "OptionalPointerDescriptor",
    "OptionalPropertyDescriptor",
    "PointerDescriptor",
    "PointerInfo",
    "PropertyDescriptor",
    "ProxyDistinctList",
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
    "get_py_type_from_gel_type",
    "get_py_type_scalar_match_rank",
    "get_py_type_typecheck_meta_bases",
    "get_scalar_type_disambiguation_for_mod",
    "get_scalar_type_disambiguation_for_py_type",
    "is_generic_type",
    "maybe_get_protocol_for_py_type",
)
