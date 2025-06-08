# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

"""Pydantic implementation of the query builder model"""

from ._fields import (
    ComputedMultiLink,
    ComputedMultiLinkWithProps,
    ComputedProperty,
    IdProperty,
    MultiLink,
    RequiredMultiLink,
    RequiredMultiLinkWithProps,
    MultiLinkWithProps,
    OptionalComputedLink,
    OptionalComputedLinkWithProps,
    OptionalComputedProperty,
    OptionalLink,
    OptionalLinkWithProps,
    OptionalProperty,
    Property,
)

from ._models import (
    GelLinkModel,
    GelModel,
    GelModelMeta,
    LinkClassNamespace,
    LinkPropsDescriptor,
    ProxyModel,
)

from ._types import (
    Array,
    MultiRange,
    Range,
    Tuple,
    PyTypeScalar,
)


__all__ = (
    "Array",
    "ComputedMultiLink",
    "ComputedMultiLinkWithProps",
    "ComputedProperty",
    "GelLinkModel",
    "GelModel",
    "GelModelMeta",
    "IdProperty",
    "LinkClassNamespace",
    "LinkPropsDescriptor",
    "MultiLink",
    "MultiLinkWithProps",
    "MultiRange",
    "OptionalComputedLink",
    "OptionalComputedLinkWithProps",
    "OptionalComputedProperty",
    "OptionalLink",
    "OptionalLinkWithProps",
    "OptionalProperty",
    "Property",
    "ProxyModel",
    "PyTypeScalar",
    "Range",
    "RequiredMultiLink",
    "RequiredMultiLinkWithProps",
    "Tuple",
)
