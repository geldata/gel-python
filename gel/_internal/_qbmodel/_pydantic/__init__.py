# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

"""Pydantic implementation of the query builder model"""

from ._fields import (
    ComputedLink,
    ComputedLinkWithProps,
    ComputedMultiLink,
    ComputedMultiLinkWithProps,
    ComputedMultiProperty,
    ComputedProperty,
    IdProperty,
    RequiredMultiLink,
    RequiredMultiLinkWithProps,
    MultiProperty,
    OptionalComputedLink,
    OptionalComputedLinkWithProps,
    OptionalComputedProperty,
    OptionalLink,
    OptionalLinkWithProps,
    OptionalMultiLink,
    OptionalMultiLinkWithProps,
    OptionalProperty,
    Property,
    RequiredLink,
    RequiredLinkWithProps,
)

from ._models import (
    GelLinkModel,
    GelModel,
    GelModelMeta,
    GelObjectModel,
    LinkClassNamespace,
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
    "ComputedLink",
    "ComputedLinkWithProps",
    "ComputedMultiLink",
    "ComputedMultiLinkWithProps",
    "ComputedMultiProperty",
    "ComputedProperty",
    "GelLinkModel",
    "GelModel",
    "GelModelMeta",
    "GelObjectModel",
    "IdProperty",
    "LinkClassNamespace",
    "MultiProperty",
    "MultiRange",
    "OptionalComputedLink",
    "OptionalComputedLinkWithProps",
    "OptionalComputedProperty",
    "OptionalLink",
    "OptionalLinkWithProps",
    "OptionalMultiLink",
    "OptionalMultiLinkWithProps",
    "OptionalProperty",
    "Property",
    "ProxyModel",
    "PyTypeScalar",
    "Range",
    "RequiredLink",
    "RequiredLinkWithProps",
    "RequiredMultiLink",
    "RequiredMultiLinkWithProps",
    "Tuple",
)
