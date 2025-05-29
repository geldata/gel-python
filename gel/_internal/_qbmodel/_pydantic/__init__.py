# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

"""Pydantic implementation of the query builder model"""

from ._fields import (
    ComputedProperty,
    IdProperty,
    MultiLink,
    MultiLinkWithProps,
    OptionalLink,
    OptionalLinkWithProps,
    OptionalProperty,
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
    "OptionalLink",
    "OptionalLinkWithProps",
    "OptionalProperty",
    "ProxyModel",
    "PyTypeScalar",
    "Range",
    "Tuple",
)
