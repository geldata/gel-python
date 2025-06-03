# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.


from __future__ import annotations

from typing import final

from gel._internal._polyfills import StrEnum


@final
class Cardinality(StrEnum):
    AtMostOne = "AtMostOne"
    One = "One"
    Many = "Many"
    AtLeastOne = "AtLeastOne"
    Empty = "Empty"

    def is_multi(self) -> bool:
        return self in {
            Cardinality.AtLeastOne,
            Cardinality.Many,
        }

    def is_optional(self) -> bool:
        return self in {
            Cardinality.AtMostOne,
            Cardinality.Many,
            Cardinality.Empty,
        }


@final
class PointerKind(StrEnum):
    Link = "Link"
    Property = "Property"


__all__ = (
    "Cardinality",
    "PointerKind",
)
