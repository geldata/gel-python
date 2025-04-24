# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.


import enum


class StrEnum(str, enum.Enum):
    pass


class SchemaPart(enum.Enum):
    STD = enum.auto()
    USER = enum.auto()


class Cardinality(StrEnum):
    AtMostOne = "AtMostOne"
    One = "One"
    Many = "Many"
    AtLeastOne = "AtLeastOne"
    Empty = "Empty"

    def is_multi(self) -> bool:
        return self in {
            Cardinality.AtLeastOne._value_,
            Cardinality.Many._value_,
        }


class TypeKind(StrEnum):
    Array = "Array"
    Enum = "Enum"
    MultiRange = "MultiRange"
    NamedTuple = "NamedTuple"
    Object = "Object"
    Range = "Range"
    Scalar = "Scalar"
    Tuple = "Tuple"
    Pseudo = "Pseudo"


class PointerKind(StrEnum):
    Link = "Link"
    Property = "Property"
