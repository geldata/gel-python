# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.


import enum
from typing import final

from gel._internal._polyfills._strenum import StrEnum


@final
class SchemaPart(enum.Enum):
    STD = enum.auto()
    USER = enum.auto()


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


@final
class TypeModifier(StrEnum):
    SetOf = "SetOfType"
    Optional = "OptionalType"
    Singleton = "SingletonType"


@final
class PointerKind(StrEnum):
    Link = "Link"
    Property = "Property"


@final
class OperatorKind(StrEnum):
    Infix = "Infix"
    Postfix = "Postfix"
    Prefix = "Prefix"
    Ternary = "Ternary"


@final
class CallableParamKind(StrEnum):
    Variadic = "VariadicParam"
    NamedOnly = "NamedOnlyParam"
    Positional = "PositionalParam"
