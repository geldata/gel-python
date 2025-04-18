# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.


from __future__ import annotations
from typing import (
    Literal,
    Generic,
    Optional,
    TypeVar,
    Union,
)

import dataclasses
import uuid

from gel import abstract
from . import _enums as enums
from . import _query


@dataclasses.dataclass(frozen=True, kw_only=True)
class Type:
    id: uuid.UUID
    kind: enums.TypeKind
    name: str
    description: Optional[str]
    is_abstract: bool


@dataclasses.dataclass(frozen=True, kw_only=True)
class InheritingType(Type):
    bases: list[uuid.UUID]


@dataclasses.dataclass(frozen=True)
class PseudoType(Type):
    kind: Literal[enums.TypeKind.Pseudo]


@dataclasses.dataclass(frozen=True, kw_only=True)
class ScalarType(InheritingType):
    kind: Literal[enums.TypeKind.Scalar]
    is_seq: bool
    enum_values: Optional[list[str]] = None
    material_id: Optional[uuid.UUID] = None
    cast_type: Optional[uuid.UUID] = None


@dataclasses.dataclass(frozen=True)
class ObjectType(InheritingType):
    kind: Literal[enums.TypeKind.Object]
    union_of: list[dict[str, uuid.UUID]]
    intersection_of: list[uuid.UUID]
    pointers: list[Pointer]
    backlinks: list[Backlink]
    exclusives: list[dict[str, Pointer]]


@dataclasses.dataclass(frozen=True, kw_only=True)
class ArrayType(Type):
    kind: Literal[enums.TypeKind.Array]
    array_element_id: uuid.UUID


@dataclasses.dataclass(frozen=True, kw_only=True)
class RangeType(InheritingType):
    kind: Literal[enums.TypeKind.Range]
    range_element_id: uuid.UUID


@dataclasses.dataclass(frozen=True, kw_only=True)
class MultiRangeType(InheritingType):
    kind: Literal[enums.TypeKind.MultiRange]
    multirange_element_id: uuid.UUID


@dataclasses.dataclass(frozen=True, kw_only=True)
class TupleElement:
    name: str
    type_id: uuid.UUID


@dataclasses.dataclass(frozen=True)
class TupleType(Type):
    kind: Literal[enums.TypeKind.Tuple]
    tuple_elements: list[TupleElement]


@dataclasses.dataclass(frozen=True)
class NamedTupleType(Type):
    kind: Literal[enums.TypeKind.NamedTuple]
    tuple_elements: list[TupleElement]


PrimitiveType = Union[
    ScalarType,
    ArrayType,
    TupleType,
    NamedTupleType,
    RangeType,
    MultiRangeType,
]

AnyType = Union[
    PseudoType,
    PrimitiveType,
    ObjectType,
]

Types = dict[uuid.UUID, AnyType]


@dataclasses.dataclass(frozen=True)
class Pointer:
    card: enums.Cardinality
    kind: enums.PointerKind
    name: str
    target_id: uuid.UUID
    is_exclusive: bool
    is_computed: bool
    is_readonly: bool
    has_default: bool
    pointers: Optional[list[Pointer]] = None


@dataclasses.dataclass(frozen=True)
class Backlink(Pointer):
    kind = enums.PointerKind.Link
    pointers = None
    stub = None


def fetch_types(
    db: abstract.ReadOnlyExecutor,
    schema_part: enums.SchemaPart,
) -> Types:
    builtin = schema_part is enums.SchemaPart.STD
    types: list[AnyType] = db.query(_query.TYPES, builtin=builtin)
    result = {}
    for t in types:
        result[t.id] = t
    return result
