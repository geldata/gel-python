# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.


from __future__ import annotations
from typing import (
    TYPE_CHECKING,
    Literal,
    TypeGuard,
)

import dataclasses
import uuid

from . import _enums as enums
from . import _query

if TYPE_CHECKING:
    from gel import abstract


@dataclasses.dataclass(frozen=True, kw_only=True)
class TypeRef:
    id: uuid.UUID


@dataclasses.dataclass(frozen=True, kw_only=True)
class Type:
    id: uuid.UUID
    kind: enums.TypeKind
    name: str
    description: str | None
    builtin: bool
    internal: bool


@dataclasses.dataclass(frozen=True, kw_only=True)
class InheritingType(Type):
    abstract: bool
    final: bool
    bases: list[TypeRef]
    ancestors: list[TypeRef]


@dataclasses.dataclass(frozen=True)
class PseudoType(Type):
    kind: Literal[enums.TypeKind.Pseudo]


@dataclasses.dataclass(frozen=True, kw_only=True)
class ScalarType(InheritingType):
    kind: Literal[enums.TypeKind.Scalar]
    is_seq: bool
    enum_values: list[str] | None = None
    material_id: uuid.UUID | None = None
    cast_type: uuid.UUID | None = None


@dataclasses.dataclass(frozen=True)
class ObjectType(InheritingType):
    kind: Literal[enums.TypeKind.Object]
    union_of: list[TypeRef]
    intersection_of: list[TypeRef]
    compound_type: bool
    pointers: list[Pointer]
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


PrimitiveType = (
    ScalarType
    | ArrayType
    | TupleType
    | NamedTupleType
    | RangeType
    | MultiRangeType
)

AnyType = (
    PseudoType
    | PrimitiveType
    | ObjectType
)

Types = dict[uuid.UUID, AnyType]


def is_pseudo_type(t: AnyType) -> TypeGuard[PseudoType]:
    return t.kind == enums.TypeKind.Pseudo


def is_object_type(t: AnyType) -> TypeGuard[ObjectType]:
    return t.kind == enums.TypeKind.Object


def is_scalar_type(t: AnyType) -> TypeGuard[ScalarType]:
    return t.kind == enums.TypeKind.Scalar


def is_non_enum_scalar_type(t: AnyType) -> TypeGuard[ScalarType]:
    return t.kind == enums.TypeKind.Scalar and not t.enum_values


def is_array_type(t: AnyType) -> TypeGuard[ArrayType]:
    return t.kind == enums.TypeKind.Array


def is_range_type(t: AnyType) -> TypeGuard[RangeType]:
    return t.kind == enums.TypeKind.Range


def is_multi_range_type(t: AnyType) -> TypeGuard[MultiRangeType]:
    return t.kind == enums.TypeKind.MultiRange


def is_tuple_type(t: AnyType) -> TypeGuard[TupleType]:
    return t.kind == enums.TypeKind.Tuple


def is_named_tuple_type(t: AnyType) -> TypeGuard[NamedTupleType]:
    return t.kind == enums.TypeKind.NamedTuple


def is_primitive_type(t: AnyType) -> TypeGuard[PrimitiveType]:
    return t.kind not in {enums.TypeKind.Object, enums.TypeKind.Pseudo}


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
    pointers: list[Pointer] | None = None


def is_link(p: Pointer) -> bool:
    return p.kind == enums.PointerKind.Link


def is_property(p: Pointer) -> bool:
    return p.kind == enums.PointerKind.Property


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
