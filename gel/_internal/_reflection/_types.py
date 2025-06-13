# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.


from __future__ import annotations
from typing import (
    TYPE_CHECKING,
    Literal,
    TypeGuard,
)

import abc
import dataclasses
import functools
import re
import uuid

from gel._internal import _dataclass_extras

from . import _enums as enums
from . import _query
from . import _support
from ._struct import struct

if TYPE_CHECKING:
    from gel import abstract


@struct
class TypeRef:
    id: str


@struct
class Type(abc.ABC):
    id: str
    kind: enums.TypeKind
    name: str
    description: str | None
    builtin: bool
    internal: bool

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Type):
            return NotImplemented
        else:
            return self.id == other.id

    def __hash__(self) -> int:
        return hash(self.id)

    @functools.cached_property
    def edgeql(self) -> str:
        return self.schemapath.as_quoted_schema_name()

    @functools.cached_property
    def schemapath(self) -> _support.SchemaPath:
        return _support.parse_name(self.name)

    @functools.cached_property
    def uuid(self) -> uuid.UUID:
        return uuid.UUID(self.id)


@struct
class InheritingType(Type):
    abstract: bool
    final: bool
    bases: tuple[TypeRef, ...]
    ancestors: tuple[TypeRef, ...]

    @functools.cached_property
    def edgeql(self) -> str:
        return self.schemapath.as_quoted_schema_name()


@struct
class PseudoType(Type):
    kind: Literal[enums.TypeKind.Pseudo]


@struct
class ScalarType(InheritingType):
    kind: Literal[enums.TypeKind.Scalar]
    is_seq: bool
    enum_values: tuple[str, ...] | None = None
    material_id: str | None = None
    cast_type: str | None = None


@struct
class ObjectType(InheritingType):
    kind: Literal[enums.TypeKind.Object]
    union_of: tuple[TypeRef, ...]
    intersection_of: tuple[TypeRef, ...]
    compound_type: bool
    pointers: tuple[Pointer, ...]


class CollectionType(Type):
    @functools.cached_property
    def schemapath(self) -> _support.SchemaPath:
        return _support.SchemaPath(re.sub(r"\|+", "::", self.name))

    @functools.cached_property
    def edgeql(self) -> str:
        return str(self.schemapath)


@struct
class ArrayType(CollectionType):
    kind: Literal[enums.TypeKind.Array]
    array_element_id: str


@struct
class RangeType(CollectionType):
    kind: Literal[enums.TypeKind.Range]
    range_element_id: str


@struct
class MultiRangeType(CollectionType):
    kind: Literal[enums.TypeKind.MultiRange]
    multirange_element_id: str


@struct
class TupleElement:
    name: str
    type_id: str


@struct
class TupleType(CollectionType):
    kind: Literal[enums.TypeKind.Tuple]
    tuple_elements: tuple[TupleElement, ...]


@struct
class NamedTupleType(CollectionType):
    kind: Literal[enums.TypeKind.NamedTuple]
    tuple_elements: tuple[TupleElement, ...]


PrimitiveType = (
    ScalarType
    | ArrayType
    | TupleType
    | NamedTupleType
    | RangeType
    | MultiRangeType
)

AnyType = PseudoType | PrimitiveType | ObjectType

Types = dict[str, AnyType]


_kind_to_class: dict[enums.TypeKind, type[Type]] = {
    enums.TypeKind.Array: ArrayType,
    enums.TypeKind.MultiRange: MultiRangeType,
    enums.TypeKind.NamedTuple: NamedTupleType,
    enums.TypeKind.Object: ObjectType,
    enums.TypeKind.Pseudo: PseudoType,
    enums.TypeKind.Range: RangeType,
    enums.TypeKind.Scalar: ScalarType,
    enums.TypeKind.Tuple: TupleType,
}


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
    id: str
    card: enums.Cardinality
    kind: enums.PointerKind
    name: str
    target_id: str
    is_exclusive: bool
    is_computed: bool
    is_readonly: bool
    has_default: bool
    pointers: tuple[Pointer, ...] | None = None

    @functools.cached_property
    def uuid(self) -> uuid.UUID:
        return uuid.UUID(self.id)


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
        vt = _dataclass_extras.coerce_to_dataclass(
            _kind_to_class[t.kind], t, cast_map={str: (uuid.UUID,)}
        )
        result[vt.id] = vt

    return result  # type: ignore [return-value]
