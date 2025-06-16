# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.


from __future__ import annotations
from typing import (
    TYPE_CHECKING,
    Any,
    Literal,
    TypeGuard,
)
from typing_extensions import TypeAliasType

import abc
import functools
import uuid
from collections import defaultdict

from gel._internal import _dataclass_extras
from gel._internal import _edgeql

from . import _query
from ._base import struct, sobject, SchemaObject, SchemaPath
from ._enums import Cardinality, PointerKind, SchemaPart, TypeKind

if TYPE_CHECKING:
    from collections.abc import Mapping
    from gel import abstract


Indirection = TypeAliasType("Indirection", tuple[str | tuple[str, int], ...])


@struct
class TypeRef:
    id: str


@sobject
class Type(SchemaObject, abc.ABC):
    kind: TypeKind
    builtin: bool
    internal: bool

    @functools.cached_property
    def edgeql(self) -> str:
        return self.schemapath.as_quoted_schema_name()

    @functools.cached_property
    def generic(self) -> bool:
        sp = self.schemapath
        return (
            len(sp.parts) > 1
            and sp.parents[0].name == "std"
            and sp.name.startswith("any")
        )

    def assignable_from(
        self,
        other: Type,
        *,
        schema: Mapping[str, Type],
        generics: Mapping[Indirection, Type] | None = None,
        generic_bindings: dict[Type, Type] | None = None,
        _path: Indirection = (),
    ) -> bool:
        if generics is None:
            generics = {}
        if generic_bindings is None:
            generic_bindings = {}

        generic = generics.get(_path)

        if generic is not None:
            this = generic_bindings.get(self, self)
            other = generic_bindings.get(other, other)
        else:
            this = self

        if this == other:
            return True
        else:
            assignable = this._assignable_from(
                other,
                schema=schema,
                generics=generics,
                generic_bindings=generic_bindings,
                path=_path,
            )
            if assignable and generic is not None:
                previous = generic_bindings.get(generic)
                if previous is None:
                    generic_bindings[generic] = other
                else:
                    # Generic is bound to incompatible types.
                    assignable = False

            return assignable

    def _assignable_from(
        self,
        other: Type,
        *,
        schema: Mapping[str, Type],
        generics: Mapping[Indirection, Type],
        generic_bindings: dict[Type, Type],
        path: Indirection,
    ) -> bool:
        raise NotImplementedError("_assignable_from()")

    def contained_generics(
        self,
        schema: Mapping[str, Type],
        *,
        path: Indirection = (),
    ) -> dict[Type, set[Indirection]]:
        if self.generic:
            return {self: {path}}
        else:
            return {}


@struct
class InheritingType(Type):
    abstract: bool
    final: bool
    bases: tuple[TypeRef, ...]
    ancestors: tuple[TypeRef, ...]

    @functools.cached_property
    def edgeql(self) -> str:
        return self.schemapath.as_quoted_schema_name()

    def _assignable_from(
        self,
        other: Type,
        *,
        schema: Mapping[str, Type],
        generics: Mapping[Indirection, Type],
        generic_bindings: dict[Type, Type],
        path: Indirection = (),
    ) -> bool:
        return self == other or (
            isinstance(other, InheritingType)
            and any(a.id == self.id for a in other.ancestors)
        )


@struct
class PseudoType(Type):
    kind: Literal[TypeKind.Pseudo]

    @functools.cached_property
    def generic(self) -> bool:
        return True

    def _assignable_from(
        self,
        other: Type,
        *,
        schema: Mapping[str, Type],
        generics: Mapping[Indirection, Type],
        generic_bindings: dict[Type, Type],
        path: Indirection = (),
    ) -> bool:
        return (
            self.name == "anytype"
            or (self.name == "anytuple" and isinstance(other, _TupleType))
            or (self.name == "anyobject" and isinstance(other, ObjectType))
        )


@struct
class ScalarType(InheritingType):
    kind: Literal[TypeKind.Scalar]
    is_seq: bool
    enum_values: tuple[str, ...] | None = None
    material_id: str | None = None
    cast_type: str | None = None


@struct
class ObjectType(InheritingType):
    kind: Literal[TypeKind.Object]
    union_of: tuple[TypeRef, ...]
    intersection_of: tuple[TypeRef, ...]
    compound_type: bool
    pointers: tuple[Pointer, ...]


class CollectionType(Type):
    @functools.cached_property
    def edgeql(self) -> str:
        return str(self.schemapath)

    @functools.cached_property
    def schemapath(self) -> SchemaPath:
        return SchemaPath(self.name)


class HomogeneousCollectionType(CollectionType):
    @functools.cached_property
    def element_type_id(self) -> str:
        raise NotImplementedError("element_type_id")

    def contained_generics(
        self,
        schema: Mapping[str, Type],
        *,
        path: Indirection = (),
    ) -> dict[Type, set[Indirection]]:
        element_type = schema[self.element_type_id]
        return element_type.contained_generics(
            schema,
            path=(*path, "__element_type__"),
        )

    def _assignable_from(
        self,
        other: Type,
        *,
        schema: Mapping[str, Type],
        generics: Mapping[Indirection, Type],
        generic_bindings: dict[Type, Type],
        path: Indirection = (),
    ) -> bool:
        return isinstance(other, type(self)) and schema[
            self.element_type_id
        ].assignable_from(
            schema[other.element_type_id],
            schema=schema,
            generics=generics,
            generic_bindings=generic_bindings,
            _path=(*path, "__element_type__"),
        )


class HeterogeneousCollectionType(CollectionType):
    @functools.cached_property
    def element_type_ids(self) -> list[str]:
        raise NotImplementedError("element_type_ids")

    def contained_generics(
        self,
        schema: Mapping[str, Type],
        *,
        path: Indirection = (),
    ) -> dict[Type, set[Indirection]]:
        el_types: defaultdict[Type, set[Indirection]] = defaultdict(set)
        for i, el_tid in enumerate(self.element_type_ids):
            el_type = schema[el_tid]
            for t, t_paths in el_type.contained_generics(
                schema, path=(*path, ("__element_types__", i))
            ).items():
                el_types[t].update(t_paths)

        return el_types


@struct
class ArrayType(HomogeneousCollectionType):
    kind: Literal[TypeKind.Array]
    array_element_id: str

    @functools.cached_property
    def element_type_id(self) -> str:
        return self.array_element_id


@struct
class RangeType(HomogeneousCollectionType):
    kind: Literal[TypeKind.Range]
    range_element_id: str

    @functools.cached_property
    def element_type_id(self) -> str:
        return self.range_element_id


@struct
class MultiRangeType(HomogeneousCollectionType):
    kind: Literal[TypeKind.MultiRange]
    multirange_element_id: str

    @functools.cached_property
    def element_type_id(self) -> str:
        return self.multirange_element_id


@struct
class TupleElement:
    name: str
    type_id: str


@struct
class _TupleType(HeterogeneousCollectionType):
    tuple_elements: tuple[TupleElement, ...]

    @functools.cached_property
    def element_type_ids(self) -> list[str]:
        return [el.type_id for el in self.tuple_elements]

    def _assignable_from(
        self,
        other: Type,
        *,
        schema: Mapping[str, Type],
        generics: Mapping[Indirection, Type],
        generic_bindings: dict[Type, Type],
        path: Indirection = (),
    ) -> bool:
        return (
            isinstance(other, _TupleType)
            and len(self.tuple_elements) == len(other.tuple_elements)
            and all(
                schema[self_el.type_id].assignable_from(
                    schema[other_el.type_id],
                    schema=schema,
                    generics=generics,
                    generic_bindings=generic_bindings,
                    _path=(*path, ("__element_types__", i)),
                )
                for i, (self_el, other_el) in enumerate(
                    zip(self.tuple_elements, other.tuple_elements, strict=True)
                )
            )
        )


@struct
class TupleType(_TupleType):
    kind: Literal[TypeKind.Tuple]


@struct
class NamedTupleType(_TupleType):
    kind: Literal[TypeKind.NamedTuple]


PrimitiveType = (
    ScalarType
    | ArrayType
    | TupleType
    | NamedTupleType
    | RangeType
    | MultiRangeType
)

Types = dict[str, Type]


_kind_to_class: dict[TypeKind, type[Type]] = {
    TypeKind.Array: ArrayType,
    TypeKind.MultiRange: MultiRangeType,
    TypeKind.NamedTuple: NamedTupleType,
    TypeKind.Object: ObjectType,
    TypeKind.Pseudo: PseudoType,
    TypeKind.Range: RangeType,
    TypeKind.Scalar: ScalarType,
    TypeKind.Tuple: TupleType,
}


def is_pseudo_type(t: Type) -> TypeGuard[PseudoType]:
    return isinstance(t, PseudoType)


def is_object_type(t: Type) -> TypeGuard[ObjectType]:
    return isinstance(t, ObjectType)


def is_scalar_type(t: Type) -> TypeGuard[ScalarType]:
    return isinstance(t, ScalarType)


def is_non_enum_scalar_type(t: Type) -> TypeGuard[ScalarType]:
    return isinstance(t, ScalarType) and not t.enum_values


def is_array_type(t: Type) -> TypeGuard[ArrayType]:
    return isinstance(t, ArrayType)


def is_range_type(t: Type) -> TypeGuard[RangeType]:
    return isinstance(t, RangeType)


def is_multi_range_type(t: Type) -> TypeGuard[MultiRangeType]:
    return isinstance(t, MultiRangeType)


def is_tuple_type(t: Type) -> TypeGuard[TupleType]:
    return isinstance(t, TupleType)


def is_named_tuple_type(t: Type) -> TypeGuard[NamedTupleType]:
    return isinstance(t, NamedTupleType)


def is_primitive_type(t: Type) -> TypeGuard[PrimitiveType]:
    return not isinstance(t, (ObjectType, PseudoType))


@sobject
class Pointer(SchemaObject):
    card: Cardinality
    kind: PointerKind
    target_id: str
    is_exclusive: bool
    is_computed: bool
    is_readonly: bool
    has_default: bool
    pointers: tuple[Pointer, ...] | None = None


def is_link(p: Pointer) -> bool:
    return p.kind == PointerKind.Link


def is_property(p: Pointer) -> bool:
    return p.kind == PointerKind.Property


def fetch_types(
    db: abstract.ReadOnlyExecutor,
    schema_part: SchemaPart,
) -> Types:
    builtin = schema_part is SchemaPart.STD
    types: list[Type] = db.query(_query.TYPES, builtin=builtin)
    result = {}
    for t in types:
        cls = _kind_to_class[t.kind]
        replace: dict[str, Any] = {}
        if issubclass(cls, CollectionType):
            replace["name"] = _edgeql.unmangle_unqual_name(t.name)
        vt = _dataclass_extras.coerce_to_dataclass(
            cls,
            t,
            cast_map={str: (uuid.UUID,)},
            replace=replace,
        )
        result[vt.id] = vt

    return result
