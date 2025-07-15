# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.


from __future__ import annotations
from typing import (
    TYPE_CHECKING,
    Any,
    Literal,
    TypeGuard,
    TypeVar,
    cast,
)
from typing_extensions import Self, TypeAliasType
from collections.abc import Iterable, Mapping

import abc
import dataclasses
import functools
import uuid
from collections import defaultdict

from gel._internal import _dataclass_extras
from gel._internal import _edgeql
from gel._internal._schemapath import SchemaPath

from . import _query
from ._base import struct, sobject, SchemaObject
from ._enums import Cardinality, PointerKind, SchemaPart, TypeKind

if TYPE_CHECKING:
    from gel import abstract


Indirection = TypeAliasType("Indirection", tuple[str | tuple[str, int], ...])
Schema = TypeAliasType("Schema", Mapping[str, "Type"])


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

    def specialize(
        self,
        spec: Mapping[Indirection, tuple[Type, Type]],
        *,
        schema: Mapping[str, Type],
    ) -> Type:
        if len(spec) > 1:
            raise ValueError(
                f"expected a single type when specializing "
                f"a non-container type, got {len(spec)}"
            )

        path, (type_, replacement) = next(iter(spec.items()))
        if type_ != self:
            raise ValueError(f"expected source type to be {self}, got {type_}")

        if path != ():
            raise ValueError(
                f"expected an empty type path when specializing "
                f"a non-container type, got {path}"
            )

        return replacement


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

    def descendants(self, schema: Mapping[str, Type]) -> tuple[Self, ...]:
        cls = type(self)
        result: tuple[Self, ...] | None = getattr(self, "_descendants", None)
        if result is None:
            descendants: list[Self] = []
            for t in schema.values():
                if isinstance(t, cls):
                    anc = t.ancestors
                    if anc and any(self.id == t.id for t in anc):
                        descendants.append(t)

            descendants.sort(key=lambda t: t.name)
            result = tuple(descendants)

        return result

    @functools.cached_property
    def ancestors_ids(self) -> frozenset[str]:
        return frozenset(t.id for t in self.ancestors)

    def get_ancestors(self, schema: Schema) -> tuple[Self, ...]:
        return tuple(cast("Self", schema[t.id]) for t in self.ancestors)

    def issubclass(self, parent: Self) -> bool:
        return self == parent or parent.id in self.ancestors_ids


_InheritingType_T = TypeVar("_InheritingType_T", bound=InheritingType)


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
        return SchemaPath.from_segments(self.name)


@struct
class HomogeneousCollectionType(CollectionType):
    element_type: Type | None = None

    def get_id_and_name(self, element_type: Type) -> tuple[str, str]:
        cls = type(self)
        raise NotImplementedError(f"{cls.__qualname__}.get_id_and_name()")

    def get_element_type(self, schema: Schema) -> Type:
        if self.element_type is not None:
            return self.element_type
        else:
            return schema[self._element_type_id]

    @functools.cached_property
    def _element_type_id(self) -> str:
        raise NotImplementedError("_element_type_id")

    def specialize(
        self,
        spec: Mapping[Indirection, tuple[Type, Type]],
        *,
        schema: Mapping[str, Type],
    ) -> Type:
        element_spec: dict[Indirection, tuple[Type, Type]] = {}

        for path, mapping in spec.items():
            if path == ():
                # Specializing self
                return super().specialize(spec, schema=schema)
            elif path[0] == "__element_type__":
                element_spec[path[1:]] = mapping
            else:
                raise ValueError(
                    f"unexpected type path when specializing "
                    f"a homogeneous container type: {path}"
                )

        element_type = self.get_element_type(schema).specialize(
            element_spec,
            schema=schema,
        )
        id_, name = self.get_id_and_name(element_type)
        return dataclasses.replace(
            self,
            id=id_,
            name=name,
            element_type=element_type,
        )

    def contained_generics(
        self,
        schema: Mapping[str, Type],
        *,
        path: Indirection = (),
    ) -> dict[Type, set[Indirection]]:
        element_type = self.get_element_type(schema)
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
        return isinstance(other, type(self)) and self.get_element_type(
            schema
        ).assignable_from(
            other.get_element_type(schema),
            schema=schema,
            generics=generics,
            generic_bindings=generic_bindings,
            _path=(*path, "__element_type__"),
        )


@struct
class HeterogeneousCollectionType(CollectionType):
    element_types: tuple[Type, ...] | None = None

    def get_id_and_name(
        self, element_types: tuple[Type, ...]
    ) -> tuple[str, str]:
        cls = type(self)
        raise NotImplementedError(f"{cls.__qualname__}.get_id_and_name()")

    def get_element_types(self, schema: Schema) -> tuple[Type, ...]:
        if self.element_types is not None:
            return self.element_types
        else:
            return tuple(schema[el_tid] for el_tid in self._element_type_ids)

    @functools.cached_property
    def _element_type_ids(self) -> list[str]:
        raise NotImplementedError("_element_type_ids")

    def specialize(
        self,
        spec: Mapping[Indirection, tuple[Type, Type]],
        *,
        schema: Mapping[str, Type],
    ) -> Type:
        element_specs: defaultdict[
            int, dict[Indirection, tuple[Type, Type]]
        ] = defaultdict(dict)

        for path, mapping in spec.items():
            match path:
                case ():
                    # Specializing self
                    return super().specialize(spec, schema=schema)
                case (("__element_types__", int(n)), *tail):
                    element_specs[n][tuple(tail)] = mapping
                case _:
                    raise ValueError(
                        f"unexpected type path when specializing "
                        f"a heterogeneous container type: {path}"
                    )

        element_types: list[Type] = []
        for i, element_type in enumerate(self.get_element_types(schema)):
            if element_spec := element_specs.get(i):
                element_types.append(
                    element_type.specialize(
                        element_spec,
                        schema=schema,
                    )
                )
            else:
                element_types.append(element_type)

        element_types_tup = tuple(element_types)
        id_, name = self.get_id_and_name(element_types_tup)
        return dataclasses.replace(
            self,
            id=id_,
            name=name,
            element_types=element_types_tup,
        )

    def contained_generics(
        self,
        schema: Mapping[str, Type],
        *,
        path: Indirection = (),
    ) -> dict[Type, set[Indirection]]:
        el_types: defaultdict[Type, set[Indirection]] = defaultdict(set)
        for i, el_type in enumerate(self.get_element_types(schema)):
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
    def _element_type_id(self) -> str:
        return self.array_element_id

    def get_id_and_name(self, element_type: Type) -> tuple[str, str]:
        id_, name = _edgeql.get_array_type_id_and_name(element_type.name)
        return str(id_), name


@struct
class RangeType(HomogeneousCollectionType):
    kind: Literal[TypeKind.Range]
    range_element_id: str

    @functools.cached_property
    def _element_type_id(self) -> str:
        return self.range_element_id

    def get_id_and_name(self, element_type: Type) -> tuple[str, str]:
        id_, name = _edgeql.get_range_type_id_and_name(element_type.name)
        return str(id_), name


@struct
class MultiRangeType(HomogeneousCollectionType):
    kind: Literal[TypeKind.MultiRange]
    multirange_element_id: str

    @functools.cached_property
    def _element_type_id(self) -> str:
        return self.multirange_element_id

    def get_id_and_name(self, element_type: Type) -> tuple[str, str]:
        id_, name = _edgeql.get_multirange_type_id_and_name(element_type.name)
        return str(id_), name


@struct
class TupleElement:
    name: str
    type_id: str


@struct
class _TupleType(HeterogeneousCollectionType):
    tuple_elements: tuple[TupleElement, ...]

    @functools.cached_property
    def _element_type_ids(self) -> list[str]:
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

    def get_id_and_name(
        self, element_types: tuple[Type, ...]
    ) -> tuple[str, str]:
        id_, name = _edgeql.get_tuple_type_id_and_name(
            el.name for el in element_types
        )
        return str(id_), name


@struct
class NamedTupleType(_TupleType):
    kind: Literal[TypeKind.NamedTuple]

    def get_id_and_name(
        self, element_types: tuple[Type, ...]
    ) -> tuple[str, str]:
        id_, name = _edgeql.get_named_tuple_type_id_and_name(
            {
                el.name: el_type.name
                for el, el_type in zip(
                    self.tuple_elements, element_types, strict=True
                )
            }
        )
        return str(id_), name


def compare_type_generality(a: Type, b: Type, *, schema: Schema) -> int:
    """Return 1 if a is more general than b, -1 if a is more specific
    than b, and 0 if a and b are considered of equal generality."""
    if a == b:
        return 0
    elif a.assignable_from(b, schema=schema):
        return 1
    elif b.assignable_from(a, schema=schema):
        return -1
    elif isinstance(a, InheritingType) and isinstance(b, InheritingType):
        common = get_nearest_common_ancestors([a, b], schema)
        if common:
            a_ancestors = [t.id for t in a.ancestors]
            b_ancestors = [t.id for t in b.ancestors]

            for ancestor in common:
                a_distance = a_ancestors.index(ancestor.id)
                b_distance = b_ancestors.index(ancestor.id)

                if a_distance < b_distance:
                    return 1
                elif a_distance > b_distance:
                    return -1

        if "std::anyreal" in {t.name for t in common}:
            # Special case for floats vs non-floats where we consider
            # floats to be more general than ints (alas their ancestry
            # distance in Gel is equal).
            a_is_float = any(
                t.name == "std::anyfloat" for t in a.get_ancestors(schema)
            )
            b_is_float = any(
                t.name == "std::anyfloat" for t in b.get_ancestors(schema)
            )
            return a_is_float - b_is_float

    return 0


def get_nearest_common_ancestors(
    types: Iterable[_InheritingType_T],
    schema: Schema,
) -> list[_InheritingType_T]:
    # First, find the intersection of parents
    types = [*types]
    first = [types[0]]
    first.extend(types[0].get_ancestors(schema))
    common = set(first).intersection(
        *({*c.get_ancestors(schema)} | {c} for c in types[1:])
    )
    nearests: list[_InheritingType_T] = []
    # Then find the common ancestors that don't have any subclasses that
    # are also nearest common ancestors.
    for anc in sorted(common, key=first.index):
        if not any(x.issubclass(anc) for x in nearests):
            nearests.append(anc)

    return nearests


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


def is_abstract_type(t: Type) -> TypeGuard[InheritingType]:
    return isinstance(t, InheritingType) and t.abstract


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
