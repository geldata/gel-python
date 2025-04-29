#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2022-present MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


import dataclasses
import typing
import uuid

from . import enums


@dataclasses.dataclass(frozen=True)
class AnyType:
    desc_id: uuid.UUID
    name: typing.Optional[str]


@dataclasses.dataclass(frozen=True)
class Element:
    type: AnyType
    cardinality: enums.Cardinality
    is_implicit: bool
    kind: enums.ElementKind


@dataclasses.dataclass(frozen=True)
class SequenceType(AnyType):
    element_type: AnyType


@dataclasses.dataclass(frozen=True)
class SetType(SequenceType):
    pass


@dataclasses.dataclass(frozen=True)
class ObjectType(AnyType):
    elements: typing.Dict[str, Element]


@dataclasses.dataclass(frozen=True)
class BaseScalarType(AnyType):
    name: str


@dataclasses.dataclass(frozen=True)
class ScalarType(AnyType):
    base_type: BaseScalarType


@dataclasses.dataclass(frozen=True)
class TupleType(AnyType):
    element_types: typing.Tuple[AnyType]


@dataclasses.dataclass(frozen=True)
class NamedTupleType(AnyType):
    element_types: typing.Dict[str, AnyType]


@dataclasses.dataclass(frozen=True)
class ArrayType(SequenceType):
    pass


@dataclasses.dataclass(frozen=True)
class EnumType(AnyType):
    members: typing.Tuple[str]


@dataclasses.dataclass(frozen=True)
class SparseObjectType(ObjectType):
    pass


@dataclasses.dataclass(frozen=True)
class RangeType(AnyType):
    value_type: AnyType


@dataclasses.dataclass(frozen=True)
class MultiRangeType(AnyType):
    value_type: AnyType
