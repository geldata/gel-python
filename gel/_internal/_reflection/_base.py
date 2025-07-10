# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.


from __future__ import annotations
from typing import TypeVar
from typing_extensions import dataclass_transform

import dataclasses
import functools
import uuid

from gel._internal._schemapath import SchemaPath


_T = TypeVar("_T")


_dataclass = dataclasses.dataclass(eq=False, frozen=True, kw_only=True)


@dataclass_transform(
    frozen_default=True,
    kw_only_default=True,
)
def struct(t: type[_T]) -> type[_T]:
    return _dataclass(t)


@dataclass_transform(
    eq_default=False,
    frozen_default=True,
    kw_only_default=True,
)
def sobject(t: type[_T]) -> type[_T]:
    return _dataclass(t)


@sobject
class SchemaObject:
    id: str
    name: str
    description: str | None

    @functools.cached_property
    def schemapath(self) -> SchemaPath:
        return SchemaPath(self.name)

    @functools.cached_property
    def uuid(self) -> uuid.UUID:
        return uuid.UUID(self.id)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, type(self)):
            return NotImplemented
        else:
            return self.id == other.id

    def __hash__(self) -> int:
        return hash(self.id)
