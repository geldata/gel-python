# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.


from __future__ import annotations
from typing import NamedTuple, TypeVar
from typing_extensions import dataclass_transform

import dataclasses
import functools
import pathlib
import uuid

from gel._internal import _edgeql


class QualName(NamedTuple):
    module: str
    name: str


class SchemaPath(pathlib.PurePosixPath):
    @classmethod
    def from_schema_name(cls, name: str) -> SchemaPath:
        return SchemaPath(*name.split("::"))

    def common_parts(self, other: SchemaPath) -> list[str]:
        prefix = []
        for a, b in zip(self.parts, other.parts, strict=False):
            if a == b:
                prefix.append(a)
            else:
                break

        return prefix

    def has_prefix(self, other: SchemaPath) -> bool:
        return self.parts[: len(other.parts)] == other.parts

    def as_schema_name(self) -> str:
        return "::".join(self.parts)

    def as_quoted_schema_name(self) -> str:
        return "::".join(_edgeql.quote_ident(p) for p in self.parts)

    def as_code(self, clsname: str = "SchemaPath") -> str:
        return f"{clsname}({', '.join(repr(p) for p in self.parts)})"


def parse_name(name: str) -> SchemaPath:
    return SchemaPath.from_schema_name(name)


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
        return parse_name(self.name)

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
