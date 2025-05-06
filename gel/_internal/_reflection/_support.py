# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

from __future__ import annotations
from typing import NamedTuple

import pathlib


class QualName(NamedTuple):
    module: str
    name: str


class SchemaPath(pathlib.PurePath):
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


def parse_name(name: str) -> SchemaPath:
    return SchemaPath.from_schema_name(name)
