# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

"""Pathlib-like implementation for qualified schema names"""

from __future__ import annotations
from typing import Any

import pathlib
import sys

from gel._internal import _edgeql


if sys.version_info >= (3, 12):

    class _SchemaPathParser:
        sep = "::"
        altsep: str | None = None

        if sys.version_info >= (3, 13):

            def __init__(self) -> None:
                self._impl = pathlib.PurePosixPath.parser  # type: ignore [attr-defined]
        else:

            def __init__(self) -> None:
                self._impl = pathlib.PurePosixPath._flavour  # type: ignore [attr-defined]

        def splitroot(self, part: str, sep: str = sep) -> tuple[str, str, str]:
            return "", "", part

        def join(self, *parts: str) -> str:
            return self.sep.join(parts)

        def __getattr__(self, name: str) -> Any:
            return getattr(self._impl, name)

    class _SchemaPath(pathlib.PurePosixPath):
        parser = _SchemaPathParser()
        _flavour = parser
else:

    class _SchemaPathParser(pathlib._PosixFlavour):  # type: ignore [name-defined, misc]
        sep = "::"
        altsep: str | None = None

        def splitroot(self, part: str, sep: str = sep) -> tuple[str, str, str]:
            return "", "", part

        def join(self, parts: list[str]) -> str:
            return self.sep.join(parts)

    class _SchemaPath(pathlib.PurePosixPath):
        _flavour = _SchemaPathParser()


class SchemaPath(_SchemaPath):
    @classmethod
    def from_schema_name(cls, name: str) -> SchemaPath:
        parts = name.split("::")
        return SchemaPath(*parts)

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
