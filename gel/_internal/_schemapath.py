# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

"""Pathlib-like implementation for qualified schema names"""

from __future__ import annotations
from typing import SupportsIndex, TypeVar, overload
from typing_extensions import Self, TypeAliasType
from collections.abc import Sequence
import dataclasses

import functools
import pathlib

from gel._internal import _edgeql


_SEP = "::"


SchemaPathLike = TypeAliasType("SchemaPathLike", "str | SchemaPath")


class SchemaPath:
    @classmethod
    def from_segments(cls, *names: str) -> Self:
        """Construct a SchemaPath from individual segments.  Unlike
        the main constructor, no attempt is made to further split
        each segment.  This is useful for names that contain `::`
        in their unqualified part, such as collection types."""
        return cls._from_parsed_parts(names)

    def __init__(self, *args: SchemaPathLike) -> None:
        paths = []
        for arg in args:
            if isinstance(arg, SchemaPath):
                paths.extend(arg._raw_paths)
            elif isinstance(arg, str):
                paths.append(arg)
            else:
                raise TypeError(
                    "argument should be a str or a SchemaPath "
                    f"not {type(arg).__name__!r}"
                )

        self._raw_paths: list[str] = paths

    def __truediv__(self, other: SchemaPathLike) -> Self:
        try:
            return type(self)(self, other)
        except TypeError:
            return NotImplemented

    def __rtruediv__(self, other: SchemaPathLike) -> Self:
        try:
            return type(self)(other, self)
        except TypeError:
            return NotImplemented

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, SchemaPath):
            return NotImplemented
        return self._str == other._str

    def __hash__(self) -> int:
        return self._hash

    def __lt__(self, other: SchemaPath) -> bool:
        if not isinstance(other, SchemaPath):
            return NotImplemented
        return self.parts < other.parts

    def __le__(self, other: SchemaPath) -> bool:
        if not isinstance(other, SchemaPath):
            return NotImplemented
        return self.parts <= other.parts

    def __gt__(self, other: SchemaPath) -> bool:
        if not isinstance(other, SchemaPath):
            return NotImplemented
        return self.parts > other.parts

    def __ge__(self, other: SchemaPath) -> bool:
        if not isinstance(other, SchemaPath):
            return NotImplemented
        return self.parts >= other.parts

    def __str__(self) -> str:
        return self._str

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({str(self)!r})"

    def __bool__(self) -> bool:
        return bool(self._raw_paths)

    @functools.cached_property
    def parts(self) -> tuple[str, ...]:
        """A sequence of components of the schema path."""
        parts: list[str] = []
        for raw_path in self._raw_paths:
            # Strip leading and trailing separators before splitting
            path = raw_path.strip(_SEP)
            parts.extend(filter(None, path.split(_SEP)))
        return tuple(parts)

    @property
    def name(self) -> str:
        """The final path component."""
        return self.parts[-1]

    @property
    def parent(self) -> Self:
        """The logical parent of the path."""
        return self._from_parsed_parts(self.parts[:-1])

    @property
    def parents(self) -> Sequence[Self]:
        """A sequence of this path's logical parents."""
        # The value of this property should not be cached on the path object,
        # as doing so would introduce a reference cycle.
        return _SchemaPathParents(self)

    def common_parts(self, other: SchemaPath) -> list[str]:
        """Return a list of segments that form common prefix between this
        and the other path."""
        prefix = []
        for a, b in zip(self.parts, other.parts, strict=False):
            if a == b:
                prefix.append(a)
            else:
                break

        return prefix

    def is_relative_to(self, other: SchemaPathLike) -> bool:
        """Determine if the path is relative to another path."""
        if not isinstance(other, SchemaPath):
            other = type(self)(other)
        return other == self or other in self.parents

    def has_prefix(self, other: SchemaPath) -> bool:
        return self.parts[: len(other.parts)] == other.parts

    def as_schema_name(self) -> str:
        return _SEP.join(p for p in self.parts)

    def as_quoted_schema_name(self) -> str:
        return _SEP.join(_edgeql.quote_ident(p) for p in self.parts)

    def as_code(self, clsname: str = "SchemaPath") -> str:
        parts = ", ".join(repr(p) for p in self.parts)
        return f"{clsname}.from_segments({parts})"

    def as_pathlib_path(self) -> pathlib.Path:
        return pathlib.Path(*self.parts)

    @functools.cached_property
    def _str(self) -> str:
        return _SEP.join(self.parts)

    @functools.cached_property
    def _hash(self) -> int:
        return hash(self._str)

    @classmethod
    def _from_parsed_parts(cls, parts: tuple[str, ...]) -> Self:
        path = cls(*parts)
        path.__dict__["parts"] = parts
        path.__dict__["_str"] = _SEP.join(parts)
        path.__dict__["_hash"] = hash(path._str)
        return path


_T = TypeVar("_T", bound=SchemaPath)


class _SchemaPathParents(Sequence[_T]):
    """This object provides sequence-like access to the logical ancestors
    of a path."""

    __slots__ = ("_parts", "_path")

    def __init__(self, path: _T) -> None:
        self._path = path
        self._parts = path.parts

    def __len__(self) -> int:
        return len(self._parts)

    @overload
    def __getitem__(self, idx: SupportsIndex) -> _T: ...

    @overload
    def __getitem__(self, idx: slice) -> Sequence[_T]: ...

    def __getitem__(self, idx: SupportsIndex | slice) -> _T | Sequence[_T]:
        self_len = len(self)

        if isinstance(idx, slice):
            return tuple(self[i] for i in range(*idx.indices(self_len)))
        else:
            idx = idx.__index__()

        if idx >= self_len or idx < -self_len:
            raise IndexError(idx)

        if idx < 0:
            idx += self_len

        return self._path._from_parsed_parts(self._parts[: -idx - 1])

    def __repr__(self) -> str:
        return f"<{type(self._path).__name__}.parents>"


@dataclasses.dataclass(frozen=True)
class ParametricTypeName:
    type_: SchemaPath
    args: list[TypeName]

    def __str__(self) -> str:
        return self.as_quoted_schema_name()

    def as_schema_name(self) -> str:
        return (
            f"{self.type_.as_schema_name()}<"
            f"{','.join(a.as_schema_name() for a in self.args)}"
            f">"
        )

    def as_quoted_schema_name(self) -> str:
        return (
            f"{self.type_.as_quoted_schema_name()}<"
            f"{','.join(a.as_quoted_schema_name() for a in self.args)}"
            f">"
        )

    @property
    def name(self) -> str:
        raise NotImplementedError


TypeName = TypeAliasType("TypeName", SchemaPath | ParametricTypeName)
