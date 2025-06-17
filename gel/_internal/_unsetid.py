# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

from __future__ import annotations
from typing import Any

import uuid


__all__ = ["UNSET_UUID"]


class _UnsetUUID(uuid.UUID):
    """A UUID subclass that only lets you str()/repr() it; everything
    else errors out."""

    def __new__(cls) -> _UnsetUUID:  # noqa: PYI034
        global _UNSET_UUID  # noqa: PLW0603
        if _UNSET_UUID is not None:
            return _UNSET_UUID
        _UNSET_UUID = super().__new__(cls)
        return _UNSET_UUID

    def __init__(self) -> None:
        # Create a “zero” UUID under the hood. It doesn't really matter what it
        # is, since we won't let anyone do anything with it except print it.
        super().__init__(int=0)

    def __repr__(self) -> str:
        return "<UUID: UNSET>"

    def __str__(self) -> str:
        return "UNSET"

    def __getattribute__(self, name: str) -> Any:
        # Allow the few methods/properties needed to make printing work
        if name in {
            "__class__",
            "__getattribute__",
            "__getstate__",
            "__reduce__",
            "__reduce_ex__",
            "__repr__",
            "__setstate__",
            "__str__",
            "__copy__",
            "__deepcopy__",
            "int",
        }:
            return object.__getattribute__(self, name)
        else:
            raise ValueError(f"_UnsetUUID.{name}: id is not set")

    def __copy__(self) -> _UnsetUUID:
        return self

    def __deepcopy__(self, _memo: dict[int, Any]) -> _UnsetUUID:
        return self

    def __getstate__(self) -> object:
        return {"int": 0}

    def __hash__(self) -> int:
        # We don't want unset uuids to be hashable or they will
        # cause bugs when used as keys in a collection.
        raise TypeError("UNSET_UUID is unhashable")

    def __eq__(self, other: object) -> bool:
        # Since unset uuids aren't hashable, the equality
        # check can only mirror the identity check.
        if isinstance(other, uuid.UUID):
            return self is other
        return NotImplemented


_UNSET_UUID: _UnsetUUID | None = None

# single shared sentinel
UNSET_UUID: uuid.UUID = _UnsetUUID()
