# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

from __future__ import annotations
from typing import (
    Any,
)

import uuid


class _UnsetUUID(uuid.UUID):
    """A UUID subclass that only lets you str()/repr() it; everything
    else errors out."""

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
            "__str__",
            "__repr__",
            "__getattribute__",
            "__reduce_ex__",
            "__reduce__",
        }:
            return object.__getattribute__(self, name)
        else:
            raise ValueError(f"_UnsetUUID.{name}: id is not set")


# single shared sentinel
UNSET_UUID: uuid.UUID = _UnsetUUID()
