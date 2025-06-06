# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.


from __future__ import annotations
from typing import TYPE_CHECKING

import dataclasses
import uuid

from . import _enums
from . import _query
from . import _types
from ._callables import Callable, CallableParam

from gel._internal import _dataclass_extras

if TYPE_CHECKING:
    from gel import abstract


@dataclasses.dataclass(frozen=True, kw_only=True)
class Function(Callable):
    id: str
    name: str
    description: str | None
    return_type: _types.TypeRef
    return_typemod: _enums.TypeModifier
    params: list[CallableParam]


def fetch_functions(
    db: abstract.ReadOnlyExecutor,
    schema_part: _enums.SchemaPart,
) -> list[Function]:
    builtin = schema_part is _enums.SchemaPart.STD
    fns: list[Function] = [
        _dataclass_extras.coerce_to_dataclass(
            Function, fn, cast_map={str: (uuid.UUID,)}
        )
        for fn in db.query(_query.FUNCTIONS, builtin=builtin)
    ]
    return fns
