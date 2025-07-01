# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

from __future__ import annotations
from typing import TYPE_CHECKING

import uuid

from gel._internal import _dataclass_extras

from . import _query

from ._base import sobject
from ._enums import SchemaPart
from ._callables import Callable

if TYPE_CHECKING:
    from gel import abstract


@sobject
class Function(Callable):
    pass


def fetch_functions(
    db: abstract.ReadOnlyExecutor,
    schema_part: SchemaPart,
) -> list[Function]:
    builtin = schema_part is SchemaPart.STD
    fns: list[Function] = [
        _dataclass_extras.coerce_to_dataclass(
            Function, fn, cast_map={str: (uuid.UUID,)}
        )
        for fn in db.query(_query.FUNCTIONS, builtin=builtin)
    ]
    return fns
