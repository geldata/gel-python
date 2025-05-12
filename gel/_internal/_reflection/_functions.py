# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.


from __future__ import annotations

import dataclasses

from gel import abstract

from . import _enums
from . import _query
from ._callables import Callable


@dataclasses.dataclass(frozen=True, kw_only=True)
class Function(Callable):
    pass


def fetch_functions(
    db: abstract.ReadOnlyExecutor,
    schema_part: _enums.SchemaPart,
) -> list[Function]:
    builtin = schema_part is _enums.SchemaPart.STD
    fns: list[Function] = db.query(_query.FUNCTIONS, builtin=builtin)
    return fns
