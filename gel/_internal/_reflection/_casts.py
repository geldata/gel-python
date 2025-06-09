# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.


from __future__ import annotations
from typing import TYPE_CHECKING
from collections.abc import (
    MutableMapping,
)
from typing_extensions import (
    Self,
    TypeAliasType,
)

import dataclasses
import uuid
from collections import ChainMap, defaultdict

from gel._internal import _dataclass_extras

from . import _enums
from . import _types
from . import _query
from ._struct import struct

if TYPE_CHECKING:
    from gel import abstract


@struct
class Cast:
    id: str
    from_type: _types.TypeRef
    to_type: _types.TypeRef
    allow_implicit: bool
    allow_assignment: bool


CastMap = TypeAliasType("CastMap", MutableMapping[str, list[str]])


def _trace_all_casts(
    from_type: str,
    cast_map: CastMap,
    *,
    _seen: set[str] | None = None,
) -> set[str]:
    if _seen is None:
        _seen = set()
    if from_type in _seen:
        return set()
    _seen.add(from_type)
    reachable = set()

    for to_type in cast_map[from_type]:
        reachable.add(to_type)
        reachable.update(_trace_all_casts(to_type, cast_map, _seen=_seen))

    return reachable


@dataclasses.dataclass(frozen=True, kw_only=True)
class CastMatrix:
    explicit_casts_from: CastMap
    explicit_casts_to: CastMap
    implicit_casts_from: CastMap
    implicit_casts_to: CastMap
    assignment_casts_from: CastMap
    assignment_casts_to: CastMap

    def chain(self, other: CastMatrix) -> Self:
        return dataclasses.replace(
            self,
            explicit_casts_from=ChainMap(
                self.explicit_casts_from,
                other.explicit_casts_from,
            ),
            explicit_casts_to=ChainMap(
                self.explicit_casts_to,
                other.explicit_casts_to,
            ),
            implicit_casts_from=ChainMap(
                self.implicit_casts_from,
                other.implicit_casts_from,
            ),
            implicit_casts_to=ChainMap(
                self.implicit_casts_to,
                other.implicit_casts_to,
            ),
            assignment_casts_from=ChainMap(
                self.assignment_casts_from,
                other.assignment_casts_from,
            ),
            assignment_casts_to=ChainMap(
                self.assignment_casts_to,
                other.assignment_casts_to,
            ),
        )


def fetch_casts(
    db: abstract.ReadOnlyExecutor,
    schema_part: _enums.SchemaPart,
) -> CastMatrix:
    builtin = schema_part is _enums.SchemaPart.STD
    casts: list[Cast] = db.query(_query.CASTS, builtin=builtin)

    casts_from: CastMap = defaultdict(list)
    casts_to: CastMap = defaultdict(list)
    implicit_casts_from: CastMap = defaultdict(list)
    implicit_casts_to: CastMap = defaultdict(list)
    assignment_casts_from: CastMap = defaultdict(list)
    assignment_casts_to: CastMap = defaultdict(list)
    types: set[str] = set()

    for raw_cast in casts:
        cast = _dataclass_extras.coerce_to_dataclass(
            Cast, raw_cast, cast_map={str: (uuid.UUID,)}
        )
        types.add(cast.from_type.id)
        types.add(cast.to_type.id)
        casts_from[cast.from_type.id].append(cast.to_type.id)
        casts_to[cast.to_type.id].append(cast.from_type.id)

        if cast.allow_implicit or cast.allow_assignment:
            assignment_casts_from[cast.from_type.id].append(cast.to_type.id)
            assignment_casts_to[cast.to_type.id].append(cast.from_type.id)

        if cast.allow_implicit:
            implicit_casts_from[cast.from_type.id].append(cast.to_type.id)
            implicit_casts_to[cast.to_type.id].append(cast.from_type.id)

    all_implicit_casts_from: CastMap = {}
    all_implicit_casts_to: CastMap = {}
    all_assignment_casts_from: CastMap = {}
    all_assignment_casts_to: CastMap = {}

    for type_ in types:
        all_implicit_casts_from[type_] = list(
            _trace_all_casts(type_, implicit_casts_from)
        )
        all_implicit_casts_to[type_] = list(
            _trace_all_casts(type_, implicit_casts_to)
        )
        all_assignment_casts_from[type_] = list(
            _trace_all_casts(type_, assignment_casts_from)
        )
        all_assignment_casts_to[type_] = list(
            _trace_all_casts(type_, assignment_casts_to)
        )

    return CastMatrix(
        explicit_casts_from=dict(casts_from),
        explicit_casts_to=dict(casts_to),
        implicit_casts_from=dict(all_implicit_casts_from),
        implicit_casts_to=dict(all_implicit_casts_to),
        assignment_casts_from=dict(all_assignment_casts_from),
        assignment_casts_to=dict(all_assignment_casts_to),
    )
