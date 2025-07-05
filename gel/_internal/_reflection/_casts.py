# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.


from __future__ import annotations
from typing import TYPE_CHECKING
from typing_extensions import (
    Self,
    TypeAliasType,
)
from collections.abc import MutableMapping

import dataclasses
import uuid
from collections import ChainMap, defaultdict, deque

from gel._internal import _dataclass_extras

from . import _enums
from . import _types
from . import _query
from ._base import struct, sobject

if TYPE_CHECKING:
    from gel import abstract


@sobject
class Cast:
    from_type: _types.TypeRef
    to_type: _types.TypeRef
    allow_implicit: bool
    allow_assignment: bool


CastMap = TypeAliasType("CastMap", MutableMapping[str, dict[str, int]])


def _trace_all_casts(
    from_type: str,
    cast_map: CastMap,
) -> dict[str, int]:
    """Compute shortest distances to all reachable types using BFS.

    Returns a dictionary mapping type names to their shortest distances.
    """
    distances: dict[str, int] = {}
    queue: deque[tuple[str, int]] = deque([(from_type, 0)])

    while queue:
        current_type, distance = queue.popleft()

        # Skip if we've already found a shorter path to this type
        if current_type in distances:
            continue

        distances[current_type] = distance

        # Add neighbors to queue with distance + 1
        for neighbor in cast_map.get(current_type, {}):
            if neighbor not in distances:
                queue.append((neighbor, distance + 1))

    # Remove the starting type from results (distance 0 to itself)
    distances.pop(from_type, None)
    return distances


@struct
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

    casts_from: CastMap = defaultdict(dict)
    casts_to: CastMap = defaultdict(dict)
    implicit_casts_from: CastMap = defaultdict(dict)
    implicit_casts_to: CastMap = defaultdict(dict)
    assignment_casts_from: CastMap = defaultdict(dict)
    assignment_casts_to: CastMap = defaultdict(dict)
    types: set[str] = set()

    for raw_cast in casts:
        cast = _dataclass_extras.coerce_to_dataclass(
            Cast, raw_cast, cast_map={str: (uuid.UUID,)}
        )
        types.add(cast.from_type.id)
        types.add(cast.to_type.id)
        casts_from[cast.from_type.id][cast.to_type.id] = 1
        casts_to[cast.to_type.id][cast.from_type.id] = 1

        if cast.allow_implicit or cast.allow_assignment:
            assignment_casts_from[cast.from_type.id][cast.to_type.id] = 1
            assignment_casts_to[cast.to_type.id][cast.from_type.id] = 1

        if cast.allow_implicit:
            implicit_casts_from[cast.from_type.id][cast.to_type.id] = 1
            implicit_casts_to[cast.to_type.id][cast.from_type.id] = 1

    all_implicit_casts_from: CastMap = {}
    all_implicit_casts_to: CastMap = {}
    all_assignment_casts_from: CastMap = {}
    all_assignment_casts_to: CastMap = {}

    for type_ in types:
        all_implicit_casts_from[type_] = _trace_all_casts(
            type_, implicit_casts_from
        )
        all_implicit_casts_to[type_] = _trace_all_casts(
            type_, implicit_casts_to
        )
        all_assignment_casts_from[type_] = _trace_all_casts(
            type_, assignment_casts_from
        )
        all_assignment_casts_to[type_] = _trace_all_casts(
            type_, assignment_casts_to
        )

    return CastMatrix(
        explicit_casts_from=dict(casts_from),
        explicit_casts_to=dict(casts_to),
        implicit_casts_from=dict(all_implicit_casts_from),
        implicit_casts_to=dict(all_implicit_casts_to),
        assignment_casts_from=dict(all_assignment_casts_from),
        assignment_casts_to=dict(all_assignment_casts_to),
    )
