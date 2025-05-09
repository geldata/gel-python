# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.


from __future__ import annotations

import dataclasses
import uuid
from collections import defaultdict

from gel import abstract
from . import _enums
from . import _types
from . import _query


@dataclasses.dataclass(frozen=True)
class Cast:
    id: uuid.UUID
    from_type: _types.TypeRef
    to_type: _types.TypeRef
    allow_implicit: bool
    allow_assignment: bool


def _trace_all_casts(
    from_type: uuid.UUID,
    cast_map: dict[uuid.UUID, list[uuid.UUID]],
    *,
    _seen: set[uuid.UUID] | None = None,
) -> set[uuid.UUID]:
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
    explicit_casts_from: dict[uuid.UUID, list[uuid.UUID]]
    explicit_casts_to: dict[uuid.UUID, list[uuid.UUID]]
    implicit_casts_from: dict[uuid.UUID, list[uuid.UUID]]
    implicit_casts_to: dict[uuid.UUID, list[uuid.UUID]]
    assignment_casts_from: dict[uuid.UUID, list[uuid.UUID]]
    assignment_casts_to: dict[uuid.UUID, list[uuid.UUID]]


def fetch_casts(
    db: abstract.ReadOnlyExecutor,
    schema_part: _enums.SchemaPart,
) -> CastMatrix:
    builtin = schema_part is _enums.SchemaPart.STD
    casts: list[Cast] = db.query(_query.CASTS, builtin=builtin)

    casts_from: dict[uuid.UUID, list[uuid.UUID]] = defaultdict(list)
    casts_to: dict[uuid.UUID, list[uuid.UUID]] = defaultdict(list)
    implicit_casts_from: dict[uuid.UUID, list[uuid.UUID]] = defaultdict(list)
    implicit_casts_to: dict[uuid.UUID, list[uuid.UUID]] = defaultdict(list)
    assignment_casts_from: dict[uuid.UUID, list[uuid.UUID]] = defaultdict(list)
    assignment_casts_to: dict[uuid.UUID, list[uuid.UUID]] = defaultdict(list)
    types: set[uuid.UUID] = set()

    for cast in casts:
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

    all_implicit_casts_from: dict[uuid.UUID, list[uuid.UUID]] = {}
    all_implicit_casts_to: dict[uuid.UUID, list[uuid.UUID]] = {}
    all_assignment_casts_from: dict[uuid.UUID, list[uuid.UUID]] = {}
    all_assignment_casts_to: dict[uuid.UUID, list[uuid.UUID]] = {}

    for type in types:
        all_implicit_casts_from[type] = list(
            _trace_all_casts(type, implicit_casts_from)
        )
        all_implicit_casts_to[type] = list(
            _trace_all_casts(type, implicit_casts_to)
        )
        all_assignment_casts_from[type] = list(
            _trace_all_casts(type, assignment_casts_from)
        )
        all_assignment_casts_to[type] = list(
            _trace_all_casts(type, assignment_casts_to)
        )

    return CastMatrix(
        explicit_casts_from=casts_from,
        explicit_casts_to=casts_to,
        implicit_casts_from=all_implicit_casts_from,
        implicit_casts_to=all_implicit_casts_to,
        assignment_casts_from=all_assignment_casts_from,
        assignment_casts_to=all_assignment_casts_to,
    )
