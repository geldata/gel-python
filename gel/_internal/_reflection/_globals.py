# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.


from __future__ import annotations
from typing import TYPE_CHECKING

import uuid

from gel._internal import _dataclass_extras

from . import _enums as enums
from . import _query

from ._base import SchemaObject, sobject
from ._types import Schema, Type, TypeRef

if TYPE_CHECKING:
    from gel import abstract


@sobject
class Global(SchemaObject):
    type: TypeRef | Type
    required: bool
    cardinality: enums.Cardinality

    def get_type(self, schema: Schema) -> Type:
        t = self.type
        return schema[t.id] if isinstance(t, TypeRef) else t


def fetch_globals(
    db: abstract.ReadOnlyExecutor,
    schema_part: enums.SchemaPart,
) -> list[Global]:
    builtin = schema_part is enums.SchemaPart.STD
    raw_globals: list[Global] = db.query(_query.GLOBALS, builtin=builtin)
    return [
        _dataclass_extras.coerce_to_dataclass(
            Global, raw_global, cast_map={str: (uuid.UUID,)}
        )
        for raw_global in raw_globals
    ]
