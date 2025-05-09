# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.


from __future__ import annotations

import dataclasses
import uuid

from gel import abstract
from . import _enums
from . import _types
from . import _query


@dataclasses.dataclass(frozen=True)
class CallableParam:
    name: str
    type: _types.TypeRef
    kind: _enums.CallableParamKind
    typemod: _enums.TypeModifier
    default: str


@dataclasses.dataclass(frozen=True)
class Operator:
    id: uuid.UUID
    name: str
    suggested_ident: str
    description: str
    return_type: _types.TypeRef
    return_typemod: _enums.TypeModifier
    params: list[CallableParam]


def fetch_operators(
    db: abstract.ReadOnlyExecutor,
    schema_part: _enums.SchemaPart,
) -> list[Operator]:
    builtin = schema_part is _enums.SchemaPart.STD
    ops: list[Operator] = db.query(_query.OPERATORS, builtin=builtin)

    return ops
