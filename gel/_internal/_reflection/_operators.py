# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.


from __future__ import annotations
from collections.abc import (
    MutableMapping,
)
from typing_extensions import (
    TypeAliasType,
    Self,
)

import dataclasses
import uuid
from collections import ChainMap, defaultdict

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
    operator_kind: _enums.OperatorKind
    return_type: _types.TypeRef
    return_typemod: _enums.TypeModifier
    params: list[CallableParam]


OperatorMap = TypeAliasType(
    "OperatorMap", MutableMapping[uuid.UUID, list[Operator]]
)


@dataclasses.dataclass(frozen=True)
class OperatorMatrix:
    """Maps of binary and unary operators indexed by first argument type."""
    binary_ops: OperatorMap
    """Binary operators."""
    unary_ops: OperatorMap
    """Unary operators."""
    other_ops: list[Operator]
    """Non binary or unary operators."""

    def chain(self, other: OperatorMatrix) -> Self:
        return dataclasses.replace(
            self,
            binary_ops=ChainMap(
                self.binary_ops,
                other.binary_ops,
            ),
            unary_ops=ChainMap(
                self.unary_ops,
                other.unary_ops,
            ),
            other_ops=self.other_ops + other.other_ops,
        )


def fetch_operators(
    db: abstract.ReadOnlyExecutor,
    schema_part: _enums.SchemaPart,
) -> OperatorMatrix:
    builtin = schema_part is _enums.SchemaPart.STD
    ops: list[Operator] = db.query(_query.OPERATORS, builtin=builtin)

    binary_ops: OperatorMap = defaultdict(list)
    unary_ops: OperatorMap = defaultdict(list)
    other_ops: list[Operator] = []

    for op in ops:
        if op.operator_kind == _enums.OperatorKind.Infix:
            binary_ops[op.params[0].type.id].append(op)
        elif op.operator_kind == _enums.OperatorKind.Prefix:
            unary_ops[op.params[0].type.id].append(op)
        else:
            other_ops.append(op)

    return OperatorMatrix(
        binary_ops=binary_ops,
        unary_ops=unary_ops,
        other_ops=other_ops,
    )
