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
from gel._internal import _dataclass_extras

from . import _enums
from . import _query
from . import _types
from ._callables import Callable, CallableParam


@dataclasses.dataclass(frozen=True, kw_only=True)
class Operator(Callable):
    id: uuid.UUID
    name: str
    description: str
    suggested_ident: str
    py_magic: str | None = None
    operator_kind: _enums.OperatorKind
    return_type: _types.TypeRef
    return_typemod: _enums.TypeModifier
    params: list[CallableParam]


OperatorMap = TypeAliasType(
    "OperatorMap", MutableMapping[uuid.UUID, list[Operator]]
)


INFIX_OPERATOR_MAP = {
    "std::=": "__eq__",
    "std::!=": "__ne__",
    "std::<": "__lt__",
    "std::<=": "__le__",
    "std::>": "__gt__",
    "std::>=": "__ge__",
    "std::+": "__add__",
    "std::++": "__add__",
    "std::-": "__sub__",
    "std::*": "__mul__",
    "std::/": "__truediv__",
    "std:://": "__floordiv__",
    "std::%": "__mod__",
    "std::^": "__pow__",
}

PREFIX_OPERATOR_MAP = {
    "std::+": "__pos__",
    "std::-": "__neg__",
}


@dataclasses.dataclass(frozen=True)
class OperatorMatrix:
    """Maps of binary and unary operators that are overloadable in Python;
    indexed by first argument type."""

    binary_ops: OperatorMap
    """Binary operators."""
    unary_ops: OperatorMap
    """Unary operators."""
    other_ops: list[Operator]
    """Non-overloadable or non binary/unary operators."""

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
        if (
            op.operator_kind == _enums.OperatorKind.Infix
            and op.name in INFIX_OPERATOR_MAP
        ):
            op = _dataclass_extras.coerce_to_dataclass(Operator, op)
            op = dataclasses.replace(op, py_magic=INFIX_OPERATOR_MAP[op.name])
            binary_ops[op.params[0].type.id].append(op)
        elif (
            op.operator_kind == _enums.OperatorKind.Prefix
            and op.name in PREFIX_OPERATOR_MAP
        ):
            op = _dataclass_extras.coerce_to_dataclass(Operator, op)
            op = dataclasses.replace(op, py_magic=PREFIX_OPERATOR_MAP[op.name])
            unary_ops[op.params[0].type.id].append(op)
        else:
            other_ops.append(op)

    return OperatorMatrix(
        binary_ops=binary_ops,
        unary_ops=unary_ops,
        other_ops=other_ops,
    )
