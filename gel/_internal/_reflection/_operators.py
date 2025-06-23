# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.


from __future__ import annotations
from typing import TYPE_CHECKING
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

from gel._internal import _dataclass_extras

from . import _enums
from . import _query
from . import _types
from ._callables import Callable, CallableParam

if TYPE_CHECKING:
    from gel import abstract


@dataclasses.dataclass(frozen=True, kw_only=True)
class Operator(Callable):
    id: str
    name: str
    description: str
    suggested_ident: str
    py_magic: tuple[str, ...] | None
    operator_kind: _enums.OperatorKind
    return_type: _types.TypeRef
    return_typemod: _enums.TypeModifier
    params: list[CallableParam]


OperatorMap = TypeAliasType("OperatorMap", MutableMapping[str, list[Operator]])


INFIX_OPERATOR_MAP: dict[str, str | tuple[str, str]] = {
    "std::=": "__eq__",
    "std::!=": "__ne__",
    "std::<": "__lt__",
    "std::<=": "__le__",
    "std::>": "__gt__",
    "std::>=": "__ge__",
    "std::+": ("__add__", "__radd__"),
    "std::++": ("__add__", "__radd__"),
    "std::-": ("__sub__", "__rsub__"),
    "std::*": ("__mul__", "__rmul__"),
    "std::/": ("__truediv__", "__rtruediv__"),
    "std:://": ("__floordiv__", "__rfloordiv__"),
    "std::%": ("__mod__", "__rmod__"),
    "std::^": ("__pow__", "__rpow__"),
    "std::[]": "__getitem__",
    "std::IN": "__contains__",
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
        opv = _dataclass_extras.coerce_to_dataclass(
            Operator, op, cast_map={str: (uuid.UUID,)}
        )
        py_magic: str | tuple[str, ...] | None
        if op.operator_kind == _enums.OperatorKind.Infix:
            py_magic = INFIX_OPERATOR_MAP.get(op.name)
            if isinstance(py_magic, str):
                py_magic = (py_magic,)
            if py_magic is not None:
                opv = dataclasses.replace(opv, py_magic=py_magic)
            binary_ops[opv.params[0].type.id].append(opv)

        elif op.operator_kind == _enums.OperatorKind.Prefix:
            py_magic = PREFIX_OPERATOR_MAP.get(op.name)
            if isinstance(py_magic, str):
                py_magic = (py_magic,)
            if py_magic is not None:
                opv = dataclasses.replace(opv, py_magic=py_magic)
            unary_ops[opv.params[0].type.id].append(opv)

        else:
            other_ops.append(opv)

    return OperatorMatrix(
        binary_ops=dict(binary_ops),
        unary_ops=dict(unary_ops),
        other_ops=other_ops,
    )
