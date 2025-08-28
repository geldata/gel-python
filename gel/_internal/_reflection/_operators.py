# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.
#
# ruff: noqa: TC001

from __future__ import annotations
from typing import TYPE_CHECKING
from typing_extensions import (
    TypeAliasType,
    Self,
)
from collections.abc import (
    Mapping,
    MutableMapping,
)

import dataclasses
import functools
import itertools
import uuid
from collections import ChainMap, defaultdict

from gel._internal import _dataclass_extras

from . import _enums
from . import _query

from ._base import sobject
from ._callables import Callable
from ._enums import OperatorKind

if TYPE_CHECKING:
    from gel import abstract


@sobject
class Operator(Callable):
    suggested_ident: str | None
    py_magic: tuple[str, ...] | None
    operator_kind: OperatorKind

    def __str__(self) -> str:
        return f"operator {self.edgeql_signature}"

    @functools.cached_property
    def ident(self) -> str:
        if self.py_magic is not None:
            return self.py_magic[0]
        elif self.suggested_ident is not None:
            return self.suggested_ident
        else:
            raise AssertionError(
                f"operator {self.name} has no defined py_magic "
                f"or suggested_ident"
            )

    @functools.cached_property
    def swapped_infix_ident(self) -> str | None:
        if self.py_magic is not None and len(self.py_magic) > 1:
            return self.py_magic[1]
        else:
            return None


OperatorMap = TypeAliasType("OperatorMap", MutableMapping[str, list[Operator]])

OPERATOR_IDENT_FIXUP: dict[str, str] = {
    "std::=": "eq",
    "std::!=": "ne",
    "std::<": "lt",
    "std::<=": "le",
    "std::>": "gt",
    "std::>=": "ge",
    "std::?=": "coal_eq",
    "std::?!=": "coal_neq",
}
"""Patch missing std::identifier for some operators."""

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

    @functools.cached_property
    def binary_ops_by_name(self) -> Mapping[str, frozenset[Operator]]:
        m: defaultdict[str, set[Operator]] = defaultdict(set)
        for op in itertools.chain.from_iterable(self.binary_ops.values()):
            m[op.name].add(op)
        return {k: frozenset(v) for k, v in m.items()}


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
        if opv.suggested_ident is None and (
            ident_fixup := OPERATOR_IDENT_FIXUP.get(opv.name)
        ):
            opv = dataclasses.replace(opv, suggested_ident=ident_fixup)
        py_magic: str | tuple[str, ...] | None
        if op.operator_kind == _enums.OperatorKind.Infix:
            py_magic = INFIX_OPERATOR_MAP.get(op.name)
            if isinstance(py_magic, str):
                py_magic = (py_magic,)
            if py_magic is not None:
                opv = dataclasses.replace(opv, py_magic=py_magic)

            if op.name in {"std::AND", "std::OR"}:
                opv.params[1] = dataclasses.replace(
                    opv.params[1], kind=_enums.CallableParamKind.Variadic
                )

            binary_ops[opv.params[0].type.id].append(opv)
            assert opv.schemapath.name, opv.schemapath

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
