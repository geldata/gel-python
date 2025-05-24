# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

"""Primitive (non-object) types used to implement class-based query builders"""

from __future__ import annotations
from typing import TYPE_CHECKING, Any, Generic, TypeVar, overload
from typing_extensions import Self, TypeVarTuple, Unpack

import builtins
import datetime
import decimal
import numbers
import typing
import uuid

from gel.datatypes import range as _range
from gel.datatypes.datatypes import CustomType

from gel._internal import _qb
from gel._internal import _typing_parametric
from gel._internal._polyfills import StrEnum

from ._base import GelType, GelTypeMeta

if TYPE_CHECKING:
    from collections.abc import Sequence

    import enum


T = TypeVar("T")


class GelPrimitiveType(GelType):
    if TYPE_CHECKING:

        @overload
        def __get__(self, obj: None, objtype: type[Any]) -> type[Self]: ...

        @overload
        def __get__(self, obj: object, objtype: Any = None) -> Self: ...

        def __get__(
            self,
            obj: Any,
            objtype: Any = None,
        ) -> type[Self] | Self: ...


class BaseScalar(GelPrimitiveType):
    pass


if TYPE_CHECKING:
    from typing import NamedTupleMeta  # type: ignore [attr-defined]

    class AnyTupleMeta(NamedTupleMeta, GelTypeMeta):  # type: ignore [misc]
        ...
else:
    AnyTupleMeta = type(GelPrimitiveType)


class AnyTuple(GelPrimitiveType, metaclass=AnyTupleMeta):
    pass


if TYPE_CHECKING:

    class AnyEnumMeta(enum.EnumMeta, GelTypeMeta):
        pass
else:
    AnyEnumMeta = type(StrEnum)


class AnyEnum(BaseScalar, StrEnum, metaclass=AnyEnumMeta):
    pass


if TYPE_CHECKING:

    class _ArrayMeta(GelTypeMeta, typing._ProtocolMeta):
        pass
else:
    _ArrayMeta = type(list)


class Array(list[T], GelPrimitiveType, metaclass=_ArrayMeta):
    if TYPE_CHECKING:

        def __set__(self, obj: Any, value: Array[T] | Sequence[T]) -> None: ...


if TYPE_CHECKING:

    class _TupleMeta(GelTypeMeta, typing._ProtocolMeta):
        pass
else:
    _TupleMeta = type(tuple)


_Ts = TypeVarTuple("_Ts")


class Tuple(tuple[Unpack[_Ts]], GelPrimitiveType, metaclass=_TupleMeta):
    __slots__ = ()

    if TYPE_CHECKING:

        def __set__(
            self,
            obj: Any,
            value: Tuple[Unpack[_Ts]] | tuple[Unpack[_Ts]],
        ) -> None: ...


if TYPE_CHECKING:

    class _RangeMeta(GelTypeMeta):
        pass
else:
    _RangeMeta = type


class Range(_range.Range[T], GelPrimitiveType, metaclass=_RangeMeta):
    if TYPE_CHECKING:

        def __set__(
            self, obj: Any, value: Range[T] | _range.Range[T]
        ) -> None: ...


if TYPE_CHECKING:

    class _MultiRangeMeta(GelTypeMeta):
        pass
else:
    _MultiRangeMeta = type


class MultiRange(
    GelPrimitiveType,
    Generic[T],
    metaclass=_MultiRangeMeta,
):
    if TYPE_CHECKING:

        def __set__(
            self,
            obj: Any,
            value: MultiRange[T] | _range.MultiRange[T],
        ) -> None: ...


# The below is a straight Union and not a type alias because
# we want isinstance/issubclass to work with it.
PyConstType = (
    builtins.bytes
    | builtins.int
    | builtins.float
    | builtins.str
    | datetime.date
    | datetime.datetime
    | datetime.time
    | datetime.timedelta
    | decimal.Decimal
    | numbers.Number
    | uuid.UUID
    | CustomType
)
"""Types of raw Python values supported in query expressions"""


_py_type_to_literal: dict[type[PyConstType], type[_qb.Literal]] = {
    builtins.bool: _qb.BoolLiteral,
    builtins.int: _qb.IntLiteral,
    builtins.float: _qb.FloatLiteral,
    builtins.str: _qb.StringLiteral,
    builtins.bytes: _qb.BytesLiteral,
    decimal.Decimal: _qb.DecimalLiteral,
}


_PT_co = TypeVar("_PT_co", bound=PyConstType, covariant=True)


def get_literal_for_value(t: type[PyConstType], v: PyConstType) -> _qb.Literal:
    if not isinstance(v, t):
        raise ValueError(
            f"get_literal_for_value: {v!r} is not an instance of {t}")
    ltype = _py_type_to_literal.get(t)
    if ltype is None:
        raise NotImplementedError(f"unsupported Python raw value type: {t}")
    else:
        if t is builtins.bool:
            v = builtins.bool(v)
        return ltype(val=v)  # type: ignore [call-arg]


class PyTypeScalar(_typing_parametric.SingleParametricType[_PT_co]):
    if TYPE_CHECKING:

        def __init__(self, val: Any) -> None: ...
        def __set__(self, obj: Any, value: _PT_co) -> None: ...  # type: ignore [misc]

    def __edgeql_literal__(self) -> _qb.Literal:
        pytype = type(self).type
        return get_literal_for_value(pytype, self)  # type: ignore [arg-type]
