# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

"""Primitive (non-object) types used to implement class-based query builders"""

from __future__ import annotations
from typing import TYPE_CHECKING, Any, Generic, TypeVar, overload
from typing_extensions import Self, TypeVarTuple, Unpack

import builtins
import decimal
import typing

from gel.datatypes import range as _range

from gel._internal import _qb
from gel._internal import _typing_parametric
from gel._internal._polyfills import StrEnum

from ._base import GelType, GelTypeMeta

if TYPE_CHECKING:
    from collections.abc import Sequence

    import enum


T = TypeVar("T")
T_co = TypeVar("T_co", covariant=True)


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


class PyTypeScalar(_typing_parametric.SingleParametricType[T_co]):
    if TYPE_CHECKING:

        def __init__(self, val: Any) -> None: ...
        def __set__(self, obj: Any, value: T_co) -> None: ...  # type: ignore [misc]

    def __edgeql_literal__(self) -> _qb.Literal:
        cls = type(self)
        match cls.type:
            case builtins.bool:
                return _qb.BoolLiteral(bool(self))
            case builtins.int:
                return _qb.IntLiteral(self)  # type: ignore [arg-type]
            case builtins.float:
                return _qb.FloatLiteral(self)  # type: ignore [arg-type]
            case builtins.str:
                return _qb.StringLiteral(self)  # type: ignore [arg-type]
            case builtins.bytes:
                return _qb.BytesLiteral(self)  # type: ignore [arg-type]
            case decimal.Decimal:
                return _qb.DecimalLiteral(self)  # type: ignore [arg-type]

        raise NotImplementedError(f"{cls}.__edgeql_literal__")
