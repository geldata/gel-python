# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

"""Primitive (non-object) types used to implement class-based query builders"""

from __future__ import annotations
from typing import TYPE_CHECKING, Any, Generic, Protocol, TypeVar, overload
from typing_extensions import Self, TypeVarTuple, Unpack

import builtins
import datetime
import decimal
import functools
import numbers
import typing
import uuid

from gel.datatypes import datatypes as _datatypes
from gel.datatypes import range as _range

from gel._internal import _qb
from gel._internal import _typing_parametric
from gel._internal._polyfills import StrEnum

from ._base import GelType, GelTypeMeta
from ._functions import assert_single


if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

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

        @classmethod
        def __gel_assert_single__(
            cls, /, *, message: str | None = None,
        ) -> type[Self]: ...

    else:

        @_qb.exprmethod
        @classmethod
        def __gel_assert_single__(
            cls,
            /,
            *,
            message: str | None = None,
            __operand__: _qb.ExprAlias | None = None,
        ) -> type[Self]:
            return _qb.AnnotatedExpr(  # type: ignore [return-value]
                cls,
                assert_single(cls, message=message, __operand__=__operand__),
            )


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
    | _datatypes.CustomType
)
"""Types of raw Python values supported in query expressions"""


class DateLike(Protocol):
    year: int
    month: int
    day: int

    def toordinal(self) -> int: ...


class TimeDeltaLike(Protocol):
    days: int
    seconds: int
    microseconds: int


@typing.runtime_checkable
class DateTimeLike(Protocol):
    def astimezone(self, tz: datetime.tzinfo) -> Self: ...
    def __sub__(self, other: datetime.datetime) -> TimeDeltaLike: ...


_scalar_type_to_py_type: dict[str, str | tuple[str, str]] = {
    "std::str": "str",
    "std::float32": "float",
    "std::float64": "float",
    "std::int16": "int",
    "std::int32": "int",
    "std::int64": "int",
    "std::bigint": "int",
    "std::bool": "bool",
    "std::uuid": ("uuid", "UUID"),
    "std::bytes": "bytes",
    "std::decimal": ("decimal", "Decimal"),
    "std::datetime": ("datetime", "datetime"),
    "std::duration": ("datetime", "timedelta"),
    "std::json": "str",
    "std::cal::local_date": ("datetime", "date"),
    "std::cal::local_time": ("datetime", "time"),
    "std::cal::local_datetime": ("datetime", "datetime"),
    "std::cal::relative_duration": ("gel", "RelativeDuration"),
    "std::cal::date_duration": ("gel", "DateDuration"),
    "cfg::memory": ("gel", "ConfigMemory"),
    "ext::pgvector::vector": ("array", "array"),
}

_abstract_scalar_type_to_py_type: dict[str, list[tuple[str, str] | str]] = {
    "std::anyfloat": [("builtins", "float")],
    "std::anyint": [("builtins", "int")],
    "std::anynumeric": [("builtins", "int"), ("decimal", "Decimal")],
    "std::anyreal": ["std::anyfloat", "std::anyint", "std::anynumeric"],
    "std::anyenum": [("builtins", "str")],
    "std::anydiscrete": [("builtins", "int")],
    "std::anycontiguous": [
        ("decimal", "Decimal"),
        ("datetime", "datetime"),
        ("datetime", "timedelta"),
        "std::anyfloat",
    ],
    "std::anypoint": ["std::anydiscrete", "std::anycontiguous"],
}


_protocolized_py_types: dict[tuple[str, str], str] = {
    ("datetime", "datetime"): "DateTimeLike",
}


@functools.cache
def get_py_type_for_scalar(
    typename: str,
    *,
    require_subclassable: bool = False,
    consider_abstract: bool = True,
) -> tuple[tuple[str, str], ...]:
    base_type = _scalar_type_to_py_type.get(typename)
    if base_type is not None:
        if isinstance(base_type, str):
            if require_subclassable and base_type == "bool":
                base_type = "int"
            base_type = ("builtins", base_type)

        return (base_type,)
    elif consider_abstract:
        return tuple(sorted(_get_py_type_for_abstract_scalar(typename)))
    else:
        return ()


def get_py_type_for_scalar_hierarchy(
    typenames: Iterable[str],
    *,
    consider_abstract: bool = True,
) -> tuple[tuple[str, str], ...]:
    for typename in typenames:
        py_type = get_py_type_for_scalar(
            typename,
            consider_abstract=consider_abstract,
        )
        if py_type:
            return py_type

    return ()


def maybe_get_protocol_for_py_type(py_type: tuple[str, str]) -> str | None:
    return _protocolized_py_types.get(py_type)


def _get_py_type_for_abstract_scalar(typename: str) -> set[tuple[str, str]]:
    types = _abstract_scalar_type_to_py_type.get(typename)
    if types is None:
        return set()

    union = set()
    for typespec in types:
        if isinstance(typespec, str):
            union.update(_get_py_type_for_abstract_scalar(typespec))
        else:
            union.add(typespec)

    return union


_py_type_to_scalar_type: dict[tuple[str, str], list[str]] = {
    ("gel", "DateDuration"): ["std::cal::date_duration"],
    ("gel", "RelativeDuration"): ["std::cal::relative_duration"],
    ("gel", "ConfigMemory"): ["cfg::memory"],
    ("builtins", "bool"): ["std::bool"],
    ("builtins", "bytes"): ["std::bytes"],
    ("builtins", "float"): [
        "std::float64",
        "std::float32",
    ],
    ("builtins", "int"): [
        "std::bigint",
        "std::int64",
        "std::int32",
        "std::int16",
    ],
    ("builtins", "str"): ["std::str", "std::json"],
    ("datetime", "datetime"): ["std::datetime"],
    ("datetime", "timedelta"): ["std::duration"],
    ("datetime", "date"): ["std::cal::local_date"],
    ("datetime", "time"): ["std::cal::local_time"],
    ("decimal", "Decimal"): ["std::decimal"],
    ("uuid", "UUID"): ["std::uuid"],
}


@functools.cache
def get_py_type_scalar_match_rank(
    py_type: tuple[str, str],
    scalar_name: str,
) -> int | None:
    scalars = _py_type_to_scalar_type.get(py_type)
    if scalars is None:
        return None

    try:
        return scalars.index(scalar_name)
    except ValueError:
        return None


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
            f"get_literal_for_value: {v!r} is not an instance of {t}"
        )
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
