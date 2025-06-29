# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

"""Primitive (non-object) types used to implement class-based query builders"""

from __future__ import annotations
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    ClassVar,
    Final,
    Generic,
    Protocol,
    TypeVar,
    SupportsIndex,
    overload,
)
from typing_extensions import Self, TypeVarTuple, TypeAliasType, Unpack

import builtins
import datetime
import decimal
import functools
import numbers
import typing
import uuid

from gel.datatypes import datatypes as _datatypes
from gel.datatypes import range as _range

from gel._internal import _edgeql
from gel._internal import _qb
from gel._internal import _typing_parametric
from gel._internal._lazyprop import LazyClassProperty
from gel._internal._polyfills._strenum import StrEnum
from gel._internal._reflection import SchemaPath

from ._base import GelType, GelTypeMeta
from ._functions import assert_single


if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

    import enum


_T = TypeVar("_T", bound=GelType)


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
            cls,
            /,
            *,
            message: str | None = None,
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

    class AnyNamedTupleMeta(NamedTupleMeta, GelTypeMeta):  # type: ignore [misc]
        ...
else:
    AnyNamedTupleMeta = type(GelPrimitiveType)


class AnyTuple(GelPrimitiveType):
    pass


class AnyNamedTuple(AnyTuple, metaclass=AnyNamedTupleMeta):
    pass


if TYPE_CHECKING:

    class AnyEnumMeta(enum.EnumMeta, GelTypeMeta):
        pass
else:
    AnyEnumMeta = type(StrEnum)


class AnyEnum(BaseScalar, StrEnum, metaclass=AnyEnumMeta):
    pass


class HomogeneousCollection(
    _typing_parametric.ParametricType,
    GelPrimitiveType,
    Generic[_T],
):
    __element_type__: ClassVar[type[_T]]  # type: ignore [misc]

    @LazyClassProperty[type[GelPrimitiveType.__gel_reflection__]]
    @classmethod
    def __gel_reflection__(cls) -> type[GelPrimitiveType.__gel_reflection__]:  # pyright: ignore [reportIncompatibleVariableOverride]
        class __gel_reflection__(GelPrimitiveType.__gel_reflection__):  # noqa: N801
            pass

        return __gel_reflection__


if TYPE_CHECKING:

    class _ArrayMeta(GelTypeMeta, typing._ProtocolMeta):
        pass
else:
    _ArrayMeta = type(list)


class Array(HomogeneousCollection[_T], list[_T], metaclass=_ArrayMeta):  # type: ignore [misc]
    if TYPE_CHECKING:

        def __set__(
            self, obj: Any, value: Array[_T] | Sequence[_T]
        ) -> None: ...

    @LazyClassProperty[type[GelPrimitiveType.__gel_reflection__]]
    @classmethod
    def __gel_reflection__(cls) -> type[GelPrimitiveType.__gel_reflection__]:  # pyright: ignore [reportIncompatibleVariableOverride]
        tid, tname = _edgeql.get_array_type_id_and_name(
            cls.__element_type__.__gel_reflection__.name.as_schema_name()
        )

        class __gel_reflection__(GelPrimitiveType.__gel_reflection__):  # noqa: N801
            id = tid
            name = SchemaPath(tname)

        return __gel_reflection__


if TYPE_CHECKING:

    class _TupleMeta(GelTypeMeta, typing._ProtocolMeta):
        pass
else:
    _TupleMeta = type(AnyTuple)


_Ts = TypeVarTuple("_Ts")


class HeterogeneousCollection(
    _typing_parametric.ParametricType, Generic[Unpack[_Ts]]
):
    __element_types__: ClassVar[
        Annotated[tuple[type[GelType], ...], Unpack[_Ts]]
    ]


class Tuple(  # type: ignore[misc]
    HeterogeneousCollection[Unpack[_Ts]],
    AnyTuple,
    tuple[Unpack[_Ts]],
    metaclass=_TupleMeta,
):
    __slots__ = ()

    if TYPE_CHECKING:

        def __set__(
            self,
            obj: Any,
            value: Tuple[Unpack[_Ts]] | tuple[Unpack[_Ts]],
        ) -> None: ...

    @LazyClassProperty[type[GelPrimitiveType.__gel_reflection__]]
    @classmethod
    def __gel_reflection__(cls) -> type[GelPrimitiveType.__gel_reflection__]:  # pyright: ignore [reportIncompatibleVariableOverride]
        tid, tname = _edgeql.get_tuple_type_id_and_name(
            el.__gel_reflection__.name.as_schema_name()
            for el in cls.__element_types__
        )

        class __gel_reflection__(GelPrimitiveType.__gel_reflection__):  # noqa: N801
            id = tid
            name = SchemaPath(tname)

        return __gel_reflection__


if TYPE_CHECKING:

    class _RangeMeta(GelTypeMeta):
        pass
else:
    _RangeMeta = type


class Range(
    HomogeneousCollection[_T],
    _range.Range[_T],
    metaclass=_RangeMeta,
):
    if TYPE_CHECKING:

        def __set__(
            self, obj: Any, value: Range[_T] | _range.Range[_T]
        ) -> None: ...

    @LazyClassProperty[type[GelPrimitiveType.__gel_reflection__]]
    @classmethod
    def __gel_reflection__(cls) -> type[GelPrimitiveType.__gel_reflection__]:  # pyright: ignore [reportIncompatibleVariableOverride]
        tid, tname = _edgeql.get_range_type_id_and_name(
            cls.__element_type__.__gel_reflection__.name.as_schema_name()
        )

        class __gel_reflection__(GelPrimitiveType.__gel_reflection__):  # noqa: N801
            id = tid
            name = SchemaPath(tname)

        return __gel_reflection__


if TYPE_CHECKING:

    class _MultiRangeMeta(GelTypeMeta):
        pass
else:
    _MultiRangeMeta = type


class MultiRange(
    HomogeneousCollection[_T],
    metaclass=_MultiRangeMeta,
):
    if TYPE_CHECKING:

        def __set__(
            self,
            obj: Any,
            value: MultiRange[_T] | _range.MultiRange[_T],
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

_generic_scalar_type_to_py_type: dict[str, list[tuple[str, str] | str]] = {
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

_pseudo_types = frozenset(("anytuple", "anyobject", "anytype"))

_generic_types = frozenset(_generic_scalar_type_to_py_type) | frozenset(
    _pseudo_types
)


def is_generic_type(typename: str) -> bool:
    return typename in _generic_types


@functools.cache
def get_py_type_for_scalar(
    typename: str,
    *,
    require_subclassable: bool = False,
    consider_generic: bool = True,
) -> tuple[tuple[str, str], ...]:
    base_type = _scalar_type_to_py_type.get(typename)
    if base_type is not None:
        if isinstance(base_type, str):
            if require_subclassable and base_type == "bool":
                base_type = "int"
            base_type = ("builtins", base_type)

        return (base_type,)
    elif consider_generic:
        return tuple(sorted(_get_py_type_for_generic_scalar(typename)))
    else:
        return ()


def get_py_type_for_scalar_hierarchy(
    typenames: Iterable[str],
    *,
    consider_generic: bool = True,
) -> tuple[tuple[str, str], ...]:
    for typename in typenames:
        py_type = get_py_type_for_scalar(
            typename,
            consider_generic=consider_generic,
        )
        if py_type:
            return py_type

    return ()


def maybe_get_protocol_for_py_type(py_type: tuple[str, str]) -> str | None:
    return _protocolized_py_types.get(py_type)


def _get_py_type_for_generic_scalar(typename: str) -> set[tuple[str, str]]:
    types = _generic_scalar_type_to_py_type.get(typename)
    if types is None:
        return set()

    union = set()
    for typespec in types:
        if isinstance(typespec, str):
            union.update(_get_py_type_for_generic_scalar(typespec))
        else:
            union.add(typespec)

    return union


MODEL_SUBSTRATE_MODULE: Final = "__gel_substrate__"
"""Sentinel module value to be replaced by a concrete imported model
substrate in generated models, e.g `gel.models.pydantic`."""


_scalar_type_impl_overrides: dict[str, tuple[str, str]] = {
    "std::uuid": (MODEL_SUBSTRATE_MODULE, "UUIDImpl"),
    "std::datetime": (MODEL_SUBSTRATE_MODULE, "DateTimeImpl"),
    "std::duration": (MODEL_SUBSTRATE_MODULE, "TimeDeltaImpl"),
    "std::cal::local_date": (MODEL_SUBSTRATE_MODULE, "DateImpl"),
    "std::cal::local_time": (MODEL_SUBSTRATE_MODULE, "TimeImpl"),
    "std::cal::local_datetime": (MODEL_SUBSTRATE_MODULE, "DateTimeImpl"),
}
"""Overrides of scalar bases for types that lack `cls(inst_of_cls)` invariant
required for scalar downcasting."""


def get_py_base_for_scalar(
    typename: str,
    *,
    require_subclassable: bool = False,
    consider_generic: bool = True,
) -> tuple[tuple[str, str], ...]:
    override = _scalar_type_impl_overrides.get(typename)
    if override is not None:
        return (override,)
    else:
        return get_py_type_for_scalar(
            typename,
            require_subclassable=require_subclassable,
            consider_generic=consider_generic,
        )


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


def get_literal_for_value(
    v: Any,
) -> _qb.Literal:
    for t, ltype in _py_type_to_literal.items():
        if isinstance(v, t):
            return ltype(val=v)  # type: ignore [call-arg]

    raise NotImplementedError(f"cannot convert Python value to Literal: {v!r}")


def get_literal_for_scalar(
    t: type[PyTypeScalar[_PT_co]],
    v: Any,
) -> _qb.Literal | _qb.CastOp:
    if not isinstance(v, t):
        v = t(v)
    ltype = _py_type_to_literal.get(t.type)  # type: ignore [arg-type]
    if ltype is not None:
        return ltype(val=v)  # type: ignore [call-arg]
    else:
        return _qb.CastOp(
            expr=_qb.StringLiteral(val=str(v)),
            type_=t.__gel_reflection__.name,
        )


class PyTypeScalar(
    _typing_parametric.SingleParametricType[_PT_co],
    BaseScalar,
):
    if TYPE_CHECKING:

        def __set__(self, obj: Any, value: _PT_co) -> None: ...  # type: ignore [misc]

    def __edgeql_literal__(self) -> _qb.Literal | _qb.CastOp:
        return get_literal_for_scalar(type(self), self)


UUIDFieldsTuple = TypeAliasType(
    "UUIDFieldsTuple", tuple[int, int, int, int, int, int]
)


class UUIDImpl(uuid.UUID):
    def __init__(  # noqa: PLR0917
        self,
        hex: uuid.UUID | str | None = None,  # noqa: A002
        bytes: builtins.bytes | None = None,  # noqa: A002
        bytes_le: builtins.bytes | None = None,
        fields: UUIDFieldsTuple | None = None,
        int: builtins.int | None = None,  # noqa: A002
        version: builtins.int | None = None,
        *,
        is_safe: uuid.SafeUUID = uuid.SafeUUID.unknown,
    ) -> None:
        if hex is not None and isinstance(hex, uuid.UUID):
            super().__init__(
                int=hex.int,
                is_safe=hex.is_safe,
            )
        else:
            super().__init__(
                hex,
                bytes,
                bytes_le,
                fields,
                int,
                version,
                is_safe=is_safe,
            )


class DateImpl(datetime.date):
    def __new__(
        cls,
        year: datetime.date | SupportsIndex,
        month: SupportsIndex,
        day: SupportsIndex,
    ) -> Self:
        if isinstance(year, datetime.date):
            dt = year
            return cls(dt.year, dt.month, dt.day)
        else:
            return super().__new__(cls, year, month, day)


class TimeImpl(datetime.time):
    if TYPE_CHECKING:

        def __new__(
            cls,
            hour: SupportsIndex = ...,
            minute: SupportsIndex = ...,
            second: SupportsIndex = ...,
            microsecond: SupportsIndex = ...,
            tzinfo: datetime.tzinfo | None = ...,
            *,
            fold: int = ...,
        ) -> Self: ...
    else:

        def __new__(
            cls,
            hour,
            *args,
            **kwargs,
        ):
            if isinstance(hour, datetime.time):
                t = hour
                return cls(
                    hour=t.hour,
                    minute=t.minute,
                    second=t.second,
                    microsecond=t.microsecond,
                    tzinfo=t.tzinfo,
                    fold=t.fold,
                )
            else:
                return super().__new__(
                    cls,
                    hour,
                    *args,
                    **kwargs,
                )


class DateTimeImpl(datetime.datetime):
    if TYPE_CHECKING:

        def __new__(  # noqa: PLR0917
            cls,
            year: SupportsIndex,
            month: SupportsIndex,
            day: SupportsIndex,
            hour: SupportsIndex = ...,
            minute: SupportsIndex = ...,
            second: SupportsIndex = ...,
            microsecond: SupportsIndex = ...,
            tzinfo: datetime.tzinfo | None = ...,
            *,
            fold: int = ...,
        ) -> Self: ...
    else:

        def __new__(
            cls,
            year,
            *args,
            **kwargs,
        ):
            if isinstance(year, datetime.datetime):
                dt = year
                return cls(
                    year=dt.year,
                    month=dt.month,
                    day=dt.day,
                    hour=dt.hour,
                    minute=dt.minute,
                    second=dt.second,
                    microsecond=dt.microsecond,
                    tzinfo=dt.tzinfo,
                    fold=dt.fold,
                )
            else:
                return super().__new__(
                    cls,
                    year,
                    *args,
                    **kwargs,
                )


class TimeDeltaImpl(datetime.timedelta):
    if TYPE_CHECKING:

        def __new__(  # noqa: PLR0917
            cls,
            days: float = ...,
            seconds: float = ...,
            microseconds: float = ...,
            milliseconds: float = ...,
            minutes: float = ...,
            hours: float = ...,
            weeks: float = ...,
        ) -> Self: ...
    else:

        def __new__(
            cls,
            days,
            *args,
            **kwargs,
        ):
            if isinstance(days, datetime.timedelta):
                td = days
                return cls(td.days, td.seconds, td.microseconds)
            else:
                return super().__new__(
                    cls,
                    days,
                    *args,
                    **kwargs,
                )
