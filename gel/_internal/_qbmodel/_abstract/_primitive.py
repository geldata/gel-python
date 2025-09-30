# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

"""Primitive (non-object) types used to implement class-based query builders"""

from __future__ import annotations
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    cast,
    ClassVar,
    Final,
    Generic,
    Protocol,
    SupportsIndex,
    overload,
)
from typing_extensions import (
    Self,
    TypeVarTuple,
    TypeAliasType,
    TypeVar,
    Unpack,
)
from collections.abc import Iterable

import builtins
import datetime
import decimal
import functools
import numbers
import sys
import typing
import warnings
import uuid

from gel.datatypes import datatypes as _datatypes
from gel.datatypes import range as _range

from gel._internal import _edgeql
from gel._internal import _qb
from gel._internal import _typing_parametric
from gel._internal._lazyprop import LazyClassProperty
from gel._internal._polyfills._strenum import StrEnum
from gel._internal._schemapath import SchemaPath

from ._base import GelType, GelTypeConstraint, GelTypeMeta
from ._functions import assert_single


if TYPE_CHECKING:
    import enum
    from collections.abc import Mapping, MutableMapping, Sequence


_T = TypeVar("_T", bound=GelType)
_GelPrimitiveType_T = TypeVar("_GelPrimitiveType_T", bound="GelPrimitiveType")


class GelPrimitiveTypeConstraint(GelTypeConstraint[_GelPrimitiveType_T]):
    pass


class GelPrimitiveType(GelType):
    if TYPE_CHECKING:

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
            return _qb.AnnotatedExpr(
                cls,
                assert_single(cls, message=message, __operand__=__operand__),
            )


class GelScalarType(GelPrimitiveType):
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
    if sys.version_info >= (3, 13):

        class AnyEnumMeta(type(StrEnum), GelTypeMeta):
            pass
    else:

        class AnyEnumMeta(type(StrEnum), GelTypeMeta):
            @classmethod
            def __prepare__(
                mcls,  # noqa: N804
                name: str,
                bases: tuple[type, ...],
                /,
                **kwargs: Any,
            ) -> MutableMapping[str, object]:
                ns = super().__prepare__(name, bases, **kwargs)

                class _NoWarningEnumDict(type(ns)):
                    def __setitem__(self, key: str, value: Any) -> None:
                        with warnings.catch_warnings():
                            # Suppress bogus DeprecationWarning from enum
                            # regarding member classes.
                            warnings.filterwarnings(
                                "ignore",
                                category=DeprecationWarning,
                            )
                            super().__setitem__(key, value)

                enum_dict = _NoWarningEnumDict()
                for k, v in ns.items():
                    dict.__setitem__(enum_dict, k, v)
                enum_dict._cls_name = name
                return enum_dict


class AnyEnum(GelScalarType, StrEnum, metaclass=AnyEnumMeta):
    def __edgeql_literal__(self) -> _qb.Literal | _qb.CastOp:
        return _qb.Literal(
            val=str(self),
            type_=type(self).__gel_reflection__.name,
        )


if TYPE_CHECKING:

    class HomogeneousCollectionMeta(
        _typing_parametric.PickleableClassParametricTypeMeta,
        GelTypeMeta,
    ):
        pass
else:

    class HomogeneousCollectionMeta(
        _typing_parametric.PickleableClassParametricTypeMeta,
        type(GelPrimitiveType),
    ):
        pass


class BaseCollection:
    @classmethod
    def __gel_get_py_type__(cls) -> type:
        raise NotImplementedError


class HomogeneousCollection(
    _typing_parametric.PickleableClassParametricType,
    GelPrimitiveType,
    BaseCollection,
    Generic[_T],
    metaclass=HomogeneousCollectionMeta,
):
    __element_type__: ClassVar[type[_T]]  # type: ignore [misc]

    @LazyClassProperty[type[GelPrimitiveType.__gel_reflection__]]
    @classmethod
    def __gel_reflection__(cls) -> type[GelPrimitiveType.__gel_reflection__]:  # pyright: ignore [reportIncompatibleVariableOverride]
        class __gel_reflection__(GelPrimitiveType.__gel_reflection__):  # noqa: N801
            pass

        return __gel_reflection__


if TYPE_CHECKING:

    class ArrayMeta(
        HomogeneousCollectionMeta,
        GelTypeMeta,
        typing._ProtocolMeta,
    ):
        pass
else:

    class ArrayMeta(type(HomogeneousCollection), GelTypeMeta):
        pass


class Array(  # type: ignore [misc]
    HomogeneousCollection[_T],
    list[_T],
    metaclass=ArrayMeta,
):
    if TYPE_CHECKING:

        def __set__(
            self,
            instance: Any,
            value: Array[_T] | Sequence[_T],
            /,
        ) -> None: ...

    @LazyClassProperty[type[GelPrimitiveType.__gel_reflection__]]
    @classmethod
    def __gel_reflection__(cls) -> type[GelPrimitiveType.__gel_reflection__]:  # pyright: ignore [reportIncompatibleVariableOverride]
        tid, tname = _edgeql.get_array_type_id_and_name(
            str(cls.__element_type__.__gel_reflection__.name)
        )

        class __gel_reflection__(GelPrimitiveType.__gel_reflection__):  # noqa: N801
            id = tid
            name = tname

        return __gel_reflection__

    def __reduce__(self) -> tuple[Any, ...]:
        cls = type(self)
        return (
            cls._reconstruct_from_pickle,
            (
                cls.__parametric_origin__,
                cls.__element_type__,
                list(self),
            ),
        )

    @staticmethod
    def _reconstruct_from_pickle(
        origin: type[Array[_T]],
        tp: type[_T],  # pyright: ignore [reportGeneralTypeIssues]
        items: list[_T],
    ) -> Array[_T]:
        cls = cast(
            "type[Array[_T]]",
            origin[tp],  # type: ignore [index]
        )
        return cls(items)

    @classmethod
    def __gel_get_py_type__(cls) -> type:
        return list


_Ts = TypeVarTuple("_Ts")


class HeterogeneousCollection(
    _typing_parametric.PickleableClassParametricType,
    BaseCollection,
    Generic[Unpack[_Ts]],
):
    __element_types__: ClassVar[
        Annotated[
            tuple[type[GelType], ...],
            Unpack[_Ts],  # pyright: ignore [reportGeneralTypeIssues]
        ]
    ]


if TYPE_CHECKING:

    class TupleMeta(
        _typing_parametric.PickleableClassParametricTypeMeta,
        GelTypeMeta,
        typing._ProtocolMeta,
    ):
        pass
else:

    class TupleMeta(type(HeterogeneousCollection), GelTypeMeta):
        pass


class Tuple(  # type: ignore[misc]
    HeterogeneousCollection[Unpack[_Ts]],
    AnyTuple,
    tuple[Unpack[_Ts]],
    metaclass=TupleMeta,
):
    __slots__ = ()

    if TYPE_CHECKING:

        def __set__(
            self,
            instance: Any,
            value: Tuple[Unpack[_Ts]] | tuple[Unpack[_Ts]],
            /,
        ) -> None: ...

    @LazyClassProperty[type[GelPrimitiveType.__gel_reflection__]]
    @classmethod
    def __gel_reflection__(cls) -> type[GelPrimitiveType.__gel_reflection__]:  # pyright: ignore [reportIncompatibleVariableOverride]
        tid, tname = _edgeql.get_tuple_type_id_and_name(
            str(el.__gel_reflection__.name) for el in cls.__element_types__
        )

        class __gel_reflection__(GelPrimitiveType.__gel_reflection__):  # noqa: N801
            id = tid
            name = tname

        return __gel_reflection__

    @classmethod
    def __gel_get_py_type__(cls) -> type:
        return tuple


if TYPE_CHECKING:

    class RangeMeta(HomogeneousCollectionMeta, GelTypeMeta):
        pass
else:

    class RangeMeta(type(HomogeneousCollection), GelTypeMeta):
        pass


class Range(
    HomogeneousCollection[_T],
    _range.Range[_T],
    metaclass=RangeMeta,
):
    if TYPE_CHECKING:

        def __set__(
            self,
            instance: Any,
            value: Range[_T] | _range.Range[_T],
            /,
        ) -> None: ...

    @LazyClassProperty[type[GelPrimitiveType.__gel_reflection__]]
    @classmethod
    def __gel_reflection__(cls) -> type[GelPrimitiveType.__gel_reflection__]:  # pyright: ignore [reportIncompatibleVariableOverride]
        tid, tname = _edgeql.get_range_type_id_and_name(
            str(cls.__element_type__.__gel_reflection__.name)
        )

        class __gel_reflection__(GelPrimitiveType.__gel_reflection__):  # noqa: N801
            id = tid
            name = tname

        return __gel_reflection__


if TYPE_CHECKING:

    class MultiRangeMeta(HomogeneousCollectionMeta, GelTypeMeta):
        pass
else:

    class MultiRangeMeta(
        type(HomogeneousCollection), type(Iterable), GelTypeMeta
    ):
        pass


class MultiRange(
    HomogeneousCollection[_T],
    _range.MultiRange[_T],
    metaclass=MultiRangeMeta,
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


def get_py_type_from_gel_type(tp: type[GelType]) -> Any:
    match tp:
        case t if issubclass(t, Array):
            base = t.__gel_get_py_type__()
            assert issubclass(base, list)
            return base.__class_getitem__(
                get_py_type_from_gel_type(t.__element_type__)
            )
        case t if issubclass(t, Tuple):
            base = t.__gel_get_py_type__()
            assert issubclass(base, tuple)
            return base.__class_getitem__(
                tuple(
                    get_py_type_from_gel_type(el) for el in t.__element_types__
                )
            )
        case t if issubclass(t, PyTypeScalar):
            return t.__gel_py_type__

        case t:
            raise NotImplementedError(
                f"get_py_type({t.__name__}) is not implemented"
            )


_scalar_type_to_py_type: dict[str, tuple[str, str]] = {
    #
    # Integers
    #
    "std::bigint": ("builtins", "int"),
    "std::int16": ("builtins", "int"),
    "std::int32": ("builtins", "int"),
    "std::int64": ("builtins", "int"),
    #
    # Floats
    #
    "std::float32": ("builtins", "float"),
    "std::float64": ("builtins", "float"),
    #
    # Rest of builtins
    #
    "std::bool": ("builtins", "bool"),
    "std::bytes": ("builtins", "bytes"),
    "std::str": ("builtins", "str"),
    #
    # Decimal
    #
    "std::decimal": ("decimal", "Decimal"),
    #
    # JSON is arbitrary data
    #
    "std::json": ("builtins", "str"),
    #
    # Dates, times, and intervals
    #
    "std::datetime": ("datetime", "datetime"),
    "std::duration": ("datetime", "timedelta"),
    "std::cal::local_date": ("datetime", "date"),
    "std::cal::local_datetime": ("datetime", "datetime"),
    "std::cal::local_time": ("datetime", "time"),
    "std::cal::date_duration": ("gel", "DateDuration"),
    "std::cal::relative_duration": ("gel", "RelativeDuration"),
    #
    # Other types
    #
    "std::uuid": ("uuid", "UUID"),
    "cfg::memory": ("gel", "ConfigMemory"),
    "ext::pgvector::vector": ("array", "array"),
}


#
# The inverse of the above, order of scalar names in value lists
# indicates priority when doing overload deconfliction.
#
_py_type_to_scalar_type: dict[tuple[str, str], tuple[str, ...]] = {
    ("gel", "DateDuration"): ("std::cal::date_duration",),
    ("gel", "RelativeDuration"): ("std::cal::relative_duration",),
    ("gel", "ConfigMemory"): ("cfg::memory",),
    ("builtins", "bool"): ("std::bool",),
    ("builtins", "bytes"): ("std::bytes",),
    ("builtins", "float"): (
        "std::float64",
        "std::float32",
    ),
    ("builtins", "int"): (
        "std::bigint",
        "std::int64",
        "std::int32",
        "std::int16",
    ),
    ("builtins", "str"): ("std::str", "std::json"),
    ("datetime", "datetime"): ("std::datetime", "std::cal::local_datetime"),
    ("datetime", "timedelta"): ("std::duration",),
    ("datetime", "date"): ("std::cal::local_date",),
    ("datetime", "time"): ("std::cal::local_time",),
    ("decimal", "Decimal"): ("std::decimal",),
    ("uuid", "UUID"): ("std::uuid",),
}

#
# Builtin types that overlap in Python but not in Gel
# NB: the order specifies the order of overloads from
#     more specific to less specific.
#
_overlapping_py_types = (
    # bool is a subclass of int
    ("builtins", "bool"),
    ("builtins", "int"),
    # bytes is a Sequence[int] and would collide
    # with set-valued ints.
    ("builtins", "bytes"),
)

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
        if require_subclassable and base_type == ("builtins", "bool"):
            base_type = ("builtins", "int")
        return (base_type,)
    elif consider_generic:
        return tuple(sorted(_get_py_type_for_generic_scalar(typename)))
    else:
        return ()


def get_base_scalars_backed_by_py_type() -> Mapping[str, tuple[str, str]]:
    return _scalar_type_to_py_type


def get_overlapping_py_types() -> tuple[tuple[str, str], ...]:
    return _overlapping_py_types


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
    "std::json": (MODEL_SUBSTRATE_MODULE, "JSONImpl"),
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


_ambiguous_py_types: dict[
    tuple[str, str], dict[SchemaPath, tuple[SchemaPath, ...]]
] = {
    key: scalars
    for key, scalars in {
        key: {
            parent: tuple(p for p in scalars if p.parent == parent)
            for parent in {p.parent for p in scalars}
        }
        for key, scalars in {
            key: tuple(SchemaPath(v) for v in vals)
            for key, vals in _py_type_to_scalar_type.items()
        }.items()
        if len(scalars) > 1
    }.items()
    if scalars
}


_ambiguous_py_types_by_mod: dict[
    SchemaPath, dict[tuple[str, str], tuple[SchemaPath, ...]]
] = {
    parent: {
        key: scalars[parent]
        for key, scalars in _ambiguous_py_types.items()
        if parent in scalars
    }
    for parent in {
        p for scalars in _ambiguous_py_types.values() for p in scalars
    }
}


def get_scalar_type_disambiguation_for_py_type(
    py_type: tuple[str, str],
) -> Mapping[SchemaPath, tuple[SchemaPath, ...]]:
    return _ambiguous_py_types.get(py_type, {})


def get_scalar_type_disambiguation_for_mod(
    mod: SchemaPath,
) -> Mapping[tuple[str, str], tuple[SchemaPath, ...]]:
    return _ambiguous_py_types_by_mod.get(mod, {})


_PROTOCOL_META = ("typing", "_ProtocolMeta")

_py_type_typecheck_meta_bases: dict[
    tuple[str, str], tuple[tuple[str, str], ...]
] = {
    ("builtins", "str"): (_PROTOCOL_META,),
    ("builtins", "bytes"): (_PROTOCOL_META,),
}


def get_py_type_typecheck_meta_bases(
    py_type: tuple[str, str],
) -> tuple[tuple[str, str], ...]:
    return _py_type_typecheck_meta_bases.get(py_type, ())


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
_ST = TypeVar("_ST", bound=GelScalarType, default=GelScalarType)


def get_literal_for_value(
    v: Any,
) -> _qb.Literal:
    for t, ltype in _py_type_to_literal.items():
        if isinstance(v, t):
            return ltype(val=v)  # type: ignore [call-arg]

    raise NotImplementedError(f"cannot convert Python value to Literal: {v!r}")


def get_literal_for_scalar(
    t: type[GelScalarType],
    v: Any,
) -> _qb.Literal | _qb.CastOp:
    if not isinstance(v, t):
        v = t(v)  # type: ignore [call-arg]
    ltype = None
    if issubclass(t, PyTypeScalar):
        ltype = _py_type_to_literal.get(t.__gel_py_type__)
    if ltype is not None:
        return ltype(val=v)  # type: ignore [call-arg]
    else:
        return _qb.CastOp(
            expr=_qb.StringLiteral(val=str(v)),
            type_=t.__gel_reflection__.name,
        )


class PyTypeScalar(
    _typing_parametric.ParametricType,
    GelScalarType,
    Generic[_PT_co],
):
    __gel_py_type__: ClassVar[type[_PT_co]]  # type: ignore [misc]

    if TYPE_CHECKING:

        @overload  # type: ignore [override, unused-ignore]
        def __get__(
            self, instance: None, owner: type[Any], /
        ) -> type[Self]: ...

        @overload
        def __get__(
            self, instance: Any, owner: type[Any] | None = None, /
        ) -> _PT_co: ...

        def __get__(  # type: ignore [override, unused-ignore]
            self,
            instance: Any | None,
            owner: type[Any] | None = None,
            /,
        ) -> type[Self] | _PT_co: ...

        def __set__(self, instance: Any, value: _PT_co, /) -> None: ...  # type: ignore [misc]

    def __edgeql_literal__(self) -> _qb.Literal | _qb.CastOp:
        return get_literal_for_scalar(type(self), self)


class PyTypeScalarConstraint(
    _typing_parametric.ParametricType,
    GelPrimitiveTypeConstraint[_ST],
    Generic[_ST],
):
    __gel_type_constraint__: ClassVar[type[_ST]]  # type: ignore [misc]


class JSONImpl(str):  # noqa: FURB189
    __slots__ = ()


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
    if TYPE_CHECKING:

        @overload
        def __new__(
            cls,
            year: SupportsIndex = ...,
            month: SupportsIndex = ...,
            day: SupportsIndex = ...,
        ) -> Self: ...

        @overload
        def __new__(
            cls,
            year: datetime.date,
        ) -> Self: ...

        def __new__(
            cls,
            year: SupportsIndex | datetime.date = ...,
            month: SupportsIndex = ...,
            day: SupportsIndex = ...,
        ) -> Self: ...
    else:

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

        @overload
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

        @overload
        def __new__(
            cls,
            hour: datetime.time,
        ) -> Self: ...

        def __new__(
            cls,
            hour: SupportsIndex | datetime.time = ...,
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

        @overload
        def __new__(
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

        @overload
        def __new__(
            cls,
            year: DateTimeLike,
        ) -> Self: ...

        def __new__(  # noqa: PLR0917
            cls,
            year: SupportsIndex | DateTimeLike,
            month: SupportsIndex = ...,
            day: SupportsIndex = ...,
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

        @overload
        def __new__(
            cls,
            days: float = ...,
            seconds: float = ...,
            microseconds: float = ...,
            milliseconds: float = ...,
            minutes: float = ...,
            hours: float = ...,
            weeks: float = ...,
        ) -> Self: ...

        @overload
        def __new__(
            cls,
            days: datetime.timedelta,
        ) -> Self: ...

        def __new__(  # noqa: PLR0917
            cls,
            days: float | datetime.timedelta = ...,
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
