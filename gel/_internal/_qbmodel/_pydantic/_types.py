# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

"""Pydantic implementation of the query builder model"""

from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Any,
)

from typing_extensions import (
    TypeVar,
    TypeVarTuple,
    Unpack,
)

import builtins
import datetime
import decimal
import uuid

# Accept raw Range/MultiRange instances from gel.datatypes
from gel.datatypes import range as _range

import pydantic_core
from pydantic_core import core_schema


from gel.datatypes.datatypes import CustomType
from gel._internal._qbmodel import _abstract
from gel._internal import _tracked_list


if TYPE_CHECKING:
    from collections.abc import Callable

    import pydantic


_T = TypeVar("_T", bound=_abstract.GelType)


class Array(_abstract.Array[_T]):
    @classmethod
    def _validate(
        cls,
        value: Any,
    ) -> _tracked_list.DowncastingTrackedList[_T, _T]:  # XXX
        tp = cls.__gel_resolve_dlist__()
        if isinstance(value, tp):
            return value
        else:
            return tp(value, __mode__=_tracked_list.Mode.ReadWrite)

    @classmethod
    def __gel_resolve_dlist__(
        cls,
    ) -> type[_tracked_list.DowncastingTrackedList[_T, _T]]:  # XXX
        down = _abstract.get_py_type_from_gel_type(cls.__element_type__)
        return _tracked_list.DowncastingTrackedList[cls.__element_type__, down]  # type: ignore [name-defined, valid-type]

    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source_type: Any,
        handler: pydantic.GetCoreSchemaHandler,
    ) -> pydantic_core.CoreSchema:
        return core_schema.no_info_plain_validator_function(
            cls._validate,
            serialization=core_schema.plain_serializer_function_ser_schema(
                list,
            ),
        )


_Ts = TypeVarTuple("_Ts")


class Tuple(_abstract.Tuple[Unpack[_Ts]]):
    __slots__ = ()

    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source_type: Any,
        handler: pydantic.GetCoreSchemaHandler,
    ) -> pydantic_core.CoreSchema:
        return core_schema.tuple_schema(
            items_schema=[
                handler.generate_schema(arg) for arg in cls.__element_types__
            ],
            serialization=core_schema.plain_serializer_function_ser_schema(
                tuple,
            ),
        )


def _get_range_core_schema(
    source_type: type[Range[_T] | MultiRange[_T]],
    handler: pydantic.GetCoreSchemaHandler,
) -> core_schema.ModelFieldsSchema:
    item_schema = handler.generate_schema(source_type.__element_type__)  # type: ignore [misc]
    opt_item_schema = core_schema.nullable_schema(item_schema)
    item_field_schema = core_schema.model_field(opt_item_schema)
    bool_schema = core_schema.bool_schema()
    bool_field_schema = core_schema.model_field(bool_schema)
    return core_schema.model_fields_schema(
        {
            "lower": item_field_schema,
            "upper": item_field_schema,
            "inc_lower": bool_field_schema,
            "inc_upper": bool_field_schema,
            "empty": bool_field_schema,
        }
    )


class Range(_abstract.Range[_T]):
    @classmethod
    def _validate_bound(cls, eltype: type, value: Any) -> Any:
        if value is None:
            return None

        if issubclass(eltype, (datetime.date, datetime.datetime)):
            return eltype.fromisoformat(value)

        return eltype(value)  # type: ignore [call-arg]

    @classmethod
    def _serialize_bound(cls, eltype: type, value: Any) -> Any:
        if value is None:
            return None

        if issubclass(eltype, (datetime.date, datetime.datetime)):
            return value.isoformat()

        return value

    @classmethod
    def _validate(cls, eltype: type, value: Any) -> _range.Range[_T]:
        if isinstance(value, _range.Range):
            # TODO -- this needs to be further validated
            return value

        elif isinstance(value, dict):
            return cls(
                lower=cls._validate_bound(eltype, value["lower"]),
                upper=cls._validate_bound(eltype, value["upper"]),
                inc_lower=value["inc_lower"],
                inc_upper=value["inc_upper"],
                empty=value["empty"],
            )
        else:
            raise TypeError(
                "value must be a gel.Range instance or "
                "a dict of range constructor arguments"
            )

    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source_type: Any,
        handler: pydantic.GetCoreSchemaHandler,
    ) -> pydantic_core.CoreSchema:
        eltype = source_type.__element_type__.__gel_py_type__
        python_schema = core_schema.no_info_plain_validator_function(
            lambda value: cls._validate(eltype, value)
        )

        json_fields_schema = _get_range_core_schema(cls, handler)

        return core_schema.json_or_python_schema(
            json_schema=json_fields_schema,
            python_schema=python_schema,
            serialization=core_schema.plain_serializer_function_ser_schema(
                lambda v: {
                    "lower": cls._serialize_bound(eltype, v.lower),
                    "upper": cls._serialize_bound(eltype, v.upper),
                    "inc_lower": v.inc_lower,
                    "inc_upper": v.inc_upper,
                    "empty": v.is_empty(),
                },
            ),
        )


class MultiRange(_abstract.MultiRange[_T]):
    @classmethod
    def _validate(cls, eltype: type, value: Any) -> _range.MultiRange[_T]:
        if isinstance(value, _range.MultiRange):
            # TODO -- this needs to be further validated
            return value

        elif isinstance(value, list) and all(
            isinstance(i, dict) for i in value
        ):
            return _range.MultiRange(
                [
                    _range.Range(  # type: ignore [misc]
                        lower=Range._validate_bound(eltype, i["lower"]),
                        upper=Range._validate_bound(eltype, i["upper"]),
                        inc_lower=i["inc_lower"],
                        inc_upper=i["inc_upper"],
                        empty=i["empty"],
                    )
                    for i in value
                ]
            )
        else:
            raise TypeError(
                "value must be a gel.MultiRange instance or "
                "a list of dicts of range constructor arguments"
            )

    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source_type: Any,
        handler: pydantic.GetCoreSchemaHandler,
    ) -> pydantic_core.CoreSchema:
        wrapped_range = source_type.__element_type__
        eltype = wrapped_range.__element_type__.__gel_py_type__

        python_schema = core_schema.no_info_plain_validator_function(
            lambda value: cls._validate(eltype, value)
        )

        json_fields_schema = core_schema.list_schema(
            _get_range_core_schema(cls, handler)
        )

        return core_schema.json_or_python_schema(
            json_schema=json_fields_schema,
            python_schema=python_schema,
            serialization=core_schema.plain_serializer_function_ser_schema(
                lambda value: [
                    {
                        "lower": Range._serialize_bound(eltype, v.lower),
                        "upper": Range._serialize_bound(eltype, v.upper),
                        "inc_lower": v.inc_lower,
                        "inc_upper": v.inc_upper,
                        "empty": v.is_empty(),
                    }
                    for v in value
                ],
            ),
        )


_PT_co = TypeVar("_PT_co", bound=_abstract.PyConstType, covariant=True)


_py_type_to_schema: dict[
    type[_abstract.PyConstType],
    Callable[[], pydantic_core.CoreSchema],
] = {
    builtins.bool: core_schema.bool_schema,
    builtins.int: core_schema.int_schema,
    builtins.float: core_schema.float_schema,
    builtins.str: core_schema.str_schema,
    builtins.bytes: core_schema.bytes_schema,
    datetime.date: core_schema.date_schema,
    datetime.datetime: core_schema.datetime_schema,
    datetime.time: core_schema.time_schema,
    datetime.timedelta: core_schema.timedelta_schema,
    decimal.Decimal: core_schema.decimal_schema,
    uuid.UUID: core_schema.uuid_schema,
}


class PyTypeScalar(_abstract.PyTypeScalar[_PT_co]):
    if TYPE_CHECKING:

        @classmethod
        def __edgeql__(cls) -> tuple[type[_PT_co], str]: ...  # type: ignore [override]

    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source_type: Any,
        handler: pydantic.GetCoreSchemaHandler,
    ) -> pydantic_core.CoreSchema:
        py_type = cls.__gel_py_type__
        schema = _py_type_to_schema.get(py_type)  # type: ignore [arg-type]
        if schema is not None:
            return schema()
        elif issubclass(py_type, CustomType):
            return core_schema.no_info_plain_validator_function(py_type)
        else:
            return core_schema.invalid_schema()
