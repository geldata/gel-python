# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

"""Pydantic implementation of the query builder model"""

from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Any,
    TypeVar,
)

from typing_extensions import (
    TypeVarTuple,
    Unpack,
)

import builtins
import datetime
import decimal
import typing
import uuid

import pydantic
import pydantic_core
from pydantic_core import core_schema


from gel.datatypes.datatypes import CustomType
from gel._internal import _typing_inspect
from gel._internal._qbmodel import _abstract


if TYPE_CHECKING:
    from collections.abc import Callable

    import pydantic.fields


_T = TypeVar("_T")


class Array(_abstract.Array[_T]):
    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source_type: Any,
        handler: pydantic.GetCoreSchemaHandler,
    ) -> pydantic_core.CoreSchema:
        if _typing_inspect.is_generic_alias(source_type):
            args = typing.get_args(source_type)
            item_type = args[0]
            return core_schema.list_schema(
                items_schema=handler.generate_schema(item_type),
                serialization=core_schema.plain_serializer_function_ser_schema(
                    list,
                ),
            )
        else:
            return handler.generate_schema(source_type)


_Ts = TypeVarTuple("_Ts")


class Tuple(_abstract.Tuple[Unpack[_Ts]]):
    __slots__ = ()

    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source_type: Any,
        handler: pydantic.GetCoreSchemaHandler,
    ) -> pydantic_core.CoreSchema:
        if _typing_inspect.is_generic_alias(source_type):
            args = typing.get_args(source_type)
            return core_schema.tuple_schema(
                items_schema=[handler.generate_schema(arg) for arg in args],
                serialization=core_schema.plain_serializer_function_ser_schema(
                    tuple,
                ),
            )
        else:
            return handler.generate_schema(source_type)


def _get_range_core_schema(
    source_type: Any,
    handler: pydantic.GetCoreSchemaHandler,
) -> core_schema.ModelFieldsSchema:
    args = typing.get_args(source_type)
    item_schema = handler.generate_schema(args[0])
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
    def __get_pydantic_core_schema__(
        cls,
        source_type: Any,
        handler: pydantic.GetCoreSchemaHandler,
    ) -> pydantic_core.CoreSchema:
        if _typing_inspect.is_generic_alias(source_type):
            return _get_range_core_schema(source_type, handler)
        else:
            return handler.generate_schema(source_type)


class MultiRange(_abstract.MultiRange[_T]):
    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source_type: Any,
        handler: pydantic.GetCoreSchemaHandler,
    ) -> pydantic_core.CoreSchema:
        if _typing_inspect.is_generic_alias(source_type):
            range_schema = _get_range_core_schema(source_type, handler)
            return core_schema.list_schema(range_schema)
        else:
            return handler.generate_schema(source_type)


_PT_co = TypeVar("_PT_co", bound=_abstract.PyConstType, covariant=True)


_py_type_to_schema: dict[
    type[_abstract.PyConstType],
    Callable[[], pydantic_core.CoreSchema],
] = {
    builtins.bool: core_schema.bool_schema,
    builtins.int: core_schema.int_schema,
    builtins.float: core_schema.float_schema,
    builtins.str: core_schema.str_schema,
    datetime.date: core_schema.date_schema,
    datetime.datetime: core_schema.datetime_schema,
    datetime.timedelta: core_schema.timedelta_schema,
    decimal.Decimal: core_schema.decimal_schema,
    uuid.UUID: core_schema.uuid_schema,
}


class PyTypeScalar(_abstract.PyTypeScalar[_PT_co]):
    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source_type: Any,
        handler: pydantic.GetCoreSchemaHandler,
    ) -> pydantic_core.CoreSchema:
        schema = _py_type_to_schema.get(cls.type)  # type: ignore [arg-type]
        if schema is not None:
            return schema()
        elif issubclass(cls.type, CustomType):
            return core_schema.no_info_plain_validator_function(
                cls.type,
            )
        else:
            return core_schema.invalid_schema()
