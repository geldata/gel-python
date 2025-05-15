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

import typing

import pydantic
import pydantic_core
from pydantic_core import core_schema


from gel._internal import _typing_inspect

from gel._internal._qbmodel import _abstract

if TYPE_CHECKING:

    import pydantic.fields


_T = TypeVar("_T")
_T_co = TypeVar("_T_co", covariant=True)


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


class PyTypeScalar(_abstract.PyTypeScalar[_T_co]):
    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source_type: Any,
        handler: pydantic.GetCoreSchemaHandler,
    ) -> pydantic_core.CoreSchema:
        return pydantic_core.core_schema.no_info_after_validator_function(
            cls.type,
            handler(cls.type),
        )
