# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

from __future__ import annotations

from typing import (
    Any,
    ClassVar,
    NamedTuple,
    TypeVar,
    cast,
)

import pydantic
import pydantic.fields
from pydantic._internal import _model_construction
import pydantic_core

from pydantic import PrivateAttr as PrivateAttr
from pydantic import computed_field as computed_field
from gel._internal import _typing_parametric as parametric


T = TypeVar("T")



class ValidatedType(parametric.SingleParametricType[T]):
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


class GelMetadata(NamedTuple):
    schema_name: str


class GelPointer(pydantic.fields.FieldInfo):
    __slots__ = tuple(pydantic.fields.FieldInfo.__slots__) + ("_gel_name",)

    def __init__(
        self,
        _gel_name: str,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self._gel_name = _gel_name


class Exclusive:
    pass


def _get_pointer_from_field(
    name: str,
    field: pydantic.fields.FieldInfo,
) -> GelPointer:
    return GelPointer(
        _gel_name=name,
        **field._attributes_set,
    )


class GelModelMeta(_model_construction.ModelMetaclass):
    def __new__(
        cls,
        name: str,
        bases: tuple[type[Any], ...],
        namespace: dict[str, Any],
        **kwargs: Any,
    ) -> GelModelMeta:
        new_cls = cast(
            type[pydantic.BaseModel],
            super().__new__(cls, name, bases, namespace, **kwargs),
        )

        for name, field in new_cls.__pydantic_fields__.items():
            col = _get_pointer_from_field(name, field)
            setattr(new_cls, name, col)

        return new_cls  # type: ignore


class GelModel(pydantic.BaseModel, metaclass=GelModelMeta):
    __gel_metadata__: ClassVar[GelMetadata]
