# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

from __future__ import annotations

from typing import (
    Annotated,
    Any,
    ClassVar,
    Generic,
    Iterable,
    NamedTuple,
    TypeVar,
    cast,
)

from typing_extensions import (
    Self,
    TypeAliasType,
)

from collections.abc import (
    Hashable,
    Sequence,
)

import dataclasses
import uuid

import pydantic
import pydantic.fields
from pydantic._internal import _model_construction
import pydantic_core
from pydantic_core import core_schema as pydantic_schema

from pydantic import Field as Field
from pydantic import PrivateAttr as PrivateAttr
from pydantic import computed_field as computed_field
from gel._internal import _typing_parametric as parametric

from . import lists


class _DistinctList(lists.DistinctList[lists.T], Generic[lists.T]):
    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        _source_type: Any,
        handler: pydantic.GetCoreSchemaHandler,
    ) -> pydantic_core.CoreSchema:
        return pydantic_schema.no_info_before_validator_function(
            cls._validate,
            pydantic_schema.list_schema(
                items_schema=pydantic_schema.any_schema(),
            ),
        )

    @classmethod
    def _validate(cls, value: Any) -> Self:
        if isinstance(value, cls):
            return value
        if isinstance(value, list):
            return cls(value)
        raise TypeError(f'could not convert {type(value)} to {cls.__name__}')

    @classmethod
    def _check_value(cls, value: Any) -> lists.T:
        t = cls.type
        if isinstance(value, t):
            return value
        elif (
            issubclass(t, ProxyModel)
            and isinstance(value, t.__bases__[0])
        ):
            return t.__from_object__(value)  # type: ignore [return-value]

        raise ValueError(
            f"{cls!r} accepts only values of type {cls.type!r}, "
            f"got {type(value)!r}"
        )


DistinctList = TypeAliasType(
    "DistinctList",
    "Annotated[_DistinctList[lists.T], Field(default_factory=_DistinctList)]",
    type_params=(lists.T,)
)

RequiredDistinctList = TypeAliasType(
    "RequiredDistinctList",
    "list[lists.T]",
    type_params=(lists.T,)
)

T = TypeVar("T")

OptionalWithDefault = TypeAliasType(
    "OptionalWithDefault",
    "Annotated[T, Field(default=None)]",
    type_params=(T,)
)


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


@dataclasses.dataclass(kw_only=True, frozen=True)
class ObjectTypeReflection:
    id: uuid.UUID
    name: str


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
    __gel_type_reflection__: ClassVar[ObjectTypeReflection]

    def __init__(self, /, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._p__id: uuid.UUID | None = None
        self._p____type__ = None

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, GelModel):
            return NotImplemented

        if self._p__id is None or other._p__id is None:
            return False
        else:
            return self._p__id == other._p__id

    def __hash__(self) -> int:
        if self._p__id is None:
            raise TypeError(
                "Model instances without id value are unhashable")

        return hash(self._p__id)


class ProxyModel(Generic[T]):
    @classmethod
    def __from_object__(cls, obj: T) -> ProxyModel[T]:
        return cls()
