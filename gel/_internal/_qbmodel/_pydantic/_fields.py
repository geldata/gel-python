# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

"""Pydantic implementation of the query builder model"""

from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    Generic,
    SupportsIndex,
    TypeVar,
    overload,
)

from typing_extensions import (
    TypeAliasType,
    Self,
)

import functools
import typing

import pydantic
import pydantic.fields
import pydantic_core
from pydantic_core import core_schema


from gel._internal import _dlist
from gel._internal import _edgeql
from gel._internal import _typing_inspect
from gel._internal import _unsetid

from gel._internal._qbmodel import _abstract

from ._models import GelModel, ProxyModel

if TYPE_CHECKING:
    from collections.abc import Sequence, Iterable


_BT_co = TypeVar("_BT_co", covariant=True)
"""Base Python type"""

_ST_co = TypeVar("_ST_co", bound=_abstract.GelPrimitiveType, covariant=True)
"""Primitive Gel type"""

_MT_co = TypeVar("_MT_co", bound=GelModel, covariant=True)
"""Derived model type"""

_BMT_co = TypeVar("_BMT_co", bound=GelModel, covariant=True)
"""Base model type (which _MT_co is directly derived from)"""

_PT_co = TypeVar("_PT_co", bound=ProxyModel[GelModel], covariant=True)
"""Proxy model"""


class Property(_abstract.PropertyDescriptor[_ST_co, _BT_co]):
    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source_type: Any,
        handler: pydantic.GetCoreSchemaHandler,
    ) -> pydantic_core.CoreSchema:
        if _typing_inspect.is_generic_alias(source_type):
            args = typing.get_args(source_type)
            return handler.generate_schema(args[0])
        else:
            return handler.generate_schema(source_type)


ComputedProperty = TypeAliasType(
    "ComputedProperty",
    Annotated[
        Property[_ST_co, _BT_co],
        pydantic.Field(init=False, frozen=True),
        _abstract.PointerInfo(
            computed=True,
            readonly=True,
            kind=_edgeql.PointerKind.Property,
        ),
    ],
    type_params=(_ST_co, _BT_co),
)


IdProperty = TypeAliasType(
    "IdProperty",
    Annotated[
        Property[_ST_co, _BT_co],
        pydantic.Field(default=_unsetid.UNSET_UUID, init=False, frozen=True),
        _abstract.PointerInfo(
            computed=True,
            readonly=True,
            kind=_edgeql.PointerKind.Property,
        ),
    ],
    type_params=(_ST_co, _BT_co),
)


class _OptionalProperty(_abstract.OptionalPropertyDescriptor[_ST_co, _BT_co]):
    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source_type: Any,
        handler: pydantic.GetCoreSchemaHandler,
    ) -> pydantic_core.CoreSchema:
        if _typing_inspect.is_generic_alias(source_type):
            args = typing.get_args(source_type)
            return core_schema.nullable_schema(
                handler.generate_schema(args[0])
            )
        else:
            return handler.generate_schema(source_type)


OptionalProperty = TypeAliasType(
    "OptionalProperty",
    Annotated[
        _OptionalProperty[_ST_co, _BT_co],
        pydantic.Field(default=None),
        _abstract.PointerInfo(
            computed=True,
            cardinality=_edgeql.Cardinality.AtMostOne,
            kind=_edgeql.PointerKind.Property,
        ),
    ],
    type_params=(_ST_co, _BT_co),
)


OptionalComputedProperty = TypeAliasType(
    "OptionalComputedProperty",
    Annotated[
        _OptionalProperty[_ST_co, _BT_co],
        pydantic.Field(default=None, init=False, frozen=True),
        _abstract.PointerInfo(
            computed=True,
            readonly=True,
            cardinality=_edgeql.Cardinality.AtMostOne,
            kind=_edgeql.PointerKind.Property,
        ),
    ],
    type_params=(_ST_co, _BT_co),
)


class _OptionalLink(_abstract.OptionalLinkDescriptor[_MT_co, _BMT_co]):
    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source_type: Any,
        handler: pydantic.GetCoreSchemaHandler,
    ) -> pydantic_core.CoreSchema:
        if _typing_inspect.is_generic_alias(source_type):
            args = typing.get_args(source_type)
            if issubclass(args[0], ProxyModel):
                return core_schema.no_info_before_validator_function(
                    functools.partial(cls._validate, generic_args=args),
                    schema=handler.generate_schema(args[0]),
                )
            else:
                return handler.generate_schema(args[0])
        else:
            return handler.generate_schema(source_type)

    @classmethod
    def _validate(
        cls,
        value: Any,
        generic_args: tuple[type[Any], type[Any]],
    ) -> _MT_co:
        mt, bmt = generic_args
        if isinstance(value, mt):
            return value  # type: ignore [no-any-return]
        elif isinstance(value, bmt):
            return mt(value)  # type: ignore [no-any-return]
        else:
            raise TypeError(
                f"could not convert {type(value)} to {mt.__name__}"
            )


OptionalLink = TypeAliasType(
    "OptionalLink",
    Annotated[
        _OptionalLink[_MT_co, _MT_co],
        pydantic.Field(default=None),
        _abstract.PointerInfo(
            cardinality=_edgeql.Cardinality.AtMostOne,
            kind=_edgeql.PointerKind.Link,
        ),
    ],
    type_params=(_MT_co,),
)

OptionalComputedLink = TypeAliasType(
    "OptionalComputedLink",
    Annotated[
        _OptionalLink[_MT_co, _MT_co],
        pydantic.Field(default=None, init=False, frozen=True),
        _abstract.PointerInfo(
            computed=True,
            readonly=True,
            cardinality=_edgeql.Cardinality.AtMostOne,
            kind=_edgeql.PointerKind.Link,
        ),
    ],
    type_params=(_MT_co,),
)

OptionalLinkWithProps = TypeAliasType(
    "OptionalLinkWithProps",
    Annotated[
        _OptionalLink[_PT_co, _MT_co],
        pydantic.Field(default=None),
        _abstract.PointerInfo(
            cardinality=_edgeql.Cardinality.AtMostOne,
            kind=_edgeql.PointerKind.Link,
            has_props=True,
        ),
    ],
    type_params=(_PT_co, _MT_co),
)

OptionalComputedLinkWithProps = TypeAliasType(
    "OptionalComputedLinkWithProps",
    Annotated[
        _OptionalLink[_PT_co, _MT_co],
        pydantic.Field(default=None, init=False, frozen=True),
        _abstract.PointerInfo(
            computed=True,
            readonly=True,
            has_props=True,
            cardinality=_edgeql.Cardinality.AtMostOne,
            kind=_edgeql.PointerKind.Link,
        ),
    ],
    type_params=(_PT_co, _MT_co),
)


class _UpcastingDistinctList(
    _dlist.DistinctList[_MT_co], Generic[_MT_co, _BMT_co]
):
    @classmethod
    def _check_value(cls, value: Any) -> _MT_co:
        t = cls.type
        if isinstance(value, t):
            return value
        elif issubclass(t, ProxyModel) and isinstance(value, t.__proxy_of__):
            return t(value)  # type: ignore [return-value]

        if issubclass(t, ProxyModel):
            raise ValueError(
                f"{cls!r} accepts only values of type {t.__name__} "
                f"or {t.__proxy_of__.__name__}, got {type(value)!r}",
            )
        else:
            raise ValueError(
                f"{cls!r} accepts only values of type {t.__name__}, "
                f"got {type(value)!r}",
            )

    if TYPE_CHECKING:

        def append(self, value: _MT_co | _BMT_co) -> None: ...
        def insert(
            self, index: SupportsIndex, value: _MT_co | _BMT_co
        ) -> None: ...
        def __setitem__(
            self,
            index: SupportsIndex | slice,
            value: _MT_co | _BMT_co | Iterable[_MT_co | _BMT_co],
        ) -> None: ...
        def extend(self, values: Iterable[_MT_co | _BMT_co]) -> None: ...
        def remove(self, value: _MT_co | _BMT_co) -> None: ...
        def index(
            self,
            value: _MT_co | _BMT_co,
            start: SupportsIndex = 0,
            stop: SupportsIndex | None = None,
        ) -> int: ...
        def count(self, value: _MT_co | _BMT_co) -> int: ...
        def __add__(self, other: Iterable[_MT_co | _BMT_co]) -> Self: ...
        def __iadd__(self, other: Iterable[_MT_co | _BMT_co]) -> Self: ...


class _MultiLinkMeta(type):
    pass


class _MultiLinkBase(
    _abstract.LinkDescriptor[_MT_co, _BMT_co], metaclass=_MultiLinkMeta
):
    @classmethod
    def _validate(
        cls,
        value: Any,
        generic_args: tuple[type[Any], type[Any]],
    ) -> Any:
        raise NotImplementedError

    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source_type: Any,
        handler: pydantic.GetCoreSchemaHandler,
    ) -> pydantic_core.CoreSchema:
        if _typing_inspect.is_generic_alias(source_type):
            args = typing.get_args(source_type)
            item_type = args[0]
            return core_schema.no_info_after_validator_function(
                functools.partial(cls._validate, generic_args=args),
                schema=core_schema.list_schema(
                    items_schema=handler.generate_schema(item_type),
                ),
                serialization=core_schema.plain_serializer_function_ser_schema(
                    list,
                ),
            )
        else:
            return handler.generate_schema(source_type)


class _MultiLink(_MultiLinkBase[_MT_co, _BMT_co]):
    if TYPE_CHECKING:

        @overload
        def __get__(self, obj: None, objtype: type[Any]) -> type[_MT_co]: ...

        @overload
        def __get__(
            self, obj: object, objtype: Any = None
        ) -> _dlist.DistinctList[_MT_co]: ...

        def __get__(
            self,
            obj: Any,
            objtype: Any = None,
        ) -> type[_MT_co] | _dlist.DistinctList[_MT_co] | None: ...

        def __set__(
            self, obj: Any, value: Sequence[_MT_co | _BMT_co]
        ) -> None: ...

    @classmethod
    def __gel_resolve_dlist__(
        cls,
        type_args: tuple[type[Any]] | tuple[type[Any], type[Any]],
    ) -> _dlist.DistinctList[_MT_co]:
        return _dlist.DistinctList[type_args[0]]  # type: ignore [return-value, valid-type]

    @classmethod
    def _validate(
        cls,
        value: Any,
        generic_args: tuple[type[Any], type[Any]],
    ) -> _dlist.DistinctList[_MT_co]:
        lt: type[_dlist.DistinctList[_MT_co]] = _dlist.DistinctList[
            generic_args[0],  # type: ignore [valid-type]
        ]
        if isinstance(value, lt):
            return value
        elif isinstance(value, (list, _dlist.DistinctList)):
            return lt(value)
        else:
            raise TypeError(
                f"could not convert {type(value)} to {cls.__name__}"
            )


class _MultiLinkWithProps(_MultiLinkBase[_MT_co, _BMT_co]):
    if TYPE_CHECKING:

        @overload
        def __get__(self, obj: None, objtype: type[Any]) -> type[_MT_co]: ...

        @overload
        def __get__(
            self, obj: object, objtype: Any = None
        ) -> _UpcastingDistinctList[_MT_co, _BMT_co]: ...

        def __get__(
            self,
            obj: Any,
            objtype: Any = None,
        ) -> type[_MT_co] | _UpcastingDistinctList[_MT_co, _BMT_co] | None: ...

        def __set__(
            self, obj: Any, value: Sequence[_MT_co | _BMT_co]
        ) -> None: ...

    @classmethod
    def __gel_resolve_dlist__(
        cls,
        type_args: tuple[type[Any]] | tuple[type[Any], type[Any]],
    ) -> _dlist.DistinctList[_MT_co]:
        return _UpcastingDistinctList[type_args[0], type_args[1]]  # type: ignore [return-value, valid-type]

    @classmethod
    def _validate(
        cls,
        value: Any,
        generic_args: tuple[type[Any], type[Any]],
    ) -> _UpcastingDistinctList[_MT_co, _BMT_co]:
        lt: type[_UpcastingDistinctList[_MT_co, _BMT_co]] = (
            _UpcastingDistinctList[
                generic_args[0],  # type: ignore [valid-type]
                generic_args[1],  # type: ignore [valid-type]
            ]
        )
        if isinstance(value, lt):
            return value
        elif isinstance(value, (list, _dlist.DistinctList)):
            return lt(value)
        else:
            raise TypeError(
                f"could not convert {type(value)} to {cls.__name__}"
            )


MultiLink = TypeAliasType(
    "MultiLink",
    Annotated[
        _MultiLink[_MT_co, _MT_co],
        pydantic.Field(
            default_factory=list,
            # Force validate call to convert the empty list
            # to a properly typed one.
            validate_default=True,
        ),
        _abstract.PointerInfo(
            cardinality=_edgeql.Cardinality.Many,
            kind=_edgeql.PointerKind.Link,
        ),
    ],
    type_params=(_MT_co,),
)

ComputedMultiLink = TypeAliasType(
    "ComputedMultiLink",
    Annotated[
        _MultiLink[_MT_co, _MT_co],
        pydantic.Field(
            default_factory=list,
            # Force validate call to convert the empty list
            # to a properly typed one.
            validate_default=True,
            init=False,
            frozen=True,
        ),
        _abstract.PointerInfo(
            computed=True,
            readonly=True,
            cardinality=_edgeql.Cardinality.Many,
            kind=_edgeql.PointerKind.Link,
        ),
    ],
    type_params=(_MT_co,),
)

MultiLinkWithProps = TypeAliasType(
    "MultiLinkWithProps",
    Annotated[
        _MultiLinkWithProps[_PT_co, _MT_co],
        pydantic.Field(
            default_factory=list,
            # Force validate call to convert the empty list
            # to a properly typed one.
            validate_default=True,
        ),
        _abstract.PointerInfo(
            has_props=True,
            cardinality=_edgeql.Cardinality.Many,
            kind=_edgeql.PointerKind.Link,
        ),
    ],
    type_params=(_PT_co, _MT_co),
)

ComputedMultiLinkWithProps = TypeAliasType(
    "ComputedMultiLinkWithProps",
    Annotated[
        _MultiLinkWithProps[_PT_co, _MT_co],
        pydantic.Field(
            default_factory=list,
            # Force validate call to convert the empty list
            # to a properly typed one.
            validate_default=True,
            init=False,
            frozen=True,
        ),
        _abstract.PointerInfo(
            computed=True,
            readonly=True,
            has_props=True,
            cardinality=_edgeql.Cardinality.Many,
            kind=_edgeql.PointerKind.Link,
        ),
    ],
    type_params=(_PT_co, _MT_co),
)
