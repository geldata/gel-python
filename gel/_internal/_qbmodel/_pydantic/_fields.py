# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

"""Pydantic implementation of the query builder model"""

from __future__ import annotations

from typing import (
    Annotated,
    Any,
    ClassVar,
    Generic,
    TypeVar,
)

from typing_extensions import (
    TypeAliasType,
)

import functools
import typing

import pydantic
import pydantic_core
from pydantic_core import core_schema


from gel._internal import _tracked_list
from gel._internal import _edgeql
from gel._internal import _typing_inspect

from gel._internal._qbmodel import _abstract
from gel._internal._qbmodel._abstract import DistinctList, ProxyDistinctList

from ._models import GelModel, ProxyModel

from . import _utils as _pydantic_utils


_T_co = TypeVar("_T_co", bound=_abstract.GelType, covariant=True)

_BT_co = TypeVar("_BT_co", covariant=True)
"""Base type"""

_ST_co = TypeVar("_ST_co", bound=_abstract.GelPrimitiveType, covariant=True)
"""Primitive Gel type"""

_MT_co = TypeVar("_MT_co", bound=GelModel, covariant=True)
"""Derived model type"""

_BMT_co = TypeVar("_BMT_co", bound=GelModel, covariant=True)
"""Base model type (which _MT_co is directly derived from)"""

_PT_co = TypeVar("_PT_co", bound=ProxyModel[GelModel], covariant=True)
"""Proxy model"""


class _MultiPointer(_abstract.PointerDescriptor[_T_co, _BT_co]):
    constructor: ClassVar[type] = list

    @classmethod
    def __gel_resolve_dlist__(
        cls,
        type_args: tuple[type[Any]] | tuple[type[Any], type[Any]],
    ) -> type:
        raise NotImplementedError

    @classmethod
    def _validate(
        cls,
        value: Any,
        generic_args: tuple[type[Any], type[Any]],
    ) -> Any:
        raise NotImplementedError


class _BaseMultiProperty(_MultiPointer[_T_co, _BT_co]):
    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source_type: Any,
        handler: pydantic.GetCoreSchemaHandler,
    ) -> pydantic_core.CoreSchema:
        if _typing_inspect.is_generic_alias(source_type):
            args = typing.get_args(source_type)
            return core_schema.no_info_plain_validator_function(
                functools.partial(cls._validate, generic_args=args),
                serialization=core_schema.plain_serializer_function_ser_schema(
                    list,
                ),
            )
        else:
            return handler.generate_schema(source_type)


class _BaseMultiLink(_MultiPointer[_T_co, _BT_co]):
    constructor: ClassVar[type] = list

    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source_type: Any,
        handler: pydantic.GetCoreSchemaHandler,
    ) -> pydantic_core.CoreSchema:
        if _typing_inspect.is_generic_alias(source_type):
            args = typing.get_args(source_type)

            return core_schema.json_or_python_schema(
                json_schema=core_schema.list_schema(
                    handler.generate_schema(args[0])
                ),
                python_schema=core_schema.no_info_plain_validator_function(
                    functools.partial(cls._validate, generic_args=args),
                ),
                serialization=core_schema.wrap_serializer_function_ser_schema(
                    lambda els, _ser, info: cls.constructor(
                        obj.model_dump(
                            **_pydantic_utils.serialization_info_to_dump_kwargs(
                                info
                            )
                        )
                        for obj in els
                    ),
                    info_arg=True,
                    when_used="always",
                ),
            )
        else:
            return handler.generate_schema(source_type)


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


class _ComputedProperty(_abstract.ComputedPropertyDescriptor[_ST_co, _BT_co]):
    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source_type: Any,
        handler: pydantic.GetCoreSchemaHandler,
    ) -> pydantic_core.CoreSchema:
        # This is the only workaround I could find to make pydantic
        # not require our *required computed fields* to be passed
        # to __init__. See also the related workaround code in
        # GelModel.__init__.
        return core_schema.with_default_schema(
            core_schema.any_schema(),
            default=None,
        )


ComputedProperty = TypeAliasType(
    "ComputedProperty",
    Annotated[
        _ComputedProperty[_ST_co, _BT_co],
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
        pydantic.Field(init=False, frozen=True),
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
            computed=False,
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


class _MultiProperty(
    _BaseMultiProperty[_ST_co, _BT_co],
    _abstract.MultiPropertyDescriptor[_ST_co, _BT_co],
):
    @classmethod
    def __gel_resolve_dlist__(  # type: ignore [override]
        cls,
        type_args: tuple[type[Any]] | tuple[type[Any], type[Any]],
    ) -> _tracked_list.DowncastingTrackedList[_ST_co, _BT_co]:
        return _tracked_list.DowncastingTrackedList[type_args[0], type_args[1]]  # type: ignore [return-value, valid-type]

    @classmethod
    def _validate(
        cls,
        value: Any,
        generic_args: tuple[type[Any], type[Any]],
    ) -> _tracked_list.DowncastingTrackedList[_ST_co, _BT_co]:
        lt: type[_tracked_list.DowncastingTrackedList[_ST_co, _BT_co]] = (
            _tracked_list.DowncastingTrackedList[
                generic_args[0],  # type: ignore [valid-type]
                generic_args[1],  # type: ignore [valid-type]
            ]
        )
        return DistinctList.__gel_validate__(lt, value)


class _ComputedMultiProperty(
    _abstract.ComputedMultiPropertyDescriptor[_T_co, _BT_co],
    _BaseMultiProperty[_T_co, _BT_co],
):
    constructor = tuple

    @classmethod
    def __gel_resolve_dlist__(  # type: ignore [override]
        cls,
        type_args: tuple[type[Any]] | tuple[type[Any], type[Any]],
    ) -> tuple[_BT_co, ...]:
        return tuple[type_args[0], ...]  # type: ignore [return-value, valid-type]


MultiProperty = TypeAliasType(
    "MultiProperty",
    Annotated[
        _MultiProperty[_ST_co, _BT_co],
        pydantic.Field(
            default_factory=_tracked_list.DefaultList,
            # Force validate call to convert the empty list
            # to a properly typed one.
            validate_default=True,
        ),
        _abstract.PointerInfo(
            cardinality=_edgeql.Cardinality.Many,
            kind=_edgeql.PointerKind.Property,
        ),
    ],
    type_params=(_ST_co, _BT_co),
)


ComputedMultiProperty = TypeAliasType(
    "ComputedMultiProperty",
    Annotated[
        _ComputedMultiProperty[_ST_co, _BT_co],
        pydantic.Field(
            default_factory=tuple,
            init=False,
            frozen=True,
        ),
        _abstract.PointerInfo(
            computed=True,
            readonly=True,
            cardinality=_edgeql.Cardinality.Many,
            kind=_edgeql.PointerKind.Property,
        ),
    ],
    type_params=(_ST_co, _BT_co),
)


class _AnyLink(Generic[_MT_co, _BMT_co]):
    @staticmethod
    def _build_serialize() -> core_schema.SerSchema:
        return core_schema.wrap_serializer_function_ser_schema(
            lambda obj, _ser, info: obj.model_dump(
                **_pydantic_utils.serialization_info_to_dump_kwargs(info)
            )
            if obj is not None
            else None,
            # XXX: maybe this should be more robust
            # and RequiredLink should explicitly error out on None?
            info_arg=True,
            when_used="always",
        )

    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source_type: Any,
        handler: pydantic.GetCoreSchemaHandler,
    ) -> pydantic_core.CoreSchema:
        if _typing_inspect.is_generic_alias(source_type):
            args = typing.get_args(source_type)
            inner_schema = handler.generate_schema(args[0])
            return core_schema.json_or_python_schema(
                json_schema=inner_schema,
                python_schema=core_schema.no_info_plain_validator_function(
                    functools.partial(cls._validate, generic_args=args),
                ),
                serialization=cls._build_serialize(),
            )
        else:
            return handler.generate_schema(source_type)

    @classmethod
    def _validate(
        cls,
        value: Any,
        generic_args: tuple[type[Any], type[Any]],
    ) -> _MT_co | None:
        mt, bmt = generic_args

        if value is None:
            raise ValueError("cannot set a required link to None")

        if mt is bmt:
            # link or optional link *without* props

            if isinstance(value, ProxyModel):
                # A proxied model -- let's just unwrap it.
                # Scenario: a user had code line `o1.link = o2.link`
                # which worked, and so it should continue working
                # if the user added a property to `o2.link`.
                value = value._p__obj__

            if isinstance(value, mt):
                return value  # type: ignore [no-any-return]
        else:
            # link or optional link *with* props
            if isinstance(value, mt):
                # Same proxy type -- we can't do anything but to return
                # the value as is; otherwise `obj.link = LinkWithProps.link()`
                # wouldn't work.
                return value  # type: ignore [no-any-return]
            elif isinstance(value, (bmt, ProxyModel)):
                # Naked target type or another proxy model are not accepted
                raise ValueError(
                    f"cannot assign a value of type {type(value).__name__} "
                    f"to a field of type {mt.__name__}"
                )

        # defer to Pydantic
        return mt.model_validate(value)  # type: ignore [no-any-return]


class _Link(
    _AnyLink[_MT_co, _BMT_co],
    _abstract.LinkDescriptor[_MT_co, _BMT_co],
):
    pass


class _OptionalLink(
    _AnyLink[_MT_co, _BMT_co],
    _abstract.OptionalLinkDescriptor[_MT_co, _BMT_co],
):
    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source_type: Any,
        handler: pydantic.GetCoreSchemaHandler,
    ) -> pydantic_core.CoreSchema:
        # Optional link should allow None as a valid value.
        # Wrap the AnyLink's schema in a nullable schema to allow that.
        schema = super().__get_pydantic_core_schema__(source_type, handler)
        schema = core_schema.nullable_schema(schema)
        return schema

    @classmethod
    def _validate(
        cls,
        value: Any,
        generic_args: tuple[type[Any], type[Any]],
    ) -> _MT_co | None:
        if value is None:
            return None
        return super()._validate(value, generic_args)


RequiredLinkWithProps = TypeAliasType(
    "RequiredLinkWithProps",
    Annotated[
        _Link[_MT_co, _BMT_co],
        _abstract.PointerInfo(
            cardinality=_edgeql.Cardinality.One,
            kind=_edgeql.PointerKind.Link,
            has_props=True,
        ),
    ],
    type_params=(_MT_co, _BMT_co),
)


ComputedLinkWithProps = TypeAliasType(
    "ComputedLinkWithProps",
    Annotated[
        _Link[_MT_co, _BMT_co],
        pydantic.Field(init=False, frozen=True),
        _abstract.PointerInfo(
            computed=True,
            readonly=True,
            has_props=True,
            cardinality=_edgeql.Cardinality.One,
            kind=_edgeql.PointerKind.Link,
        ),
    ],
    type_params=(_MT_co, _BMT_co),
)

ComputedLink = TypeAliasType(
    "ComputedLink",
    Annotated[
        _Link[_MT_co, _MT_co],
        pydantic.Field(init=False, frozen=True),
        _abstract.PointerInfo(
            computed=True,
            readonly=True,
            cardinality=_edgeql.Cardinality.One,
            kind=_edgeql.PointerKind.Link,
        ),
    ],
    type_params=(_MT_co,),
)


RequiredLink = TypeAliasType(
    "RequiredLink",
    Annotated[
        _Link[_MT_co, _MT_co],
        pydantic.Field(default=None),
        _abstract.PointerInfo(
            cardinality=_edgeql.Cardinality.AtMostOne,
            kind=_edgeql.PointerKind.Link,
        ),
    ],
    type_params=(_MT_co,),
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


class _ComputedMultiLink(
    _abstract.ComputedMultiLinkDescriptor[_MT_co, _BMT_co],
    _BaseMultiLink[_MT_co, _BMT_co],
):
    constructor = tuple

    @classmethod
    def __gel_resolve_dlist__(  # type: ignore [override]
        cls,
        type_args: tuple[type[Any]] | tuple[type[Any], type[Any]],
    ) -> tuple[_BMT_co, ...]:
        return tuple[type_args[0], ...]  # type: ignore [return-value, valid-type]


class _MultiLink(
    _BaseMultiLink[_MT_co, _BMT_co],
    _abstract.MultiLinkDescriptor[_MT_co, _BMT_co],
):
    @classmethod
    def __gel_resolve_dlist__(  # type: ignore [override]
        cls,
        type_args: tuple[type[Any]] | tuple[type[Any], type[Any]],
    ) -> DistinctList[_MT_co]:
        return DistinctList[type_args[0]]  # type: ignore [return-value, valid-type]

    @classmethod
    def _validate(
        cls,
        value: Any,
        generic_args: tuple[type[Any], type[Any]],
    ) -> DistinctList[_MT_co]:
        lt: type[DistinctList[_MT_co]] = DistinctList[
            generic_args[0],  # type: ignore [valid-type]
        ]
        return DistinctList.__gel_validate__(lt, value)


class _MultiLinkWithProps(
    _BaseMultiLink[_PT_co, _BMT_co],
    _abstract.MultiLinkWithPropsDescriptor[_PT_co, _BMT_co],
):
    @classmethod
    def __gel_resolve_dlist__(  # type: ignore [override]
        cls,
        type_args: tuple[type[Any]] | tuple[type[Any], type[Any]],
    ) -> DistinctList[_PT_co]:
        return ProxyDistinctList[
            type_args[0],  # type: ignore [valid-type]
            type_args[1],  # type: ignore [valid-type]
        ]  # type: ignore [return-value]

    @classmethod
    def _validate(
        cls,
        value: Any,
        generic_args: tuple[type[Any], type[Any]],
    ) -> ProxyDistinctList[_PT_co, _BMT_co]:
        lt: type[ProxyDistinctList[_PT_co, _BMT_co]] = ProxyDistinctList[
            generic_args[0],  # type: ignore [valid-type]
            generic_args[1],  # type: ignore [valid-type]
        ]
        return DistinctList.__gel_validate__(lt, value)


OptionalMultiLink = TypeAliasType(
    "OptionalMultiLink",
    Annotated[
        _MultiLink[_MT_co, _MT_co],
        pydantic.Field(
            default_factory=_tracked_list.DefaultList,
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

RequiredMultiLink = TypeAliasType(
    "RequiredMultiLink",
    Annotated[
        _MultiLink[_MT_co, _MT_co],
        pydantic.Field(
            default_factory=_tracked_list.DefaultList,
            # Force validate call to convert the empty list
            # to a properly typed one.
            validate_default=True,
        ),
        _abstract.PointerInfo(
            cardinality=_edgeql.Cardinality.AtLeastOne,
            kind=_edgeql.PointerKind.Link,
        ),
    ],
    type_params=(_MT_co,),
)

ComputedMultiLink = TypeAliasType(
    "ComputedMultiLink",
    Annotated[
        _ComputedMultiLink[_MT_co, _MT_co],
        pydantic.Field(
            default_factory=tuple,
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

OptionalMultiLinkWithProps = TypeAliasType(
    "OptionalMultiLinkWithProps",
    Annotated[
        _MultiLinkWithProps[_PT_co, _MT_co],
        pydantic.Field(
            default_factory=_tracked_list.DefaultList,
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

RequiredMultiLinkWithProps = TypeAliasType(
    "RequiredMultiLinkWithProps",
    Annotated[
        _MultiLinkWithProps[_PT_co, _MT_co],
        pydantic.Field(
            default_factory=_tracked_list.DefaultList,
            # Force validate call to convert the empty list
            # to a properly typed one.
            validate_default=True,
        ),
        _abstract.PointerInfo(
            has_props=True,
            cardinality=_edgeql.Cardinality.AtLeastOne,
            kind=_edgeql.PointerKind.Link,
        ),
    ],
    type_params=(_PT_co, _MT_co),
)

ComputedMultiLinkWithProps = TypeAliasType(
    "ComputedMultiLinkWithProps",
    Annotated[
        _ComputedMultiLink[_PT_co, _MT_co],
        pydantic.Field(
            default_factory=tuple,
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
