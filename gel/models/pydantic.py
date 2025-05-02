# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

from __future__ import annotations

import typing
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    ClassVar,
    Generic,
    TypeVar,
    cast,
    final,
    overload,
)

from typing_extensions import (
    Self,
    TypeAliasType,
)

import dataclasses
import functools
import uuid
import warnings

import pydantic
import pydantic.fields
import pydantic_core
from pydantic import Field
from pydantic import PrivateAttr as PrivateAttr
from pydantic import computed_field as computed_field
from pydantic import field_serializer as field_serializer
from pydantic._internal import _model_construction  # noqa: PLC2701
from pydantic_core import core_schema as pydantic_schema

from gel._internal import _typing_parametric as parametric
from gel._internal import _typing_inspect
from gel._internal import _polyfills

from . import lists
from . import unsetid

if TYPE_CHECKING:
    from collections.abc import Sequence


T = TypeVar("T")
T_co = TypeVar("T_co", covariant=True)


@final
class UnspecifiedType:
    """A type used as a sentinel for unspecified values."""


Unspecified = UnspecifiedType()


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
    __slots__ = (*pydantic.fields.FieldInfo.__slots__, "_gel_name")

    def __set_name__(self, owner: Any, name: str) -> None:
        self._gel_name = name


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
    kwargs = dict(field._attributes_set)
    ptr = GelPointer(**kwargs)  # type: ignore [arg-type]
    ptr.__set_name__(None, name)
    return ptr


class GelModelMeta(_model_construction.ModelMetaclass, type):
    def __new__(
        cls,
        name: str,
        bases: tuple[type[Any], ...],
        namespace: dict[str, Any],
        **kwargs: Any,
    ) -> GelModelMeta:
        with warnings.catch_warnings():
            # Make pydantic shut up about attribute redefinition.
            warnings.filterwarnings(
                "ignore",
                message=r".*shadows an attribute in parent.*",
            )
            new_cls = cast(
                "type[pydantic.BaseModel]",
                super().__new__(cls, name, bases, namespace, **kwargs),
            )

        for fname, field in new_cls.__pydantic_fields__.items():
            col = _get_pointer_from_field(fname, field)
            setattr(new_cls, fname, col)

        return new_cls  # type: ignore [return-value]


class GelType:
    pass


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


class BaseScalar(GelPrimitiveType):
    pass


class AnyTuple(GelPrimitiveType):
    pass


class AnyEnum(BaseScalar, _polyfills.StrEnum):
    pass


class Array(list[T], GelPrimitiveType):
    if TYPE_CHECKING:

        def __set__(self, obj: Any, value: Array[T] | Sequence[T]) -> None: ...

    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source_type: Any,
        handler: pydantic.GetCoreSchemaHandler,
    ) -> pydantic_core.CoreSchema:
        if _typing_inspect.is_generic_alias(source_type):
            args = typing.get_args(source_type)
            item_type = args[0]
            return pydantic_schema.list_schema(
                items_schema=handler.generate_schema(item_type),
                serialization=pydantic_schema.plain_serializer_function_ser_schema(
                    list,
                ),
            )
        else:
            return handler.generate_schema(source_type)


class PyTypeScalar(parametric.SingleParametricType[T_co]):
    if TYPE_CHECKING:

        def __set__(self, obj: Any, value: T_co) -> None: ...  # type: ignore [misc]

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


class GelModelMetadata:
    __gel_type_reflection__: ClassVar[ObjectTypeReflection]


class GelModel(
    pydantic.BaseModel, GelModelMetadata, GelType, metaclass=GelModelMeta
):
    model_config = pydantic.ConfigDict(
        json_encoders={uuid.UUID: str},
    )

    def __init__(self, /, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._p__id: uuid.UUID = unsetid.UNSET_UUID
        self._p____type__ = None

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, GelModel):
            return NotImplemented

        if self._p__id is None or other._p__id is None:
            return False
        else:
            return self._p__id == other._p__id

    def __hash__(self) -> int:
        if self._p__id is unsetid.UNSET_UUID:
            raise TypeError("Model instances without id value are unhashable")

        return hash(self._p__id)

    @classmethod
    def filter(cls, /, *args: Any, **kwargs: Any) -> type[Self]:
        return cls


class GelLinkModel(pydantic.BaseModel, metaclass=GelModelMeta):
    pass


MT = TypeVar("MT", bound=GelModel, covariant=True)


class ProxyModel(GelModel, Generic[MT]):
    __proxy_of__: ClassVar[type[MT]]  # type: ignore [misc]

    def __init__(self, obj: MT, /) -> None:
        pass

    @classmethod
    def __pydantic_init_subclass__(cls, **kwargs: Any) -> None:
        super().__pydantic_init_subclass__(**kwargs)
        generic_meta = cls.__pydantic_generic_metadata__
        if generic_meta["origin"] is ProxyModel and generic_meta["args"]:
            cls.__proxy_of__ = generic_meta["args"][0]

    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source_type: Any,
        handler: pydantic.GetCoreSchemaHandler,
    ) -> pydantic_core.CoreSchema:
        if cls.__name__ == "ProxyModel" or cls.__name__.startswith(
            "ProxyModel[",
        ):
            return handler(source_type)
        else:
            return pydantic_schema.no_info_before_validator_function(
                cls,
                schema=handler.generate_schema(cls.__proxy_of__),
            )


#
# Metaclass for type __links__ namespaces.  Facilitates
# proper forward type resolution by raising a NameError
# instead of AttributeError when resolving names in its
# namespace, thus not confusing users of typing._eval_type
#
class LinkClassNamespaceMeta(type):
    def __getattr__(cls, name: str) -> Any:
        if name == "__isabstractmethod__":
            return False

        raise NameError(name)


class LinkClassNamespace(metaclass=LinkClassNamespaceMeta):
    pass


BT_co = TypeVar("BT_co", covariant=True)


class OptionalPointer(GelPointer, Generic[T_co, BT_co]):
    if TYPE_CHECKING:

        @overload
        def __get__(self, obj: None, objtype: type[Any]) -> type[T_co]: ...

        @overload
        def __get__(self, obj: object, objtype: Any = None) -> T_co | None: ...

        def __get__(
            self,
            obj: Any,
            objtype: Any = None,
        ) -> type[T_co] | T_co | None: ...

        def __set__(self, obj: Any, value: BT_co | None) -> None: ...


ST = TypeVar("ST", bound=GelPrimitiveType, covariant=True)


class _OptionalProperty(OptionalPointer[ST, BT_co]):
    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source_type: Any,
        handler: pydantic.GetCoreSchemaHandler,
    ) -> pydantic_core.CoreSchema:
        if _typing_inspect.is_generic_alias(source_type):
            args = typing.get_args(source_type)
            return pydantic_schema.nullable_schema(
                handler.generate_schema(args[0])
            )
        else:
            return handler.generate_schema(source_type)


OptionalProperty = TypeAliasType(
    "OptionalProperty",
    "Annotated[_OptionalProperty[ST, BT_co], Field(default=None)]",
    type_params=(ST, BT_co),
)


BMT = TypeVar("BMT", bound=GelModel, covariant=True)
PT = TypeVar("PT", bound=ProxyModel[GelModel], covariant=True)


class _OptionalLink(OptionalPointer[MT, BMT]):
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


OptionalLink = TypeAliasType(
    "OptionalLink",
    "Annotated[_OptionalLink[MT, MT], Field(default=None)]",
    type_params=(MT,),
)

OptionalLinkWithProps = TypeAliasType(
    "OptionalLinkWithProps",
    "Annotated[_OptionalLink[PT, MT], Field(default=None)]",
    type_params=(PT, MT),
)


class _UpcastingDistinctList(lists.DistinctList[MT], Generic[MT, BMT]):
    @classmethod
    def _check_value(cls, value: Any) -> MT:
        t = cls.type
        if isinstance(value, t):
            return value
        elif issubclass(t, ProxyModel) and isinstance(value, t.__proxy_of__):
            return t(value)  # type: ignore [return-value]

        raise ValueError(
            f"{cls!r} accepts only values of type {cls.type!r}, "
            f"got {type(value)!r}",
        )


class _MultiLinkMeta(type):
    _list_type: type[lists.DistinctList[GelModel | ProxyModel[GelModel]]]


class _MultiLink(GelPointer, Generic[MT, BMT], metaclass=_MultiLinkMeta):
    if TYPE_CHECKING:

        @overload
        def __get__(self, obj: None, objtype: type[Any]) -> type[MT]: ...

        @overload
        def __get__(
            self, obj: object, objtype: Any = None
        ) -> lists.DistinctList[MT]: ...

        def __get__(
            self,
            obj: Any,
            objtype: Any = None,
        ) -> type[MT] | lists.DistinctList[MT] | None: ...

        def __set__(self, obj: Any, value: Sequence[MT | BMT]) -> None: ...

    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source_type: Any,
        handler: pydantic.GetCoreSchemaHandler,
    ) -> pydantic_core.CoreSchema:
        if _typing_inspect.is_generic_alias(source_type):
            args = typing.get_args(source_type)
            item_type = args[0]
            return pydantic_schema.no_info_before_validator_function(
                functools.partial(cls._validate, generic_args=args),
                schema=pydantic_schema.list_schema(
                    items_schema=handler.generate_schema(item_type),
                ),
                serialization=pydantic_schema.plain_serializer_function_ser_schema(
                    list,
                ),
            )
        else:
            return handler.generate_schema(source_type)

    @classmethod
    def _validate(
        cls,
        value: Any,
        generic_args: tuple[type[Any], type[Any]],
    ) -> lists.DistinctList[MT]:
        lt: type[_UpcastingDistinctList[MT, BMT]] = _UpcastingDistinctList[
            generic_args[0],  # type: ignore [valid-type]
            generic_args[1]   # type: ignore [valid-type]
        ]
        if isinstance(value, lt):
            return value
        elif isinstance(value, (list, lists.DistinctList)):
            return lt(value)
        else:
            raise TypeError(
                f"could not convert {type(value)} to {cls.__name__}"
            )


MultiLink = TypeAliasType(
    "MultiLink",
    "Annotated[_MultiLink[MT, MT], Field(default_factory=_MultiLink)]",
    type_params=(MT,),
)

MultiLinkWithProps = TypeAliasType(
    "MultiLinkWithProps",
    "Annotated[_MultiLink[PT, MT], Field(default_factory=_MultiLink)]",
    type_params=(PT, MT),
)
