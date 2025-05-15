# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

"""Pydantic implementation of the query builder model"""

from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    ClassVar,
    Generic,
    TypeVar,
    cast,
    overload,
)

from typing_extensions import (
    Self,
    TypeAliasType,
    TypeVarTuple,
    Unpack,
)

import functools
import typing
import uuid
import warnings

import pydantic
import pydantic.fields
import pydantic_core
from pydantic import ConfigDict as ConfigDict
from pydantic import Field
from pydantic import PrivateAttr as PrivateAttr
from pydantic import computed_field as computed_field
from pydantic import field_serializer as field_serializer
from pydantic_core import core_schema as pydantic_schema

from pydantic._internal import _model_construction  # noqa: PLC2701

from gel._internal import _dlist
from gel._internal import _qb
from gel._internal import _typing_inspect
from gel._internal import _unsetid

from . import _abstract

if TYPE_CHECKING:
    from collections.abc import (
        Sequence,
    )


T = TypeVar("T")
T_co = TypeVar("T_co", covariant=True)


class Array(_abstract.Array[T]):
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


Ts = TypeVarTuple("Ts")


class Tuple(_abstract.Tuple[Unpack[Ts]]):
    __slots__ = ()

    if TYPE_CHECKING:

        def __set__(self, obj: Any, value: Tuple[T] | Sequence[T]) -> None: ...

    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source_type: Any,
        handler: pydantic.GetCoreSchemaHandler,
    ) -> pydantic_core.CoreSchema:
        if _typing_inspect.is_generic_alias(source_type):
            args = typing.get_args(source_type)
            return pydantic_schema.tuple_schema(
                items_schema=[handler.generate_schema(arg) for arg in args],
                serialization=pydantic_schema.plain_serializer_function_ser_schema(
                    tuple,
                ),
            )
        else:
            return handler.generate_schema(source_type)


def _get_range_pydantic_schema(
    source_type: Any,
    handler: pydantic.GetCoreSchemaHandler,
) -> pydantic_schema.ModelFieldsSchema:
    args = typing.get_args(source_type)
    item_schema = handler.generate_schema(args[0])
    opt_item_schema = pydantic_schema.nullable_schema(item_schema)
    item_field_schema = pydantic_schema.model_field(opt_item_schema)
    bool_schema = pydantic_schema.bool_schema()
    bool_field_schema = pydantic_schema.model_field(bool_schema)
    return pydantic_schema.model_fields_schema(
        {
            "lower": item_field_schema,
            "upper": item_field_schema,
            "inc_lower": bool_field_schema,
            "inc_upper": bool_field_schema,
            "empty": bool_field_schema,
        }
    )


class Range(_abstract.Range[T]):
    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source_type: Any,
        handler: pydantic.GetCoreSchemaHandler,
    ) -> pydantic_core.CoreSchema:
        if _typing_inspect.is_generic_alias(source_type):
            return _get_range_pydantic_schema(source_type, handler)
        else:
            return handler.generate_schema(source_type)


class MultiRange(_abstract.MultiRange[T]):
    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source_type: Any,
        handler: pydantic.GetCoreSchemaHandler,
    ) -> pydantic_core.CoreSchema:
        if _typing_inspect.is_generic_alias(source_type):
            range_schema = _get_range_pydantic_schema(source_type, handler)
            return pydantic_schema.list_schema(range_schema)
        else:
            return handler.generate_schema(source_type)


class PyTypeScalar(_abstract.PyTypeScalar[T_co]):
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


class GelModelMeta(_model_construction.ModelMetaclass, _abstract.GelModelMeta):
    def __new__(  # noqa: PYI034
        mcls,
        name: str,
        bases: tuple[type[Any], ...],
        namespace: dict[str, Any],
        *,
        __gel_type_id__: uuid.UUID | None = None,
        **kwargs: Any,
    ) -> GelModelMeta:
        with warnings.catch_warnings():
            # Make pydantic shut up about attribute redefinition.
            warnings.filterwarnings(
                "ignore",
                message=r".*shadows an attribute in parent.*",
            )
            cls = cast(
                "type[GelModel]",
                super().__new__(mcls, name, bases, namespace, **kwargs),
            )

        for fname, field in cls.__pydantic_fields__.items():
            if fname in cls.__annotations__:
                if field.annotation is None:
                    raise AssertionError(
                        f"unexpected unnannotated model field: {name}.{fname}"
                    )
                desc = _qb.field_descriptor(cls, fname, field.annotation)
                setattr(cls, fname, desc)

        if __gel_type_id__ is not None:
            mcls.register_class(__gel_type_id__, cls)

        return cls

    def __setattr__(cls, name: str, value: Any, /) -> None:  # noqa: N805
        if name == "__pydantic_fields__":
            fields: dict[str, pydantic.fields.FieldInfo] = value
            for field in fields.values():
                fdef = field.default
                if isinstance(
                    fdef, (_qb.AbstractDescriptor, _qb.PathAlias)
                ) or (
                    _typing_inspect.is_annotated(fdef)
                    and isinstance(fdef.__origin__, _qb.AbstractDescriptor)
                ):
                    field.default = pydantic_core.PydanticUndefined

        super().__setattr__(name, value)


class GelModel(
    pydantic.BaseModel,
    _abstract.GelModel,
    metaclass=GelModelMeta,
):
    model_config = pydantic.ConfigDict(
        json_encoders={uuid.UUID: str},
        validate_assignment=True,
        defer_build=True,
    )

    _p__id__: uuid.UUID = PrivateAttr()

    def __init__(self, /, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._p__id: uuid.UUID = _unsetid.UNSET_UUID
        self._p____type__ = None

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, GelModel):
            return NotImplemented

        if self._p__id is None or other._p__id is None:
            return False
        else:
            return self._p__id == other._p__id

    def __hash__(self) -> int:
        if self._p__id is _unsetid.UNSET_UUID:
            raise TypeError("Model instances without id value are unhashable")

        return hash(self._p__id)


class LinkPropsDescriptor(Generic[T_co]):
    @overload
    def __get__(self, obj: None, owner: type[Any]) -> type[T_co]: ...

    @overload
    def __get__(self, obj: object, owner: type[Any] | None = None) -> T_co: ...

    def __get__(
        self, obj: Any, owner: type[Any] | None = None
    ) -> T_co | type[T_co]:
        if obj is None:
            assert owner is not None
            return owner.__lprops__  # type: ignore [no-any-return]
        else:
            return obj._p__lprops__  # type: ignore [no-any-return]


class GelLinkModel(pydantic.BaseModel, metaclass=GelModelMeta):
    model_config = pydantic.ConfigDict(
        validate_assignment=True,
        defer_build=True,
    )

    @classmethod
    def __descriptor__(cls) -> LinkPropsDescriptor[Self]:
        return LinkPropsDescriptor()


MT = TypeVar("MT", bound=GelModel, covariant=True)


class ProxyModel(GelModel, Generic[MT]):
    __proxy_of__: ClassVar[type[MT]]  # type: ignore [misc]
    __gel_proxied_dunders__: ClassVar[frozenset[str]] = frozenset(
        {
            "__linkprops__",
        }
    )

    _p__obj__: MT

    def __init__(self, obj: MT, /) -> None:
        object.__setattr__(self, "_p__obj__", obj)

    def __getattribute__(self, name: str) -> Any:
        model_fields = type(self).__proxy_of__.model_fields
        if name in model_fields or name == "_p__id":
            base = object.__getattribute__(self, "_p__obj__")
            return getattr(base, name)
        return super().__getattribute__(name)

    def __setattr__(self, name: str, value: Any) -> None:
        model_fields = type(self).__proxy_of__.model_fields
        if name in model_fields:
            # writing to a field: mutate the  wrapped model
            base = object.__getattribute__(self, "_p__obj__")
            setattr(base, name, value)
        else:
            # writing anything else (including _proxied) is normal
            super().__setattr__(name, value)

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


ST = TypeVar("ST", bound=_abstract.GelPrimitiveType, covariant=True)


class _OptionalProperty(_qb.OptionalPointerDescriptor[ST, BT_co]):
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


class _OptionalLink(_qb.OptionalPointerDescriptor[MT, BMT]):
    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source_type: Any,
        handler: pydantic.GetCoreSchemaHandler,
    ) -> pydantic_core.CoreSchema:
        if _typing_inspect.is_generic_alias(source_type):
            args = typing.get_args(source_type)
            if issubclass(args[0], ProxyModel):
                return pydantic_schema.no_info_before_validator_function(
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
    ) -> MT:
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
    "Annotated[_OptionalLink[MT, MT], Field(default=None)]",
    type_params=(MT,),
)

OptionalLinkWithProps = TypeAliasType(
    "OptionalLinkWithProps",
    "Annotated[_OptionalLink[PT, MT], Field(default=None)]",
    type_params=(PT, MT),
)


class _UpcastingDistinctList(_dlist.DistinctList[MT], Generic[MT, BMT]):
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
    _list_type: type[_dlist.DistinctList[GelModel | ProxyModel[GelModel]]]


class _MultiLink(_qb.PointerDescriptor[MT, BMT], metaclass=_MultiLinkMeta):
    if TYPE_CHECKING:

        @overload
        def __get__(self, obj: None, objtype: type[Any]) -> type[MT]: ...

        @overload
        def __get__(
            self, obj: object, objtype: Any = None
        ) -> _dlist.DistinctList[MT]: ...

        def __get__(
            self,
            obj: Any,
            objtype: Any = None,
        ) -> type[MT] | _dlist.DistinctList[MT] | None: ...

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
    ) -> _dlist.DistinctList[MT]:
        lt: type[_UpcastingDistinctList[MT, BMT]] = _UpcastingDistinctList[
            generic_args[0],  # type: ignore [valid-type]
            generic_args[1],  # type: ignore [valid-type]
        ]
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
        _MultiLink[MT, MT],
        Field(default_factory=_dlist.DistinctList[GelModel]),
    ],
    type_params=(MT,),
)

MultiLinkWithProps = TypeAliasType(
    "MultiLinkWithProps",
    Annotated[
        _MultiLink[PT, MT],
        Field(default_factory=_dlist.DistinctList[ProxyModel[GelModel]]),
    ],
    type_params=(PT, MT),
)
