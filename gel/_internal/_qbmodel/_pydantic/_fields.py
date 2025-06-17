# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

"""Pydantic implementation of the query builder model"""

from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    cast,
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

from gel._internal._qbmodel import _abstract

from ._models import GelModel, ProxyModel

if TYPE_CHECKING:
    from typing_extensions import Never
    from collections.abc import Sequence, Iterable


_T_co = TypeVar("_T_co", covariant=True)

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
            return core_schema.no_info_plain_validator_function(
                functools.partial(cls._validate, generic_args=args),
                serialization=core_schema.plain_serializer_function_ser_schema(
                    list,
                ),
            )
        else:
            return handler.generate_schema(source_type)


class _ComputedMultiPointer(_abstract.PointerDescriptor[_T_co, _BT_co]):
    if TYPE_CHECKING:

        @overload
        def __get__(self, obj: None, objtype: type[Any]) -> type[_T_co]: ...

        @overload
        def __get__(
            self, obj: object, objtype: Any = None
        ) -> tuple[_T_co, ...]: ...

        def __get__(
            self,
            obj: Any,
            objtype: Any = None,
        ) -> type[_T_co] | tuple[_T_co, ...]: ...

    @classmethod
    def __gel_resolve_dlist__(
        cls,
        type_args: tuple[type[Any]] | tuple[type[Any], type[Any]],
    ) -> tuple[_BT_co, ...]:
        return tuple[type_args[0], ...]  # type: ignore [return-value, valid-type]

    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source_type: Any,
        handler: pydantic.GetCoreSchemaHandler,
    ) -> pydantic_core.CoreSchema:
        if _typing_inspect.is_generic_alias(source_type):
            args = typing.get_args(source_type)
            item_type = args[0]
            return core_schema.tuple_schema(
                items_schema=[handler.generate_schema(item_type)],
                variadic_item_index=0,
            )
        else:
            return handler.generate_schema(source_type)


class Property(_abstract.PropertyDescriptor[_ST_co, _BT_co]):
    if TYPE_CHECKING:

        @overload
        def __get__(self, obj: None, objtype: type[Any]) -> type[_ST_co]: ...

        @overload
        def __get__(self, obj: object, objtype: Any = None) -> _ST_co: ...

        def __get__(
            self,
            obj: Any,
            objtype: Any = None,
        ) -> type[_ST_co] | _ST_co: ...

        def __set__(self, obj: Any, value: _ST_co | _BT_co) -> None: ...

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


class _ComputedProperty(_abstract.PropertyDescriptor[_ST_co, _BT_co]):
    if TYPE_CHECKING:

        @overload
        def __get__(self, obj: None, objtype: type[Any]) -> type[_ST_co]: ...

        @overload
        def __get__(self, obj: object, objtype: Any = None) -> _ST_co: ...

        def __get__(
            self,
            obj: Any,
            objtype: Any = None,
        ) -> type[_ST_co] | _ST_co: ...

        # XXX -- using Final[] would probably be better, but it's not clear
        # how to wrap it in our aliases.
        def __set__(self, obj: Any, value: Never) -> None: ...

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
    _MultiPointer[_ST_co, _BT_co],
    _abstract.PropertyDescriptor[_ST_co, _BT_co],
):
    if TYPE_CHECKING:

        @overload
        def __get__(self, obj: None, objtype: type[Any]) -> type[_ST_co]: ...

        @overload
        def __get__(
            self, obj: object, objtype: Any = None
        ) -> _dlist.DowncastingTrackedList[_ST_co, _BT_co]: ...

        def __get__(
            self,
            obj: Any,
            objtype: Any = None,
        ) -> (
            type[_ST_co] | _dlist.DowncastingTrackedList[_ST_co, _BT_co] | None
        ): ...

        def __set__(
            self, obj: Any, value: Sequence[_ST_co | _BT_co]
        ) -> None: ...

    @classmethod
    def __gel_resolve_dlist__(
        cls,
        type_args: tuple[type[Any]] | tuple[type[Any], type[Any]],
    ) -> _dlist.DowncastingTrackedList[_ST_co, _BT_co]:
        return _dlist.DowncastingTrackedList[type_args[0], type_args[1]]  # type: ignore [return-value, valid-type]

    @classmethod
    def _validate(
        cls,
        value: Any,
        generic_args: tuple[type[Any], type[Any]],
    ) -> _dlist.DowncastingTrackedList[_ST_co, _BT_co]:
        lt: type[_dlist.DowncastingTrackedList[_ST_co, _BT_co]] = (
            _dlist.DowncastingTrackedList[
                generic_args[0],  # type: ignore [valid-type]
                generic_args[1],  # type: ignore [valid-type]
            ]
        )
        if isinstance(value, lt):
            return value
        elif isinstance(value, (list, _dlist.TrackedList)):
            return lt(value)
        else:
            raise TypeError(
                f"could not convert {type(value)} to {cls.__name__}"
            )


class _ComputedMultiProperty(
    _ComputedMultiPointer[_ST_co, _BT_co],
    _abstract.LinkDescriptor[_ST_co, _BT_co],
):
    pass


MultiProperty = TypeAliasType(
    "MultiProperty",
    Annotated[
        _MultiProperty[_ST_co, _BT_co],
        pydantic.Field(
            default_factory=list,
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
    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source_type: Any,
        handler: pydantic.GetCoreSchemaHandler,
    ) -> pydantic_core.CoreSchema:
        if _typing_inspect.is_generic_alias(source_type):
            args = typing.get_args(source_type)
            if issubclass(args[0], ProxyModel):
                return core_schema.no_info_plain_validator_function(
                    functools.partial(cls._validate, generic_args=args),
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
    ) -> _MT_co | None:
        mt, bmt = generic_args
        if value is None or isinstance(value, mt):
            return value
        elif isinstance(value, bmt):
            return mt(value)  # type: ignore [no-any-return]
        else:
            raise TypeError(
                f"could not convert {type(value)} to {mt.__name__}"
            )


class _Link(
    _AnyLink[_MT_co, _BMT_co], _abstract.LinkDescriptor[_MT_co, _BMT_co]
):
    if TYPE_CHECKING:

        @overload
        def __get__(self, obj: None, objtype: type[Any]) -> type[_MT_co]: ...

        @overload
        def __get__(self, obj: object, objtype: Any = None) -> _MT_co: ...

        def __get__(
            self,
            obj: Any,
            objtype: Any = None,
        ) -> type[_MT_co] | _MT_co: ...

        def __set__(self, obj: Any, value: _MT_co | _BMT_co) -> None: ...

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


class _OptionalLink(
    _AnyLink[_MT_co, _BMT_co],
    _abstract.OptionalLinkDescriptor[_MT_co, _BMT_co],
):
    pass


LinkWithProps = TypeAliasType(
    "LinkWithProps",
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
    _dlist.DistinctList[_PT_co], Generic[_PT_co, _BMT_co]
):
    # Mapping of object IDs to ProxyModels that wrap them.
    _wrapped_index: dict[int, _PT_co] | None = None

    def _init_tracking(self) -> None:
        super()._init_tracking()

        if self._wrapped_index is None:
            self._wrapped_index = {}
            for item in self._items:
                assert isinstance(item, ProxyModel)
                self._wrapped_index[id(item._p__obj__)] = cast("_PT_co", item)

    def _track_item(self, item: _PT_co) -> None:  # type: ignore [misc]
        assert isinstance(item, ProxyModel)
        super()._track_item(cast("_PT_co", item))
        assert self._wrapped_index is not None
        self._wrapped_index[id(item._p__obj__)] = cast("_PT_co", item)

    def _untrack_item(self, item: _PT_co) -> None:  # type: ignore [misc]
        assert isinstance(item, ProxyModel)
        super()._untrack_item(cast("_PT_co", item))
        assert self._wrapped_index is not None
        self._wrapped_index.pop(id(item._p__obj__), None)

    def _is_tracked(self, item: _PT_co | _BMT_co) -> bool:
        self._init_tracking()
        assert self._wrapped_index is not None

        if isinstance(item, ProxyModel):
            return id(item._p__obj__) in self._wrapped_index
        else:
            return id(item) in self._wrapped_index

    def extend(self, values: Iterable[_PT_co | _BMT_co]) -> None:
        # An optimized version of `extend()`

        if not values:
            # Empty list => early return
            return

        if values is self:
            values = list(values)

        self._ensure_snapshot()

        cls = type(self)
        t = cls.type

        assert self._wrapped_index is not None
        assert self._set is not None
        assert self._unhashables is not None

        # For an empty list we can call one extend() call instead
        # of slow iterative appends.
        fast_extend = len(self._wrapped_index) == 0

        for v in values:
            if type(v) is t.__proxy_of__:
                # Fast path -- `v` is an instance of the base type.
                # It has no link props, wrap it in a proxy in
                # a fast way.
                proxy = t.__gel_proxy_construct__(v, {})
                obj = v
            else:
                proxy, obj = self._cast_value(v)

            oid = id(obj)
            try:
                existing_proxy = self._wrapped_index[oid]
            except KeyError:
                self._wrapped_index[oid] = proxy
            else:
                if (
                    existing_proxy.__linkprops__.__dict__
                    != proxy.__linkprops__.__dict__
                ):
                    raise ValueError(
                        f"the list already contains {v!r} with "
                        f"a different set of link properties"
                    )

            try:
                self._set.add(proxy)
            except TypeError:
                self._unhashables[id(proxy)] = proxy

            if not fast_extend:
                self._items.append(proxy)

        if fast_extend:
            self._items.extend(self._wrapped_index.values())

    def _cast_value(self, value: Any) -> tuple[_PT_co, _BMT_co]:
        cls = type(self)
        t = cls.type

        assert issubclass(t, ProxyModel)

        if type(value) is t.__proxy_of__:
            # Fast path before we make all expensive isinstance calls.
            return (
                t.__gel_proxy_construct__(value, {}),
                value,
            )  # type: ignore [return-value]

        if type(value) is t:
            # It's a correct proxy for this link... return as is.
            return value, value._p__obj__  # type: ignore [return-value]

        if not isinstance(value, ProxyModel) and isinstance(
            value, t.__proxy_of__
        ):
            # It's not a proxy, but the object is of the correct type --
            # re-wrap it in a correct proxy.
            return (
                t.__gel_proxy_construct__(value, {}),
                value,
            )  # type: ignore [return-value]

        raise ValueError(
            f"{cls!r} accepts only values of type {t.__name__} "
            f"or {t.__proxy_of__.__name__}, got {type(value)!r}",
        )

    def _check_value(self, value: Any) -> _PT_co:
        proxy, obj = self._cast_value(value)

        # We have to check if a proxy around the same object is already
        # present in the list.
        self._init_tracking()
        assert self._wrapped_index is not None
        try:
            existing_proxy = self._wrapped_index[id(obj)]
        except KeyError:
            return proxy

        assert isinstance(existing_proxy, ProxyModel)

        if (
            existing_proxy.__linkprops__.__dict__
            != proxy.__linkprops__.__dict__
        ):
            raise ValueError(
                f"the list already contains {value!r} with "
                f" a different set of link properties"
            )
        # Return the already present identical proxy instead of inserting
        # another one
        return existing_proxy  # type: ignore [return-value]

    def _find_proxied_obj(self, item: _PT_co | _BMT_co) -> _PT_co | None:
        self._init_tracking()
        assert self._wrapped_index is not None

        if isinstance(item, ProxyModel):
            item = item._p__obj__

        return self._wrapped_index.get(id(item), None)

    def clear(self) -> None:
        super().clear()
        self._wrapped_index = None

    if TYPE_CHECKING:

        def append(self, value: _PT_co | _BMT_co) -> None: ...
        def insert(
            self, index: SupportsIndex, value: _PT_co | _BMT_co
        ) -> None: ...
        def __setitem__(
            self,
            index: SupportsIndex | slice,
            value: _PT_co | _BMT_co | Iterable[_PT_co | _BMT_co],
        ) -> None: ...
        def extend(self, values: Iterable[_PT_co | _BMT_co]) -> None: ...
        def remove(self, value: _PT_co | _BMT_co) -> None: ...
        def index(
            self,
            value: _PT_co | _BMT_co,
            start: SupportsIndex = 0,
            stop: SupportsIndex | None = None,
        ) -> int: ...
        def count(self, value: _PT_co | _BMT_co) -> int: ...
        def __add__(self, other: Iterable[_PT_co | _BMT_co]) -> Self: ...
        def __iadd__(self, other: Iterable[_PT_co | _BMT_co]) -> Self: ...


class _ComputedMultiLink(
    _ComputedMultiPointer[_MT_co, _BMT_co],
    _abstract.LinkDescriptor[_MT_co, _BMT_co],
):
    pass


class _MultiLink(
    _MultiPointer[_MT_co, _BMT_co],
    _abstract.LinkDescriptor[_MT_co, _BMT_co],
):
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


@functools.cache
def _make_dlist_type(
    types: tuple[type[_PT_co], type[_BMT_co]],
) -> type[_UpcastingDistinctList[_PT_co, _BMT_co]]:
    return _UpcastingDistinctList[types[0], types[1]]  # type: ignore [valid-type]


class _MultiLinkWithProps(
    _MultiPointer[_PT_co, _BMT_co],
    _abstract.LinkDescriptor[_PT_co, _BMT_co],
):
    if TYPE_CHECKING:

        @overload
        def __get__(self, obj: None, objtype: type[Any]) -> type[_PT_co]: ...

        @overload
        def __get__(
            self, obj: object, objtype: Any = None
        ) -> _UpcastingDistinctList[_PT_co, _BMT_co]: ...

        def __get__(
            self,
            obj: Any,
            objtype: Any = None,
        ) -> type[_PT_co] | _UpcastingDistinctList[_PT_co, _BMT_co] | None: ...

        def __set__(
            self, obj: Any, value: Sequence[_PT_co | _BMT_co]
        ) -> None: ...

    @classmethod
    def __gel_resolve_dlist__(
        cls,
        type_args: tuple[type[Any]] | tuple[type[Any], type[Any]],
    ) -> _dlist.DistinctList[_MT_co]:
        return _make_dlist_type(type_args)  # type: ignore [return-value]

    @classmethod
    def _validate(
        cls,
        value: Any,
        generic_args: tuple[type[Any], type[Any]],
    ) -> _UpcastingDistinctList[_PT_co, _BMT_co]:
        lt: type[_UpcastingDistinctList[_PT_co, _BMT_co]] = _make_dlist_type(
            generic_args
        )
        if type(lt) is list:  # type: ignore [comparison-overlap]
            # Optimization for the most common scenario - user passes
            # a list of objects to the constructor.
            return lt(value)
        elif isinstance(value, lt):
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

RequiredMultiLink = TypeAliasType(
    "RequiredMultiLink",
    Annotated[
        _MultiLink[_MT_co, _MT_co],
        pydantic.Field(
            default_factory=list,
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

RequiredMultiLinkWithProps = TypeAliasType(
    "RequiredMultiLinkWithProps",
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
