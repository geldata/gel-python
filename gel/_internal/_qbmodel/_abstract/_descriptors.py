# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

"""EdgeQL query builder descriptors for attribute magic"""

from __future__ import annotations
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Generic,
    TypeVar,
    overload,
)
from typing_extensions import Self, Never

import dataclasses
import typing

# XXX: get rid of this
from pydantic._internal import _namespace_utils  # noqa: PLC2701

from gel._internal import _qb
from gel._internal import _namespace
from gel._internal import _tracked_list
from gel._internal import _typing_eval
from gel._internal import _typing_inspect
from gel._internal import _typing_parametric
from gel._internal import _utils

from ._base import (
    GelType,
    AbstractGelModel,
    AbstractGelLinkModel,
    is_gel_type,
    maybe_collapse_object_type_variant_union,
)


if TYPE_CHECKING:
    import types
    from collections.abc import Sequence
    from ._distinct_list import AbstractDistinctList, ProxyDistinctList


class ModelFieldDescriptor(_qb.AbstractFieldDescriptor):
    __slots__ = (
        "__gel_annotation__",
        "__gel_name__",
        "__gel_origin__",
        "__gel_resolved_descriptor__",
        "__gel_resolved_type__",
    )

    def __init__(
        self,
        origin: type[GelType],
        name: str,
        annotation: type[Any],
    ) -> None:
        self.__gel_origin__ = origin
        self.__gel_name__ = name
        self.__gel_annotation__ = annotation
        self.__gel_resolved_type__: type[GelType] | None = None
        self.__gel_resolved_descriptor__: types.GenericAlias | None = None

    def __repr__(self) -> str:
        qualname = f"{self.__gel_origin__.__qualname__}.{self.__gel_name__}"
        anno = self.__gel_annotation__
        return f"<{self.__class__.__name__} {qualname}: {anno}>"

    def __set_name__(self, owner: Any, name: str) -> None:
        self.__gel_name__ = name

    @property
    def _fqname(self) -> str:
        return f"{_utils.type_repr(self.__gel_origin__)}.{self.__gel_name__}"

    def _try_resolve_type(self) -> Any:
        origin = self.__gel_origin__
        globalns = _namespace.module_ns_of(origin)
        ns = _namespace_utils.NsResolver(
            parent_namespace=getattr(
                origin,
                "__pydantic_parent_namespace__",
                None,
            ),
        )
        with ns.push(origin):
            globalns, localns = ns.types_namespace

        t = _typing_eval.try_resolve_type(
            self.__gel_annotation__,
            owner=origin,
            globals=globalns,
            locals=localns,
        )
        if (
            t is not None
            and _typing_inspect.is_generic_alias(t)
            and issubclass(typing.get_origin(t), PointerDescriptor)
        ):
            self.__gel_resolved_descriptor__ = t
            t = typing.get_args(t)[0]

        if t is not None:
            if _typing_inspect.is_union_type(t):
                collapsed = maybe_collapse_object_type_variant_union(t)
                if collapsed is not None:
                    t = collapsed

            if not is_gel_type(t):
                raise AssertionError(
                    f"{self._fqname} type argument is not a GelType: {t}"
                )

            self.__gel_resolved_type__ = t

        return t

    def get_resolved_pointer_descriptor(self) -> types.GenericAlias | None:
        t = self.__gel_resolved_descriptor__
        if t is None:
            t = self._try_resolve_type()
        if t is None:
            raise RuntimeError(f"cannot resolve type of {self._fqname}")
        else:
            return self.__gel_resolved_descriptor__

    def get_resolved_type(self) -> type[GelType] | None:
        t = self.__gel_resolved_type__
        if t is None:
            t = self._try_resolve_type()
        if t is None:
            return None
        else:
            return t

    def get(
        self,
        owner: type[_qb.GelSourceMetadata],
        expr: _qb.BaseAlias | None = None,
    ) -> Any:
        t = self.get_resolved_type()
        if t is None:
            return self
        else:
            source: _qb.Expr
            if expr is not None:
                source = expr.__gel_metadata__
            elif _qb.is_expr_compatible(owner):
                source = _qb.edgeql_qb_expr(owner)
            else:
                return t
            try:
                ptr = owner.__gel_reflection__.pointers[self.__gel_name__]
            except KeyError:
                # This is a user-defined ad-hoc computed pointer
                type_ = t.__gel_reflection__.name
            else:
                type_ = ptr.type
            metadata = _qb.Path(
                type_=type_,
                source=source,
                name=self.__gel_name__,
                is_lprop=False,
            )
            return _qb.AnnotatedPath(t, metadata)

    def __get__(
        self,
        instance: object | None,
        owner: type[Any] | None = None,
        /,
    ) -> Any:
        if instance is not None:
            raise AttributeError(f"{self.__gel_name__!r} is not set")
        else:
            assert owner is not None
            cache_attr = f"__cached_path_{self.__gel_name__}"

            try:
                return object.__getattribute__(owner, cache_attr)
            except AttributeError:
                pass

            path = self.get(owner)
            setattr(owner, cache_attr, path)
            return path


def field_descriptor(
    origin: type[Any],
    name: str,
    annotation: type[Any],
) -> ModelFieldDescriptor:
    return ModelFieldDescriptor(origin, name, annotation)


T_co = TypeVar("T_co", bound=GelType, covariant=True)
BT_co = TypeVar("BT_co", covariant=True)

_MT_co = TypeVar("_MT_co", bound=AbstractGelModel, covariant=True)
"""Derived model type"""

_BMT_co = TypeVar("_BMT_co", bound=AbstractGelModel, covariant=True)
"""Base model type (which _MT_co is directly derived from)"""

_LM_co = TypeVar("_LM_co", bound=AbstractGelLinkModel, covariant=True)
"""Link model (defines link properties)."""


class PointerDescriptor(_qb.AbstractDescriptor, Generic[T_co, BT_co]):
    pass


class OptionalPointerDescriptor(PointerDescriptor[T_co, BT_co]):
    pass


class ComputedPointerDescriptor(PointerDescriptor[T_co, BT_co]):
    pass


class AnyPropertyDescriptor(PointerDescriptor[T_co, BT_co]):
    pass


class PropertyDescriptor(AnyPropertyDescriptor[T_co, BT_co]):
    if TYPE_CHECKING:

        @overload
        def __get__(
            self,
            instance: None,
            owner: type[Any],
            /,
        ) -> type[T_co]: ...

        @overload
        def __get__(
            self,
            instance: Any,
            objtype: type[Any] | None = None,
            /,
        ) -> BT_co: ...

        def __get__(
            self,
            instance: Any,
            owner: type[Any] | None = None,
            /,
        ) -> type[T_co] | BT_co: ...

        def __set__(
            self,
            instance: Any,
            value: T_co | BT_co,
            /,
        ) -> None: ...


class ComputedPropertyDescriptor(AnyPropertyDescriptor[T_co, BT_co]):
    if TYPE_CHECKING:

        @overload
        def __get__(
            self,
            instance: None,
            owner: type[Any],
            /,
        ) -> type[T_co]: ...

        @overload
        def __get__(
            self,
            instance: Any,
            objtype: type[Any] | None = None,
            /,
        ) -> BT_co: ...

        def __get__(
            self,
            instance: Any,
            owner: type[Any] | None = None,
            /,
        ) -> type[T_co] | BT_co: ...

        def __set__(
            self,
            instance: Any,
            value: Never,
            /,
        ) -> None: ...


class MultiPropertyDescriptor(AnyPropertyDescriptor[T_co, BT_co]):
    if TYPE_CHECKING:

        @overload
        def __get__(
            self,
            instance: None,
            owner: type[Any],
            /,
        ) -> type[T_co]: ...

        @overload
        def __get__(
            self,
            instance: Any,
            owner: type[Any] | None = None,
            /,
        ) -> _tracked_list.DowncastingTrackedList[T_co, BT_co]: ...

        def __get__(
            self,
            instance: Any,
            owner: type[Any] | None = None,
            /,
        ) -> (
            type[T_co]
            | _tracked_list.DowncastingTrackedList[T_co, BT_co]
            | None
        ): ...

        def __set__(
            self,
            instance: Any,
            value: Sequence[T_co | BT_co],
            /,
        ) -> None: ...


class ComputedMultiPropertyDescriptor(
    ComputedPointerDescriptor[T_co, BT_co],
    AnyPropertyDescriptor[T_co, BT_co],
):
    if TYPE_CHECKING:

        @overload
        def __get__(
            self, instance: None, owner: type[Any], /
        ) -> type[T_co]: ...

        @overload
        def __get__(
            self,
            instance: Any,
            owner: type[Any] | None = None,
            /,
        ) -> tuple[T_co, ...]: ...

        def __get__(
            self,
            instance: Any,
            owner: type[Any] | None = None,
        ) -> type[T_co] | tuple[T_co, ...]: ...


class OptionalPropertyDescriptor(
    OptionalPointerDescriptor[T_co, BT_co],
    AnyPropertyDescriptor[T_co, BT_co],
):
    if TYPE_CHECKING:

        @overload
        def __get__(
            self,
            instance: None,
            owner: type[Any],
            /,
        ) -> type[T_co]: ...

        @overload
        def __get__(
            self,
            instance: Any,
            owner: type[Any] | None = None,
            /,
        ) -> BT_co | None: ...

        def __get__(
            self,
            instance: Any,
            owner: type[Any] | None = None,
        ) -> type[T_co] | BT_co | None: ...

        def __set__(
            self,
            instance: Any,
            value: T_co | BT_co | None,
            /,
        ) -> None: ...


class AnyLinkDescriptor(PointerDescriptor[T_co, BT_co]):
    pass


class LinkDescriptor(AnyLinkDescriptor[T_co, BT_co]):
    if TYPE_CHECKING:

        @overload
        def __get__(
            self,
            instance: None,
            owner: type[Any],
            /,
        ) -> type[T_co]: ...

        @overload
        def __get__(
            self,
            instance: Any,
            owner: type[Any] | None = None,
            /,
        ) -> T_co: ...

        def __get__(
            self,
            instance: Any | None,
            owner: type[Any] | None = None,
            /,
        ) -> type[T_co] | T_co: ...

        def __set__(
            self,
            instance: Any,
            value: BT_co,  # type: ignore [misc]
            /,
        ) -> None: ...


class MultiLinkDescriptor(AnyLinkDescriptor[_MT_co, _BMT_co]):
    if TYPE_CHECKING:

        @overload
        def __get__(
            self,
            instance: None,
            owner: type[Any],
            /,
        ) -> type[_MT_co]: ...

        @overload
        def __get__(
            self,
            instance: Any,
            owner: type[Any] | None = None,
            /,
        ) -> AbstractDistinctList[_MT_co]: ...

        def __get__(
            self,
            instance: Any,
            owner: type[Any] | None = None,
            /,
        ) -> type[_MT_co] | AbstractDistinctList[_MT_co] | None: ...

        def __set__(
            self,
            instance: Any,
            value: Sequence[_MT_co | _BMT_co],
            /,
        ) -> None: ...


class ComputedMultiLinkDescriptor(
    ComputedPointerDescriptor[_MT_co, _BMT_co],
    AnyLinkDescriptor[_MT_co, _BMT_co],
):
    if TYPE_CHECKING:

        @overload
        def __get__(
            self, instance: None, owner: type[Any], /
        ) -> type[_MT_co]: ...

        @overload
        def __get__(
            self,
            instance: Any,
            owner: type[Any] | None = None,
            /,
        ) -> tuple[_MT_co, ...]: ...

        def __get__(
            self,
            instance: Any,
            owner: type[Any] | None = None,
        ) -> type[_MT_co] | tuple[_MT_co, ...]: ...


class OptionalLinkDescriptor(
    OptionalPointerDescriptor[T_co, BT_co],
    AnyLinkDescriptor[T_co, BT_co],
):
    if TYPE_CHECKING:

        @overload
        def __get__(
            self,
            instance: None,
            owner: type[Any],
            /,
        ) -> type[T_co]: ...

        @overload
        def __get__(
            self,
            instance: Any,
            owner: type[Any] | None = None,
            /,
        ) -> T_co | None: ...

        def __get__(
            self,
            instance: Any | None,
            owner: type[Any] | None = None,
            /,
        ) -> type[T_co] | T_co | None: ...

        def __set__(
            self,
            instance: Any,
            value: BT_co | None,
            /,
        ) -> None: ...


class GelLinkModelDescriptor(
    _typing_parametric.PickleableClassParametricType,
    _qb.AbstractFieldDescriptor,
    Generic[_LM_co],
):
    _link_model_class: ClassVar[type[_LM_co]]  # type: ignore [misc]

    def __set_name__(self, owner: type[Any], name: str) -> None:
        self._link_model_attr = name

    @overload
    def __get__(self, instance: None, owner: type[Any], /) -> type[_LM_co]: ...

    @overload
    def __get__(
        self, instance: Any, owner: type[Any] | None = None, /
    ) -> _LM_co: ...

    def __get__(
        self,
        instance: Any | None,
        owner: type[Any] | None = None,
        /,
    ) -> type[_LM_co] | _LM_co:
        if instance is None:
            return self._link_model_class
        else:
            attr = self._link_model_attr
            linkobj: _LM_co | None = instance.__dict__.get(attr)
            if linkobj is None:
                linkobj = self._link_model_class.__gel_model_construct__({})
                instance.__dict__[attr] = linkobj

            return linkobj

    def get(
        self,
        owner: type[AbstractGelProxyModel[AbstractGelModel, _LM_co]],
        expr: _qb.BaseAlias | None = None,
    ) -> Any:
        source: _qb.Expr
        if expr is not None:
            source = expr.__gel_metadata__
        else:
            raise AssertionError("missing source for link path")

        if (
            not isinstance(source, _qb.PathPrefix)
            or source.source_link is None
        ):
            raise AttributeError(
                "__linkprops__", name="__linkprops__", obj=owner
            )

        prefix = dataclasses.replace(source, lprop_pivot=True)
        return _qb.AnnotatedExpr(owner.__linkprops__, prefix)  # pyright: ignore [reportGeneralTypeIssues]


class AbstractGelProxyModel(AbstractGelModel, Generic[_MT_co, _LM_co]):
    __linkprops__: GelLinkModelDescriptor[_LM_co]

    if TYPE_CHECKING:
        _p__obj__: _MT_co
        __proxy_of__: ClassVar[type[_MT_co]]  # type: ignore [misc]

    @classmethod
    def __gel_proxy_construct__(
        cls,
        obj: _MT_co,  # type: ignore [misc]
        lprops: dict[str, Any],
    ) -> Self:
        raise NotImplementedError

    def without_linkprops(self) -> _MT_co:
        raise NotImplementedError


_PT_co = TypeVar(
    "_PT_co",
    bound=AbstractGelProxyModel[AbstractGelModel, AbstractGelLinkModel],
    covariant=True,
)
"""Proxy model"""


class MultiLinkWithPropsDescriptor(MultiLinkDescriptor[_PT_co, _BMT_co]):
    if TYPE_CHECKING:

        @overload
        def __get__(
            self,
            instance: None,
            owner: type[Any],
            /,
        ) -> type[_PT_co]: ...

        @overload
        def __get__(
            self,
            instance: Any,
            owner: type[Any] | None = None,
            /,
        ) -> ProxyDistinctList[_PT_co, _BMT_co]: ...

        def __get__(
            self,
            instance: Any,
            owner: type[Any] | None = None,
            /,
        ) -> type[_PT_co] | ProxyDistinctList[_PT_co, _BMT_co] | None: ...

        def __set__(
            self,
            instance: Any,
            value: Sequence[_PT_co | _BMT_co],
            /,
        ) -> None: ...
