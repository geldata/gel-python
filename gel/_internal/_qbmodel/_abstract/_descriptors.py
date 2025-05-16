# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

"""EdgeQL query builder descriptors for attribute magic"""

from __future__ import annotations
from typing import TYPE_CHECKING, Any, Generic, TypeVar, overload

import typing

# XXX: get rid of this
from pydantic._internal import _namespace_utils  # noqa: PLC2701

from gel._internal import _qb
from gel._internal import _typing_eval
from gel._internal import _typing_inspect
from gel._internal import _utils

from ._base import GelType


class ModelFieldDescriptor(_qb.AbstractFieldDescriptor):
    __slots__ = (
        "__gel_annotation__",
        "__gel_name__",
        "__gel_origin__",
        "__gel_resolved_type__",
        "__gel_resolved_type_origin__",
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
        self.__gel_resolved_type_origin__: type | None = None

    def __repr__(self) -> str:
        qualname = f"{self.__gel_origin__.__qualname__}.{self.__gel_name__}"
        anno = self.__gel_annotation__
        return f"<{self.__class__.__name__} {qualname}: {anno}>"

    def __set_name__(self, owner: Any, name: str) -> None:
        self.__gel_name__ = name

    def _try_resolve_type(self) -> Any:
        origin = self.__gel_origin__
        globalns = _utils.module_ns_of(origin)
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
            self.__gel_resolved_type_origin__ = typing.get_origin(t)
            t = typing.get_args(t)[0]
            if not isinstance(
                t, type
            ) and not _typing_inspect.is_generic_alias(t):
                raise AssertionError(
                    f"BasePointer type argument is not a type: {t}"
                )

        if t is not None:
            self.__gel_resolved_type__ = t

        return t

    def get_resolved_type_generic_origin(self) -> type | None:
        t = self.__gel_resolved_type__
        if t is None:
            t = self._try_resolve_type()
        if t is None:
            raise RuntimeError(
                f"cannot resolve type of "
                f"{self.__gel_origin__.__name__}.{self.__gel_name__}"
            )
        else:
            return self.__gel_resolved_type_origin__

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
        owner: type[Any] | _qb.PathAlias,
    ) -> Any:
        t = self.get_resolved_type()
        if t is None:
            return self
        else:
            source: _qb.Expr
            if isinstance(owner, _qb.PathAlias):
                source = owner.__gel_metadata__
            elif hasattr(owner, "__edgeql_qb_expr__"):
                source = _qb.edgeql_qb_expr(owner)
            else:
                return t
            metadata = _qb.Path(
                type_=t.__gel_reflection__.name,
                source=source,
                name=self.__gel_name__,
                is_lprop=False,
            )
            return _qb.AnnotatedPath(t, metadata)

    def __get__(
        self,
        instance: object | None,
        owner: type[Any] | None = None,
    ) -> Any:
        if instance is not None:
            return self
        else:
            assert owner is not None
            return self.get(owner)


def field_descriptor(
    origin: type[Any],
    name: str,
    annotation: type[Any],
) -> ModelFieldDescriptor:
    return ModelFieldDescriptor(origin, name, annotation)


T = TypeVar("T")
T_co = TypeVar("T_co", covariant=True)
BT_co = TypeVar("BT_co", covariant=True)


class PointerDescriptor(_qb.AbstractDescriptor, Generic[T_co, BT_co]):
    if TYPE_CHECKING:

        def __get__(self, obj: None, objtype: type[Any]) -> type[T_co]: ...


class OptionalPointerDescriptor(PointerDescriptor[T_co, BT_co]):
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


class PropertyDescriptor(PointerDescriptor[T_co, BT_co]):
    pass


class OptionalPropertyDescriptor(OptionalPointerDescriptor[T_co, BT_co]):
    pass


class LinkDescriptor(PointerDescriptor[T_co, BT_co]):
    pass


class OptionalLinkDescriptor(OptionalPointerDescriptor[T_co, BT_co]):
    pass
