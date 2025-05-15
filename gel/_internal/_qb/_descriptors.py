# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

"""EdgeQL query builder descriptors for attribute magic"""

from __future__ import annotations
from typing import TYPE_CHECKING, Any, Generic, TypeVar, overload

import typing

# XXX: get rid of this
from pydantic._internal import _namespace_utils  # noqa: PLC2701

from gel._internal import _typing_eval
from gel._internal import _typing_inspect
from gel._internal import _utils

from ._abstract import AbstractDescriptor, AbstractFieldDescriptor, Expr
from ._expressions import Path
from ._generics import PathAlias, AnnotatedPath
from ._protocols import edgeql_qb_expr


class ModelFieldDescriptor(AbstractFieldDescriptor):
    __slots__ = (
        "__gel_annotation__",
        "__gel_name__",
        "__gel_origin__",
        "__gel_resolved_type__",
    )

    def __init__(self, origin: type, name: str, annotation: type[Any]) -> None:
        self.__gel_origin__ = origin
        self.__gel_name__ = name
        self.__gel_annotation__ = annotation
        self.__gel_resolved_type__ = None

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
            t = typing.get_args(t)[0]
            if not isinstance(
                t, type
            ) and not _typing_inspect.is_generic_alias(t):
                raise AssertionError(
                    f"BasePointer type argument is not a type: {t}"
                )

        return t

    def get(
        self,
        owner: type[Any] | PathAlias,
    ) -> Any:
        t = self.__gel_resolved_type__
        if t is None:
            t = self._try_resolve_type()
            if t is not None:
                self.__gel_resolved_type__ = t

        if t is None:
            return self
        else:
            source: Expr
            if isinstance(owner, PathAlias):
                source = owner.__gel_metadata__
            elif hasattr(owner, "__edgeql_qb_expr__"):
                source = edgeql_qb_expr(owner)
            else:
                return t
            metadata = Path(
                source=source,
                name=self.__gel_name__,
                is_lprop=False,
            )
            return AnnotatedPath(t, metadata)

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


class PointerDescriptor(AbstractDescriptor, Generic[T_co, BT_co]):
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
