# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

"""Base object types used to implement class-based query builders"""

from __future__ import annotations
from typing import TYPE_CHECKING, Any, ClassVar
from typing_extensions import Self

import weakref

from gel._internal import _qb

from ._base import GelType, GelTypeMeta
from ._expressions import add_filter, select

if TYPE_CHECKING:
    import uuid


class GelModelMeta(GelTypeMeta):
    __gel_class_registry__: ClassVar[
        weakref.WeakValueDictionary[uuid.UUID, type[Any]]
    ] = weakref.WeakValueDictionary()

    def __new__(  # noqa: PYI034
        mcls,
        name: str,
        bases: tuple[type[Any], ...],
        namespace: dict[str, Any],
        *,
        __gel_type_id__: uuid.UUID | None = None,
        **kwargs: Any,
    ) -> GelModelMeta:
        cls = super().__new__(mcls, name, bases, namespace, **kwargs)
        if __gel_type_id__ is not None:
            mcls.__gel_class_registry__[__gel_type_id__] = cls
        return cls

    @classmethod
    def get_class_by_id(cls, tid: uuid.UUID) -> type[GelModel]:
        try:
            return cls.__gel_class_registry__[tid]
        except KeyError:
            raise LookupError(
                f"cannot find GelModel for object type id {tid}"
            ) from None

    @classmethod
    def register_class(cls, tid: uuid.UUID, type_: type[GelModel]) -> None:
        cls.__gel_class_registry__[tid] = cls


class GelModel(
    GelType,
    metaclass=GelModelMeta,
):
    if TYPE_CHECKING:

        @classmethod
        def select(cls, /, **kwargs: bool | type[GelType]) -> type[Self]: ...
    else:

        @_qb.exprmethod
        @classmethod
        def select(
            cls,
            /,
            *elements: _qb.PathAlias,
            __operand__: _qb.ExprAlias | None = None,
            **kwargs: bool | type[GelType],
        ) -> type[Self]:
            return _qb.AnnotatedExpr(  # type: ignore [return-value]
                cls,
                select(cls, *elements, __operand__=__operand__, **kwargs),
            )

    if TYPE_CHECKING:

        @classmethod
        def filter(
            cls,
            /,
            *exprs: Any,
            **properties: Any,
        ) -> type[Self]: ...
    else:

        @_qb.exprmethod
        @classmethod
        def filter(
            cls,
            /,
            *exprs: Any,
            __operand__: _qb.ExprAlias | None = None,
            **properties: Any,
        ) -> type[Self]:
            return _qb.AnnotatedExpr(  # type: ignore [return-value]
                cls,
                add_filter(cls, *exprs, __operand__=__operand__, **properties),
            )

    @classmethod
    def __edgeql_qb_expr__(cls) -> _qb.Expr:  # pyright: ignore [reportIncompatibleMethodOverride]
        this_type = cls.__gel_reflection__.name
        return _qb.Shape(
            type_=this_type,
            expr=_qb.SchemaSet(type_=this_type),
            star_splat=True,
        )
