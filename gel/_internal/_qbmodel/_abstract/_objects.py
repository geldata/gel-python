# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

"""Base object types used to implement class-based query builders"""

from __future__ import annotations
from typing import TYPE_CHECKING, Any, ClassVar, Literal
from typing_extensions import Self

import functools
import weakref

from gel._internal import _qb
from gel._internal._xmethod import classonlymethod, hybridmethod

from ._base import GelObjectType, GelObjectTypeMeta
from ._expressions import (
    add_filter,
    add_limit,
    add_offset,
    delete,
    order_by,
    select,
    update,
)
from ._functions import (
    assert_single,
)

if TYPE_CHECKING:
    import uuid
    from collections.abc import Callable


class GelModelMeta(GelObjectTypeMeta):
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
        __gel_variant__: str | None = None,
        **kwargs: Any,
    ) -> GelModelMeta:
        cls = super().__new__(mcls, name, bases, namespace, **kwargs)
        if __gel_type_id__ is not None:
            mcls.__gel_class_registry__[__gel_type_id__] = cls
        if __gel_variant__ is not None:
            cls.set_variant(__gel_variant__)
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

    def set_variant(self, variant: str | None) -> None:
        self.__gel_variant__ = variant


class GelSourceModel(_qb.GelSourceMetadata):
    pass


class GelModel(
    GelSourceModel,
    GelObjectType,
    metaclass=GelModelMeta,
):
    if TYPE_CHECKING:

        @classmethod
        def __edgeql__(cls) -> tuple[type[Self], str]: ...  # pyright: ignore [reportIncompatibleMethodOverride]

        @classmethod
        def select(cls, /, **kwargs: Any) -> type[Self]: ...

        @classmethod
        def update(cls, /, **kwargs: Any) -> type[Self]: ...

        @classmethod
        def delete(cls, /) -> type[Self]: ...

        @classmethod
        def filter(cls, /, *exprs: Any, **properties: Any) -> type[Self]: ...

        @classmethod
        def order_by(
            cls,
            /,
            *exprs: (
                Callable[[type[Self]], _qb.ExprCompatible]
                | tuple[Callable[[type[Self]], _qb.ExprCompatible], str]
                | tuple[Callable[[type[Self]], _qb.ExprCompatible], str, str]
            ),
            **kwargs: bool | str | tuple[str, str],
        ) -> type[Self]: ...

        @classmethod
        def limit(cls, /, expr: Any) -> type[Self]: ...

        @classmethod
        def offset(cls, /, expr: Any) -> type[Self]: ...

        @classmethod
        def __gel_assert_single__(
            cls,
            /,
            *,
            message: str | None = None,
        ) -> type[Self]: ...

    else:

        @classonlymethod
        @_qb.exprmethod
        @classmethod
        def select(
            cls,
            /,
            *elements: _qb.PathAlias | Literal["*"],
            __operand__: _qb.ExprAlias | None = None,
            **kwargs: Any,
        ) -> type[Self]:
            return _qb.AnnotatedExpr(  # type: ignore [return-value]
                cls,
                select(cls, *elements, __operand__=__operand__, **kwargs),
            )

        @classonlymethod
        @_qb.exprmethod
        @classmethod
        def update(
            cls,
            /,
            __operand__: _qb.ExprAlias | None = None,
            **kwargs: Any,
        ) -> type[Self]:
            return _qb.AnnotatedExpr(  # type: ignore [return-value]
                cls,
                update(cls, __operand__=__operand__, **kwargs),
            )

        @classonlymethod
        @_qb.exprmethod
        @classmethod
        def delete(
            cls,
            /,
            __operand__: _qb.ExprAlias | None = None,
        ) -> type[Self]:
            return _qb.AnnotatedExpr(  # type: ignore [return-value]
                cls,
                delete(cls, __operand__=__operand__),
            )

        @classonlymethod
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

        @classonlymethod
        @_qb.exprmethod
        @classmethod
        def order_by(
            cls,
            /,
            *elements: (
                Callable[[type[Self]], _qb.ExprCompatible]
                | tuple[Callable[[type[Self]], _qb.ExprCompatible], str]
                | tuple[Callable[[type[Self]], _qb.ExprCompatible], str, str]
            ),
            __operand__: _qb.ExprAlias | None = None,
            **kwargs: bool | str | tuple[str, str],
        ) -> type[Self]:
            return _qb.AnnotatedExpr(  # type: ignore [return-value]
                cls,
                order_by(cls, *elements, __operand__=__operand__, **kwargs),
            )

        @classonlymethod
        @_qb.exprmethod
        @classmethod
        def limit(
            cls,
            /,
            value: Any,
            __operand__: _qb.ExprAlias | None = None,
        ) -> type[Self]:
            return _qb.AnnotatedExpr(  # type: ignore [return-value]
                cls,
                add_limit(cls, value, __operand__=__operand__),
            )

        @classonlymethod
        @_qb.exprmethod
        @classmethod
        def offset(
            cls,
            /,
            value: Any,
            __operand__: _qb.ExprAlias | None = None,
        ) -> type[Self]:
            return _qb.AnnotatedExpr(  # type: ignore [return-value]
                cls,
                add_offset(cls, value, __operand__=__operand__),
            )

        @classonlymethod
        @_qb.exprmethod
        @classmethod
        def __gel_assert_single__(
            cls,
            /,
            *,
            message: str | None = None,
            __operand__: _qb.ExprAlias | None = None,
        ) -> type[Self]:
            return _qb.AnnotatedExpr(  # type: ignore [return-value]
                cls,
                assert_single(cls, message=message, __operand__=__operand__),
            )

        @hybridmethod
        def __edgeql__(self) -> tuple[type, str]:
            if isinstance(self, type):
                return self, _qb.toplevel_edgeql(
                    self,
                    splat_cb=functools.partial(
                        _qb.get_object_type_splat, self
                    ),
                )
            else:
                raise NotImplementedError(
                    f"{type(self)} instances are not queryable"
                )

    @classmethod
    def __edgeql_qb_expr__(cls) -> _qb.Expr:  # pyright: ignore [reportIncompatibleMethodOverride]
        this_type = cls.__gel_reflection__.name
        return _qb.SchemaSet(type_=this_type)


class GelLinkModel(GelSourceModel):
    pass
