# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

"""Definitions of query builder methods on models."""

from __future__ import annotations
from typing import (
    TYPE_CHECKING,
    Any,
    Literal,
)
from typing_extensions import Self


from gel._internal import _qb
from gel._internal._xmethod import classonlymethod

from ._base import AbstractGelModel

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
    from collections.abc import Callable


class BaseGelModel(AbstractGelModel):
    if TYPE_CHECKING:

        @classmethod
        def select(
            cls,
            /,
            *elements: _qb.PathAlias | Literal["*"],
            **kwargs: Any,
        ) -> type[Self]: ...

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
            *elements: _qb.PathAlias | Literal["*", "**"],
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

    @classmethod
    def __edgeql_qb_expr__(cls) -> _qb.Expr:  # pyright: ignore [reportIncompatibleMethodOverride]
        this_type = cls.__gel_reflection__.name
        return _qb.SchemaSet(type_=this_type)
