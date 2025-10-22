# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

"""Definitions of query builder methods on models."""

from __future__ import annotations
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Generic,
    Literal,
    TypeVar,
)
from typing_extensions import Self
import weakref

from gel._internal import _qb
from gel._internal._schemapath import (
    TypeNameIntersection,
)
from gel._internal._xmethod import classonlymethod

from ._base import AbstractGelModel

from ._expressions import (
    add_filter,
    add_limit,
    add_offset,
    add_object_type_filter,
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


_T_OtherModel = TypeVar("_T_OtherModel", bound="BaseGelModel")


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
        def is_(
            cls: type[Self], /, other_model: type[_T_OtherModel]
        ) -> type[_T_OtherModel]: ...

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
        def is_(
            cls: type[Self],
            /,
            value: type[_T_OtherModel],
            __operand__: _qb.ExprAlias | None = None,
        ) -> type[BaseGelModelIntersection[type[Self], type[_T_OtherModel]]]:
            return _qb.AnnotatedExpr(  # type: ignore [return-value]
                create_intersection(cls, value),
                add_object_type_filter(cls, value, __operand__=__operand__),
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
        this_type = cls.__gel_reflection__.type_name
        return _qb.SchemaSet(type_=this_type)


_T_Lhs = TypeVar("_T_Lhs", bound="type[AbstractGelModel]")
_T_Rhs = TypeVar("_T_Rhs", bound="type[AbstractGelModel]")


class BaseGelModelIntersection(
    BaseGelModel,
    Generic[_T_Lhs, _T_Rhs],
):
    __gel_type_class__: ClassVar[type]

    lhs: ClassVar[type[AbstractGelModel]]
    rhs: ClassVar[type[AbstractGelModel]]


T = TypeVar('T')
U = TypeVar('U')


def unchanged(l: T) -> T:
    return l


def take_left(l: T, r: T) -> T:
    return l


def combine_dicts(
    lhs: dict[str, T],
    rhs: dict[str, T],
    *,
    process_unique: Callable[[T], U | None] = unchanged,  # type: ignore[assignment]
    process_common: Callable[[T, T], U | None] = take_left,  # type: ignore[assignment]
) -> dict[str, U]:
    result: dict[str, U] = {}

    # unique pointers
    result |= {
        p_name: p_ref
        for p_name, lhs_p_ref in lhs.items()
        if p_name not in rhs
        if (p_ref := process_unique(lhs_p_ref)) is not None
    }
    result |= {
        p_name: p_ref
        for p_name, rhs_p_ref in rhs.items()
        if p_name not in lhs
        if (p_ref := process_unique(rhs_p_ref)) is not None
    }

    # common pointers
    result |= {
        p_name: p_ref
        for p_name, lhs_p_ref in rhs.items()
        if (
            (rhs_p_ref := rhs.get(p_name)) is not None
            and (p_ref := process_common(lhs_p_ref, rhs_p_ref)) is not None
        )
    }

    return result


_type_intersection_cache: weakref.WeakKeyDictionary[
    type[AbstractGelModel],
    weakref.WeakKeyDictionary[
        type[AbstractGelModel],
        type[
            BaseGelModelIntersection[
                type[AbstractGelModel], type[AbstractGelModel]
            ]
        ],
    ],
] = weakref.WeakKeyDictionary()


def create_intersection(
    lhs: _T_Lhs,
    rhs: _T_Rhs,
) -> type[BaseGelModelIntersection[_T_Lhs, _T_Rhs]]:
    """Create a runtime intersection type which acts like a GelModel."""

    if (lhs_entry := _type_intersection_cache.get(lhs)) and (
        rhs_entry := lhs_entry.get(rhs)
    ):
        return rhs_entry  # type: ignore[return-value]

    # Combine pointer reflections from args
    ptr_reflections: dict[str, _qb.GelPointerReflection] = combine_dicts(
        lhs.__gel_reflection__.pointers,
        rhs.__gel_reflection__.pointers,
        process_common=lambda l, r: l if l == r else None,
    )

    # Create type reflection for intersection type
    class __gel_reflection__(_qb.GelObjectTypeExprMetadata.__gel_reflection__):  # noqa: N801
        expr_object_types: set[type[AbstractGelModel]] = getattr(
            lhs.__gel_reflection__, 'expr_object_types', {lhs}
        ) | getattr(rhs.__gel_reflection__, 'expr_object_types', {rhs})

        type_name = TypeNameIntersection(
            args=(
                lhs.__gel_reflection__.type_name,
                rhs.__gel_reflection__.type_name,
            )
        )

        pointers = ptr_reflections

        @classmethod
        def object(
            cls,
        ) -> Any:
            raise NotImplementedError(
                "Type expressions schema objects are inaccessible"
            )

    result = type(
        f"({lhs.__name__} & {rhs.__name__})",
        (BaseGelModelIntersection,),
        {
            'lhs': lhs,
            'rhs': rhs,
            '__gel_reflection__': __gel_reflection__,
        },
    )

    # Generate path aliases for pointers.
    #
    # These are used to generate the appropriate path prefix when getting
    # pointers in shapes.
    #
    # For example, doing `Foo.select(foo=lambda x: x.is_(Bar).bar)`
    # will produce the query:
    #    select Foo { [is Bar].bar }
    lhs_prefix = _qb.PathTypeIntersectionPrefix(
        type_=__gel_reflection__.type_name,
        type_filter=lhs.__gel_reflection__.type_name,
    )
    rhs_prefix = _qb.PathTypeIntersectionPrefix(
        type_=__gel_reflection__.type_name,
        type_filter=rhs.__gel_reflection__.type_name,
    )

    def process_path_alias(
        p_name: str,
        p_refl: _qb.GelPointerReflection,
        path_alias: _qb.PathAlias,
        source: _qb.Expr,
    ) -> _qb.PathAlias:
        return _qb.PathAlias(
            path_alias.__gel_origin__,
            _qb.Path(
                type_=p_refl.type,
                source=source,
                name=p_name,
                is_lprop=False,
            ),
        )

    path_aliases: dict[str, _qb.PathAlias] = combine_dicts(
        {
            p_name: process_path_alias(p_name, p_refl, path_alias, lhs_prefix)
            for p_name, p_refl in lhs.__gel_reflection__.pointers.items()
            if (
                hasattr(lhs, p_name)
                and (path_alias := getattr(lhs, p_name, None)) is not None
                and isinstance(path_alias, _qb.PathAlias)
            )
        },
        {
            p_name: process_path_alias(p_name, p_refl, path_alias, rhs_prefix)
            for p_name, p_refl in rhs.__gel_reflection__.pointers.items()
            if (
                hasattr(rhs, p_name)
                and (path_alias := getattr(rhs, p_name, None)) is not None
                and isinstance(path_alias, _qb.PathAlias)
            )
        },
    )
    for p_name, path_alias in path_aliases.items():
        setattr(result, p_name, path_alias)

    if lhs not in _type_intersection_cache:
        _type_intersection_cache[lhs] = weakref.WeakKeyDictionary()
    _type_intersection_cache[lhs][rhs] = result

    return result
