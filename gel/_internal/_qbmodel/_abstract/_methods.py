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
    TypeNameExpr,
)
from gel._internal import _type_expression
from gel._internal._xmethod import classonlymethod

from ._base import AbstractGelModel, AbstractGelObjectBacklinksModel

from ._descriptors import (
    GelObjectBacklinksModelDescriptor,
    ModelFieldDescriptor,
    field_descriptor,
)
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

        # We pretend that the return type is _T_OtherModel so that the type
        # checker is aware of _T_OtherModel's pointers. We don't get Self's
        # pointers, but that's ok most of the time.
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


_T_Lhs = TypeVar("_T_Lhs", bound="AbstractGelModel")
_T_Rhs = TypeVar("_T_Rhs", bound="AbstractGelModel")


class BaseGelModelIntersection(
    BaseGelModel,
    _type_expression.Intersection,
    Generic[_T_Lhs, _T_Rhs],
):
    __gel_type_class__: ClassVar[type]

    lhs: ClassVar[type[AbstractGelModel]]
    rhs: ClassVar[type[AbstractGelModel]]


class BaseGelModelIntersectionBacklinks(
    AbstractGelObjectBacklinksModel,
    _type_expression.Intersection,
):
    lhs: ClassVar[type[AbstractGelObjectBacklinksModel]]
    rhs: ClassVar[type[AbstractGelObjectBacklinksModel]]


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
        type[BaseGelModelIntersection[AbstractGelModel, AbstractGelModel]],
    ],
] = weakref.WeakKeyDictionary()


def create_intersection(
    lhs: type[_T_Lhs],
    rhs: type[_T_Rhs],
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

    # Create the resulting intersection type
    result = type(
        f"({lhs.__name__} & {rhs.__name__})",
        (BaseGelModelIntersection,),
        {
            'lhs': lhs,
            'rhs': rhs,
            '__gel_reflection__': __gel_reflection__,
            "__gel_proxied_dunders__": frozenset(
                {
                    "__backlinks__",
                }
            ),
        },
    )

    # Generate field descriptors.
    descriptors: dict[str, ModelFieldDescriptor] = combine_dicts(
        {
            p_name: field_descriptor(result, p_name, path_alias.__gel_origin__)
            for p_name, p_refl in lhs.__gel_reflection__.pointers.items()
            if (
                hasattr(lhs, p_name)
                and (path_alias := getattr(lhs, p_name, None)) is not None
                and isinstance(path_alias, _qb.PathAlias)
            )
        },
        {
            p_name: field_descriptor(result, p_name, path_alias.__gel_origin__)
            for p_name, p_refl in rhs.__gel_reflection__.pointers.items()
            if (
                hasattr(rhs, p_name)
                and (path_alias := getattr(rhs, p_name, None)) is not None
                and isinstance(path_alias, _qb.PathAlias)
            )
        },
    )
    for p_name, descriptor in descriptors.items():
        setattr(result, p_name, descriptor)

    # Generate backlinks if required (they should generally be)
    if (lhs_backlinks := getattr(lhs, "__backlinks__", None)) and (
        rhs_backlinks := getattr(rhs, "__backlinks__", None)
    ):
        backlinks_model = create_intersection_backlinks(
            lhs_backlinks,
            rhs_backlinks,
            result,
            __gel_reflection__.type_name,
        )
        setattr(  # noqa: B010
            result,
            "__backlinks__",
            GelObjectBacklinksModelDescriptor[backlinks_model](),  # type: ignore [valid-type]
        )

    if lhs not in _type_intersection_cache:
        _type_intersection_cache[lhs] = weakref.WeakKeyDictionary()
    _type_intersection_cache[lhs][rhs] = result

    return result


def _order_base_types(lhs: type, rhs: type) -> tuple[type, ...]:
    if lhs == rhs:
        return (lhs,)
    elif issubclass(lhs, rhs):
        return (lhs, rhs)
    elif issubclass(rhs, lhs):
        return (rhs, lhs)
    else:
        return (lhs, rhs)


def create_intersection_backlinks(
    lhs_backlinks: type[AbstractGelObjectBacklinksModel],
    rhs_backlinks: type[AbstractGelObjectBacklinksModel],
    result: type[BaseGelModelIntersection[Any, Any]],
    result_type_name: TypeNameExpr,
) -> type[AbstractGelObjectBacklinksModel]:
    reflection = type(
        "__gel_reflection__",
        _order_base_types(
            lhs_backlinks.__gel_reflection__,
            rhs_backlinks.__gel_reflection__,
        ),
        {
            "name": result_type_name,
            "type_name": result_type_name,
            "pointers": (
                lhs_backlinks.__gel_reflection__.pointers
                | rhs_backlinks.__gel_reflection__.pointers
            ),
        },
    )

    # Generate field descriptors for backlinks.
    field_descriptors: dict[str, ModelFieldDescriptor] = combine_dicts(
        {
            p_name: field_descriptor(result, p_name, path_alias.__gel_origin__)
            for p_name in lhs_backlinks.__gel_reflection__.pointers
            if (
                hasattr(lhs_backlinks, p_name)
                and (path_alias := getattr(lhs_backlinks, p_name, None))
                is not None
                and isinstance(path_alias, _qb.PathAlias)
            )
        },
        {
            p_name: field_descriptor(result, p_name, path_alias.__gel_origin__)
            for p_name in rhs_backlinks.__gel_reflection__.pointers
            if (
                hasattr(rhs_backlinks, p_name)
                and (path_alias := getattr(rhs_backlinks, p_name, None))
                is not None
                and isinstance(path_alias, _qb.PathAlias)
            )
        },
    )

    backlinks = type(
        f"__{result_type_name.name}_backlinks__",
        (BaseGelModelIntersectionBacklinks,),
        {
            'lhs': lhs_backlinks,
            'rhs': rhs_backlinks,
            '__gel_reflection__': reflection,
            '__module__': __name__,
            **field_descriptors,
        },
    )

    return backlinks
