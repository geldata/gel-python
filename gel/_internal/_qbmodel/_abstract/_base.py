# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

from __future__ import annotations
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Generic,
    Final,
    TypeGuard,
    TypeVar,
    cast,
    final,
    overload,
)

from typing_extensions import Self

import functools
import typing
import weakref

from gel._internal import _qb
from gel._internal._xmethod import hybridmethod

if TYPE_CHECKING:
    import abc
    import types
    import uuid
    from collections.abc import Iterator


T = TypeVar("T")
T_co = TypeVar("T_co", covariant=True)


if TYPE_CHECKING:

    class GelTypeMeta(abc.ABCMeta):
        def __edgeql_qb_expr__(cls) -> _qb.Expr: ...

    class GelType(
        _qb.AbstractDescriptor,
        _qb.GelTypeMetadata,
        metaclass=GelTypeMeta,
    ):
        __gel_type_class__: ClassVar[type]

        def __edgeql_qb_expr__(self) -> _qb.Expr: ...

        @classmethod
        def __edgeql__(cls) -> tuple[type[Self], str]: ...

        @staticmethod
        def __edgeql_expr__() -> str: ...

        @overload
        def __get__(
            self, instance: None, owner: type[Any], /
        ) -> type[Self]: ...

        @overload
        def __get__(
            self, instance: Any, owner: type[Any] | None = None, /
        ) -> Self: ...

        def __get__(
            self,
            instance: Any | None,
            owner: type[Any] | None = None,
            /,
        ) -> type[Self] | Self: ...

else:

    class GelTypeMeta(_qb.CheckedContainsOpType, type):
        pass

    class GelType(
        _qb.AbstractDescriptor,
        _qb.GelTypeMetadata,
        metaclass=GelTypeMeta,
    ):
        @hybridmethod
        def __edgeql_qb_expr__(self) -> _qb.Expr:
            if isinstance(self, type):
                return _qb.ExprPlaceholder()
            else:
                return self.__edgeql_literal__()

        def __edgeql_literal__(self) -> _qb.Literal:
            raise NotImplementedError(
                f"{type(self).__name__}.__edgeql_literal__"
            )

        @hybridmethod
        def __edgeql__(self) -> tuple[type, str]:
            if isinstance(self, type):
                raise NotImplementedError(f"{type(self).__name__}.__edgeql__")
            else:
                return type(self), _qb.toplevel_edgeql(self)


_GelType_T = TypeVar("_GelType_T", bound=GelType)


class GelTypeConstraint(Generic[_GelType_T]):
    pass


def is_gel_type(t: Any) -> TypeGuard[type[GelType]]:
    return isinstance(t, type) and issubclass(t, GelType)


class AbstractGelSourceModel(_qb.GelSourceMetadata):
    """Base class for property-bearing classes."""

    if TYPE_CHECKING:
        # Whether the model is new (no `.id` set) or it has
        # an `.id` corresponding to a database object.
        __gel_new__: bool

    @classmethod
    def __gel_validate__(cls, value: Any) -> Self:
        raise NotImplementedError

    @classmethod
    def __gel_model_construct__(cls, __dict__: dict[str, Any] | None) -> Self:
        raise NotImplementedError


class AbstractGelModelMeta(GelTypeMeta):
    __gel_class_registry__: ClassVar[
        weakref.WeakValueDictionary[uuid.UUID, type[Any]]
    ] = weakref.WeakValueDictionary()

    if TYPE_CHECKING:
        # Splat qb protocol
        def __iter__(cls) -> Iterator[_qb.ShapeElement]: ...

    def __new__(
        mcls,
        name: str,
        bases: tuple[type[Any], ...],
        namespace: dict[str, Any],
        *,
        __gel_type_id__: uuid.UUID | None = None,
        __gel_shape__: str | None = None,
        **kwargs: Any,
    ) -> AbstractGelModelMeta:
        cls = cast(
            "type[AbstractGelModel]",
            super().__new__(mcls, name, bases, namespace, **kwargs),
        )
        if __gel_type_id__ is not None:
            mcls.__gel_class_registry__[__gel_type_id__] = cls
        cls.__gel_shape__ = __gel_shape__
        return cls

    @classmethod
    def get_class_by_id(cls, tid: uuid.UUID) -> type[AbstractGelModel]:
        try:
            return cls.__gel_class_registry__[tid]
        except KeyError:
            raise LookupError(
                f"cannot find GelModel for object type id {tid}"
            ) from None

    @classmethod
    def register_class(
        cls, tid: uuid.UUID, type_: type[AbstractGelModel]
    ) -> None:
        cls.__gel_class_registry__[tid] = cls


class AbstractGelModel(
    GelType,
    AbstractGelSourceModel,
    _qb.GelObjectTypeMetadata,
    metaclass=AbstractGelModelMeta,
):
    __gel_shape__: ClassVar[str | None] = None
    """Auto-reflected model variant marker."""

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()
        cls.__gel_shape__ = None

    @classmethod
    def __edgeql_qb_expr__(cls) -> _qb.Expr:  # pyright: ignore [reportIncompatibleMethodOverride]
        this_type = cls.__gel_reflection__.name
        return _qb.SchemaSet(type_=this_type)

    if TYPE_CHECKING:

        @classmethod
        def __edgeql__(cls) -> tuple[type[Self], str]: ...  # pyright: ignore [reportIncompatibleMethodOverride]

    else:

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


class AbstractGelLinkModel(AbstractGelSourceModel):
    pass


def is_gel_model(t: Any) -> TypeGuard[type[AbstractGelModel]]:
    return isinstance(t, type) and issubclass(t, AbstractGelModel)


def maybe_collapse_object_type_variant_union(
    t: types.UnionType,
) -> type[AbstractGelModel] | None:
    """If *t* is a Union of GelObjectType reflections of the same object
    type, find and return the first union component that is a default
    variant."""
    default_variant: type[AbstractGelModel] | None = None
    typename = None
    for union_arg in typing.get_args(t):
        if not is_gel_model(union_arg):
            # Not an object type reflection union at all!
            return None
        if typename is None:
            typename = union_arg.__gel_reflection__.name
        elif typename != union_arg.__gel_reflection__.name:
            # Reflections of different object types, cannot collapse.
            return None
        if union_arg.__gel_shape__ == "Default" and default_variant is None:
            default_variant = union_arg

    return default_variant


@final
class DefaultValue:
    def __repr__(self) -> str:
        return "<DEFAULT_VALUE>"


DEFAULT_VALUE: Final = DefaultValue()
"""Sentinel value indicating that the object should use the default value
from the schema for a pointer on which this is set."""
