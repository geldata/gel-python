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
    from collections.abc import Iterator


T = TypeVar("T")
T_co = TypeVar("T_co", covariant=True)


LITERAL_TAG_FIELDS = ('tname__',)


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

        # Set of fields that have been changed since the last commit;
        # used by `client.save()`.
        __gel_changed_fields__: set[str] | None

    @classmethod
    def __gel_validate__(cls, value: Any) -> Self:
        raise NotImplementedError

    @classmethod
    def __gel_model_construct__(cls, __dict__: dict[str, Any] | None) -> Self:
        raise NotImplementedError

    def __gel_get_changed_fields__(self) -> set[str]:
        raise NotImplementedError


class AbstractGelModelMeta(GelTypeMeta):
    __gel_class_registry__: ClassVar[
        weakref.WeakValueDictionary[str, type[Any]]
    ] = weakref.WeakValueDictionary()

    # Splat qb protocol
    def __iter__(cls) -> Iterator[_qb.ShapeElement]:
        cls = cast("type[AbstractGelModel]", cls)
        shape = _qb.get_object_type_splat(cls)
        return iter(shape.elements)

    def __new__(
        mcls,
        name: str,
        bases: tuple[type[Any], ...],
        namespace: dict[str, Any],
        *,
        __gel_shape__: str | None = None,
        **kwargs: Any,
    ) -> AbstractGelModelMeta:
        cls = cast(
            "type[AbstractGelModel]",
            super().__new__(mcls, name, bases, namespace, **kwargs),
        )
        reflection = cls.__gel_reflection__
        if (
            # The class registry only tracks the canonical base instances,
            # which are declared with __gel_is_canonical__ = True.
            (tname := getattr(reflection, "name", None)) is not None
            and namespace.get('__gel_is_canonical__')
        ):
            mcls.__gel_class_registry__[str(tname)] = cls
        else:
            cls.__gel_is_canonical__ = False
        cls.__gel_shape__ = __gel_shape__
        return cls

    @classmethod
    def get_class_by_name(cls, tname: str) -> type[AbstractGelModel]:
        try:
            return cls.__gel_class_registry__[tname]
        except KeyError:
            raise LookupError(
                f"cannot find GelModel for object type {tname}"
            ) from None


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
        this_type = cls.__gel_reflection__.type_name
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


class AbstractGelObjectBacklinksModel(
    AbstractGelSourceModel,
    _qb.GelTypeMetadata,
):
    if TYPE_CHECKING:
        # Whether the model was copied by reference and must
        # be copied by value before being accessed by the user.
        __gel_copied_by_ref__: bool

    class __gel_reflection__(  # noqa: N801
        _qb.GelSourceMetadata.__gel_reflection__,
        _qb.GelTypeMetadata.__gel_reflection__,
    ):
        pass

    @classmethod
    def __edgeql_qb_expr__(cls) -> _qb.Expr:  # pyright: ignore [reportIncompatibleMethodOverride]
        this_type = cls.__gel_reflection__.type_name
        return _qb.SchemaSet(type_=this_type)


class AbstractGelLinkModel(AbstractGelSourceModel):
    if TYPE_CHECKING:
        # Whether the model was copied by reference and must
        # be copied by value before being accessed by the user.
        __gel_copied_by_ref__: bool

        # Whether the model has mutable properties; determined
        # at the codegen time.
        __gel_has_mutable_props__: ClassVar[bool]


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
            typename = union_arg.__gel_reflection__.type_name
        elif typename != union_arg.__gel_reflection__.type_name:
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
