# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

from __future__ import annotations

import typing
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    ClassVar,
    Generic,
    NamedTuple,
    TypeVar,
    cast,
    final,
    overload,
)

from typing_extensions import (
    Self,
    TypeAliasType,
)

import functools
import sys
import uuid
import warnings
import weakref

import pydantic
import pydantic.fields
import pydantic_core
from pydantic import ConfigDict as ConfigDict
from pydantic import Field
from pydantic import PrivateAttr as PrivateAttr
from pydantic import computed_field as computed_field
from pydantic import field_serializer as field_serializer
from pydantic_core import core_schema as pydantic_schema

from pydantic._internal import _model_construction  # noqa: PLC2701
from pydantic._internal import _namespace_utils  # noqa: PLC2701

from gel._internal import _typing_eval
from gel._internal import _typing_inspect
from gel._internal import _typing_parametric as parametric
from gel._internal import _polyfills
from gel._internal import _qb
from gel._internal._reflection import SchemaPath as SchemaPath

from . import lists
from . import unsetid

if TYPE_CHECKING:
    from collections.abc import (
        Callable,
        Sequence,
    )


T = TypeVar("T")
T_co = TypeVar("T_co", covariant=True)


@final
class UnspecifiedType:
    """A type used as a sentinel for unspecified values."""


Unspecified = UnspecifiedType()


def edgeql(source: object) -> str:
    dunder = getattr(source, "__edgeql__", Unspecified)
    if dunder is Unspecified:
        raise TypeError(f"{type(source)} does not support __edgeql__ protocol")
    if not callable(dunder):
        raise TypeError(f"{type(source)}.__edgeql__ is not callable")
    value = dunder()
    if not isinstance(value, str):
        raise ValueError("{type(source)}.__edgeql__()")
    return value


class GelClassVar:
    pass


def _is_dunder(attr: str) -> bool:
    return attr.startswith("__") and attr.endswith("__")


def _module_ns_of(obj: object) -> dict[str, Any]:
    """Return the namespace of the module where *obj* is defined."""
    module_name = getattr(obj, "__module__", None)
    if module_name:
        module = sys.modules.get(module_name)
        if module is not None:
            return module.__dict__

    return {}


def _type_repr(t: type) -> str:
    if isinstance(t, type):
        if t.__module__ == "builtins":
            return t.__qualname__
        else:
            return f"{t.__module__}.{t.__qualname__}"
    else:
        return repr(t)


def _get_field_descriptor(cls: type, name: str) -> GelFieldDescriptor | None:
    for ancestor in cls.__mro__:
        desc = ancestor.__dict__.get(name, Unspecified)
        if desc is not Unspecified and isinstance(desc, GelFieldDescriptor):
            return desc

    return None


class _PathAlias:
    def __init__(self, origin: type, metadata: Path) -> None:
        self.__gel_origin__ = origin
        self.__gel_metadata__ = metadata

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self.__gel_origin__(*args, **kwargs)

    def __mro_entries__(self, bases: tuple[type, ...]) -> tuple[type, ...]:
        return (self.__gel_origin__,)

    def __getattr__(self, attr: str) -> Any:
        if "__gel_origin__" in self.__dict__ and not _is_dunder(attr):
            origin = self.__gel_origin__
            descriptor = _get_field_descriptor(origin, attr)
            if descriptor is not None:
                return descriptor.get(self)
            else:
                return getattr(origin, attr)
        else:
            raise AttributeError(attr)

    def __setattr__(self, attr: str, val: Any) -> None:
        if _is_dunder(attr):
            super().__setattr__(attr, val)
        else:
            setattr(self.__gel_origin__, attr, val)

    def __instancecheck__(self, obj: object) -> bool:
        return isinstance(obj, self.__gel_origin__)

    def __subclasscheck__(self, cls: type) -> bool:
        return issubclass(cls, self.__gel_origin__)

    def __dir__(self) -> list[str]:
        return dir(self.__gel_origin__)

    def __repr__(self) -> str:
        origin = _type_repr(self.__gel_origin__)
        metadata = repr(self.__gel_metadata__)
        return f"gel.models.PathAlias[{origin}, {metadata}]"

    def __edgeql__(self) -> str:
        return self.__gel_metadata__.__edgeql__()


def AnnotatedPath(origin: type, metadata: Path) -> _PathAlias:  # noqa: N802
    return _PathAlias(origin, metadata)


PathSource = TypeAliasType("PathSource", "Symbol | SchemaSet | Path")


class GelFieldDescriptor(GelClassVar):
    __slots__ = (
        "__gel_annotation__",
        "__gel_name__",
        "__gel_origin__",
        "__gel_resolved_type__",
    )

    @classmethod
    def _from_pydantic_field(
        cls,
        origin: type,
        name: str,
        field: pydantic.fields.FieldInfo,
    ) -> Self:
        if field.annotation is None:
            raise AssertionError(
                f"unexpected unnannotated model field: {name}"
            )
        return cls(origin, name, field.annotation)

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
        globalns = _module_ns_of(origin)
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
            and issubclass(typing.get_origin(t), BasePointer)
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
        owner: type[Any] | _PathAlias,
    ) -> Any:
        t = self.__gel_resolved_type__
        if t is None:
            t = self._try_resolve_type()
            if t is not None:
                self.__gel_resolved_type__ = t

        if t is None:
            return self
        else:
            source: PathSource
            if isinstance(owner, _PathAlias):
                source = owner.__gel_metadata__
            elif isinstance(owner, type) and issubclass(
                owner, GelModelMetadata
            ):
                source = SchemaSet(name=owner.__reflection__.name)
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


class Exclusive:
    pass


class Symbol(NamedTuple):
    symbol: str


class SchemaSet(NamedTuple):
    name: SchemaPath

    def __edgeql__(self) -> str:
        return "::".join(self.name.parts)


class GelModelMetadata:
    class __reflection__:  # noqa: N801
        id: ClassVar[uuid.UUID]
        name: ClassVar[SchemaPath]


class Path(NamedTuple):
    source: Symbol | SchemaSet | Path
    name: str
    is_lprop: bool

    def __edgeql__(self) -> str:
        steps = []
        current: PathSource = self
        while isinstance(current, Path):
            steps.append(current.name)
            current = current.source

        if not isinstance(current, SchemaSet):
            raise ValueError("Path does not start with a SourceSet")
        steps.append(current.__edgeql__())

        return ".".join(reversed(steps))


class GelType(GelClassVar, _qb.Expression):
    pass


class GelPrimitiveType(GelType):
    if TYPE_CHECKING:

        @overload
        def __get__(self, obj: None, objtype: type[Any]) -> type[Self]: ...

        @overload
        def __get__(self, obj: object, objtype: Any = None) -> Self: ...

        def __get__(
            self,
            obj: Any,
            objtype: Any = None,
        ) -> type[Self] | Self: ...


class BaseScalar(GelPrimitiveType):
    pass


class AnyTuple(GelPrimitiveType):
    pass


class AnyEnum(BaseScalar, _polyfills.StrEnum):
    pass


class Array(list[T], GelPrimitiveType):
    if TYPE_CHECKING:

        def __set__(self, obj: Any, value: Array[T] | Sequence[T]) -> None: ...

    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source_type: Any,
        handler: pydantic.GetCoreSchemaHandler,
    ) -> pydantic_core.CoreSchema:
        if _typing_inspect.is_generic_alias(source_type):
            args = typing.get_args(source_type)
            item_type = args[0]
            return pydantic_schema.list_schema(
                items_schema=handler.generate_schema(item_type),
                serialization=pydantic_schema.plain_serializer_function_ser_schema(
                    list,
                ),
            )
        else:
            return handler.generate_schema(source_type)


class PyTypeScalar(parametric.SingleParametricType[T_co]):
    if TYPE_CHECKING:

        def __set__(self, obj: Any, value: T_co) -> None: ...  # type: ignore [misc]

    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source_type: Any,
        handler: pydantic.GetCoreSchemaHandler,
    ) -> pydantic_core.CoreSchema:
        return pydantic_core.core_schema.no_info_after_validator_function(
            cls.type,
            handler(cls.type),
        )


class GelModelMeta(_model_construction.ModelMetaclass, type):
    __gel_class_registry__: ClassVar[
        weakref.WeakValueDictionary[uuid.UUID, type[Any]]
    ] = weakref.WeakValueDictionary()

    def __new__(
        mcls,
        name: str,
        bases: tuple[type[Any], ...],
        namespace: dict[str, Any],
        *,
        __gel_type_id__: uuid.UUID | None = None,
        **kwargs: Any,
    ) -> GelModelMeta:
        with warnings.catch_warnings():
            # Make pydantic shut up about attribute redefinition.
            warnings.filterwarnings(
                "ignore",
                message=r".*shadows an attribute in parent.*",
            )
            cls = cast(
                "type[pydantic.BaseModel]",
                super().__new__(mcls, name, bases, namespace, **kwargs),
            )

        for fname, field in cls.__pydantic_fields__.items():
            if fname in cls.__annotations__:
                col = GelFieldDescriptor._from_pydantic_field(
                    cls, fname, field
                )
                setattr(cls, fname, col)

        if __gel_type_id__ is not None:
            mcls.__gel_class_registry__[__gel_type_id__] = cls

        return cls  # type: ignore [return-value]

    def __setattr__(cls, name: str, value: Any, /) -> None:
        if name == "__pydantic_fields__":
            fields: dict[str, pydantic.fields.FieldInfo] = value
            for field in fields.values():
                fdef = field.default
                if (
                    isinstance(fdef, (GelClassVar, _PathAlias))
                    or (
                        _typing_inspect.is_annotated(fdef)
                        and isinstance(fdef.__origin__, GelClassVar)
                    )
                ):
                    field.default = pydantic_core.PydanticUndefined

        super().__setattr__(name, value)

    @classmethod
    def get_class_by_id(cls, tid: uuid.UUID) -> type[GelModel]:
        try:
            return cls.__gel_class_registry__[tid]
        except KeyError:
            raise LookupError(
                f"cannot find GelModel for object type id {tid}"
            ) from None


class GelModel(
    pydantic.BaseModel,
    GelModelMetadata,
    GelType,
    metaclass=GelModelMeta,
):
    model_config = pydantic.ConfigDict(
        json_encoders={uuid.UUID: str},
        validate_assignment=True,
        defer_build=True,
    )

    _p__id__: uuid.UUID = PrivateAttr()

    def __init__(self, /, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._p__id: uuid.UUID = unsetid.UNSET_UUID
        self._p____type__ = None

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, GelModel):
            return NotImplemented

        if self._p__id is None or other._p__id is None:
            return False
        else:
            return self._p__id == other._p__id

    def __hash__(self) -> int:
        if self._p__id is unsetid.UNSET_UUID:
            raise TypeError("Model instances without id value are unhashable")

        return hash(self._p__id)

    @classmethod
    def select(cls, /, **kwargs: bool | type[GelType]) -> type[Self]:
        return cls

    @classmethod
    def filter(cls, /, *args: Any, **kwargs: Any) -> type[Self]:
        return cls


class GelLinkModel(pydantic.BaseModel, metaclass=GelModelMeta):
    model_config = pydantic.ConfigDict(
        validate_assignment=True,
        defer_build=True,
    )


MT = TypeVar("MT", bound=GelModel, covariant=True)


class ProxyModel(GelModel, Generic[MT]):
    __proxy_of__: ClassVar[type[MT]]  # type: ignore [misc]
    _p__obj__: MT

    def __init__(self, obj: MT, /) -> None:
        object.__setattr__(self, "_p__obj__", obj)

    def __getattribute__(self, name: str) -> Any:
        model_fields = object.__getattribute__(self, "model_fields")
        if name in model_fields or name == "_p__id":
            base = object.__getattribute__(self, "_p__obj__")
            return getattr(base, name)
        return super().__getattribute__(name)

    def __setattr__(self, name: str, value: Any) -> None:
        model_fields = object.__getattribute__(self, "model_fields")
        if name in model_fields:
            # writing to a field: mutate the wrapped model
            base = object.__getattribute__(self, "_p__obj__")
            setattr(base, name, value)
        else:
            # writing anything else (including _proxied) is normal
            super().__setattr__(name, value)

    @classmethod
    def __pydantic_init_subclass__(cls, **kwargs: Any) -> None:
        super().__pydantic_init_subclass__(**kwargs)
        generic_meta = cls.__pydantic_generic_metadata__
        if generic_meta["origin"] is ProxyModel and generic_meta["args"]:
            cls.__proxy_of__ = generic_meta["args"][0]

    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source_type: Any,
        handler: pydantic.GetCoreSchemaHandler,
    ) -> pydantic_core.CoreSchema:
        if cls.__name__ == "ProxyModel" or cls.__name__.startswith(
            "ProxyModel[",
        ):
            return handler(source_type)
        else:
            return pydantic_schema.no_info_before_validator_function(
                cls,
                schema=handler.generate_schema(cls.__proxy_of__),
            )


#
# Metaclass for type __links__ namespaces.  Facilitates
# proper forward type resolution by raising a NameError
# instead of AttributeError when resolving names in its
# namespace, thus not confusing users of typing._eval_type
#
class LinkClassNamespaceMeta(type):
    def __getattr__(cls, name: str) -> Any:
        if name == "__isabstractmethod__":
            return False

        raise NameError(name)


class LinkClassNamespace(metaclass=LinkClassNamespaceMeta):
    pass


BT_co = TypeVar("BT_co", covariant=True)


class BasePointer(GelClassVar, Generic[T_co, BT_co]):
    if TYPE_CHECKING:

        def __get__(self, obj: None, objtype: type[Any]) -> type[T_co]: ...


class OptionalPointer(BasePointer[T_co, BT_co]):
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


ST = TypeVar("ST", bound=GelPrimitiveType, covariant=True)


class _OptionalProperty(OptionalPointer[ST, BT_co]):
    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source_type: Any,
        handler: pydantic.GetCoreSchemaHandler,
    ) -> pydantic_core.CoreSchema:
        if _typing_inspect.is_generic_alias(source_type):
            args = typing.get_args(source_type)
            return pydantic_schema.nullable_schema(
                handler.generate_schema(args[0])
            )
        else:
            return handler.generate_schema(source_type)


OptionalProperty = TypeAliasType(
    "OptionalProperty",
    "Annotated[_OptionalProperty[ST, BT_co], Field(default=None)]",
    type_params=(ST, BT_co),
)


BMT = TypeVar("BMT", bound=GelModel, covariant=True)
PT = TypeVar("PT", bound=ProxyModel[GelModel], covariant=True)


class _OptionalLink(OptionalPointer[MT, BMT]):
    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source_type: Any,
        handler: pydantic.GetCoreSchemaHandler,
    ) -> pydantic_core.CoreSchema:
        if _typing_inspect.is_generic_alias(source_type):
            args = typing.get_args(source_type)
            if issubclass(args[0], ProxyModel):
                return pydantic_schema.no_info_before_validator_function(
                    functools.partial(cls._validate, generic_args=args),
                    schema=handler.generate_schema(args[0]),
                )
            else:
                return handler.generate_schema(args[0])
        else:
            return handler.generate_schema(source_type)

    @classmethod
    def _validate(
        cls,
        value: Any,
        generic_args: tuple[type[Any], type[Any]],
    ) -> MT:
        mt, bmt = generic_args
        if isinstance(value, mt):
            return value  # type: ignore [no-any-return]
        elif isinstance(value, bmt):
            return mt(value)  # type: ignore [no-any-return]
        else:
            raise TypeError(
                f"could not convert {type(value)} to {mt.__name__}"
            )


OptionalLink = TypeAliasType(
    "OptionalLink",
    "Annotated[_OptionalLink[MT, MT], Field(default=None)]",
    type_params=(MT,),
)

OptionalLinkWithProps = TypeAliasType(
    "OptionalLinkWithProps",
    "Annotated[_OptionalLink[PT, MT], Field(default=None)]",
    type_params=(PT, MT),
)


class _UpcastingDistinctList(lists.DistinctList[MT], Generic[MT, BMT]):
    @classmethod
    def _check_value(cls, value: Any) -> MT:
        t = cls.type
        if isinstance(value, t):
            return value
        elif issubclass(t, ProxyModel) and isinstance(value, t.__proxy_of__):
            return t(value)  # type: ignore [return-value]

        raise ValueError(
            f"{cls!r} accepts only values of type {cls.type!r}, "
            f"got {type(value)!r}",
        )


class _MultiLinkMeta(type):
    _list_type: type[lists.DistinctList[GelModel | ProxyModel[GelModel]]]


class _MultiLink(BasePointer[MT, BMT], metaclass=_MultiLinkMeta):
    if TYPE_CHECKING:

        @overload
        def __get__(self, obj: None, objtype: type[Any]) -> type[MT]: ...

        @overload
        def __get__(
            self, obj: object, objtype: Any = None
        ) -> lists.DistinctList[MT]: ...

        def __get__(
            self,
            obj: Any,
            objtype: Any = None,
        ) -> type[MT] | lists.DistinctList[MT] | None: ...

        def __set__(self, obj: Any, value: Sequence[MT | BMT]) -> None: ...

    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source_type: Any,
        handler: pydantic.GetCoreSchemaHandler,
    ) -> pydantic_core.CoreSchema:
        if _typing_inspect.is_generic_alias(source_type):
            args = typing.get_args(source_type)
            item_type = args[0]
            return pydantic_schema.no_info_before_validator_function(
                functools.partial(cls._validate, generic_args=args),
                schema=pydantic_schema.list_schema(
                    items_schema=handler.generate_schema(item_type),
                ),
                serialization=pydantic_schema.plain_serializer_function_ser_schema(
                    list,
                ),
            )
        else:
            return handler.generate_schema(source_type)

    @classmethod
    def _validate(
        cls,
        value: Any,
        generic_args: tuple[type[Any], type[Any]],
    ) -> lists.DistinctList[MT]:
        lt: type[_UpcastingDistinctList[MT, BMT]] = _UpcastingDistinctList[
            generic_args[0],  # type: ignore [valid-type]
            generic_args[1],  # type: ignore [valid-type]
        ]
        if isinstance(value, lt):
            return value
        elif isinstance(value, (list, lists.DistinctList)):
            return lt(value)
        else:
            raise TypeError(
                f"could not convert {type(value)} to {cls.__name__}"
            )


MultiLink = TypeAliasType(
    "MultiLink",
    "Annotated[_MultiLink[MT, MT], Field(default_factory=lists.DistinctList)]",
    type_params=(MT,),
)

MultiLinkWithProps = TypeAliasType(
    "MultiLinkWithProps",
    "Annotated[_MultiLink[PT, MT], Field(default_factory=lists.DistinctList)]",
    type_params=(PT, MT),
)


class LazyClassProperty(Generic[T]):
    def __init__(
        self, meth: Callable[[type[Any]], T] | classmethod[Any, Any, T], /
    ) -> None:
        if isinstance(meth, classmethod):
            self._func = meth.__func__
        else:
            raise TypeError(
                f"{self.__class__.__name__} must be used to "
                f"decorate classmethods"
            )

    def __set_name__(self, owned: type[Any], name: str) -> None:
        self._name = name

    def __get__(self, instance: Any, owner: type[Any] | None = None) -> T:
        if owner is None:
            raise RuntimeError(
                f"{self.__class__.__name__} called on an instance not a class"
            )
        value = self._func(owner)
        setattr(owner, self._name, value)
        return value


class LazyLinkClassDef:
    def __init__(self, name: str) -> None:
        self._recursion_guard = False
        self._name = name

    def _define(self, name: str) -> type[Any]:
        raise NotImplementedError

    def __set_name__(self, owner: type[Any], name: str) -> None:
        self._name = name

    def __get__(
        self,
        instance: object | None,
        owner: type[Any] | None = None,
    ) -> Any:
        if instance is not None:
            raise AssertionError(
                "unexpected lazy class def access on containing "
                "class instance (not class)"
            )

        assert owner is not None

        fqname = f"{owner.__qualname__}.{self._name}"
        if self._recursion_guard:
            raise NameError(f"recursion while resolving {fqname}")

        self._recursion_guard = True

        try:
            defined = self._define(self._name)
        except AttributeError as e:
            raise NameError(f"cannot define {fqname} yet") from e
        finally:
            self._recursion_guard = False

        setattr(owner, self._name, defined)
        return defined
