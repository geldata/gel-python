# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

"""Pydantic implementation of the query builder model"""

from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Generic,
    TypeVar,
    cast,
    overload,
)

from typing_extensions import (
    Self,
)

import dataclasses
import inspect
import typing
import uuid
import warnings
import weakref

import pydantic
import pydantic.fields
import pydantic_core
from pydantic_core import core_schema

from pydantic._internal import _model_construction  # noqa: PLC2701

from gel._internal import _edgeql
from gel._internal import _qb
from gel._internal import _lazyprop
from gel._internal import _typing_inspect
from gel._internal import _unsetid
from gel._internal import _utils

from gel._internal._qbmodel import _abstract

if TYPE_CHECKING:
    from typing import Type  # noqa: UP035
    from collections.abc import Iterator, Iterable


@dataclasses.dataclass(kw_only=True, frozen=True)
class Pointer:
    cardinality: _edgeql.Cardinality
    computed: bool
    has_props: bool
    kind: _edgeql.PointerKind
    name: str
    readonly: bool
    type: type[Any]

    @classmethod
    def from_ptr_info(
        cls,
        name: str,
        # We're using `Type[Any]` below because `type` is bound
        # to `type[Any]` a few lines above.
        type: Type[Any],  # noqa: UP006, A002
        kind: _edgeql.PointerKind,
        ptrinfo: _abstract.PointerInfo,
    ) -> Self:
        return cls(
            cardinality=ptrinfo.cardinality,
            computed=ptrinfo.computed,
            has_props=ptrinfo.has_props,
            kind=kind,
            name=name,
            readonly=ptrinfo.readonly,
            type=type,
        )


_model_pointers_cache: weakref.WeakKeyDictionary[
    type[GelModel], dict[str, Pointer]
] = weakref.WeakKeyDictionary()


class GelModelMeta(_model_construction.ModelMetaclass, _abstract.GelModelMeta):
    def __new__(  # noqa: PYI034
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
                "type[GelModel]",
                super().__new__(mcls, name, bases, namespace, **kwargs),
            )

        for fname, field in cls.__pydantic_fields__.items():
            if fname in cls.__annotations__:
                if field.annotation is None:
                    raise AssertionError(
                        f"unexpected unnannotated model field: {name}.{fname}"
                    )
                desc = _abstract.field_descriptor(cls, fname, field.annotation)
                setattr(cls, fname, desc)

        if __gel_type_id__ is not None:
            mcls.register_class(__gel_type_id__, cls)

        return cls

    def __setattr__(cls, name: str, value: Any, /) -> None:  # noqa: N805
        if name == "__pydantic_fields__":
            fields, ptr_infos = _process_pydantic_fields(cls, value)  # type: ignore [arg-type]
            super().__setattr__("__pydantic_fields__", fields)
            super().__setattr__("__gel_pointer_infos__", ptr_infos)
            _model_pointers_cache.pop(cls, None)  # type: ignore [call-overload]
        else:
            super().__setattr__(name, value)

    # Splat qb protocol
    def __iter__(cls) -> Iterator[_qb.PathAlias]:  # noqa: N805
        return iter(cls.__gel_eager_pointers__)

    @_lazyprop.LazyProperty[tuple[_qb.PathAlias, ...]]
    def __gel_eager_pointers__(cls) -> tuple[_qb.PathAlias, ...]:  # noqa: N805
        cls = cast("type[GelModel]", cls)
        ptrs = []
        for fname in cls.__pydantic_fields__:
            desc = _utils.maybe_get_descriptor(
                cls,
                fname,
                of_type=_abstract.ModelFieldDescriptor,
            )
            if desc is None:
                continue
            fgeneric = desc.get_resolved_type_generic()
            if fgeneric is None or issubclass(
                typing.get_origin(fgeneric), _abstract.AnyPropertyDescriptor
            ):
                ptrs.append(getattr(cls, fname))

        return tuple(ptrs)

    def __gel_pointers__(cls) -> dict[str, Pointer]:  # noqa: N805
        cls = cast("type[GelModel]", cls)
        result = _model_pointers_cache.get(cls)
        if result is None:
            result = _resolve_pointers(cls)
            _model_pointers_cache[cls] = result

        return result


def _resolve_pointers(cls: type[GelModel]) -> dict[str, Pointer]:
    if not cls.__pydantic_complete__ and cls.model_rebuild() is False:
        raise TypeError(f"{cls} has unresolved fields")

    pointers = {}
    for ptr_name, ptr_info in cls.__gel_pointer_infos__.items():
        descriptor = inspect.getattr_static(cls, ptr_name)
        if not isinstance(descriptor, _abstract.ModelFieldDescriptor):
            raise AssertionError(
                f"'{cls}.{ptr_name}' is not a ModelFieldDescriptor"
            )
        orig_t = t = descriptor.get_resolved_type()
        if t is None:
            raise TypeError(
                f"the type of '{cls}.{ptr_name}' has not been resolved"
            )
        tgeneric = descriptor.get_resolved_type_generic()
        if tgeneric is not None:
            torigin = typing.get_origin(tgeneric)
            if (
                issubclass(torigin, _abstract.PointerDescriptor)
                and (
                    resolve := getattr(torigin, "__gel_resolve_dlist__", None)
                )
                is not None
            ):
                args = typing.get_args(tgeneric)
                t = resolve(args)
                if t is None:
                    raise TypeError(
                        f"the type of '{cls}.{ptr_name}' has not been resolved"
                    )

        kind = (
            _edgeql.PointerKind.Link
            if isinstance(orig_t, GelModelMeta)
            else _edgeql.PointerKind.Property
        )
        pointers[ptr_name] = Pointer.from_ptr_info(
            ptr_name,
            t,
            kind,
            ptr_info,
        )

    return pointers


def _process_pydantic_fields(
    cls: type[GelModel],
    fields: dict[str, pydantic.fields.FieldInfo],
) -> tuple[
    dict[str, pydantic.fields.FieldInfo],
    dict[str, _abstract.PointerInfo],
]:
    ptr_infos_dict: dict[str, _abstract.PointerInfo] = {}

    for fn, field in fields.items():
        fdef = field.default
        overrides: dict[str, Any] = {}

        if isinstance(fdef, (_qb.AbstractDescriptor, _qb.PathAlias)) or (
            _typing_inspect.is_annotated(fdef)
            and isinstance(fdef.__origin__, _qb.AbstractDescriptor)
        ):
            field.default = pydantic_core.PydanticUndefined
            field._attributes_set.pop("default", None)
            overrides["default"] = ...

        anno = _typing_inspect.inspect_annotation(
            field.annotation,
            annotation_source=_typing_inspect.AnnotationSource.CLASS,
            unpack_type_aliases="lenient",
        )

        if _typing_inspect.is_generic_type_alias(field.annotation):
            field_infos = [
                a
                for a in anno.metadata
                if isinstance(a, pydantic.fields.FieldInfo)
            ]
            if field_infos:
                overrides["annotation"] = field.annotation

            ptr_infos = [
                a
                for a in anno.metadata
                if isinstance(a, _abstract.PointerInfo)
            ]
        else:
            field_infos = []
            ptr_infos = []

        num_ptr_infos = len(ptr_infos)
        if num_ptr_infos == 0:
            ptr_info = _abstract.PointerInfo()
        elif num_ptr_infos == 1:
            ptr_info = ptr_infos[0]
        else:
            ptr_info_kwargs: dict[str, Any] = {}
            for entry in ptr_infos:
                ptr_info_kwargs |= dataclasses.asdict(entry)
            ptr_info = _abstract.PointerInfo(**ptr_info_kwargs)

        ptr_infos_dict[fn] = ptr_info

        if overrides:
            if field_infos:
                merged = pydantic.fields.FieldInfo.merge_field_infos(
                    field,
                    *field_infos,
                    **overrides,
                )
            else:
                merged = pydantic.fields.FieldInfo(
                    **field._attributes_set,  # type: ignore [arg-type]
                    **overrides,
                )

            fields[fn] = merged

    return fields, ptr_infos_dict


class GelModel(
    pydantic.BaseModel,
    _abstract.GelModel,
    metaclass=GelModelMeta,
):
    model_config = pydantic.ConfigDict(
        json_encoders={uuid.UUID: str},
        validate_assignment=True,
        defer_build=True,
        extra="forbid",
    )

    __gel_pointer_infos__: ClassVar[dict[str, _abstract.PointerInfo]]

    if TYPE_CHECKING:
        id: uuid.UUID

        @classmethod
        def __edgeql__(cls) -> tuple[type[Self], str]: ...

    def __init__(self, /, **kwargs: Any) -> None:
        cls = type(self)

        # Prohibit passing computed fields to the constructor.
        # Unfortunately `init=False` doesn't work with BaseModel
        # pydantic classes (the docs states it only works in
        # dataclasses mode).
        #
        # We can potentially optimize this by caching a frozenset
        # of field names that are computed.
        has_computed_fields = False
        cls.model_rebuild()
        for field_name, field in cls.__pydantic_fields__.items():
            # ignore `field.init=None` - unset, so we're fine with it.
            if field.init is False:
                has_computed_fields = True
                if field_name in kwargs:
                    raise ValueError(
                        f"cannot set field {field_name!r} on {cls.__name__}"
                    )

        super().__init__(**kwargs)

        # This might be a bit too aggressive, but we want to clear
        # computed fields from the instance. Currently computed fields
        # use a custom validator that forces pydantic to auto-set
        # *required* computed fields to None, which in turn is what
        # allows us to not pass them in constructors. But we don't
        # want those None values to ever surface anywhere, be that
        # attribute access or serialization or anything else that
        # reads from __dict__.
        if has_computed_fields:
            for field_name, field in cls.__pydantic_fields__.items():
                # ignore `field.init=None` - unset, so we're fine with it.
                if field.init is False and field_name != "id":
                    self.__dict__.pop(field_name, None)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, GelModel):
            return NotImplemented

        if self.id is None or other.id is None:
            return False
        else:
            return self.id == other.id

    def __hash__(self) -> int:
        if self.id is _unsetid.UNSET_UUID:
            raise TypeError("Model instances without id value are unhashable")

        return hash(self.id)

    def __repr_name__(self) -> str:  # type: ignore [override]
        cls = type(self)
        return f"{cls.__module__}.{cls.__qualname__}"


_T_co = TypeVar("_T_co", covariant=True)


class LinkPropsDescriptor(Generic[_T_co]):
    @overload
    def __get__(self, obj: None, owner: type[Any]) -> type[_T_co]: ...

    @overload
    def __get__(
        self, obj: object, owner: type[Any] | None = None
    ) -> _T_co: ...

    def __get__(
        self, obj: Any, owner: type[Any] | None = None
    ) -> _T_co | type[_T_co]:
        if obj is None:
            assert owner is not None
            return owner.__lprops__  # type: ignore [no-any-return]
        else:
            return obj.__linkprops__  # type: ignore [no-any-return]


class GelLinkModel(pydantic.BaseModel, metaclass=GelModelMeta):
    model_config = pydantic.ConfigDict(
        validate_assignment=True,
        defer_build=True,
    )

    @classmethod
    def __descriptor__(cls) -> LinkPropsDescriptor[Self]:
        return LinkPropsDescriptor()


_MT_co = TypeVar("_MT_co", bound=GelModel, covariant=True)


class ProxyModel(GelModel, Generic[_MT_co]):
    __proxy_of__: ClassVar[type[_MT_co]]  # type: ignore [misc]
    __gel_proxied_dunders__: ClassVar[frozenset[str]] = frozenset(
        {
            "__linkprops__",
        }
    )

    _p__obj__: _MT_co

    def __init__(self, obj: _MT_co, /) -> None:
        if not isinstance(obj, self.__proxy_of__):
            # A long time of debugging revealed that it's very important to
            # check `obj` being of a correct type. Pydantic can instantiate
            # a ProxyModel with an incorrect type, e.g. when you pass
            # a list like `[1]` into a MultiLinkWithProps field --
            # Pydantic will try to wrap `[1]` into a list of ProxyModels.
            # And when it eventually fails to do so, everything is broken,
            # even error reporting and repr().
            #
            # Codegen'ed ProxyModel subclasses explicitly call
            # ProxyModel.__init__() in their __init__() methods to
            # make sure that this check is always performed.
            #
            # If it ever has to be removed, make sure to at least check
            # that `obj` is an instance of `GelModel`.
            raise ValueError(
                f"only instances of {self.__proxy_of__.__name__} are allowed, "
                f"got {type(obj).__name__}",
            )
        object.__setattr__(self, "_p__obj__", obj)

    def __getattribute__(self, name: str) -> Any:
        model_fields = type(self).__proxy_of__.model_fields
        if name in model_fields:
            base = object.__getattribute__(self, "_p__obj__")
            return getattr(base, name)
        return super().__getattribute__(name)

    def __setattr__(self, name: str, value: Any) -> None:
        model_fields = type(self).__proxy_of__.model_fields
        if name in model_fields:
            # writing to a field: mutate the  wrapped model
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
            return core_schema.no_info_before_validator_function(
                cls,
                schema=handler.generate_schema(cls.__proxy_of__),
            )

    def __repr_name__(self) -> str:  # type: ignore [override]
        cls = type(self)
        base_cls = cls.__bases__[0]
        return f"Proxy[{base_cls.__module__}.{base_cls.__qualname__}]"

    def __repr_args__(self) -> Iterable[tuple[str | None, Any]]:
        yield from self.__linkprops__.__repr_args__()
        yield from self._p__obj__.__repr_args__()


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
