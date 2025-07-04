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

import inspect
import typing
import warnings
import weakref

import pydantic
import pydantic.fields
import pydantic_core
from pydantic_core import core_schema

from pydantic._internal import _model_construction  # noqa: PLC2701

from gel._internal import _qb
from gel._internal import _typing_inspect
from gel._internal import _utils
from gel._internal._unsetid import UNSET_UUID

from gel._internal._qbmodel import _abstract

from . import _utils as _pydantic_utils

if TYPE_CHECKING:
    import types
    import uuid

    from collections.abc import (
        Iterator,
        Iterable,
        Mapping,
        Set as AbstractSet,
    )
    from gel._internal._qbmodel._abstract import GelType


_model_pointers_cache: weakref.WeakKeyDictionary[
    type[GelModel], dict[str, type[GelType]]
] = weakref.WeakKeyDictionary()


class GelModelMeta(_model_construction.ModelMetaclass, _abstract.GelModelMeta):
    def __new__(  # noqa: PYI034
        mcls,
        name: str,
        bases: tuple[type[Any], ...],
        namespace: dict[str, Any],
        *,
        __gel_type_id__: uuid.UUID | None = None,
        __gel_variant__: str | None = None,
        __gel_root_class__: bool = False,
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
                super().__new__(
                    mcls,
                    name,
                    bases,
                    namespace | {"__gel_variant__": __gel_variant__},
                    **kwargs,
                ),
            )

        model_config = getattr(cls, "model_config", None)
        def_model_config = model_config == DEFAULT_MODEL_CONFIG
        if __gel_root_class__ and not def_model_config:
            # We have optimizations in __gel_model_construct__ that
            # assume model_config is DEFAULT_MODEL_CONFIG.
            # To prevent accidental breakage (by ourselves),
            # we raise an error if model_config is set to something else.
            raise TypeError(
                f"class {name}(__gel_root_class__=True) has a non-default"
                f"model config"
            )
        cls.__gel_default_model_config__ = def_model_config

        # Workaround for https://github.com/pydantic/pydantic/issues/11975
        for base in reversed(cls.__mro__[1:]):
            decinfos = base.__dict__.get("__pydantic_decorators__")
            if decinfos is None:
                try:
                    decinfos = type(cls.__pydantic_decorators__)()
                    base.__pydantic_decorators__ = decinfos  # type: ignore [attr-defined]
                except TypeError:
                    pass

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

        cls.__gel_variant__ = __gel_variant__

        return cls

    def __setattr__(cls, name: str, value: Any, /) -> None:  # noqa: N805
        if name == "__pydantic_fields__":
            cls = cast("type[GelModel]", cls)
            fields = _process_pydantic_fields(cls, value)
            super().__setattr__("__pydantic_fields__", fields)
            _model_pointers_cache.pop(cls, None)
        elif name == "__pydantic_parent_namespace__":
            # pydantic.ModelMetaclass blindly assumes that it is never
            # subclassed and that its __new__ is called directly from
            # the model definition site, which is not the case here,
            # so we need to rebuild the parent namespace weakvaluedict
            # with locals from the correct frame.
            #
            # To avoid repeating the above mistake, walk the stack
            # until we cross the __new__ boundary, i.e when we see
            # a frame that called __new__ but is itself not a __new__.
            defining_frame: types.FrameType | None = None
            stack_frame = inspect.currentframe()
            while stack_frame is not None:
                prev_frame = stack_frame.f_back
                if (
                    prev_frame is not None
                    and stack_frame.f_code.co_name == "__new__"
                    and prev_frame.f_code.co_name != "__new__"
                ):
                    defining_frame = prev_frame
                    break
                stack_frame = prev_frame

            if defining_frame is not None:
                if (
                    defining_frame.f_back is None
                    or defining_frame.f_code.co_name == "<module>"
                ):
                    value = None
                else:
                    value = _model_construction.build_lenient_weakvaluedict(
                        defining_frame.f_locals
                    )

            super().__setattr__(name, value)
        else:
            super().__setattr__(name, value)

    # Splat qb protocol
    def __iter__(cls) -> Iterator[_qb.ShapeElement]:  # noqa: N805
        cls = cast("type[GelModel]", cls)
        shape = _qb.get_object_type_splat(cls)
        return iter(shape.elements)

    def __gel_pointers__(cls) -> Mapping[str, type[GelType]]:  # noqa: N805
        cls = cast("type[GelModel]", cls)
        result = _model_pointers_cache.get(cls)
        if result is None:
            result = _resolve_pointers(cls)
            _model_pointers_cache[cls] = result

        return result

    # We don't need the complicated isinstance checking inherited
    # by Pydantic's ModelMetaclass from abc.Meta -- it's incredibly
    # slow. For GelModels we can just use the built-in
    # type.__instancecheck__ and type.__subclasscheck__. It's not
    # clear why an ABC-level "compatibility" would even be useful
    # for GelModels given how specialized they are.
    #
    # Context: without this, IMDBench's data loading takes 2x longer.
    #
    # Alternatively, we could just overload these for ProxyModel --
    # that's where most impact is. So if *you*, the reader of this code,
    # have a use case for supporting the broader isinstance/issubclass
    # semantics please onen an issue and let us know.
    __instancecheck__ = type.__instancecheck__  # type: ignore [assignment]
    __subclasscheck__ = type.__subclasscheck__  # type: ignore [assignment]


def _resolve_pointers(cls: type[GelSourceModel]) -> dict[str, type[GelType]]:
    if not cls.__pydantic_complete__ and cls.model_rebuild() is False:
        raise TypeError(f"{cls} has unresolved fields")

    pointers = {}
    for ptr_name in cls.__pydantic_fields__:
        descriptor = inspect.getattr_static(cls, ptr_name)
        if not isinstance(descriptor, _abstract.ModelFieldDescriptor):
            raise AssertionError(
                f"'{cls}.{ptr_name}' is not a ModelFieldDescriptor"
            )
        t = descriptor.get_resolved_type()
        if t is None:
            raise TypeError(
                f"the type of '{cls}.{ptr_name}' has not been resolved"
            )
        tgeneric = descriptor.get_resolved_pointer_descriptor()
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

        pointers[ptr_name] = t

    return pointers


_NO_DEFAULT = frozenset({pydantic_core.PydanticUndefined, None})
"""Field default values that signal that a schema default is to be used."""


def _process_pydantic_fields(
    cls: type[GelModel],
    fields: dict[str, pydantic.fields.FieldInfo],
) -> dict[str, pydantic.fields.FieldInfo]:
    for fn, field in fields.items():
        fdef = field.default
        overrides: dict[str, Any] = {}
        ptr = cls.__gel_reflection__.pointers.get(fn)

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
        else:
            field_infos = []

        fdef_is_desc = _qb.is_pointer_descriptor(fdef)
        if (
            ptr is not None
            and ptr.has_default
            and fn != "id"
            and (fdef_is_desc or fdef in _NO_DEFAULT)
            and all(
                fi.default in _NO_DEFAULT and fi.default_factory is None
                for fi in field_infos
            )
        ):
            overrides["default"] = _abstract.DEFAULT_VALUE
        elif fdef_is_desc:
            overrides["default"] = ...

        if (
            cls.__gel_variant__ is None
            and fn not in cls.__gel_reflection__.pointers
        ):
            # This is an ad-hoc computed pointer in a user-defined variant
            overrides["init"] = False
            overrides["frozen"] = True

        if overrides:
            field_attrs = dict(field._attributes_set)
            for override in overrides:
                field_attrs.pop(override, None)

            if field_infos:
                merged = pydantic.fields.FieldInfo.merge_field_infos(
                    field,
                    *field_infos,
                    **overrides,
                )
            else:
                merged = pydantic.fields.FieldInfo(
                    **field_attrs,  # type: ignore [arg-type]
                    **overrides,
                )

            fields[fn] = merged

    return fields


_unset: object = object()
_empty_str_set: frozenset[str] = frozenset()

# Low-level attribute functions
ll_setattr = object.__setattr__
ll_getattr = object.__getattribute__
ll_type_getattr = type.__getattribute__


DEFAULT_MODEL_CONFIG = pydantic.ConfigDict(
    validate_assignment=True,
    defer_build=True,
    extra="forbid",
)


class GelSourceModel(
    pydantic.BaseModel,
    _abstract.GelSourceModel,
    metaclass=GelModelMeta,
    __gel_root_class__=True,
):
    model_config = DEFAULT_MODEL_CONFIG

    # We use slots because PyDantic overrides `__dict__`
    # making state management for "special" properties like
    # these hard.
    __slots__ = ("__gel_changed_fields__",)

    if TYPE_CHECKING:
        # Set of fields that have been changed since the last commit;
        # used by `client.save()`.
        __gel_changed_fields__: set[str] | None

        # Whether the model uses DEFAULT_MODEL_CONFIG or not.
        __gel_default_model_config__: bool

    @classmethod
    def __gel_model_construct__(cls, __dict__: dict[str, Any] | None) -> Self:
        def_model_config = ll_type_getattr(
            cls,
            "__gel_default_model_config__",
        )
        if not def_model_config:
            # __gel_model_construct__ is much faster than model_construct,
            # but its because it's fine-tuned for *our* model_config and
            # for our specific use case. If a user subclasses one of
            # the models and, say, allows extra fields, then this
            # optimization will break pydantic, so in cases like this
            # we fall back to model_construct.
            if __dict__ is not None:
                return cls.model_construct(**__dict__)
            else:
                return cls.model_construct()

        self = cls.__new__(cls)
        if __dict__ is not None:
            ll_setattr(self, "__dict__", __dict__)
            ll_setattr(self, "__pydantic_fields_set__", set(__dict__.keys()))
        else:
            ll_setattr(self, "__pydantic_fields_set__", set())
        ll_setattr(self, "__pydantic_extra__", None)
        ll_setattr(self, "__pydantic_private__", None)
        ll_setattr(self, "__gel_changed_fields__", None)
        return self

    @classmethod
    def model_construct(
        cls, _fields_set: set[str] | None = None, **values: Any
    ) -> Self:
        self = super().model_construct(_fields_set, **values)
        ll_setattr(self, "__gel_changed_fields__", None)
        return self

    def __getstate__(self) -> dict[Any, Any]:
        state = super().__getstate__()
        state["__gel_changed_fields__"] = self.__gel_changed_fields__
        return state

    def __setstate__(self, state: dict[Any, Any]) -> None:
        super().__setstate__(state)
        self.__gel_changed_fields__ = state["__gel_changed_fields__"]

    def __copy__(self) -> Self:
        cp = super().__copy__()

        changed_fields = ll_getattr(self, "__gel_changed_fields__")
        ll_setattr(
            cp,
            "__gel_changed_fields__",
            set(changed_fields) if changed_fields is not None else None,
        )
        return cp

    def __deepcopy__(self, memo: dict[int, Any] | None = None) -> Self:
        cp = super().__deepcopy__(memo)

        changed_fields = ll_getattr(self, "__gel_changed_fields__")
        ll_setattr(
            cp,
            "__gel_changed_fields__",
            set(changed_fields) if changed_fields is not None else None,
        )
        return cp

    def model_copy(
        self, *, update: Mapping[str, Any] | None = None, deep: bool = False
    ) -> Self:
        # Mimicking pydantic.BaseModel.model_copy() implementation
        # but handling __gel_changed_fields__ specially.

        copied = self.__deepcopy__() if deep else self.__copy__()  # noqa: PLC2801
        if update:
            copied.__dict__.update(update)

            keys = update.keys()
            ll_getattr(copied, "__pydantic_fields_set__").update(keys)

            ch_fields = ll_getattr(copied, "__gel_changed_fields__")
            if ch_fields is not None:
                ch_fields.update(keys)
            else:
                ll_setattr(copied, "__gel_changed_fields__", set(keys))

        return copied

    def __init__(self, /, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        ll_setattr(
            self, "__gel_changed_fields__", set(self.__pydantic_fields_set__)
        )

    def __gel_get_changed_fields__(self) -> AbstractSet[str]:
        dirty: set[str] | None = ll_getattr(self, "__gel_changed_fields__")
        if dirty is None:
            return _empty_str_set
        return dirty

    def __gel_commit__(self) -> None:
        ll_setattr(self, "__gel_changed_fields__", None)

    def __setattr__(self, name: str, value: Any) -> None:
        if name not in self.__pydantic_fields__:
            super().__setattr__(name, value)
            return

        current_value = getattr(self, name, _unset)
        if value == current_value:
            return

        super().__setattr__(name, value)

        dirty: set[str] | None = object.__getattribute__(
            self, "__gel_changed_fields__"
        )
        if dirty is None:
            dirty = set()
            object.__setattr__(self, "__gel_changed_fields__", dirty)
        dirty.add(name)

    def __delattr__(self, name: str) -> None:
        # The semantics of 'del' isn't straightforward. Probably we should
        # disable deleting required fields, but then what do we do for optional
        # fields? Still delete them, or assign them to the default? The default
        # can be an EdgeQL expression in the schema, so this is where
        # the Python <> Gel interaction can get weird. So let's disable it
        # at least for now.
        raise NotImplementedError(
            'Gel models do not support the "del" operation'
        )


def _kwargs_exclude_id(
    id_: uuid.UUID, kwargs: dict[str, Any], /
) -> dict[str, Any]:
    if id_ is UNSET_UUID:
        try:
            exclude = kwargs["exclude"]
        except KeyError:
            kwargs["exclude"] = {"id"}
        else:
            if isinstance(exclude, set):
                exclude.add("id")
            else:
                assert isinstance(exclude, dict)
                exclude["id"] = True

    return kwargs


class GelModel(
    GelSourceModel,
    _abstract.GelModel,
    __gel_root_class__=True,
):
    if TYPE_CHECKING:
        id: uuid.UUID
        __gel_computed_fields__: ClassVar[frozenset[str] | None]

    @classmethod
    def __gel_gen_computed_fields__(cls) -> frozenset[str] | None:
        cls.model_rebuild()
        ret: set[str] = set()
        for field_name, field in cls.__pydantic_fields__.items():
            # ignore `field.init=None` - unset, so we're fine with it.
            if field.init is False:
                ret.add(field_name)
        ret.discard("id")
        comp_fields = frozenset(ret) if ret else None
        cls.__gel_computed_fields__ = comp_fields
        return comp_fields

    def __init__(
        self,
        /,
        *,
        id: uuid.UUID = UNSET_UUID,  # noqa: A002
        **kwargs: Any,
    ) -> None:
        cls = type(self)

        try:
            comp_fields = type.__getattribute__(cls, "__gel_computed_fields__")
        except AttributeError:
            comp_fields = cls.__gel_gen_computed_fields__()

        if id is not UNSET_UUID:
            raise ValueError(
                "models do not support setting `id` on construction; "
                "`id` is set automatically by the `client.save()` method"
            )

        # Prohibit passing computed fields to the constructor.
        # Unfortunately `init=False` doesn't work with BaseModel
        # pydantic classes (the docs states it only works in
        # dataclasses mode).
        #
        # We can potentially optimize this by caching a frozenset
        # of field names that are computed.
        if comp_fields is not None and (
            comp_args := comp_fields & kwargs.keys()
        ):
            comp_arg = next(iter(comp_args))
            raise ValueError(
                f"{cls.__qualname__} model does not accept {comp_arg!r} "
                f"argument, it is a computed field "
                f"(the database computes it for you)"
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
        if comp_fields is not None:
            pop = self.__dict__.pop
            for field_name in comp_fields:
                pop(field_name, None)

    def __getattr__(self, name: str) -> Any:
        cls = type(self)

        cls.model_rebuild()
        try:
            field = cls.__pydantic_fields__[name]
        except KeyError:
            # Not a field.
            #
            # We call `object.__getattribute__` here because we want to
            # the descriptor to be called and raise a proper error or
            # do something else.
            return object.__getattribute__(self, name)

        # We're accesing a field that was not set. Let's check if it's
        # a "multi link". If it is, we have to create an empty list,
        # so that the caller can append to it and save() later.
        # This happens when an object is fetched from the database
        # without the link specified, in which case the codec wouldn't
        # construct the list; we can do that lazily.

        if field.default_factory is list:
            # A multi link?
            ptrs = cls.__gel_reflection__.pointers
            ptr = ptrs.get(name)
            if ptr is not None and ptr.cardinality.is_multi():
                # Definitely a multi link.

                # This is a hack... we need to force Pydantic to apply its
                # `Field(validate_default=True)` logic and the safest way to
                # do that without using a whole bunch of private APIs is to
                # simply call `__setattr__` directly.
                pydantic.BaseModel.__setattr__(
                    self, name, field.get_default(call_default_factory=True)
                )
                # Fetch the validated/coerced value (`list` will be converted
                # to a variant of TrackedList.)
                return getattr(self, name)

        # Delegate to the descriptor.
        return object.__getattribute__(self, name)

    def __gel_is_new__(self) -> bool:
        return self.id is UNSET_UUID

    def __gel_commit__(self, new_id: uuid.UUID | None = None) -> None:
        if new_id is not None:
            if self.id is not UNSET_UUID:
                raise ValueError(
                    f"cannot set id on {self!r} after it has been set"
                )
            object.__setattr__(self, "id", new_id)

        super().__gel_commit__()

    def __eq__(self, other: object) -> bool:
        # We make two models equal to each other if they:
        #
        #   - both have the same *set* UUID (not UNSET_UUID)
        #     (ignoring differences in their data attributes)
        #
        #   - if they are both ProxyModels and wrap objects
        #     with equal *set* UUIDs.
        #
        #   - if one is a ProxyModel and the other is not
        #     if they wrap objects with equal *set* UUIDs,
        #     regardless of whether those proxies have
        #     different __linkprops__ or not.
        #
        # Why do we want equality by id?:
        #
        #   - In EdgeQL objects are compared by theid IDs only.
        #
        #   - It'd be hard to reason about / compare objects in
        #     Python code, unless objects are always fetched
        #     in the same way. This is the reason why all ORMs
        #     do the same.
        #
        #   - ProxyModels act as a fully transparent wrapper
        #     around GelModels. They are meant to be used as
        #     transitive objects acting exactly like the objects
        #     they wrap, PLUS having link properties data.
        #
        #   - ProxyModels have to be designed this way or
        #     refactoring schema becomes incredibly hard --
        #     adding the first link property to a link would
        #     change types and runtime behavior incompatibly
        #     in your Python code.
        if self is other:
            return True

        is_other_proxy = isinstance(other, ProxyModel)
        if not is_other_proxy and not isinstance(other, GelModel):
            return NotImplemented

        other_obj = cast(
            "GelModel",
            ll_getattr(other, "_p__obj__") if is_other_proxy else other,
        )

        if self is other_obj:
            return True

        return self.id == other_obj.id

    def __hash__(self) -> int:
        mid = self.id
        if mid is UNSET_UUID:
            raise TypeError("Model instances without id value are unhashable")
        return hash(mid)

    def __repr_name__(self) -> str:
        cls = type(self)
        return f"{cls.__module__}.{cls.__qualname__} <{id(self)}>"

    @_utils.inherit_signature(  # type: ignore [arg-type]
        pydantic.BaseModel.model_dump,
    )
    def model_dump(self, /, **kwargs: Any) -> dict[str, Any]:
        # We omit "id" from *unsaved* new objects when serialized.
        # While this isn't ideal (the field is "required") it's our best
        # defense against passing an unsaved object through an API boundary.
        # Out of all options:
        #   - return string "unset"
        #   - return invalid UUID or UUID with all-zero bytes
        #   - return "null"
        #   - not include "id" at all
        # the latter seems like the least bad option and easier to deal
        # with for the reciever when they validate the data.
        kwargs = _kwargs_exclude_id(self.id, kwargs)
        return super().model_dump(**kwargs)

    @_utils.inherit_signature(  # type: ignore [arg-type]
        pydantic.BaseModel.model_dump_json,
    )
    def model_dump_json(self, /, **kwargs: Any) -> str:
        kwargs = _kwargs_exclude_id(self.id, kwargs)
        return super().model_dump_json(**kwargs)


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


class GelLinkModel(
    GelSourceModel,
    __gel_root_class__=True,
):
    # Base class for __lprops__ classes.
    __slots__ = ()


_MT_co = TypeVar("_MT_co", bound=GelModel, covariant=True)


class ProxyModel(
    GelModel,
    Generic[_MT_co],
    __gel_root_class__=True,
):
    __slots__ = ("__linkprops__", "_p__obj__")

    __gel_proxied_dunders__: ClassVar[frozenset[str]] = frozenset(
        {
            "__linkprops__",
        }
    )

    if TYPE_CHECKING:
        _p__obj__: _MT_co

        __proxy_of__: ClassVar[type[_MT_co]]  # type: ignore [misc]
        __linkprops__: GelLinkModel
        __lprops__: ClassVar[type[GelLinkModel]]

    def __init__(self, /, **kwargs: Any) -> None:
        # We want ProxyModel to be a trasparent wrapper, so we
        # forward the constructor arguments to the wrapped object.
        wrapped = self.__proxy_of__(**kwargs)
        ll_setattr(self, "_p__obj__", wrapped)
        ll_setattr(
            self, "__linkprops__", self.__lprops__.__gel_model_construct__({})
        )

    @classmethod
    def link(cls, obj: _MT_co, /, **link_props: Any) -> Self:  # type: ignore [misc]
        proxy_of = ll_type_getattr(cls, "__proxy_of__")
        lprops_cls = ll_type_getattr(cls, "__lprops__")

        if type(obj) is not proxy_of:
            if isinstance(obj, ProxyModel):
                raise TypeError(
                    f"ProxyModel {cls.__qualname__} cannot wrap "
                    f"another ProxyModel {type(obj).__qualname__}"
                )
            if not isinstance(obj, proxy_of):
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
                    f"only instances of {proxy_of.__name__} "
                    f"are allowed, got {type(obj).__name__}",
                )

        self = cls.__new__(cls)

        lprops = lprops_cls(**link_props)
        ll_setattr(self, "__linkprops__", lprops)

        ll_setattr(self, "_p__obj__", obj)

        return self

    def __getattribute__(self, name: str) -> Any:
        if name in {
            "_p__obj__",
            "__linkprops__",
            "__proxy_of__",
            "__class__",
            "__lprops__",
            "model_dump",
            "model_dump_json",
            "__pydantic_serializer__",
        }:
            # Fast path for the wrapped object itself / linkprops model
            # (this optimization is informed by profiling model
            # instantiation and save() operation)
            return ll_getattr(self, name)

        if name == "id" or not name.startswith("_"):
            # Faster path for "public-like" attributes
            return ll_getattr(ll_getattr(self, "_p__obj__"), name)

        model_fields = type(self).__proxy_of__.model_fields
        if name in model_fields:
            base = ll_getattr(self, "_p__obj__")
            return getattr(base, name)

        return super().__getattribute__(name)

    def __setattr__(self, name: str, value: Any) -> None:
        if not name.startswith("_"):
            base = ll_getattr(self, "_p__obj__")
            setattr(base, name, value)
            return

        model_fields = type(self).__proxy_of__.model_fields
        if name in model_fields:
            # writing to a field: mutate the  wrapped model
            base = ll_getattr(self, "_p__obj__")
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
                cls,  # pyright: ignore [reportArgumentType]
                schema=handler.generate_schema(cls.__proxy_of__),
                serialization=core_schema.wrap_serializer_function_ser_schema(
                    lambda obj, _ser, info: obj.model_dump(
                        **_pydantic_utils.serialization_info_to_dump_kwargs(
                            info
                        )
                    ),
                    info_arg=True,
                    when_used="always",  # Make sure it's always used
                ),
            )

    @classmethod
    def __gel_proxy_construct__(
        cls,
        obj: _MT_co,  # type: ignore [misc]
        lprops: dict[str, Any],
    ) -> Self:
        pnv = cls.__gel_model_construct__(None)
        object.__setattr__(pnv, "_p__obj__", obj)
        object.__setattr__(
            pnv,
            "__linkprops__",
            cls.__lprops__.__gel_model_construct__(lprops),
        )
        return pnv

    def __eq__(self, other: object) -> bool:
        if self is other:
            return True

        is_other_proxy = isinstance(other, ProxyModel)
        if not is_other_proxy and not isinstance(other, GelModel):
            return NotImplemented

        other_obj = cast(
            "GelModel",
            ll_getattr(other, "_p__obj__") if is_other_proxy else other,
        )
        self_obj: GelModel = ll_getattr(self, "_p__obj__")

        if self_obj is other_obj:
            return True

        return self_obj.id == other_obj.id

    def __hash__(self) -> int:
        mid = ll_getattr(self, "_p__obj__").id
        if mid is UNSET_UUID:
            raise TypeError("Model instances without id value are unhashable")
        return hash(mid)

    def __repr_name__(self) -> str:
        cls = type(self)
        base_cls = cls.__bases__[0]
        return f"Proxy[{base_cls.__module__}.{base_cls.__qualname__}]"

    def __repr_args__(self) -> Iterable[tuple[str | None, Any]]:
        yield from self.__linkprops__.__repr_args__()
        yield from self._p__obj__.__repr_args__()

    @_utils.inherit_signature(  # type: ignore [arg-type]
        pydantic.BaseModel.model_dump,
    )
    def model_dump(self, /, **kwargs: Any) -> dict[str, Any]:
        kwargs = _kwargs_exclude_id(self.id, kwargs)
        wrapped: GelModel = ll_getattr(self, "_p__obj__")

        dump = wrapped.model_dump(**kwargs)

        # TODO: figure out how to pass exlude/include/etc
        # to the wrapped model. For now we just remove them
        # from the kwargs so that we don't exclude/include
        # linkprop with rules aimed at the wrapped model.
        kwargs.pop("exclude", None)
        kwargs.pop("include", None)

        dump["__linkprops__"] = self.__linkprops__.model_dump(**kwargs)
        return dump

    @_utils.inherit_signature(  # type: ignore [arg-type]
        pydantic.BaseModel.model_dump_json,
    )
    def model_dump_json(self, /, **kwargs: Any) -> str:
        kwargs = _kwargs_exclude_id(self.id, kwargs)
        return super().model_dump_json(**kwargs)

    def __getstate__(self) -> dict[Any, Any]:
        return {
            "obj": ll_getattr(self, "_p__obj__"),
            "linkprops": ll_getattr(self, "__linkprops__"),
        }

    def __setstate__(self, state: dict[Any, Any]) -> None:
        ll_setattr(self, "_p__obj__", state["obj"])
        ll_setattr(self, "__linkprops__", state["linkprops"])


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
