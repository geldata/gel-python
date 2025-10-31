# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

"""Pydantic implementation of the query builder model"""

from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    TypeVar,
    cast,
)

from typing_extensions import (
    Self,
)

import copyreg
import inspect
import itertools
import types
import typing
import warnings
import weakref
import uuid
import sys

from collections.abc import Iterable

import pydantic
import pydantic.fields
import pydantic_core
from pydantic_core import core_schema

from pydantic._internal import _model_construction  # noqa: PLC2701
from pydantic._internal import _decorators  # noqa: PLC2701
from pydantic._internal import _core_utils as _pydantic_core_utils  # noqa: PLC2701

from gel._internal import _qb
from gel._internal import _tracked_list
from gel._internal import _typing_inspect
from gel._internal import _utils
from gel._internal._unsetid import UNSET_UUID

from gel._internal._qbmodel import _abstract

from . import _utils as _pydantic_utils
from . import _fields

if TYPE_CHECKING:
    from collections.abc import Mapping
    from gel._internal._qbmodel._abstract import GelType


_model_pointers_cache: weakref.WeakKeyDictionary[
    type[GelModel], dict[str, type[GelType]]
] = weakref.WeakKeyDictionary()


class GelModelMeta(
    _model_construction.ModelMetaclass,
    _abstract.AbstractGelModelMeta,
):
    if TYPE_CHECKING:
        __gel_cached_decorator_fields__: dict[
            str, _decorators.Decorator[pydantic.fields.ComputedFieldInfo]
        ]

    def __new__(  # noqa: PYI034
        mcls,
        name: str,
        bases: tuple[type[Any], ...],
        namespace: dict[str, Any],
        *,
        __gel_shape__: str | None = None,
        __gel_root_class__: bool = False,
        **kwargs: Any,
    ) -> GelModelMeta:
        if __gel_shape__ is None:
            # This is to make the top-level reflection of user-defined
            # types look less noisy. `__gel_shape__` is inferred from
            # the top-level __gel_default_shape__ attribute of the module
            # where the class is defined.
            #
            # This adds a negligible overhead to class creation, but:
            # we only do this for user-defined schema (std has explicit
            # __gel_shape__ class argument everywhere), and there shouldn't
            # be so many of user-defined Python subclasses for this to become
            # an issue.
            module = namespace.get("__module__")
            if (
                module
                and (mod := sys.modules.get(module))
                and (v := getattr(mod, "__gel_default_shape__", _unset))
                is not _unset
            ):
                assert isinstance(v, str)
                __gel_shape__ = v

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
                    namespace | {"__gel_shape__": __gel_shape__},
                    **kwargs,
                ),
            )

        # See the comment in mcls.__setattr__ where
        # we set __gel_cached_decorator_fields__.
        cls.__pydantic_decorators__.computed_fields = {
            **cls.__gel_cached_decorator_fields__,
            **cls.__pydantic_decorators__.computed_fields,
        }
        cls.__pydantic_computed_fields__ = {
            k: v.info
            for k, v in cls.__pydantic_decorators__.computed_fields.items()
        }
        del cls.__gel_cached_decorator_fields__

        # We have optimizations in __gel_model_construct__ that
        # assume model_config is DEFAULT_MODEL_CONFIG.
        # To prevent accidental breakage (by ourselves),
        # we raise an error if model_config is set to something else.
        model_config = getattr(cls, "model_config", None)
        def_model_config = model_config == DEFAULT_MODEL_CONFIG
        if __gel_root_class__ and not def_model_config:
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

        for fname, comp_field in cls.__pydantic_computed_fields__.items():
            if fname in cls.__annotations__:
                if comp_field.return_type is None:
                    raise AssertionError(
                        f"unexpected unnannotated model computed field: "
                        f"{name}.{fname}"
                    )
                desc = _abstract.field_descriptor(
                    cls, fname, comp_field.return_type
                )
                setattr(cls, fname, desc)

        reflection = cls.__gel_reflection__
        if (tname := getattr(reflection, "name", None)) is not None:
            mcls.__gel_class_registry__[tname] = cls

        cls.__gel_shape__ = __gel_shape__

        if not __gel_root_class__:
            cls.__gel_id_shape__ = _determine_gel_id_shape(cls)
        else:
            cls.__gel_id_shape__ = None

        return cls

    def __setattr__(cls, name: str, value: Any, /) -> None:  # noqa: N805
        if name == "__pydantic_fields__" and not cls.__dict__.get(
            "__pydantic_complete__"
        ):
            # We have to post-process `__pydantic_fields__` to
            # add computed fields and resolve annotations.
            # We only do this if the model is not complete yet,
            # allowing us to "plug" into the the logic of
            # `pydantic.set_model_fields()` function used by
            # the pydantic metaclass and model_rebuild().
            cls = cast("type[GelModel]", cls)
            _process_pydantic_fields(cls, value)
            _model_pointers_cache.pop(cls, None)
        elif (
            name == "__pydantic_parent_namespace__"
            and "__pydantic_fields__" not in cls.__dict__
        ):
            # Intercept the first assignment to __pydantic_parent_namespace__
            # (happens in `pydantic.ModelMetaclass.__new__`.)
            #
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

            ll_type_setattr(cls, name, value)

        elif (
            name == "__pydantic_decorators__"
            and "__pydantic_fields__" not in cls.__dict__
        ):
            # We intercept __pydantic_decorators__ to reset its
            # `computed_fields` attribute to an empty dict.
            # This happens during `pydantic.ModelMetaclass.__new__`;
            # this is how we can slightly alter its logic.
            #
            # This is to prevent Pydantic's "collect_model_fields()"
            # crashing when a field in __pydantic_fields__ is
            # shadowed by a computed field. In our case, there
            # is no shadowing, as computed field annotations
            # only exist in __pydantic_fields__ temporarily while
            # it's being built in the first place.
            # We restore the original computed_fields in
            # the metaclass, right after the call to
            # `super().__new__()`.
            assert isinstance(value, _decorators.DecoratorInfos)
            # Pydantic sets __pydantic_decorators__ at least twice
            # in __new__ (to the same value).
            if not hasattr(cls, "__gel_cached_decorator_fields__"):
                ll_type_setattr(
                    cls,
                    "__gel_cached_decorator_fields__",
                    value.computed_fields,
                )
            value.computed_fields = {}
            ll_type_setattr(cls, name, value)

        else:
            super().__setattr__(name, value)

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
    for ptr_name in itertools.chain(
        cls.__pydantic_fields__, cls.__pydantic_computed_fields__
    ):
        descriptor = inspect.getattr_static(cls, ptr_name)
        if not isinstance(descriptor, _abstract.ModelFieldDescriptor):
            # This is a regular Pydantic computed_field
            continue
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


_ID_SHAPES = frozenset(("NoId", "OptionalId", "RequiredId"))


def _determine_gel_id_shape(cls: type[GelModel]) -> str | None:
    if cls.__gel_shape__ == "Base":
        return None
    elif cls.__gel_shape__ in _ID_SHAPES:
        return cls.__gel_shape__
    else:
        id_shape: tuple[str, str] | None = None
        for base in cls.__bases__:
            base_id_shape = getattr(base, "__gel_id_shape__", None)
            if base_id_shape is not None:
                if id_shape is None:
                    id_shape = (base_id_shape, base.__qualname__)
                elif id_shape[0] != base_id_shape:
                    raise TypeError(
                        f"cannot inherit from {id_shape[1]} and "
                        f"{base.__qualname__} simultaneously: "
                        f"{id_shape[1]} is a {id_shape[0]} shape while "
                        f"{base.__qualname__} is a {base_id_shape} shape "
                        f"which are mutually incompatible"
                    )

        if id_shape is None:
            return None
        else:
            return id_shape[0]


_NO_DEFAULT = frozenset({pydantic_core.PydanticUndefined, None})
"""Field default values that signal that a schema default is to be used."""


def _new_computed_func(fn: str) -> property:
    # We don't use these (we have direct __dict__ access)
    # but maybe pydantic will, so let's have an idiomatic
    # pydantic-style computed property.

    def getter(self: GelModel) -> Any:
        return self.__dict__[fn]

    def setter(self: GelModel, value: Any) -> None:
        raise ValueError(f"computed field {fn} is read-only")

    return property(getter, setter)


def _process_pydantic_fields(
    cls: type[GelModel],
    fields: dict[str, pydantic.fields.FieldInfo],
) -> None:
    computeds: dict[str, pydantic.fields.ComputedFieldInfo] = {}
    new_fields: dict[str, pydantic.fields.FieldInfo] = {}

    for fn, field in fields.items():
        ptr = cls.__gel_reflection__.pointers.get(fn)
        if (
            (ptr is None or ptr.computed)
            and fn != "__linkprops__"
            and fn not in _abstract.LITERAL_TAG_FIELDS
        ):
            # Regarding `fn != "__linkprops__"`: see MergedModelMeta --
            # it renames `linkprops____` to `__linkprops__` to circumvent
            # Pydantic's restriction on fields starting with `_`.
            # In that case, the merged model does not have `__linkprops__`
            # in its pointers, but we still want it to be a regular
            # filed, not computed (for validation to work.)

            computeds[fn] = pydantic.fields.ComputedFieldInfo(
                wrapped_property=_new_computed_func(fn),
                return_type=field.annotation,
                alias=None,
                alias_priority=None,
                title=None,
                field_title_generator=None,
                description=None,
                deprecated=None,
                examples=None,
                json_schema_extra=None,
                # We'll include computeds manually in __repr_args__
                # or pydantic would crash with an AttributeError
                # on unfetched computed fields
                repr=False,
            )

            # Computeds will be excluded from __pydantic_fields__
            # and will flow into __pydantic_computed_fields__ and
            # __pydantic_decorators__ instead.
            continue

        fdef = field.default
        overrides: dict[str, Any] = {}

        field_anno: Any = field.annotation
        if isinstance(field_anno, type) and issubclass(field_anno, GelModel):
            # Required link -- inject the annotation.
            # Context: we want to have the less amount of noise in generated
            # files possible, so instead of:
            #
            #    class Type:
            #        another: RequiredLink[AnotherType]        # noqa: ERA001
            #
            # we generate:
            #
            #    class Type:
            #        another: AnotherType                      # noqa: ERA001
            #
            # But we do need to inject the required link annotation
            # so that pydantic can set up our custom serialization
            # for the field.
            field_anno = _fields.RequiredLink[field_anno]
            cls.__annotations__[fn] = field_anno
            overrides["annotation"] = field_anno

        elif isinstance(field_anno, type) and issubclass(
            field_anno, _abstract.GelPrimitiveType
        ):
            field_anno = _fields.Property[field_anno, field_anno]
            cls.__annotations__[fn] = field_anno
            overrides["annotation"] = field_anno

        anno = _typing_inspect.inspect_annotation(
            field_anno,
            annotation_source=_typing_inspect.AnnotationSource.CLASS,
            unpack_type_aliases="lenient",
        )

        pointer_info: _fields.PointerInfo | None = None
        if _typing_inspect.is_generic_type_alias(field_anno):
            for metadata in anno.metadata:
                if isinstance(metadata, _fields.PointerInfo):
                    if pointer_info is not None:
                        raise TypeError(
                            f"found multiple instances of PointerInfo in "
                            f"{cls.__qualname__}.{fn} annotation"
                        )
                    pointer_info = metadata

        if pointer_info is None:
            # This is not a Gel model pointer field
            new_fields[fn] = field
            continue

        fdef_is_desc = _qb.is_pointer_descriptor(fdef)
        if (
            ptr is not None
            and ptr.has_default
            and fn != "id"
            and (
                fdef_is_desc
                or fdef in _NO_DEFAULT
                or fdef is _abstract.DEFAULT_VALUE
            )
            and pointer_info.default in _NO_DEFAULT
            and pointer_info.default_factory is None
        ):
            # This field's default must come from the database --
            # `db.save()` must not set it to pydantic's default:
            # it must not include the field in the INSERT statement
            # if the field was not initialized with a value explicitly.
            overrides["default"] = _abstract.DEFAULT_VALUE
        else:
            # ... is a special Pydantic marker meaning "the field is required
            # but has no default"
            if pointer_info.default is not pydantic_core.PydanticUndefined:
                overrides["default"] = pointer_info.default
            else:
                has_factory = (
                    field.default_factory is not None
                    or pointer_info.default_factory is not None
                )
                is_ptr_optional = (
                    ptr is not None and ptr.cardinality.is_optional()
                )
                overrides["default"] = (
                    None if is_ptr_optional and not has_factory else ...
                )

        # This is a schema computed or an ad-hoc computed pointer in a
        # user-defined variant.
        if pointer_info.computed or (
            cls.__gel_shape__ is None and ptr is None and fn != "__linkprops__"
        ):
            # Regarding `fn != "__linkprops__"`: see MergedModelMeta --
            # it renames `linkprops____` to `__linkprops__` to circumvent
            # Pydantic's restriction on fields starting with `_`.
            # In that case we still want the `__linkprops__` field
            # to be writeable (for validation to work.)

            overrides["init"] = False
            overrides["frozen"] = True

        if pointer_info.default_factory is not None:
            overrides["default_factory"] = pointer_info.default_factory

        if pointer_info.validate_default is not None:
            overrides["validate_default"] = pointer_info.validate_default

        if overrides:
            field_attrs = dict(field._attributes_set)
            for override in overrides:
                field_attrs.pop(override, None)

            new_fields[fn] = pydantic.fields.FieldInfo(
                **field_attrs,  # type: ignore [arg-type]
                **overrides,
            )
        else:
            new_fields[fn] = field

    ll_type_setattr(cls, "__pydantic_fields__", new_fields)

    if computeds:
        # We can't just populate __pydantic_computed_fields__, sadly.
        # Pydantic wants __pydantic_decorators__ to have a record of
        # a `@computed_field` decorator for each computed field.
        # We have to mock it.
        decs = cls.__pydantic_decorators__
        for field_name, comp_field_info in computeds.items():
            func = comp_field_info.wrapped_property.fget
            dec = _decorators.Decorator(
                _pydantic_core_utils.get_type_ref(cls),
                cls_var_name=field_name,
                shim=None,
                info=comp_field_info,
                func=func,  # type: ignore [arg-type]
            )
            decs.computed_fields[field_name] = dec

        ll_type_setattr(
            cls,
            "__pydantic_computed_fields__",
            {
                **getattr(cls, "__pydantic_computed_fields__", {}),
                **computeds,
            },
        )


_unset: object = object()
_empty_str_set: frozenset[str] = frozenset()

# Low-level attribute functions
ll_setattr = object.__setattr__
ll_getattr = object.__getattribute__
ll_type_getattr = type.__getattribute__
ll_type_setattr = type.__setattr__


DEFAULT_MODEL_CONFIG = pydantic.ConfigDict(
    validate_assignment=True,
    defer_build=True,
    extra="forbid",
    serialize_by_alias=True,
    validate_by_name=False,
)


class GelSourceModel(
    pydantic.BaseModel,
    _abstract.AbstractGelSourceModel,
    metaclass=GelModelMeta,
    __gel_root_class__=True,
):
    model_config = DEFAULT_MODEL_CONFIG

    # We use slots because Pydantic overrides `__dict__`
    # making state management for "special" properties like
    # these hard. We also mess with `__dict__` so we want
    # these fields to be outside of it.
    __slots__ = ("__gel_changed_fields__", "__gel_new__")

    # Functions like __gel_model_construct__ are performance-critical,
    # we don't want to add another super() call to them, so we use
    # this class-level toggle to do some id-related work for GelModel,
    # and avoid doing it for ProxyModel / link props.
    __gel_has_id_field__: ClassVar[bool] = False

    if TYPE_CHECKING:
        # Whether the model uses DEFAULT_MODEL_CONFIG or not.
        __gel_default_model_config__: ClassVar[bool]

        # Explicitly declared shape for this model
        __gel_shape__: ClassVar[str | None]

        # Which Id shape this model inherits from.  Determines handling of
        # the id property.
        __gel_id_shape__: ClassVar[str | None]

    @classmethod
    def __gel_model_construct__(cls, __dict__: dict[str, Any] | None) -> Self:
        if (
            not ll_type_getattr(cls, "__pydantic_complete__")
            and cls.model_rebuild() is False
        ):
            # This will save a lot of debugging time.
            raise TypeError(f"{cls} has unresolved fields")

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
                self = cls.model_construct(**__dict__)
            else:
                self = cls.model_construct()
        else:
            self = object.__new__(cls)
            if __dict__ is not None:
                ll_setattr(self, "__dict__", __dict__)
                ll_setattr(
                    self, "__pydantic_fields_set__", set(__dict__.keys())
                )
            else:
                ll_setattr(self, "__pydantic_fields_set__", set())
            ll_setattr(self, "__pydantic_extra__", None)
            ll_setattr(self, "__pydantic_private__", None)
            ll_setattr(self, "__gel_changed_fields__", None)

        ll_setattr(self, "tname__", str(cls.__gel_reflection__.name))

        if cls.__gel_has_id_field__:
            mid = self.__dict__.get("id", _unset)
            assert mid is not UNSET_UUID
            ll_setattr(self, "__gel_new__", mid is _unset)

        return self

    @property
    def __tname__(self) -> str:
        return self.tname__  # type: ignore[no-any-return, attr-defined]

    @classmethod
    def model_construct(
        cls,
        _fields_set: set[str] | None = None,
        *,
        id: uuid.UUID = UNSET_UUID,  # noqa: A002
        **values: Any,
    ) -> Self:
        if id is UNSET_UUID:
            self = super().model_construct(_fields_set, **values)
        else:
            if not isinstance(id, uuid.UUID) and cls.__gel_has_id_field__:
                raise TypeError(f"id must be a UUID, got {type(id).__name__}")
            self = super().model_construct(_fields_set, id=id, **values)

        if cls.__gel_has_id_field__:
            self.__gel_new__ = id is UNSET_UUID

        __dict__ = self.__dict__

        pointer_ctrs = cls.__gel_pointers__()
        for name, val in __dict__.items():
            if not isinstance(val, list):
                continue
            try:
                ptr = cls.__gel_reflection__.pointers[name]
            except KeyError:
                continue
            if not ptr.cardinality.is_multi():
                continue

            wrapped: _tracked_list.AbstractCollection[Any]
            if not isinstance(val, _tracked_list.AbstractCollection):
                dlist_ctr = pointer_ctrs[name]
                assert issubclass(dlist_ctr, _tracked_list.AbstractCollection)
                wrapped = dlist_ctr(
                    val,
                    __wrap_list__=True,
                    __mode__=_tracked_list.Mode.ReadWrite,
                )
                __dict__[name] = wrapped
            else:
                wrapped = val

            if type(val) is _tracked_list.DefaultList or (
                not val and name not in values
            ):
                # The list is empty and it wasn't provided by the user
                # explicitly, so it must be coming from a default.
                # Adjuist the tracking flags accordingly.
                wrapped._mode = _tracked_list.Mode.Write
                wrapped.__gel_overwrite_data__ = False
            else:
                wrapped.__gel_overwrite_data__ = True
                wrapped._mode = _tracked_list.Mode.ReadWrite

        ll_setattr(
            self, "__gel_changed_fields__", set(self.__pydantic_fields_set__)
        )

        return self

    def __repr_args__(self) -> Iterable[tuple[str | None, Any]]:
        yielded = set()
        for arg, val in super().__repr_args__():
            yield arg, val
            yielded.add(arg)

        # Pydantic assumes computed fields are always present,
        # but that's not the case for GelModel, where they might
        # not be fetched. We don't want __repr__ to crash
        # with an AttributeError - that's a guaranteed bad debug
        # time.
        for name in self.__pydantic_computed_fields__:
            if name in yielded:
                continue
            val = getattr(self, name, _unset)
            if val is not _unset:
                yield name, val

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
            # `model_copy()` is supposed to be non-validating, but we can't
            # have that for GelModel, as things liks exact types of collections
            # of linked objects are very important for the API to function.
            # So we go the slow way and set every attribute, which will trigger
            # validation. There's no other way.
            for k, v in update.items():
                setattr(copied, k, v)
        return copied

    def __init__(self, /, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        ll_setattr(
            self, "__gel_changed_fields__", set(self.__pydantic_fields_set__)
        )

    def __gel_get_changed_fields__(self) -> set[str]:
        dirty: set[str] | None = ll_getattr(self, "__gel_changed_fields__")
        if dirty is None:
            return _empty_str_set  # type: ignore [return-value]
        return dirty

    def __gel_commit__(self) -> None:
        ll_setattr(self, "__gel_changed_fields__", None)

    def __setattr__(self, name: str, value: Any) -> None:
        # Implement state tracking and multi-link/multi-prop custom handling
        cls = type(self)
        ptr = cls.__gel_reflection__.pointers.get(name, None)

        try:
            if ptr is None or not ptr.cardinality.is_multi():
                # It's either:
                # * not a field defined by the database reflection;
                # * or a field, but not a multi-link/multi-prop.
                # In both cases, we can just delegate to pydantic's
                # __setattr__.
                try:
                    return super().__setattr__(name, value)
                except Exception as e:
                    if name in cls.__pydantic_computed_fields__:
                        raise AttributeError(
                            f"cannot set attribute on a computed field "
                            f"{name!r}"
                        ) from e
                    else:
                        raise

            # *multi links* and *multi props* are implemented with a special
            # collection type that tracks changes. We need to make sure
            # that the user can't override the collection with assignments and
            # mess up the state tracking.

            current_value = getattr(self, name, _unset)
            if current_value is value:
                # Operations like `+=` and `-=` would cause `__setattr__`
                # to be called, in which case we just silently return.
                return

            if not isinstance(value, Iterable):
                raise TypeError(
                    f"cannot assign values of type {type(value).__name__!r} "
                    f"to {type(self).__name__}.{name}; an iterable is expected"
                )

            assert isinstance(current_value, _tracked_list.AbstractCollection)
            current_value.__gel_replace_with_empty__()
            if value:
                current_value.__gel_extend__(
                    value.__gel_basetype_iter__()
                    if isinstance(value, _tracked_list.AbstractCollection)
                    else value
                )

            # Ensure compatibility with pydantic's __setattr__
            self.__pydantic_fields_set__.add(name)

            # A new list is assigned to a multi-link/multi-prop field
            # signalling that the data should be replaced with new
            # data on save.
            current_value.__gel_overwrite_data__ = True
            current_value._mode = _tracked_list.Mode.ReadWrite

        finally:
            if ptr is not None:
                dirty: set[str] | None = ll_getattr(
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

    @classmethod
    def __gel_validate__(cls, value: Any) -> GelSourceModel:
        # __gel_validate__ is called when validating a gel type
        # reached by a link, and Gel links may point to subtypes of
        # the declared type.  We dispatch on that ourselves, using a
        # __tname__ field in the dict.
        ccls = cls
        key = '__tname__'
        if isinstance(value, dict) and key in value:
            ncls: Any = cls.get_class_by_name(value[key])
            if ccls is not ncls:
                # For proxy models, we need an appropriate proxy.
                if issubclass(ccls, ProxyModel):
                    ccls = ccls._get_subtype_proxy(ncls)
                else:
                    ccls = ncls

        res = ccls.model_validate(value)

        return res


class GelModel(
    GelSourceModel,
    _abstract.BaseGelModel,
    __gel_root_class__=True,
):
    __slots__ = ()

    __gel_has_id_field__: ClassVar[bool] = True

    if TYPE_CHECKING:
        __gel_new__: bool
        __gel_custom_serializer__: ClassVar[pydantic_core.SchemaSerializer]

    def __new__(
        cls,
        /,
        id: uuid.UUID = UNSET_UUID,  # noqa: A002
        **kwargs: Any,
    ) -> Self:
        if cls.__gel_reflection__.abstract:
            raise TypeError(
                f"cannot instantiate abstract type "
                f"{cls.__module__}.{cls.__qualname__}"
            )

        if id is UNSET_UUID:
            # No 'id' argument: new object, just follow the normal
            # __new__ / __init__ machinery.
            # UNSET_UUID is a transient value that will not be set
            # on the object.
            new = super().__new__
            if new is object.__new__:
                return new(cls)
            else:
                return new(cls, **kwargs)

        if type(id) is not uuid.UUID or not isinstance(id, uuid.UUID):
            id = _pydantic_utils.validate_id(id)  # noqa: A001

        # We allow creating a model with an id. In that case, we assume
        # that the model is bound to an existing object in the database.
        # We will create the object in `__new__`, set fields, including
        # the `id` and return. The `__init__` will see that the `id` is
        # set and skip the initialization.
        #
        # All set fields are considered "dirty" and will be saved to the
        # database when `client.save()` is called.

        # Using __gel_model_construct__ here makes objects created with
        # an `id` to behave just like objects fetched from the db.
        # E.g. if a multi-pointer wasn't specified it wouldn't be set
        # (just like it wouldn't be set if it wasn't fetched). This makes
        # Write/ReadWrite modes work the same way for both cases.
        self = cls.__gel_model_construct__(None)

        validator = cls.__pydantic_validator__
        for arg, value in kwargs.items():
            arg = _pydantic_utils.GEL_ALIASES.get(arg, arg)  # noqa: PLW2901  # ignoring it in ruff.toml failed?
            # Faster than setattr()
            validator.validate_assignment(self, arg, value)

        self.__gel_changed_fields__ = set(self.__pydantic_fields_set__)
        self.__dict__["id"] = id
        self.__gel_new__ = False
        return self

    def __init__(
        self,
        /,
        id: uuid.UUID = UNSET_UUID,  # noqa: A002
        **kwargs: Any,
    ) -> None:
        has_id_field = "id" in type(self).__pydantic_fields__
        if has_id_field:
            if id is not UNSET_UUID:
                if self.__dict__.get("id", _unset) is _unset:
                    raise ValueError(
                        "models do not support setting `id` on construction; "
                        "`id` is set automatically by the `client.save()` "
                        "method"
                    )
                else:
                    # The object was created and initialized by __new__,
                    # nothing to do in `__init__`.
                    return

            super().__init__(id=id, **kwargs)

            # UNSET_UUID is a transient value, it must not leak to user code.
            marker = self.__dict__.pop("id")
            assert marker is UNSET_UUID
        else:
            if id is not UNSET_UUID:
                raise ValueError(
                    f"unexpecged `id` argument to constructor of "
                    f"{type(self).__qualname__}, which is a "
                    f"{type(self).__gel_id_shape__} shape"
                )

            super().__init__(**kwargs)

        self.__gel_new__ = True

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
            return ll_getattr(self, name)

        # We're accesing a field that was not set. Let's check if it's
        # a "multi link". If it is, we have to create an empty list,
        # so that the caller can append to it and save() later.
        # This happens when an object is fetched from the database
        # without the link specified, in which case the codec wouldn't
        # construct the list; we can do that lazily.

        if field.default_factory is _tracked_list.DefaultList:
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
                # to a variant of AbstractCollection.)
                lst: _tracked_list.AbstractCollection[Any] = getattr(
                    self, name
                )
                lst._mode = _tracked_list.Mode.Write
                # This list was created on demand to allow append/remove
                # operations. We don't want to *replace* the data with
                # elements from this list, we want to update it by
                # adding/removing elements.
                lst.__gel_overwrite_data__ = False
                return lst

        # Delegate to the descriptor.
        return ll_getattr(self, name)

    def __gel_commit__(
        self,
        *,
        new_id: uuid.UUID | None = None,
    ) -> None:
        if new_id is not None:
            if not self.__gel_new__:
                raise ValueError(
                    f"cannot set id on {self!r} after it has been set"
                )
            self.__gel_new__ = False
            self.__dict__["id"] = new_id

        assert not self.__gel_new__
        super().__gel_commit__()

    def __eq__(self, other: object) -> bool:
        # We make two models equal to each other if they:
        #
        #   - both have the same *set* UUID (not __gel_new__)
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

        if self.__gel_new__ or other_obj.__gel_new__:
            return False

        if not self.__gel_id_shape__ or not other_obj.__gel_id_shape__:
            return False

        return self.id == other_obj.id  # type: ignore [no-any-return]

    def __hash__(self) -> int:
        if self.__gel_new__:
            raise TypeError("Model instances without id value are unhashable")
        return hash(self.id)

    def __repr_name__(self) -> str:
        cls = type(self)
        return f"{cls.__module__}.{cls.__qualname__} <{id(self)}>"

    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source_type: Any,
        handler: pydantic.GetCoreSchemaHandler,
    ) -> pydantic_core.CoreSchema:
        # We want GelModel's custom model_dump() to be always called,
        # no matter how the dump is happening:
        #
        # - when caled directly
        # - when called via another GelModel's model_dump()
        # - when called via a generic pydantic.BaseModel's
        #   model_dump().
        #
        # The latter turns out to be a very tricky scenario.
        # Pydantic's `__get_pydantic_core_schema__` is intended to
        # be defined on non-pydantic types to customize their
        # serialization and validation. When defined on a model
        # it can work, as long as the schema isn't built for the
        # model itself but one of its nested attributes. If one
        # attempts to customize the schema or rendering of the
        # model itself pydantic crashes with RecursionError.
        #
        # Moreover, pydantic_core does not call overloaded
        # model_dump() methods, it instead proceeds to serialize
        # model's __dict__ according to its schema. I guess to
        # make things faster.
        #
        # E.g. if we just have a vanilla model_dump() implementation,
        # the serialization strategy we define here would unfold as
        # follows:
        #
        #  (a) model_dump() is called -> it runs
        #      __pydantic_serializer__.to_python
        #  (b) the serializer looks into the schema, finds the
        #      custom serializer we defined - calls it
        #  (c) we're back to where we started -- the loop.
        #
        # Using the `@model_serializer(mode="wrap")` decorator
        # leads to the exactly same rabbit hole.
        #
        # It all would be much simpler if pydantic_core exposed
        # the default "dump" implementation -- we'd simply call it
        # from our custom model_dump().
        #
        # There simply appears to be no "official" way to customize
        # the model's model_dump() behavior.
        #
        # So we cheat. In `GelModel.model_dump()` we create an
        # ad-hoc model subtype (see `__build_custom_serializer`)
        # of the current `type(self)` and **replace** this
        # very implementation of `__get_pydantic_core_schema__`
        # with a vanilla one. We then get its __pydantic_serializer__
        # and use it as a "default" serializer.
        #
        # This way we can customize our dump (ignoring missing
        # computeds, handling missing `.id` for unsaved models,
        # etc etc.)

        schema = handler(source_type)
        schema["serialization"] = (  # type: ignore [index]
            core_schema.wrap_serializer_function_ser_schema(
                lambda obj, _ser, info: obj.model_dump(
                    **_pydantic_utils.serialization_info_to_dump_kwargs(info)
                ),
                info_arg=True,
                when_used="always",
            )
        )
        return schema

    @classmethod
    def __build_custom_serializer(
        cls,
    ) -> pydantic_core.SchemaSerializer:
        # See the comment in __get_pydantic_core_schema__

        try:
            return cls.__dict__["__gel_custom_serializer__"]  # type: ignore [no-any-return]
        except KeyError:
            pass

        new_cls: type[GelModel] = type(
            cls.__name__,
            (cls,),
            {
                "__get_pydantic_core_schema__": classmethod(
                    lambda cls, source_type, handler: handler(source_type)
                ),
                "__gel_is_custom_serializer__": True,
            },
        )

        ser = new_cls.__pydantic_serializer__
        cls.__gel_custom_serializer__ = ser
        return ser

    @_utils.inherit_signature(  # type: ignore [arg-type]
        _pydantic_utils.model_dump_signature,
    )
    def model_dump(
        self,
        /,
        *,
        context: _pydantic_utils.GelDumpContext | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        _pydantic_utils.massage_model_dump_kwargs(
            self,
            caller="model_dump",
            kwargs=kwargs,
            context=context,
        )

        # See the comment in __get_pydantic_core_schema__
        ser = self.__build_custom_serializer()
        dump = ser.to_python(self, context=context, **kwargs)
        return dump  # type: ignore [no-any-return]

    @_utils.inherit_signature(  # type: ignore [arg-type]
        _pydantic_utils.model_dump_json_signature,
    )
    def model_dump_json(
        self,
        /,
        *,
        context: _pydantic_utils.GelDumpContext | None = None,
        **kwargs: Any,
    ) -> str:
        _pydantic_utils.massage_model_dump_kwargs(
            self,
            caller="model_dump_json",
            kwargs=kwargs,
            context=context,
        )
        return super().model_dump_json(context=context, **kwargs)

    def __getstate__(self) -> dict[Any, Any]:
        state = super().__getstate__()
        state["__gel_new__"] = self.__gel_new__
        return state

    def __setstate__(self, state: dict[Any, Any]) -> None:
        super().__setstate__(state)
        self.__gel_new__ = state["__gel_new__"]

    def __copy__(self) -> Self:
        cp = super().__copy__()
        ll_setattr(cp, "__gel_new__", ll_getattr(self, "__gel_new__"))
        return cp

    def __deepcopy__(self, memo: dict[int, Any] | None = None) -> Self:
        cp = super().__deepcopy__(memo)
        ll_setattr(cp, "__gel_new__", ll_getattr(self, "__gel_new__"))
        return cp

    def model_copy(
        self, *, update: Mapping[str, Any] | None = None, deep: bool = False
    ) -> Self:
        copied = super().model_copy(update=update, deep=deep)
        if update:
            ll_setattr(copied, "__gel_new__", ll_getattr(self, "__gel_new__"))
        return copied


class GelObjectModel(
    GelModel,
    _abstract.AbstractGelObjectModel,
    __gel_root_class__=True,
):
    # Base class for object classes.
    __gel_proxied_dunders__: ClassVar[frozenset[str]] = frozenset(
        {
            "__backlinks__",
        }
    )


class GelObjectBacklinksModel(
    GelSourceModel,
    _abstract.AbstractGelObjectBacklinksModel,
    __gel_root_class__=True,
):
    # Base class for __backlinks__ classes.
    __slots__ = ("__gel_copied_by_ref__",)

    def __getstate__(self) -> dict[Any, Any]:
        state = super().__getstate__()
        state["__gel_copied_by_ref__"] = getattr(
            self, "__gel_copied_by_ref__", False
        )
        return state

    def __setstate__(self, state: dict[Any, Any]) -> None:
        super().__setstate__(state)
        self.__gel_copied_by_ref__ = state["__gel_copied_by_ref__"]


class GelLinkModel(
    GelSourceModel,
    _abstract.AbstractGelLinkModel,
    __gel_root_class__=True,
):
    # Base class for __linkprops__ classes.
    __slots__ = ("__gel_copied_by_ref__",)

    def __getstate__(self) -> dict[Any, Any]:
        state = super().__getstate__()
        state["__gel_copied_by_ref__"] = getattr(
            self, "__gel_copied_by_ref__", False
        )
        return state

    def __setstate__(self, state: dict[Any, Any]) -> None:
        super().__setstate__(state)
        self.__gel_copied_by_ref__ = state["__gel_copied_by_ref__"]


_MT_co = TypeVar("_MT_co", bound=GelModel, covariant=True)


class _MergedModelMeta(GelModelMeta):
    def __setattr__(cls, name: str, value: Any, /) -> None:  # noqa: N805
        # We really need a "__linkprops__" field despite Pydantic's
        # restriction on fields starting with `_`. So we rename it
        # with force.

        if name == "__pydantic_fields__" and not cls.__dict__.get(
            "__pydantic_complete__"
        ):
            if "linkprops____" in value:
                value["__linkprops__"] = value.pop("linkprops____")
            if "linkprops____" in cls.__annotations__:
                cls.__annotations__["__linkprops__"] = cls.__annotations__.pop(
                    "linkprops____"
                )
        super().__setattr__(name, value)


class _MergedModelBase(
    GelModel,
    metaclass=_MergedModelMeta,
    __gel_root_class__=True,
):
    # Used exclusively by ProxyModel.__gel_proxy_make_merged_model__.
    if TYPE_CHECKING:
        __gel_new__: bool
        __gel_changed_fields__: set[str] | None

    # This is tricky: we have custom __get_pydantic_core_schema__ &
    # model_dump() in this class to shadow GelModel's respective
    # implementations. If we don't do this, GelModel would build
    # a subclass of this class to get a custom serializer and
    # our `__linkprops__` attribute will disappear (it's not
    # a valid pydantic name). Good news is that nobody will
    # ever be able to call this class' model_dump() implementation
    # directly so we're good.

    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source_type: Any,
        handler: pydantic.GetCoreSchemaHandler,
    ) -> pydantic_core.CoreSchema:
        return handler(source_type)

    def model_dump(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return pydantic.BaseModel.model_dump(self, *args, **kwargs)


def _pickle_dynamic_proxy_model(cls: Any) -> Any:
    # See discussion in _typing_parametric. We do the same tricks as
    # PickleableClassParametricType, basically.
    if "__gel_dynamic_proxy_base__" in cls.__dict__:
        base = cls.__dict__["__gel_dynamic_proxy_base__"]
        return base._get_subtype_proxy, (cls.__proxy_of__,)
    else:
        # If it is not one of our dynamic things, we return the
        # classname and pickle goes and does something useful.
        return cls.__qualname__


class ProxyModel(
    GelModel,
    _abstract.AbstractGelProxyModel[_MT_co, GelLinkModel],
    __gel_root_class__=True,
):
    __gel_has_id_field__ = False

    if TYPE_CHECKING:
        __gel_proxy_merged_model_cache__: ClassVar[type[_MergedModelBase]]

    # A cache for dynamically generated proxies where the __proxy_of__
    # class is a *subtype*. Generated by _get_subtype_proxy.
    __gel_subtype_proxy_cache__: ClassVar[
        dict[type[GelModel], type[ProxyModel[Any]]] | None
    ] = None

    # NB: __linkprops__ is not in slots because it is managed by
    #     GelLinkModelDescriptor.

    __slots__ = ("__gel_linked__", "_p__obj__")

    __gel_proxied_dunders__: ClassVar[frozenset[str]] = frozenset(
        {
            "__linkprops__",
        }
    )

    if TYPE_CHECKING:
        __linkprops__: _abstract.GelLinkModelDescriptor[GelLinkModel]

    def __new__(cls, *args: Any, **kwargs: Any) -> Self:
        self = cls.__gel_model_construct__(None)
        ll_setattr(self, "__gel_linked__", False)  # noqa: FBT003
        return self

    def __init__(
        self,
        /,
        *,
        __linkprops__: Any = _unset,
        **kwargs: Any,
    ) -> None:
        # Note, we don't call super().__init__() here.
        # GelModel.__init__ and GelSourceModel.__init__ don't do anything
        # useful for the proxy.

        # We want ProxyModel to be a trasparent wrapper, so we
        # forward the constructor arguments to the wrapped object.
        wrapped = self.__proxy_of__(**kwargs)
        assert not isinstance(wrapped, ProxyModel)
        ll_setattr(self, "_p__obj__", wrapped)
        # __linkprops__ is written into __dict__ by GelLinkModelDescriptor

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()

        # Register a custom reducer for every *metaclass*
        # of ProxyModels. We need this to deal with our dynamic classes.
        # Shouldn't be many.
        copyreg.pickle(
            type(cls),
            _pickle_dynamic_proxy_model,  # pyright: ignore [reportArgumentType]
        )

    @classmethod
    def _get_subtype_proxy(cls, ncls: type[GelModel]) -> type[Self]:
        """Generate a subtype that has ncls as its __proxy_of__"""

        if ncls is cls.__proxy_of__:
            return cls

        if cls.__gel_subtype_proxy_cache__ is None:
            cls.__gel_subtype_proxy_cache__ = {}

        if cached := cls.__gel_subtype_proxy_cache__.get(ncls):
            return cast('type[Self]', cached)

        core_schema = ProxyModel.__dict__['__get_pydantic_core_schema__']
        new_proxy = type(cls)(  # type: ignore[misc]
            f'{cls.__name__}[{ncls.__name__}]',
            (ncls, ProxyModel[ncls], cls),  # type: ignore[valid-type]
            {
                k: cls.__dict__[k]
                for k in (
                    '__module__',
                    '__qualname__',
                )
                if k in cls.__dict__
            }
            | {
                # This gets overridden in some cases, and we need to restore it
                '__get_pydantic_core_schema__': core_schema,
                '__gel_dynamic_proxy_base__': cls,
            },
        )
        new_proxy.model_rebuild()
        cls.__gel_subtype_proxy_cache__[ncls] = new_proxy
        return cast('type[Self]', new_proxy)

    @classmethod
    def link(cls, obj: _MT_co, /, **link_props: Any) -> Self:  # type: ignore [misc]
        proxy_of = ll_type_getattr(cls, "__proxy_of__")

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

        ncls = cls._get_subtype_proxy(type(obj))

        self = ncls.__new__(ncls)
        lprops = ncls.__linkprops__(**link_props)
        ll_setattr(self, "__linkprops__", lprops)
        assert not isinstance(obj, ProxyModel)
        ll_setattr(self, "_p__obj__", obj)

        # Treat newly created link props as if they had all their values
        # changed. Newly created proxy models will overwrite any existing
        # link props.
        lprop_names = set(self.__linkprops__.__dict__.keys())
        ll_setattr(self.__linkprops__, '__gel_changed_fields__', lprop_names)

        return self

    def __getattribute__(self, name: str) -> Any:
        if name in {
            "_p__obj__",
            "__proxy_of__",
            "__class__",
            "__gel_linked__",
            "without_linkprops",
        }:
            # Fast path for the wrapped object itself / linkprops model
            # (this optimization is informed by profiling model
            # instantiation and save() operation)
            return ll_getattr(self, name)

        if (
            name == "id"
            or not name.startswith(("_", "model_"))
            or name in {"__gel_new__", "__gel_changed_fields__"}
        ):
            # Faster path for "public-like" attributes
            return ll_getattr(ll_getattr(self, "_p__obj__"), name)

        if name == "__linkprops__":
            lp = _abstract.get_proxy_linkprops(self)
            if getattr(lp, "__gel_copied_by_ref__", False):
                # save() and other internal machinery can use
                # `object.__getattribute__` to request __linkprops__ without
                # triggering this code.
                # But when it is triggered - it's called by the user, and in
                # this case, if __linkprops__ object is copied by ref we want
                # to do an actual hard copy of it.
                # No need to do deep copy, as copying by ref can only happen
                # when __linkprops__ only has immutable properties in it.
                lp = lp.model_copy()
                # __gel_copied_by_ref__ won't be copied by model_copy()
                ll_setattr(self, "__linkprops__", lp)
                return lp

        model_fields = type(self).__proxy_of__.model_fields
        if name in model_fields:
            base = ll_getattr(self, "_p__obj__")
            return getattr(base, name)

        # We don't do anything around __gel_new__ or __gel_changed_fields__
        # because we never use them on proxies, we always unwrap proxy
        # and reason about the wrapped objects/lprops.

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
            assert issubclass(cls.__proxy_of__, _abstract.AbstractGelModel)

    @classmethod
    def __make_merged_model(cls) -> type[_MergedModelBase]:
        # Build a model that has all fields of the proxied model +
        # the `__linkprops__` field. This is by far the most robust way
        # (albeit maybe a resource demanding one) to implement
        # validation schema for ProxyModel.
        try:
            # Make sure we get the cached model for *this* class.
            return cast(
                "type[_MergedModelBase]",
                cls.__dict__["__gel_proxy_merged_model_cache__"],
            )
        except KeyError:
            pass

        if cls.__proxy_of__ is None or cls.__linkprops__ is None:
            raise TypeError("Subclass must set __proxy_of__ and __linkprops__")

        # _MergedModelBase has a custom metaclass, so we must
        # create a common subclass of that and whatever the
        # metaclass of the real type is.
        pmcls = type(cls.__proxy_of__)
        if issubclass(pmcls, _MergedModelMeta):
            metaclass = pmcls
        else:
            metaclass = types.new_class(
                f"{cls.__name__}Meta",
                (_MergedModelMeta, pmcls),
            )

        merged = cast(
            "type[_MergedModelBase]",
            pydantic.create_model(
                cls.__name__,
                __base__=(
                    cls.__proxy_of__,
                    _MergedModelBase,
                ),  # inherit all wrapped fields
                __config__=DEFAULT_MODEL_CONFIG,
                __cls_kwargs__={"metaclass": metaclass},
                linkprops____=(
                    cls.__linkprops__,
                    pydantic.Field(None, alias="__linkprops__"),
                ),
            ),
        )

        cls.__gel_proxy_merged_model_cache__ = merged
        return merged

    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source_type: Any,
        handler: pydantic.GetCoreSchemaHandler,
    ) -> pydantic_core.CoreSchema:
        if cls is ProxyModel:
            return handler(source_type)
        else:

            def _validate(value: Any) -> Any:
                if isinstance(value, cls.__gel_proxy_merged_model_cache__):
                    dct = value.__dict__

                    lps = dct.pop("__linkprops__", None)
                    lps_dct = None
                    if lps is not None:
                        lps_dct = lps.__dict__

                    # construct in the fastest way possible, the data
                    # has already been validated by the "merged" model.
                    obj = cls.__proxy_of__.__gel_model_construct__(dct)
                    return cls.__gel_proxy_construct__(obj, lps_dct or {})

                if isinstance(value, cls):  # already wrapped (edge-case)
                    return value

                if isinstance(value, cls.__proxy_of__):
                    return cls.__gel_proxy_construct__(value, {})

                raise TypeError(
                    f"cannot validate proxy, unexpected value type: "
                    f"{type(value)}"
                )

            merged_schema = handler.generate_schema(cls.__make_merged_model())
            inner_schema = core_schema.union_schema(
                [
                    merged_schema,
                    handler.generate_schema(cls.__proxy_of__),
                ]
            )
            return core_schema.no_info_after_validator_function(
                _validate,
                schema=inner_schema,
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
        lprops: dict[str, Any] | GelLinkModel,
        *,
        linked: bool = False,
    ) -> Self:
        ncls = cls._get_subtype_proxy(type(obj))

        pnv = ncls.__gel_model_construct__(None)
        assert not isinstance(obj, ProxyModel)
        ll_setattr(pnv, "_p__obj__", obj)

        if type(lprops) is dict:
            lp_obj = ncls.__linkprops__.__gel_model_construct__(lprops)
        else:
            lp_obj = lprops  # type: ignore [assignment]

        ll_setattr(
            pnv,
            "__linkprops__",
            lp_obj,
        )
        ll_setattr(pnv, "__gel_linked__", linked)
        return pnv

    def __gel_merge_other_proxy__(  # pyright: ignore [reportIncompatibleMethodOverride]
        self,
        other: Self,
    ) -> None:
        cls = type(self)
        assert type(other) is cls

        if not _abstract.is_proxy_linked(other):
            # `other` isn't link, so it must be a new proxy created
            # by instantiating the model or calling the `ProxyModel.link()`
            # classmethod.
            lp = _abstract.copy_or_ref_lprops(other.__linkprops__)
        else:
            lp = cls.__linkprops__.__gel_model_construct__({})

        ll_setattr(self, "__linkprops__", lp)
        ll_setattr(self, "_p__obj__", ll_getattr(other, "_p__obj__"))

    def __gel_replace_wrapped_model__(
        self,
        new: _MT_co,  # type: ignore [misc]
    ) -> None:
        assert isinstance(new, self.__proxy_of__)
        ll_setattr(self, "_p__obj__", new)

    def __gel_replace_linkprops__(
        self,
        new: GelLinkModel,
    ) -> None:
        ll_setattr(self, "__linkprops__", new)

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

        if self_obj.__gel_new__ or other_obj.__gel_new__:
            return False

        if not self.__gel_id_shape__ or not other_obj.__gel_id_shape__:
            return False

        return self_obj.id == other_obj.id  # type: ignore [no-any-return]

    def __hash__(self) -> int:
        wrapped = ll_getattr(self, "_p__obj__")
        if wrapped.__gel_new__:
            raise TypeError("Model instances without id value are unhashable")
        return hash(wrapped.id)

    def __repr_name__(self) -> str:
        cls = type(self)
        base_cls = cls.__bases__[0]

        # Thank me later; this can happen when adjusting our complex
        # serialization or __init__ / __new__ logic.
        incomplete = ""
        no_lprops = not hasattr(self, "__linkprops__")
        no_obj = not hasattr(self, "_p__obj__")
        if no_lprops or no_obj:
            incomplete = "Incomplete["
            if no_lprops:
                incomplete += "no lprops"
            if no_obj:
                if no_lprops:
                    incomplete += ","
                incomplete += "no obj"
            incomplete += "]"

        return (
            f"Proxy{incomplete}[{base_cls.__module__}.{base_cls.__qualname__}]"
        )

    def __repr_args__(self) -> Iterable[tuple[str | None, Any]]:
        # We use `hasattr()` because an object might require working
        # repr() halfway through its initialization. We don't want
        # to crash in that case.
        if hasattr(self, "__linkprops__"):
            # We render `__linkprops__` as a field, not mixing in
            # link properties with regular properties/links. It's
            # utterly confusing when repr() disagrees with model_dump().
            yield ("__linkprops__", self.__linkprops__)
        if hasattr(self, "_p__obj__"):
            yield from self._p__obj__.__repr_args__()

    def __proxy_model_dump(
        self,
        to_json: bool,  # noqa: FBT001
        /,
        *,
        context: _pydantic_utils.GelDumpContext | None = None,
        **kwargs: Any,
    ) -> dict[str, Any] | str:
        wrapped: GelModel = ll_getattr(self, "_p__obj__")

        _pydantic_utils.massage_model_dump_kwargs(
            wrapped,
            caller="model_dump_json" if to_json else "model_dump",
            kwargs=kwargs,
            context=context,
        )

        # We should find a better way to do dumps of ProxyModel instances.
        # But in the interim, this is the most straightforward and robust
        # way to get everything working:
        #
        # - 'exclude' on __linkprops__ works fully
        # - model_dump_json() is properly supported without risking breakage
        # - let pydantic care about the appropriate context for __linkprops__

        merged_model_cls = type(self).__make_merged_model()
        merged = object.__new__(merged_model_cls)
        for attr in (
            "__pydantic_fields_set__",
            "__pydantic_extra__",
            "__pydantic_private__",
            "__dict__",
            "__gel_new__",
            "__gel_changed_fields__",
        ):
            ll_setattr(merged, attr, ll_getattr(wrapped, attr))

        assert "__linkprops__" not in merged.__dict__
        merged.__dict__["__linkprops__"] = _abstract.get_proxy_linkprops(self)
        try:
            if to_json:
                return merged.model_dump_json(context=context, **kwargs)
            else:
                return merged.model_dump(context=context, **kwargs)
        finally:
            del merged.__dict__["__linkprops__"]

    @_utils.inherit_signature(  # type: ignore [arg-type]
        _pydantic_utils.model_dump_signature,
    )
    def model_dump(
        self,
        /,
        *,
        context: _pydantic_utils.GelDumpContext | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        dump = self.__proxy_model_dump(
            False,  # noqa: FBT003
            context=context,
            **kwargs,
        )
        assert type(dump) is dict
        return dump

    @_utils.inherit_signature(  # type: ignore [arg-type]
        _pydantic_utils.model_dump_json_signature,
    )
    def model_dump_json(
        self,
        /,
        *,
        context: _pydantic_utils.GelDumpContext | None = None,
        **kwargs: Any,
    ) -> str:
        dump = self.__proxy_model_dump(
            True,  # noqa: FBT003
            context=context,
            **kwargs,
        )
        assert type(dump) is str
        return dump

    def __getstate__(self) -> dict[Any, Any]:
        return {
            "obj": ll_getattr(self, "_p__obj__"),
            "linkprops": ll_getattr(self, "__linkprops__"),
            "linked": ll_getattr(self, "__gel_linked__"),
        }

    def __setstate__(self, state: dict[Any, Any]) -> None:
        ll_setattr(self, "_p__obj__", state["obj"])
        ll_setattr(self, "__linkprops__", state["linkprops"])
        ll_setattr(self, "__gel_linked__", state["linked"])

    def without_linkprops(self) -> _MT_co:
        return ll_getattr(self, "_p__obj__")  # type: ignore [no-any-return]


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
