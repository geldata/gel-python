from __future__ import annotations

import collections
import copy
import dataclasses
import pathlib
import warnings
import weakref
import uuid

from typing import (
    TYPE_CHECKING,
    Any,
    Literal,
    NamedTuple,
    TypeGuard,
    TypeVar,
    Generic,
    cast,
)
from typing_extensions import TypeAliasType, dataclass_transform

from gel._internal._qbmodel._abstract import (
    DEFAULT_VALUE,
    AbstractLinkSet,
    AbstractMutableLinkSet,
    LinkSet,
    LinkWithPropsSet,
    AbstractGelSourceModel,
    GelPrimitiveType,
    get_proxy_linkprops,
    reconcile_link,
    reconcile_proxy_link,
)
from gel._internal._qbmodel._pydantic._models import (
    GelModel,
    GelSourceModel,
    ProxyModel,
)
from gel._internal._tracked_list import (
    AbstractCollection,
    AbstractTrackedList,
    TrackedList,
    DowncastingTrackedList,
)
from gel._internal._edgeql import PointerKind, quote_ident

if TYPE_CHECKING:
    from collections.abc import (
        Iterable,
        Iterator,
        Callable,
        Sequence,
    )

    from gel._internal._qb import GelPointerReflection


# Yeah... don't use getattr() on GelModel in save() -- we can't
# trigger GelModel.__setattr__() here as it will mess with
# __pydantic_fields_set__ and we won't understand if the field
# was set by the user or by Gel.
# This will prevent us from using it by accident.
if TYPE_CHECKING:
    getattr: None = None  # noqa: A001
else:

    def getattr(*args: Any) -> Any:  # noqa: A001
        raise RuntimeError(
            "getattr() is not allowed on GelModel in save(), use model_attr()"
        )


# Warn if sync() is creating more than this many objects and suggest
# using save() instead.
SYNC_NEW_THRESHOLD = 50
# Ditto for refetching.
SYNC_REFETCH_THRESHOLD = 100
# Base stacklevel for the above sync() warnings
SYNC_BASE_STACKLEVEL = 6


T = TypeVar("T")
T_co = TypeVar("T_co", covariant=True)
V = TypeVar("V")


def _identity_func(x: T) -> T:
    return x


_unset = object()
_missing_arg = object()

_STOP_LINK_TRAVERSAL = (None, _unset, DEFAULT_VALUE)
_STOP_FIELDS_NO_ID = ("__tid__", "__tname__")
_STOP_FIELDS = ("id", *_STOP_FIELDS_NO_ID)

_sorted_pointers_cache: weakref.WeakKeyDictionary[
    type[GelSourceModel], Iterable[GelPointerReflection]
] = weakref.WeakKeyDictionary()


ll_attr = object.__getattribute__


LinkPropertiesValues = TypeAliasType(
    "LinkPropertiesValues", dict[str, object | None]
)


_dataclass = dataclasses.dataclass(frozen=True, kw_only=True)


@dataclass_transform(
    frozen_default=True,
    kw_only_default=True,
)
def _struct(t: type[T]) -> type[T]:
    return _dataclass(t)


@_struct
class BaseFieldInfo:
    name: str
    """Pointer name"""
    info: GelPointerReflection
    """Static pointer schema reflection"""


@_struct
class SingleLinkChange(BaseFieldInfo):
    target: GelModel | None

    props_info: dict[str, GelPointerReflection] | None = None
    props: LinkPropertiesValues | None = None

    def __post_init__(self) -> None:
        assert not isinstance(self.target, ProxyModel)
        assert not self.info.cardinality.is_multi()
        assert (self.props_info is None and self.props is None) or (
            self.props_info is not None and self.props is not None
        )


@_struct
class MultiLinkReset(BaseFieldInfo):
    pass


@_struct
class MultiLinkAdd(BaseFieldInfo):
    added: list[GelModel]

    added_props: list[LinkPropertiesValues] | None = None
    props_info: dict[str, GelPointerReflection] | None = None

    replace: bool = False

    def __post_init__(self) -> None:
        assert not any(isinstance(o, ProxyModel) for o in self.added)
        assert self.info.cardinality.is_multi()
        assert self.added_props is None or (
            self.props_info is not None
            and len(self.added_props) > 0
            and len(self.added) == len(self.added_props)
        )


@_struct
class MultiLinkRemove(BaseFieldInfo):
    removed: Iterable[GelModel]

    def __post_init__(self) -> None:
        assert not any(isinstance(o, ProxyModel) for o in self.removed)
        assert self.info.cardinality.is_multi()


@_struct
class PropertyChange(BaseFieldInfo):
    value: object | None

    def __post_init__(self) -> None:
        assert not self.info.cardinality.is_multi()


@_struct
class MultiPropAdd(BaseFieldInfo):
    added: Iterable[object]

    def __post_init__(self) -> None:
        assert self.info.cardinality.is_multi()


@_struct
class MultiPropRemove(BaseFieldInfo):
    removed: Iterable[object]

    def __post_init__(self) -> None:
        assert self.info.cardinality.is_multi()


FieldChange = TypeAliasType(
    "FieldChange",
    PropertyChange
    | MultiPropAdd
    | MultiPropRemove
    | SingleLinkChange
    | MultiLinkAdd
    | MultiLinkRemove
    | MultiLinkReset,
)

FieldChangeMap = TypeAliasType("FieldChangeMap", dict[str, FieldChange])
FieldChangeLists = TypeAliasType(
    "FieldChangeLists", dict[str, list[FieldChange]]
)


@_struct
class RefetchShape:
    fields: dict[str, GelPointerReflection] = dataclasses.field(
        default_factory=dict
    )
    models: list[GelModel] = dataclasses.field(default_factory=list)


RefetchBatch = TypeAliasType(
    "RefetchBatch", dict[type[GelModel], RefetchShape]
)


@dataclasses.dataclass
class ModelChange:
    model: GelModel
    fields: FieldChangeMap

    def __post_init__(self) -> None:
        assert not isinstance(self.model, ProxyModel)


ChangeBatch = TypeAliasType("ChangeBatch", list[ModelChange])


class SavePlan(NamedTuple):
    # Lists of lists of queries to create new objects.
    # Every list of query is safe to executute in a "batch" --
    # basically send them all at once. Follow up lists of queries
    # will use objects inserted by the previous batches.
    insert_batches: list[ChangeBatch]

    # Optional links of newly inserted objects and changes to
    # links between existing objects.
    update_batch: ChangeBatch

    # When we sync() we refetch -- this field is the spec for that.
    # Only non-empty when refetch is True.
    refetch_batch: RefetchBatch

    # When we refetch computed links we also need to know which
    # objects were in the sync() graph that theoretically can end
    # up in the computed link. So we track then in this dict.
    existing_objects: dict[uuid.UUID, GelModel | IDTracker[GelModel, None]]


class IDTracker(Generic[T, V]):
    _seen: dict[int, tuple[T, V | None]]

    def __init__(
        self, initial_set: Iterable[T] | Iterable[tuple[T, V]] | None = None, /
    ):
        if initial_set is not None:
            self._seen = {
                id(y[0]): y
                for y in (
                    x if isinstance(x, tuple) else (x, None)
                    for x in initial_set
                )
            }
        else:
            self._seen = {}

    def track(self, obj: T, value: V | None = None, /) -> None:
        self._seen[id(obj)] = (obj, value)

    def untrack(self, obj: T, /) -> None:
        self._seen.pop(id(obj), None)

    def track_many(self, more: Iterable[T] | Iterable[tuple[T, V]], /) -> None:
        for obj in more:
            ret = (obj, None) if not isinstance(obj, tuple) else obj
            self._seen[id(ret[0])] = ret

    def untrack_many(self, more: Iterable[T], /) -> None:
        for obj in more:
            self._seen.pop(id(obj), None)

    def get_tracked_by_hash(self, hash: int, /) -> T:  # noqa: A002
        obj_value = self._seen.get(hash, None)
        if obj_value is None:
            raise KeyError(hash)
        return obj_value[0]

    def __setitem__(self, obj: T, value: V, /) -> None:
        self._seen[id(obj)] = (obj, value)

    def __getitem__(self, obj: T) -> V | None:
        try:
            return self._seen[id(obj)][1]
        except KeyError:
            raise KeyError((id(obj), obj)) from None

    def get(self, obj: T, /) -> V | None:
        try:
            v = self._seen[id(obj)][1]
        except KeyError:
            raise KeyError((id(obj), obj)) from None
        return v

    def get_not_none(self, obj: T, /) -> V:
        try:
            v = self._seen[id(obj)][1]
        except KeyError:
            raise KeyError((id(obj), obj)) from None
        assert v is not None
        return v

    def values(self) -> Iterable[V | None]:
        return (v[1] for v in self._seen.values())

    def values_not_none(self) -> Iterable[V]:
        for _, v in self._seen.values():
            assert v is not None
            yield v

    def keys(self) -> Iterable[T]:
        return (v[0] for v in self._seen.values())

    def items(self) -> Iterable[tuple[T, V | None]]:
        return self._seen.values()

    def __contains__(self, obj: T) -> bool:
        return id(obj) in self._seen

    def __len__(self) -> int:
        return len(self._seen)

    def __bool__(self) -> bool:
        return bool(self._seen)

    def __iter__(self) -> Iterator[T]:
        for t, _ in self._seen.values():
            yield t


def is_prop_list(val: object) -> TypeGuard[TrackedList[GelPrimitiveType]]:
    return isinstance(
        val, (TrackedList, DowncastingTrackedList)
    ) and issubclass(type(val).type, GelPrimitiveType)  # type: ignore [misc]


def is_link_set(val: object) -> TypeGuard[LinkSet[GelModel]]:
    return isinstance(val, LinkSet) and issubclass(type(val).type, GelModel)


def is_link_abstract_dlist(
    val: object,
) -> TypeGuard[AbstractLinkSet[GelModel]]:
    return isinstance(val, AbstractLinkSet) and issubclass(
        type(val).type, GelModel
    )


def is_link_abstract_mutable_dlist(
    val: object,
) -> TypeGuard[AbstractMutableLinkSet[GelModel]]:
    return isinstance(val, AbstractMutableLinkSet) and issubclass(
        type(val).type, GelModel
    )


def is_link_wprops_set(
    val: object,
) -> TypeGuard[LinkWithPropsSet[ProxyModel[GelModel], GelModel]]:
    return isinstance(val, LinkWithPropsSet)


def is_proxy(
    val: object,
) -> TypeGuard[ProxyModel[GelModel]]:
    return isinstance(val, ProxyModel)


def unwrap_proxy(val: GelModel) -> GelModel:
    if isinstance(val, ProxyModel):
        # This is perf-sensitive function as it's called on
        # every edge of the graph multiple times.
        return ll_attr(val, "_p__obj__")  # type: ignore [no-any-return]
    else:
        return val


def unwrap_proxy_no_check(val: ProxyModel[GelModel]) -> GelModel:
    return ll_attr(val, "_p__obj__")  # type: ignore [no-any-return]


def unwrap_dlist(val: Iterable[GelModel]) -> Iterable[GelModel]:
    for o in val:
        yield unwrap_proxy(o)


def get_pointers(tp: type[GelSourceModel]) -> Iterable[GelPointerReflection]:
    # We sort pointers to produce similar queies regardless of Python
    # hashing -- this is to maximize the probability of a generated
    # uodate/insert query to hit the Gel's compiled cache.

    try:
        return _sorted_pointers_cache[tp]
    except KeyError:
        pass

    pointers = tp.__gel_reflection__.pointers
    ret = tuple(pointers[name] for name in sorted(pointers))
    _sorted_pointers_cache[tp] = ret
    return ret


def iter_graph(
    objs: Iterable[GelModel],
    id_tracker: IDTracker[GelModel, None],
) -> Iterable[GelModel]:
    # Simple recursive traverse of a model

    def _traverse(obj: GelModel) -> Iterable[GelModel]:
        if obj in id_tracker:
            return

        id_tracker.track(obj)
        yield obj

        for prop in get_pointers(type(obj)):
            if prop.computed or prop.kind is not PointerKind.Link:
                # We don't want to traverse computeds (they don't form the
                # actual dependency graph, real links do)
                continue

            linked = model_attr(obj, prop.name, _unset)
            if linked in _STOP_LINK_TRAVERSAL:
                # If users mess-up with user-defined types and smoe
                # of the data isn't fetched, we don't want to crash
                # with an AttributeErorr, it's not critical here.
                # Not fetched means not used, which is good for save().
                continue

            if prop.cardinality.is_multi():
                if is_link_wprops_set(linked):
                    for proxy in linked._items:
                        yield from _traverse(unwrap_proxy_no_check(proxy))
                else:
                    assert is_link_set(linked)
                    for lobj in linked._items:
                        yield from _traverse(lobj)
            else:
                yield from _traverse(unwrap_proxy(cast("GelModel", linked)))

    for o in objs:
        yield from _traverse(o)


def get_linked_new_objects(obj: GelModel) -> Iterable[GelModel]:
    visited: IDTracker[GelModel, None] = IDTracker()
    visited.track(obj)

    for prop in get_pointers(type(obj)):
        # Skip computed, non-link, and optional links;
        # only required links determine batch order
        if (
            prop.computed
            or prop.kind is not PointerKind.Link
            or prop.cardinality.is_optional()
        ):
            # optional links will be created via update operations later
            continue

        linked = model_attr(obj, prop.name, _unset)
        if linked in _STOP_LINK_TRAVERSAL:
            continue

        if prop.cardinality.is_multi():
            if is_link_wprops_set(linked):
                for pmod in linked._items:
                    unwrapped = unwrap_proxy_no_check(pmod)
                    if unwrapped.__gel_new__ and unwrapped not in visited:
                        visited.track(unwrapped)
                        yield unwrapped
            else:
                assert is_link_set(linked)
                for mod in linked._items:
                    if mod.__gel_new__ and mod not in visited:
                        visited.track(mod)
                        yield mod

        elif linked is not None:
            assert isinstance(linked, GelModel)
            unwrapped = unwrap_proxy(linked)
            if unwrapped.__gel_new__ and unwrapped not in visited:
                visited.track(unwrapped)
                yield unwrapped


def obj_to_name_ql(obj: GelModel) -> str:
    return type(obj).__gel_reflection__.name.as_quoted_schema_name()


def shift_dict_list(inp: dict[str, list[T]]) -> dict[str, T]:
    ret: dict[str, T] = {}
    for key, lst in list(inp.items()):
        if not lst:
            continue
        ret[key] = lst.pop(0)
        if not lst:
            inp.pop(key)
    return ret


def push_change(
    requireds: FieldChangeMap,
    sched: FieldChangeLists,
    change: FieldChange,
) -> None:
    """Push a field change either to *requireds* or *sched*."""

    if requireds.get(change.name):
        sched[change.name].append(change)
        return

    if isinstance(change, (PropertyChange, MultiPropAdd)):
        # For properties, generally we don't care -- we can try
        # packing as many of property sets as possible -- so hopefully
        # only one query will be generated for the entire object updates.
        requireds[change.name] = change
        return

    if (
        isinstance(change, (MultiLinkAdd, SingleLinkChange))
        and not change.info.cardinality.is_optional()
    ):
        # For links we do care -- we want to populate *requireds* with
        # only required links to make `insert` queries have as few dependencies
        # as possible.
        requireds[change.name] = change
        return

    sched[change.name].append(change)


def multi_prop_has_changes_recursive(
    val: list[Any] | tuple[Any, ...] | AbstractCollection[Any],
) -> bool:
    # We have to traverse the entire collection graph to check if
    # there are any changes in it. We can't just check the topmost
    # collection; e.g. consider you have a tuple of arrays -- the tuple
    # can't have changes on it's own, but one of the nested arrays can.

    def walk(collection: Iterable[Any]) -> bool:
        if isinstance(collection, AbstractCollection):
            if collection.__gel_has_changes__():
                return True
            iter_over = collection.unsafe_iter()
        else:
            iter_over = iter(collection)

        for item in iter_over:
            if isinstance(item, AbstractCollection):
                if item.__gel_has_changes__():
                    return True

            if isinstance(item, (list, tuple, AbstractCollection)):
                if walk(item):
                    return True

        return False

    return walk(val)


def multi_prop_commit_recursive(
    val: list[Any] | tuple[Any, ...] | AbstractCollection[Any],
    **commit_kwargs: Any,
) -> None:
    # Exists for the same reason as `multi_prop_has_changes_recursive`

    def walk(collection: Iterable[Any]) -> None:
        if isinstance(collection, AbstractCollection):
            collection.__gel_commit__(**commit_kwargs)
            iter_over = collection.unsafe_iter()
        else:
            iter_over = iter(collection)

        for item in iter_over:
            if isinstance(item, AbstractCollection):
                item.__gel_commit__(**commit_kwargs)

            if isinstance(item, (list, tuple, AbstractCollection)):
                walk(item)

    walk(val)


def model_attr(
    obj: AbstractGelSourceModel, name: str, default: Any = _missing_arg
) -> Any:
    dct = obj.__dict__
    if default is _missing_arg:
        return dct[name]
    else:
        return dct.get(name, default)


def _add_refetch_shape(
    obj: GelModel,
    refetch_ops: RefetchBatch,
) -> None:
    tp_obj: type[GelModel] = type(obj)
    tp_pointers = tp_obj.__gel_reflection__.pointers
    ref_shape = refetch_ops[tp_obj]
    ref_shape.models.append(obj)
    if not obj.__gel_new__:
        # Existing objects should refetch anything that was previously set
        for field_name in obj.__pydantic_fields_set__:
            if (
                field_name in ref_shape.fields
                or field_name in _STOP_FIELDS_NO_ID
            ):
                continue

            try:
                ptr_info = tp_pointers[field_name]
            except KeyError:
                # ad-hoc computed
                pass
            else:
                ref_shape.fields[field_name] = ptr_info

    else:
        # New objects only need computed properties refetched
        for ptr_name, ptr_info in tp_pointers.items():
            if not ptr_info.computed or ptr_info.kind != PointerKind.Property:
                continue

            ref_shape.fields[ptr_name] = ptr_info


def push_refetch_new(
    obj: GelModel,
    refetch_ops: RefetchBatch,
) -> None:
    _add_refetch_shape(obj, refetch_ops)
    # new objects are created and tracked by the SaveExecutor


def push_refetch_existing(
    obj: GelModel,
    existing_objects: dict[uuid.UUID, GelModel | IDTracker[GelModel, None]],
    refetch_ops: RefetchBatch,
) -> None:
    _add_refetch_shape(obj, refetch_ops)
    try:
        elst = existing_objects[obj.id]
    except KeyError:
        existing_objects[obj.id] = obj
    else:
        if isinstance(elst, IDTracker):
            elst.track(obj)
        else:
            # We'll have to track multiple GelModel instances with the same
            # `.id` and thus equal to each other. We still don't want to have
            # duplicates, so in (hopefully) rare cases when we have multiple
            # objects with the same `.id`, we'll have to tell them apart by
            # their Python address.
            existing_objects[obj.id] = IDTracker([elst, obj])


def make_plan(
    objs: Iterable[GelModel],
    /,
    *,
    refetch: bool,
    warn_on_large_sync_set: bool,
) -> SavePlan:
    insert_ops: ChangeBatch = []
    update_ops: ChangeBatch = []
    refetch_ops: RefetchBatch = collections.defaultdict(RefetchShape)
    removed_objects: list[Iterable[GelModel]] = []

    iter_graph_tracker: IDTracker[GelModel, None] = IDTracker()
    existing_objects: dict[
        uuid.UUID, GelModel | IDTracker[GelModel, None]
    ] = {}

    for obj in iter_graph(objs, iter_graph_tracker):
        tp_obj: type[GelModel] = type(obj)

        pointers = get_pointers(tp_obj)
        is_new = obj.__gel_new__

        # Capture changes in *properties* and *single links*
        field_changes = obj.__gel_get_changed_fields__()

        requireds: FieldChangeMap = {}
        sched: FieldChangeLists = collections.defaultdict(list)

        if refetch:
            if not obj.__gel_new__:
                push_refetch_existing(obj, existing_objects, refetch_ops)

            elif any(
                prop.kind == PointerKind.Property and prop.computed
                for prop in pointers
            ):
                # Refetch computed properties, they may depend on other objects
                # being inserted later
                push_refetch_new(obj, refetch_ops)

        for prop in pointers:
            # Skip computeds, we can't update them.
            if prop.computed or prop.readonly:
                # Exclude computeds but also props like `__type__` and `id`
                continue

            # Iterate through changes of *propertes* and *single links*.
            if prop.name in field_changes:
                # Since there was a change and we don't implement `del`,
                # the attribute must be set

                val = model_attr(obj, prop.name)
                if (
                    prop.kind is PointerKind.Property
                    and not prop.cardinality.is_multi()
                ):
                    # Single property.
                    #
                    # Multi properties, like multi links, will
                    # be preocessed separately.
                    assert not isinstance(val, GelModel)
                    push_change(
                        requireds,
                        sched,
                        PropertyChange(
                            name=prop.name,
                            value=val,
                            info=prop,
                        ),
                    )
                    continue

                elif (
                    prop.kind is PointerKind.Link
                    and not prop.cardinality.is_multi()
                ):
                    # Single link got overwritten with a new value.
                    #
                    # (Multi links are more complicated
                    # as they can be changed without being picked up by
                    # `__gel_get_changed_fields__()`, se we process them
                    # separately).

                    assert val is None or isinstance(val, GelModel)
                    assert not prop.cardinality.is_multi()

                    link_prop_variant = False
                    if prop.properties:
                        assert isinstance(val, ProxyModel)
                        link_prop_variant = bool(
                            get_proxy_linkprops(
                                val
                            ).__gel_get_changed_fields__()
                        )

                    if link_prop_variant:
                        assert isinstance(val, ProxyModel)
                        # Link with link properties
                        ptrs = get_pointers(type(val).__linkprops__)

                        val_lp = get_proxy_linkprops(val)
                        props = {
                            p.name: model_attr(val_lp, p.name, None)
                            for p in ptrs
                        }

                        sch = SingleLinkChange(
                            name=prop.name,
                            info=prop,
                            target=unwrap_proxy(val),
                            props=props,
                            props_info={p.name: p for p in ptrs},
                        )
                    else:
                        # Link without link properties
                        sch = SingleLinkChange(
                            name=prop.name,
                            info=prop,
                            target=unwrap_proxy(val),
                        )

                    push_change(requireds, sched, sch)
                    continue

            if (
                prop.kind is PointerKind.Property
                and prop.cardinality.is_multi()
            ):
                val = model_attr(obj, prop.name, _unset)
                if val in _STOP_LINK_TRAVERSAL:
                    continue

                assert is_prop_list(val)

                if mp_added := val.__gel_get_added__():
                    push_change(
                        requireds,
                        sched,
                        MultiPropAdd(
                            name=prop.name,
                            info=prop,
                            added=mp_added,
                        ),
                    )

                if mp_removed := val.__gel_get_removed__():
                    push_change(
                        requireds,
                        sched,
                        MultiPropRemove(
                            name=prop.name,
                            info=prop,
                            removed=mp_removed,
                        ),
                    )

                continue

            if prop.kind is PointerKind.Property:
                # Changes of properties with collection types (e.g. Array)
                # will not be picked up by `__gel_get_changed_fields__()`
                # as modifying them does not require touching their host
                # GelModel instance. Check them for changes manually.
                val = model_attr(obj, prop.name, _unset)
                if val in _STOP_LINK_TRAVERSAL:
                    continue

                if (
                    prop.mutable
                    and isinstance(val, (tuple, list, AbstractTrackedList))
                    and multi_prop_has_changes_recursive(val)
                ):
                    push_change(
                        requireds,
                        sched,
                        PropertyChange(
                            name=prop.name,
                            value=val,
                            info=prop,
                        ),
                    )
                    continue

            if (
                prop.kind is PointerKind.Link
                and not prop.cardinality.is_multi()
                and prop.properties
            ):
                val = model_attr(obj, prop.name, _unset)
                if val in _STOP_LINK_TRAVERSAL:
                    continue
                assert isinstance(val, ProxyModel)
                lprops = get_proxy_linkprops(val)
                if not lprops.__gel_changed_fields__:
                    continue

                # An existing single link has updated link props.

                ptrs = get_pointers(type(val).__linkprops__)
                props = {
                    p: model_attr(lprops, p, None)
                    for p in lprops.__gel_get_changed_fields__()
                }

                sch = SingleLinkChange(
                    name=prop.name,
                    info=prop,
                    target=unwrap_proxy(val),
                    props=props,
                    props_info={p.name: p for p in ptrs},
                )

                push_change(requireds, sched, sch)
                continue

            if (
                prop.kind is not PointerKind.Link
                or not prop.cardinality.is_multi()
            ):
                # We handle changes to properties and single links above;
                # getting here means there was no change for this field.
                assert prop.name not in field_changes
                continue

            # Let's unwind multi link changes.

            linked = model_attr(obj, prop.name, _unset)
            if linked is _unset or linked is DEFAULT_VALUE:
                # The link wasn't fetched at all
                continue

            # `linked` should be either an empty LinkSet or
            # a non-empty one
            assert linked is not None
            assert is_link_abstract_mutable_dlist(linked), (
                f"{prop.name!r} is not dlist, it is {type(linked)}"
            )

            replace = linked.__gel_overwrite_data__

            removed = linked.__gel_get_removed__()
            if removed:
                # 'replace' is set for new collections only
                assert not replace

                # TODO: this should probably be handled in the LinkSet
                # itself (GelModel and LinkSet are tightly coupled anyway)
                removed = [
                    m
                    for m in unwrap_dlist(removed)
                    if not ll_attr(m, "__gel_new__")
                ]
                if removed:
                    m_rem = MultiLinkRemove(
                        name=prop.name,
                        info=prop,
                        removed=removed,
                    )

                    push_change(requireds, sched, m_rem)

                    if refetch:
                        removed_objects.append(removed)

            # __gel_get_added__() will return *new* links, but we also
            # have to take care about link property updates on *existing*
            # links too. So we first get the list of the new ones, and then
            # we iterate through *all* linked objects checking if their link
            # props got any updates.
            #
            # - Why we mixed everything into the `added` list? Because it
            #   doesn't matter -- the syntax for inserting a new multi link
            #   or updating link props on the existing one is still the same:
            #   `+=`
            #
            # - Why aren't we capturing which link props have changes
            #   specifically? Becuse with EdgeQL we don't have a mechanism
            #   (yet) to update a specific link prop -- we'll have to submit
            #   all of them.
            added = linked.__gel_get_added__()
            if prop.properties:
                added_index = IDTracker[ProxyModel[GelModel], None]()
                added_index.track_many(
                    cast("list[ProxyModel[GelModel]]", added)
                )
                for el in linked._items:
                    assert isinstance(el, ProxyModel)
                    if el in added_index:
                        continue

                    lp = get_proxy_linkprops(el)
                    if lp.__gel_get_changed_fields__():
                        added.append(el)

            # No adds or changes for this link?
            if not added:
                if replace and not is_new:
                    # Need to reset this link to an empty list
                    push_change(
                        requireds,
                        sched,
                        MultiLinkReset(
                            name=prop.name,
                            info=prop,
                        ),
                    )

                # Continue to the next object
                continue

            added_proxies: Sequence[ProxyModel[GelModel]] = added  # type: ignore [assignment]

            # Simple case -- no link props!
            if not prop.properties or all(
                not get_proxy_linkprops(link).__gel_get_changed_fields__()
                for link in added_proxies
            ):
                mch = MultiLinkAdd(
                    name=prop.name,
                    info=prop,
                    added=list(unwrap_dlist(added)),
                    replace=replace,
                )
                push_change(requireds, sched, mch)
                continue

            # OK, we have to deal with link props
            #
            # First, we iterate through the list of added new objects
            # to the list. All of them should be ProxyModels
            # (as we use LinkWithPropsSet for links with props.)
            # Our goal is to segregate different combinations of
            # set link properties into separate groups.
            link_tp = type(added_proxies[0])
            assert issubclass(link_tp, ProxyModel)
            props_info = get_pointers(link_tp.__linkprops__)

            mch = MultiLinkAdd(
                name=prop.name,
                info=prop,
                added=list(unwrap_dlist(added)),
                added_props=[
                    {
                        p: model_attr(link_lp, p, None)
                        for p in (link_lp.__gel_get_changed_fields__())
                    }
                    for link_lp in (
                        get_proxy_linkprops(link)
                        for link in cast("list[ProxyModel[GelModel]]", added)
                    )
                ],
                props_info={p.name: p for p in props_info},
                replace=replace,
            )

            push_change(requireds, sched, mch)

        # After all this work we found no changes -- move on to the next obj.
        if not (is_new or sched or requireds):
            continue

        # If the object is new, we want to do the bare minimum to insert it,
        # so we create a minimal insert op for it -- properties and
        # required links.
        #
        # Note that for required multilinks we only have to
        # create at least one link -- that's why we're OK with getting just
        # one "batch" out of `req_multi_link_changes`
        if is_new:
            insert_ops.append(
                ModelChange(
                    model=obj,
                    fields=requireds,
                )
            )
            requireds = {}

        if requireds:
            for n, f in requireds.items():
                sched[n].insert(0, f)

        # Now let's create ops for all the remaning changes
        while sched:
            change = ModelChange(model=obj, fields=shift_dict_list(sched))
            update_ops.append(change)

    if refetch:
        # If we are refetching, we also want to traverse objects in
        # the sync() graph that are on the way out of the graph, but
        # are still part of this sync() call.
        for removed in removed_objects:
            for m in removed:
                if m not in iter_graph_tracker:
                    # `iter_graph_tracker` was used in the big loop above;
                    # now we're using it to make sure we don't submit
                    # removed objects to refetch more than once.
                    iter_graph_tracker.track(m)
                    push_refetch_existing(m, existing_objects, refetch_ops)

    # Plan batch inserts of new objects -- we have to insert objects
    # in batches respecting their required cross-dependencies.

    insert_batches: list[ChangeBatch] = []
    inserted: IDTracker[GelModel, ModelChange] = IDTracker()
    remaining_to_make: IDTracker[GelModel, ModelChange] = IDTracker(
        (ch.model, ch) for ch in insert_ops
    )
    obj_model_index: IDTracker[GelModel, ModelChange] = IDTracker(
        (ch.model, ch) for ch in insert_ops
    )

    while remaining_to_make:
        ready = [
            o
            for o in remaining_to_make
            if all(dep in inserted for dep in get_linked_new_objects(o))
        ]
        if not ready:
            # TODO: improve this error message by rendering a graph
            raise RuntimeError(
                "Cannot resolve recursive dependencies among objects"
            )

        insert_batches.append(
            [obj_model_index.get_not_none(mod) for mod in ready]
        )
        inserted.track_many(ready)
        remaining_to_make.untrack_many(ready)

    if (
        refetch
        and warn_on_large_sync_set
        and len(inserted) > SYNC_NEW_THRESHOLD
    ):
        _warn_on_large_sync("new", stacklevel=0, num_objects=len(inserted))

    return SavePlan(
        insert_batches,
        update_ops,
        refetch_ops,
        existing_objects,
    )


def make_save_executor_constructor(
    objs: tuple[GelModel, ...],
    *,
    refetch: bool,
    warn_on_large_sync_set: bool = False,
    save_postcheck: bool = False,
) -> Callable[[], SaveExecutor]:
    (
        create_batches,
        updates,
        refetch_batch,
        existing_objects,
    ) = make_plan(
        objs,
        refetch=refetch,
        warn_on_large_sync_set=warn_on_large_sync_set,
    )
    return lambda: SaveExecutor(
        objs=objs,
        create_batches=create_batches,
        updates=updates,
        refetch_batch=refetch_batch,
        existing_objects=existing_objects,
        refetch=refetch,
        save_postcheck=save_postcheck,
        warn_on_large_sync_set=warn_on_large_sync_set,
    )


class TypeWrapper(Generic[T_co]):
    def __init__(self, tp: type[T_co], query: str) -> None:
        self.tp = tp
        self.query = query

    def __edgeql__(self) -> tuple[type[T_co], str]:
        return self.tp, self.query

    def __repr__(self) -> str:
        return (
            f"save.TypeWrapper["
            f"{self.tp.__module__}.{self.tp.__qualname__}; {self.query!r}]"
        )


# How many INSERT/UPDATE operations can be combined into a single query.
# Significantly improves performance, but appears to hit the ceiling of
# improvements at around 100.
MAX_BATCH_SIZE = 1280


@_struct
class QueryBatch:
    executor: SaveExecutor
    query: str | TypeWrapper[Any]
    args_query: str
    args: list[tuple[object, ...] | int]
    changes: list[ModelChange]
    insert: bool

    def record_inserted_data(self, obj_data: Iterable[Any]) -> None:
        match self.executor.refetch, self.insert:
            case False, True:
                # in this case `obj_data` is a list of UUIDs
                for obj_upd, change in zip(
                    obj_data, self.changes, strict=True
                ):
                    self.executor.new_object_ids[change.model] = obj_upd

            case True, True:
                # in this case `obj_data` is a list of GelModel instances
                for obj_upd, change in zip(
                    obj_data, self.changes, strict=True
                ):
                    self.executor.new_object_ids[change.model] = obj_upd.id
                    self.executor.new_objects[obj_upd.id] = obj_upd

            case _, _:
                # TODO: Implement a check that ids from updates
                # match 'changes'?
                pass


# Refetching links will only refetch linked objects which are already in the
# link, or are in the sync objects.
#
# A refetched object will therefore need, per link:
# - whether that link is being being refetched, and if so,
# - the objects already in the link.
#
# The order of links is kept consistent with the refetch shape.
RefetchSpecEntry = TypeAliasType(
    "RefetchSpecEntry",
    tuple[
        uuid.UUID,  # Refetched obj id
        list[
            tuple[
                bool,  # Whether the link is being refetched
                list[uuid.UUID],  # Objects in the link
            ]
        ],
    ],
)


@dataclasses.dataclass(kw_only=True, frozen=True)
class QueryRefetchArgs:
    spec: list[RefetchSpecEntry]
    new: list[uuid.UUID]
    existing: list[uuid.UUID]


@_struct
class QueryRefetch:
    executor: SaveExecutor
    query: TypeWrapper[type[GelModel]]
    args: QueryRefetchArgs
    shape: RefetchShape

    def record_refetched_data(self, obj_data: Iterable[GelModel]) -> None:
        self.executor.refetched_data.append((self.shape, obj_data))


@_struct
class CompiledQuery:
    single_query: str
    multi_query: str
    args_query: str
    arg: tuple[object, ...] | int
    change: ModelChange


@dataclasses.dataclass
class SaveExecutor:
    objs: tuple[GelModel, ...]
    create_batches: list[ChangeBatch]
    updates: ChangeBatch
    refetch_batch: RefetchBatch
    existing_objects: dict[uuid.UUID, GelModel | IDTracker[GelModel, None]]
    refetch: bool
    save_postcheck: bool
    warn_on_large_sync_set: bool

    refetched_data: list[tuple[RefetchShape, Iterable[GelModel]]] = (
        dataclasses.field(init=False)
    )

    new_object_ids: IDTracker[GelModel, uuid.UUID] = dataclasses.field(
        init=False
    )

    new_objects: dict[uuid.UUID, GelModel] = dataclasses.field(init=False)

    def __post_init__(self) -> None:
        self.new_object_ids = IDTracker()
        if self.refetch:
            # Let it crash with AttributeError if we try to access this
            # without refetch -- that should be caught by tests.
            self.new_objects = {}
            self.refetched_data = []

    def commit(self) -> None:
        try:
            self._commit()
        finally:
            # Make things GC faster
            self.objs = None  # type: ignore [assignment]
            self.create_batches = None  # type: ignore [assignment]
            self.updates = None  # type: ignore [assignment]
            self.refetch_batch = None  # type: ignore [assignment]
            self.new_object_ids = None  # type: ignore [assignment]
            self.new_objects = None  # type: ignore [assignment]
            self.refetched_data = None  # type: ignore [assignment]

    def _compile_refetch(
        self,
    ) -> list[
        tuple[
            type[GelModel],
            TypeWrapper[type[GelModel]],
            list[RefetchSpecEntry],
            RefetchShape,
        ]
    ]:
        ret = []

        total_refetch_obj_count = 0

        for tp, shape in self.refetch_batch.items():
            select_shape: list[str] = []

            link_arg_order: list[str] = []

            total_refetch_obj_count += len(shape.models)

            for ptr in shape.fields.values():
                if ptr.kind.is_link():
                    link_arg_order.append(ptr.name)
                    link_num = len(link_arg_order) - 1

                    if ptr.properties:
                        props = ",".join(
                            f"@{quote_ident(p)}" for p in ptr.properties
                        )
                        props = f"{{ {props} }}"
                    else:
                        props = ""

                    maybe_assert = maybe_assert_end = ""

                    if not ptr.cardinality.is_optional():
                        maybe_assert = "(select assert_exists("
                        maybe_assert_end = "))"

                    computed_filter = ""
                    if ptr.computed:
                        computed_filter = "or .id in __existing"

                    # The logic of the `filter` clause is:
                    # - first test if the link was fetched for this object
                    #   at all - if not - we don't need to update it
                    # - if the link was fetched for this particular GelModel --
                    #   filter it by all existing objects prior to sync() in
                    #   this link PLUS all new objects that were inserted
                    #   during sync().
                    select_shape.append(f"""
                        {quote_ident(ptr.name)} := {maybe_assert}(
                            with
                                __link := __obj_data.1[{link_num}]
                            select .{quote_ident(ptr.name)} {props}
                            filter __link.0 and (
                                .id in array_unpack(__link.1)
                                or .id in __new
                                {computed_filter}
                            )
                        ){maybe_assert_end}
                    """)

                else:
                    select_shape.append(quote_ident(ptr.name))

            obj_links_all: dict[uuid.UUID, dict[str, list[uuid.UUID]]] = (
                collections.defaultdict(dict)
            )
            for obj in shape.models:
                link_ids = obj_links_all[self._get_id(obj)]
                for lname in link_arg_order:
                    if lname not in obj.__pydantic_fields_set__:
                        # `model_attr` below this 'if' will always succeed
                        continue

                    obj_link_ids = link_ids.get(lname)
                    if obj_link_ids is None:
                        link_ids[lname] = obj_link_ids = []

                    ptr = shape.fields[lname]
                    if ptr.cardinality.is_multi():
                        m_link: AbstractCollection[GelModel] = model_attr(
                            obj, lname
                        )
                        if ptr.computed:
                            # m_link will be a tuple.
                            m_iter = iter(m_link)
                        else:
                            # m_link will be an AbstractCollection.
                            m_iter = m_link.unsafe_iter()

                        if ptr.properties:
                            obj_link_ids.extend(
                                self._get_id(unwrap_proxy(sub))
                                for sub in m_iter
                            )
                        else:
                            obj_link_ids.extend(
                                self._get_id(unwrap_proxy(sub))
                                for sub in m_iter
                            )
                    else:
                        s_link: GelModel = model_attr(obj, lname)
                        if ptr.properties:
                            obj_link_ids.append(
                                self._get_id(unwrap_proxy(s_link))
                            )
                        else:
                            obj_link_ids.append(self._get_id(s_link))

            spec_arg: list[RefetchSpecEntry] = [
                (
                    obj_id,
                    [
                        (False, [])
                        if (li := link_ids.get(lname)) is None
                        else (True, li)
                        for lname in link_arg_order
                    ],
                )
                for obj_id, link_ids in obj_links_all.items()
            ]

            tp_ql_name = tp.__gel_reflection__.name.as_quoted_schema_name()

            query = f"""
                with
                    __new := std::array_unpack(<array<std::uuid>>$new),
                    __spec := <array<tuple<
                        std::uuid,
                        array<tuple<std::bool,array<std::uuid>>>
                    >>>$spec,
                    __existing := std::array_unpack(
                        <array<std::uuid>>$existing
                    )

                for __obj_data in std::array_unpack(__spec) union (
                    select {tp_ql_name} {{
                        {",".join(select_shape)}
                    }}
                    filter .id = __obj_data.0
                )
            """

            ret.append((tp, TypeWrapper(tp, query), spec_arg, shape))

        if (
            self.refetch
            and self.warn_on_large_sync_set
            and total_refetch_obj_count > SYNC_REFETCH_THRESHOLD
        ):
            _warn_on_large_sync(
                "refetch",
                stacklevel=0,
                num_objects=total_refetch_obj_count,
            )

        return ret  # type: ignore [return-value]

    def get_refetch_queries(
        self,
    ) -> list[QueryRefetch]:
        new_ids = list(self.new_object_ids.values_not_none())

        return [
            QueryRefetch(
                executor=self,
                query=q[1],
                args=QueryRefetchArgs(
                    spec=q[2],
                    new=new_ids,
                    existing=list(self.existing_objects),
                ),
                shape=q[3],
            )
            for q in self._compile_refetch()
        ]

    def _compile_batch(
        self, batch: ChangeBatch, /, *, for_insert: bool
    ) -> list[QueryBatch]:
        compiled = [
            (
                type(change.model),
                self._compile_change(change, for_insert=for_insert),
            )
            for change in batch
        ]

        # Queries must be independent of each other within the same
        # ChangeBatch, so we can sort them to group queries.
        compiled.sort(
            key=lambda x: (x[0].__gel_reflection__.name, x[1].single_query)
        )

        icomp = iter(compiled)
        local_queries = [[next(icomp)]]

        for ctype, cq in icomp:
            if (
                ctype is local_queries[-1][0][0]
                and cq.single_query == local_queries[-1][0][1].single_query
                and len(local_queries[-1]) < MAX_BATCH_SIZE
            ):
                local_queries[-1].append((ctype, cq))
            else:
                local_queries.append([(ctype, cq)])

        return [
            QueryBatch(
                executor=self,
                query=(
                    TypeWrapper(lqs[0][0], lqs[0][1].multi_query)
                    if self.refetch
                    else lqs[0][1].multi_query
                ),
                args_query=lqs[0][1].args_query,
                args=[lq[1].arg for lq in lqs],
                changes=[lq[1].change for lq in lqs],
                insert=for_insert,
            )
            for lqs in local_queries
        ]

    def __iter__(self) -> Iterator[list[QueryBatch]]:
        if self.create_batches:
            for batch in self.create_batches:
                yield self._compile_batch(batch, for_insert=True)
        if self.updates:
            yield self._compile_batch(self.updates, for_insert=False)

    def _apply_refetched_data(self) -> None:
        for shape, obj_data in self.refetched_data:
            self._apply_refetched_data_shape(shape, obj_data)

    def _apply_refetched_data_shape(
        self, shape: RefetchShape, obj_data: Iterable[GelModel]
    ) -> None:
        for obj in obj_data:
            obj_dict = obj.__dict__
            gel_id: uuid.UUID = obj.id

            model_or_models: GelModel | IDTracker[GelModel, None]
            if gel_id in self.new_objects:
                model_or_models = self.new_objects[gel_id]
            else:
                model_or_models = self.existing_objects[gel_id]

            models: Iterable[GelModel]
            if isinstance(model_or_models, IDTracker):
                # In some situations we might have multiple objects with
                # the same `.id` - in this case we want to deeep copy mutable
                # `multi prop` and arrays in `single props`.
                models = model_or_models
                deepcopy = copy.deepcopy
            else:
                models = (model_or_models,)
                # But when we have a single object for the given UUID
                # (the most common case) we don't need to copy anything
                # and can just use the collection returned by the codec.
                # `obj` is essentially a throwaway object that will be
                # GCed after sync().
                deepcopy = _identity_func  # type: ignore [assignment]

            for field, new_value in obj_dict.items():
                if field in _STOP_FIELDS:
                    continue

                shape_field = shape.fields[field]
                is_multi = shape_field.cardinality.is_multi()
                is_link = shape_field.kind.is_link()

                for model in models:
                    assert model.id == gel_id
                    model_dict = model.__dict__

                    model.__pydantic_fields_set__.add(field)

                    if is_link and is_multi:
                        link = model_dict.get(field)
                        if link is None:
                            # This instance never had this link, but
                            # now it will.
                            # TODO: this needs to be optimized.
                            link = copy.copy(new_value)
                            model_dict[field] = link
                            link.__gel_replace_with_empty__()

                        assert is_link_abstract_dlist(link)
                        assert type(new_value) is type(link)
                        model_dict[field] = link.__gel_reconcile__(
                            # no need to copy `new_value`,
                            # it will only be iterated over
                            new_value,
                            self.existing_objects,  # type: ignore [arg-type]
                            self.new_objects,  # type: ignore [arg-type]
                        )

                    elif is_link:
                        if shape_field.properties:
                            model_dict[field] = reconcile_proxy_link(
                                existing=model_dict.get(field),
                                refetched=new_value,  # pyright: ignore [reportArgumentType]
                                existing_objects=self.existing_objects,  # type: ignore [arg-type]
                                new_objects=self.new_objects,
                            )
                        else:
                            model_dict[field] = reconcile_link(
                                existing=model_dict.get(field),
                                refetched=new_value,
                                existing_objects=self.existing_objects,  # type: ignore [arg-type]
                                new_objects=self.new_objects,
                            )

                    elif not is_link:
                        # could be a multi prop, could be a single prop
                        # with an array value
                        if shape_field.mutable:
                            model_dict[field] = deepcopy(new_value)
                        else:
                            model_dict[field] = new_value

            # Let's be extra cautious and help GC a bit here. `obj` can
            # have recursive references to other objects via links and
            # computed backlinks.
            obj.__dict__.clear()

    def _commit(self) -> None:
        for obj, new_id in self.new_object_ids.items():
            assert new_id is not None

            if self.refetch:
                updated = self.new_objects[new_id]
                pydantic_set_fields = obj.__pydantic_fields_set__
                for field in updated.__dict__:
                    if field == "id":
                        continue
                    obj.__dict__[field] = updated.__dict__[field]
                    pydantic_set_fields.add(field)

            obj.__gel_commit__(new_id=new_id)

        if self.refetch:
            self._apply_refetched_data()

        self._commit_recursive()

    def _commit_recursive(self) -> None:
        visited: IDTracker[GelModel, None] = IDTracker()

        def _traverse(obj: GelModel) -> None:
            if obj in visited:
                return

            assert not isinstance(obj, ProxyModel)

            visited.track(obj)
            obj.__gel_commit__()

            for prop in get_pointers(type(obj)):
                if prop.computed:
                    # (1) we don't want to traverse computeds (they don't
                    #     form the actual dependency graph, real links do)
                    #
                    # (2) we need to commit changes to multi props and
                    #     multi links
                    continue

                linked = model_attr(obj, prop.name, _unset)
                if linked in _STOP_LINK_TRAVERSAL:
                    # If users mess-up with user-defined types and some
                    # of the data isn't fetched, we don't want to crash
                    # with an AttributeErorr, it's not critical here.
                    # Not fetched means not used, which is good for save().
                    continue

                if prop.kind is PointerKind.Link:
                    if prop.cardinality.is_multi():
                        if is_link_wprops_set(linked):
                            for proxy in linked._items:
                                get_proxy_linkprops(proxy).__gel_commit__()
                                _traverse(unwrap_proxy_no_check(proxy))
                        else:
                            assert is_link_set(linked)
                            for model in linked._items:
                                _traverse(model)
                        linked.__gel_commit__()
                    else:
                        if isinstance(linked, ProxyModel):
                            get_proxy_linkprops(linked).__gel_commit__()
                            _traverse(unwrap_proxy_no_check(linked))
                        else:
                            _traverse(cast("GelModel", linked))

                else:
                    assert prop.kind is PointerKind.Property
                    if prop.cardinality.is_multi():
                        assert is_prop_list(linked)

                        if prop.mutable and not self.refetch:
                            # If we're not refetching, we can't use
                            # nested tracked lists will not get "committed"
                            # (nothing will call "__gel_commit__" on them),
                            # so we have to commit them manually.
                            multi_prop_commit_recursive(linked)
                        else:
                            linked.__gel_commit__()
                    elif prop.mutable and not self.refetch:
                        # Single property can be an array -- in this case
                        # we still need to commit it recursively;
                        # see the above comment.
                        multi_prop_commit_recursive(linked)

            if (
                self.refetch
                and obj in self.new_object_ids
                and (new_obj_id := self.new_object_ids.get(obj))
            ):
                # Update computed properties for new objects
                for prop in get_pointers(type(obj)):
                    if not prop.computed or prop.kind is PointerKind.Link:
                        continue

                    assert prop.kind is PointerKind.Property

                    new_obj = self.new_objects.get(new_obj_id)
                    obj.__dict__[prop.name] = new_obj.__dict__[prop.name]
                    obj.__pydantic_fields_set__.add(prop.name)

        for o in self.objs:
            _traverse(o)

        if self.save_postcheck:
            self._post_commit_check()

    def _post_commit_check(self) -> None:
        # This is only run in debug mode, specifically enabled in our tests
        # to double check that everything is in a proper committed state after
        # save(), e.g.:
        #
        # - all models are committed
        # - all models (and things like link props) have no changes
        # - all collections (multi links, multi props) are in a sound state

        visited: IDTracker[GelModel, None] = IDTracker()

        def _check_recursive(obj: GelModel, path: pathlib.Path) -> None:
            if obj in visited:
                return

            assert not isinstance(obj, ProxyModel)

            visited.track(obj)

            path /= f"{type(obj).__qualname__}:{obj.id}"

            if obj.__gel_get_changed_fields__():
                raise ValueError(f"{path} has changed fields after save")
            if not hasattr(obj, "id"):
                raise ValueError(f"{path} has no id after save()")
            if not isinstance(obj.id, uuid.UUID):
                raise ValueError(f"{path} has non-uuid id after save()")
            if obj.__gel_new__:
                raise ValueError(f"{path} has __gel_new__ set")

            for prop in get_pointers(type(obj)):
                val = model_attr(obj, prop.name, _unset)
                if val in _STOP_LINK_TRAVERSAL:
                    continue

                link_path = path / prop.name

                if prop.kind is PointerKind.Link:
                    if prop.cardinality.is_multi():
                        if is_link_wprops_set(val):
                            val.__gel_post_commit_check__(link_path)
                            for i, proxy in enumerate(val._items):
                                list_path = link_path / str(i)
                                lps = get_proxy_linkprops(proxy)
                                if lps.__gel_get_changed_fields__():
                                    raise ValueError(
                                        f"{list_path} has changed link props "
                                        f"after save"
                                    )
                                unwrapped = unwrap_proxy_no_check(proxy)
                                _check_recursive(unwrapped, list_path)
                        elif not prop.computed:
                            assert is_link_set(val)
                            val.__gel_post_commit_check__(link_path)
                            for i, model in enumerate(val._items):
                                list_path = link_path / str(i)
                                _check_recursive(model, list_path)
                    else:
                        if isinstance(val, ProxyModel):
                            if get_proxy_linkprops(
                                val
                            ).__gel_get_changed_fields__():
                                raise ValueError(
                                    f"{link_path} has changed link props "
                                    f"after save"
                                )
                            _check_recursive(
                                unwrap_proxy_no_check(val), link_path
                            )
                        else:
                            _check_recursive(cast("GelModel", val), link_path)

                else:
                    assert prop.kind is PointerKind.Property
                    if prop.cardinality.is_multi() and not prop.computed:
                        assert is_prop_list(val)
                        val.__gel_post_commit_check__(link_path)

        for o in self.objs:
            _check_recursive(o, pathlib.Path())

        # Final check: make sure that the save plan is empty
        # in case we've missed something in `_check_recursive()`.
        create_batches, updates, _, _ = make_plan(
            self.objs,
            refetch=self.refetch,
            warn_on_large_sync_set=False,
        )
        if create_batches or updates:
            raise ValueError("non-empty save plan after save()")

    def _get_id(self, obj: GelModel) -> uuid.UUID:
        if obj.__gel_new__:
            return self.new_object_ids.get_not_none(obj)
        else:
            return obj.id  # type: ignore [no-any-return]

    def _compile_change(
        self,
        change: ModelChange,
        /,
        *,
        for_insert: bool,
    ) -> CompiledQuery:
        shape_parts: list[str] = []

        args: list[object] = []
        args_types: list[str] = []

        def arg_cast(
            type_ql: str,
        ) -> tuple[str, Callable[[str], str], Callable[[object], object]]:
            # As a workaround for the current limitation
            # of EdgeQL (tuple arguments can't have empty
            # sets as elements, and free objects input
            # hasn't yet landed), we represent empty set
            # as an empty array, and non-empty values as
            # an array of one element. More specifically,
            # if a link property has type `<optional T>`, then
            # its value will be represented here as
            # `<array<tuple<T>>`. When the value is empty
            # set, the argument will be an empty array,
            # when it's non-empty, it will be a one-element
            # array with a one-element tuple. Then to unpack:
            #
            #    @prop := (array_unpack(val).0 limit 1)
            #
            # The nested tuple indirection is needed to support
            # link props that have types of arrays.
            if type_ql.startswith("array<"):
                cast = f"array<tuple<{type_ql}>>"
                ret = lambda x: f"(select std::array_unpack({x}).0 limit 1)"  # noqa: E731
                arg_pack = lambda x: [(x,)] if x is not None else []  # noqa: E731
            else:
                cast = f"array<{type_ql}>"
                ret = lambda x: f"(select std::array_unpack({x}) limit 1)"  # noqa: E731
                arg_pack = lambda x: [x] if x is not None else []  # noqa: E731
            return cast, ret, arg_pack

        def add_arg(
            type_ql: str,
            value: Any,
        ) -> str:
            argnum = str(len(args))
            args_types.append(type_ql)
            args.append(value)
            return f"__data.{argnum}"

        obj = change.model
        type_name = obj_to_name_ql(obj)
        q_type_name = quote_ident(type_name)

        for ch in change.fields.values():
            if isinstance(ch, PropertyChange):
                if ch.info.cardinality.is_optional():
                    tp_ql, tp_unpack, arg_pack = arg_cast(ch.info.typexpr)
                    arg = add_arg(
                        tp_ql,
                        arg_pack(ch.value),
                    )
                    shape_parts.append(
                        f"{quote_ident(ch.name)} := {tp_unpack(arg)}"
                    )
                else:
                    assert ch.value is not None
                    arg = add_arg(ch.info.typexpr, ch.value)
                    shape_parts.append(f"{quote_ident(ch.name)} := {arg}")

            elif isinstance(ch, MultiPropAdd):
                # Since we're passing a set of values packed into an array
                # we need to wrap elements in tuples to allow multi props
                # or arrays etc.

                assign_op = ":=" if for_insert else "+="
                if ch.info.typexpr.startswith("array<"):
                    arg_t = f"array<tuple<{ch.info.typexpr}>>"
                    arg = add_arg(arg_t, [(el,) for el in ch.added])
                    shape_parts.append(
                        f"{quote_ident(ch.name)} {assign_op} "
                        f"std::array_unpack({arg}).0"
                    )
                else:
                    arg_t = f"array<{ch.info.typexpr}>"
                    arg = add_arg(arg_t, ch.added)
                    shape_parts.append(
                        f"{quote_ident(ch.name)} {assign_op} "
                        f"std::array_unpack({arg})"
                    )

            elif isinstance(ch, MultiPropRemove):
                assert not for_insert

                if ch.info.typexpr.startswith("array<"):
                    arg_t = f"array<tuple<{ch.info.typexpr}>>"
                    arg = add_arg(arg_t, [(el,) for el in ch.removed])
                    shape_parts.append(
                        f"{quote_ident(ch.name)} -= std::array_unpack({arg}).0"
                    )
                else:
                    arg_t = f"array<{ch.info.typexpr}>"
                    arg = add_arg(arg_t, ch.removed)
                    shape_parts.append(
                        f"{quote_ident(ch.name)} -= std::array_unpack({arg})"
                    )

            elif isinstance(ch, SingleLinkChange):
                tid = (
                    self._get_id(ch.target) if ch.target is not None else None
                )
                linked_name = ch.info.typexpr

                if ch.props and ch.target is not None:
                    assert ch.props_info is not None
                    assert tid is not None

                    arg_casts = {
                        k: arg_cast(ch.props_info[k].typexpr)
                        for k in ch.props_info
                    }

                    if ch.props.keys() == ch.props_info.keys() or for_insert:
                        # Simple case -- we overwrite all link props

                        sl_subt = [arg_casts[k][0] for k in ch.props_info]

                        sl_args = [
                            tid,
                            *(
                                arg_casts[k][2](ch.props[k])
                                for k in ch.props_info
                            ),
                        ]

                        arg = add_arg(
                            f"tuple<std::uuid, {','.join(sl_subt)}>",
                            sl_args,
                        )

                        subq_shape = [
                            f"@{quote_ident(pname)} := "
                            f"{arg_casts[pname][1](f'{arg}.{i}')}"
                            for i, pname in enumerate(ch.props_info, 1)
                        ]

                        shape_parts.append(
                            f"{quote_ident(ch.name)} := "
                            f"(select (<{linked_name}>{arg}.0) {{ "
                            f"  {', '.join(subq_shape)}"
                            f"}})"
                        )

                    else:
                        # Harder case -- we update *some* props, meaning
                        # that those props that we don't update must retain
                        # their set value in the DB.

                        sl_subt = [
                            f"tuple<std::bool, {arg_casts[k][0]}>"
                            for k in ch.props_info
                        ]

                        sl_args = [
                            tid,
                            *(
                                (
                                    k in ch.props,
                                    arg_casts[k][2](ch.props.get(k)),
                                )
                                for k in ch.props_info
                            ),
                        ]

                        arg = add_arg(
                            f"tuple<std::uuid, {','.join(sl_subt)}>",
                            sl_args,
                        )

                        lps_to_select_shape = ",".join(
                            f"__{quote_ident(k)} := "
                            f"std::array_agg(@{quote_ident(k)})"
                            for k in ch.props_info
                        )

                        lps_to_select_shape_tup = ",".join(
                            f"__m.{quote_ident(ch.name)}.__{quote_ident(k)}"
                            for k in ch.props_info
                        )

                        lp_assign_reload = ", ".join(
                            f"""
                                @{quote_ident(p)} :=
                                (
                                    {arg_casts[p][1](f"{arg}.{i + 1}.1")}
                                    if {arg}.{i + 1}.0 else
                                    (
                                        select std::array_unpack(__lprops.{i})
                                        limit 1
                                    )
                                )
                            """
                            for i, p in enumerate(ch.props_info)
                        )

                        shape_parts.append(
                            f"""
                                {quote_ident(ch.name)} :=
                                (
                                    with __lprops := (
                                        with __m := (
                                            select {q_type_name} {{
                                                {quote_ident(ch.name)}: {{
                                                    {lps_to_select_shape}
                                                }} filter .id = {arg}.0
                                            }}
                                        )
                                        select (
                                            {lps_to_select_shape_tup},
                                        )
                                    )
                                    select (<{linked_name}>{arg}.0) {{
                                        {lp_assign_reload}
                                    }}
                                )
                            """
                        )

                else:
                    if ch.info.cardinality.is_optional():
                        arg = add_arg(
                            "array<std::uuid>",
                            [tid] if tid is not None else [],
                        )
                        shape_parts.append(
                            f"{quote_ident(ch.name)} := "
                            f"<{linked_name}><std::uuid>("
                            f"  select std::array_unpack({arg}) limit 1"
                            f")"
                        )
                    else:
                        assert tid is not None
                        arg = add_arg("std::uuid", tid)
                        shape_parts.append(
                            f"{quote_ident(ch.name)} := "
                            f"<{linked_name}><std::uuid>{arg}"
                        )

            elif isinstance(ch, MultiLinkRemove):
                assert not for_insert

                arg = add_arg(
                    "array<std::uuid>",
                    [self._get_id(o) for o in ch.removed],
                )

                shape_parts.append(
                    f"{quote_ident(ch.name)} -= "
                    f"<{ch.info.typexpr}>std::array_unpack({arg})"
                )

            elif isinstance(ch, MultiLinkAdd):
                new_link = for_insert or ch.replace
                assign_op = ":=" if new_link else "+="

                if ch.added_props:
                    assert ch.props_info

                    link_args: list[tuple[Any, ...]] = []
                    tuple_subt: list[str] | None = None

                    arg_casts = {
                        k: arg_cast(ch.props_info[k].typexpr)
                        for k in ch.props_info
                    }

                    tuple_subt = [
                        f"tuple<std::bool, {arg_casts[k][0]}>"
                        for k in ch.props_info
                    ]

                    for addo, addp in zip(
                        ch.added, ch.added_props, strict=True
                    ):
                        link_args.append(
                            (
                                self._get_id(addo),
                                *(
                                    (k in addp, arg_casts[k][2](addp.get(k)))
                                    for k in ch.props_info
                                ),
                            )
                        )

                    arg = add_arg(
                        f"array<tuple<std::uuid, {','.join(tuple_subt)}>>",
                        link_args,
                    )

                    if new_link:
                        lp_assign = ", ".join(
                            f"@{quote_ident(p)} := "
                            f"{arg_casts[p][1](f'__tup.{i + 1}.1')}"
                            for i, p in enumerate(ch.props_info)
                        )

                        shape_parts.append(
                            f"{quote_ident(ch.name)} {assign_op} "
                            f"assert_distinct(("
                            f"for __tup in std::array_unpack({arg}) union ("
                            f"select (<{ch.info.typexpr}>__tup.0) {{ "
                            f"{lp_assign}"
                            f"}})))"
                        )
                    else:
                        lps_to_select_shape = ",".join(
                            f"std::array_agg(__m@{quote_ident(k)})"
                            for k in ch.props_info
                        )

                        lp_assign_reload = ", ".join(
                            f"""
                                @{quote_ident(p)} :=
                                (
                                    {arg_casts[p][1](f"__tup.{i + 1}.1")}
                                    if __tup.{i + 1}.0 else
                                    (
                                        select std::array_unpack(__lprops.{i})
                                        limit 1
                                    )
                                )
                            """
                            for i, p in enumerate(ch.props_info)
                        )

                        # Re `__lprops` below -- currently this is just about
                        # the only way to "load" existing link properties on
                        # the link and use them later.  We do that to support
                        # "partial" updates to link props -- when only one
                        # changed other should stay as is.
                        shape_parts.append(
                            f"""
                            {quote_ident(ch.name)} {assign_op}
                            assert_distinct((
                                for __tup in array_unpack({arg}) union (
                                    with __lprops := (
                                        for __m in .{quote_ident(ch.name)}
                                        select (
                                            {lps_to_select_shape},
                                        )
                                        filter __m.id = __tup.0
                                    )
                                    select (<{ch.info.typexpr}>__tup.0) {{
                                        {lp_assign_reload}
                                    }}
                                )
                            ))
                            """
                        )

                else:
                    arg = add_arg(
                        "array<std::uuid>",
                        [self._get_id(o) for o in ch.added],
                    )

                    shape_parts.append(
                        f"{quote_ident(ch.name)} {assign_op} "
                        f"assert_distinct("
                        f"<{ch.info.typexpr}>std::array_unpack({arg})"
                        f")"
                    )

            elif isinstance(ch, MultiLinkReset):
                assert not for_insert

                shape_parts.append(
                    f"{quote_ident(ch.name)} := <{ch.info.typexpr}>{{}}"
                )

            else:
                raise TypeError(f"unknown model change {type(ch).__name__}")

        shape = ", ".join(shape_parts)
        query: str

        select_shape = "{*}" if self.refetch and for_insert else ".id"

        if for_insert:
            if shape:
                assert args_types
                query = f"insert {q_type_name} {{ {shape} }}"
            else:
                assert not args_types
                query = f"insert {q_type_name}"
        else:
            assert shape

            arg = add_arg("std::uuid", self._get_id(obj))
            query = f"""\
                update <{q_type_name}>{arg}
                set {{ {shape} }}
            """  # noqa: S608

        ret_args: tuple[object, ...] | int

        if args_types:
            assert args
            ret_args = tuple(args)

            single_query = f"""
                with __query := (
                    with __data := <tuple<{",".join(args_types)}>>$0
                    select ({query})
                ) select __query{select_shape}
            """

            multi_query = f"""
                with __query := (
                    with __all_data := <array<tuple<{",".join(args_types)}>>>$0
                    for __data in std::array_unpack(__all_data) union (
                        ({query})
                    )
                ) select __query{select_shape}
            """

            args_query = f"""
                with __all_data := <array<tuple<{",".join(args_types)}>>>$0
                select std::count(std::array_unpack(__all_data))
            """

        else:
            assert not args
            ret_args = 0

            single_query = f"""
                with __query := (
                    with __data := <int64>$0
                    select ({query})
                ) select __query{select_shape}
            """

            multi_query = f"""
                with __query := (
                    with __all_data := <array<int64>>$0
                    for __data in std::array_unpack(__all_data) union (
                        ({query})
                    )
                ) select __query{select_shape}
            """

            args_query = "select 'no args'"

        return CompiledQuery(
            single_query=single_query,
            multi_query=multi_query,
            args_query=args_query,
            arg=ret_args,
            change=change,
        )


def _warn_on_large_sync(
    case: Literal["new", "refetch"],
    *,
    stacklevel: int,
    num_objects: int,
) -> None:
    warning_body = (
        "* `save()` is much faster than `sync()` if you do not need "
        "to re-fetch all objects from the database\n\n"
        "* in application code you usually want to use `sync()` to make "
        "sure your objects are up to date\n\n"
        "* for data loading scripts `save()` is often preferrable "
        "over `sync()`\n\n"
        "This warning can be silenced by passing "
        "`warn_on_large_sync=False` to `sync()`"
    )

    match case:
        case "new":
            warnings.warn(
                f"`sync()` is creating {num_objects} objects (the threshold "
                f"for this warning is {SYNC_NEW_THRESHOLD}), "
                f"consider the following:\n\n"
                f"{warning_body}",
                UserWarning,
                stacklevel=stacklevel + SYNC_BASE_STACKLEVEL,
            )
        case "refetch":
            warnings.warn(
                f"`sync()` is refetching {num_objects} objects (the threshold "
                f"for this warning is {SYNC_REFETCH_THRESHOLD}), "
                f"consider the following:\n\n"
                f"{warning_body}",
                UserWarning,
                stacklevel=stacklevel + SYNC_BASE_STACKLEVEL,
            )
        case _:
            raise AssertionError("unreachable")
