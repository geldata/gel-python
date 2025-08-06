from __future__ import annotations

import collections
import dataclasses
import pathlib
import weakref
import uuid

from typing import (
    TYPE_CHECKING,
    Any,
    NamedTuple,
    TypeGuard,
    TypeVar,
    Generic,
    cast,
)
from typing_extensions import TypeAliasType, dataclass_transform

from gel._internal._qbmodel._abstract import (
    GelPrimitiveType,
    get_proxy_linkprops,
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
from gel._internal._qbmodel._abstract import (
    DEFAULT_VALUE,
    AbstractLinkSet,
    LinkSet,
    LinkWithPropsSet,
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


T = TypeVar("T")
T_co = TypeVar("T_co", covariant=True)
V = TypeVar("V")

_unset = object()
_STOP_LINK_TRAVERSAL = (None, _unset, DEFAULT_VALUE)

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
class BaseFieldChange:
    name: str
    """Pointer name"""
    info: GelPointerReflection
    """Static pointer schema reflection"""


@_struct
class SingleLinkChange(BaseFieldChange):
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
class MultiLinkReset(BaseFieldChange):
    pass


@_struct
class MultiLinkAdd(BaseFieldChange):
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
class MultiLinkRemove(BaseFieldChange):
    removed: Iterable[GelModel]

    def __post_init__(self) -> None:
        assert not any(isinstance(o, ProxyModel) for o in self.removed)
        assert self.info.cardinality.is_multi()


@_struct
class PropertyChange(BaseFieldChange):
    value: object | None

    def __post_init__(self) -> None:
        assert not self.info.cardinality.is_multi()


@_struct
class MultiPropAdd(BaseFieldChange):
    added: Iterable[object]

    def __post_init__(self) -> None:
        assert self.info.cardinality.is_multi()


@_struct
class MultiPropRemove(BaseFieldChange):
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

    def untrack(self, obj: T) -> None:
        self._seen.pop(id(obj), None)

    def track_many(self, more: Iterable[T] | Iterable[tuple[T, V]], /) -> None:
        for obj in more:
            ret = (obj, None) if not isinstance(obj, tuple) else obj
            self._seen[id(ret[0])] = ret

    def untrack_many(self, more: Iterable[T], /) -> None:
        for obj in more:
            self._seen.pop(id(obj), None)

    def __getitem__(self, obj: T) -> V | None:
        try:
            return self._seen[id(obj)][1]
        except KeyError:
            raise KeyError((id(obj), obj)) from None

    def get_not_none(self, obj: T) -> V:
        try:
            v = self._seen[id(obj)][1]
        except KeyError:
            raise KeyError((id(obj), obj)) from None
        assert v is not None
        return v

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


def is_link_wprops_set(
    val: object,
) -> TypeGuard[LinkWithPropsSet[ProxyModel[GelModel], GelModel]]:
    return isinstance(val, LinkWithPropsSet)


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


def iter_graph(objs: Iterable[GelModel]) -> Iterable[GelModel]:
    # Simple recursive traverse of a model

    visited: IDTracker[GelModel, None] = IDTracker()

    def _traverse(obj: GelModel) -> Iterable[GelModel]:
        if obj in visited:
            return

        visited.track(obj)
        yield obj

        for prop in get_pointers(type(obj)):
            if prop.computed or prop.kind is not PointerKind.Link:
                # We don't want to traverse computeds (they don't form the
                # actual dependency graph, real links do)
                continue

            linked = getattr(obj, prop.name, _unset)
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

        linked = getattr(obj, prop.name, _unset)
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


def has_changes_recursive(
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

        for item in collection:
            if isinstance(item, AbstractCollection):
                if item.__gel_has_changes__():
                    return True

            if isinstance(item, (list, tuple, AbstractCollection)):
                if walk(item):
                    return True

        return False

    return walk(val)


def make_plan(objs: Iterable[GelModel]) -> SavePlan:
    insert_ops: ChangeBatch = []
    update_ops: ChangeBatch = []

    for obj in iter_graph(objs):
        pointers = get_pointers(type(obj))
        is_new = obj.__gel_new__

        # Capture changes in *properties* and *single links*
        field_changes = obj.__gel_get_changed_fields__()

        requireds: FieldChangeMap = {}
        sched: FieldChangeLists = collections.defaultdict(list)

        for prop in pointers:
            # Skip computeds, we can't update them.
            if prop.computed or prop.readonly:
                # Exclude computeds but also props like `__type__` and `id`
                continue

            # Iterate through changes of *propertes* and *single links*.
            if prop.name in field_changes:
                # Since there was a change and we don't implement `del`,
                # the attribute must be set

                val = getattr(obj, prop.name)
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
                            p.name: getattr(val_lp, p.name, None) for p in ptrs
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
                val = getattr(obj, prop.name, _unset)
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
                val = getattr(obj, prop.name, _unset)
                if val in _STOP_LINK_TRAVERSAL:
                    continue

                if isinstance(
                    val, (tuple, list, AbstractTrackedList)
                ) and has_changes_recursive(val):
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
                val = getattr(obj, prop.name, _unset)
                if val in _STOP_LINK_TRAVERSAL:
                    continue
                assert isinstance(val, ProxyModel)
                lprops = get_proxy_linkprops(val)
                if not lprops.__gel_changed_fields__:
                    continue

                # An existing single link has updated link props.

                ptrs = get_pointers(type(val).__linkprops__)
                props = {
                    p: getattr(lprops, p, None)
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

            linked = getattr(obj, prop.name, _unset)
            if linked is _unset or linked is DEFAULT_VALUE:
                # The link wasn't fetched at all
                continue

            # `linked` should be either an empty LinkSet or
            # a non-empty one
            assert linked is not None
            assert is_link_abstract_dlist(linked), (
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
                        p: getattr(link_lp, p, None)
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

    return SavePlan(insert_batches, update_ops)


def make_save_executor_constructor(
    objs: tuple[GelModel, ...],
    *,
    refetch: bool,
    save_postcheck: bool = False,
) -> Callable[[], SaveExecutor]:
    create_batches, updates = make_plan(objs)
    return lambda: SaveExecutor(
        objs=objs,
        create_batches=create_batches,
        updates=updates,
        refetch=refetch,
        save_postcheck=save_postcheck,
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

    def feed_db_data(self, obj_data: Iterable[Any]) -> None:
        if not self.executor.refetch:
            # in this case `obj_data` is a list of UUIDs
            if not self.insert:
                return
            for obj_upd, change in zip(obj_data, self.changes, strict=True):
                self.executor.object_ids[id(change.model)] = obj_upd
            return

        # In this case `obj_data` is a list of GelModel instances
        # unpacked from `update` and `insert` queries generated by `save()`,
        # as they're wrapped in `select {*}` to refetch the data.
        for obj_upd, change in zip(obj_data, self.changes, strict=True):
            for field in obj_upd.__dict__:
                if field in {"id", "__tid__"}:
                    continue
                change.model.__dict__[field] = obj_upd.__dict__[field]
            if self.insert:
                self.executor.object_ids[id(change.model)] = obj_upd.id


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
    refetch: bool
    save_postcheck: bool

    object_ids: dict[int, uuid.UUID] = dataclasses.field(init=False)

    def __post_init__(self) -> None:
        self.object_ids = {}

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
            key=lambda x: (x[0].__gel_reflection__.id, x[1].single_query)
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
                query=TypeWrapper(lqs[0][0], lqs[0][1].multi_query)
                if self.refetch
                else lqs[0][1].multi_query,
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

    def commit(self) -> None:
        visited: IDTracker[GelModel, None] = IDTracker()

        def _traverse(obj: GelModel) -> None:
            if obj in visited:
                return

            assert not isinstance(obj, ProxyModel)

            visited.track(obj)
            obj.__gel_commit__(
                new_id=self.object_ids.get(id(obj)),
                refetch_mode=self.refetch,
            )

            for prop in get_pointers(type(obj)):
                if prop.computed:
                    # (1) we don't want to traverse computeds (they don't
                    #     form the actual dependency graph, real links do)
                    #
                    # (2) we need to commit changes to multi props and
                    #     multi links
                    continue

                linked = getattr(obj, prop.name, _unset)
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
                                get_proxy_linkprops(proxy).__gel_commit__(
                                    refetch_mode=self.refetch
                                )
                                _traverse(unwrap_proxy_no_check(proxy))
                        else:
                            assert is_link_set(linked)
                            for model in linked._items:
                                _traverse(model)
                        linked.__gel_commit__(refetch_mode=self.refetch)
                    else:
                        if isinstance(linked, ProxyModel):
                            get_proxy_linkprops(linked).__gel_commit__(
                                refetch_mode=self.refetch
                            )
                            _traverse(unwrap_proxy_no_check(linked))
                        else:
                            _traverse(cast("GelModel", linked))

                else:
                    assert prop.kind is PointerKind.Property
                    if prop.cardinality.is_multi():
                        assert is_prop_list(linked)
                        linked.__gel_commit__(refetch_mode=self.refetch)

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
                val = getattr(obj, prop.name, _unset)
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
                        else:
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
        create_batches, updates = make_plan(self.objs)
        if create_batches or updates:
            raise ValueError("non-empty save plan after save()")

    def _get_id(self, obj: GelModel) -> uuid.UUID:
        if obj.__gel_new__:
            return self.object_ids[id(obj)]
        else:
            return obj.id  # type: ignore [no-any-return]

    def _compile_change(
        self, change: ModelChange, /, *, for_insert: bool
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

        select_shape = "{*}" if self.refetch else ".id"

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
