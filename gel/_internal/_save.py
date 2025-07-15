from __future__ import annotations

import collections
import dataclasses
import weakref

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

from gel._internal._qbmodel._abstract import GelPrimitiveType
from gel._internal._qbmodel._pydantic._models import (
    GelModel,
    GelSourceModel,
    ProxyModel,
)
from gel._internal._tracked_list import (
    TrackedList,
    DowncastingTrackedList,
)
from gel._internal._qbmodel._abstract import (
    AbstractDistinctList,
    DistinctList,
    ProxyDistinctList,
)
from gel._internal._edgeql import PointerKind, quote_ident

if TYPE_CHECKING:
    import uuid
    from collections.abc import (
        Iterable,
        Iterator,
        Callable,
        Sequence,
    )

    from gel._internal._qb import GelPointerReflection


T = TypeVar("T")
V = TypeVar("V")

_unset = object()

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
            self.props_info is not None
            and self.props is not None
            and self.props_info.keys() == self.props.keys()
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
            and next(iter(self.added_props)).keys() == self.props_info.keys()
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


def is_link_dlist(val: object) -> TypeGuard[DistinctList[GelModel]]:
    return isinstance(val, DistinctList) and issubclass(
        type(val).type, GelModel
    )


def is_link_abstract_dlist(
    val: object,
) -> TypeGuard[AbstractDistinctList[GelModel]]:
    return isinstance(val, AbstractDistinctList) and issubclass(
        type(val).type, GelModel
    )


def is_proxy_link_list(
    val: object,
) -> TypeGuard[ProxyDistinctList[ProxyModel[GelModel], GelModel]]:
    return isinstance(val, ProxyDistinctList)


def unwrap_proxy(val: GelModel) -> GelModel:
    if isinstance(val, ProxyModel):
        # This is perf-sensitive function as it's called on
        # every edge of the graph multiple times.
        return ll_attr(val, "_p__obj__")  # type: ignore [no-any-return]
    else:
        return val


def unwrap_proxy_no_check(val: ProxyModel[GelModel]) -> GelModel:
    return ll_attr(val, "_p__obj__")  # type: ignore [no-any-return]


def unwrap_dlist(val: Iterable[GelModel]) -> list[GelModel]:
    return [unwrap_proxy(o) for o in val]


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
            if linked is _unset or linked is None:
                # If users mess-up with user-defined types and smoe
                # of the data isn't fetched, we don't want to crash
                # with an AttributeErorr, it's not critical here.
                # Not fetched means not used, which is good for save().
                continue

            if prop.cardinality.is_multi():
                if is_proxy_link_list(linked):
                    for proxy in linked._items:
                        yield from _traverse(unwrap_proxy_no_check(proxy))
                else:
                    assert is_link_dlist(linked)
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
        if linked is _unset:
            # See _get_all_deps for explanation.
            continue

        if prop.cardinality.is_multi():
            if is_proxy_link_list(linked):
                for pmod in linked._items:
                    unwrapped = unwrap_proxy_no_check(pmod)
                    if unwrapped.__gel_new__ and unwrapped not in visited:
                        visited.track(unwrapped)
                        yield unwrapped
            else:
                assert is_link_dlist(linked)
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
    """Push a fiekd change either to *requireds* or *sched*."""

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
            if prop.computed:
                continue

            # Iterate through changes of *propertes* and *single links*.
            if prop.name in field_changes:
                # Since there was a change and we don't implement `del`,
                # the attribute must be set

                if prop.name == "id":
                    # id is special, we don't need to track it
                    continue

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
                    # Single link.
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
                            ll_attr(
                                val, "__linkprops__"
                            ).__gel_get_changed_fields__()
                        )

                    if link_prop_variant:
                        assert isinstance(val, ProxyModel)
                        # Link with link properties
                        ptrs = get_pointers(type(val).__linkprops__)

                        props = {
                            p.name: getattr(
                                ll_attr(val, "__linkprops__"), p.name, None
                            )
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
                val = getattr(obj, prop.name, _unset)
                if val is None or val is _unset:
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
            if linked is _unset:
                # The link wasn't fetched at all
                continue

            # `linked` should be either an empty DistinctList or
            # a non-empty one
            assert linked is not None
            assert is_link_abstract_dlist(linked), (
                f"`linked` is not dlist, it is {type(linked)}"
            )

            replace = linked.__gel_overwrite_data__

            removed = linked.__gel_get_removed__()
            if removed:
                # 'replace' is set for new collections only
                assert not replace

                # TODO: this should probably be handled in the DistinctList
                # itself (GelModel and DistinctList are tightly coupled anyway)
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
            # - Why aern't we capturing which link props have changes
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

                    lp = ll_attr(el, "__linkprops__")
                    if lp.__gel_get_changed_fields__():
                        added.append(el)

            # No adds or changes for this link?
            if not added:
                if replace:
                    # Need to reset this link to an empty list
                    push_change(
                        requireds,
                        sched,
                        MultiLinkReset(
                            name=prop.name,
                            info=prop,
                        ),
                    )
                else:
                    # Continue to the next object
                    continue

            added_proxies: Sequence[ProxyModel[GelModel]] = added  # type: ignore [assignment]

            # Simple case -- no link props!
            if not prop.properties or all(
                not ll_attr(link, "__linkprops__").__gel_get_changed_fields__()
                for link in added_proxies
            ):
                mch = MultiLinkAdd(
                    name=prop.name,
                    info=prop,
                    added=unwrap_dlist(added),
                    replace=replace,
                )
                push_change(requireds, sched, mch)
                continue

            # OK, we have to deal with link props
            #
            # First, we iterate through the list of added new objects
            # to the list. All of them should be ProxyModels
            # (as we use ProxyDistinctList for links with props.)
            # Our goal is to segregate different combinations of
            # set link properties into separate groups.
            link_tp = type(added_proxies[0])
            assert issubclass(link_tp, ProxyModel)
            props_info = get_pointers(link_tp.__linkprops__)

            mch = MultiLinkAdd(
                name=prop.name,
                info=prop,
                added=unwrap_dlist(added),
                added_props=[
                    {
                        p.name: getattr(
                            ll_attr(link, "__linkprops__"), p.name, None
                        )
                        for p in props_info
                    }
                    for link in cast("list[ProxyModel[GelModel]]", added)
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
) -> Callable[[], SaveExecutor]:
    create_batches, updates = make_plan(objs)
    return lambda: SaveExecutor(objs, create_batches, updates)


# How many INSERT/UPDATE operations can be combined into a single query.
# Significantly improves performance, but appears to hit the ceiling of
# improvements at around 100.
MAX_BATCH_SIZE = 1280


@_struct
class QueryBatch:
    executor: SaveExecutor
    query: str
    args_query: str
    args: list[tuple[object, ...] | int]
    changes: list[ModelChange]
    insert: bool

    def feed_ids(self, obj_ids: Iterable[uuid.UUID]) -> None:
        if not self.insert:
            return
        for obj_id, change in zip(obj_ids, self.changes, strict=True):
            self.executor.object_ids[id(change.model)] = obj_id


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

    object_ids: dict[int, uuid.UUID] = dataclasses.field(init=False)

    def __post_init__(self) -> None:
        self.object_ids = {}

    def _compile_batch(
        self, batch: ChangeBatch, /, *, for_insert: bool
    ) -> list[QueryBatch]:
        compiled = [
            self._compile_change(obj, for_insert=for_insert) for obj in batch
        ]

        # Queries must be independent of each other within the same
        # ChangeBatch, so we can sort them to group queries.
        compiled.sort(key=lambda x: x.single_query)

        icomp = iter(compiled)
        local_queries = [[next(icomp)]]

        for cq in icomp:
            if (
                cq.single_query == local_queries[-1][0].single_query
                and len(local_queries[-1]) < MAX_BATCH_SIZE
            ):
                local_queries[-1].append(cq)
            else:
                local_queries.append([cq])

        return [
            QueryBatch(
                executor=self,
                query=lqs[0].multi_query,
                args_query=lqs[0].args_query,
                args=[lq.arg for lq in lqs],
                changes=[lq.change for lq in lqs],
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
            obj.__gel_commit__(self.object_ids.get(id(obj)))

            for prop in get_pointers(type(obj)):
                if prop.computed:
                    # (1) we don't want to traverse computeds (they don't
                    #     form the actual dependency graph, real links do)
                    #
                    # (2) we need to commit changes to multi props and
                    #     multi links
                    continue

                linked = getattr(obj, prop.name, _unset)
                if linked is _unset or linked is None:
                    # If users mess-up with user-defined types and some
                    # of the data isn't fetched, we don't want to crash
                    # with an AttributeErorr, it's not critical here.
                    # Not fetched means not used, which is good for save().
                    continue

                if prop.kind is PointerKind.Link:
                    if prop.cardinality.is_multi():
                        if is_proxy_link_list(linked):
                            for proxy in linked._items:
                                ll_attr(
                                    proxy, "__linkprops__"
                                ).__gel_commit__()
                                _traverse(unwrap_proxy_no_check(proxy))
                        else:
                            assert is_link_dlist(linked)
                            for model in linked._items:
                                _traverse(model)
                        linked.__gel_commit__()
                    else:
                        if isinstance(linked, ProxyModel):
                            ll_attr(linked, "__linkprops__").__gel_commit__()
                            _traverse(unwrap_proxy_no_check(linked))
                        else:
                            _traverse(cast("GelModel", linked))

                else:
                    assert prop.kind is PointerKind.Property
                    if prop.cardinality.is_multi():
                        assert is_prop_list(linked)
                        linked.__gel_commit__()

        for o in self.objs:
            _traverse(o)

    def _get_id(self, obj: GelModel) -> uuid.UUID:
        if obj.__gel_new__:
            return self.object_ids[id(obj)]
        else:
            return obj.id

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
                ret = lambda x: f"(select array_unpack({x}).0 limit 1)"  # noqa: E731
                arg_pack = lambda x: [(x,)] if x is not None else []  # noqa: E731
            else:
                cast = f"array<{type_ql}>"
                ret = lambda x: f"(select array_unpack({x}) limit 1)"  # noqa: E731
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
                        f"array_unpack({arg}).0"
                    )
                else:
                    arg_t = f"array<{ch.info.typexpr}>"
                    arg = add_arg(arg_t, ch.added)
                    shape_parts.append(
                        f"{quote_ident(ch.name)} {assign_op} "
                        f"array_unpack({arg})"
                    )

            elif isinstance(ch, MultiPropRemove):
                assert not for_insert

                if ch.info.typexpr.startswith("array<"):
                    arg_t = f"array<tuple<{ch.info.typexpr}>>"
                    arg = add_arg(arg_t, [(el,) for el in ch.removed])
                    shape_parts.append(
                        f"{quote_ident(ch.name)} -= array_unpack({arg}).0"
                    )
                else:
                    arg_t = f"array<{ch.info.typexpr}>"
                    arg = add_arg(arg_t, ch.removed)
                    shape_parts.append(
                        f"{quote_ident(ch.name)} -= array_unpack({arg})"
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

                    sl_subt = [arg_casts[k][0] for k in ch.props_info]

                    sl_args = [
                        tid,
                        *(arg_casts[k][2](ch.props[k]) for k in ch.props_info),
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
                    if ch.info.cardinality.is_optional():
                        arg = add_arg(
                            "array<std::uuid>",
                            [tid] if tid is not None else [],
                        )
                        shape_parts.append(
                            f"{quote_ident(ch.name)} := "
                            f"<{linked_name}><std::uuid>("
                            f"  select array_unpack({arg}) limit 1"
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
                    f"<{ch.info.typexpr}>array_unpack({arg})"
                )

            elif isinstance(ch, MultiLinkAdd):
                assign_op = ":=" if (for_insert or ch.replace) else "+="

                if ch.added_props:
                    assert ch.props_info

                    link_args: list[tuple[Any, ...]] = []
                    tuple_subt: list[str] | None = None

                    arg_casts = {
                        k: arg_cast(ch.props_info[k].typexpr)
                        for k in ch.props_info
                    }

                    tuple_subt = [arg_casts[k][0] for k in ch.props_info]

                    for addo, addp in zip(
                        ch.added, ch.added_props, strict=True
                    ):
                        link_args.append(
                            (
                                self._get_id(addo),
                                *(
                                    arg_casts[k][2](addp[k])
                                    for k in ch.props_info
                                ),
                            )
                        )

                    arg = add_arg(
                        f"array<tuple<std::uuid, {','.join(tuple_subt)}>>",
                        link_args,
                    )

                    lp_assign = ", ".join(
                        f"@{quote_ident(p)} := "
                        f"{arg_casts[p][1](f'__tup.{i + 1}')}"
                        for i, p in enumerate(ch.props_info)
                    )

                    shape_parts.append(
                        f"{quote_ident(ch.name)} {assign_op} "
                        f"assert_distinct(("
                        f"for __tup in array_unpack({arg}) union ("
                        f"select (<{ch.info.typexpr}>__tup.0) {{ "
                        f"{lp_assign}"
                        f"}})))"
                    )

                else:
                    arg = add_arg(
                        "array<std::uuid>",
                        [self._get_id(o) for o in ch.added],
                    )

                    shape_parts.append(
                        f"{quote_ident(ch.name)} {assign_op} "
                        f"assert_distinct("
                        f"<{ch.info.typexpr}>array_unpack({arg})"
                        f")"
                    )

            elif isinstance(ch, MultiLinkReset):
                assert not for_insert

                shape_parts.append(
                    f"{quote_ident(ch.name)} := <{ch.info.typexpr}>{{}}"
                )

            else:
                raise TypeError(f"unknown model change {type(ch).__name__}")

        q_type_name = quote_ident(type_name)

        shape = ", ".join(shape_parts)
        query: str
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
                update {q_type_name}
                filter .id = {arg}
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
                ) select __query.id
            """

            multi_query = f"""
                with __query := (
                    with __all_data := <array<tuple<{",".join(args_types)}>>>$0
                    for __data in array_unpack(__all_data) union (
                        ({query})
                    )
                ) select __query.id
            """

            args_query = f"""
                with __all_data := <array<tuple<{",".join(args_types)}>>>$0
                select count(array_unpack(__all_data))
            """

        else:
            assert not args
            ret_args = 0

            single_query = f"""
                with __query := (
                    with __data := <int64>$0
                    select ({query})
                ) select __query.id
            """

            multi_query = f"""
                with __query := (
                    with __all_data := <array<int64>>$0
                    for __data in array_unpack(__all_data) union (
                        ({query})
                    )
                ) select __query.id
            """

            args_query = "select 'no args'"

        return CompiledQuery(
            single_query=single_query,
            multi_query=multi_query,
            args_query=args_query,
            arg=ret_args,
            change=change,
        )
