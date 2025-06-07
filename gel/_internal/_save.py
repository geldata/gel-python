from __future__ import annotations

import collections
import dataclasses

from typing import TYPE_CHECKING, Any, NamedTuple, TypeVar, Generic, cast

from gel._internal._qbmodel._pydantic._models import (
    GelModel,
    ProxyModel,
    GelPointers,
    Pointer,
)
from gel._internal._dlist import DistinctList
from gel._internal._unsetid import UNSET_UUID
from gel._internal._edgeql import PointerKind, quote_ident

if TYPE_CHECKING:
    import uuid

    from collections.abc import (
        Iterable,
        Iterator,
        Callable,
        Mapping,
    )
    from typing import TypeGuard

    from gel._internal._qbmodel._abstract import GelType


from typing_extensions import TypeAliasType


T = TypeVar("T")
V = TypeVar("V")

_unset = object()


LinkPropertiesValues = TypeAliasType(
    "LinkPropertiesValues", dict[str, object | None]
)


@dataclasses.dataclass(frozen=True)
class SingleLinkChange:
    link_name: str
    info: Pointer

    target: GelModel | None

    props_info: GelPointers | None = None
    props: LinkPropertiesValues | None = None

    def __post_init__(self) -> None:
        assert not isinstance(self.target, ProxyModel)


@dataclasses.dataclass(frozen=True)
class MultiLinkAdd:
    link_name: str
    info: Pointer

    added: Iterable[GelModel]

    added_props: Iterable[LinkPropertiesValues] | None = None
    props_info: GelPointers | None = None

    def __post_init__(self) -> None:
        assert not any(isinstance(o, ProxyModel) for o in self.added)


@dataclasses.dataclass(frozen=True)
class MultiLinkRemove:
    link_name: str
    info: Pointer

    removed: Iterable[GelModel]

    def __post_init__(self) -> None:
        assert not any(isinstance(o, ProxyModel) for o in self.removed)


@dataclasses.dataclass(frozen=True)
class PropertyChange:
    prop_name: str
    info: Pointer

    value: object | None


# Changes are organized by pointer name. Each pointer can have
# have multiple ops.
PropertyChanges = TypeAliasType("PropertyChanges", dict[str, PropertyChange])
SingleLinkChanges = TypeAliasType(
    "SingleLinkChanges", dict[str, SingleLinkChange]
)


@dataclasses.dataclass
class ModelChange:
    model: GelModel
    props: PropertyChanges | None = None
    single_links: SingleLinkChanges | None = None
    multi_links: dict[str, MultiLinkAdd | MultiLinkRemove] | None = None

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
    uodate_batch: ChangeBatch


QueryWithArgs = TypeAliasType("QueryWithArgs", tuple[str, list[object]])


def shift_dict_list(inp: dict[str, list[T]]) -> dict[str, T]:
    ret: dict[str, T] = {}
    for key, lst in list(inp.items()):
        if not lst:
            continue
        ret[key] = lst.pop(0)
        if not lst:
            inp.pop(key)
    return ret


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


def is_dlist(val: object) -> TypeGuard[DistinctList[GelModel]]:
    return isinstance(val, DistinctList) and issubclass(
        type(val).type, GelModel
    )


def unwrap_proxy(val: GelModel) -> GelModel:
    if isinstance(val, ProxyModel):
        assert isinstance(val._p__obj__, GelModel)
        return val._p__obj__
    else:
        return val


def unwrap_dlist(val: Iterable[GelModel]) -> list[GelModel]:
    return [unwrap_proxy(o) for o in val]


def unwrap(val: GelModel) -> tuple[ProxyModel[GelModel] | None, GelModel]:
    if isinstance(val, ProxyModel):
        assert isinstance(val._p__obj__, GelModel)
        return val, val._p__obj__
    else:
        return None, val


def iter_graph(objs: Iterable[GelModel]) -> Iterable[GelModel]:
    # Simple recursive traverse of a model

    visited: IDTracker[GelModel, None] = IDTracker()

    def _traverse(
        parent_obj: GelModel | None, parent_link: str | None, obj: GelModel
    ) -> Iterable[GelModel]:
        obj = unwrap_proxy(obj)

        if obj in visited:
            return

        visited.track(obj)
        yield obj

        for prop in type(obj).__gel_pointers__().values():
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
                assert is_dlist(linked)
                for ref in linked:
                    yield from _traverse(obj, prop.name, ref)
            else:
                assert isinstance(linked, GelModel)
                yield from _traverse(obj, prop.name, linked)

    for o in objs:
        yield from _traverse(None, None, o)


def get_linked_new_objects(obj: GelModel) -> Iterable[GelModel]:
    visited: IDTracker[GelModel, None] = IDTracker()
    visited.track(obj)

    for prop in type(obj).__gel_pointers__().values():
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
            assert is_dlist(linked)
            for ref in linked:
                unwrapped = unwrap_proxy(ref)
                if unwrapped.id is UNSET_UUID and unwrapped not in visited:
                    visited.track(unwrapped)
                    yield unwrapped

        elif linked is not None:
            assert isinstance(linked, GelModel)
            unwrapped = unwrap_proxy(linked)
            if unwrapped.id is UNSET_UUID and unwrapped not in visited:
                visited.track(unwrapped)
                yield unwrapped


def type_to_ql(tp: type[GelType]) -> str:
    return quote_ident(tp.__gel_reflection__.name.as_schema_name())


def obj_to_name_ql(obj: GelModel) -> str:
    return quote_ident(type(obj).__gel_reflection__.name.as_schema_name())


def make_plan(objs: Iterable[GelModel]) -> SavePlan:
    insert_ops: ChangeBatch = []
    update_ops: ChangeBatch = []

    for obj in iter_graph(objs):
        pointers = type(obj).__gel_pointers__()
        is_new = obj.id is UNSET_UUID

        # Capture changes in *properties* and *single links*
        field_changes = obj.__gel_get_changed_fields__()

        prop_changes: PropertyChanges = {}

        req_single_link_changes: SingleLinkChanges = {}
        opt_single_link_changes: SingleLinkChanges = {}

        req_multi_link_adds: dict[str, list[MultiLinkAdd]] = (
            collections.defaultdict(list)
        )
        opt_multi_link_adds: dict[str, list[MultiLinkAdd]] = (
            collections.defaultdict(list)
        )

        multi_link_rems: dict[str, MultiLinkRemove] = {}

        for prop in pointers.values():
            # Skip computeds, we can't update them.
            if prop.computed:
                continue

            # Iterate through changes of *propertes* and *single links*.
            if prop.name in field_changes:
                # Since there was a change and we don't implement `del`,
                # the attribute must be set
                val = getattr(obj, prop.name)
                if prop.kind is PointerKind.Property:
                    # Property
                    assert not isinstance(val, GelModel)
                    prop_changes[prop.name] = PropertyChange(
                        prop_name=prop.name,
                        value=val,
                        info=prop,
                    )
                else:
                    # Link
                    assert prop.kind is PointerKind.Link
                    assert isinstance(val, GelModel)
                    assert not prop.cardinality.is_multi()

                    if prop.has_props and isinstance(val, ProxyModel):
                        # Link with link properties

                        link_changed_fields = (
                            val.__linkprops__.__gel_get_changed_fields__()
                        )

                        props = {
                            n: getattr(val.__linkprops__, n)
                            for n in link_changed_fields
                        }

                        sch = SingleLinkChange(
                            link_name=prop.name,
                            info=prop,
                            target=unwrap_proxy(val),
                            props=props,
                            props_info=type(val).__lprops__.__gel_pointers__(),
                        )
                    else:
                        # Link without link properties
                        sch = SingleLinkChange(
                            link_name=prop.name,
                            info=prop,
                            target=val,
                        )

                    if prop.cardinality.is_optional():
                        opt_single_link_changes[prop.name] = sch
                    else:
                        req_single_link_changes[prop.name] = sch

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
            assert is_dlist(linked), (
                f"`linked` is not dlist, it is {type(linked)}"
            )
            assert issubclass(prop.type, DistinctList)

            removed = linked.__gel_get_removed__()
            if removed:
                m_rem = MultiLinkRemove(
                    link_name=prop.name,
                    info=prop,
                    removed=unwrap_dlist(removed),
                )

                multi_link_rems[prop.name] = m_rem

            added = linked.__gel_get_added__()
            # No adds for this link? Continue to the next one.
            if not added:
                continue

            # Simple case -- no link props!
            if not prop.has_props:
                mch = MultiLinkAdd(
                    link_name=prop.name,
                    info=prop,
                    added=unwrap_dlist(added),
                )

                if prop.cardinality.is_optional():
                    opt_multi_link_adds[prop.name].append(mch)
                else:
                    req_multi_link_adds[prop.name].append(mch)

                continue

            # OK, we have to deal with link props.
            #
            # First, we iterate through the list of added new objects
            # to the list. All of them should be ProxyModels
            # (as we use UpcastingDistinctList for links with props.)
            # Our goal is to segregate different combinations of
            # set link properties into separate groups.
            props_map: dict[
                frozenset[str],
                list[tuple[GelModel, LinkPropertiesValues]],
            ] = collections.defaultdict(list)
            for link in added:
                assert isinstance(link, ProxyModel)

                link_changed_fields = (
                    link.__linkprops__.__gel_get_changed_fields__()
                )
                if link_changed_fields is None:
                    # For this specific link no link property was
                    # ever set or changed
                    props_map[frozenset()].append((link, {}))
                else:
                    link_changed_fields = frozenset(link_changed_fields)
                    lps = {
                        n: getattr(link.__linkprops__, n)
                        for n in link_changed_fields
                    }
                    props_map[link_changed_fields].append((link, lps))

            link_tp = prop.type
            assert issubclass(link_tp.type, ProxyModel)
            props_info = link_tp.type.__lprops__.__gel_pointers__()

            # Generate a MultiLinkAdd op for every distinct
            # link props pattern.
            for link_fields, link_upd in props_map.items():
                link_added = [unwrap_proxy(upd[0]) for upd in link_upd]
                if len(link_fields):
                    link_added_props = [upd[1] for upd in link_upd]
                else:
                    link_added_props = None

                mch = MultiLinkAdd(
                    link_name=prop.name,
                    info=prop,
                    added=link_added,
                    added_props=link_added_props,
                    props_info=props_info if link_added_props else None,
                )

                if prop.cardinality.is_optional():
                    opt_multi_link_adds[prop.name].append(mch)
                else:
                    req_multi_link_adds[prop.name].append(mch)

        # After all this work we found no changes -- move on to the next obj.
        if not (
            is_new
            or prop_changes
            or opt_single_link_changes
            or req_single_link_changes
            or opt_multi_link_adds
            or req_multi_link_adds
            or multi_link_rems
        ):
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
                    props=prop_changes,
                    single_links=req_single_link_changes,
                    multi_links=cast(
                        "dict[str, MultiLinkAdd | MultiLinkRemove]",
                        shift_dict_list(req_multi_link_adds),
                    ),
                )
            )

            prop_changes = {}
            req_single_link_changes = {}

        # Now let's create ops for all the remaning changes
        while (
            prop_changes
            or req_multi_link_adds
            or opt_multi_link_adds
            or req_single_link_changes
            or opt_single_link_changes
            or multi_link_rems
        ):
            change = ModelChange(
                model=obj,
            )

            if prop_changes:
                change.props = prop_changes
                prop_changes = {}

            if req_single_link_changes:
                change.single_links = req_single_link_changes
                req_single_link_changes = {}

            if opt_single_link_changes:
                if change.single_links is not None:
                    change.single_links |= opt_single_link_changes
                else:
                    change.single_links = opt_single_link_changes
                opt_single_link_changes = {}

            if req_multi_link_adds:
                mlinks = cast(
                    "dict[str, MultiLinkAdd | MultiLinkRemove]",
                    shift_dict_list(req_multi_link_adds),
                )

                if opt_multi_link_adds:
                    mlinks |= cast(
                        "dict[str, MultiLinkAdd | MultiLinkRemove]",
                        shift_dict_list(opt_multi_link_adds),
                    )

                if multi_link_rems and not (
                    mlinks.keys() & multi_link_rems.keys()
                ):
                    mlinks |= multi_link_rems
                    multi_link_rems = {}

                change.multi_links = mlinks

            elif opt_multi_link_adds:
                change.multi_links = cast(
                    "dict[str, MultiLinkAdd | MultiLinkRemove]",
                    shift_dict_list(
                        opt_multi_link_adds,
                    ),
                )

            elif multi_link_rems:
                change.multi_links = cast(
                    "dict[str, MultiLinkAdd | MultiLinkRemove]",
                    multi_link_rems,
                )
                multi_link_rems = {}

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
            raise RuntimeError(
                f"Cannot resolve dependencies "
                f"among objects: {remaining_to_make}"
            )

        insert_batches.append(
            [obj_model_index.get_not_none(mod) for mod in ready]
        )
        inserted.track_many(ready)
        remaining_to_make.untrack_many(ready)

    return SavePlan(insert_batches, update_ops)


class ParamBuilder:
    _param_cnt: int

    def __init__(self) -> None:
        self._param_cnt = 0

    def __call__(self) -> str:
        self._param_cnt += 1
        return f"__p_{self._param_cnt}"


def make_save_executor_constructor(
    objs: tuple[GelModel, ...],
) -> Callable[[], SaveExecutor]:
    create_batches, updates = make_plan(objs)
    return lambda: SaveExecutor(objs, create_batches, updates)


@dataclasses.dataclass
class SaveExecutor:
    objs: tuple[GelModel, ...]
    create_batches: list[ChangeBatch]
    updates: ChangeBatch

    object_ids: dict[int, uuid.UUID] = dataclasses.field(init=False)
    param_builder: ParamBuilder = dataclasses.field(init=False)
    iter_index: int = dataclasses.field(init=False)

    def __post_init__(self) -> None:
        self.object_ids = {}
        self.param_builder = ParamBuilder()
        self.iter_index = 0

    def __iter__(self) -> Iterator[list[QueryWithArgs]]:
        return self

    def __next__(self) -> list[QueryWithArgs]:
        if self.iter_index < len(self.create_batches):
            batch = self.create_batches[self.iter_index]
            self.iter_index += 1
            return [
                self._compile_change(obj, for_insert=True) for obj in batch
            ]
        elif self.iter_index == len(self.create_batches):
            self.iter_index += 1
            return [
                self._compile_change(change, for_insert=False)
                for change in self.updates
            ]
        else:
            raise StopIteration

    def feed_ids(self, obj_ids: Iterable[uuid.UUID]) -> None:
        if self.iter_index > len(self.create_batches):
            return

        for obj_id, change in zip(
            obj_ids, self.create_batches[self.iter_index - 1], strict=True
        ):
            self.object_ids[id(change.model)] = obj_id

    def commit(self) -> None:
        assert self.iter_index == len(self.create_batches) + 1

        visited: IDTracker[GelModel, None] = IDTracker()

        def _traverse(obj: GelModel) -> None:
            if not isinstance(obj, GelModel) or obj in visited:
                return

            visited.track(obj)
            unwrapped = unwrap_proxy(obj)
            unwrapped.__gel_commit__(self.object_ids.get(id(unwrapped)))

            for prop in type(obj).__gel_pointers__().values():
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
                    assert is_dlist(linked)
                    for ref in linked:
                        _traverse(unwrap_proxy(ref))
                    linked.__gel_commit__()
                else:
                    assert isinstance(linked, GelModel)
                    _traverse(unwrap_proxy(linked))

        for o in self.objs:
            _traverse(o)

    def _get_id(self, obj: GelModel) -> uuid.UUID:
        if obj.id is not UNSET_UUID:
            return obj.id
        else:
            return self.object_ids[id(obj)]

    def _compile_link_prop_expr(
        self,
        proxy: ProxyModel[GelModel],
        obj_ql: str,
    ) -> str:
        if proxy is None:
            return obj_ql

        changes = proxy.__gel_get_changed_fields__()
        if not changes:
            return obj_ql

        # for field_name in changes:
        raise RuntimeError

    def _compile_change(
        self, change: ModelChange, /, *, for_insert: bool
    ) -> QueryWithArgs:
        shape_parts: list[str] = []
        with_clauses: list[str] = []
        args: list[object] = []

        def add_arg(
            type_ql: str,
            value: Any,
            /,
            *,
            optional: bool = False,
        ) -> str:
            arg = str(len(args))
            opt = "optional " if optional else ""
            with_clauses.append(f"__a_{arg} := <{opt}{type_ql}>${arg}")
            args.append(value)
            return f"__a_{arg}"

        obj = change.model
        type_name = obj_to_name_ql(obj)

        if change.props:
            for pch in change.props.values():
                arg = add_arg(
                    type_to_ql(pch.info.type),
                    pch.value,
                    optional=pch.info.cardinality.is_optional(),
                )
                shape_parts.append(f"{quote_ident(pch.prop_name)} := {arg}")

        if change.single_links:
            for sch in change.single_links.values():
                if sch.target is None:
                    shape_parts.append(f"{quote_ident(sch.link_name)} := {{}}")
                    continue

                tid = self._get_id(sch.target)
                linked_name = obj_to_name_ql(sch.target)

                if sch.props:
                    assert sch.props_info is not None

                    els: list[Any] = [tid]
                    els_ql = ["std::uuid"]

                    subq_shape: list[str] = []

                    for pname, pval in sch.props.items():
                        els.append(pval)
                        els_ql.append(type_to_ql(sch.props_info[pname].type))

                    arg = add_arg(f"tuple<{', '.join(els_ql)}>", tuple(els))

                    for idx, pname in enumerate(sch.props):
                        subq_shape.append(
                            f"@{quote_ident(pname)} := {arg}.{idx + 1}"
                        )

                    shape_parts.append(
                        f"{quote_ident(sch.link_name)} := "
                        f"(select (<{linked_name}>{arg}.0) {{ "
                        f"  {', '.join(subq_shape)}"
                        f"}})"
                    )

                else:
                    arg = add_arg("std::uuid", tid)
                    shape_parts.append(
                        f"{quote_ident(sch.link_name)} := "
                        f"<{linked_name}><uuid>{arg}"
                    )

        if change.multi_links:
            for op in change.multi_links.values():
                if isinstance(op, MultiLinkRemove):
                    assert not for_insert

                    arg = add_arg(
                        "array<uuid>", [self._get_id(o) for o in op.removed]
                    )

                    assert issubclass(op.info.type, DistinctList)

                    shape_parts.append(
                        f"{quote_ident(op.link_name)} -= "
                        f"<{type_to_ql(op.info.type.type)}>array_unpack({arg})"
                    )

                    continue

                assert isinstance(op, MultiLinkAdd)

                if op.added_props:
                    assert op.props_info is not None

                    link_args: list[tuple[Any, ...]] = []
                    prop_order = None
                    tuple_subt: list[str] | None = None

                    for addo, addp in zip(
                        op.added, op.added_props, strict=True
                    ):
                        if prop_order is None:
                            prop_order = addp.keys()
                            tuple_subt = [
                                type_to_ql(op.props_info[k].type)
                                for k in prop_order
                            ]

                        assert prop_order == addp.keys()

                        link_args.append(
                            (
                                self._get_id(addo),
                                *(addp[k] for k in prop_order),
                            )
                        )

                    assert prop_order
                    assert tuple_subt

                    arg = add_arg(
                        f"array<tuple<std::uuid, {','.join(tuple_subt)}>>",
                        link_args,
                    )

                    lp_assign = ", ".join(
                        f"@{p} := tup.{i + 1}"
                        for i, p in enumerate(prop_order)
                    )

                    assert issubclass(op.info.type, DistinctList)

                    shape_parts.append(
                        f"{quote_ident(op.link_name)} += ("
                        f"for tup in array_unpack({arg}) union ("
                        f"select (<{type_to_ql(op.info.type.type)}>tup.0) {{ "
                        f"{lp_assign}"
                        f"}}))"
                    )

                else:
                    arg = add_arg(
                        "array<std::uuid>",
                        [self._get_id(o) for o in op.added],
                    )

                    assert issubclass(op.info.type, DistinctList)

                    shape_parts.append(
                        f"{quote_ident(op.link_name)} += "
                        f"<{type_to_ql(op.info.type.type)}>array_unpack({arg})"
                    )

        q_type_name = quote_ident(type_name)

        shape = ", ".join(shape_parts)
        query: str
        if for_insert:
            query = f"insert {q_type_name} {{ {shape} }}"
        else:
            arg = add_arg("std::uuid", self._get_id(obj))
            query = f"""\
                update {q_type_name}
                filter .id = {arg}
                set {{ {shape} }}
            """  # noqa: S608

        if with_clauses:
            query = f"with\n{', '.join(with_clauses)}\n\n{query}"

        return query, args
