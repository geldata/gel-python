from __future__ import annotations

import dataclasses

from typing import TYPE_CHECKING, NamedTuple, TypeAlias, TypeVar, Generic

from gel._internal._qbmodel._abstract import GelPrimitiveType
from gel._internal._qbmodel._pydantic._models import GelModel, ProxyModel
from gel._internal._dlist import TrackedList, DistinctList
from gel._internal._unsetid import UNSET_UUID
from gel._internal._edgeql import PointerKind, quote_ident

if TYPE_CHECKING:
    import uuid

    from collections.abc import Iterable, Iterator, Callable
    from typing import TypeGuard


T = TypeVar("T")

_unset = object()


class MultiPropertyChanges(NamedTuple):
    name: str
    added: Iterable[GelPrimitiveType]
    removed: Iterable[GelPrimitiveType]


class MultiLinkChanges(NamedTuple):
    name: str
    added: Iterable[GelModel]
    removed: Iterable[GelModel]


class ModelChanges(NamedTuple):
    model: GelModel
    fields: Iterable[str]
    multi_props: Iterable[MultiPropertyChanges]
    multi_links: Iterable[MultiLinkChanges]


CreateBatch: TypeAlias = list[GelModel]
QueryWithArgs: TypeAlias = tuple[str, dict[str, object]]


class IDTracker(Generic[T]):
    _seen: dict[int, T]

    def __init__(self, initial_set: Iterable[T] | None = None, /):
        if initial_set is not None:
            self._seen = {id(x): x for x in initial_set}
        else:
            self._seen = {}

    def track(self, obj: T) -> None:
        self._seen[id(obj)] = obj

    def untrack(self, obj: T) -> None:
        self._seen.pop(id(obj), None)

    def track_many(self, more: Iterable[T], /) -> None:
        for obj in more:
            self._seen[id(obj)] = obj

    def untrack_many(self, more: Iterable[T], /) -> None:
        for obj in more:
            self._seen.pop(id(obj), None)

    def __contains__(self, obj: T) -> bool:
        return id(obj) in self._seen

    def __len__(self) -> int:
        return len(self._seen)

    def __bool__(self) -> bool:
        return bool(self._seen)

    def __iter__(self) -> Iterator[T]:
        yield from self._seen.values()


def _is_prop_list(val: object) -> TypeGuard[TrackedList[GelPrimitiveType]]:
    return isinstance(val, TrackedList) and issubclass(
        type(val).type, GelPrimitiveType
    )


def _is_link_list(val: object) -> TypeGuard[DistinctList[GelModel]]:
    return isinstance(val, DistinctList) and issubclass(
        type(val).type, GelModel
    )


def unwrap_proxy(val: GelModel) -> GelModel:
    if isinstance(val, ProxyModel):
        assert isinstance(val._p__obj__, GelModel)
        return val._p__obj__
    else:
        return val


def iter_graph(objs: Iterable[GelModel]) -> Iterable[GelModel]:
    # Simple recursive traverse of a model

    visited: IDTracker[GelModel] = IDTracker()

    def _traverse(obj: GelModel) -> Iterable[GelModel]:
        if not isinstance(obj, GelModel) or obj in visited:
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
                assert _is_link_list(linked)
                for ref in linked:
                    yield from _traverse(unwrap_proxy(ref))
            else:
                assert isinstance(linked, GelModel)
                yield from _traverse(unwrap_proxy(linked))

    for o in objs:
        yield from _traverse(o)


def get_linked_new_objects(obj: GelModel) -> Iterable[GelModel]:
    visited: IDTracker[GelModel] = IDTracker()
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
            assert _is_link_list(linked)
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


def compute_ops(
    objs: Iterable[GelModel],
) -> tuple[list[CreateBatch], list[ModelChanges]]:
    new_objects: list[GelModel] = []
    update_ops: list[ModelChanges] = []

    for obj in iter_graph(objs):
        pointers = type(obj).__gel_pointers__()
        is_new = obj.id is UNSET_UUID

        # Capture changes in *properties* and *single links*
        field_changes = obj.__gel_get_changed_fields__()

        # Compute changes in *multi links* and *multi properties*
        # (for existing objects)
        prop_changes: list[MultiPropertyChanges] = []
        link_changes: list[MultiLinkChanges] = []
        for prop in pointers.values():
            if prop.computed or not prop.cardinality.is_multi():
                # See _get_all_deps for explanation.
                continue

            val = getattr(obj, prop.name, _unset)
            if val is _unset or val is None:
                continue

            if prop.kind is PointerKind.Link:
                assert _is_link_list(val)
                added_objs = val.__gel_get_added__()
                removed_objs = val.__gel_get_removed__()

                if added_objs or removed_objs:
                    link_changes.append(
                        MultiLinkChanges(prop.name, added_objs, removed_objs)
                    )
            else:
                assert _is_prop_list(val)
                added = val.__gel_get_added__()
                removed = val.__gel_get_removed__()

                if added or removed:
                    prop_changes.append(
                        MultiPropertyChanges(prop.name, added, removed)
                    )

        if is_new:
            new_objects.append(obj)
            # schedule optional links to be applied post-insert
            optional_fields: list[str] = []
            optional_links: list[MultiLinkChanges] = []
            for prop in pointers.values():
                # only optional links
                if (
                    prop.kind is not PointerKind.Link
                    or not prop.cardinality.is_optional()
                ):
                    continue
                val = getattr(obj, prop.name, _unset)
                if val is _unset:
                    continue
                if prop.cardinality.is_multi():
                    assert _is_link_list(val)
                    if val:
                        optional_links.append(
                            MultiLinkChanges(prop.name, list(val), [])
                        )
                else:
                    optional_fields.append(prop.name)

            if optional_fields or optional_links:
                update_ops.append(
                    ModelChanges(obj, optional_fields, (), optional_links)
                )

        elif field_changes or link_changes:
            update_ops.append(
                ModelChanges(obj, field_changes, (), link_changes)
            )

    # Compute batch creation of new objects

    batches: list[list[GelModel]] = []
    inserted: IDTracker[GelModel] = IDTracker()
    remaining_to_make: IDTracker[GelModel] = IDTracker(new_objects)

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

        batches.append(ready)
        inserted.track_many(ready)
        remaining_to_make.untrack_many(ready)

    return batches, update_ops


class ParamBuilder:
    _param_cnt: int

    def __init__(self) -> None:
        self._param_cnt = 0

    def __call__(self) -> str:
        self._param_cnt += 1
        return f"p_{self._param_cnt}"


def make_save_executor_constructor(
    objs: tuple[GelModel, ...],
) -> Callable[[], SaveExecutor]:
    create_batches, updates = compute_ops(objs)
    return lambda: SaveExecutor(objs, create_batches, updates)


@dataclasses.dataclass
class SaveExecutor:
    objs: tuple[GelModel, ...]
    create_batches: list[CreateBatch]
    updates: list[ModelChanges]

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
            return [self._compile_insert(obj) for obj in batch]
        elif self.iter_index == len(self.create_batches):
            self.iter_index += 1
            return [self._compile_update(change) for change in self.updates]
        else:
            raise StopIteration

    def feed_ids(self, obj_ids: Iterable[uuid.UUID]) -> None:
        if self.iter_index > len(self.create_batches):
            return

        for obj_id, obj in zip(
            obj_ids, self.create_batches[self.iter_index - 1], strict=True
        ):
            self.object_ids[id(obj)] = obj_id

    def commit(self) -> None:
        assert self.iter_index == len(self.create_batches) + 1

        visited: IDTracker[GelModel] = IDTracker()

        def _traverse(obj: GelModel) -> None:
            if not isinstance(obj, GelModel) or obj in visited:
                return

            visited.track(obj)
            unwrapped = unwrap_proxy(obj)
            unwrapped.__gel_commit__(self.object_ids.get(id(unwrapped)))

            for prop in type(obj).__gel_pointers__().values():
                if prop.computed:
                    # We don't want to traverse computeds (they don't form the
                    # actual dependency graph, real links do)
                    continue

                val = getattr(obj, prop.name, _unset)
                if val is _unset or val is None:
                    # If users mess-up with user-defined types and smoe
                    # of the data isn't fetched, we don't want to crash
                    # with an AttributeErorr, it's not critical here.
                    # Not fetched means not used, which is good for save().
                    continue

                if prop.kind is PointerKind.Link:
                    if prop.cardinality.is_multi():
                        assert _is_link_list(val)
                        for ref in val:
                            _traverse(unwrap_proxy(ref))
                        val.__gel_commit__()
                    else:
                        assert isinstance(val, GelModel)
                        _traverse(unwrap_proxy(val))
                else:
                    assert prop.kind is PointerKind.Property
                    if prop.cardinality.is_multi():
                        assert _is_prop_list(val)
                        val.__gel_commit__()

        for o in self.objs:
            _traverse(o)

    def _get_id(self, obj: GelModel) -> uuid.UUID:
        if obj.id is not UNSET_UUID:
            return obj.id
        else:
            return self.object_ids[id(obj)]

    def _compile_insert(self, obj: GelModel) -> QueryWithArgs:
        assert obj.id is UNSET_UUID
        # Prepare metadata for insert
        pointers = type(obj).__gel_pointers__()
        type_name = type(obj).__gel_reflection__.name.as_schema_name()
        q_type_name = quote_ident(type_name)

        args: dict[str, object] = {}
        shape_parts: list[str] = []

        for prop_name, prop in pointers.items():
            if prop.computed:
                continue

            val = getattr(obj, prop_name, _unset)
            if val is _unset or val is None:
                continue

            q_name = quote_ident(prop_name)

            if prop.kind is PointerKind.Property:
                prim_type = prop.type.__gel_reflection__.name.as_schema_name()
                arg = self.param_builder()
                args[arg] = val

                if prop.cardinality.is_multi():
                    expr = f"std::array_unpack(<array<{prim_type}>>${arg})"
                else:
                    expr = f"<{prim_type}>${arg}"

                shape_parts.append(f"{q_name} := {expr}")

            else:
                assert prop.kind is PointerKind.Link
                if prop.cardinality.is_optional():
                    # We populate optional links via update operations
                    continue

                if prop.cardinality.is_multi():
                    items: list[str] = []
                    assert _is_link_list(val)
                    for linked in val:
                        u = unwrap_proxy(linked)
                        arg = self.param_builder()
                        args[arg] = self._get_id(u)
                        t_name = type(
                            u
                        ).__gel_reflection__.name.as_schema_name()
                        q_t = quote_ident(t_name)
                        items.append(f"<{q_t}><uuid>${arg}")
                    shape_parts.append(
                        f"{q_name} := assert_distinct({{{', '.join(items)}}})"
                    )

                else:
                    assert isinstance(val, GelModel)
                    u = unwrap_proxy(val)
                    arg = self.param_builder()
                    args[arg] = self._get_id(u)
                    t_name = type(u).__gel_reflection__.name.as_schema_name()
                    q_t = quote_ident(t_name)
                    shape_parts.append(f"{q_name} := <{q_t}><uuid>${arg}")

        shape = ", ".join(shape_parts)
        query = f"insert {q_type_name} {{ {shape} }}"
        return query, args

    def _compile_update(self, change: ModelChanges) -> QueryWithArgs:
        obj = change.model
        pointers = type(obj).__gel_pointers__()
        # prepare type name and quoted identifier
        type_name = type(obj).__gel_reflection__.name.as_schema_name()
        q_type_name = quote_ident(type_name)

        args: dict[str, object] = {}
        assignments: list[str] = []

        # filter by id
        args["id"] = self._get_id(obj)

        # `change.fields` contains changes to properties and single links
        for name in change.fields:
            prop = pointers[name]
            q_name = quote_ident(name)
            val = getattr(obj, name)
            if prop.kind is PointerKind.Property:
                prim_type = prop.type.__gel_reflection__.name.as_schema_name()
                param = self.param_builder()
                args[param] = val
                assignments.append(f"{q_name} := <{prim_type}>${param}")
            else:
                assert prop.kind is PointerKind.Link

                if val is None:
                    assignments.append(f"{q_name} := {{}}")
                else:
                    u = unwrap_proxy(val)
                    param = self.param_builder()
                    args[param] = self._get_id(u)
                    t_name = type(u).__gel_reflection__.name.as_schema_name()
                    q_t = quote_ident(t_name)
                    assignments.append(f"{q_name} := <{q_t}><uuid>${param}")

        # multi links: perform add/remove updates
        for ml in change.multi_links:
            name = ml.name
            prop = pointers[name]
            q_name = quote_ident(name)

            if ml.added:
                items_add: list[str] = []
                for linked in ml.added:
                    u = unwrap_proxy(linked)
                    param = self.param_builder()
                    args[param] = self._get_id(u)
                    t_name = type(u).__gel_reflection__.name.as_schema_name()
                    q_t = quote_ident(t_name)
                    items_add.append(f"<{q_t}><uuid>${param}")
                assignments.append(f"{q_name} += {{{', '.join(items_add)}}}")

            if ml.removed:
                items_rm: list[str] = []
                for linked in ml.removed:
                    u = unwrap_proxy(linked)
                    param = self.param_builder()
                    args[param] = self._get_id(u)
                    t_name = type(u).__gel_reflection__.name.as_schema_name()
                    q_t = quote_ident(t_name)
                    items_rm.append(f"<{q_t}><uuid>${param}")
                assignments.append(f"{q_name} -= {{{', '.join(items_rm)}}}")

        # multi props: perform add/remove updates
        for mp in change.multi_props:
            name = mp.name
            prop = pointers[name]
            prim_type = prop.type.__gel_reflection__.name.as_schema_name()
            q_name = quote_ident(name)

            if mp.added:
                arg = self.param_builder()
                args[arg] = list(mp.added)
                expr = f"std::array_unpack(<array<{prim_type}>>${arg})"
                assignments.append(f"{q_name} += {expr}")

            if mp.removed:
                arg = self.param_builder()
                args[arg] = list(mp.removed)
                expr = f"std::array_unpack(<array<{prim_type}>>${arg})"
                assignments.append(f"{q_name} -= {expr}")

        assert assignments

        shape = ", ".join(assignments)
        query = f"""\
            update {q_type_name}
            filter .id = <uuid>$id
            set {{ {shape} }}
        """  # noqa: S608

        return query, args


if TYPE_CHECKING:
    from gel import Client


def _save(*objs: GelModel, client: Client) -> None:
    make_executor = make_save_executor_constructor(objs)

    executor = make_executor()

    for batch in executor:
        ids = []
        for query, args in batch:
            ids.append(client.query_required_single(query, **args).id)
        executor.feed_ids(ids)

    executor.commit()
