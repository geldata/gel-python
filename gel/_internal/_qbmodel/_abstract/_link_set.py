from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Generic,
    TypeVar,
    cast,
)
from typing_extensions import Self
from collections.abc import Iterable, Collection


from gel._internal import _typing_parametric as parametric
from gel._internal._qbmodel._abstract._base import AbstractGelLinkModel
from gel._internal._tracked_list import (
    AbstractCollection,
    DefaultList,
    Mode,
    requires_read,
)

from ._base import AbstractGelSourceModel, AbstractGelModel
from ._descriptors import AbstractGelProxyModel, proxy_link

if TYPE_CHECKING:
    import uuid
    from pathlib import Path
    from collections.abc import Iterator
    from ._base import AbstractGelSourceModel


ll_getattr = object.__getattribute__


_MT_co = TypeVar("_MT_co", bound="AbstractGelSourceModel", covariant=True)
_ADL_co = TypeVar("_ADL_co", bound=AbstractCollection[Any], covariant=True)


class AbstractLinkSet(  # noqa: PLW1641 (__hash__ is implemented)
    AbstractCollection[_MT_co],
    Collection[_MT_co],
):
    """A mutable, ordered set-like list that enforces element-type covariance
    at runtime and maintains distinctness of elements in insertion order using

    There are some differences between LinkSet and normal Python set:

    - LinkSet is ordered
    - LinkSet does not have the `pop()` method and other non-sensical
      methods for persisted database objects
    - LinkSet can be compared to a list (see the __eq__ comments)
    - LinkSet supports `+=` and `-=` operators to mimic EdgeQL a list and set.
    """

    __slots__ = (
        "_index_snapshot",
        "_tracking_index",
        "_tracking_set",
    )

    _allowed_write_only_ops: ClassVar[list[str]] = [
        ".add()",
        ".discard()",
        ".update()",
        "+=",
        "-=",
    ]

    # Set of (hashable) items to maintain distinctness.
    _tracking_set: dict[_MT_co, _MT_co] | None
    # Mapping of `self._pyid(item)` to `item`
    _tracking_index: dict[int, _MT_co] | None
    # Copy of `_tracking_index` at the moment of `_ensure_snapshot()` call
    _index_snapshot: dict[int, _MT_co] | None

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self._tracking_set = None
        self._tracking_index = None
        self._index_snapshot = None
        super().__init__(*args, **kwargs)

    @staticmethod
    def _pyid(item: _MT_co) -> int:  # type: ignore [misc]
        # For ListSet it's `id(item)`, but for LinkWithPropsSet
        # it's `id(item.without_linkprops())`.
        raise NotImplementedError

    def _ensure_value_indexable(self, value: Any) -> _MT_co | None:
        # ProxyModels are designed to be transparent and are ignored
        # in GelModel.__eq__. That said we do want to validate the
        # wrapped object's type and bail early if it's doesn't match
        # this collection's type.
        if isinstance(value, AbstractGelProxyModel):
            value = value.without_linkprops()
        if isinstance(value, type(self).type):
            return value
        return None

    def _ensure_snapshot(self) -> None:
        # "_ensure_snapshot" is called right before any mutation:
        # this is the perfect place to init tracking, as it's required
        # for making a snapshot.
        self._init_tracking()

        if self._index_snapshot is None:
            assert self._tracking_index is not None
            self._index_snapshot = dict(self._tracking_index)

    def __gel_replace_with_empty__(self) -> None:
        self._items.clear()
        self._index_snapshot = None
        self._tracking_set = None
        self._tracking_index = None

    def _init_tracking(self) -> None:
        if self._tracking_set is None:
            # Why is `set(self._items)` OK? `self._items` can be
            # in one of two states:
            #
            #  - have 0 elements -- new collection
            #  - have non-zero elements -- existing collection
            #    loaded from database (we trust its contents)
            #    *before any mutations*.
            #
            # So it's either no elements or all elements are hashable
            # (have IDs).
            self._tracking_set = dict(
                zip(self._items, self._items, strict=True)
            )

            assert self._tracking_index is None
            self._tracking_index = {self._pyid(o): o for o in self._items}
        else:
            assert self._tracking_index is not None

    def __gel_reconcile__(
        self,
        updated: AbstractLinkSet[_MT_co],
        new_objects: dict[uuid.UUID, _MT_co],
    ) -> None:
        raise NotImplementedError

    def __gel_get_added__(self) -> list[_MT_co]:
        match bool(self._index_snapshot), bool(self._tracking_index):
            case True, False:
                # _index_snapshot has data in it, _tracking_index is empty --
                # everything was removed
                return []
            case False, True:
                # _index_snapshot is empty, _tracking_index has data in it --
                # everything was added
                return list(self._items)
            case True, True:
                # _index_snapshot and _tracking_index have data in it --
                # some items were added, some were removed
                assert self._index_snapshot is not None
                assert self._tracking_index is not None
                return [
                    item
                    for item_id, item in self._tracking_index.items()
                    if item_id not in self._index_snapshot
                ]
            case False, False:
                # _index_snapshot and _tracking_index are empty -- no changes
                return []

        raise AssertionError("unreachable")

    def __gel_get_removed__(self) -> Iterable[_MT_co]:
        # See the comment in __gel_get_added__
        match bool(self._index_snapshot), bool(self._tracking_index):
            case True, False:
                # _index_snapshot has data in it, _tracking_index is empty --
                # everything was removed
                assert self._index_snapshot is not None
                return list(self._index_snapshot.values())
            case False, True:
                # _index_snapshot is empty, _tracking_index has data in it --
                # everything was added
                return ()
            case True, True:
                # _index_snapshot and _tracking_index have data in it --
                # some items were added, some were removed
                assert self._index_snapshot is not None
                assert self._tracking_index is not None
                return [
                    item
                    for item_id, item in self._index_snapshot.items()
                    if item_id not in self._tracking_index
                ]
            case False, False:
                # _index_snapshot and _tracking_index are empty -- no changes
                return ()

        raise AssertionError("unreachable")

    def __gel_has_changes__(self) -> bool:
        if self._index_snapshot is None:
            # We don't even have a snapshot -- no changes
            return False
        return self._tracking_index != self._index_snapshot

    def __gel_commit__(self) -> None:
        super().__gel_commit__()

        if self._tracking_index is not None:
            assert self._tracking_set is not None
            if len(self._tracking_set) != len(self._tracking_index):
                # There are unhashable items in our collection
                # which are now hashable after save().
                self._tracking_set.update(
                    zip(
                        self._tracking_index.values(),
                        self._tracking_index.values(),
                        strict=True,
                    )
                )

        if self._index_snapshot is not None:
            assert self._tracking_index is not None
            if len(self._tracking_index) != len(self._index_snapshot):
                self._index_snapshot = dict(self._tracking_index)

    def __gel_post_commit_check__(self, path: Path) -> None:
        super().__gel_post_commit_check__(path)

        if self._index_snapshot != self._tracking_index:
            raise ValueError(
                f"{path} `self._index_snapshot` != `self._tracking_index` "
                f"after save()"
            )

        if self._tracking_set is not None and len(self._tracking_set) != len(
            self._items
        ):
            raise ValueError(
                f"{path}: `{len(self._tracking_set or {})=}` != "
                f"`{len(self._items)=}`"
            )

        if self._tracking_index is not None and len(
            self._tracking_index
        ) != len(self._items):
            raise ValueError(
                f"{path}: `{len(self._tracking_index or {})=}` != "
                f"`{len(self._items)=}`"
            )

    def _track_item(self, item: _MT_co) -> None:  # type: ignore [misc]
        assert self._tracking_set is not None
        if not item.__gel_new__:
            self._tracking_set[item] = item

        assert self._tracking_index is not None
        self._tracking_index[self._pyid(item)] = item

    def _untrack_item(self, item: _MT_co) -> None:  # type: ignore [misc]
        assert self._tracking_set is not None
        if not item.__gel_new__:
            self._tracking_set.pop(item)

        assert self._tracking_index is not None
        self._tracking_index.pop(self._pyid(item), None)

    def _is_tracked(self, item: _MT_co) -> bool:  # type: ignore [misc]
        self._init_tracking()

        assert self._tracking_index is not None
        if self._pyid(item) in self._tracking_index:
            # Fast path
            return True

        # The item is not in the index, but it might have an equal
        # one in the tracking set.
        assert self._tracking_set is not None
        if not item.__gel_new__:
            return item in self._tracking_set

        return False

    def clear(self) -> None:
        """Remove all items but keep element-type enforcement."""
        self._ensure_snapshot()
        self._items.clear()
        assert self._tracking_set is not None
        self._tracking_set.clear()
        assert self._tracking_index is not None
        self._tracking_index.clear()

    @requires_read("use `in` operator on")
    def __contains__(self, item: object) -> bool:
        if isinstance(item, AbstractGelProxyModel):
            item = item.without_linkprops()
        if not isinstance(item, type(self).type):
            return False
        return self._is_tracked(item)

    def __gel_add__(self, value: _MT_co) -> None:  # type: ignore [misc]
        raise NotImplementedError

    def __gel_remove__(self, value: _MT_co) -> _MT_co | None:  # type: ignore [misc]
        raise NotImplementedError

    def update(self, values: Iterable[_MT_co]) -> None:
        self.__gel_extend__(values)

    def add(self, value: _MT_co) -> None:  # type: ignore [misc]
        self.__gel_add__(value)

    def discard(self, value: _MT_co) -> None:  # type: ignore [misc]
        """Remove item; raise ValueError if missing."""
        self.__gel_remove__(value)

    def remove(self, value: _MT_co) -> None:  # type: ignore [misc]
        """Remove an element. If not a member, raise a KeyError."""
        existing = self.__gel_remove__(value)
        if existing is None:
            raise KeyError(value)

    @requires_read("get the length of", unsafe="unsafe_len()")
    def __len__(self) -> int:
        return len(self._items)

    @requires_read("iterate over", unsafe="unsafe_iter()")
    def __iter__(self) -> Iterator[_MT_co]:
        return iter(self._items)

    def __iadd__(self, other: Iterable[_MT_co]) -> Self:
        self.__gel_extend__(other)
        return self

    def __isub__(self, other: Iterable[_MT_co]) -> Self:
        for item in other:
            self.__gel_remove__(item)
        return self

    def __eq__(self, other: object) -> bool:
        if isinstance(other, AbstractLinkSet):
            if self._mode is Mode.Write:
                # __eq__ is pretty fundamental in Python and we don't
                # want it to crash in all random places where it can
                # be called from. So we just return False.
                return False

            if len(self._items) != len(other._items):
                return False

            if self._tracking_index and other._tracking_index:
                # Both collections are tracked so we can compare
                # the indexes -- that's faster
                return self._tracking_index == other._tracking_index

            if self._tracking_index is None:
                self._init_tracking()
            if other._tracking_index is None:
                other._init_tracking()
            return self._tracking_index == other._tracking_index

        elif isinstance(other, set):
            if self._mode is Mode.Write:
                return False

            if (
                self._tracking_set is not None
                and self._tracking_index is not None
                and len(self._tracking_set) != len(self._tracking_index)
            ):
                # There are unhashable items in our collection
                # (added after the link was fetched or this is a new link),
                # so we're not equal to any valid Python set.
                return False

            if self._tracking_set is not None:
                return self._tracking_set.keys() == other

            return set(self._items) == other

        elif isinstance(other, list):
            # In an ideal world we'd only allow comparisions with sets.
            # But in our world we already allow initialization from a list
            # and an assigment to a list (we can't require users to use
            # sets because unsaved objects are unhashable).
            # It's weird if comparison doesn't work with lists, but also
            # you can't even put unsaved objects in a set (again, they
            # are unhashable).
            if self._mode is Mode.Write:
                return False

            return self._items == other

        else:
            return NotImplemented

    @staticmethod
    def __gel_validate__(
        tp: type[_ADL_co],
        value: Any,
    ) -> _ADL_co:
        if type(value) is list:
            # Optimization for the most common scenario - user passes
            # a list of objects to the constructor.
            return tp(value, __mode__=Mode.ReadWrite)
        elif isinstance(value, DefaultList):
            assert not value
            # GelModel will adjust __mode__ to Write for
            # unfetched multi-link/multi-prop fields.
            return tp(__mode__=Mode.ReadWrite)
        elif isinstance(value, list):
            return tp(value, __mode__=Mode.ReadWrite)
        elif isinstance(value, AbstractLinkSet):
            return tp(value._items, __mode__=Mode.ReadWrite)
        else:
            raise TypeError(
                f"could not convert {type(value)} to {tp.__name__}"
            )

    def __repr__(self) -> str:
        if self._mode is Mode.Write:
            return f"<WRITE-ONLY{self._items!r}>"
        else:
            return repr(self._items)


class LinkSet(
    parametric.SingleParametricType[_MT_co],
    AbstractLinkSet[_MT_co],
):
    __slots__ = ()

    def __gel_add__(self, value: _MT_co) -> None:  # type: ignore [misc]
        value = self._check_value(value)
        self._ensure_snapshot()
        if self._is_tracked(value):
            return
        self._track_item(value)
        self._items.append(value)

    def __gel_remove__(self, value: _MT_co) -> _MT_co | None:  # type: ignore [misc]
        """Remove item; raise ValueError if missing."""
        value = self._check_value(value)
        self._ensure_snapshot()
        if not self._is_tracked(value):
            return None

        self._untrack_item(value)
        self._items.remove(value)
        return value

    def _check_value(self, value: Any) -> _MT_co:
        cls = type(self)
        t = cls.type

        if isinstance(value, AbstractGelProxyModel):
            value = value.without_linkprops()

        if isinstance(value, t):
            return value

        return t.__gel_validate__(value)

    @staticmethod
    def _pyid(item: _MT_co) -> int:  # type: ignore [misc]
        return id(item)

    def __gel_reconcile__(  # pyright: ignore [reportIncompatibleMethodOverride]
        self,
        updated: LinkSet[_MT_co],  # type: ignore [override]
        new_objects: dict[uuid.UUID, _MT_co],
    ) -> None:
        # This method is called by sync() when it refetches the link.
        #
        # - `updated` is a collection of GelModels that only have the `id`
        #   field set, but no other data. It is the latest state of the link:
        #
        #   - right after we run EdgeQL `update` command we refetch the link
        #     with a 'select' command
        #   - that select command would be filtering the link by all model
        #     IDs that were in the link prior to calling sync() PLUS ALL
        #     IDs of all objects that the sync() call inserted.
        #
        # - `new_objects` is a collection of all newly inserted GelModels
        #   by sync().
        #
        # When we look through `.id` attributes of objects in the `updated`
        # collection we can have two situations:
        #
        # - this `id` is in `self._tracking_set` -- we had this object before
        #   sync() call; it was not a new object.
        # - this `id` is not in `self._tracking_set` -- it was one of the
        #   objects that was new.
        #
        # Mind that we can have *fewer* `updated` objects than `self._items`
        # because there are situations when objects that were submitted to
        # sync() would not be refetched:
        #
        # - an existing object could be concurrently removed while
        #   between the time it was fetched and then synced.
        # - an object could be intercepted by a trigger while it was being
        #   synced.
        #
        # So we know that `updated` is the new `_items` of this collection,
        # with just one caveat: if an item in `updated` has an `.id` that we
        # already have -- we keep that original object (it will be updated
        # separately with freshly refetched data), and if it's a *new* `.id`
        # we get the new object from `new_objects`.

        existing_set = self._tracking_set
        if existing_set is None:
            # `self._init_tracking()` wasn't called on this model prior to
            # sync() -- there were no modifications. But we need a quick
            # way of checking if an updated object was one of the existing
            # objects in this link before to keep it, so let's build an
            # index of them.
            existing_set = {m: m for m in self._items if not m.__gel_new__}

        updated_items = []
        for obj in updated:
            try:
                # This works because `GelModel` hashes and compares by `.id`
                existing = existing_set[obj]
            except KeyError:  # noqa: PERF203
                updated_items.append(new_objects[obj.id])  # type: ignore [attr-defined]
            else:
                updated_items.append(existing)

        self._items = updated_items

        # Let's reset tracking -- it will likely be not needed. Typically there
        # should be just one `sync()` call with no modifications of anything
        # after it.
        self._tracking_index = self._tracking_set = self._index_snapshot = None

    def __gel_extend__(self, values: Iterable[_MT_co]) -> None:
        if values is self:
            # This is a "unique list" with a set-like behavior, so
            # LinkSet.extend(self) is a no-op.
            return

        if isinstance(values, AbstractLinkSet):
            values = values.__gel_basetype_iter__()
        for v in values:
            self.add(v)

    def __reduce__(self) -> tuple[Any, ...]:
        cls = type(self)
        return (
            cls._reconstruct_from_pickle,
            (
                cls.__parametric_origin__,
                cls.type,
                self._items,
                self._tracking_set,
                self._tracking_index,
                self._index_snapshot,
                self._mode,
                self.__gel_overwrite_data__,
            ),
        )

    @staticmethod
    def _reconstruct_from_pickle(  # noqa: PLR0917
        origin: type[LinkSet[_MT_co]],
        tp: type[_MT_co],  # pyright: ignore [reportGeneralTypeIssues]
        items: list[_MT_co],
        tracking_set: dict[_MT_co, _MT_co] | None,
        tracking_index: dict[int, _MT_co] | None,
        index_snapshot: dict[int, _MT_co] | None,
        mode: Mode,
        gel_overwrite_data: bool,  # noqa: FBT001
    ) -> LinkSet[_MT_co]:
        cls = cast(
            "type[LinkSet[_MT_co]]",
            origin[tp],  # type: ignore [index]
        )
        lst = cls.__new__(cls)

        lst._items = items
        lst._tracking_set = tracking_set
        lst._tracking_index = tracking_index
        lst._index_snapshot = index_snapshot

        lst._mode = mode
        lst.__gel_overwrite_data__ = gel_overwrite_data

        return lst


_BMT_co = TypeVar("_BMT_co", bound=AbstractGelModel, covariant=True)
"""Base model type"""

_PT_co = TypeVar(
    "_PT_co",
    bound=AbstractGelProxyModel[AbstractGelModel, AbstractGelLinkModel],
    covariant=True,
)
"""Proxy model"""


class LinkWithPropsSet(
    parametric.ParametricType,
    AbstractLinkSet[_PT_co],
    Generic[_PT_co, _BMT_co],
):
    __slots__ = ()

    proxytype: ClassVar[type[_PT_co]]  # type: ignore [misc]
    type: ClassVar[type[_BMT_co]]  # type: ignore [assignment, misc]

    @staticmethod
    def _pyid(item: _PT_co) -> int:  # type: ignore [misc]
        return id(item.without_linkprops())

    def _find_proxy(self, item: _PT_co | _BMT_co) -> _PT_co | None:
        assert self._tracking_index is not None
        assert self._tracking_set is not None

        cls = type(self)
        t = type(item)

        if t is cls.proxytype:
            oid = id(cast("_PT_co", item).without_linkprops())

        elif t is cls.type:
            oid = id(item)

        elif isinstance(item, AbstractGelProxyModel):
            oid = id(item.without_linkprops())

        else:
            # We don't know what `v` is, but...
            # what's the worst that can happen?
            oid = id(item)

        existing = self._tracking_index.get(oid)
        if existing is not None:
            return existing

        if item.__gel_new__:
            return None

        return self._tracking_set.get(item)  # type: ignore [arg-type]

    def __gel_extend__(self, values: Iterable[_PT_co | _BMT_co]) -> None:
        if not values:
            # Empty list => early return
            return

        if values is self:
            # This is a "unique list" with a set-like behavior, so
            # LinkSet.extend(self) is a no-op.
            return

        if isinstance(values, AbstractLinkSet):
            values = list(values.__gel_basetype_iter__())

        self._ensure_snapshot()

        proxy_type = type(self).proxytype

        assert self._tracking_index is not None
        assert self._tracking_set is not None

        # For an empty list we can call one extend() call instead
        # of slow iterative appends.
        empty_items = not self._items

        for v in values:
            existing = self._find_proxy(v)

            proxy = cast(
                "_PT_co",
                proxy_link(
                    existing=existing,
                    new=v,
                    proxy_type=proxy_type,
                ),
            )

            if proxy is not existing:
                assert existing is None
                self._track_item(proxy)

                if not empty_items:
                    self._items.append(proxy)

        if empty_items:
            # A LOT faster than `extend()` ¯\_(ツ)_/¯
            self._items = list(self._tracking_index.values())

    def __gel_add__(self, value: _PT_co | _BMT_co) -> None:
        self._ensure_snapshot()

        existing = self._find_proxy(value)
        proxy = cast(
            "_PT_co",
            proxy_link(
                existing=existing,
                new=value,
                proxy_type=type(self).proxytype,
            ),
        )

        if proxy is not existing:
            assert existing is None
            self._track_item(proxy)
            self._items.append(proxy)

    def __gel_remove__(self, value: _PT_co | _BMT_co) -> _PT_co | None:
        """Remove item; raise ValueError if missing."""

        self._ensure_snapshot()

        existing = self._find_proxy(value)
        if existing is None:
            return None

        self._untrack_item(existing)

        # TODO: Maybe there's a faster way to do this?
        # __eq__ on ProxyModels isn't cheap.
        self._items.remove(existing)

        return existing

    def __gel_basetype_iter__(self) -> Iterator[_BMT_co]:  # type: ignore [override]
        for item in self._items:
            yield item._p__obj__  # type: ignore [misc]

    def __reduce__(self) -> tuple[Any, ...]:
        cls = type(self)
        return (
            cls._reconstruct_from_pickle,
            (
                cls.__parametric_origin__,
                cls.type,
                cls.proxytype,
                self._items,
                self._tracking_index.values()
                if self._tracking_index is not None
                else None,
                self._tracking_set,
                self._index_snapshot,
                self._mode,
                self.__gel_overwrite_data__,
            ),
        )

    def __gel_reconcile__(  # pyright: ignore [reportIncompatibleMethodOverride]
        self,
        updated: LinkWithPropsSet[_PT_co, _BMT_co],  # type: ignore [override]
        new_objects: dict[uuid.UUID, _BMT_co],  # type: ignore [override]
    ) -> None:
        # See comments in `LinkSet.__gel_reconcile__()` for most implementation
        # details.
        #
        # This implementaion has a couple differences w.r.t. how ProxyModels
        # are handled.

        existing_set = self._tracking_set
        if existing_set is None:
            existing_set = {m: m for m in self._items if not m.__gel_new__}

        updated_items = []
        for obj in updated:
            try:
                existing = existing_set[obj]
            except KeyError:  # noqa: PERF203
                # `obj` will have updated __linkprops__ but the wrapped
                # model will be coming from new_objects
                obj.__gel_replace_wrapped_model__(new_objects[obj.id])  # type: ignore [attr-defined]
                updated_items.append(obj)
            else:
                # updated will have newly refetched __linkprops__, so
                # copy them
                existing.__gel_replace_linkprops__(obj.__linkprops__)
                updated_items.append(existing)

        self._items = updated_items
        self._tracking_index = self._tracking_set = self._index_snapshot = None

    @staticmethod
    def _reconstruct_from_pickle(  # noqa: PLR0917
        origin: type[LinkWithPropsSet[_PT_co, _BMT_co]],  # type: ignore [valid-type]
        tp: type[_PT_co],  # type: ignore [valid-type]
        proxytp: type[_BMT_co],  # type: ignore [valid-type]
        items: list[_PT_co],
        tracking_index: list[_PT_co] | None,
        tracking_set: dict[_PT_co, _PT_co] | None,
        index_snapshot: list[_PT_co] | None,
        mode: Mode,
        gel_overwrite_data: bool,  # noqa: FBT001
    ) -> LinkWithPropsSet[_PT_co, _BMT_co]:
        cls = cast(
            "type[LinkWithPropsSet[_PT_co, _BMT_co]]",
            origin[proxytp, tp],  # type: ignore [index]
        )
        lst = cls.__new__(cls)

        lst._items = items

        if tracking_index is None:
            lst._tracking_index = None
        else:
            lst._tracking_index = {
                cls._pyid(item): item for item in tracking_index
            }

        lst._tracking_set = tracking_set

        if index_snapshot is None:
            lst._index_snapshot = None
        else:
            lst._index_snapshot = {
                cls._pyid(item): item for item in index_snapshot
            }

        lst._mode = mode
        lst.__gel_overwrite_data__ = gel_overwrite_data

        return lst

    if TYPE_CHECKING:

        def add(self, value: _PT_co | _BMT_co) -> None: ...
        def update(self, values: Iterable[_PT_co | _BMT_co]) -> None: ...
        def discard(self, value: _PT_co | _BMT_co) -> None: ...
        def remove(self, value: _PT_co | _BMT_co) -> None: ...
        def __add__(self, other: Iterable[_PT_co | _BMT_co]) -> Self: ...
        def __iadd__(self, other: Iterable[_PT_co | _BMT_co]) -> Self: ...
        def __isub__(self, other: Iterable[_PT_co | _BMT_co]) -> Self: ...
