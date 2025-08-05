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
from ._descriptors import AbstractGelProxyModel

if TYPE_CHECKING:
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
    _tracking_set: set[_MT_co] | None
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
        # ProxyModels are designed to be tranparent and are ignored
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
            self._tracking_set = set(self._items)

            assert self._tracking_index is None
            self._tracking_index = {self._pyid(o): o for o in self._items}
        else:
            assert self._tracking_index is not None

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
                return []

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
                self._tracking_set.update(self._tracking_index.values())

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
            self._tracking_set.add(item)

        assert self._tracking_index is not None
        self._tracking_index[self._pyid(item)] = item

    def _untrack_item(self, item: _MT_co) -> None:  # type: ignore [misc]
        assert self._tracking_set is not None
        if not item.__gel_new__:
            self._tracking_set.remove(item)

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

    def _check_value(self, value: Any) -> _MT_co:
        raise NotImplementedError

    @requires_read("use `in` operator on")
    def __contains__(self, item: object) -> bool:
        item = self._ensure_value_indexable(item)
        if item is None:
            return False
        return self._is_tracked(item)

    def update(self, values: Iterable[_MT_co]) -> None:
        self.__gel_extend__(values)

    def add(self, value: _MT_co) -> None:  # type: ignore [misc]
        value = self._check_value(value)
        self._ensure_snapshot()
        if self._is_tracked(value):
            return
        self._track_item(value)
        self._items.append(value)

    def discard(self, value: _MT_co) -> None:  # type: ignore [misc]
        """Remove item; raise ValueError if missing."""
        if not self._is_tracked(value):
            pass

        self._ensure_snapshot()
        value = self._check_value(value)
        self._untrack_item(value)
        self._items.remove(value)

    def remove(self, value: _MT_co) -> None:  # type: ignore [misc]
        """Remove an element. If not a member, raise a KeyError."""
        if not self._is_tracked(value):
            raise KeyError(value)
        self.discard(value)

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
            self.discard(item)
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
                return self._tracking_set == other

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


class LinkSet(
    parametric.SingleParametricType[_MT_co],
    AbstractLinkSet[_MT_co],
):
    __slots__ = ()

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
        tracking_set: set[_MT_co] | None,
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
        return id(item._p__obj__)

    def __gel_extend__(self, values: Iterable[_PT_co | _BMT_co]) -> None:
        # An optimized version of `extend()`

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

        cls = type(self)
        t = cls.proxytype
        proxy_of = t.__proxy_of__

        assert self._tracking_index is not None
        assert self._tracking_set is not None

        # For an empty list we can call one extend() call instead
        # of slow iterative appends.
        empty_items = not self._items

        proxy: _PT_co
        for v in values:
            tv = type(v)
            if tv is proxy_of:
                # Fast path -- `v` is an instance of the base type.
                # It has no link props, wrap it in a proxy in
                # a fast way.
                proxy = t.__gel_proxy_construct__(v, {})
                obj = v
            elif tv is t:
                # Another fast path -- `v` is already the correct proxy.
                proxy = v  # type: ignore [assignment]  # typecheckers unable to cope
                obj = ll_getattr(v, "_p__obj__")
            else:
                proxy, obj = self._cast_value(v)

            oid = id(obj)
            if oid in self._tracking_index:
                continue

            self._tracking_index[oid] = proxy

            if not obj.__gel_new__:
                self._tracking_set.add(proxy)

            if not empty_items:
                self._items.append(proxy)

        if empty_items:
            # A LOT faster than `extend()` ¯\_(ツ)_/¯
            self._items = list(self._tracking_index.values())

    def _cast_value(self, value: Any) -> tuple[_PT_co, _BMT_co]:
        cls = type(self)
        t = cls.proxytype

        bt: type[_BMT_co] = t.__proxy_of__  # pyright: ignore [reportAssignmentType]
        tp_value = type(value)

        if tp_value is bt:
            # Fast path before we make all expensive isinstance calls.
            return (
                t.__gel_proxy_construct__(value, {}),
                value,
            )

        if tp_value is t:
            # It's a correct proxy for this link... return as is.
            return (
                value,
                ll_getattr(value, "_p__obj__"),
            )

        if not isinstance(value, AbstractGelProxyModel) and isinstance(
            value, bt
        ):
            # It's not a proxy, but the object is of the correct type --
            # re-wrap it in a correct proxy.
            return (
                t.__gel_proxy_construct__(value, {}),
                value,
            )

        if isinstance(value, AbstractGelProxyModel):
            # We unwrap different kinds of proxies - we can't inherit their
            # linkprops
            value = ll_getattr(value, "_p__obj__")

        proxy = t.__gel_validate__(value)
        return (
            proxy,
            ll_getattr(proxy, "_p__obj__"),
        )

    def _check_value(self, value: Any) -> _PT_co:
        proxy, _ = self._cast_value(value)
        return proxy

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

    @staticmethod
    def _reconstruct_from_pickle(  # noqa: PLR0917
        origin: type[LinkWithPropsSet[_PT_co, _BMT_co]],  # type: ignore [valid-type]
        tp: type[_PT_co],  # type: ignore [valid-type]
        proxytp: type[_BMT_co],  # type: ignore [valid-type]
        items: list[_PT_co],
        tracking_index: list[_PT_co] | None,
        tracking_set: set[_PT_co] | None,
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
