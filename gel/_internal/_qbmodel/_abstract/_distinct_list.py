from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Generic,
    SupportsIndex,
    TypeVar,
    cast,
)
from typing_extensions import Self
from collections.abc import Iterable

import functools

from gel._internal import _typing_parametric as parametric
from gel._internal._qbmodel._abstract._base import AbstractGelLinkModel
from gel._internal._tracked_list import (
    AbstractTrackedList,
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
_ADL_co = TypeVar("_ADL_co", bound=AbstractTrackedList[Any], covariant=True)


@functools.total_ordering
class AbstractDistinctList(AbstractTrackedList[_MT_co]):
    """A mutable, ordered set-like list that enforces element-type covariance
    at runtime and maintains distinctness of elements in insertion order using
    a list and set.
    """

    # Set of (hashable) items to maintain distinctness.
    _set: set[_MT_co] | None

    # Assuming unhashable items compare by object identity,
    # the dict below is used as an extension for distinctness
    # checks.
    _unhashables: dict[int, _MT_co] | None

    def __init__(
        self,
        iterable: Iterable[_MT_co] = (),
        *,
        __wrap_list__: bool = False,
        __mode__: Mode,
    ) -> None:
        self._set = None
        self._unhashables = None
        super().__init__(
            iterable,
            __wrap_list__=__wrap_list__,
            __mode__=__mode__,
        )

    def _check_value(self, value: Any) -> _MT_co:
        cls = type(self)
        t = cls.type

        if isinstance(value, AbstractGelProxyModel):
            value = value.without_linkprops()

        if isinstance(value, t):
            return value

        return t.__gel_validate__(value)

    def _ensure_snapshot(self) -> None:
        # "_ensure_snapshot" is called right before any mutation:
        # this is the perfect place to initialize `self._set` and
        # `self._unhashables`.
        self._init_tracking()
        super()._ensure_snapshot()

    def __gel_reset_snapshot__(self) -> None:
        super().__gel_reset_snapshot__()
        self._set = None
        self._unhashables = None

    def __gel_post_commit_check__(self, path: Path) -> None:
        super().__gel_post_commit_check__(path)
        if self._unhashables:
            raise ValueError(
                f"{path} has non-empty `self._unhashables` after save()"
            )

        if self._set is not None and len(self._set) != len(self._items):
            # tracked but _set is not agreeing with _items
            raise ValueError(
                f"{path}: `{len(self._set or {})=}` != `{len(self._items)=}`"
            )

    def __gel_commit__(self) -> None:
        super().__gel_commit__()

        if self._unhashables is not None:
            assert self._set is not None
            for item in self._unhashables.values():
                self._set.add(item)
            self._unhashables.clear()

    def _init_tracking(self) -> None:
        if self._set is None:
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
            self._set = set(self._items)

            assert self._unhashables is None
            self._unhashables = {}
        else:
            assert self._unhashables is not None

    def _track_item(self, item: _MT_co) -> None:  # type: ignore [misc]
        assert self._set is not None
        try:
            self._set.add(item)
        except TypeError:
            pass
        else:
            return

        assert self._unhashables is not None
        self._unhashables[id(item)] = item

    def _untrack_item(self, item: _MT_co) -> None:  # type: ignore [misc]
        assert self._set is not None
        try:
            self._set.remove(item)
        except (TypeError, KeyError):
            # Either unhashable or not in the list
            pass
        else:
            return

        assert self._unhashables is not None
        self._unhashables.pop(id(item), None)

    def _is_tracked(self, item: _MT_co) -> bool:  # type: ignore [misc]
        self._init_tracking()
        assert self._set is not None

        try:
            return item in self._set
        except TypeError:
            # unhashable
            pass

        assert self._unhashables is not None
        return id(item) in self._unhashables

    def __setitem__(
        self,
        index: SupportsIndex | slice,
        value: _MT_co | Iterable[_MT_co],
    ) -> None:
        self._ensure_snapshot()

        if isinstance(index, slice):
            start, stop, step = index.indices(len(self._items))
            if step != 1:
                raise ValueError(
                    "Slice assignment with step != 1 not supported",
                )

            assert isinstance(value, Iterable)
            new_values = self._check_values(value)

            for item in self._items[start:stop]:
                self._untrack_item(item)

            new_filtered_values = [
                v for v in new_values if not self._is_tracked(v)
            ]

            self._items = [
                *self._items[:start],
                *new_filtered_values,
                *self._items[stop:],
            ]

            for item in new_values:
                self._track_item(item)

        else:
            new_value = self._check_value(value)

            old = self._items[index]
            self._untrack_item(old)
            del self._items[index]

            if self._is_tracked(new_value):
                return

            self._items.insert(index, new_value)
            self._track_item(new_value)

    def __delitem__(self, index: SupportsIndex | slice) -> None:
        self._ensure_snapshot()

        if isinstance(index, slice):
            to_remove = self._items[index]
            del self._items[index]
            for item in to_remove:
                self._untrack_item(item)
        else:
            item = self._items[index]
            del self._items[index]
            self._untrack_item(item)

    @requires_read("use `in` operator on")
    def __contains__(self, item: object) -> bool:
        return self._is_tracked(item)  # type: ignore [arg-type]

    def insert(self, index: SupportsIndex, value: _MT_co) -> None:  # type: ignore [misc]
        """Insert item at index if not already present."""
        value = self._check_value(value)

        if self._is_tracked(value):
            return

        # clamp index
        index = int(index)
        if index < 0:
            index = max(0, len(self._items) + index + 1)
        index = min(index, len(self._items))

        self._items.insert(index, value)
        self._track_item(value)

    def extend(self, values: Iterable[_MT_co]) -> None:
        if values is self:
            # This is a "unique list" with a set-like behavior, so
            # DistinctList.extend(self) is a no-op.
            return

        if isinstance(values, AbstractTrackedList):
            values = values.__gel_basetype_iter__()
        for v in values:
            self.append(v)

    def append(self, value: _MT_co) -> None:  # type: ignore [misc]
        value = self._check_value(value)
        self._ensure_snapshot()
        self._append_no_check(value)

    def _append_no_check(self, value: _MT_co) -> None:  # type: ignore[misc]
        if self._is_tracked(value):
            return
        self._track_item(value)
        self._items.append(value)

    def remove(self, value: _MT_co) -> None:  # type: ignore [misc]
        """Remove item; raise ValueError if missing."""
        if not self._is_tracked(value):
            pass

        self._ensure_snapshot()
        value = self._check_value(value)
        self._untrack_item(value)
        self._items.remove(value)

    def pop(self, index: SupportsIndex = -1) -> _MT_co:
        """Remove and return item at index (default last)."""
        self._ensure_snapshot()
        item = self._items.pop(index)
        self._untrack_item(item)
        return item

    def clear(self) -> None:
        """Remove all items but keep element-type enforcement."""
        self._ensure_snapshot()
        self._items.clear()
        self._set = None
        self._unhashables = None

    @requires_read("index items of")
    def index(
        self,
        value: _MT_co,  # type: ignore [misc]
        start: SupportsIndex = 0,
        stop: SupportsIndex | None = None,
    ) -> int:
        """Return first index of value."""
        value = self._check_value(value)
        return self._items.index(
            value,
            start,
            len(self._items) if stop is None else stop,
        )

    @requires_read("count items of")
    def count(self, value: _MT_co) -> int:  # type: ignore [misc]
        """Return 1 if item is present, else 0."""
        value = self._check_value(value)
        if self._is_tracked(value):
            return 1
        else:
            return 0


class DistinctList(
    parametric.SingleParametricType[_MT_co],
    AbstractDistinctList[_MT_co],
):
    def __reduce__(self) -> tuple[Any, ...]:
        cls = type(self)
        return (
            cls._reconstruct_from_pickle,
            (
                cls.__parametric_origin__,
                cls.type,
                self._items,
                self._initial_items,
                self._set,
                self._unhashables.values()
                if self._unhashables is not None
                else None,
                self._mode,
                self.__gel_overwrite_data__,
            ),
        )

    @staticmethod
    def _reconstruct_from_pickle(  # noqa: PLR0917
        origin: type[DistinctList[_MT_co]],
        tp: type[_MT_co],  # pyright: ignore [reportGeneralTypeIssues]
        items: list[_MT_co],
        initial_items: list[_MT_co] | None,
        hashables: set[_MT_co] | None,
        unhashables: list[_MT_co] | None,
        mode: Mode,
        gel_overwrite_data: bool,  # noqa: FBT001
    ) -> DistinctList[_MT_co]:
        cls = cast(
            "type[DistinctList[_MT_co]]",
            origin[tp],  # type: ignore [index]
        )
        lst = cls.__new__(cls)

        lst._items = items
        lst._initial_items = initial_items
        lst._set = hashables
        if unhashables is None:
            lst._unhashables = None
        else:
            lst._unhashables = {id(item): item for item in unhashables}

        lst._mode = mode
        lst.__gel_overwrite_data__ = gel_overwrite_data

        return lst

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
        elif isinstance(value, (list, AbstractTrackedList)):
            return tp(value, __mode__=Mode.ReadWrite)
        else:
            raise TypeError(
                f"could not convert {type(value)} to {tp.__name__}"
            )


_BMT_co = TypeVar("_BMT_co", bound=AbstractGelModel, covariant=True)
"""Base model type"""

_PT_co = TypeVar(
    "_PT_co",
    bound=AbstractGelProxyModel[AbstractGelModel, AbstractGelLinkModel],
    covariant=True,
)
"""Proxy model"""


class ProxyDistinctList(
    parametric.ParametricType,
    AbstractDistinctList[_PT_co],
    Generic[_PT_co, _BMT_co],
):
    # Mapping of GelModels' id()s to ProxyModels that wrap them.
    #
    # We need to ensure that we don't have two proxies wrapping the same
    # object in `self._items`. So:
    #
    # - `self._set`` is a set of hashable proxies
    #   (proxies over an object with .id)
    #
    # - `self._unhashables` is a mapping of unhashable proxies' id()s to
    #   themselves.
    #
    # - `self._wrapped_index` is a mapping of wrapped object's id()s
    #   to proxies. We need it because "unsaved" objects without `.id`
    #   can only be tracked by their address. And `self._set` and
    #   `self._unhashables` are already tasked with tracking ProxyModel's,
    #   but we need something to track the uniquiness of *unsaved*
    #   wrapped GelModels.
    #
    # Let's look at different cases, for the following example list:
    #
    #     `[p1(a), p2(b_new)]`
    #
    # where:
    #
    # - `a` is an object with `.id`,
    # - `b_new` is an object without `.id` (not saved yet), and
    # - `pN(x)` is a `ProxyModel` instance wrapping `x` with Python's
    #   id() of the *proxy* object equal to `N`.
    #
    # Cases:
    #
    # - we attempt to add `a` again: `a` will be wrapped in a proxy and
    #   become `p3(a)`; `p3(a) in self._set` will evaluate to `True`, so
    #   `a` will not be added again.
    #
    # - we attempt to add `p4(a)`: `p4(a) in self._set` will evaluate to
    #   `True`; `p4(a)` will not be added as we already have `p1(a)`
    #
    # - we attempt to add `p2(b_new)`; but it's in `self._unhashables` so we
    #   won't add it.
    #
    # - we attempt to add `b_new`. `id(b_new) in self._wrapped_index` will
    #   be `True`, so `b_new` won't be added again.
    #
    # - we attempt to add `p5(b_new)` -- `b_new` will be unwrapped and since
    #   it's in the `_wrapped_index` it won't be added again.
    #
    _wrapped_index: dict[int, _PT_co] | None = None

    basetype: ClassVar[type[_BMT_co]]  # type: ignore [misc]
    type: ClassVar[type[_PT_co]]  # type: ignore [misc]

    def __gel_post_commit_check__(self, path: Path) -> None:
        super().__gel_post_commit_check__(path)

        if self._set is None:
            # This collection isn't tracked.
            assert self._wrapped_index is None
            assert self._unhashables is None
            return

        if len(self._items) != len(self._wrapped_index or {}):
            raise ValueError(
                f"{path}: `{len(self._wrapped_index or {})=}` != "
                f"`{len(self._items)=}`"
            )

        if self._items:
            assert self._wrapped_index
            if set(self._items) != set(self._wrapped_index.values()):
                raise ValueError(
                    f"{path}: `self._items` != `self._wrapped_index.values()`"
                )

    def __gel_commit__(self) -> None:
        super().__gel_commit__()

        if self._set is None:
            # This collection isn't tracked.
            assert self._wrapped_index is None
            return

        assert self._wrapped_index is not None
        if len(self._items) != len(self._wrapped_index):
            self._wrapped_index = {
                id(ll_getattr(item, "_p__obj__")): item for item in self._items
            }

    def _init_tracking(self) -> None:
        super()._init_tracking()

        if self._wrapped_index is None:
            self._wrapped_index = {}
            for item in self._items:
                assert isinstance(item, AbstractGelProxyModel)
                wrapped = ll_getattr(item, "_p__obj__")
                self._wrapped_index[id(wrapped)] = item

    def _track_item(self, item: _PT_co) -> None:  # type: ignore [misc]
        assert isinstance(item, AbstractGelProxyModel)
        super()._track_item(item)
        assert self._wrapped_index is not None
        wrapped = ll_getattr(item, "_p__obj__")
        self._wrapped_index[id(wrapped)] = item

    def _untrack_item(self, item: _PT_co) -> None:  # type: ignore [misc]
        assert isinstance(item, AbstractGelProxyModel)
        super()._untrack_item(item)
        assert self._wrapped_index is not None
        wrapped = ll_getattr(item, "_p__obj__")
        self._wrapped_index.pop(id(wrapped), None)

    def _is_tracked(self, item: _PT_co | _BMT_co) -> bool:
        self._init_tracking()
        assert self._wrapped_index is not None

        if isinstance(item, AbstractGelProxyModel):
            return id(item._p__obj__) in self._wrapped_index
        else:
            return id(item) in self._wrapped_index

    def __gel_reset_snapshot__(self) -> None:
        super().__gel_reset_snapshot__()
        self._wrapped_index = None

    def extend(self, values: Iterable[_PT_co | _BMT_co]) -> None:
        # An optimized version of `extend()`

        if not values:
            # Empty list => early return
            return

        if values is self:
            # This is a "unique list" with a set-like behavior, so
            # DistinctList.extend(self) is a no-op.
            return

        if isinstance(values, AbstractTrackedList):
            values = list(values.__gel_basetype_iter__())

        self._ensure_snapshot()

        cls = type(self)
        t = cls.type
        proxy_of = t.__proxy_of__

        assert self._wrapped_index is not None
        assert self._set is not None
        assert self._unhashables is not None

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
            if oid in self._wrapped_index:
                continue

            self._wrapped_index[oid] = proxy

            if obj.__gel_new__:
                self._unhashables[id(proxy)] = proxy
            else:
                self._set.add(proxy)

            if not empty_items:
                self._items.append(proxy)

        if empty_items:
            # A LOT faster than `extend()` ¯\_(ツ)_/¯
            self._items = list(self._wrapped_index.values())

    def _cast_value(self, value: Any) -> tuple[_PT_co, _BMT_co]:
        cls = type(self)
        t = cls.type

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

    def _check_values(self, values: Iterable[Any]) -> list[_PT_co]:
        return [self._check_value(value) for value in values]

    def _check_value(self, value: Any) -> _PT_co:
        proxy, _ = self._cast_value(value)
        return proxy

    def _find_proxied_obj(self, item: _PT_co | _BMT_co) -> _PT_co | None:
        self._init_tracking()
        assert self._wrapped_index is not None

        if isinstance(item, AbstractGelProxyModel):
            item = item._p__obj__  # pyright: ignore [reportAssignmentType]

        return self._wrapped_index.get(id(item), None)

    def clear(self) -> None:
        super().clear()
        self._wrapped_index = None

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
                cls.basetype,
                self._items,
                self._wrapped_index.values()
                if self._wrapped_index is not None
                else None,
                self._initial_items,
                self._set,
                self._unhashables.values()
                if self._unhashables is not None
                else None,
                self._mode,
                self.__gel_overwrite_data__,
            ),
        )

    @staticmethod
    def _reconstruct_from_pickle(  # noqa: PLR0917
        origin: type[ProxyDistinctList[_PT_co, _BMT_co]],  # type: ignore [valid-type]
        tp: type[_PT_co],  # type: ignore [valid-type]
        basetp: type[_BMT_co],  # type: ignore [valid-type]
        items: list[_PT_co],
        wrapped_index: list[_PT_co] | None,
        initial_items: list[_PT_co] | None,
        hashables: set[_PT_co] | None,
        unhashables: list[_PT_co] | None,
        mode: Mode,
        gel_overwrite_data: bool,  # noqa: FBT001
    ) -> ProxyDistinctList[_PT_co, _BMT_co]:
        cls = cast(
            "type[ProxyDistinctList[_PT_co, _BMT_co]]",
            origin[tp, basetp],  # type: ignore [index]
        )
        lst = cls.__new__(cls)

        lst._items = items
        if wrapped_index is None:
            lst._wrapped_index = None
        else:
            lst._wrapped_index = {
                id(item._p__obj__): item for item in wrapped_index
            }

        lst._initial_items = initial_items
        lst._set = hashables
        if unhashables is None:
            lst._unhashables = None
        else:
            lst._unhashables = {id(item): item for item in unhashables}

        lst._mode = mode
        lst.__gel_overwrite_data__ = gel_overwrite_data

        return lst

    if TYPE_CHECKING:

        def append(self, value: _PT_co | _BMT_co) -> None: ...
        def insert(
            self, index: SupportsIndex, value: _PT_co | _BMT_co
        ) -> None: ...
        def __setitem__(
            self,
            index: SupportsIndex | slice,
            value: _PT_co | _BMT_co | Iterable[_PT_co | _BMT_co],
        ) -> None: ...
        def extend(self, values: Iterable[_PT_co | _BMT_co]) -> None: ...
        def remove(self, value: _PT_co | _BMT_co) -> None: ...
        def index(
            self,
            value: _PT_co | _BMT_co,
            start: SupportsIndex = 0,
            stop: SupportsIndex | None = None,
        ) -> int: ...
        def count(self, value: _PT_co | _BMT_co) -> int: ...
        def __add__(self, other: Iterable[_PT_co | _BMT_co]) -> Self: ...
        def __iadd__(self, other: Iterable[_PT_co | _BMT_co]) -> Self: ...
        def __isub__(self, other: Iterable[_PT_co | _BMT_co]) -> Self: ...
