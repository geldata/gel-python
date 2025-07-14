from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Any,
    SupportsIndex,
    TypeVar,
    cast,
)


import functools

from collections.abc import Iterable

from gel._internal import _typing_parametric as parametric

if TYPE_CHECKING:
    from ._base import AbstractGelSourceModel

from ._descriptors import AbstractGelProxyModel
from gel._internal._tracked_list import (
    AbstractTrackedList,
    DefaultList,
    Mode,
    requires_read,
)


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
            value = value.__gel_unwrap_proxy__()

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
            values = list(values)
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
