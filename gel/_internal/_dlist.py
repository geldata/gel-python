# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Generic,
    SupportsIndex,
    TypeVar,
    overload,
)

from typing_extensions import (
    Self,
)

from collections.abc import (
    Hashable,
    Iterable,
    Iterator,
    MutableSequence,
    Sequence,
)

import functools

from gel._internal import _typing_inspect
from gel._internal import _typing_parametric as parametric


_T_co = TypeVar("_T_co", covariant=True)


@functools.total_ordering
class AbstractTrackedList(
    Sequence[_T_co],
    Generic[_T_co],
):
    """A mutable sequence that enforces element-type covariance at runtime
    and tracks changes to itself.
    """

    type: ClassVar[type[_T_co]]  # type: ignore [misc]

    # Current items in order.
    _items: list[_T_co]

    # Initial snapshot for change tracking
    _initial_items: list[_T_co] | None

    def __init__(
        self,
        iterable: Iterable[_T_co] = (),
        *,
        __wrap_list__: bool = False,
    ) -> None:
        self._initial_items = None

        if __wrap_list__:
            # __wrap_list__ is set to True inside the codecs pipeline
            # because we can trust that the objects are of the correct
            # type and can avoid the costly validation.

            if type(iterable) is not list:
                raise ValueError(
                    "__wrap_list__ is True but iterable is not a list"
                )

            self._items = iterable
        else:
            self._initial_items = []
            self._items = []
            # 'extend' is optimized in ProxyDistinctList
            # for use in __init__
            self.extend(iterable)

    def _ensure_snapshot(self) -> None:
        if self._initial_items is None:
            self._initial_items = list(self._items)

    def __gel_get_added__(self) -> list[_T_co]:
        if self._initial_items is None:
            return []
        return [
            item for item in self._items if item not in self._initial_items
        ]

    def __gel_get_removed__(self) -> Iterable[_T_co]:
        if self._initial_items is None:
            return ()
        return [
            item for item in self._initial_items if item not in self._items
        ]

    def __gel_commit__(self) -> None:
        self._initial_items = None

    def _check_value(self, value: Any) -> _T_co:
        """Ensure `value` is of type T and return it."""
        cls = type(self)

        if isinstance(value, cls.type):
            return value  # type: ignore [no-any-return]

        raise ValueError(
            f"{cls!r} accepts only values of type {cls.type!r}, "
            f"got {type(value)!r}",
        )

    def _check_values(self, values: Iterable[Any]) -> list[_T_co]:
        """Ensure `values` is an iterable of type T and return it as a list."""
        return [self._check_value(value) for value in values]

    def __len__(self) -> int:
        return len(self._items)

    if TYPE_CHECKING:

        @overload
        def __getitem__(self, index: SupportsIndex) -> _T_co: ...

        @overload
        def __getitem__(self, index: slice) -> Self: ...

    def __getitem__(self, index: SupportsIndex | slice) -> _T_co | Self:
        if isinstance(index, slice):
            return type(self)(self._items[index])
        else:
            return self._items[index]

    def __setitem__(
        self,
        index: SupportsIndex | slice,
        value: _T_co | Iterable[_T_co],
    ) -> None:
        self._ensure_snapshot()
        if isinstance(index, slice):
            new_values = self._check_values(value)  # type: ignore [arg-type]
            self._items[index] = new_values
        else:
            new_value = self._check_value(value)
            self._items[index] = new_value

    def __delitem__(self, index: SupportsIndex | slice) -> None:
        self._ensure_snapshot()
        del self._items[index]

    def __iter__(self) -> Iterator[_T_co]:
        return iter(self._items)

    def __contains__(self, item: object) -> bool:
        return item in self._items

    def insert(self, index: SupportsIndex, value: _T_co) -> None:  # type: ignore [misc]
        value = self._check_value(value)
        self._ensure_snapshot()
        self._items.insert(index, value)

    def extend(self, values: Iterable[_T_co]) -> None:
        values = self._check_values(values)
        self._ensure_snapshot()
        self._items.extend(values)

    def append(self, value: _T_co) -> None:  # type: ignore [misc]
        value = self._check_value(value)
        self._ensure_snapshot()
        self._items.append(value)

    def remove(self, value: _T_co) -> None:  # type: ignore [misc]
        """Remove item; raise ValueError if missing."""
        self._ensure_snapshot()
        self._items.remove(value)

    def pop(self, index: SupportsIndex = -1) -> _T_co:
        """Remove and return item at index (default last)."""
        self._ensure_snapshot()
        return self._items.pop(index)

    def clear(self) -> None:
        """Remove all items but keep element-type enforcement."""
        self._ensure_snapshot()
        self._items.clear()

    def index(
        self,
        value: _T_co,  # type: ignore [misc]
        start: SupportsIndex = 0,
        stop: SupportsIndex | None = None,
    ) -> int:
        """Return first index of value."""
        return self._items.index(
            value,
            start,
            len(self._items) if stop is None else stop,
        )

    def count(self, value: _T_co) -> int:  # type: ignore [misc]
        return self._items.count(value)

    __hash__ = None  # type: ignore [assignment]

    def __eq__(self, other: object) -> bool:
        if isinstance(other, AbstractTrackedList):
            return self._items == other._items
        elif isinstance(other, list):
            return self._items == other
        else:
            return NotImplemented

    def __lt__(self, other: Any) -> bool:
        if isinstance(other, AbstractTrackedList):
            return self._items < other._items
        elif isinstance(other, list):
            return self._items < other
        else:
            return NotImplemented

    def __repr__(self) -> str:
        return repr(self._items)

    def __add__(self, other: Iterable[_T_co]) -> Self:
        new = type(self)(self._items)
        new.extend(other)
        return new

    def __iadd__(self, other: Iterable[_T_co]) -> Self:
        self.extend(other)
        return self

    if TYPE_CHECKING:  # pragma: no cover

        @overload
        def __set__(self, obj: Any, val: list[_T_co]) -> None: ...

        @overload
        def __set__(
            self, obj: Any, val: AbstractTrackedList[_T_co]
        ) -> None: ...

        def __set__(self, obj: Any, val: Any) -> None: ...


MutableSequence.register(AbstractTrackedList)  # pyright: ignore [reportAttributeAccessIssue]


_BT = TypeVar("_BT")


class AbstractDowncastingList(Generic[_T_co, _BT]):
    supertype: ClassVar[type[_BT]]  # type: ignore [misc]
    type: ClassVar[type[_T_co]]  # type: ignore [misc]

    def _check_value(self, value: Any) -> _T_co:
        cls = type(self)

        t = cls.type
        bt = cls.supertype
        if isinstance(value, cls.type):
            return value  # type: ignore [no-any-return]
        elif not _typing_inspect.is_valid_isinstance_arg(bt) or isinstance(
            value, bt
        ):
            return t(value)  # type: ignore [no-any-return]

        raise ValueError(
            f"{cls!r} accepts only values of type {t.__name__} "
            f"or {bt.__name__}, got {type(value)!r}",
        )

    if TYPE_CHECKING:

        def append(self, value: _T_co | _BT) -> None: ...
        def insert(self, index: SupportsIndex, value: _T_co | _BT) -> None: ...
        def __setitem__(
            self,
            index: SupportsIndex | slice,
            value: _T_co | _BT | Iterable[_T_co | _BT],
        ) -> None: ...
        def extend(self, values: Iterable[_T_co | _BT]) -> None: ...
        def remove(self, value: _T_co | _BT) -> None: ...
        def index(
            self,
            value: _T_co | _BT,
            start: SupportsIndex = 0,
            stop: SupportsIndex | None = None,
        ) -> int: ...
        def count(self, value: _T_co | _BT) -> int: ...
        def __add__(self, other: Iterable[_T_co | _BT]) -> Self: ...
        def __iadd__(self, other: Iterable[_T_co | _BT]) -> Self: ...


class TrackedList(
    parametric.SingleParametricType[_T_co],
    AbstractTrackedList[_T_co],
):
    pass


class _DowncastingList(parametric.ParametricType, Generic[_T_co, _BT]):
    supertype: ClassVar[type[_BT]]  # type: ignore [misc]
    type: ClassVar[type[_T_co]]  # type: ignore [misc]


class DowncastingTrackedList(
    _DowncastingList[_T_co, _BT],
    AbstractDowncastingList[_T_co, _BT],
    AbstractTrackedList[_T_co],
):
    pass


_HT_co = TypeVar("_HT_co", bound=Hashable, covariant=True)


@functools.total_ordering
class AbstractDistinctList(AbstractTrackedList[_HT_co]):
    """A mutable, ordered set-like list that enforces element-type covariance
    at runtime and maintains distinctness of elements in insertion order using
    a list and set.
    """

    # Set of (hashable) items to maintain distinctness.
    _set: set[_HT_co] | None

    # Assuming unhashable items compare by object identity,
    # the dict below is used as an extension for distinctness
    # checks.
    _unhashables: dict[int, _HT_co] | None

    def __init__(
        self, iterable: Iterable[_HT_co] = (), *, __wrap_list__: bool = False
    ) -> None:
        self._set = None
        self._unhashables = None
        super().__init__(iterable, __wrap_list__=__wrap_list__)

    def _ensure_snapshot(self) -> None:
        # "_ensure_snapshot" is called right before any mutation:
        # this is the perfect place to initialize `self._set` and
        # `self._unhashables`.
        self._init_tracking()
        super()._ensure_snapshot()

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

    def _track_item(self, item: _HT_co) -> None:  # type: ignore [misc]
        assert self._set is not None
        try:
            self._set.add(item)
        except TypeError:
            pass
        else:
            return

        assert self._unhashables is not None
        self._unhashables[id(item)] = item

    def _untrack_item(self, item: _HT_co) -> None:  # type: ignore [misc]
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

    def _is_tracked(self, item: _HT_co) -> bool:  # type: ignore [misc]
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
        value: _HT_co | Iterable[_HT_co],
    ) -> None:
        self._ensure_snapshot()

        if isinstance(index, slice):
            start, stop, step = index.indices(len(self._items))
            if step != 1:
                raise ValueError(
                    "Slice assignment with step != 1 not supported",
                )

            new_values = self._check_values(value)  # type: ignore [arg-type]

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

    def __contains__(self, item: object) -> bool:
        return self._is_tracked(item)  # type: ignore [arg-type]

    def insert(self, index: SupportsIndex, value: _HT_co) -> None:  # type: ignore [misc]
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

    def extend(self, values: Iterable[_HT_co]) -> None:
        if values is self:
            values = list(values)
        for v in values:
            self.append(v)

    def append(self, value: _HT_co) -> None:  # type: ignore [misc]
        value = self._check_value(value)
        self._ensure_snapshot()
        self._append_no_check(value)

    def _append_no_check(self, value: _HT_co) -> None:  # type: ignore[misc]
        if self._is_tracked(value):
            return
        self._track_item(value)
        self._items.append(value)

    def remove(self, value: _HT_co) -> None:  # type: ignore [misc]
        """Remove item; raise ValueError if missing."""
        if not self._is_tracked(value):
            pass

        self._ensure_snapshot()
        value = self._check_value(value)
        self._untrack_item(value)
        self._items.remove(value)

    def pop(self, index: SupportsIndex = -1) -> _HT_co:
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

    def index(
        self,
        value: _HT_co,  # type: ignore [misc]
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

    def count(self, value: _HT_co) -> int:  # type: ignore [misc]
        """Return 1 if item is present, else 0."""
        value = self._check_value(value)
        if self._is_tracked(value):
            return 1
        else:
            return 0


class DistinctList(
    parametric.SingleParametricType[_T_co],
    AbstractDistinctList[_T_co],
):
    pass


class DowncastingDistinctList(
    _DowncastingList[_T_co, _BT],
    AbstractDowncastingList[_T_co, _BT],
    AbstractDistinctList[_T_co],
):
    pass
