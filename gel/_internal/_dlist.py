# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Any,
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

from gel._internal import _typing_parametric as parametric


T = TypeVar("T", bound=Hashable, covariant=True)


@functools.total_ordering
class DistinctList(
    parametric.SingleParametricType[T],
    Sequence[T],
    Generic[T],
):
    """A mutable, ordered set-like list that enforces element-type invariance
    at runtime and maintains distinctness of elements in insertion order using
    a list and set.
    """

    # Current items in order.
    _items: list[T]

    # Set of (hashable) items to maintain distinctness.
    _set_impl: set[T] | None

    # Assuming unhashable items compare by object identity,
    # the dict below is used as an extension for distinctness
    # checks.
    _unhashables_impl: dict[int, T] | None

    # Initial snapshot for change tracking
    _initial_items: list[T] | None

    def __init__(
        self, iterable: Iterable[T] = (), *, __wrap_list__: bool = False
    ) -> None:
        self._set_impl = None
        self._unhashables_impl = None
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
            self._items = []
            for item in iterable:
                self.append(item)

    def _ensure_snapshot(self) -> None:
        if self._initial_items is None:
            self._initial_items = list(self._items)

    def __gel_get_added__(self) -> Iterable[T]:
        if self._initial_items is None:
            return ()
        return [
            item for item in self._items if item not in self._initial_items
        ]

    def __gel_get_removed__(self) -> Iterable[T]:
        if self._initial_items is None:
            return ()
        return [
            item for item in self._initial_items if item not in self._items
        ]

    def __gel_commit__(self) -> None:
        self._initial_items = None

    @property
    def _set(self) -> set[T]:
        if self._set_impl is None:
            self._set_impl = set(self._items)
            assert len(self._set_impl) == len(self._items)
        return self._set_impl

    @property
    def _unhashables(self) -> dict[int, T]:
        if self._unhashables_impl is None:
            self._unhashables_impl = {}
        return self._unhashables_impl

    @classmethod
    def _check_value(cls, value: Any) -> T:
        """Ensure `value` is of type T and return it."""
        if isinstance(value, cls.type):
            return value

        raise ValueError(
            f"{cls!r} accepts only values of type {cls.type!r}, "
            f"got {type(value)!r}",
        )

    @classmethod
    def _check_values(cls, values: Iterable[Any]) -> list[T]:
        """Ensure `values` is an iterable of type T and return it as a list."""
        return [cls._check_value(value) for value in values]

    def __len__(self) -> int:
        return len(self._items)

    if TYPE_CHECKING:

        @overload
        def __getitem__(self, index: SupportsIndex) -> T: ...

        @overload
        def __getitem__(self, index: slice) -> Self: ...

    def __getitem__(self, index: SupportsIndex | slice) -> T | Self:
        if isinstance(index, slice):
            return type(self)(self._items[index])
        else:
            return self._items[index]

    def __setitem__(
        self,
        index: SupportsIndex | slice,
        value: T | Iterable[T],
    ) -> None:
        if isinstance(index, slice):
            self._ensure_snapshot()
            start, stop, step = index.indices(len(self._items))
            if step != 1:
                raise ValueError(
                    "Slice assignment with step != 1 not supported",
                )
            prefix = self._items[:start]
            suffix = self._items[stop:]
            new_values = self._check_values(value)  # type: ignore [arg-type]
            self.clear()
            for item in (*prefix, *new_values, *suffix):
                self._append_no_check(item)
        else:
            new_value = self._check_value(value)
            old = self._items[index]
            if new_value is old or new_value == old:
                return
            self._ensure_snapshot()
            del self._items[index]
            vid = id(new_value)
            if self._unhashables.pop(vid, None) is None:
                self._set.remove(old)

            if new_value not in self:
                try:
                    self._set.add(new_value)
                except TypeError:
                    self._unhashables[vid] = new_value

                self._items.insert(index, new_value)

    def __delitem__(self, index: SupportsIndex | slice) -> None:
        self._ensure_snapshot()
        if isinstance(index, slice):
            to_remove = type(self)(self._items[index])
            for item in to_remove:
                vid = id(item)
                if self._unhashables.pop(vid, None) is None:
                    # safe to assume hashable if not in _unhashables
                    self._set.discard(item)
            self._items = [it for it in self._items if it not in to_remove]
        else:
            item = self._items.pop(index)
            vid = id(item)
            if vid in self._unhashables:
                del self._unhashables[vid]
            else:
                self._set.remove(item)

    def __iter__(self) -> Iterator[T]:
        return iter(self._items)

    def __contains__(self, item: object) -> bool:
        if id(item) in self._unhashables:
            return True

        try:
            return item in self._set
        except TypeError:
            return False

    def insert(self, index: SupportsIndex, value: T) -> None:  # type: ignore [misc]
        """Insert item at index if not already present."""
        if value in self:
            return

        self._check_value(value)
        self._ensure_snapshot()

        # clamp index
        index = int(index)
        if index < 0:
            index = max(0, len(self._items) + index + 1)
        index = min(index, len(self._items))

        try:
            if value not in self._set:
                self._items.insert(index, value)
                self._set.add(value)
        except TypeError:
            # fallback for unhashables
            vid = id(value)
            if vid not in self._unhashables:
                self._items.insert(index, value)
                self._unhashables[vid] = value

    def extend(self, values: Iterable[T]) -> None:
        if values is self:
            values = list(values)
        for v in values:
            self.append(v)

    def append(self, value: T) -> None:  # type: ignore [misc]
        self._check_value(value)
        self._ensure_snapshot()
        self._append_no_check(value)

    def _append_no_check(self, value: T) -> None:  # type: ignore[misc]
        if value in self:
            return
        else:
            try:
                self._set.add(value)
            except TypeError:
                self._unhashables[id(value)] = value

            self._items.append(value)

    def remove(self, value: T) -> None:  # type: ignore [misc]
        """Remove item; raise ValueError if missing."""
        self._ensure_snapshot()
        if self._unhashables.pop(id(value), None) is None:
            try:
                self._set.remove(value)
            except (KeyError, TypeError):
                raise ValueError(
                    "DisinctList.remove(x): x not in list",
                ) from None

        self._items.remove(value)

    def pop(self, index: SupportsIndex = -1) -> T:
        """Remove and return item at index (default last)."""
        self._ensure_snapshot()
        item = self._items.pop(index)
        if self._unhashables.pop(id(item), None) is None:
            self._set.remove(item)
        return item

    def clear(self) -> None:
        """Remove all items but keep element-type enforcement."""
        self._ensure_snapshot()
        self._items.clear()
        self._set.clear()
        self._unhashables.clear()

    def index(
        self,
        value: T,  # type: ignore [misc]
        start: SupportsIndex = 0,
        stop: SupportsIndex | None = None,
    ) -> int:
        """Return first index of value."""
        return self._items.index(
            value,
            start,
            len(self._items) if stop is None else stop,
        )

    def count(self, value: T) -> int:  # type: ignore [misc]
        """Return 1 if item is present, else 0."""
        if id(value) in self._unhashables:
            return 1
        else:
            try:
                return 1 if value in self._set else 0
            except TypeError:
                return 0

    def promote_unhashables(self, *items: T) -> None:  # type: ignore [misc]
        """Try hashing each unhashable: if it now hashes, move into `_set`, or
        if it duplicates an existing, drop it from the list."""
        if not items:
            pairs = list(self._unhashables.items())
        else:
            pairs = [(id(item), item) for item in items]

        for vid, item in pairs:
            try:
                hash(item)
            except TypeError:
                continue  # still unhashable

            # now hashable: if duplicate, remove outright; otherwise add to set
            if item in self._set:
                # drop from items list to keep distinctness
                self._items.remove(item)
            else:
                self._set.add(item)

            # in either case, no longer "unhashable"
            del self._unhashables[vid]

    __hash__ = None  # type: ignore [assignment]

    def __eq__(self, other: object) -> bool:
        if isinstance(other, DistinctList):
            return self._items == other._items
        elif isinstance(other, list):
            return self._items == other
        else:
            return NotImplemented

    def __lt__(self, other: Any) -> bool:
        if isinstance(other, DistinctList):
            return self._items < other._items
        elif isinstance(other, list):
            return self._items < other
        else:
            return NotImplemented

    def __repr__(self) -> str:
        return repr(self._items)

    def __add__(self, other: Iterable[T]) -> Self:
        new = type(self)(self._items)
        new.extend(other)
        return new

    def __iadd__(self, other: Iterable[T]) -> Self:
        self.extend(other)
        return self

    if TYPE_CHECKING:  # pragma: no cover

        @overload
        def __set__(self, obj: Any, val: list[T]) -> None: ...

        @overload
        def __set__(self, obj: Any, val: DistinctList[T]) -> None: ...

        def __set__(self, obj: Any, val: Any) -> None: ...


MutableSequence.register(DistinctList)  # pyright: ignore [reportAttributeAccessIssue]
