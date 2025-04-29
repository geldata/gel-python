# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    ClassVar,
    Generic,
    Iterable,
    Iterator,
    NamedTuple,
    SupportsIndex,
    TypeVar,
    cast,
    overload,
)

from typing_extensions import (
    Self,
)

from collections.abc import (
    Hashable,
    MutableSequence,
    Sequence,
)

import functools

import pydantic
import pydantic.fields
from pydantic._internal import _model_construction
import pydantic_core

from pydantic import Field as Field
from pydantic import PrivateAttr as PrivateAttr
from pydantic import computed_field as computed_field
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
    def __init__(self, iterable: Iterable[T] = ()) -> None:
        self._items: list[T] = []
        self._set: set[T] = set()
        self._type: type | None = None
        for item in iterable:
            self.append(item)

    @classmethod
    def _check_value(cls, value: Any) -> T:
        """Ensure `value` is of type T and return it."""
        if isinstance(value, cls.type):
            return value

        raise ValueError(
            f"{cls!r} accepts only values of type {cls.type!r}, "
            f"got {type(value)!r}"
        )

    @classmethod
    def _check_values(cls, values: Iterable[Any]) -> list[T]:
        """Ensure `values` is an iterable of type T and return it as a list."""
        result = []
        for value in values:
            result.append(cls._check_value(value))
        return result

    def __len__(self) -> int:
        return len(self._items)

    @overload
    def __getitem__(self, index: SupportsIndex) -> T:
        ...

    @overload
    def __getitem__(self, index: slice) -> Self:
        ...

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
            start, stop, step = index.indices(len(self._items))
            if step != 1:
                raise ValueError(
                    "Slice assignment with step != 1 not supported")
            prefix = self._items[:start]
            suffix = self._items[stop:]
            new_values = self._check_values(value)  # type: ignore [arg-type]
            self._items.clear()
            self._set.clear()
            for item in prefix + new_values + suffix:
                if item not in self._set:
                    self._items.append(item)
                    self._set.add(item)
        else:
            new_value = self._check_value(value)
            old = self._items[index]
            if value is old or value == old:
                return
            del self._items[index]
            self._set.remove(old)
            if value in self._set:
                j = self._items.index(new_value)
                del self._items[j]
                self._set.remove(new_value)
                index = int(index)
                if j < index:
                    index -= 1
            self._items.insert(index, new_value)
            self._set.add(new_value)

    def __delitem__(self, index: SupportsIndex | slice) -> None:
        if isinstance(index, slice):
            to_del = set(self._items[index])
            self._set -= to_del
            self._items = [item for item in self._items if item not in to_del]
        else:
            item = self._items.pop(index)
            self._set.remove(item)

    def insert(self, index: SupportsIndex, value: T) -> None:  # type: ignore [misc]
        """Insert item at index if not already present."""
        self._check_value(value)
        if value in self._set:
            return
        # clamp index
        index = int(index)
        if index < 0:
            index = max(0, len(self._items) + index + 1)
        if index > len(self._items):
            index = len(self._items)
        self._items.insert(index, value)
        self._set.add(value)

    def extend(self, values: Iterable[T]) -> None:
        if values is self:
            values = list(values)
        for v in values:
            self.append(v)

    def append(self, value: T) -> None:  # type: ignore [misc]
        self._check_value(value)
        if value not in self._set:
            self._items.append(value)
            self._set.add(value)

    def remove(self, value: T) -> None:  # type: ignore [misc]
        """Remove item; raise ValueError if missing."""
        try:
            self._set.remove(value)
        except KeyError:
            raise ValueError(f"DisinctList.remove(x): x not in list")
        else:
            self._items.remove(value)

    def pop(self, index: SupportsIndex = -1) -> T:
        """Remove and return item at index (default last)."""
        item = self._items.pop(index)
        self._set.remove(item)
        return item

    def clear(self) -> None:
        """Remove all items but keep element-type enforcement."""
        self._items.clear()
        self._set.clear()

    def __iter__(self) -> Iterator[T]:
        return iter(self._items)

    def __contains__(self, item: object) -> bool:
        return item in self._set

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
        return 1 if value in self._set else 0

    def __eq__(self, other: Any) -> bool:
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

    if TYPE_CHECKING:
        @overload
        def __set__(self, obj: Any, val: list[T]) -> None:
            ...

        @overload
        def __set__(self, obj: Any, val: DistinctList[T]) -> None:
            ...

        def __set__(self, obj: Any, val: Any) -> None:
            ...


MutableSequence.register(DistinctList)  # pyright: ignore [reportAttributeAccessIssue]
