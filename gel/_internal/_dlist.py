# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Any,
    Concatenate,
    ClassVar,
    Generic,
    SupportsIndex,
    TypeVar,
    ParamSpec,
    cast,
    overload,
    final,
)

from typing_extensions import (
    Self,
)

from collections.abc import (
    Callable,
    Hashable,
    Iterable,
    Iterator,
    MutableSequence,
    Sequence,
)

import functools

from gel._internal import _typing_inspect
from gel._internal import _typing_parametric as parametric
from gel._internal._polyfills._strenum import StrEnum


_T_co = TypeVar("_T_co", covariant=True)


class DefaultList(list[Any]):  # noqa: FURB189
    # A special marker list that can only come from Pydantic initializing
    # the multi-link/multi-prop field with a default value.
    pass


@final
class Mode(StrEnum):
    Write = "Write"
    ReadWrite = "ReadWrite"


P = ParamSpec("P")
R = TypeVar("R")
S = TypeVar("S", bound="AbstractTrackedList[Any]")


def requires_read(
    action: str, /, *, unsafe: str | None = None
) -> Callable[
    [Callable[Concatenate[S, P], R]],
    Callable[Concatenate[S, P], R],
]:
    def decorator(
        func: Callable[Concatenate[S, P], R],
    ) -> Callable[Concatenate[S, P], R]:
        @functools.wraps(func)
        def wrapper(self: S, *args: P.args, **kwargs: P.kwargs) -> R:
            self._require_read(action, unsafe=unsafe)
            return func(self, *args, **kwargs)

        return cast("Callable[Concatenate[S, P], R]", wrapper)

    return decorator


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

    # Internal "mode" of the list.
    _mode: Mode
    # External "mode" for the list, used to guide if save() should
    # replace the multi-link/multi-prop with the new data or
    # update it with the changes.
    __gel_overwrite_data__: bool = False

    def __init__(
        self,
        iterable: Iterable[_T_co] = (),
        *,
        __wrap_list__: bool = False,
        __mode__: Mode,
    ) -> None:
        self._initial_items = None

        if __wrap_list__:
            # __wrap_list__ is set to True inside the codecs pipeline
            # because we can trust that the objects are of the correct
            # type and can avoid the costly validation.

            if type(iterable) not in {list, DefaultList}:
                raise ValueError(
                    "__wrap_list__ is True but iterable is not a list or "
                    "DefaultList"
                )

            self._items = cast("list[_T_co]", iterable)

            # This collection was loaded from the database, we don't
            # want to override its link/prop on save with new data,
            # we want to track changes instead.
            self.__gel_overwrite_data__ = False
            assert __mode__ is Mode.ReadWrite
            self._mode = Mode.ReadWrite
        else:
            self._initial_items = []
            self._items = []
            # 'extend' is optimized in ProxyDistinctList
            # for use in __init__
            self.extend(iterable)

            # This is a new collection set to link/prop explicitly,
            # we want to override the link/prop with this new data
            # on save. That said, this could be set to "False" by
            # GelModel.__getattr__.
            self.__gel_overwrite_data__ = True

            self._mode = __mode__

    def _require_read(
        self,
        action: str,
        /,
        *,
        unsafe: str | None = None,
    ) -> None:
        if self._mode is Mode.ReadWrite:
            return

        # XXX Add a link to our docs right here!
        msg = (
            f"Cannot {action} the collection in write-only mode. "
            f"The only allowed operations are `.append()`, `.extend()`, "
            f"`.remove()`, and their shortcuts `+=` and `-=` operators."
            f"\n\n"
            f"This happens when a collection is accessed without being "
            f"fetched from the database or without an explicit assignment "
            f"to a list."
        )

        if unsafe is not None:
            msg += (
                f"\n\nIf you must, use the {unsafe} alternative function, "
                f"but it is only advised for debugging purposes."
            )

        raise RuntimeError(msg)

    def _ensure_snapshot(self) -> None:
        if self._initial_items is None:
            self._initial_items = list(self._items)

    def __gel_reset_snapshot__(self) -> None:
        self._initial_items = None

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

        # Flip "override mode" to False; it can be set back to True
        # if the user assigns a new list to the field, e.g.
        # `model.multilink = [ ... ]`. Setting it back to False means
        # that now we'll be tracking changes and generating update
        # queries for the collection (not replacement queries.)
        self.__gel_overwrite_data__ = False

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

    @requires_read("get the length of", unsafe="unsafe_len()")
    def __len__(self) -> int:
        return len(self._items)

    if TYPE_CHECKING:

        @overload
        def __getitem__(self, index: SupportsIndex) -> _T_co: ...

        @overload
        def __getitem__(self, index: slice) -> Self: ...

    @requires_read("index items of")
    def __getitem__(self, index: SupportsIndex | slice) -> _T_co | Self:
        if isinstance(index, slice):
            return type(self)(self._items[index], __mode__=Mode.ReadWrite)
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

    @requires_read("iterate over", unsafe="unsafe_iter()")
    def __iter__(self) -> Iterator[_T_co]:
        return iter(self._items)

    @requires_read("use `in` operator on")
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

    @requires_read("index items of")
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

    @requires_read("count items of")
    def count(self, value: _T_co) -> int:  # type: ignore [misc]
        return self._items.count(value)

    __hash__ = None  # type: ignore [assignment]

    def __eq__(self, other: object) -> bool:
        if isinstance(other, AbstractTrackedList):
            if self._mode is Mode.Write:
                return False
            return self._items == other._items
        elif isinstance(other, list):
            if self._mode is Mode.Write:
                return False
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
        if self._mode is Mode.Write:
            return f"[WRITE-ONLY {self._items!r}]"
        else:
            return repr(self._items)

    @requires_read("add another collection to")
    def __add__(self, other: Iterable[_T_co]) -> Self:
        new = type(self)(self._items, __mode__=Mode.ReadWrite)
        new.extend(other)
        return new

    def __iadd__(self, other: Iterable[_T_co]) -> Self:
        self.extend(other)
        return self

    def __isub__(self, other: Iterable[_T_co]) -> Self:
        for item in other:
            self.remove(item)
        return self

    def unsafe_iter(self) -> Iterator[_T_co]:
        """Iterate over the list disregarding the access mode."""
        return iter(self._items)

    def unsafe_len(self) -> int:
        """Return the length of the list disregarding the access mode."""
        return len(self._items)

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
        def __isub__(self, other: Iterable[_T_co | _BT]) -> Self: ...


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
    def __reduce__(self) -> tuple[Any, ...]:
        cls = type(self)
        return (
            cls._reconstruct_from_pickle,
            (
                cls.__parametric_origin__,
                cls.type,
                cls.supertype,
                self._items,
                self._initial_items,
                self._mode,
                self.__gel_overwrite_data__,
            ),
        )

    @staticmethod
    def _reconstruct_from_pickle(  # noqa: PLR0917
        origin: type[DowncastingTrackedList[_T_co, _BT]],
        tp: type[_T_co],  # pyright: ignore [reportGeneralTypeIssues]
        supertp: type[_BT],
        items: list[_T_co],
        initial_items: list[_T_co] | None,
        mode: Mode,
        gel_overwrite_data: bool,  # noqa: FBT001
    ) -> DowncastingTrackedList[_T_co, _BT]:
        cls = cast(
            "type[DowncastingTrackedList[_T_co, _BT]]",
            origin[tp, supertp],  # type: ignore [index]
        )
        lst = cls.__new__(cls)

        lst._items = items
        lst._initial_items = initial_items

        lst._mode = mode
        lst.__gel_overwrite_data__ = gel_overwrite_data

        return lst


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
        self,
        iterable: Iterable[_HT_co] = (),
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

    @requires_read("use `in` operator on")
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

    @requires_read("index items of")
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

    @requires_read("count items of")
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
        origin: type[DistinctList[_T_co]],
        tp: type[_T_co],  # pyright: ignore [reportGeneralTypeIssues]
        items: list[_T_co],
        initial_items: list[_T_co] | None,
        hashables: set[_T_co] | None,
        unhashables: list[_T_co] | None,
        mode: Mode,
        gel_overwrite_data: bool,  # noqa: FBT001
    ) -> DistinctList[_T_co]:
        cls = cast(
            "type[DistinctList[_T_co]]",
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


_ADL_co = TypeVar("_ADL_co", bound=AbstractTrackedList[Any], covariant=True)


def convert_to_dlist(
    value: Any,
    tp: type[_ADL_co],
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
        raise TypeError(f"could not convert {type(value)} to {tp.__name__}")
