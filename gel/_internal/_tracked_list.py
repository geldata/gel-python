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
    Iterable,
    Iterator,
    MutableSequence,
    Sequence,
)

import functools

from gel._internal import _typing_inspect
from gel._internal import _typing_parametric as parametric
from gel._internal._polyfills._strenum import StrEnum

if TYPE_CHECKING:
    from pathlib import Path


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
S = TypeVar("S", bound="AbstractCollection[Any]", covariant=True)


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
        def wrapper(self: S, *args: P.args, **kwargs: P.kwargs) -> R:  # type: ignore [misc]
            self._require_read(action, unsafe=unsafe)
            return func(self, *args, **kwargs)

        return cast("Callable[Concatenate[S, P], R]", wrapper)

    return decorator


class AbstractCollection(Generic[_T_co]):
    type: ClassVar[type[_T_co]]  # type: ignore [misc]

    # Current items in order.
    _items: list[_T_co]

    # Internal "mode" of the collection.
    _mode: Mode
    # External "mode" for the collection, used to guide if save() should
    # replace the multi-link/multi-prop with the new data or
    # update it with the changes.
    __gel_overwrite_data__: bool = False

    _allowed_write_only_ops: ClassVar[list[str]]

    def __init__(
        self,
        iterable: Iterable[_T_co] = (),
        *,
        __wrap_list__: bool = False,
        __mode__: Mode,
        __overwrite_data__: bool | None = None,
    ) -> None:
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
            self.__gel_overwrite_data__ = (
                False if __overwrite_data__ is None else __overwrite_data__
            )
            assert __mode__ is Mode.ReadWrite
            self._mode = Mode.ReadWrite

            self.__gel_reset_snapshot__()
        else:
            self._items = []

            # 'extend' is optimized in ProxyDistinctList
            # for use in __init__
            self.__gel_reset_snapshot__()
            self.__gel_extend__(iterable)

            # This is a new collection set to link/prop explicitly,
            # we want to override the link/prop with this new data
            # on save. That said, this could be set to "False" by
            # GelModel.__getattr__.
            self.__gel_overwrite_data__ = (
                True if __overwrite_data__ is None else __overwrite_data__
            )

            self._mode = __mode__

            self.__gel_empty_snapshot__()

    def __gel_extend__(self, it: Iterable[_T_co]) -> None:
        raise NotImplementedError

    def __gel_empty_snapshot__(self) -> None:
        raise NotImplementedError

    def __gel_reset_snapshot__(self) -> None:
        raise NotImplementedError

    def __gel_get_added__(self) -> list[_T_co]:
        raise NotImplementedError

    def __gel_get_removed__(self) -> Iterable[_T_co]:
        raise NotImplementedError

    def __gel_has_changes__(self) -> bool:
        raise NotImplementedError

    def __gel_commit__(self) -> None:
        # Flip "override mode" to False; it can be set back to True
        # if the user assigns a new list to the field, e.g.
        # `model.multilink = [ ... ]`. Setting it back to False means
        # that now we'll be tracking changes and generating update
        # queries for the collection (not replacement queries.)
        self.__gel_overwrite_data__ = False

    def __gel_post_commit_check__(self, path: Path) -> None:
        if self.__gel_overwrite_data__:
            raise ValueError(
                f"{path} list did not reset self.__gel_overwrite_data__"
            )

    def __gel_basetype_iter__(self) -> Iterator[_T_co]:
        return iter(self._items)

    def unsafe_iter(self) -> Iterator[_T_co]:
        """Iterate over the list disregarding the access mode."""
        return iter(self._items)

    def unsafe_len(self) -> int:
        """Return the length of the list disregarding the access mode."""
        return len(self._items)

    def _require_read(
        self,
        action: str,
        /,
        *,
        unsafe: str | None = None,
    ) -> None:
        if self._mode is Mode.ReadWrite:
            return

        allowed_ops = ", ".join(
            f"`{op}`" for op in self._allowed_write_only_ops
        )

        # XXX Add a link to our docs right here!
        msg = (
            f"Cannot {action} the collection in write-only mode. "
            f"The only allowed operations are: {allowed_ops}"
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


@functools.total_ordering
class AbstractTrackedList(
    Sequence[_T_co],
    AbstractCollection[_T_co],
):
    """A mutable sequence that enforces element-type covariance at runtime
    and tracks changes to itself.
    """

    _allowed_write_only_ops: ClassVar[list[str]] = [
        ".append()",
        ".extend()",
        ".remove()",
        "+=",
        "-=",
    ]

    # Initial snapshot for change tracking
    _initial_items: list[_T_co] | None

    def __gel_extend__(self, it: Iterable[_T_co]) -> None:
        self.extend(it)

    def _ensure_snapshot(self) -> None:
        if self._initial_items is None:
            self._initial_items = list(self._items)

    def __gel_empty_snapshot__(self) -> None:
        self._initial_items = []

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

    def __gel_has_changes__(self) -> bool:
        if self._initial_items is None:
            return False
        return self._items != self._initial_items

    def __gel_commit__(self) -> None:
        self._initial_items = None
        super().__gel_commit__()

    def __gel_post_commit_check__(self, path: Path) -> None:
        if self._initial_items:
            raise ValueError(f"{path} has non-empty `self._initial_items`")
        super().__gel_post_commit_check__(path)

    def _check_value(self, value: Any) -> _T_co:
        """Ensure `value` is of type T and return it."""
        cls = type(self)

        if isinstance(value, cls.type):
            return value

        raise ValueError(
            f"{cls!r} accepts only values of type {cls.type!r}, "
            f"got {type(value)!r}",
        )

    def _check_values(self, values: Iterable[Any]) -> list[_T_co]:
        """Ensure `values` is an iterable of type T and return it as a list."""
        if isinstance(values, AbstractTrackedList):
            values = values.__gel_basetype_iter__()
        return [self._check_value(value) for value in values]

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

    @requires_read("get the length of", unsafe="unsafe_len()")
    def __len__(self) -> int:
        return len(self._items)

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
        if values is self:
            values = list(self)
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

    def __add__(self, other: Iterable[_T_co]) -> Self:
        new = type(self)(self._items, __mode__=self._mode)
        new.extend(other)
        return new

    def __iadd__(self, other: Iterable[_T_co]) -> Self:
        self.extend(other)
        return self

    def __isub__(self, other: Iterable[_T_co]) -> Self:
        for item in other:
            self.remove(item)
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
