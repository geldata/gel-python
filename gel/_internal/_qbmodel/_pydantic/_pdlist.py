# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    cast,
    Any,
    ClassVar,
    Generic,
    SupportsIndex,
    TypeVar,
)

if TYPE_CHECKING:
    from collections.abc import Iterable
    from gel._internal._tracked_list import Mode

from typing_extensions import (
    Self,
)

from gel._internal._qbmodel._abstract._distinct_list import (
    AbstractDistinctList,
)
from gel._internal import _typing_parametric as parametric

from ._models import GelModel, ProxyModel

_BMT_co = TypeVar("_BMT_co", bound=GelModel, covariant=True)
"""Base model type"""

_PT_co = TypeVar("_PT_co", bound=ProxyModel[GelModel], covariant=True)
"""Proxy model"""


ll_getattr = object.__getattribute__


class ProxyDistinctList(
    parametric.ParametricType,
    AbstractDistinctList[_PT_co],
    Generic[_PT_co, _BMT_co],
):
    # Mapping of object IDs to ProxyModels that wrap them.
    _wrapped_index: dict[int, _PT_co] | None = None

    basetype: ClassVar[type[_BMT_co]]  # type: ignore [misc]
    type: ClassVar[type[_PT_co]]  # type: ignore [misc]

    def _init_tracking(self) -> None:
        super()._init_tracking()

        if self._wrapped_index is None:
            self._wrapped_index = {}
            for item in self._items:
                assert isinstance(item, ProxyModel)
                wrapped = ll_getattr(item, "_p__obj__")
                self._wrapped_index[id(wrapped)] = cast("_PT_co", item)

    def _track_item(self, item: _PT_co) -> None:  # type: ignore [misc]
        assert isinstance(item, ProxyModel)
        super()._track_item(cast("_PT_co", item))
        assert self._wrapped_index is not None
        wrapped = ll_getattr(item, "_p__obj__")
        self._wrapped_index[id(wrapped)] = cast("_PT_co", item)

    def _untrack_item(self, item: _PT_co) -> None:  # type: ignore [misc]
        assert isinstance(item, ProxyModel)
        super()._untrack_item(cast("_PT_co", item))
        assert self._wrapped_index is not None
        wrapped = ll_getattr(item, "_p__obj__")
        self._wrapped_index.pop(id(wrapped), None)

    def _is_tracked(self, item: _PT_co | _BMT_co) -> bool:
        self._init_tracking()
        assert self._wrapped_index is not None

        if isinstance(item, ProxyModel):
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
            values = list(values)

        self._ensure_snapshot()

        cls = type(self)
        t = cls.type
        proxy_of = t.__proxy_of__

        assert self._wrapped_index is not None
        assert self._set is not None
        assert self._unhashables is not None

        # For an empty list we can call one extend() call instead
        # of slow iterative appends.
        empty_items = len(self._wrapped_index) == len(self._items) == 0

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
            existing_proxy = self._wrapped_index.get(oid)
            if existing_proxy is None:
                self._wrapped_index[oid] = proxy
            else:
                if (
                    existing_proxy.__linkprops__.__dict__
                    != proxy.__linkprops__.__dict__
                ):
                    raise ValueError(
                        f"the list already contains {v!r} with "
                        f"a different set of link properties"
                    )

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

        if not isinstance(value, ProxyModel) and isinstance(value, bt):
            # It's not a proxy, but the object is of the correct type --
            # re-wrap it in a correct proxy.
            return (
                t.__gel_proxy_construct__(value, {}),
                value,
            )

        proxy = t.model_validate(value)
        return (
            proxy,
            ll_getattr(proxy, "_p__obj__"),
        )

    def _check_value(self, value: Any) -> _PT_co:
        proxy, obj = self._cast_value(value)

        # We have to check if a proxy around the same object is already
        # present in the list.
        self._init_tracking()
        assert self._wrapped_index is not None
        try:
            existing_proxy = self._wrapped_index[id(obj)]
        except KeyError:
            return proxy

        assert isinstance(existing_proxy, ProxyModel)

        if (
            existing_proxy.__linkprops__.__dict__
            != proxy.__linkprops__.__dict__
        ):
            raise ValueError(
                f"the list already contains {value!r} with "
                f" a different set of link properties"
            )
        # Return the already present identical proxy instead of inserting
        # another one
        return existing_proxy  # type: ignore [return-value]

    def _find_proxied_obj(self, item: _PT_co | _BMT_co) -> _PT_co | None:
        self._init_tracking()
        assert self._wrapped_index is not None

        if isinstance(item, ProxyModel):
            item = item._p__obj__  # pyright: ignore [reportAssignmentType]

        return self._wrapped_index.get(id(item), None)

    def clear(self) -> None:
        super().clear()
        self._wrapped_index = None

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
