# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

"""Collection utilities and extensions."""

from __future__ import annotations

from typing import TypeVar, overload
from collections.abc import Iterator, KeysView, ValuesView, ItemsView
from collections.abc import Mapping

from gel._internal._utils import UnspecifiedType, Unspecified


_K = TypeVar("_K")
_V_co = TypeVar("_V_co", covariant=True)
_T = TypeVar("_T")


class ImmutableChainMap(Mapping[_K, _V_co]):
    """Immutable version of ChainMap for read-only access to multiple mappings.

    Like ChainMap, earlier mappings take precedence over later ones.
    Unlike ChainMap, this class is immutable and hashable.
    """

    def __init__(self, *maps: Mapping[_K, _V_co]) -> None:
        self._maps = maps

    def __getitem__(self, key: _K) -> _V_co:
        for m in self._maps:
            if key in m:
                return m[key]
        raise KeyError(key)

    def __iter__(self) -> Iterator[_K]:
        seen = set()
        for m in self._maps:
            for k in m:
                if k not in seen:
                    seen.add(k)
                    yield k

    def __len__(self) -> int:
        return len({k for m in self._maps for k in m})

    def __repr__(self) -> str:
        combined = {k: self[k] for k in self}
        return f"{self.__class__.__name__}({combined})"

    def __contains__(self, key: object) -> bool:
        return any(key in m for m in self._maps)

    def __bool__(self) -> bool:
        return any(self._maps)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Mapping):
            return False
        return dict(self) == dict(other)

    def __hash__(self) -> int:
        return hash(tuple(sorted(self.items())))

    @overload
    def get(self, key: _K, /) -> _V_co | None: ...

    @overload
    def get(self, key: _K, /, default: _T | _V_co) -> _V_co | _T: ...

    def get(
        self,
        key: _K,
        default: _T | _V_co | UnspecifiedType = Unspecified,
    ) -> _V_co | _T | None:
        try:
            return self[key]
        except KeyError:
            if isinstance(default, UnspecifiedType):
                return None
            else:
                return default

    def keys(self) -> KeysView[_K]:
        return {k: self[k] for k in self}.keys()

    def values(self) -> ValuesView[_V_co]:
        return {k: self[k] for k in self}.values()

    def items(self) -> ItemsView[_K, _V_co]:
        return {k: self[k] for k in self}.items()

    def copy(self) -> ImmutableChainMap[_K, _V_co]:
        return self.__class__(*self._maps)

    @property
    def maps(self) -> tuple[Mapping[_K, _V_co], ...]:
        return self._maps

    @property
    def parents(self) -> ImmutableChainMap[_K, _V_co]:
        return self.__class__(*self._maps[1:])
