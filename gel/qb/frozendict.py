from __future__ import annotations

from collections.abc import Iterator, Mapping
from functools import reduce
from typing import Any


class FrozenDict(Mapping[str, Any]):
    """Python hashable mapping type.

    Python 3.10 has no hashable mapping type, so I steel this class from here:
    https://stackoverflow.com/a/2704866.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self._d = dict(*args, **kwargs)

    def __iter__(self) -> Iterator[str]:
        return iter(self._d)

    def __len__(self) -> int:
        return len(self._d)

    def __getitem__(self, key: str) -> Any:
        return self._d[key]

    def __or__(self, other: FrozenDict) -> FrozenDict:
        new = dict(self)
        new |= other
        return FrozenDict(new)

    def __hash__(self) -> int:
        return reduce(lambda acc, pair: acc ^ hash(pair), self.items(), 0)

    def __repr__(self) -> str:
        return repr(self._d)
