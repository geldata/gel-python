from collections.abc import Callable
from dataclasses import dataclass

from edgeql_qb.frozendict import FrozenDict


@dataclass(slots=True, frozen=True)
class RenderedQuery:
    query: str = ''
    context: FrozenDict = FrozenDict()  # noqa: RUF009

    def map(self, f: Callable[['RenderedQuery'], 'RenderedQuery']) -> 'RenderedQuery':
        return f(self)
