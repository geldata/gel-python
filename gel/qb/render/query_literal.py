from datetime import date, datetime, time, timedelta
from decimal import Decimal
from functools import singledispatch
from typing import Any

from edgeql_qb.frozendict import FrozenDict
from edgeql_qb.render.types import RenderedQuery
from edgeql_qb.types import GenericHolder


@singledispatch
def render_query_literal(value: Any, name: str) -> RenderedQuery:
    if isinstance(value, str | bool | bytes):
        # singledispatch not working for unions
        return RenderedQuery(
            f'<{value.__class__.__name__}>${name}',
            FrozenDict({name: value}),
        )
    return RenderedQuery(f'${name}', FrozenDict({name: value}))  # pragma: no cover


@render_query_literal.register
def _(value: GenericHolder, name: str) -> RenderedQuery:  # type: ignore[type-arg]
    return RenderedQuery(f'<{value.edgeql_name}>${name}', FrozenDict({name: value.value}))


@render_query_literal.register
def _(value: datetime, name: str) -> RenderedQuery:
    if value.tzinfo is None:
        return RenderedQuery(f'<cal::local_datetime>${name}', FrozenDict({name: value}))
    return RenderedQuery(f'<{value.__class__.__name__}>${name}', FrozenDict({name: value}))


@render_query_literal.register
def _(value: date, name: str) -> RenderedQuery:
    return RenderedQuery(f'<cal::local_date>${name}', FrozenDict({name: value}))


@render_query_literal.register
def _(value: time, name: str) -> RenderedQuery:
    return RenderedQuery(f'<cal::local_time>${name}', FrozenDict({name: value}))


@render_query_literal.register
def _(value: timedelta, name: str) -> RenderedQuery:
    return RenderedQuery(f'<duration>${name}', FrozenDict({name: value}))


@render_query_literal.register
def _(value: Decimal, name: str) -> RenderedQuery:
    return RenderedQuery(f'<decimal>${name}', FrozenDict({name: value}))
