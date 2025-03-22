from functools import reduce

from edgeql_qb.func import Function
from edgeql_qb.render.tools import (
    combine_many_renderers,
    join_renderers,
    render_parentheses,
)
from edgeql_qb.render.types import RenderedQuery


def render_function_args(args: list[RenderedQuery]) -> RenderedQuery:
    return render_parentheses(reduce(join_renderers(', '), args) if args else RenderedQuery())


def render_function(func: Function, arg_renderers: list[RenderedQuery]) -> RenderedQuery:
    return combine_many_renderers(
        RenderedQuery(f'{func.module}::' if func.module != 'std' else ''),
        RenderedQuery(func.name),
        render_function_args(arg_renderers),
    )
