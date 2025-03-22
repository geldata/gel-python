from collections.abc import Callable, Iterator
from functools import reduce

from edgeql_qb.expression import Expression
from edgeql_qb.render.expression import render_expression
from edgeql_qb.render.tools import combine_many_renderers, join_renderers
from edgeql_qb.render.types import RenderedQuery


def render_with_expressions(
        expressions: list[RenderedQuery],
) -> Callable[[RenderedQuery], RenderedQuery]:
    def inner(rendered_with: RenderedQuery) -> RenderedQuery:
        return (
            combine_many_renderers(
                rendered_with,
                reduce(join_renderers(', '), expressions),
            )
            if expressions
            else rendered_with
        )
    return inner


def render_with_module(module: str | None = None) -> Callable[[RenderedQuery], RenderedQuery]:
    def inner(rendered_with: RenderedQuery) -> RenderedQuery:
        return (
            combine_many_renderers(
                rendered_with,
                RenderedQuery(f'module {module}'),
            )
            if module
            else rendered_with
        )
    return inner


def render_with_separator(
    expressions: list[RenderedQuery],
    module: str | None = None,
) -> Callable[[RenderedQuery], RenderedQuery]:
    """Put comma between module and expressions if both are presented."""
    def inner(rendered_with: RenderedQuery) -> RenderedQuery:
        comma = RenderedQuery(', ')
        return (
            combine_many_renderers(rendered_with, comma)
            if (module and expressions)
            else rendered_with
        )
    return inner


def render_with_ending(
    expressions: list[RenderedQuery],
    module: str | None = None,
) -> Callable[[RenderedQuery], RenderedQuery]:
    """Put space on the end of expressions if any module or expressions presented."""
    def inner(rendered_with: RenderedQuery) -> RenderedQuery:
        space = RenderedQuery(' ')
        empty = RenderedQuery()
        return (
            combine_many_renderers(rendered_with, space)
            if module or expressions else space
            if module and expressions else empty
        )
    return inner


def render_with_expression(
        with_aliases: tuple[Expression, ...],
        generator: Iterator[int],
        module: str | None = None,
) -> RenderedQuery:
    renderers = [
        render_expression(alias.to_infix_notation(), 'with', generator)
        for alias in with_aliases
    ]
    return (
        RenderedQuery('with ')
        .map(render_with_module(module))
        .map(render_with_separator(renderers, module))
        .map(render_with_expressions(renderers))
        .map(render_with_ending(renderers, module))
    )
