from edgeql_qb.render.types import RenderedQuery


def render_delete(model_name: str) -> RenderedQuery:
    return RenderedQuery(f'delete {model_name}')
