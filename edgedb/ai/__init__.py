# Auto-generated shim
TYPE_CHECKING = False
if TYPE_CHECKING:
    from gel.ai import *  # noqa
    create_ai = create_rag_client  # noqa
    EdgeDBAI = RAGClient  # noqa
    create_async_ai = create_async_rag_client  # noqa
    AsyncEdgeDBAI = AsyncRAGClient  # noqa
    AIOptions = RAGOptions  # noqa
import gel.ai as _mod
import sys as _sys
_cur = _sys.modules['edgedb.ai']
for _k in vars(_mod):
    if not _k.startswith('__') or _k in ('__all__', '__doc__'):
        setattr(_cur, _k, getattr(_mod, _k))
_cur.create_ai = _mod.create_rag_client
_cur.EdgeDBAI = _mod.RAGClient
_cur.create_async_ai = _mod.create_async_rag_client
_cur.AsyncEdgeDBAI = _mod.AsyncRAGClient
_cur.AIOptions = _mod.RAGOptions
if hasattr(_cur, '__all__'):
    _cur.__all__ = _cur.__all__ + [
        'create_ai',
        'EdgeDBAI',
        'create_async_ai',
        'AsyncEdgeDBAI',
        'AIOptions',
    ]
del _cur
del _sys
del _mod
del _k
