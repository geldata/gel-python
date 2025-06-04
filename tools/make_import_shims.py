import os

MODS = sorted(['gel', 'gel._version', 'gel.abstract', 'gel.ai', 'gel.ai.core', 'gel.ai.types', 'gel.asyncio_client', 'gel.base_client', 'gel.blocking_client', 'gel.codegen', 'gel.color', 'gel.con_utils', 'gel.credentials', 'gel.datatypes', 'gel.datatypes.datatypes', 'gel.datatypes.range', 'gel.describe', 'gel.enums', 'gel.errors', 'gel.errors._base', 'gel.errors.tags', 'gel.introspect', 'gel.options', 'gel.pgproto', 'gel.pgproto.pgproto', 'gel.pgproto.types', 'gel.platform', 'gel.protocol', 'gel.protocol.asyncio_proto', 'gel.protocol.blocking_proto', 'gel.protocol.protocol', 'gel.scram', 'gel.scram.saslprep', 'gel.transaction'])
COMPAT = {
    'gel.ai': {
        'create_ai': 'create_rag_client',
        'EdgeDBAI': 'RAGClient',
        'create_async_ai': 'create_async_rag_client',
        'AsyncEdgeDBAI': 'AsyncRAGClient',
        'AIOptions': 'RAGOptions',
    },
}


def main():
    for mod in MODS:
        is_package = any(k.startswith(mod + '.') for k in MODS)

        nmod = 'edgedb' + mod[len('gel'):]
        slash_name = nmod.replace('.', '/')
        if is_package:
            try:
                os.mkdir(slash_name)
            except FileExistsError:
                pass
            fname = slash_name + '/__init__.py'
        else:
            fname = slash_name + '.py'

        # import * skips things not in __all__ or with underscores at
        # the start, so we have to do some nonsense.
        with open(fname, 'w') as f:
            f.write(f'''\
# Auto-generated shim
TYPE_CHECKING = False
if TYPE_CHECKING:
    from {mod} import *  # noqa
''')
            if mod in COMPAT:
                for k, v in COMPAT[mod].items():
                    f.write(f'    {k} = {v}  # noqa\n')
            f.write(f'''\
import {mod} as _mod
import sys as _sys
_cur = _sys.modules['{nmod}']
for _k in vars(_mod):
    if not _k.startswith('__') or _k in ('__all__', '__doc__'):
        setattr(_cur, _k, getattr(_mod, _k))
''')
            if mod in COMPAT:
                for k, v in COMPAT[mod].items():
                    f.write(f"_cur.{k} = _mod.{v}\n")
                f.write(f'''\
if hasattr(_cur, '__all__'):
    _cur.__all__ = _cur.__all__ + [
        {',\n        '.join(repr(k) for k in COMPAT[mod])},
    ]
''')
            f.write(f'''\
del _cur
del _sys
del _mod
del _k
''')

    with open('edgedb/py.typed', 'w') as f:
        pass


if __name__ == '__main__':
    main()
