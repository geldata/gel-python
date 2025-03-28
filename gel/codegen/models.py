#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2025-present MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import argparse
import io
import os
import pathlib
import sys
import textwrap
import typing

from collections import defaultdict
from contextlib import contextmanager
from pydantic import BaseModel

import gel
from gel import abstract
from gel import describe
from gel.con_utils import find_gel_project_dir
from gel.color import get_color

from gel.compatibility.introspection import FilePrinter, get_mod_and_name
from gel.compatibility.clihelper import print_msg, print_error, _get_conn_args


C = get_color()
SYS_VERSION_INFO = os.getenv("EDGEDB_PYTHON_CODEGEN_PY_VER")
if SYS_VERSION_INFO:
    SYS_VERSION_INFO = tuple(map(int, SYS_VERSION_INFO.split(".")))[:2]
else:
    SYS_VERSION_INFO = sys.version_info[:2]

TYPE_MAPPING = {
    "std::str": "str",
    "std::float32": "float",
    "std::float64": "float",
    "std::int16": "int",
    "std::int32": "int",
    "std::int64": "int",
    "std::bigint": "int",
    "std::bool": "bool",
    "std::uuid": "uuid.UUID",
    "std::bytes": "bytes",
    "std::decimal": "decimal.Decimal",
    "std::datetime": "datetime.datetime",
    "std::duration": "datetime.timedelta",
    "std::json": "str",
    "cal::local_date": "datetime.date",
    "cal::local_time": "datetime.time",
    "cal::local_datetime": "datetime.datetime",
    "cal::relative_duration": "gel.RelativeDuration",
    "cal::date_duration": "gel.DateDuration",
    "cfg::memory": "gel.ConfigMemory",
    "ext::pgvector::vector": "array.array",
}

TYPE_IMPORTS = {
    "std::uuid": "uuid",
    "std::decimal": "decimal",
    "std::datetime": "datetime",
    "std::duration": "datetime",
    "cal::local_date": "datetime",
    "cal::local_time": "datetime",
    "cal::local_datetime": "datetime",
    "ext::pgvector::vector": "array",
}

INPUT_TYPE_MAPPING = TYPE_MAPPING.copy()
INPUT_TYPE_MAPPING.update(
    {
        "ext::pgvector::vector": "typing.Sequence[float]",
    }
)

INPUT_TYPE_IMPORTS = TYPE_IMPORTS.copy()
INPUT_TYPE_IMPORTS.update(
    {
        "ext::pgvector::vector": "typing",
    }
)


INTRO_QUERY = '''
with module schema
select ObjectType {
    name,
    links: {
        name,
        readonly,
        required,
        cardinality,
        exclusive := exists (
            select .constraints
            filter .name = 'std::exclusive'
        ),
        target: {name},
        constraints: {
            name,
            params: {name, @value},
        },

        properties: {
            name,
            readonly,
            required,
            cardinality,
            exclusive := exists (
                select .constraints
                filter .name = 'std::exclusive'
            ),
            target: {name},
            constraints: {
                name,
                params: {name, @value},
            },
        },
    } filter .name != '__type__' and not exists .expr,
    properties: {
        name,
        readonly,
        required,
        cardinality,
        exclusive := exists (
            select .constraints
            filter .name = 'std::exclusive'
        ),
        target: {name},
        constraints: {
            name,
            params: {name, @value},
        },
    } filter .name != 'id' and not exists .expr,
    backlinks := <array<str>>[],
}
filter
    not .builtin
    and
    not .internal
    and
    not .from_alias
    and
    not re_test('^(std|cfg|sys|schema)::', .name)
    and
    not any(re_test('^(cfg|sys|schema)::', .ancestors.name));
'''

MODULE_QUERY = '''
with
    module schema,
    m := (select `Module` filter not .builtin)
select _ := m.name order by _;
'''

COMMENT = '''\
#
# Automatically generated from Gel schema.
#
# Do not edit directly as re-generating this file will overwrite any changes.
#\
'''

class Generator(FilePrinter):
    def __init__(self, args: argparse.Namespace, client=None):
        self._default_module = "default"
        self._targets = args.target
        self._async = False
        if client is not None:
            self._client = client
        else:
            self._client = gel.create_client(**_get_conn_args(args))
        self._describe_results = []

        self._cache = {}
        self._imports = set()
        self._aliases = {}
        self._defs = {}
        self._names = set()

        self._basemodule = args.mod
        self._outdir = pathlib.Path(args.out)
        self._modules = {}
        self._types = {}

        self.init_dir(self._outdir)

        super().__init__()

    def run(self):
        try:
            self._client.ensure_connected()
        except gel.EdgeDBError as e:
            print(f"Failed to connect to EdgeDB instance: {e}")
            sys.exit(61)

        self.get_schema()

        with self._client:
            for mod, maps in self._modules.items():
                if not maps:
                    # skip apparently empty modules
                    continue

                with self.init_module(mod):
                    self.write_types(maps)

        print_msg(f"{C.GREEN}{C.BOLD}Done.{C.ENDC}")

    def get_schema(self):
        for mod in self._client.query(MODULE_QUERY):
            self._modules[mod] = {
                'object_types': {},
                'scalar_types': {},
            }

        for t in self._client.query(INTRO_QUERY):
            mod, name = get_mod_and_name(t.name)
            self._types[t.name] = t
            self._modules[mod]['object_types'][t.name] = t

    def init_dir(self, dirpath):
        if not dirpath:
            # nothing to initialize
            return

        path = pathlib.Path(dirpath).resolve()

        # ensure `path` directory exists
        if not path.exists():
            path.mkdir()
        elif not path.is_dir():
            raise NotADirectoryError(
                f'{path!r} exists, but it is not a directory')

        # ensure `path` directory contains `__init__.py`
        (path / '__init__.py').touch()

    @contextmanager
    def init_module(self, mod):
        if any(m.startswith(f'{mod}::') for m in self._modules):
            # This is a prefix in another module, thus it is part of a nested
            # module structure.
            dirpath = mod.split('::')
            filename = '__init__.py'
        else:
            # This is a leaf module, so we just need to create a corresponding
            # <mod>.py file.
            *dirpath, filename = mod.split('::')
            filename = f'{filename}.py'

        # Along the dirpath we need to ensure that all packages are created
        path = self._outdir
        for el in dirpath:
            path = path / el
            self.init_dir(path)

        with open(path / filename, 'wt') as f:
            try:
                self.out = f
                self.write(f'{COMMENT}\n')
                yield f
            finally:
                self.out = None

    def write_types(self, maps):
        object_types = maps['object_types']
        scalar_types = maps['scalar_types']

        if object_types:
            self.write(f'import pydantic')
            self.write(f'import typing as pt')
            self.write(f'import uuid')
            self.write(f'from gel.compatibility import pydmodels as gm')

        objects = sorted(
            object_types.values(), key=lambda x: x.name
        )
        for obj in objects:
            self.render_type(obj, variant='Base')
            self.render_type(obj, variant='Update')
            self.render_type(obj)

    def render_type(self, objtype, *, variant=None):
        mod, name = get_mod_and_name(objtype.name)
        is_empty = True

        self.write()
        self.write()
        match variant:
            case 'Base':
                self.write(f'class _{variant}{name}(gm.BaseGelModel):')
                self.indent()
                self.write(f'__gel_name__ = {objtype.name!r}')
            case 'Update':
                self.write(f'class _{variant}{name}(gm.UpdateGelModel):')
                self.indent()
                self.write(f'__gel_name__ = {objtype.name!r}')
                self.write(
                    f"id: pt.Annotated[uuid.UUID, gm.GelType('std::uuid'), "
                    f"gm.Exclusive]"
                )
            case _:
                self.write(f'class {name}(_Base{name}):')
                self.indent()

        if variant and len(objtype.properties) > 0:
            is_empty = False
            self.write()
            self.write('# Properties:')
            for prop in objtype.properties:
                self.render_prop(prop, mod, variant=variant)

        if variant != 'Base' and len(objtype.links) > 0:
            if variant or not is_empty:
                self.write()
            is_empty = False
            self.write('# Links:')
            for link in objtype.links:
                self.render_link(link, mod, variant=variant)

        if not variant:
            if not is_empty:
                self.write()
            self.write('# Class variants:')
            self.write(f'base: pt.ClassVar = _Base{name}')
            self.write(f'update: pt.ClassVar = _Update{name}')

        self.dedent()

    def render_prop(self, prop, curmod, *, variant=None):
        pytype = TYPE_MAPPING.get(prop.target.name)
        annotated = [f'gm.GelType({prop.target.name!r})']
        defval = ''
        if not pytype:
            # skip
            return

        if str(prop.cardinality) == 'Many':
            annotated.append('gm.Multi')
            pytype = f'pt.List[{pytype}]'
            defval = ' = []'

        if variant == 'Update' or not prop.required:
            pytype = f'pt.Optional[{pytype}]'
            # A value does not need to be supplied
            defval = ' = None'

        if prop.exclusive:
            annotated.append('gm.Exclusive')

        anno = ', '.join([pytype] + annotated)
        pytype = f'pt.Annotated[{anno}]'

        self.write(
            f'{prop.name}: {pytype}{defval}'
        )

    def render_link(self, link, curmod, *, variant=None):
        mod, name = get_mod_and_name(link.target.name)
        annotated = [f'gm.GelType({link.target.name!r})', 'gm.Link']
        defval = ''
        if curmod == mod:
            pytype = name
        else:
            pytype = link.target.name.replace('::', '.')
        pytype = repr(pytype)

        if str(link.cardinality) == 'Many':
            annotated.append('gm.Multi')
            pytype = f'pt.List[{pytype}]'
            defval = ' = []'

        if variant == 'Update' or not link.required:
            pytype = f'pt.Optional[{pytype}]'
            # A value does not need to be supplied
            defval = ' = None'

        anno = ', '.join([pytype] + annotated)
        pytype = f'pt.Annotated[{anno}]'

        self.write(
            f'{link.name}: {pytype}{defval}'
        )
