import pathlib
import re
import warnings

from contextlib import contextmanager

from .introspection import get_sql_name, get_mod_and_name
from .introspection import GelORMWarning, FilePrinter


GEL_SCALAR_MAP = {
    'std::bool': ('bool', 'sa.Boolean'),
    'std::str': ('str', 'sa.String'),
    'std::int16': ('int', 'sa.Integer'),
    'std::int32': ('int', 'sa.Integer'),
    'std::int64': ('int', 'sa.Integer'),
    'std::float32': ('float', 'sa.Float'),
    'std::float64': ('float', 'sa.Float'),
    'std::uuid': ('uuid.UUID', 'sa.Uuid'),
    'std::bytes': ('bytes', 'sa.LargeBinary'),
    'std::cal::local_date': ('datetime.date', 'sa.Date'),
    'std::cal::local_time': ('datetime.time', 'sa.Time'),
    'std::cal::local_datetime': ('datetime.datetime', 'sa.DateTime'),
    'std::datetime': ('datetime.datetime', 'sa.TIMESTAMP'),
}

ARRAY_RE = re.compile(r'^array<(?P<el>.+)>$')
NAME_RE = re.compile(r'^(?P<alpha>\w+?)(?P<num>\d*)$')

COMMENT = '''\
#
# Automatically generated from Gel schema.
#
# Do not edit directly as re-generating this file will overwrite any changes.
#\
'''

BASE_STUB = f'''\
{COMMENT}

from sqlalchemy import orm as orm


class Base(orm.DeclarativeBase):
    pass\
'''

MODELS_STUB = f'''\
{COMMENT}

import datetime
import uuid

from typing import List, Optional

import sqlalchemy as sa
from sqlalchemy import orm as orm
'''


def field_name_sort(spec):
    key = spec['name']

    match = NAME_RE.fullmatch(key)
    res = (match.group('alpha'), int(match.group('num') or -1))

    return res


class ModelGenerator(FilePrinter):
    def __init__(self, *, outdir=None, basemodule=None):
        # set the output to be stdout by default, but this is generally
        # expected to be overridden by appropriate files in the `outdir`
        if outdir is not None:
            self.outdir = pathlib.Path(outdir)
        else:
            self.outdir = None

        self.basemodule = basemodule
        super().__init__()

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

    def init_sqlabase(self):
        with open(self.outdir / '_sqlabase.py', 'wt') as f:
            self.out = f
            self.write(BASE_STUB)

    @contextmanager
    def init_module(self, mod, modules):
        if any(m.startswith(f'{mod}::') for m in modules):
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
        path = self.outdir
        for el in dirpath:
            path = path / el
            self.init_dir(path)

        with open(path / filename, 'wt') as f:
            try:
                self.out = f
                self.write(MODELS_STUB)
                self.write(f'from {self.basemodule}._sqlabase import Base')
                self.write(f'from {self.basemodule}._tables import *')
                yield f
            finally:
                self.out = None

    def get_fk(self, mod, table, curmod):
        if mod == curmod:
            # No need for anything fancy within the same schema
            return f'sa.ForeignKey("{table}.id")'
        else:
            return f'sa.ForeignKey("{mod}.{table}.id")'

    def get_py_name(self, mod, name, curmod):
        if False and mod == curmod:
            # No need for anything fancy within the same module
            return f"'{name}'"
        else:
            mod = mod.replace('::', '.')
            return f"'{self.basemodule}.{mod}.{name}'"

    def spec_to_modules_dict(self, spec):
        modules = {
            mod: {} for mod in sorted(spec['modules'])
        }

        if len(spec['prop_objects']) > 0:
            warnings.warn(
                f"Skipping multi properties: SQLAlchemy reflection doesn't "
                f"support multi properties as they produce models without a "
                f"clear identity.",
                GelORMWarning,
            )

        for rec in spec['link_tables']:
            mod = rec['module']
            if 'link_tables' not in modules[mod]:
                modules[mod]['link_tables'] = []
            modules[mod]['link_tables'].append(rec)

        for lobj in spec['link_objects']:
            mod = lobj['module']
            if 'link_objects' not in modules[mod]:
                modules[mod]['link_objects'] = {}
            modules[mod]['link_objects'][lobj['name']] = lobj

        for rec in spec['object_types']:
            mod, name = get_mod_and_name(rec['name'])
            if 'object_types' not in modules[mod]:
                modules[mod]['object_types'] = {}
            modules[mod]['object_types'][name] = rec

        return modules

    def render_models(self, spec):
        # The modules dict will be populated with the respective types, link
        # tables, etc., since they will need to be put in their own files. We
        # sort the modules so that nested modules are initialized from root to
        # leaf.
        modules = self.spec_to_modules_dict(spec)

        # Initialize the base directory
        self.init_dir(self.outdir)
        self.init_sqlabase()

        with open(self.outdir / '_tables.py', 'wt') as f:
            self.out = f
            self.write(MODELS_STUB)
            self.write(f'from ._sqlabase import Base')

            link_tables = sorted(spec['link_tables'], key=lambda x: x['name'])
            for rec in link_tables:
                self.write()
                self.render_link_table(rec)

        for mod, maps in modules.items():
            if not maps:
                # skip apparently empty modules
                continue

            with self.init_module(mod, modules):
                link_objects = sorted(
                    maps.get('link_objects', {}).values(),
                    key=lambda x: x['name'],
                )
                for lobj in link_objects:
                    self.write()
                    self.render_link_object(lobj, modules)

                object_types = sorted(
                    maps.get('object_types', {}).values(),
                    key=lambda x: x['name'],
                )
                for rec in object_types:
                    self.write()
                    self.render_type(rec, modules)

    def render_link_table(self, spec):
        mod, source = get_mod_and_name(spec["source"])
        tmod, target = get_mod_and_name(spec["target"])
        s_fk = self.get_fk(mod, source, 'default')
        t_fk = self.get_fk(tmod, target, 'default')

        self.write()
        self.write(f'{spec["name"]} = sa.Table(')
        self.indent()
        self.write(f'{spec["table"]!r},')
        self.write(f'Base.metadata,')
        # source is in the same module as this table
        self.write(f'sa.Column("source", {s_fk}),')
        self.write(f'sa.Column("target", {t_fk}),')
        self.write(f'schema={mod!r},')
        self.dedent()
        self.write(f')')

    def render_link_object(self, spec, modules):
        mod = spec['module']
        name = spec['name']
        sql_name = spec['table']
        source_name, source_link = sql_name.split('.')

        self.write()
        self.write(f'class {name}(Base):')
        self.indent()
        self.write(f'__tablename__ = {sql_name!r}')
        if mod != 'default':
            self.write(f'__table_args__ = {{"schema": {mod!r}}}')
        # We rely on Gel for maintaining integrity and various on delete
        # triggers, so the rows may be deleted in a different way from what
        # SQLAlchemy expects.
        self.write('__mapper_args__ = {"confirm_deleted_rows": False}')
        self.write()

        # No ids for these intermediate objects
        if spec['links']:
            self.write()
            self.write('# Links:')

            for link in spec['links']:
                lname = link['name']
                tmod, target = get_mod_and_name(link['target']['name'])
                fk = self.get_fk(tmod, target, mod)
                pyname = self.get_py_name(tmod, target, mod)
                self.write(f'{lname}_id: orm.Mapped[uuid.UUID] = orm.mapped_column(')
                self.indent()
                self.write(f'{lname!r},')
                self.write(f'sa.Uuid(),')
                self.write(f'{fk},')
                self.write(f'primary_key=True,')
                self.write(f'nullable=False,')
                self.dedent()
                self.write(')')

                if lname == 'source':
                    bklink = source_link
                else:
                    src = modules[mod]['object_types'][source_name]
                    bklink = f'_{source_link}_{source_name}'

                self.write(
                    f'{lname}: orm.Mapped[{pyname}] = '
                    f'orm.relationship(back_populates={bklink!r})'
                )

        if spec['properties']:
            self.write()
            self.write('# Properties:')

            for prop in spec['properties']:
                self.render_prop(prop, mod, name, {})

        self.dedent()

    def render_type(self, spec, modules):
        # assume nice names for now
        mod, name = get_mod_and_name(spec['name'])
        sql_name = get_sql_name(spec['name'])

        self.write()
        self.write(f'class {name}(Base):')
        self.indent()
        self.write(f'__tablename__ = {sql_name!r}')
        if mod != 'default':
            self.write(f'__table_args__ = {{"schema": {mod!r}}}')
        # We rely on Gel for maintaining integrity and various on delete
        # triggers, so the rows may be deleted in a different way from what
        # SQLAlchemy expects.
        self.write('__mapper_args__ = {"confirm_deleted_rows": False}')
        self.write()

        # Add two fields that all objects have
        self.write(f'id: orm.Mapped[uuid.UUID] = orm.mapped_column(')
        self.indent()
        self.write(f"sa.Uuid(),")
        self.write(f"primary_key=True,")
        self.write(f"server_default='uuid_generate_v4()',")
        self.dedent()
        self.write(f')')

        # This is maintained entirely by Gel, the server_default simply
        # indicates to SQLAlchemy that this value may be omitted.
        self.write(f'gel_type_id: orm.Mapped[uuid.UUID] = orm.mapped_column(')
        self.indent()
        self.write(f"'__type__',")
        self.write(f"sa.Uuid(),")
        self.write(f"server_default='PLACEHOLDER',")
        self.dedent()
        self.write(f")")

        if spec['properties']:
            self.write()
            self.write('# Properties:')

            properties = sorted(spec['properties'], key=field_name_sort)
            for prop in properties:
                self.render_prop(prop, mod, name, modules)

        if spec['links']:
            self.write()
            self.write('# Links:')

            links = sorted(spec['links'], key=field_name_sort)
            for link in links:
                self.render_link(link, mod, name, modules)

        if spec['backlinks']:
            self.write()
            self.write('# Back-links:')

            backlinks = sorted(spec['backlinks'], key=field_name_sort)
            for link in backlinks:
                self.render_backlink(link, mod, modules)

        self.dedent()

    def render_prop(self, spec, mod, parent, modules, *, is_pk=False):
        name = spec['name']
        nullable = not spec['required']
        cardinality = spec['cardinality']

        target = spec['target']['name']
        is_array = False
        match = ARRAY_RE.fullmatch(target)
        if match:
            is_array = True
            target = match.group('el')

        try:
            pytype, sqlatype = GEL_SCALAR_MAP[target]
            sqlatype = sqlatype + '()'
        except KeyError:
            warnings.warn(
                f'Scalar type {target} is not supported',
                GelORMWarning,
            )
            # Skip rendering this one
            return

        if is_array:
            pytype = f'List[{pytype}]'
            sqlatype = f'sa.ARRAY({sqlatype})'

        if is_pk:
            # special case of a primary key property (should only happen to
            # 'target' in multi property table)
            self.write(f'{name}: orm.Mapped[{pytype}] = orm.mapped_column(')
            self.indent()
            self.write(f'{sqlatype}, primary_key=True, nullable=False,')
            self.dedent()
            self.write(f')')
        elif cardinality == 'Many':
            # skip it
            return

        else:
            # plain property
            self.write(f'{name}: orm.Mapped[{pytype}] = orm.mapped_column(')
            self.indent()
            self.write(f'{sqlatype}, nullable={nullable},')
            self.dedent()
            self.write(f')')

    def render_link(self, spec, mod, parent, modules):
        name = spec['name']
        nullable = not spec['required']
        tmod, target = get_mod_and_name(spec['target']['name'])
        source = modules[mod]['object_types'][parent]
        cardinality = spec['cardinality']
        bklink = f'_{name}_{parent}'

        if spec.get('has_link_object'):
            # intermediate object will have the actual source and target
            # links, so the link here needs to be treated similar to a
            # back-link.
            linkobj = modules[mod]['link_objects'][f'{parent}_{name}_link']
            target = linkobj['name']
            tmod = linkobj['module']
            pyname = self.get_py_name(tmod, target, mod)

            if cardinality == 'One':
                self.write(
                    f'{name}: orm.Mapped[{pyname}] = '
                    f"orm.relationship(back_populates='source')"
                )
            elif cardinality == 'Many':
                self.write(
                    f'{name}: orm.Mapped[List[{pyname}]] = '
                    f"orm.relationship(back_populates='source')"
                )

            if cardinality == 'One':
                tmap = f'orm.Mapped[{pyname}]'
            elif cardinality == 'Many':
                tmap = f'orm.Mapped[List[{pyname}]]'
            # We want the cascade to delete orphans here as the intermediate
            # objects represent links and must not exist without source.
            self.write(f'{name}: {tmap} = orm.relationship(')
            self.indent()
            self.write(f"back_populates='source',")
            self.write(f"cascade='all, delete-orphan',")
            self.dedent()
            self.write(')')

        else:
            fk = self.get_fk(tmod, target, mod)
            pyname = self.get_py_name(tmod, target, mod)

            if cardinality == 'One':
                self.write(
                    f'{name}_id: orm.Mapped[uuid.UUID] = orm.mapped_column(')
                self.indent()
                self.write(f'sa.Uuid(), {fk}, nullable={nullable},')
                self.dedent()
                self.write(f')')

                self.write(f'{name}: orm.Mapped[{pyname}] = orm.relationship(')
                self.indent()
                self.write(f'back_populates={bklink!r},')
                self.dedent()
                self.write(f')')

            elif cardinality == 'Many':
                secondary = f'{parent}_{name}_table'

                self.write(
                    f'{name}: orm.Mapped[List[{pyname}]] = orm.relationship(')
                self.indent()
                self.write(f'{pyname},')
                self.write(f'secondary={secondary},')
                self.write(f'back_populates={bklink!r},')
                self.dedent()
                self.write(f')')

    def render_backlink(self, spec, mod, modules):
        name = spec['name']
        tmod, target = get_mod_and_name(spec['target']['name'])
        cardinality = spec['cardinality']
        exclusive = spec['exclusive']
        bklink = spec['fwname']

        if spec.get('has_link_object'):
            # intermediate object will have the actual source and target
            # links, so the link here needs to refer to the intermediate
            # object and 'target' as back-link.
            linkobj = modules[tmod]['link_objects'][f'{target}_{bklink}_link']
            target = linkobj['name']
            tmod = linkobj['module']
            pyname = self.get_py_name(tmod, target, mod)

            if cardinality == 'One':
                tmap = f'orm.Mapped[{pyname}]'
            elif cardinality == 'Many':
                tmap = f'orm.Mapped[List[{pyname}]]'
            # We want the cascade to delete orphans here as the intermediate
            # objects represent links and must not exist without target.
            self.write(f'{name}: {tmap} = orm.relationship(')
            self.indent()
            self.write(f"back_populates='target',")
            self.write(f"cascade='all, delete-orphan',")
            self.dedent()
            self.write(')')

        else:
            pyname = self.get_py_name(tmod, target, mod)
            if exclusive:
                # This is a backlink from a single link. There is no link table
                # involved.
                if cardinality == 'One':
                    self.write(f'{name}: orm.Mapped[{pyname}] = \\')
                    self.indent()
                    self.write(f'orm.relationship(back_populates={bklink!r})')
                    self.dedent()

                elif cardinality == 'Many':
                    self.write(f'{name}: orm.Mapped[List[{pyname}]] = \\')
                    self.indent()
                    self.write(f'orm.relationship(back_populates={bklink!r})')
                    self.dedent()

            else:
                # This backlink involves a link table, so we still treat it as
                # a Many-to-Many.
                secondary = f'{target}_{bklink}_table'

                self.write(f'{name}: orm.Mapped[List[{pyname}]] = \\')
                self.indent()
                self.write(f'orm.relationship(')
                self.indent()
                self.write(f'{pyname},')
                self.write(f'secondary={secondary},')
                self.write(f'back_populates={bklink!r},')
                self.dedent()
                self.write(')')
                self.dedent()
