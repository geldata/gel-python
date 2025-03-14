import pathlib
import re
import warnings

from contextlib import contextmanager

from .introspection import get_sql_name, get_mod_and_name
from .introspection import GelORMWarning, FilePrinter


GEL_SCALAR_MAP = {
    'std::bool': ('bool', None),
    'std::str': ('str', None),
    'std::int16': ('int', None),
    'std::int32': ('int', None),
    'std::int64': ('int', None),
    'std::float32': ('float', None),
    'std::float64': ('float', None),
    'std::uuid': ('uuid.UUID', None),
    'std::bytes': ('bytes', None),
    'std::cal::local_date': ('datetime.date', None),
    'std::cal::local_time': ('datetime.time', None),
    'std::cal::local_datetime': ('datetime.datetime', 'sa.DateTime()'),
    'std::datetime': ('datetime.datetime', 'sa.TIMESTAMP(timezone=True)'),
}

ARRAY_RE = re.compile(r'^array<(?P<el>.+)>$')
CLEAN_RE = re.compile(r'[^A-Za-z0-9]+')
NAME_RE = re.compile(r'^(?P<alpha>\w+?)(?P<num>\d*)$')

COMMENT = '''\
#
# Automatically generated from Gel schema.
#
# Do not edit directly as re-generating this file will overwrite any changes.
#\
'''

MODELS_STUB = f'''\
{COMMENT}

import datetime
import uuid

import sqlmodel as sm
import sqlalchemy as sa
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
                self.write(f'{COMMENT}\n')
                self.write(f'from typing import Optional')
                self.write(f'from {self.basemodule}._tables import *')
                yield f
            finally:
                self.out = None

    def get_fk(self, mod, table, curmod):
        if mod == curmod:
            # No need for anything fancy within the same schema
            return f'foreign_key="{table}.id"'
        else:
            return f'foreign_key="{mod}.{table}.id"'

    def get_sqla_fk(self, mod, table, curmod):
        if mod == curmod:
            # No need for anything fancy within the same schema
            return f'sa.ForeignKey("{table}.id")'
        else:
            return f'sa.ForeignKey("{mod}.{table}.id")'

    def get_py_name(self, mod, name, curmod):
        if mod == curmod:
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
                f"Skipping multi properties: SQLModel reflection doesn't "
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

        with open(self.outdir / '_tables.py', 'wt') as f:
            self.out = f
            self.write(MODELS_STUB)

            for rec in spec['link_tables']:
                self.render_link_table(rec)

        for mod, maps in modules.items():
            if not maps:
                # skip apparently empty modules
                continue

            with self.init_module(mod, modules):
                link_objects = sorted(
                    maps.get('link_objects', {}).values(),
                    key=lambda x: x['name']
                )
                for lobj in link_objects:
                    self.write()
                    self.render_link_object(lobj, modules)

                objects = sorted(
                    maps.get('object_types', {}).values(),
                    key=lambda x: x['name']
                )
                for rec in maps.get('object_types', {}).values():
                    self.write()
                    self.render_base_type(rec, modules)
                    self.write()
                    self.render_type(rec, modules)

    def render_link_table(self, spec):
        mod, source = get_mod_and_name(spec["source"])
        tmod, target = get_mod_and_name(spec["target"])
        s_fk = self.get_fk(mod, source, 'default')
        t_fk = self.get_fk(tmod, target, 'default')

        self.write()
        self.write(f'class {spec["name"]}(sm.SQLModel, table=True):')
        self.indent()
        self.write(f'__tablename__ = {spec["table"]!r}')
        if mod != 'default':
            self.write(f'__table_args__ = {{"schema": {mod!r}}}')
        # We rely on Gel for maintaining integrity and various on delete
        # triggers, so the rows may be deleted in a different way from what
        # SQLAlchemy expects.
        self.write('__mapper_args__ = {"confirm_deleted_rows": False}')
        self.write()
        # source is in the same module as this table
        self.write(f'source: uuid.UUID = sm.Field(')
        self.indent()
        self.write(f'{s_fk}, primary_key=True,')
        self.dedent()
        self.write(f')')

        self.write(f'target: uuid.UUID = sm.Field(')
        self.indent()
        self.write(f'{t_fk}, primary_key=True,')
        self.dedent()
        self.write(f')')
        self.dedent()

    def render_link_object(self, spec, modules):
        mod = spec['module']
        name = spec['name']
        sql_name = spec['table']
        source_name, source_link = sql_name.split('.')

        self.write()
        self.write(f'class {name}(sm.SQLModel, table=True):')
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
                sqlafk = self.get_sqla_fk(tmod, target, mod)
                pyname = self.get_py_name(tmod, target, mod)
                self.write(
                    f'{lname}_id: uuid.UUID = sm.Field(sa_column=sa.Column(')
                self.indent()
                self.write(f'{lname!r},')
                self.write(f'{sqlafk},')
                self.write(f'primary_key=True,')
                self.write(f'nullable=False,')
                self.dedent()
                self.write(f'))')

                if lname == 'source':
                    bklink = source_link
                else:
                    src = modules[mod]['object_types'][source_name]
                    bklink = f'_{source_link}_{source_name}'

                self.write(
                    f'{lname}: {pyname} = sm.Relationship(')
                self.indent()
                self.write(f'back_populates={bklink!r},')
                self.dedent()
                self.write(f')')

        if spec['properties']:
            self.write()
            self.write('# Properties:')

            for prop in spec['properties']:
                self.render_prop(prop, mod, name, {})

        self.dedent()

    def render_base_type(self, spec, modules):
        # assume nice names for now
        mod, name = get_mod_and_name(spec['name'])
        sql_name = get_sql_name(spec['name'])

        self.write()
        self.write(f'class {name}Base(sm.SQLModel):')
        self.indent()

        if spec['properties']:
            self.write('# Properties:')

            properties = sorted(spec['properties'], key=field_name_sort)
            for prop in properties:
                self.render_prop(prop, mod, name, modules)

        single_links = [
            link for link in spec['links']
            if not link.get('has_link_object')
               and link['cardinality'] == 'One'
        ]
        if single_links:
            self.write()
            self.write('# Links:')

            links = sorted(spec['links'], key=field_name_sort)
            for link in links:
                self.render_link_col(link, mod, name, modules)

        if not (spec['properties'] or single_links):
            # Empty stub for a base type
            self.write('pass')

        self.dedent()

    def render_type(self, spec, modules):
        # assume nice names for now
        mod, name = get_mod_and_name(spec['name'])
        sql_name = get_sql_name(spec['name'])

        self.write()
        self.write(f'class {name}({name}Base, table=True):')
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
        self.write(f'id: uuid.UUID | None = sm.Field(')
        self.indent()
        self.write(f"default=None,")
        self.write(f"primary_key=True,")
        self.write(
            f"sa_column_kwargs=dict(server_default='uuid_generate_v4()'),")
        self.dedent()
        self.write(')')

        # This is maintained entirely by Gel, the server_default simply
        # indicates to SQLAlchemy that this value may be omitted.
        self.write(f'gel_type_id: uuid.UUID | None = sm.Field(')
        self.indent()
        self.write(f"default=None,")
        self.write(
            f"sa_column=sa.Column('__type__', server_default='PLACEHOLDER'),")
        self.dedent()
        self.write(')')

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
            pytype, sa_col = GEL_SCALAR_MAP[target]

        except KeyError:
            warnings.warn(
                f'Scalar type {target} is not supported',
                GelORMWarning,
            )
            # Skip rendering this one
            return

        if is_array:
            pytype = f'list[{pytype}]'

        if cardinality == 'Many':
            # skip it
            return

        else:
            # plain property
            if nullable and not is_array:
                opt = ' | None'
            else:
                opt = ''

            if sa_col:
                if is_array:
                    sa_col = f'sa.ARRAY({sa_col})'

                self.write(
                    f'{name}: {pytype}{opt} = sm.Field(sa_column=sa.Column(')
                self.indent()
                self.write(f'{sa_col},')
                self.write(f'nullable={nullable},')
                self.dedent()
                self.write(f'))')
            elif is_array:
                # Needs an sa_column, but no explicit type
                self.write(
                    f'{name}: {pytype}{opt} = sm.Field(sa_column=sa.Column(')
                self.indent()
                self.write(f'nullable={nullable},')
                self.dedent()
                self.write(f'))')
            else:
                self.write(
                    f'{name}: {pytype}{opt} = sm.Field(nullable={nullable})'
                )

    def render_link_col(self, spec, mod, parent, modules):
        name = spec['name']
        nullable = not spec['required']
        tmod, target = get_mod_and_name(spec['target']['name'])
        source = modules[mod]['object_types'][parent]
        cardinality = spec['cardinality']

        # If there's a link object there's no special column that needs to be
        # created here.
        if not spec.get('has_link_object'):
            fk = self.get_fk(tmod, target, mod)
            pyname = self.get_py_name(tmod, target, mod)

            # cardinality 'Many' means there's an intermediate table referring
            # to this object.
            if cardinality == 'One':
                opt = ' | None' if nullable else ''
                self.write(f'{name}_id: uuid.UUID{opt} = sm.Field(')
                self.indent()
                self.write(f'{fk},')
                self.write(f'nullable={nullable},')
                self.dedent()
                self.write(')')

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
                tmap = f'Optional[{pyname}]'
            elif cardinality == 'Many':
                tmap = f'list[{pyname}]'
            # We want the cascade to delete orphans here as the intermediate
            # objects represent links and must not exist without source.
            self.write(f'{name}: {tmap} = sm.Relationship(')
            self.indent()
            self.write(f"back_populates='source',")
            self.write(f"cascade_delete=True,")
            self.dedent()
            self.write(')')

        else:
            fk = self.get_fk(tmod, target, mod)
            pyname = self.get_py_name(tmod, target, mod)

            if cardinality == 'One':
                if nullable:
                    pyname = f'Optional[{pyname}]'
                self.write(f'{name}: {pyname} = sm.Relationship(')
                self.indent()
                self.write(f'back_populates={bklink!r},')
                self.dedent()
                self.write(')')

            elif cardinality == 'Many':
                secondary = f'{parent}_{name}_table'
                self.write(f'{name}: list[{pyname}] = sm.Relationship(')
                self.indent()
                self.write(f'back_populates={bklink!r},')
                self.write(f'link_model={secondary},')
                self.dedent()
                self.write(')')

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
                tmap = f'Optional[{pyname}]'
            elif cardinality == 'Many':
                tmap = f'list[{pyname}]'
            # We want the cascade to delete orphans here as the intermediate
            # objects represent links and must not exist without target.
            self.write(f'{name}: {tmap} = sm.Relationship(')
            self.indent()
            self.write(f"back_populates='target',")
            self.write(f"cascade_delete=True,")
            self.dedent()
            self.write(')')

        else:
            pyname = self.get_py_name(tmod, target, mod)
            if exclusive:
                # This is a backlink from a single link. There is no link table
                # involved.
                if cardinality == 'One':
                    self.write(f'{name}: Optional[{pyname}] = sm.Relationship(')
                    self.indent()
                    self.write(f"back_populates={bklink!r},")
                    self.dedent()
                    self.write(')')
                elif cardinality == 'Many':
                    self.write(f'{name}: list[{pyname}] = sm.Relationship(')
                    self.indent()
                    self.write(f"back_populates={bklink!r},")
                    self.dedent()
                    self.write(')')

            else:
                # This backlink involves a link table, so we still treat it as
                # a Many-to-Many.
                secondary = f'{target}_{bklink}_table'
                self.write(
                    f'{name}: list[{pyname}] = sm.Relationship('
                )
                self.indent()
                self.write(f'back_populates={bklink!r},')
                self.write(f'link_model={secondary},')
                self.dedent()
                self.write(')')
