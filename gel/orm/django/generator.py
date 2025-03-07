import pathlib
import re
import warnings

from ..introspection import get_mod_and_name, GelORMWarning, FilePrinter


GEL_SCALAR_MAP = {
    'std::uuid': 'UUIDField',
    'std::bigint': 'DecimalField',
    'std::bool': 'BooleanField',
    'std::bytes': 'BinaryField',
    'std::decimal': 'DecimalField',
    'std::float32': 'FloatField',
    'std::float64': 'FloatField',
    'std::int16': 'SmallIntegerField',
    'std::int32': 'IntegerField',
    'std::int64': 'BigIntegerField',
    'std::json': 'JSONField',
    'std::str': 'TextField',
    # Extreme caution is needed for datetime field, the TZ aware and naive
    # values are controlled in Django via settings (USE_TZ) and are mutually
    # exclusive in the same app under default circumstances.
    'std::datetime': 'DateTimeField',
    'std::cal::local_date': 'DateField',
    'std::cal::local_datetime': 'DateTimeField',
    'std::cal::local_time': 'TimeField',
    # all kinds of durations are not supported due to this error:
    # iso_8601 intervalstyle currently not supported
}

BASE_STUB = f'''\
#
# Automatically generated from Gel schema.
#
# This is based on the auto-generated Django model module, which has been
# updated to fit Gel schema more closely.
#

from django.db import models
from django.contrib.postgres import fields as pgf


class GelUUIDField(models.UUIDField):
    # This field must be treated as a auto-generated UUID.
    db_returning = True


class LTForeignKey(models.ForeignKey):
    # Linked tables need to return their source/target ForeignKeys.
    db_returning = True\
'''

GEL_META = f'''
class GelPGMeta:
    'This is a model reflected from Gel using Postgres protocol.'
'''

CLOSEPAR_RE = re.compile(r'\)(?=\s+#|$)')
ARRAY_RE = re.compile(r'^array<(?P<el>.+)>$')
NAME_RE = re.compile(r'^(?P<alpha>\w+?)(?P<num>\d*)$')


def field_name_sort(item):
    key, val = item

    match = NAME_RE.fullmatch(key)
    res = (match.group('alpha'), int(match.group('num') or -1))

    return res


class ModelClass(object):
    def __init__(self, name):
        self.name = name
        self.props = {}
        self.links = {}
        self.mlinks = {}
        self.meta = {'managed': False}
        self.backlinks = {}
        self.backlink_renames = {}

    @property
    def table(self):
        return self.meta['db_table'].strip("'")

    def get_backlink_name(self, name, srcname):
        return f'_{name}_{srcname}'


class ModelGenerator(FilePrinter):
    def __init__(self, *, out):
        super().__init__()
        # record the output file path
        self.outfile = pathlib.Path(out).resolve()

    def spec_to_modules_dict(self, spec):
        modules = {
            mod: {'link_tables': {}, 'object_types': {}}
            for mod in sorted(spec['modules'])
        }

        for rec in spec['link_tables']:
            mod = rec['module']
            modules[mod]['link_tables'][rec['table']] = rec

        for rec in spec['object_types']:
            mod, name = get_mod_and_name(rec['name'])
            modules[mod]['object_types'][name] = rec

        return modules['default']

    def replace_foreignkey(self, fval, origtarget, newtarget, bkname=None):
        # Replace the reference with the string quoted
        # (because we don't check the order of definition)
        # name.
        fval = fval.replace(origtarget, repr(newtarget))

        if bkname:
            # Add a backlink reference
            fval = CLOSEPAR_RE.sub(f', related_name={bkname!r})', fval)

        return fval

    def build_models(self, maps):
        modmap = {}

        for name, rec in maps['object_types'].items():
            mod = ModelClass(name)
            mod.meta['db_table'] = repr(name)
            if 'backlink_renames' in rec:
                mod.backlink_renames = rec['backlink_renames']

            # copy backlink information
            for link in rec['backlinks']:
                mod.backlinks[link['name']] = link

            # process properties as fields
            for prop in rec['properties']:
                pname = prop['name']
                if prop['cardinality'] == 'Many':
                    continue

                code = self.render_prop(prop)
                if code:
                    mod.props[pname] = code

            # process single links as fields
            for link in rec['links']:
                if link['cardinality'] != 'One':
                    # Multi links require link tables and are handled
                    # separately.
                    continue

                lname = link['name']
                bklink = mod.get_backlink_name(lname, name)
                code = self.render_link(link, bklink)
                if code:
                    mod.links[lname] = code

            modmap[mod.name] = mod

        for table, rec in maps['link_tables'].items():
            source, fwname = table.split('.')
            mod = ModelClass(f'{source}{fwname.title()}')
            mod.meta['db_table'] = repr(table)
            mod.meta['unique_together'] = "(('source', 'target'),)"

            # Only have source and target
            mtgt, target = get_mod_and_name(rec['target'])
            if mtgt != 'default':
                # skip this whole link table
                warnings.warn(
                    f'Skipping link {fwname!r}: link target '
                    f'{rec["target"]!r} is not supported',
                    GelORMWarning,
                )
                continue

            mod.links['source'] = (
                f"LTForeignKey({source!r}, models.DO_NOTHING, "
                f"db_column='source')"
            )
            mod.links['target'] = (
                f"LTForeignKey({target!r}, models.DO_NOTHING, "
                f"db_column='target', primary_key=True)"
            )

            # Update the source model with the corresponding
            # ManyToManyField.
            src = modmap[source]
            tgt = modmap[target]
            bkname = src.get_backlink_name(fwname, source)
            src.mlinks[fwname] = (
                f'models.ManyToManyField('
                f'{tgt.name!r}, '
                f'through={mod.name!r}, '
                f'through_fields=("source", "target"), '
                f'related_name={bkname!r})'
            )

            modmap[mod.name] = mod

        return modmap

    def render_prop(self, prop):
        if prop['required']:
            req = ''
        else:
            req = 'blank=True, null=True'

        target = prop['target']['name']
        is_array = False
        match = ARRAY_RE.fullmatch(target)
        if match:
            is_array = True
            target = match.group('el')

        try:
            ftype = GEL_SCALAR_MAP[target]
        except KeyError:
            warnings.warn(
                f'Scalar type {target} is not supported',
                GelORMWarning,
            )
            return ''

        if is_array:
            return f'pgf.ArrayField(models.{ftype}({req}))'
        else:
            return f'models.{ftype}({req})'

    def render_link(self, link, bklink=None):
        if link['required']:
            req = ''
        else:
            req = ', blank=True, null=True'

        mod, target = get_mod_and_name(link['target']['name'])

        if mod != 'default':
            warnings.warn(
                f'Skipping link {link["name"]!r}: link target '
                f'{link["target"]["name"]!r} is not supported',
                GelORMWarning,
            )
            return ''

        if bklink:
            bklink = f', related_name={bklink!r}'
        else:
            bklink = ''

        return (f'models.ForeignKey('
                f'{target!r}, models.DO_NOTHING{bklink}{req})')

    def render_models(self, spec):
        # Check that there is only "default" module
        mods = spec['modules']
        if mods[0] != 'default' or len(mods) > 1:
            skipped = ', '.join([repr(m) for m in mods if m != 'default'])
            warnings.warn(
                f"Skipping modules {skipped}: Django reflection doesn't "
                f"support multiple modules or non-default modules.",
                GelORMWarning,
            )
        # Check that we don't have multiprops or link properties as they
        # produce models without `id` field and Django doesn't like that. It
        # causes Django to mistakenly use `source` as `id` and also attempt to
        # UPDATE `target` on link tables.
        if len(spec['prop_objects']) > 0:
            warnings.warn(
                f"Skipping multi properties: Django reflection doesn't "
                f"support multi properties as they produce models without "
                f"`id` field.",
                GelORMWarning,
            )
        if len(spec['link_objects']) > 0:
            warnings.warn(
                f"Skipping link properties: Django reflection doesn't support "
                f"link properties as they produce models without `id` field.",
                GelORMWarning,
            )

        maps = self.spec_to_modules_dict(spec)
        modmap = self.build_models(maps)

        with open(self.outfile, 'w+t') as f:
            self.out = f
            self.write(BASE_STUB)

            for mod in sorted(modmap.values(), key=lambda x: x.name):
                self.write()
                self.write()
                self.render_model_class(mod)

    def render_model_class(self, mod):
        self.write(f'class {mod.name}(models.Model):')
        self.indent()

        if '.' not in mod.table:
            # This is only valid for regular objects, not link tables.
            self.write(f"id = GelUUIDField(primary_key=True)")
            self.write(f"gel_type_id = models.UUIDField(db_column='__type__')")

        if mod.props:
            self.write()
            self.write(f'# properties as Fields')
            props = sorted(mod.props.items(), key=field_name_sort)
            for name, val in props:
                self.write(f'{name} = {val}')

        if mod.links:
            self.write()
            self.write(f'# links as ForeignKeys')
            links = sorted(mod.links.items(), key=field_name_sort)
            for name, val in links:
                self.write(f'{name} = {val}')

        if mod.mlinks:
            self.write()
            self.write(f'# multi links as ManyToManyFields')
            mlinks = sorted(mod.mlinks.items(), key=field_name_sort)
            for name, val in mlinks:
                self.write(f'{name} = {val}')

        if '.' not in mod.table:
            self.write(GEL_META)

        self.write('class Meta:')
        self.indent()
        for name, val in mod.meta.items():
            self.write(f'{name} = {val}')
        self.dedent()

        self.dedent()