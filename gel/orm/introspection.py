import json
import re
import collections
import textwrap
import warnings


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
select m.name;
'''

CLEAN_NAME = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')


class GelORMWarning(Warning):
    pass


def get_sql_name(name):
    # Just remove the module name
    name = name.rsplit('::', 1)[-1]

    return name


def get_mod_and_name(name):
    # Assume the names are already validated to be properly formed
    # alphanumeric identifiers that may be prefixed by a module. If the module
    # is present assume it is safe to drop it (currently only defualt module
    # is allowed).

    # Split on module separator. Potentially if we ever handle more unusual
    # names, there may be more processing done.
    return name.rsplit('::', 1)


def valid_name(name):
    # Just remove module separators and check the rest
    name = name.replace('::', '')
    if not CLEAN_NAME.fullmatch(name):
        warnings.warn(
            f'Skipping {name!r}: non-alphanumeric names are not supported',
            GelORMWarning,
        )
        return False
    return True


def get_schema_json(client):
    types = json.loads(client.query_json(INTRO_QUERY))
    modules = json.loads(client.query_json(MODULE_QUERY))

    return _process_links(types, modules)


async def async_get_schema_json(client):
    types = json.loads(await client.query_json(INTRO_QUERY))
    modules = json.loads(client.query_json(MODULE_QUERY))

    return _process_links(types, modules)


def _skip_invalid_names(spec_list, recurse_into=None):
    valid = []
    for spec in spec_list:
        # skip invalid names
        if valid_name(spec['name']):
            if recurse_into is not None:
                for fname in recurse_into:
                    if fname not in spec:
                        continue
                    spec[fname] = _skip_invalid_names(
                        spec[fname], recurse_into)

            valid.append(spec)

    return valid


def _process_links(types, modules):
    # Figure out all the backlinks, link tables, and links with link
    # properties that require their own intermediate objects.
    type_map = {}
    link_tables = []
    link_objects = []
    prop_objects = []

    # All the names of types, props and links are valid beyond this point.
    types = _skip_invalid_names(types, ['properties', 'links'])
    for spec in types:
        type_map[spec['name']] = spec
        spec['backlink_renames'] = {}

    for spec in types:
        mod = spec["name"].rsplit('::', 1)[0]
        sql_source = get_sql_name(spec["name"])

        for prop in spec['properties']:
            name = prop['name']
            exclusive = prop['exclusive']
            cardinality = prop['cardinality']
            sql_name = get_sql_name(name)

            if cardinality == 'Many':
                # Multi property will make its own "link table". But since it
                # doesn't link to any other object the link table itself must
                # be reflected as an object.
                pobj = {
                    'module': mod,
                    'name': f'{sql_source}_{sql_name}_prop',
                    'table': f'{sql_source}.{sql_name}',
                    'links': [{
                        'name': 'source',
                        'required': True,
                        'cardinality': 'One' if exclusive else 'Many',
                        'exclusive': cardinality == 'One',
                        'target': {'name': spec['name']},
                        'has_link_object': False,
                    }],
                    'properties': [{
                        'name': 'target',
                        'required': True,
                        'cardinality': 'One',
                        'exclusive': False,
                        'target': prop['target'],
                        'has_link_object': False,
                    }],
                }
                prop_objects.append(pobj)

        for link in spec['links']:
            if link['name'] != '__type__':
                name = link['name']
                target = link['target']['name']
                cardinality = link['cardinality']
                exclusive = link['exclusive']
                sql_name = get_sql_name(name)

                objtype = type_map[target]
                objtype['backlinks'].append({
                    # naming scheme mimics .<link[is Type]
                    'name': f'_{sql_name}_{sql_source}',
                    'fwname': sql_name,
                    # flip cardinality and exclusivity
                    'cardinality': 'One' if exclusive else 'Many',
                    'exclusive': cardinality == 'One',
                    'target': {'name': spec['name']},
                    'has_link_object': False,
                })

                link['has_link_object'] = False
                # Any link with properties should become its own intermediate
                # object, since ORMs generally don't have a special convenient
                # way of exposing this as just a link table.
                if len(link['properties']) > 2:
                    # more than just 'source' and 'target' properties
                    lobj = {
                        'module': mod,
                        'name': f'{sql_source}_{sql_name}_link',
                        'table': f'{sql_source}.{sql_name}',
                        'links': [],
                        'properties': [],
                    }
                    for prop in link['properties']:
                        if prop['name'] in {'source', 'target'}:
                            lobj['links'].append(prop)
                        else:
                            lobj['properties'].append(prop)

                    link_objects.append(lobj)
                    link['has_link_object'] = True
                    objtype['backlinks'][-1]['has_link_object'] = True

                elif cardinality == 'Many':
                    # Add a link table for One-to-Many and Many-to-Many
                    link_tables.append({
                        'module': mod,
                        'name': f'{sql_source}_{sql_name}_table',
                        'table': f'{sql_source}.{sql_name}',
                        'source': spec["name"],
                        'target': target,
                    })

    return {
        'modules': modules,
        'object_types': types,
        'link_tables': link_tables,
        'link_objects': link_objects,
        'prop_objects': prop_objects,
    }


class FilePrinter(object):
    INDENT = ' ' * 4

    def __init__(self):
        # set the output to be stdout by default, but this is generally
        # expected to be overridden
        self.out = None
        self._indent_level = 0

    def indent(self):
        self._indent_level += 1

    def dedent(self):
        if self._indent_level > 0:
            self._indent_level -= 1

    def reset_indent(self):
        self._indent_level = 0

    def write(self, text=''):
        print(
            textwrap.indent(text, prefix=self.INDENT * self._indent_level),
            file=self.out,
        )
