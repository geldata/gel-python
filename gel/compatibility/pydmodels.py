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

import typing
import uuid

from collections import defaultdict
from contextlib import contextmanager
from pydantic import BaseModel

import gel

from gel.compatibility.introspection import FilePrinter, get_mod_and_name


class Exclusive:
    pass


class GelType:
    def __init__(self, name):
        self.name = name


class Link:
    pass


class Multi:
    pass


class BaseGelModel(BaseModel):
    def exclusive_fields(self):
        results = []

        for name, info in self.model_fields.items():
            if Exclusive in info.metadata:
                results.append(name)

        return results

    def prop_fields(self):
        results = []

        for name, info in self.model_fields.items():
            if Link not in info.metadata:
                results.append(name)

        return results

    def link_fields(self):
        results = []

        for name, info in self.model_fields.items():
            if Link in info.metadata:
                results.append(name)

        return results

    def eq_props(self, other):
        if other.__class__ is not self.__class__:
            return False

        for name in self.prop_fields():
            if getattr(self, name) != getattr(other, name):
                return False

        return True

    def get_field_gel_type(self, name):
        info = self.model_fields[name]
        for anno in info.metadata:
            if isinstance(anno, GelType):
                return anno.name

        return None


class UpdateGelModel(BaseGelModel):
    pass


class ObjData(BaseModel):
    obj: BaseGelModel
    rank: int | None = None
    gelid: uuid.UUID | None = None
    exval: tuple | None = None


def is_optional(field):
    return (
        typing.get_origin(field) is typing.Union and
        type(None) in typing.get_args(field)
    )


class Session:
    def __init__(self, data, client, *, identity_merge=False):
        self._data = list(data)
        self._client = client
        self._identity_merge = identity_merge
        # insert order will come in tiers, where each tier is itself a list of
        # objects that have the same insert precedence and can be inserted in
        # parallel.
        self._insert_order = [[]]
        self._idmap = {}
        # map based on exclusive properties, once an object is inserted, all
        # other copies will be updated with the gelid
        self._exmap = defaultdict(list)

        self.process_exclusive()
        self.compute_insert_order()

    def commit(self):
        for tx in self._client.transaction():
            with tx:
                for objs in self._insert_order:
                    for item in objs:
                        objdata = self._idmap[id(item)]
                        if objdata.gelid is not None:
                            query, args = self.generate_update_new(item)
                        elif isinstance(item, UpdateGelModel):
                            query, args = self.generate_update(item)
                        else:
                            query, args = self.generate_insert(item)

                        gelobj = tx.query_single(query, *args)
                        objdata.gelid = gelobj.id
                        if self._identity_merge:
                            # Update all identical copies of this object with
                            # the same gelid
                            exlist = self._exmap[objdata.exval]
                            for val in exlist:
                                self._idmap[id(val)].gelid = gelobj.id

        self.clear()

    def clear(self):
        self._data = []
        self._insert_order = [[]]
        self._idmap = {}
        self._exmap = {}

    def generate_insert(self, item, *, arg_start=0):
        args = []
        arg = arg_start
        query = f'insert {item.__gel_name__} {{'

        for name, info in item.model_fields.items():
            val = getattr(item, name)

            if val is None:
                # skip empty values
                continue

            if Link in info.metadata:
                subqueries = []
                if Multi in info.metadata:
                    links = val
                else:
                    links = [val]

                # multi link potentially needs several subqueries
                for el in links:
                    subquery, subargs = self.generate_select(
                        el, arg_start=arg)
                    arg += len(subargs)
                    args += subargs
                    subqueries.append(f'({subquery})')

                query += f'{name} := '
                if len(subqueries) > 1:
                    subq = ", ".join(subqueries)
                    query += f'assert_distinct({{ {subq} }}), '
                else:
                    query += f'{subqueries[0]}, '

            else:
                query += f'{name} := '
                geltype = item.get_field_gel_type(name)
                if Multi in info.metadata:
                    query += f'array_unpack(<array<{geltype}>>${arg}), '
                else:
                    query += f'<{geltype}>${arg}, '

                arg += 1
                args.append(val)

        query += '}'

        return query, args

    def generate_update_new(self, item, *, arg_start=0):
        gelid = self._idmap[id(item)].gelid
        args = []
        arg = arg_start
        query = f'update detached {item.__gel_name__} '
        query += f'filter .id = <uuid>${arg} set {{'
        arg += 1
        args.append(gelid)

        for name, info in item.model_fields.items():
            val = getattr(item, name)

            if val is None:
                # skip empty values
                continue

            # only update links
            if Link in info.metadata:
                subqueries = []
                if Multi in info.metadata:
                    links = val
                else:
                    links = [val]

                # multi link potentially needs several subqueries
                for el in links:
                    subquery, subargs = self.generate_select(
                        el, arg_start=arg)
                    arg += len(subargs)
                    args += subargs
                    subqueries.append(f'({subquery})')

                query += f'{name} := '
                if len(subqueries) > 1:
                    subq = ", ".join(subqueries)
                    query += f'assert_distinct({{ {subq} }}), '
                else:
                    query += f'{subqueries[0]}, '

        query += '}'

        return query, args

    def generate_select(self, item, *, arg_start=0):
        gelid = self._idmap[id(item)].gelid
        args = []
        arg = arg_start
        query = f'select detached {item.__gel_name__} filter '
        fquery = []

        if gelid is not None:
            fquery.append(f'.id = <uuid>${arg}')
            arg += 1
            args.append(gelid)
        else:
            for name in item.exclusive_fields():
                val = getattr(item, name)
                geltype = item.get_field_gel_type(name)
                fquery.append(f'.{name} = <{geltype}>${arg}')
                arg += 1
                args.append(val)

        query += ' and '.join(fquery)

        return query, args

    def generate_update(self, item, *, arg_start=0):
        # This is an update query for a pre-existing object, so all fields are
        # optional except for id.
        gelid = item.id
        args = []
        arg = arg_start
        query = f'update detached {item.__gel_name__} '
        query += f'filter .id = <uuid>${arg} set {{'
        arg += 1
        args.append(gelid)

        for name, info in item.model_fields.items():
            if name == 'id':
                continue

            val = getattr(item, name)

            # FIXME: instead we need to track the modified fields
            if val is None:
                # skip empty values
                continue

            if Link in info.metadata:
                subqueries = []
                if Multi in info.metadata:
                    links = val
                else:
                    links = [val]

                # multi link potentially needs several subqueries
                for el in links:
                    subquery, subargs = self.generate_select(
                        el, arg_start=arg)
                    arg += len(subargs)
                    args += subargs
                    subqueries.append(f'({subquery})')

                query += f'{name} := '
                if len(subqueries) > 1:
                    subq = ", ".join(subqueries)
                    query += f'assert_distinct({{ {subq} }}), '
                else:
                    query += f'{subqueries[0]}, '

            else:
                query += f'{name} := '
                geltype = item.get_field_gel_type(name)
                if Multi in info.metadata:
                    query += f'array_unpack(<array<{geltype}>>${arg}), '
                else:
                    query += f'<{geltype}>${arg}, '

                arg += 1
                args.append(val)

        query += '}'

        return query, args

    def compute_insert_order(self):
        # We traverse all the distinct objects to be inserted and follow up on
        # their links, recursively constructing a forest. All leaves get rank
        # 0, i.e. they can be inserted first without dependencies on other
        # objects. Any objects that have non-empty links have a rank that's
        # the maximum rank of all their links + 1.
        for obj in self._data:
            self.rank_object(obj)

    def rank_object(self, obj):
        oid = id(obj)
        # Check if this object has been ranked already.
        objdata = self._idmap[oid]
        if objdata.rank is not None:
            return

        rank = 0

        for name, info in obj.model_fields.items():
            val = getattr(obj, name)
            # We care about actual link value, because it can be None
            if val is not None and Link in info.metadata:
                if Multi in info.metadata:
                    links = val
                else:
                    links = [val]

                for el in links:
                    self.rank_object(el)
                    linked = self._idmap[id(el)]
                    rank = max(rank, linked.rank + 1)

        if rank >= len(self._insert_order):
            # We only need to grow the _insert_order by 1 more rank since we
            # are guaranteed that the rank can only increase by 1 at each
            # step.
            self._insert_order.append([])

        self._insert_order[rank].append(obj)
        objdata.rank = rank

    def _get_ex_values(self, obj):
        # Make a tuple out of the Gel type name and all of the exclusive
        # values.
        vals = [obj.__gel_name__]
        for name in sorted(obj.exclusive_fields()):
            val = getattr(obj, name)
            vals.append(val)

        if len(vals) > 1:
            return tuple(vals)
        else:
            return None

    def process_exclusive(self):
        errors = defaultdict(list)

        for obj in self._data:
            self.map_exclusive(obj, errors)

        if len(errors) > 0:
            num_err = 0
            msg = 'The following objects have clashing exclusive fields:\n'
            for key, val in errors.items():
                first = self._exmap[key][:1]
                msg += f'{key[0]}: '
                if num_err + len(val) < 100:
                    # include all objects
                    msg += ', '.join(
                        str(obj) for obj in first + val)
                    msg += '\n'
                else:
                    # clip objects in error message
                    msg += ', '.join(
                        str(obj) for obj in (first + val)[:100 - num_err])
                    break

            raise Exception(msg)

    def map_exclusive(self, obj, errors):
        oid = id(obj)
        if self._idmap.get(oid) is not None:
            return

        exval = self._get_ex_values(obj)
        self._idmap[oid] = ObjData(obj=obj, exval=exval)

        if exval is not None:
            # has exclusive fields
            exlist = self._exmap[exval]
            if exlist:
                other = exlist[0]
            else:
                other = None

            if other:
                if self._identity_merge:
                    # Objects with the same exclusive fields and the same values
                    # for other properties are asssumed to be the same object.
                    if other.eq_props(obj):
                        self._exmap[exval].append(obj)
                    else:
                        errors[exval].append(obj)

                else:
                    # Objects with the same exclusive fields cannot exist and
                    # should be flagged as an error. But we want to collect them
                    # all first.
                    errors[exval].append(obj)
            else:
                # No pre-existing copy
                self._exmap[exval].append(obj)

        # recurse into links
        for name, info in obj.model_fields.items():
            val = getattr(obj, name)
            # We care about actual link value, because it can be None
            if val is not None and Link in info.metadata:
                if Multi in info.metadata:
                    links = val
                else:
                    links = [val]

                for el in links:
                    self.map_exclusive(el, errors)


def commit(client, data, *, identity_merge=False):
    sess = Session(data, client, identity_merge=identity_merge)
    sess.commit()
