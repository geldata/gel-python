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

from gel.orm.introspection import FilePrinter, get_mod_and_name


# FIXME: this should be replaced with a special Annotated value using the
# exact type from the schema. No reason to guess.
GEL_TYPE_MAPPING = {
    str: "std::str",
    float: "std::float64",
    int: "std::int64",
    bool: "std::bool",
    # "uuid.UUID": "std::uuid",
    bytes: "std::bytes",
    # "decimal.Decimal": "std::decimal",
    # "datetime.datetime": "std::datetime",
    # "datetime.timedelta": "std::duration",
    # "datetime.date": "cal::local_date",
    # "datetime.time": "cal::local_time",
    # "gel.RelativeDuration": "cal::relative_duration",
    # "gel.DateDuration": "cal::date_duration",
    # "gel.ConfigMemory": "cfg::memory",
    # "array.array": "ext::pgvector::vector",
}


class Exclusive:
    pass


class BaseGelModel(BaseModel):
    def exclusive_fields(self):
        results = []

        for name, info in self.model_fields.items():
            for meta in info.metadata:
                if meta is Exclusive:
                    results.append(name)

        return results


class ObjData(BaseModel):
    obj: BaseGelModel
    rank: int | None = None
    gelid: uuid.UUID | None = None


def is_optional(field):
    return (
        typing.get_origin(field) is typing.Union and
        type(None) in typing.get_args(field)
    )


class Session:
    def __init__(self, data, client):
        self._data = list(data)
        self._client = client
        # insert order will come in tiers, where each tier is itself a list of
        # objects that have the same insert precedence and can be inserted in
        # parallel.
        self._insert_order = [[]]
        self._idmap = {}

        self.compute_insert_order()

    def commit(self):
        for tx in self._client.transaction():
            with tx:
                for objs in self._insert_order:
                    for item in objs:
                        query, args = self.generate_insert(item)
                        gelobj = tx.query_single(query, *args)
                        self._idmap[id(item)].gelid = gelobj.id
        self.clear()

    def clear(self):
        self._data = []
        self._insert_order = [[]]
        self._idmap = {}

    def generate_insert(self, item, *, arg_start=0):
        args = []
        arg = arg_start
        query = f'insert {item.__gel_name__} {{'

        for name, info in item.model_fields.items():
            val = getattr(item, name)

            if val is None:
                # skip empty values
                continue

            if isinstance(val, BaseModel):
                subquery, subargs = self.generate_select(
                    val, arg_start=arg)
                arg += len(subargs)
                args += subargs
                query += f'{name} := ({subquery}), '

            else:
                geltype = GEL_TYPE_MAPPING[type(val)]
                query += f'{name} := <{geltype}>${arg}, '
                arg += 1
                args.append(val)

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
                geltype = GEL_TYPE_MAPPING[type(val)]
                fquery.append(f'.{name} = <{geltype}>${arg}')
                arg += 1
                args.append(val)

        query += ' and '.join(fquery)

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
        if self._idmap.get(oid) is not None:
            return

        rank = 0

        for name, info in obj.model_fields.items():
            val = getattr(obj, name)
            # We care about actual link value, because it can be None
            if isinstance(val, BaseModel):
                self.rank_object(val)
                linked = self._idmap[id(val)]
                rank = max(rank, linked.rank + 1)

        if rank >= len(self._insert_order):
            # We only need to grow the _insert_order by 1 more rank since we
            # are guaranteed that the rank can only increase by 1 at each
            # step.
            self._insert_order.append([])

        self._insert_order[rank].append(obj)
        self._idmap[oid] = ObjData(obj=obj, rank=rank)

    # def break_cycle(self):
    #     # We have a set of objects that form a link cycle. We need to find one
    #     # of them with optional links to this cycle and set that link to be
    #     # empty instead. The object's link will be updated separately.
    #     link_found = False
    #     for oid in self._cycle:
    #         obj = self._idmap[oid].obj

    #         for name, info in obj.model_fields.items():
    #             val = self.getmodelattr(obj, name)
    #             # We care about actual link value, because it can be None
    #             if (
    #                 isinstance(val, BaseModel) and
    #                 is_optional(info.annotation)
    #             ):
    #                 self._updates[oid].add(name)
    #                 link_found = True
    #                 break

    #     if not link_found:
    #         cycle = ', '.join(self._idmap[oid].obj for oid in self._cycle)
    #         raise Exception('Cycle detected: {cycle}')

    #     self._cycle.clear()

    # def getmodelattr(self, obj, name):
    #     links = self._updates.get(id(obj), set())
    #     if name in links:
    #         # skip this link
    #         return None
    #     else:
    #         return getattr(obj, name)


def commit(client, data, *, identity_merge=False):
    sess = Session(data, client)
    sess.commit()
