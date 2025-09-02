#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2019-present MagicStack Inc. and the EdgeDB authors.
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

import json

from gel._internal import _testbase as tb
from gel import errors


class TestGlobals(tb.AsyncQueryTestCase):

    SETUP = '''
        CREATE GLOBAL glob -> str;
        CREATE REQUIRED GLOBAL req_glob -> str {
            SET default := '!';
        };
        CREATE GLOBAL def_glob -> str {
            SET default := '!';
        };
        CREATE GLOBAL computed := '!';
        CREATE MODULE foo;
        CREATE MODULE foo::bar;
        CREATE GLOBAL foo::bar::baz -> str;

        CREATE ALIAS GlobalTest := {
            glob := global glob,
            req_glob := global req_glob,
            def_glob := global def_glob,
        };
    '''

    async def test_globals_01(self) -> None:
        db = self.client
        if db.is_proto_lt_1_0:
            self.skipTest("Global is added in EdgeDB 2.0")

        gdb = db.with_globals(glob='test')
        x = await gdb.query_single('select global glob')
        self.assertEqual(x, 'test')

        x = await gdb.query_single('select global req_glob')
        self.assertEqual(x, '!')

        x = await gdb.query_single('select global def_glob')
        self.assertEqual(x, '!')

        gdb = db.with_globals(req_glob='test')
        x = await gdb.query_single('select global req_glob')
        self.assertEqual(x, 'test')

        gdb = db.with_globals(def_glob='test')
        x = await gdb.query_single('select global def_glob')
        self.assertEqual(x, 'test')

        # Setting def_glob explicitly to None should override
        gdb = db.with_globals(def_glob=None)
        x = await gdb.query_single('select global def_glob')
        self.assertEqual(x, None)

        # Setting computed global should produce error
        gdb = db.with_globals(computed='test')
        with self.assertRaises(errors.QueryArgumentError):
            await gdb.query_single('select global computed')

        gdb = db.with_globals({'foo::bar::baz': 'asdf'})
        x = await gdb.query_single('select global foo::bar::baz')
        self.assertEqual(x, 'asdf')

    async def test_global_graphql_01(self) -> None:
        if self.server_version.major < 7:
            self.skipTest("GraphQL added in 7.0")

        db = self.client

        query = '''
            query {
                GlobalTest {
                    glob
                    req_glob
                    def_glob
                }
            }
        '''

        gdb = db.with_globals(glob='test')
        x = json.loads(await gdb.query_graphql_json(query))
        self.assertEqual(
            x,
            dict(GlobalTest=[
                dict(
                    glob='test',
                    req_glob='!',
                    def_glob='!',
                )
            ]),
            )

        gdb = db.with_globals(req_glob='test1', def_glob='test2')
        x = json.loads(await gdb.query_graphql_json(query))
        self.assertEqual(
            x,
            dict(GlobalTest=[
                dict(
                    glob=None,
                    req_glob='test1',
                    def_glob='test2',
                )
            ]),
        )

        gdb = db.with_globals(def_glob=None)
        x = json.loads(await gdb.query_graphql_json(query))
        self.assertEqual(
            x,
            dict(GlobalTest=[
                dict(
                    glob=None,
                    req_glob='!',
                    def_glob=None,
                )
            ]),
        )

    async def test_client_state_mismatch(self) -> None:
        db = self.client
        if db.is_proto_lt_1_0:
            self.skipTest("State over protocol is added in EdgeDB 2.0")

        await db.execute('create global mglob -> int32')

        c = self.make_test_client(database=self.get_database_name())
        c = c.with_globals(mglob=42)

        try:
            self.assertEqual(await c.query_single('select global mglob'), 42)

            await db.execute('create global mglob2 -> str')
            self.assertEqual(await c.query_single('select global mglob'), 42)

            await db.execute(
                'alter global mglob set type str reset to default')
            with self.assertRaises(errors.InvalidArgumentError):
                await c.query_single('select global mglob')
        finally:
            await c.aclose()
