#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2016-present MagicStack Inc. and the EdgeDB authors.
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

import itertools
from concurrent.futures import ThreadPoolExecutor

import edgedb

from gel import _testbase as tb
from edgedb import TransactionOptions


class TestSyncTx(tb.SyncQueryTestCase):

    SETUP = '''
        CREATE TYPE test::TransactionTest EXTENDING std::Object {
            CREATE PROPERTY name -> std::str;
        };

        CREATE TYPE test::Tmp {
            CREATE REQUIRED PROPERTY tmp -> std::str;
        };
        CREATE TYPE test::TmpConflict {
            CREATE REQUIRED PROPERTY tmp -> std::str {
                CREATE CONSTRAINT exclusive;
            }
        };

        CREATE TYPE test::TmpConflictChild extending test::TmpConflict;
    '''

    def test_sync_transaction_regular_01(self):
        tr = self.client.transaction()

        with self.assertRaises(ZeroDivisionError):
            for with_tr in tr:
                with with_tr:
                    with_tr.execute('''
                        INSERT test::TransactionTest {
                            name := 'Test Transaction'
                        };
                    ''')

                    1 / 0

        result = self.client.query('''
            SELECT
                test::TransactionTest
            FILTER
                test::TransactionTest.name = 'Test Transaction';
        ''')

        self.assertEqual(result, [])

    async def test_sync_transaction_kinds(self):
        isolations = [
            None,
            edgedb.IsolationLevel.Serializable,
        ]
        if not (
            str(self.server_version.stage) != 'dev'
            and (self.server_version.major, self.server_version.minor) < (6, 5)
        ):
            isolations += [
                edgedb.IsolationLevel.PreferRepeatableRead,
                edgedb.IsolationLevel.RepeatableRead,
            ]

        booleans = [None, True, False]
        all = itertools.product(isolations, booleans, booleans)
        for isolation, readonly, deferrable in all:
            opt = dict(
                isolation=isolation,
                readonly=readonly,
                deferrable=deferrable,
            )
            # skip None
            opt = {k: v for k, v in opt.items() if v is not None}
            client = self.client.with_transaction_options(
                TransactionOptions(**opt)
            )
            try:
                for tx in client.transaction():
                    with tx:
                        tx.execute(
                            'INSERT test::TransactionTest {name := "test"}')
            except edgedb.TransactionError:
                self.assertTrue(readonly)
            else:
                self.assertFalse(readonly)

            for tx in client.transaction():
                with tx:
                    pass

    def _try_bogus_rr_tx(self, con, first_try):
        # A transaction that needs to be serializable
        for tx in con.transaction():
            with tx:
                res1 = tx.query_single('''
                    select {
                        ins := (insert test::Tmp { tmp := "test1" }),
                        level := sys::get_transaction_isolation(),
                    }
                ''')
                # If this is the second time we've tried to run this
                # transaction, then the cache should ensure we *only*
                # try Serializable.
                if not first_try:
                    self.assertEqual(res1.level, 'Serializable')

                res2 = tx.query_single('''
                    select {
                        ins := (insert test::TmpConflict {
                            tmp := <str>random()
                        }),
                        level := sys::get_transaction_isolation(),
                    }
                ''')

                # N.B: res1 will be RepeatableRead on the first
                # iteration, maybe, but contingent on the second query
                # succeeding it will be Serializable!
                self.assertEqual(res1.level, 'Serializable')
                self.assertEqual(res2.level, 'Serializable')

    def test_sync_transaction_prefer_rr(self):
        if (
            str(self.server_version.stage) != 'dev'
            and (self.server_version.major, self.server_version.minor) < (6, 5)
        ):
            self.skipTest("DML in RepeatableRead not supported yet")
        con = self.client.with_transaction_options(
            edgedb.TransactionOptions(
                isolation=edgedb.IsolationLevel.PreferRepeatableRead
            )
        )
        # A transaction that needs to be serializable
        self._try_bogus_rr_tx(con, first_try=True)
        self._try_bogus_rr_tx(con, first_try=False)

        # And one that doesn't
        for tx in con.transaction():
            with tx:
                res = tx.query_single('''
                    select {
                        ins := (insert test::Tmp { tmp := "test" }),
                        level := sys::get_transaction_isolation(),
                    }
                ''')
            self.assertEqual(str(res.level), 'RepeatableRead')

    def test_sync_transaction_commit_failure(self):
        with self.assertRaises(edgedb.errors.QueryError):
            for tx in self.client.transaction():
                with tx:
                    tx.execute("start migration to {};")
        self.assertEqual(self.client.query_single("select 42"), 42)

    def test_sync_transaction_exclusive(self):
        for tx in self.client.transaction():
            with tx:
                query = "select sys::_sleep(0.5)"
                with ThreadPoolExecutor(max_workers=2) as executor:
                    f1 = executor.submit(tx.execute, query)
                    f2 = executor.submit(tx.execute, query)
                    with self.assertRaisesRegex(
                        edgedb.InterfaceError,
                        "concurrent queries within the same transaction "
                        "are not allowed"
                    ):
                        f1.result(timeout=5)
                        f2.result(timeout=5)
