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

import datetime
import decimal
import json
import random
import threading
import time
import uuid

import gel

from gel import abstract
from gel import _testbase as tb
from gel import blocking_client
from gel.protocol import protocol


class TestSyncQuery(tb.SyncQueryTestCase):

    SETUP = '''
        CREATE TYPE test::Tmp {
            CREATE REQUIRED PROPERTY tmp -> std::str;
        };

        CREATE TYPE test::TmpConflict {
            CREATE REQUIRED PROPERTY tmp -> std::str {
                CREATE CONSTRAINT exclusive;
            }
        };

        CREATE TYPE test::TmpConflictChild extending test::TmpConflict;

        CREATE SCALAR TYPE MyEnum EXTENDING enum<"A", "B">;

        CREATE SCALAR TYPE test::MyType EXTENDING int32;
        CREATE SCALAR TYPE test::MyType2 EXTENDING int64;
        CREATE SCALAR TYPE test::MyType3 EXTENDING int16;
    '''

    TEARDOWN = '''
        DROP SCALAR TYPE test::MyType3;
        DROP SCALAR TYPE test::MyType2;
        DROP SCALAR TYPE test::MyType;
        DROP TYPE test::Tmp;
        DROP TYPE test::TmpConflictChild;
        DROP TYPE test::TmpConflict;
    '''

    def test_sync_parse_error_recover_01(self):
        for _ in range(2):
            with self.assertRaises(gel.EdgeQLSyntaxError):
                self.client.query('select syntax error')

            with self.assertRaises(gel.EdgeQLSyntaxError):
                self.client.query('select syntax error')

            with self.assertRaises(gel.EdgeQLSyntaxError):
                self.client.query('select (')

            with self.assertRaises(gel.EdgeQLSyntaxError):
                self.client.query_json('select (')

            for _ in range(10):
                self.assertEqual(
                    self.client.query('select 1;'),
                    gel.Set((1,)))

            self.assertFalse(self.client.connection.is_closed())

    def test_sync_parse_error_recover_02(self):
        for _ in range(2):
            with self.assertRaises(gel.EdgeQLSyntaxError):
                self.client.execute('select syntax error')

            with self.assertRaises(gel.EdgeQLSyntaxError):
                self.client.execute('select syntax error')

            for _ in range(10):
                self.client.execute('select 1; select 2;')

    def test_sync_exec_error_recover_01(self):
        for _ in range(2):
            with self.assertRaises(gel.DivisionByZeroError):
                self.client.query('select 1 / 0;')

            with self.assertRaises(gel.DivisionByZeroError):
                self.client.query('select 1 / 0;')

            for _ in range(10):
                self.assertEqual(
                    self.client.query('select 1;'),
                    gel.Set((1,)))

    def test_sync_exec_error_recover_02(self):
        for _ in range(2):
            with self.assertRaises(gel.DivisionByZeroError):
                self.client.execute('select 1 / 0;')

            with self.assertRaises(gel.DivisionByZeroError):
                self.client.execute('select 1 / 0;')

            for _ in range(10):
                self.client.execute('select 1;')

    def test_sync_exec_error_recover_03(self):
        query = 'select 10 // <int64>$0;'
        for i in [1, 2, 0, 3, 1, 0, 1]:
            if i:
                self.assertEqual(
                    self.client.query(query, i),
                    gel.Set([10 // i]))
            else:
                with self.assertRaises(gel.DivisionByZeroError):
                    self.client.query(query, i)

    def test_sync_exec_error_recover_04(self):
        for i in [1, 2, 0, 3, 1, 0, 1]:
            if i:
                self.client.execute(f'select 10 // {i};')
            else:
                with self.assertRaises(gel.DivisionByZeroError):
                    self.client.query(f'select 10 // {i};')

    def test_sync_exec_error_recover_05(self):
        with self.assertRaises(gel.DivisionByZeroError):
            self.client.execute(f'select 1 / 0')
        self.assertEqual(
            self.client.query('SELECT "HELLO"'),
            ["HELLO"])

    def test_sync_query_single_01(self):
        res = self.client.query_single("SELECT 1")
        self.assertEqual(res, 1)
        res = self.client.query_single("SELECT <str>{}")
        self.assertEqual(res, None)
        res = self.client.query_required_single("SELECT 1")
        self.assertEqual(res, 1)

        with self.assertRaises(gel.NoDataError):
            self.client.query_required_single("SELECT <str>{}")

    def test_sync_query_single_command_01(self):
        r = self.client.query('''
            CREATE TYPE test::server_query_single_command_01 {
                CREATE REQUIRED PROPERTY server_query_single_command_01 ->
                    std::str;
            };
        ''')
        self.assertEqual(r, [])

        r = self.client.query('''
            DROP TYPE test::server_query_single_command_01;
        ''')
        self.assertEqual(r, [])

        r = self.client.query('''
            CREATE TYPE test::server_query_single_command_01 {
                CREATE REQUIRED PROPERTY server_query_single_command_01 ->
                    std::str;
            };
        ''')
        self.assertEqual(r, [])

        r = self.client.query('''
            DROP TYPE test::server_query_single_command_01;
        ''')
        self.assertEqual(r, [])

        r = self.client.query_json('''
            CREATE TYPE test::server_query_single_command_01 {
                CREATE REQUIRED PROPERTY server_query_single_command_01 ->
                    std::str;
            };
        ''')
        self.assertEqual(r, '[]')

        r = self.client.query_json('''
            DROP TYPE test::server_query_single_command_01;
        ''')
        self.assertEqual(r, '[]')

        self.assertTrue(
            self.client.connection._get_last_status().startswith('DROP')
        )

    def test_sync_query_no_return(self):
        with self.assertRaisesRegex(
                gel.InterfaceError,
                r'cannot be executed with query_required_single\(\).*'
                r'not return'):
            self.client.query_required_single('create type Foo123')

        with self.assertRaisesRegex(
                gel.InterfaceError,
                r'cannot be executed with query_required_single_json\(\).*'
                r'not return'):
            self.client.query_required_single_json('create type Bar123')

    def test_sync_basic_datatypes_01(self):
        for _ in range(10):
            self.assertEqual(
                self.client.query_single(
                    'select ()'),
                ())

            self.assertEqual(
                self.client.query(
                    'select (1,)'),
                gel.Set([(1,)]))

            self.assertEqual(
                self.client.query_single(
                    'select <array<int64>>[]'),
                [])

            self.assertEqual(
                self.client.query(
                    'select ["a", "b"]'),
                gel.Set([["a", "b"]]))

            self.assertEqual(
                self.client.query('''
                    SELECT {(a := 1 + 1 + 40, world := ("hello", 32)),
                            (a:=1, world := ("yo", 10))};
                '''),
                gel.Set([
                    gel.NamedTuple(a=42, world=("hello", 32)),
                    gel.NamedTuple(a=1, world=("yo", 10)),
                ]))

            with self.assertRaisesRegex(
                    gel.InterfaceError,
                    r'query cannot be executed with query_single\('):
                self.client.query_single('SELECT {1, 2}')

            with self.assertRaisesRegex(gel.NoDataError,
                                        r'\bquery_required_single_json\('):
                self.client.query_required_single_json('SELECT <int64>{}')

    def test_sync_basic_datatypes_02(self):
        self.assertEqual(
            self.client.query(
                r'''select [b"\x00a", b"b", b'', b'\na']'''),
            gel.Set([[b"\x00a", b"b", b'', b'\na']]))

        self.assertEqual(
            self.client.query(
                r'select <bytes>$0', b'he\x00llo'),
            gel.Set([b'he\x00llo']))

    def test_sync_basic_datatypes_03(self):
        for _ in range(10):
            self.assertEqual(
                self.client.query_json(
                    'select ()'),
                '[[]]')

            self.assertEqual(
                self.client.query_json(
                    'select (1,)'),
                '[[1]]')

            self.assertEqual(
                self.client.query_json(
                    'select <array<int64>>[]'),
                '[[]]')

            self.assertEqual(
                json.loads(
                    self.client.query_json(
                        'select ["a", "b"]')),
                [["a", "b"]])

            self.assertEqual(
                json.loads(
                    self.client.query_single_json(
                        'select ["a", "b"]')),
                ["a", "b"])

            self.assertEqual(
                json.loads(
                    self.client.query_json('''
                        SELECT {(a := 1 + 1 + 40, world := ("hello", 32)),
                                (a:=1, world := ("yo", 10))};
                    ''')),
                [
                    {"a": 42, "world": ["hello", 32]},
                    {"a": 1, "world": ["yo", 10]}
                ])

            self.assertEqual(
                json.loads(
                    self.client.query_json('SELECT {1, 2}')),
                [1, 2])

            self.assertEqual(
                json.loads(self.client.query_json('SELECT <int64>{}')),
                [])

            with self.assertRaises(gel.NoDataError):
                self.client.query_required_single_json('SELECT <int64>{}')

            self.assertEqual(
                json.loads(self.client.query_single_json('SELECT <int64>{}')),
                None
            )

    def test_sync_args_01(self):
        self.assertEqual(
            self.client.query(
                'select (<array<str>>$foo)[0] ++ (<array<str>>$bar)[0];',
                foo=['aaa'], bar=['bbb']),
            gel.Set(('aaabbb',)))

    def test_sync_args_02(self):
        self.assertEqual(
            self.client.query(
                'select (<array<str>>$0)[0] ++ (<array<str>>$1)[0];',
                ['aaa'], ['bbb']),
            gel.Set(('aaabbb',)))

    def test_sync_args_03(self):
        with self.assertRaisesRegex(gel.QueryError, r'missing \$0'):
            self.client.query('select <int64>$1;')

        with self.assertRaisesRegex(gel.QueryError, r'missing \$1'):
            self.client.query('select <int64>$0 + <int64>$2;')

        with self.assertRaisesRegex(gel.QueryError,
                                    'combine positional and named parameters'):
            self.client.query('select <int64>$0 + <int64>$bar;')

    def test_sync_args_04(self):
        aware_datetime = datetime.datetime.now(datetime.timezone.utc)
        naive_datetime = datetime.datetime.now()

        date = datetime.date.today()
        naive_time = datetime.time(hour=11)
        aware_time = datetime.time(hour=11, tzinfo=datetime.timezone.utc)

        self.assertEqual(
            self.client.query_single(
                'select <datetime>$0;',
                aware_datetime),
            aware_datetime)

        self.assertEqual(
            self.client.query_single(
                'select <cal::local_datetime>$0;',
                naive_datetime),
            naive_datetime)

        self.assertEqual(
            self.client.query_single(
                'select <cal::local_date>$0;',
                date),
            date)

        self.assertEqual(
            self.client.query_single(
                'select <cal::local_time>$0;',
                naive_time),
            naive_time)

        with self.assertRaisesRegex(gel.InvalidArgumentError,
                                    r'a timezone-aware.*expected'):
            self.client.query_single(
                'select <datetime>$0;',
                naive_datetime)

        with self.assertRaisesRegex(gel.InvalidArgumentError,
                                    r'a naive time object.*expected'):
            self.client.query_single(
                'select <cal::local_time>$0;',
                aware_time)

        with self.assertRaisesRegex(gel.InvalidArgumentError,
                                    r'a naive datetime object.*expected'):
            self.client.query_single(
                'select <cal::local_datetime>$0;',
                aware_datetime)

        with self.assertRaisesRegex(gel.InvalidArgumentError,
                                    r'datetime.datetime object was expected'):
            self.client.query_single(
                'select <cal::local_datetime>$0;',
                date)

        with self.assertRaisesRegex(gel.InvalidArgumentError,
                                    r'datetime.datetime object was expected'):
            self.client.query_single(
                'select <datetime>$0;',
                date)

    def test_sync_mismatched_args_01(self):
        with self.assertRaisesRegex(
                gel.QueryArgumentError,
                r"expected {'a'} arguments, "
                "got {'[bc]', '[bc]'}, "
                r"missed {'a'}, extra {'[bc]', '[bc]'}"):

            self.client.query("""SELECT <int64>$a;""", b=1, c=2)

    def test_sync_mismatched_args_02(self):
        with self.assertRaisesRegex(
                gel.QueryArgumentError,
                r"expected {'[ab]', '[ab]'} arguments, "
                r"got {'[acd]', '[acd]', '[acd]'}, "
                r"missed {'b'}, extra {'[cd]', '[cd]'}"):

            self.client.query("""
                SELECT <int64>$a + <int64>$b;
            """, a=1, c=2, d=3)

    def test_sync_mismatched_args_03(self):
        with self.assertRaisesRegex(
                gel.QueryArgumentError,
                "expected {'a'} arguments, got {'b'}, "
                "missed {'a'}, extra {'b'}"):

            self.client.query("""SELECT <int64>$a;""", b=1)

    def test_sync_mismatched_args_04(self):
        with self.assertRaisesRegex(
                gel.QueryArgumentError,
                r"expected {'[ab]', '[ab]'} arguments, "
                r"got {'a'}, "
                r"missed {'b'}"):

            self.client.query("""SELECT <int64>$a + <int64>$b;""", a=1)

    def test_sync_mismatched_args_05(self):
        with self.assertRaisesRegex(
                gel.QueryArgumentError,
                r"expected {'a'} arguments, "
                r"got {'[ab]', '[ab]'}, "
                r"extra {'b'}"):

            self.client.query("""SELECT <int64>$a;""", a=1, b=2)

    def test_sync_mismatched_args_06(self):
        with self.assertRaisesRegex(
                gel.QueryArgumentError,
                r"expected {'a'} arguments, "
                r"got nothing, "
                r"missed {'a'}"):

            self.client.query("""SELECT <int64>$a;""")

    def test_sync_mismatched_args_07(self):
        with self.assertRaisesRegex(
            gel.QueryArgumentError,
            "expected no named arguments",
        ):

            self.client.query("""SELECT 42""", a=1, b=2)

    def test_sync_args_uuid_pack(self):
        obj = self.client.query_single(
            'select schema::Object {id, name} limit 1')

        # Test that the custom UUID that our driver uses can be
        # passed back as a parameter.
        ot = self.client.query_single(
            'select schema::Object {name} filter .id=<uuid>$id',
            id=obj.id)
        self.assertEqual(obj.id, ot.id)
        self.assertEqual(obj.name, ot.name)

        # Test that a string UUID is acceptable.
        ot = self.client.query_single(
            'select schema::Object {name} filter .id=<uuid>$id',
            id=str(obj.id))
        self.assertEqual(obj.id, ot.id)
        self.assertEqual(obj.name, ot.name)

        # Test that a standard uuid.UUID is acceptable.
        ot = self.client.query_single(
            'select schema::Object {name} filter .id=<uuid>$id',
            id=uuid.UUID(bytes=obj.id.bytes))
        self.assertEqual(obj.id, ot.id)
        self.assertEqual(obj.name, ot.name)

        with self.assertRaisesRegex(gel.InvalidArgumentError,
                                    'invalid UUID.*length must be'):
            self.client.query(
                'select schema::Object {name} filter .id=<uuid>$id',
                id='asdasas')

    def test_sync_args_bigint_basic(self):
        testar = [
            0,
            -0,
            +0,
            1,
            -1,
            123,
            -123,
            123789,
            -123789,
            19876,
            -19876,
            19876,
            -19876,
            198761239812739812739801279371289371932,
            -198761182763908473812974620938742386,
            98761239812739812739801279371289371932,
            -98761182763908473812974620938742386,
            8761239812739812739801279371289371932,
            -8761182763908473812974620938742386,
            761239812739812739801279371289371932,
            -761182763908473812974620938742386,
            61239812739812739801279371289371932,
            -61182763908473812974620938742386,
            1239812739812739801279371289371932,
            -1182763908473812974620938742386,
            9812739812739801279371289371932,
            -3908473812974620938742386,
            98127373373209,
            -4620938742386,
            100000000000,
            -100000000000,
            10000000000,
            -10000000000,
            10000000100,
            -10000000010,
            1000000000,
            -1000000000,
            100000000,
            -100000000,
            10000000,
            -10000000,
            1000000,
            -1000000,
            100000,
            -100000,
            10000,
            -10000,
            1000,
            -1000,
            100,
            -100,
            10,
            -10,
        ]

        for _ in range(500):
            num = ''
            for _ in range(random.randint(1, 50)):
                num += random.choice("0123456789")
            testar.append(int(num))

        for _ in range(500):
            num = ''
            for _ in range(random.randint(1, 50)):
                num += random.choice("0000000012")
            testar.append(int(num))

        val = self.client.query_single(
            'select <array<bigint>>$arg',
            arg=testar)

        self.assertEqual(testar, val)

    def test_sync_args_bigint_pack(self):
        val = self.client.query_single(
            'select <bigint>$arg',
            arg=10)
        self.assertEqual(val, 10)

        with self.assertRaisesRegex(gel.InvalidArgumentError,
                                    'expected an int'):
            self.client.query(
                'select <bigint>$arg',
                arg='bad int')

        with self.assertRaisesRegex(gel.InvalidArgumentError,
                                    'expected an int'):
            self.client.query(
                'select <bigint>$arg',
                arg=10.11)

        with self.assertRaisesRegex(gel.InvalidArgumentError,
                                    'expected an int'):
            self.client.query(
                'select <bigint>$arg',
                arg=decimal.Decimal('10.0'))

        with self.assertRaisesRegex(gel.InvalidArgumentError,
                                    'expected an int'):
            self.client.query(
                'select <bigint>$arg',
                arg=decimal.Decimal('10.11'))

        with self.assertRaisesRegex(gel.InvalidArgumentError,
                                    'expected an int'):
            self.client.query(
                'select <bigint>$arg',
                arg='10')

        with self.assertRaisesRegex(gel.InvalidArgumentError,
                                    'expected an int'):
            self.client.query_single(
                'select <bigint>$arg',
                arg=decimal.Decimal('10'))

        with self.assertRaisesRegex(gel.InvalidArgumentError,
                                    'expected an int'):
            class IntLike:
                def __int__(self):
                    return 10

            self.client.query_single(
                'select <bigint>$arg',
                arg=IntLike())

    def test_sync_args_intlike(self):
        class IntLike:
            def __int__(self):
                return 10

        with self.assertRaisesRegex(gel.InvalidArgumentError,
                                    'expected an int'):
            self.client.query_single(
                'select <int16>$arg',
                arg=IntLike())

        with self.assertRaisesRegex(gel.InvalidArgumentError,
                                    'expected an int'):
            self.client.query_single(
                'select <int32>$arg',
                arg=IntLike())

        with self.assertRaisesRegex(gel.InvalidArgumentError,
                                    'expected an int'):
            self.client.query_single(
                'select <int64>$arg',
                arg=IntLike())

    def test_sync_args_decimal(self):
        class IntLike:
            def __int__(self):
                return 10

        val = self.client.query_single(
            'select <decimal>$0', decimal.Decimal("10.0")
        )
        self.assertEqual(val, 10)

        with self.assertRaisesRegex(gel.InvalidArgumentError,
                                    'expected a Decimal or an int'):
            self.client.query_single(
                'select <decimal>$arg',
                arg=IntLike())

        with self.assertRaisesRegex(gel.InvalidArgumentError,
                                    'expected a Decimal or an int'):
            self.client.query_single(
                'select <decimal>$arg',
                arg="10.2")

    def test_sync_wait_cancel_01(self):
        underscored_lock = self.client.query_single("""
            SELECT EXISTS(
                SELECT schema::Function FILTER .name = 'sys::_advisory_lock'
            )
        """)
        if not underscored_lock:
            self.skipTest("No sys::_advisory_lock function")

        # Test that client protocol handles waits interrupted
        # by closing.
        lock_key = tb.gen_lock_key()

        client = self.client.with_retry_options(
            gel.RetryOptions(attempts=1)
        )
        client2 = self.make_test_client(
            database=self.client.dbname
        ).with_retry_options(
            gel.RetryOptions(attempts=1)
        ).ensure_connected()

        for tx in client.transaction():
            with tx:
                self.assertTrue(tx.query_single(
                    'select sys::_advisory_lock(<int64>$0)',
                    lock_key))

                evt = threading.Event()

                def exec_to_fail():
                    with self.assertRaises((
                        gel.ClientConnectionClosedError,
                        gel.ClientConnectionFailedError,
                    )):
                        for tx2 in client2.transaction():
                            with tx2:
                                # start the lazy transaction
                                tx2.query('SELECT 42;')
                                evt.set()

                                tx2.query(
                                    'select sys::_advisory_lock(<int64>$0)',
                                    lock_key,
                                )

                t = threading.Thread(target=exec_to_fail)
                t.start()

                try:
                    evt.wait(1)
                    time.sleep(0.1)

                    with self.assertRaises(gel.InterfaceError):
                        # close() will ask the server nicely to
                        # disconnect, but since the server is blocked on
                        # the lock, close() will timeout and get
                        # cancelled, which, in turn, will terminate the
                        # connection rudely, and exec_to_fail() will get
                        # ConnectionResetError.
                        client2.close(timeout=0.5)
                finally:
                    t.join()
                    self.assertEqual(
                        tx.query(
                            'select sys::_advisory_unlock(<int64>$0)',
                            lock_key),
                        [True])

    def test_empty_set_unpack(self):
        self.client.query_single('''
          select schema::Function {
            name,
            params: {
              kind,
            } limit 0,
            multi setarr := <array<int32>>{}
          }
          filter .name = 'std::str_repeat'
          limit 1
        ''')

    def test_enum_argument_01(self):
        A = self.client.query_single('SELECT <MyEnum><str>$0', 'A')
        self.assertEqual(str(A), 'A')

        with self.assertRaisesRegex(
            gel.InvalidValueError, 'invalid input value for enum'
        ):
            for tx in self.client.transaction():
                with tx:
                    tx.query_single('SELECT <MyEnum><str>$0', 'Oups')

        self.assertEqual(
            self.client.query_single('SELECT <MyEnum>$0', 'A'),
            A)

        self.assertEqual(
            self.client.query_single('SELECT <MyEnum>$0', A),
            A)

        with self.assertRaisesRegex(
            gel.InvalidValueError, 'invalid input value for enum'
        ):
            for tx in self.client.transaction():
                with tx:
                    tx.query_single('SELECT <MyEnum>$0', 'Oups')

        with self.assertRaisesRegex(
            gel.InvalidArgumentError, 'a str or gel.EnumValue'
        ):
            self.client.query_single('SELECT <MyEnum>$0', 123)

    def test_json(self):
        self.assertEqual(
            self.client.query_json('SELECT {"aaa", "bbb"}'),
            '["aaa", "bbb"]')

    def test_json_elements(self):
        self.client.ensure_connected()
        result = blocking_client.iter_coroutine(
            self.client.connection.raw_query(
                abstract.QueryContext(
                    query=abstract.QueryWithArgs(
                        'SELECT {"aaa", "bbb"}', None, (), {}
                    ),
                    cache=self.client._get_query_cache(),
                    query_options=abstract.QueryOptions(
                        output_format=protocol.OutputFormat.JSON_ELEMENTS,
                        expect_one=False,
                        required_one=False,
                    ),
                    retry_options=None,
                    state=None,
                    transaction_options=None,
                    warning_handler=lambda _ex, _: None,
                    annotations={},
                )
            )
        )
        self.assertEqual(
            result,
            gel.Set(['"aaa"', '"bbb"']))

    def _test_sync_cancel_01(self):
        # TODO(fantix): enable when command_timeout is implemented
        has_sleep = self.client.query_single("""
            SELECT EXISTS(
                SELECT schema::Function FILTER .name = 'sys::_sleep'
            )
        """)
        if not has_sleep:
            self.skipTest("No sys::_sleep function")

        client = self.make_test_client(database=self.client.dbname)

        try:
            self.assertEqual(client.query_single('SELECT 1'), 1)

            protocol_before = client._impl._holders[0]._con._protocol

            with self.assertRaises(gel.InterfaceError):
                client.with_timeout_options(command_timeout=0.1).query_single(
                    'SELECT sys::_sleep(10)'
                )

            client.query('SELECT 2')

            protocol_after = client._impl._holders[0]._con._protocol
            self.assertIsNot(
                protocol_before, protocol_after, "Reconnect expected"
            )
        finally:
            client.close()

    def test_sync_log_message(self):
        msgs = []

        def on_log(con, msg):
            msgs.append(msg)

        self.client.ensure_connected()
        con = self.client.connection
        con.add_log_listener(on_log)
        try:
            self.client.query(
                'configure system set __internal_restart := true;'
            )
            # self.con.query('SELECT 1')
        finally:
            con.remove_log_listener(on_log)

        for msg in msgs:
            if (msg.get_severity_name() == 'NOTICE' and
                    'server restart is required' in str(msg)):
                break
        else:
            raise AssertionError('a notice message was not delivered')

    def test_sync_banned_transaction(self):
        with self.assertRaisesRegex(
            gel.CapabilityError,
            r'cannot execute transaction control commands',
        ):
            self.client.query('start transaction')

        with self.assertRaisesRegex(
            gel.CapabilityError,
            r'cannot execute transaction control commands',
        ):
            self.client.execute('start transaction')

    def test_transaction_state(self):
        with self.assertRaisesRegex(gel.QueryError, "cannot assign to.*id"):
            for tx in self.client.transaction():
                with tx:
                    tx.execute('''
                        INSERT test::Tmp { id := <uuid>$0, tmp := '' }
                    ''', uuid.uuid4())

        client = self.client.with_config(allow_user_specified_id=True)
        for tx in client.transaction():
            with tx:
                tx.execute('''
                    INSERT test::Tmp { id := <uuid>$0, tmp := '' }
                ''', uuid.uuid4())

    def test_sync_default_isolation_01(self):
        if (self.server_version.major, self.server_version.minor) < (6, 0):
            self.skipTest("RepeatableRead not supported yet")

        # Test the bug fixed in geldata/gel#8623
        with self.client.with_config(
            default_transaction_isolation=gel.IsolationLevel.RepeatableRead
        ) as db2:
            db2.query('''
                select 1; select 2;
            ''')

    def test_sync_default_isolation_02(self):
        if (self.server_version.major, self.server_version.minor) < (6, 0):
            self.skipTest("RepeatableRead not supported yet")

        with self.client.with_transaction_options(
            gel.TransactionOptions(
                isolation=gel.IsolationLevel.RepeatableRead
            )
        ) as db2:
            res = db2.query_single('''
                select sys::get_transaction_isolation()
            ''')
            self.assertEqual(str(res), 'RepeatableRead')

        with self.client.with_transaction_options(
            gel.TransactionOptions(
                isolation=gel.IsolationLevel.PreferRepeatableRead
            )
        ) as db2:
            res = db2.query_single('''
                select sys::get_transaction_isolation()
            ''')
            self.assertEqual(str(res), 'RepeatableRead')

        # transaction_options takes precedence over config
        with self.client.with_config(
            default_transaction_isolation=gel.IsolationLevel.RepeatableRead
        ) as db1, db1.with_transaction_options(
            gel.TransactionOptions(
                isolation=gel.IsolationLevel.Serializable,
            )
        ) as db2:
            res = db2.query_single('''
                select sys::get_transaction_isolation()
            ''')
            self.assertEqual(str(res), 'Serializable')

        with self.client.with_transaction_options(
            gel.TransactionOptions(readonly=True)
        ) as db2:
            with self.assertRaises(gel.errors.TransactionError):
                res = db2.execute('''
                    insert test::Tmp { tmp := "test" }
                ''')

    def test_sync_prefer_rr_01(self):
        if (
            str(self.server_version.stage) != 'dev'
            and (self.server_version.major, self.server_version.minor) < (6, 5)
        ):
            self.skipTest("DML in RepeatableRead not supported yet")

        with self.client.with_config(
            default_transaction_isolation=gel.IsolationLevel.PreferRepeatableRead
        ) as db2:
            # This query can run in RepeatableRead mode
            res = db2.query_single('''
                select {
                    ins := (insert test::Tmp { tmp := "test" }),
                    level := sys::get_transaction_isolation(),
                }
            ''')
            self.assertEqual(res.level, 'RepeatableRead')

            # This one can't
            res = db2.query_single('''
                select {
                    ins := (insert test::TmpConflict { tmp := "test" }),
                    level := sys::get_transaction_isolation(),
                }
            ''')
            self.assertEqual(str(res.level), 'Serializable')

        with self.client.with_transaction_options(
            gel.TransactionOptions(
                isolation=gel.IsolationLevel.PreferRepeatableRead
            )
        ) as db2:
            res = db2.query_single('''
                select {
                    level := sys::get_transaction_isolation(),
                }
            ''')
            self.assertEqual(str(res.level), 'RepeatableRead')

    def test_retry_mismatch_input_typedesc(self):
        # Cache the input type descriptor first
        val = self.client.query_single("SELECT <test::MyType>$0", 42)
        self.assertEqual(val, 42)

        # Modify the schema to outdate the previous type descriptor
        self.client.execute("""
            DROP SCALAR TYPE test::MyType;
            CREATE SCALAR TYPE test::MyType EXTENDING std::int64;
        """)

        # Run again with the outdated type descriptor. The server shall send a
        # ParameterTypeMismatchError following a CommandDataDescription message
        # with an updated type descriptor. Because we can re-encode `42` as
        # `int64`, the client should just retry and work fine.
        val = self.client.query_single("SELECT <test::MyType>$0", 42)
        self.assertEqual(val, 42)

        # Modify the schema again to an incompatible type
        self.client.execute("""
            DROP SCALAR TYPE test::MyType;
            CREATE SCALAR TYPE test::MyType EXTENDING std::str;
        """)

        # The cached codec doesn't know about the change, so the client cannot
        # encode this string yet:
        with self.assertRaisesRegex(
            gel.InvalidArgumentError, 'expected an int'
        ):
            self.client.query_single("SELECT <test::MyType>$0", "foo")

        # Try `42` again. The cached codec encodes properly, but the server
        # sends a ParameterTypeMismatchError due to mismatching input type, and
        # we shall receive the new string codec. Because we cannot re-encode
        # `42` as str, the client shouldn't retry but raise a translated error.
        with self.assertRaisesRegex(
            gel.QueryArgumentError, 'expected str, got int'
        ):
            self.client.query_single("SELECT <test::MyType>$0", 42)

        # At last, verify that a string gets encoded properly
        val = self.client.query_single("SELECT <test::MyType>$0", "foo")
        self.assertEqual(val, 'foo')

    def test_batch_01(self):
        for bx in self.client._batch():
            with bx:
                bx.send_query_single('SELECT 1')
                bx.send_query_single('SELECT 2')
                bx.send_query_single('SELECT 3')

                self.assertEqual(bx.wait(), [1, 2, 3])

                bx.send_query_single('SELECT 4')
                bx.send_query_single('SELECT 5')
                bx.send_query_single('SELECT 6')

                self.assertEqual(bx.wait(), [4, 5, 6])

    def test_batch_02(self):
        for bx in self.client._batch():
            with bx:
                bx.send_query_required_single('''
                    INSERT test::Tmp {
                        tmp := 'Test Batch'
                    };
                ''')
                bx.send_query('''
                    SELECT
                        test::Tmp
                    FILTER
                        .tmp = 'Test Batch';
                ''')
                inserted, selected = bx.wait()

        self.assertEqual([inserted.id], [o.id for o in selected])

    def test_batch_03(self):
        for bx in self.client._batch():
            with bx:
                bx.send_execute('''
                    INSERT test::Tmp {
                        tmp := 'Test Auto Wait'
                    };
                ''')
                # No explicit wait() - should auto-wait on scope exit

        rv = self.client.query('''
            SELECT
                test::Tmp
            FILTER
                .tmp = 'Test Auto Wait';
        ''')
        self.assertEqual(len(rv), 1)

    def test_batch_04(self):
        with self.assertRaises(gel.TransactionError):
            for bx in self.client._batch():
                with bx:
                    bx.send_execute('''
                        INSERT test::Tmp {
                            tmp := 'Test Atomic'
                        };
                    ''')
                    bx.send_query_single('SELECT 1/0')
                    bx.send_execute('''
                        INSERT test::Tmp {
                            tmp := 'Test Atomic'
                        };
                    ''')

                    with self.assertRaises(gel.DivisionByZeroError):
                        bx.wait()

        rv = self.client.query('''
            SELECT
                test::Tmp
            FILTER
                .tmp = 'Test Atomic';
        ''')
        self.assertEqual(len(rv), 0)

    def test_batch_05(self):
        for bx in self.client._batch():
            with bx:
                # Test alternating queries that need Parse
                bx.send_query_single('SELECT 1')
                bx.send_query_single('SELECT <int16>$0', 2)
                bx.send_query_single('SELECT 3')
                bx.send_query_single('SELECT <int32>$0', 4)
                bx.send_query_single('SELECT 5')
                bx.send_query_single('SELECT <int64>$0', 6)
                bx.send_query_single('SELECT 7')
                self.assertEqual(bx.wait(), [1, 2, 3, 4, 5, 6, 7])

    def test_batch_06(self):
        # Cache the input type descriptors first
        val = self.client.query_single("SELECT <test::MyType>$0", 42)
        self.assertEqual(val, 42)
        val = self.client.query_single("SELECT <test::MyType2>$0", 42)
        self.assertEqual(val, 42)
        val = self.client.query_single("SELECT <test::MyType3>$0", 42)
        self.assertEqual(val, 42)

        # Modify the schema to outdate the previous type descriptors
        self.client.execute("""
            DROP SCALAR TYPE test::MyType;
            DROP SCALAR TYPE test::MyType2;
            DROP SCALAR TYPE test::MyType3;
            CREATE SCALAR TYPE test::MyType EXTENDING std::int64;
            CREATE SCALAR TYPE test::MyType2 EXTENDING std::int16;
            CREATE SCALAR TYPE test::MyType3 EXTENDING std::int32;
        """)

        # We should retry only once and succeed here
        c = self.client.with_retry_options(gel.RetryOptions(attempts=2))
        for bx in c._batch():
            with bx:
                bx.send_query_single('SELECT <test::MyType>$0', 42)
                bx.send_query_single('SELECT <test::MyType2>$0', 42)
                bx.send_query_single('SELECT <test::MyType3>$0', 42)
                self.assertEqual(bx.wait(), [42, 42, 42])

    def test_batch_07(self):
        c = self.client.with_config(session_idle_transaction_timeout=0.2)
        for bx in c._batch():
            with bx:
                bx.send_query_single("select 42")
                self.assertEqual(bx.wait(), [42])
                time.sleep(0.6)
                bx.send_query_single("select 42")
                self.assertEqual(bx.wait(), [42])
