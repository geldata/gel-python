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

import unittest

import gel

from gel._internal import _testbase as tb


class TestProto(tb.SyncQueryTestCase):

    def test_json(self):
        self.assertEqual(
            self.client.query_json('SELECT {"aaa", "bbb"}'),
            '["aaa", "bbb"]')

    # std::datetime is now in range of Python datetime,
    # so another way to trigger codec failure is needed.
    @unittest.skip("""
        std::datetime is now in range of Python date,
        so another way to trigger codec failure is needed.
    """)
    async def test_proto_codec_error_recovery_01(self):
        for _ in range(5):  # execute a few times for OE
            with self.assertRaisesRegex(
                    gel.ClientError,
                    "unable to decode data to Python objects"):
                # Python dattime.Date object can't represent this date, so
                # we know that the codec will fail.
                # The test will be rewritten once it's possible to override
                # default codecs.
                self.client.query("""
                    SELECT
                        cal::to_local_date('0001-01-01 BC', 'YYYY-MM-DD AD');
                """)

            # The protocol, though, shouldn't be in some inconsistent
            # state; it should allow new queries to execute successfully.
            self.assertEqual(
                self.client.query('SELECT {"aaa", "bbb"}'),
                ['aaa', 'bbb'])

    @unittest.skip("""
        std::date is now in range of Python date,
        so another way to trigger codec failure is needed.
    """)
    async def test_proto_codec_error_recovery_02(self):
        for _ in range(5):  # execute a few times for OE
            with self.assertRaisesRegex(
                    gel.ClientError,
                    "unable to decode data to Python objects"):
                # Python dattime.Date object can't represent this date, so
                # we know that the codec will fail.
                # The test will be rewritten once it's possible to override
                # default codecs.
                self.client.query(r"""
                    SELECT cal::to_local_date(
                        {
                            '2010-01-01 AD',
                            '2010-01-02 AD',
                            '2010-01-03 AD',
                            '0001-01-01 BC',
                            '2010-01-04 AD',
                            '2010-01-05 AD',
                        },
                        'YYYY-MM-DD AD'
                    );
                """)

            # The protocol, though, shouldn't be in some inconsistent
            # state; it should allow new queries to execute successfully.
            self.assertEqual(
                self.client.query('SELECT {"aaa", "bbb"}'),
                ['aaa', 'bbb'])
