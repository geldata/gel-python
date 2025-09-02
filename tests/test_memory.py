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

from gel._internal import _testbase as tb


class TestConfigMemory(tb.SyncQueryTestCase):

    async def test_config_memory_01(self):
        if (
            self.client.query_required_single(
                "select exists "
                "(select schema::Type filter .name = 'cfg::memory')"
            ) is False
        ):
            self.skipTest("feature not implemented")

        mem_strs = [
            "0B",
            "0GiB",
            "1024MiB",
            "9223372036854775807B",
            "123KiB",
            "9MiB",
            "102938GiB",
            "108TiB",
            "42PiB",
        ]

        # Test that ConfigMemory.__str__ formats the
        # same as <str><cfg::memory>
        mem_tuples = self.client.query('''
            FOR a IN array_unpack(<array<str>>$0) UNION
            (
                <cfg::memory>a,
                <str><cfg::memory>a,
                <int64><cfg::memory>a,
            );
        ''', mem_strs)

        mem_vals = [t[0] for t in mem_tuples]

        # Test encode/decode roundtrip
        roundtrip = self.client.query('''
            SELECT array_unpack(<array<cfg::memory>>$0);
        ''', mem_vals)

        self.assertEqual(
            [str(t[0]) for t in mem_tuples],
            [t[1] for t in mem_tuples]
        )
        self.assertEqual(
            [t[0].as_bytes() for t in mem_tuples],
            [t[2] for t in mem_tuples]
        )
        self.assertEqual(list(roundtrip), mem_vals)
