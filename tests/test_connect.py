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


import socket

import gel

from gel import _testbase as tb


class TestConnect(tb.AsyncQueryTestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.port = cls._get_free_port()

    @classmethod
    def _get_free_port(cls):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.bind(('127.0.0.1', 0))
            return sock.getsockname()[1]
        except Exception:
            return None
        finally:
            sock.close()

    async def test_connect_async_01(self):
        orig_conn_args = self.get_connect_args()
        conn_args = orig_conn_args.copy()
        conn_args['port'] = self.port
        conn_args['wait_until_available'] = 0

        with self.assertRaisesRegex(
                gel.ClientConnectionError,
                f'(?s).*Is the server running.*port {self.port}.*'):
            conn_args['host'] = '127.0.0.1'
            await gel.create_async_client(**conn_args).ensure_connected()

        with self.assertRaisesRegex(
                gel.ClientConnectionError,
                f'(?s).*Is the server running.*port {self.port}.*'):
            conn_args['host'] = orig_conn_args['host']
            await gel.create_async_client(**conn_args).ensure_connected()

    def test_connect_sync_01(self):
        orig_conn_args = self.get_connect_args()
        conn_args = orig_conn_args.copy()
        conn_args['port'] = self.port
        conn_args['wait_until_available'] = 0

        with self.assertRaisesRegex(
                gel.ClientConnectionError,
                f'(?s).*Is the server running.*port {self.port}.*'):
            conn_args['host'] = '127.0.0.1'
            gel.create_client(**conn_args).ensure_connected()

        with self.assertRaisesRegex(
                gel.ClientConnectionError,
                f'(?s).*Is the server running.*port {self.port}.*'):
            conn_args['host'] = orig_conn_args['host']
            gel.create_client(**conn_args).ensure_connected()
