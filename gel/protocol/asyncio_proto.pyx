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


import asyncio

from gel import errors
from gel.pgproto.pgproto cimport (
    WriteBuffer,
    ReadBuffer,
)

from . cimport protocol


cdef class AsyncIOProtocol(protocol.SansIOProtocolBackwardsCompatible):

    def __init__(self, con_params, loop):
        protocol.SansIOProtocolBackwardsCompatible.__init__(self, con_params)

        self.loop = loop
        self.transport = None
        self.connected_fut = loop.create_future()
        self.disconnected_fut = None
        self.writable = asyncio.Event()
        self.writable.set()

        self.msg_waiter = None

    cpdef abort(self):
        self.connected = False
        self.terminate()
        if self.transport is not None:
            self.transport.close()
            self.transport = None

    cdef write(self, WriteBuffer buf):
        if self.transport is None:
            raise errors.ClientConnectionFailedTemporarilyError()
        self.transport.write(memoryview(buf))

    async def wait_for_message(self):
        if self.buffer.take_message():
            return

        try:
            self.msg_waiter = self.loop.create_future()
            await self.msg_waiter
            return
        except asyncio.CancelledError:
            # TODO: A proper cancellation requires server/protocol
            # support, which isn't yet available.  Therefore,
            # we're disabling asyncio cancellation completely
            # until we can implement it properly.
            try:
                self.cancelled = True
                self.abort()
            finally:
                raise

    async def try_recv_eagerly(self):
        pass

    async def wait_for_connect(self):
        if self.connected_fut is not None:
            await self.connected_fut

    async def wait_for_disconnect(self):
        if not self.connected:
            return
        else:
            self.disconnected_fut = self.loop.create_future()
            try:
                await self.disconnected_fut
            except ConnectionError:
                pass
            finally:
                self.disconnected_fut = None

    def connection_made(self, transport):
        if self.transport is not None:
            raise RuntimeError('connection_made: invalid connection status')
        self.transport = transport
        self.connected_fut.set_result(True)
        self.connected_fut = None

    def connection_lost(self, exc):
        self.connected = False

        if self.connected_fut is not None and not self.connected_fut.done():
            self.connected_fut.set_exception(ConnectionAbortedError())

        if (
            self.disconnected_fut is not None
            and not self.disconnected_fut.done()
        ):
            self.disconnected_fut.set_exception(ConnectionResetError())

        if self.msg_waiter is not None and not self.msg_waiter.done():
            self.msg_waiter.set_exception(errors.ClientConnectionClosedError())
            self.msg_waiter = None

        if self.transport is not None:
            # With asyncio sslproto on CPython 3.10 or lower, a normal exit
            # (connection closed by peer) cannot set the transport._closed
            # properly, leading to false ResourceWarning. Let's fix that by
            # closing the transport again.
            if not self.transport.is_closing():
                self.transport.close()
            self.transport = None

    def pause_writing(self):
        self.writable.clear()

    def resume_writing(self):
        self.writable.set()

    async def _backpressure(self):
        await self.writable.wait()

    def data_received(self, data):
        self.buffer.feed_data(data)

        if (self.msg_waiter is not None and
                self.buffer.take_message() and
                not self.msg_waiter.done()):
            self.msg_waiter.set_result(True)
            self.msg_waiter = None

    def eof_received(self):
        pass
