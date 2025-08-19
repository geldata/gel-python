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
import time

from gel.pgproto.pgproto cimport (
    WriteBuffer,
    ReadBuffer,
)

from .. import con_utils
from .. import errors
from . cimport protocol


DEF RECV_BUF = 65536


cdef class BlockingIOProtocol(protocol.SansIOProtocolBackwardsCompatible):

    def __init__(self, con_params, sock):
        protocol.SansIOProtocolBackwardsCompatible.__init__(self, con_params)
        self.sock = sock
        self.deadline = 0
        # debug socket peer name
        import threading, sys
        ident = f"{threading.get_ident()} {id(self)}"
        print(ident, "Protocol: connected to", sock.getpeername(), file=sys.stderr)

    cpdef abort(self):
        # set send timeout to 0 to prevent blocking
        import struct
        self.sock.settimeout(1)
        tl = struct.pack('ll', 1, 0)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDTIMEO, tl)
        # self.sock.setblocking(False)
        self.terminate()
        self._disconnect()

    cdef _disconnect(self):
        import threading, sys
        ident = f"{threading.get_ident()} {id(self)}"
        print(ident, "Protocol: disconnecting", file=sys.stderr)
        self.connected = False
        sock, self.sock = self.sock, None
        if sock is not None:
            try:
                sock.shutdown(socket.SHUT_RDWR)
                print(ident, "Protocol: shutdown is sent", file=sys.stderr)
            except OSError:
                print(ident, "Protocol: shutdown failed, skipping", file=sys.stderr)
                pass
            sock.close()
            print(ident, "Protocol: socket is closed", file=sys.stderr)

    cdef write(self, WriteBuffer buf):
        import threading, sys
        ident = f"{threading.get_ident()} {id(self)}"
        print(ident, "Protocol: writing", len(bytes(buf)), "bytes", file=sys.stderr)
        print(ident, "Protocol: sendbuf size", self.sock.getsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF), file=sys.stderr)
        print(ident, "Protocol: send timeout", self.sock.getsockopt(socket.SOL_SOCKET, socket.SO_SNDTIMEO), file=sys.stderr)
        try:
            self.sock.send(buf)
            print(ident, "Protocol: send done", file=sys.stderr)
        except OSError as e:
            print(ident, "Protocol: send failed", e, file=sys.stderr)
            self._disconnect()
            raise con_utils.wrap_error(e) from e

    async def wait_for_message(self):
        cdef float timeout
        if self.deadline > 0:
            while not self.buffer.take_message():
                timeout = self.deadline - time.monotonic()
                if timeout <= 0:
                    self.abort()
                    raise TimeoutError
                try:
                    self.sock.settimeout(timeout)
                    data = self.sock.recv(RECV_BUF)
                except OSError as e:
                    self._disconnect()
                    raise con_utils.wrap_error(e) from e
                if not data:
                    self._disconnect()
                    raise errors.ClientConnectionClosedError()
                self.buffer.feed_data(data)
        else:
            while not self.buffer.take_message():
                try:
                    data = self.sock.recv(RECV_BUF)
                except OSError as e:
                    self._disconnect()
                    raise con_utils.wrap_error(e) from e
                if not data:
                    self._disconnect()
                    raise errors.ClientConnectionClosedError()
                self.buffer.feed_data(data)
        self.last_active_timestamp = time.monotonic()

    async def try_recv_eagerly(self):
        if self.buffer.take_message():
            return

        self.sock.settimeout(0)  # Make non-blocking.
        try:
            while not self.buffer.take_message():
                data = self.sock.recv(RECV_BUF)
                if not data:
                    self._disconnect()
                    raise errors.ClientConnectionClosedError()
                self.buffer.feed_data(data)
        except BlockingIOError:
            # No data in the socket net buffer.
            return
        except OSError as e:
            self._disconnect()
            raise con_utils.wrap_error(e) from e
        else:
            self.last_active_timestamp = time.monotonic()
        finally:
            self.sock.settimeout(None)

    async def wait_for_connect(self):
        return True

    async def wait_for_disconnect(self):
        if self.cancelled or not self.connected:
            return
        try:
            while True:
                if not self.buffer.take_message():
                    await self.wait_for_message()
                self.fallthrough()
        except errors.ClientConnectionClosedError:
            pass

    async def wait_for(self, coro, timeout):
        if timeout is None:
            return await coro
        else:
            self.deadline = time.monotonic() + timeout
            try:
                return await coro
            finally:
                self.deadline = 0
                if self.sock is not None:
                    self.sock.settimeout(None)
