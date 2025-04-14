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

cimport cython

cimport cpython
cimport cpython.datetime

import asyncio
import collections
import datetime
import json
import time
import types
import typing
import weakref

from gel.pgproto.pgproto cimport (
    WriteBuffer,
    ReadBuffer,

    FRBuffer,
    frb_init,
    frb_read,
    frb_read_all,
    frb_slice_from,
    frb_check,
    frb_set_len,
    frb_get_len,
)

from gel.pgproto import pgproto
from gel.pgproto cimport pgproto
from gel.pgproto cimport hton
from gel.pgproto.pgproto import UUID


from libc.stdint cimport int8_t, uint8_t, int16_t, uint16_t, \
                         int32_t, uint32_t, int64_t, uint64_t, \
                         UINT32_MAX

from gel.datatypes cimport datatypes
from . cimport cpythonx

from gel import enums
from gel import errors
from gel import scram


include "./consts.pxi"
include "./lru.pyx"
include "./codecs/codecs.pyx"


cpython.datetime.import_datetime()


_QUERY_SINGLE_METHOD = {
    True: {
        OutputFormat.JSON: 'query_required_single_json',
        OutputFormat.JSON_ELEMENTS: 'raw_query',
        OutputFormat.BINARY: 'query_required_single',
    },
    False: {
        OutputFormat.JSON: 'query_single_json',
        OutputFormat.JSON_ELEMENTS: 'raw_query',
        OutputFormat.BINARY: 'query_single',
    },
}

ALL_CAPABILITIES = 0xFFFFFFFFFFFFFFFF

cdef dict OLD_ERROR_CODES = {
    0x05_03_00_01: 0x05_03_01_01,  # TransactionSerializationError #2431
    0x05_03_00_02: 0x05_03_01_02,  # TransactionDeadlockError      #2431
}


cdef class ExecuteContext:
    def __init__(
        self,
        *,
        query: str,
        args,
        kwargs,
        reg: CodecsRegistry,
        qc: LRUMapping,
        input_language: InputLanguage,
        output_format: OutputFormat,
        expect_one: bool = False,
        required_one: bool = False,
        implicit_limit: int = 0,
        inline_typenames: bool = False,
        inline_typeids: bool = False,
        allow_capabilities: enums.Capability = enums.Capability.ALL,
        state: typing.Optional[dict] = None,
        annotations: typing.Optional[dict[str, str]] = None,
    ):
        self.query = query
        self.args = args
        self.kwargs = kwargs
        self.reg = reg
        self.qc = qc
        self.input_language = input_language
        self.output_format = output_format
        self.expect_one = bool(expect_one)
        self.required_one = bool(required_one)
        self.implicit_limit = implicit_limit
        self.inline_typenames = bool(inline_typenames)
        self.inline_typeids = bool(inline_typeids)
        self.allow_capabilities = allow_capabilities
        self.state = state

        self.cardinality = None
        self.in_dc = self.out_dc = None
        self.capabilities = 0
        self.warnings = ()
        self.unsafe_isolation_dangers = ()
        self.annotations = annotations

    cdef inline bint has_na_cardinality(self):
        return self.cardinality == CARDINALITY_NOT_APPLICABLE

    cdef bint load_from_cache(self):
        key = (
            self.query,
            self.output_format,
            self.implicit_limit,
            self.inline_typenames,
            self.inline_typeids,
            self.expect_one,
        )
        rv = self.qc.get(key, None)
        if rv is None:
            return False
        else:
            (
                self.cardinality,
                self.in_dc,
                self.out_dc,
                self.capabilities,
                self.unsafe_isolation_dangers
            ) = rv
            return True

    cdef inline store_to_cache(self):
        assert self.in_dc is not None
        assert self.out_dc is not None
        key = (
            self.query,
            self.output_format,
            self.implicit_limit,
            self.inline_typenames,
            self.inline_typeids,
            self.expect_one,
        )
        self.qc[key] = (
            self.cardinality,
            self.in_dc,
            self.out_dc,
            self.capabilities,
            self.unsafe_isolation_dangers,
        )

    cdef prefers_repeatable_read(self):
        return (
            self.state
            and (config := self.state.get('config'))
            and config.get('default_transaction_isolation')
            == 'PreferRepeatableRead'
        )


cdef class SansIOProtocol:

    def __init__(self, con_params):
        self.buffer = ReadBuffer()

        self.con_params = con_params

        self.connected = False
        self.cancelled = False
        self.backend_secret = None

        self.xact_status = TRANS_UNKNOWN

        self.internal_reg = CodecsRegistry()
        self.server_settings = {}
        self.reset_status()
        self.protocol_version = (PROTO_VER_MAJOR, PROTO_VER_MINOR)

        self.state_type_id = NULL_CODEC_ID
        self.state_codec = None
        self.state_cache = (None, None)

    cdef reset_status(self):
        self.last_status = None
        self.last_details = None

    def get_settings(self):
        return types.MappingProxyType(self.server_settings)

    def is_in_transaction(self):
        return self.xact_status in (TRANS_INTRANS, TRANS_INERROR)

    def set_connection(self, con):
        self.con = weakref.ref(con)

    cpdef abort(self):
        raise NotImplementedError

    cdef write(self, WriteBuffer buf):
        raise NotImplementedError

    async def wait_for_message(self):
        raise NotImplementedError

    async def try_recv_eagerly(self):
        # If there's data in the socket try reading it into the
        # buffer.  Needed for blocking-io connections.
        raise NotImplementedError

    async def wait_for_connect(self):
        raise NotImplementedError

    async def wait_for_disconnect(self):
        raise NotImplementedError

    cdef inline ignore_headers(self):
        cdef uint16_t num_fields = <uint16_t>self.buffer.read_int16()
        if self.is_legacy:
            while num_fields:
                self.buffer.read_int16()  # key
                self.buffer.read_len_prefixed_bytes()  # value
                num_fields -= 1
        else:
            while num_fields:
                self.buffer.read_len_prefixed_bytes()  # key
                self.buffer.read_len_prefixed_bytes()  # value
                num_fields -= 1

    cdef inline dict read_headers(self):
        cdef uint16_t num_fields = <uint16_t>self.buffer.read_int16()
        headers = {}
        if self.is_legacy:
            while num_fields:
                self.buffer.read_int16()  # key
                self.buffer.read_len_prefixed_bytes()  # value
                num_fields -= 1
        else:
            while num_fields:
                key = self.buffer.read_len_prefixed_utf8()
                value = self.buffer.read_len_prefixed_utf8()
                headers[key] = value
                num_fields -= 1

        return headers

    cdef write_annotations(self, ExecuteContext ctx, WriteBuffer buf):
        num_annos = len(ctx.annotations) if ctx.annotations is not None else 0
        if self.protocol_version >= (3, 0) and num_annos > 0:
            if num_annos >= 1 << 16:
                raise errors.InvalidArgumentError("too many annotations")
            buf.write_int16(num_annos)
            for key, value in ctx.annotations.items():
                buf.write_len_prefixed_utf8(key)
                buf.write_len_prefixed_utf8(value)
        else:
            buf.write_int16(0)  # no annotations

    cdef ensure_connected(self):
        if self.cancelled:
            raise errors.ClientConnectionClosedError(
                'the connection has been closed '
                'because an operation was cancelled on it')
        if not self.connected:
            raise errors.ClientConnectionClosedError(
                'the connection has been closed')

    # state is its own argument because some paths override it
    cdef WriteBuffer encode_parse_params(self, ExecuteContext ctx, dict state):
        cdef:
            WriteBuffer buf

        compilation_flags = enums.CompilationFlag.INJECT_OUTPUT_OBJECT_IDS
        if ctx.inline_typenames:
            compilation_flags |= enums.CompilationFlag.INJECT_OUTPUT_TYPE_NAMES
        if ctx.inline_typeids:
            compilation_flags |= enums.CompilationFlag.INJECT_OUTPUT_TYPE_IDS

        buf = WriteBuffer.new()
        buf.write_int64(<int64_t>ctx.allow_capabilities)
        buf.write_int64(<int64_t><uint64_t>compilation_flags)
        buf.write_int64(<int64_t>ctx.implicit_limit)
        if self.protocol_version >= (3, 0):
            buf.write_byte(ctx.input_language)
        buf.write_byte(ctx.output_format)
        buf.write_byte(CARDINALITY_ONE if ctx.expect_one else CARDINALITY_MANY)
        buf.write_len_prefixed_utf8(ctx.query)

        state_type_id, state_data = self.encode_state(state)
        buf.write_bytes(state_type_id)
        buf.write_bytes(state_data)

        return buf

    def _get_active_state(self, ctx: ExecuteContext, *, bint is_execute):
        '''Make any adjustments to state before encoding it.

        Currently this just consists of modifying config to implement
        PrefersRepeatableRead.
        '''
        state = ctx.state
        if ctx.prefers_repeatable_read():
            state = state.copy()
            state['config'] = state['config'].copy()

            # If we are actually doing an execute, and parse didn't
            # report any isolation dangers with the query, then adjust
            # default_transaction_isolation to RepeatableRead.
            # Otherwise, clear it.
            if (
                is_execute
                and not ctx.unsafe_isolation_dangers
                and not self.is_in_transaction()
            ):
                state['config']['default_transaction_isolation'] = (
                    'RepeatableRead'
                )
            else:
                del state['config']['default_transaction_isolation']

        return state

    async def _parse(self, ctx: ExecuteContext):
        cdef:
            WriteBuffer buf, params
            char mtype
            int16_t type_size
            bytes in_type_id
            bytes out_type_id

        if not self.connected:
            raise RuntimeError('not connected')

        buf = WriteBuffer.new_message(PREPARE_MSG)
        self.write_annotations(ctx, buf)

        params = self.encode_parse_params(
            ctx, self._get_active_state(ctx, is_execute=False)
        )

        buf.write_buffer(params)
        buf.end_message()
        buf.write_bytes(SYNC_MESSAGE)
        self.write(buf)

        exc = None
        while True:
            if not self.buffer.take_message():
                await self.wait_for_message()
            mtype = self.buffer.get_message_type()

            try:
                if mtype == STMT_DATA_DESC_MSG:
                    self.parse_describe_type_message(ctx)

                elif mtype == STATE_DATA_DESC_MSG:
                    self.parse_describe_state_message()

                elif mtype == ERROR_RESPONSE_MSG:
                    exc = self.parse_error_message()
                    exc._query = ctx.query
                    exc = self._amend_parse_error(
                        exc,
                        ctx.output_format,
                        ctx.expect_one,
                        ctx.required_one,
                    )

                elif mtype == READY_FOR_COMMAND_MSG:
                    self.parse_sync_message()
                    break

                else:
                    self.fallthrough()
            finally:
                self.buffer.finish_message()

        if exc is not None:
            raise exc

        if ctx.required_one and ctx.has_na_cardinality():
            assert ctx.output_format != OutputFormat.NONE
            methname = _QUERY_SINGLE_METHOD[ctx.required_one][ctx.output_format]
            raise errors.InterfaceError(
                f'query cannot be executed with {methname}() as it '
                f'does not return any data')

    async def _execute(self, ctx: ExecuteContext):
        cdef:
            WriteBuffer packet
            WriteBuffer buf
            WriteBuffer params
            char mtype
            object result

        params = self.encode_parse_params(
            ctx, self._get_active_state(ctx, is_execute=True)
        )

        buf = WriteBuffer.new_message(EXECUTE_MSG)
        self.write_annotations(ctx, buf)

        buf.write_buffer(params)

        buf.write_bytes(ctx.in_dc.get_tid())
        buf.write_bytes(ctx.out_dc.get_tid())

        self.encode_args(ctx.in_dc, buf, ctx.args, ctx.kwargs)

        buf.end_message()

        packet = WriteBuffer.new()
        packet.write_buffer(buf)
        packet.write_bytes(SYNC_MESSAGE)
        self.write(packet)

        result = []
        exc = None
        while True:
            if not self.buffer.take_message():
                await self.wait_for_message()
            mtype = self.buffer.get_message_type()

            try:
                if mtype == STMT_DATA_DESC_MSG:
                    # our in/out type spec is out-dated
                    self.parse_describe_type_message(ctx)
                    ctx.store_to_cache()

                elif mtype == STATE_DATA_DESC_MSG:
                    self.parse_describe_state_message()

                elif mtype == DATA_MSG:
                    if exc is None:
                        try:
                            self.parse_data_messages(ctx.out_dc, result)
                        except Exception as ex:
                            # An error during data decoding.  We need to
                            # handle this as gracefully as possible:
                            # * save the exception to raise it once SYNC is
                            #   received;
                            # * ignore all 'D' messages for this query.
                            exc = errors.ClientError(
                                'unable to decode data to Python objects')
                            exc.__cause__ = ex
                            # Take care of a partially consumed 'D' message
                            # and the ones yet unparsed.
                            while self.buffer.take_message_type(DATA_MSG):
                                self.buffer.discard_message()
                    else:
                        self.buffer.discard_message()

                elif mtype == COMMAND_COMPLETE_MSG:
                    self.parse_command_complete_message()

                elif mtype == ERROR_RESPONSE_MSG:
                    exc = self.parse_error_message()
                    exc._query = ctx.query
                    if exc.get_code() == parameter_type_mismatch_code:
                        if not isinstance(ctx.in_dc, NullCodec):
                            buf = WriteBuffer.new()
                            try:
                                self.encode_args(
                                    ctx.in_dc, buf, ctx.args, ctx.kwargs
                                )
                            except errors.QueryArgumentError as ex:
                                exc = ex
                            finally:
                                buf = None
                    else:
                        exc = self._amend_parse_error(
                            exc,
                            ctx.output_format,
                            ctx.expect_one,
                            ctx.required_one,
                        )

                elif mtype == READY_FOR_COMMAND_MSG:
                    self.parse_sync_message()
                    break

                else:
                    self.fallthrough()

            finally:
                self.buffer.finish_message()

        if exc is not None:
            raise exc
        else:
            return result

    cdef encode_state(self, state):
        cdef WriteBuffer buf

        if state is not None:
            if self.state_cache[0] is state:
                state_data = self.state_cache[1]
            else:
                assert self.state_codec is not None
                buf = WriteBuffer.new()
                self.state_codec.encode(buf, state)
                state_data = bytes(buf)
                self.state_cache = (state, state_data)
            return self.state_type_id, state_data
        else:
            return NULL_CODEC_ID, EMPTY_NULL_DATA

    async def execute(self, ctx: ExecuteContext):
        self.ensure_connected()
        self.reset_status()

        if ctx.load_from_cache():
            pass
        elif (
            not ctx.args
            and not ctx.kwargs
            and not ctx.required_one
            and not ctx.prefers_repeatable_read()
        ):
            # We don't have knowledge about the in/out desc of the command, but
            # the caller didn't provide any arguments, so let's try using NULL
            # for both in (assumed) and out (the server will correct it) desc
            # without an additional Parse, unless required_one is set because
            # it'll be too late to find out the cardinality is wrong when the
            # command is already executed.
            #
            # We also can't do it if we want to lower to
            # RepeatableRead if possible, since we need to find out if
            # it is allowed.
            ctx.in_dc = ctx.out_dc = NULL_CODEC
        else:
            await self._parse(ctx)
            ctx.store_to_cache()

        return await self._execute(ctx)

    async def query(self, ctx: ExecuteContext):
        ret = await self.execute(ctx)
        if ctx.expect_one:
            if ret or not ctx.required_one:
                if ret:
                    return ret[0]
                else:
                    if ctx.output_format == OutputFormat.JSON:
                        return 'null'
                    else:
                        return None
            else:
                methname = (
                    _QUERY_SINGLE_METHOD[ctx.required_one][ctx.output_format]
                )
                raise errors.NoDataError(
                    f'query executed via {methname}() returned no data')
        else:
            if ret:
                if ctx.output_format == OutputFormat.JSON:
                    return ret[0]
                else:
                    return ret
            else:
                if ctx.output_format == OutputFormat.JSON:
                    return '[]'
                else:
                    return ret

    async def dump(self, header_callback, block_callback):
        cdef:
            WriteBuffer buf
            char mtype

        self.ensure_connected()
        self.reset_status()

        buf = WriteBuffer.new_message(DUMP_MSG)
        if self.protocol_version >= (3, 0):
            buf.write_int16(0)  # no annotations
            buf.write_int64(0)  # flags
            buf.end_message()
        else:
            buf.write_int16(0)  # no headers
            buf.end_message()
        buf.write_bytes(SYNC_MESSAGE)
        self.write(buf)

        header_received = False
        data_received = False
        exc = None
        while True:
            if not self.buffer.take_message():
                await self.wait_for_message()
            mtype = self.buffer.get_message_type()

            try:
                if mtype == DUMP_BLOCK_MSG:
                    if not header_received:
                        raise RuntimeError('data block before header block')
                    data_received = True

                    await block_callback(
                        self.buffer.consume_message()
                    )

                elif mtype == DUMP_HEADER_BLOCK_MSG:
                    if header_received:
                        raise RuntimeError('more than one header block')
                    if data_received:
                        raise RuntimeError('header block after data block')

                    header_received = True

                    await header_callback(
                        self.buffer.consume_message()
                    )

                elif mtype == ERROR_RESPONSE_MSG:
                    # ErrorResponse
                    exc = self.parse_error_message()

                elif mtype == COMMAND_COMPLETE_MSG:
                    pass

                elif mtype == READY_FOR_COMMAND_MSG:
                    self.parse_sync_message()
                    break

                else:
                    self.fallthrough()

            finally:
                self.buffer.finish_message()

        if not header_received:
            raise RuntimeError('header block was not received')

        if exc is not None:
            raise exc

    async def _sync(self):
        cdef char mtype
        self.write(WriteBuffer.new_message(SYNC_MSG).end_message())
        while True:
            if not self.buffer.take_message():
                await self.wait_for_message()
            mtype = self.buffer.get_message_type()

            if mtype == READY_FOR_COMMAND_MSG:
                self.parse_sync_message()
                break
            else:
                self.fallthrough()

    async def ping(self):
        cdef char mtype
        self.write(WriteBuffer.new_message(SYNC_MSG).end_message())
        exc = None
        while True:
            if not self.buffer.take_message():
                await self.wait_for_message()
            mtype = self.buffer.get_message_type()

            if mtype == READY_FOR_COMMAND_MSG:
                self.parse_sync_message()
                break
            elif mtype == ERROR_RESPONSE_MSG:
                exc = self.parse_error_message()
                self.buffer.finish_message()
                break
            else:
                self.fallthrough()
        if exc is not None:
            raise exc

    async def restore(self, bytes header, data_gen):
        cdef:
            WriteBuffer buf
            char mtype

        self.ensure_connected()
        self.reset_status()

        buf = WriteBuffer.new_message(RESTORE_MSG)
        buf.write_int16(0)  # no attributes
        buf.write_int16(1)  # -j level
        buf.write_bytes(header)
        buf.end_message()
        self.write(buf)

        exc = None
        while True:
            if not self.buffer.take_message():
                await self.wait_for_message()
            mtype = self.buffer.get_message_type()

            try:
                if mtype == RESTORE_READY_MSG:
                    self.ignore_headers()
                    self.buffer.read_int16()  # discard -j level
                    break
                elif mtype == ERROR_RESPONSE_MSG:
                    exc = self.parse_error_message()
                    break
                else:
                    self.fallthrough()
            finally:
                self.buffer.finish_message()

        if exc is not None:
            await self._sync()
            raise exc

        # TODO: flow-control is missing here. For now this is fine
        # since this method is only called on a blocking connection.

        async for data in data_gen:
            buf = WriteBuffer.new_message(DUMP_BLOCK_MSG)
            buf.write_bytes(data)
            self.write(buf.end_message())

            if not self.buffer.take_message():
                await self.try_recv_eagerly()
            if self.buffer.take_message():
                # Check if we received an error.
                mtype = self.buffer.get_message_type()
                if mtype == ERROR_RESPONSE_MSG:
                    exc = self.parse_error_message()
                    self.buffer.finish_message()
                    break
                else:
                    self.fallthrough()

        if exc is not None:
            await self._sync()
            raise exc

        self.write(WriteBuffer.new_message(RESTORE_EOF_MSG).end_message())

        exc = None
        while True:
            if not self.buffer.take_message():
                await self.wait_for_message()
            mtype = self.buffer.get_message_type()

            try:
                if mtype == COMMAND_COMPLETE_MSG:
                    break
                elif mtype == ERROR_RESPONSE_MSG:
                    exc = self.parse_error_message()
                    break
                else:
                    self.fallthrough()
            finally:
                self.buffer.finish_message()

        if exc is not None:
            await self._sync()
            raise exc

    def terminate(self):
        try:
            self.write(WriteBuffer.new_message(TERMINATE_MSG).end_message())
        except errors.ClientConnectionError:
            pass

    async def connect(self):
        cdef:
            WriteBuffer ver_buf
            WriteBuffer msg_buf
            WriteBuffer buf
            char mtype
            int32_t status

        await self.wait_for_connect()

        if self.connected:
            raise RuntimeError('already connected')

        # protocol version
        handshake_buf = WriteBuffer.new_message(CLIENT_HANDSHAKE_MSG)
        handshake_buf.write_int16(PROTO_VER_MAJOR)
        handshake_buf.write_int16(PROTO_VER_MINOR)
        self.protocol_version = (PROTO_VER_MAJOR, PROTO_VER_MINOR)

        # params
        params = {
            'user': self.con_params.user,
            'database': self.con_params.database,
        }
        if self.con_params.secret_key:
            params['secret_key'] = self.con_params.secret_key
        handshake_buf.write_int16(len(params))
        for k, v in params.items():
            handshake_buf.write_len_prefixed_utf8(k)
            handshake_buf.write_len_prefixed_utf8(v)

        handshake_buf.write_int16(0)  # reserved
        handshake_buf.end_message()

        self.write(handshake_buf)

        while True:
            if not self.buffer.take_message():
                await self.wait_for_message()
            mtype = self.buffer.get_message_type()

            if mtype == SERVER_HANDSHAKE_MSG:
                # Server responded with ServerHandshake, this
                # means protocol negotiation.
                major = self.buffer.read_int16()
                minor = self.buffer.read_int16()

                # TODO: drop this branch when dropping protocol_v0
                if major == 0:
                    self.is_legacy = True
                    self.ignore_headers()

                self.buffer.finish_message()

                if (major, minor) < (MIN_PROTO_VER_MAJOR, MIN_PROTO_VER_MINOR):
                    raise errors.ClientConnectionError(
                        f'the server requested an unsupported version of '
                        f'the protocol: {major}.{minor}'
                    )
                else:
                    self.protocol_version = (major, minor)

            elif mtype == AUTH_REQUEST_MSG:
                # Authentication...
                status = self.buffer.read_int32()
                if status == AuthenticationStatuses.AUTH_OK:
                    pass
                elif status == AuthenticationStatuses.AUTH_SASL:
                    await self._auth_sasl()
                else:
                    self.abort()
                    raise RuntimeError(
                        f'unsupported authentication method requested by the '
                        f'server: {status}')

            elif mtype == SERVER_KEY_DATA_MSG:
                self.backend_secret = self.buffer.read_bytes(32)

            elif mtype == STATE_DATA_DESC_MSG:
                self.parse_describe_state_message()

            elif mtype == ERROR_RESPONSE_MSG:
                raise self.parse_error_message()

            elif mtype == READY_FOR_COMMAND_MSG:
                # ReadyForQuery
                self.parse_sync_message()
                if self.xact_status == TRANS_IDLE:
                    self.connected = True
                    break
                else:
                    raise RuntimeError('non-idle status after connect')

            else:
                self.fallthrough()

            self.buffer.finish_message()

    async def _auth_sasl(self):
        num_methods = self.buffer.read_int32()
        if num_methods <= 0:
            raise RuntimeError(
                'the server requested SASL authentication but did not '
                'offer any methods')

        methods = []
        for i in range(num_methods):
            method = self.buffer.read_len_prefixed_bytes()
            methods.append(method)

        self.buffer.finish_message()

        for method in methods:
            if method == b'SCRAM-SHA-256':
                break
        else:
            raise RuntimeError(
                f'the server offered the following SASL authentication '
                f'methods: {", ".join(methods)}, neither are supported.')

        client_nonce = scram.generate_nonce()
        client_first, client_first_bare = scram.build_client_first_message(
            client_nonce, self.con_params.user)

        msg_buf = WriteBuffer.new_message(AUTH_INITIAL_RESPONSE_MSG)
        msg_buf.write_len_prefixed_bytes(b'SCRAM-SHA-256')
        msg_buf.write_len_prefixed_utf8(client_first)
        msg_buf.end_message()
        self.write(msg_buf)

        while True:
            if not self.buffer.take_message():
                await self.wait_for_message()
            mtype = self.buffer.get_message_type()

            if mtype == ERROR_RESPONSE_MSG:
                # ErrorResponse
                exc = self.parse_error_message()
                self.buffer.finish_message()
                raise exc

            elif mtype == AUTH_REQUEST_MSG:
                break

            else:
                self.fallthrough()

        status = self.buffer.read_int32()
        if status != AuthenticationStatuses.AUTH_SASL_CONTINUE:
            raise RuntimeError(
                f'expected SASLContinue from the server, received {status}')

        server_first = self.buffer.read_len_prefixed_bytes()
        self.buffer.finish_message()

        server_nonce, salt, itercount = (
            scram.parse_server_first_message(server_first))

        client_final, expected_server_sig = scram.build_client_final_message(
            self.con_params.password or '',
            salt,
            itercount,
            client_first_bare.encode('utf-8'),
            server_first,
            server_nonce)

        msg_buf = WriteBuffer.new_message(AUTH_RESPONSE_MSG)
        msg_buf.write_len_prefixed_utf8(client_final)
        msg_buf.end_message()
        self.write(msg_buf)

        while True:
            if not self.buffer.take_message():
                await self.wait_for_message()
            mtype = self.buffer.get_message_type()

            if mtype == ERROR_RESPONSE_MSG:
                exc = self.parse_error_message()
                self.buffer.finish_message()
                raise exc

            elif mtype == AUTH_REQUEST_MSG:
                break

            else:
                self.fallthrough()

        status = self.buffer.read_int32()
        if status != AuthenticationStatuses.AUTH_SASL_FINAL:
            raise RuntimeError(
                f'expected SASLFinal from the server, received {status}')

        server_final = self.buffer.read_len_prefixed_bytes()
        self.buffer.finish_message()
        server_sig = scram.parse_server_final_message(server_final)

        if server_sig != expected_server_sig:
            raise RuntimeError(
                f'server SCRAM proof does not match')

    cdef parse_system_config(self, BaseCodec codec, bytes data):
        cdef:
            decode_row_method decoder = <decode_row_method>codec.decode

            const char* buf
            ssize_t buf_len

            FRBuffer _rbuf
            FRBuffer *rbuf = &_rbuf

        buf = cpython.PyBytes_AS_STRING(data)
        buf_len = cpython.PyBytes_GET_SIZE(data)

        frb_init(rbuf, buf, buf_len)

        return decoder(codec, rbuf)

    cdef parse_server_settings(self, str name, bytes val):
        if name == 'suggested_pool_concurrency':
            self.server_settings[name] = int(val.decode('utf-8'))
        elif name == 'system_config':
            buf = ReadBuffer()
            buf.feed_data(val)
            typedesc_len = buf.read_int32() - 16
            typedesc_id = buf.read_bytes(16)
            typedesc = buf.read_bytes(typedesc_len)

            if self.internal_reg.has_codec(typedesc_id):
                codec = self.internal_reg.get_codec(typedesc_id)
            else:
                codec = self.internal_reg.build_codec(
                    typedesc, self.protocol_version)

            data = buf.read_len_prefixed_bytes()
            self.server_settings[name] = self.parse_system_config(codec, data)
        else:
            self.server_settings[name] = val

    cdef fallthrough(self):
        cdef:
            char mtype = self.buffer.get_message_type()

        if mtype == PARAMETER_STATUS_MSG:
            name = self.buffer.read_len_prefixed_utf8()
            val = self.buffer.read_len_prefixed_bytes()
            self.buffer.finish_message()
            self.parse_server_settings(name, val)

        elif mtype == LOG_MSG:
            severity = <uint8_t>self.buffer.read_byte()
            code = <uint32_t>self.buffer.read_int32()
            message = self.buffer.read_len_prefixed_utf8()
            # Ignore any headers: not yet specified for log messages.
            self.ignore_headers()
            self.buffer.finish_message()

            msg = errors.EdgeDBMessage._from_code(code, severity, message)
            if self.con is not None:
                con = self.con()
                if con is not None:
                    con._on_log_message(msg)

        else:
            self.abort()

            raise errors.ProtocolError(
                f'unexpected message type {chr(mtype)!r}')

    cdef encode_args(self, BaseCodec in_dc, WriteBuffer buf, args, kwargs):
        if args and kwargs:
            raise errors.QueryArgumentError(
                'either positional or named arguments are supported; '
                'not both')

        in_dc_type = type(in_dc)

        if in_dc_type is NullCodec:
            if args:
                raise errors.QueryArgumentError(
                    'expected no positional arguments')
            if kwargs:
                raise errors.QueryArgumentError(
                    'expected no named arguments')

            buf.write_bytes(EMPTY_NULL_DATA)
            return

        if in_dc_type is not ObjectCodec:
            raise errors.QueryArgumentError(
                'unexpected query argument codec')

        if args:
            kwargs = {str(i): v for i, v in enumerate(args)}

        (<ObjectCodec>in_dc).encode_args(buf, kwargs)

    cdef parse_describe_type_message(self, ExecuteContext ctx):
        assert self.buffer.get_message_type() == COMMAND_DATA_DESC_MSG

        try:
            headers = self.read_headers()
            if headers and 'warnings' in headers:
                warnings = tuple([
                    errors.EdgeDBError._from_json(w)
                    for w in json.loads(headers['warnings'])
                ])
                for w in warnings:
                    w._query = ctx.query
                ctx.warnings = warnings
            if headers and 'unsafe_isolation_dangers' in headers:
                dangers = tuple([
                    errors.EdgeDBError._from_json(w)
                    for w in json.loads(headers['unsafe_isolation_dangers'])
                ])
                for w in dangers:
                    w._query = ctx.query
                ctx.unsafe_isolation_dangers = dangers

            ctx.capabilities = self.buffer.read_int64()
            ctx.cardinality = self.buffer.read_byte()
            ctx.in_dc, ctx.out_dc = self.parse_type_data(ctx.reg)
        finally:
            self.buffer.finish_message()

    cdef parse_describe_state_message(self):
        assert self.buffer.get_message_type() == STATE_DATA_DESC_MSG
        try:
            state_type_id = self.buffer.read_bytes(16)
            state_typedesc = self.buffer.read_len_prefixed_bytes()

            if state_type_id != self.state_type_id:
                self.state_type_id = state_type_id
                self.state_cache = (None, None)
                if self.internal_reg.has_codec(state_type_id):
                    self.state_codec = self.internal_reg.get_codec(
                        state_type_id
                    )
                else:
                    self.state_codec = self.internal_reg.build_codec(
                        state_typedesc, self.protocol_version
                    )
        finally:
            self.buffer.finish_message()

    cdef parse_type_data(self, CodecsRegistry reg):
        cdef:
            bytes type_id
            BaseCodec in_dc, out_dc

        type_id = self.buffer.read_bytes(16)
        type_data = self.buffer.read_len_prefixed_bytes()

        if reg.has_codec(type_id):
            in_dc = reg.get_codec(type_id)
        else:
            in_dc = reg.build_codec(type_data, self.protocol_version)

        type_id = self.buffer.read_bytes(16)
        type_data = self.buffer.read_len_prefixed_bytes()

        if reg.has_codec(type_id):
            out_dc = reg.get_codec(type_id)
        else:
            out_dc = reg.build_codec(type_data, self.protocol_version)

        return in_dc, out_dc

    cdef parse_data_messages(self, BaseCodec out_dc, result):
        cdef:
            ReadBuffer buf = self.buffer

            decode_row_method decoder = <decode_row_method>out_dc.decode
            pgproto.try_consume_message_method try_consume_message = \
                <pgproto.try_consume_message_method>buf.try_consume_message
            pgproto.take_message_type_method take_message_type = \
                <pgproto.take_message_type_method>buf.take_message_type

            const char* cbuf
            ssize_t cbuf_len
            object row

            FRBuffer _rbuf
            FRBuffer *rbuf = &_rbuf

        if PG_DEBUG:
            if buf.get_message_type() != DATA_MSG:
                raise RuntimeError('first message is not "DataMsg"')

            if not isinstance(result, list):
                raise RuntimeError(
                    f'result is not a list, but {result!r}')

        while take_message_type(buf, DATA_MSG):
            cbuf = try_consume_message(buf, &cbuf_len)
            if cbuf == NULL:
                mem = buf.consume_message()
                cbuf = cpython.PyBytes_AS_STRING(mem)
                cbuf_len = cpython.PyBytes_GET_SIZE(mem)

            if PG_DEBUG:
                frb_init(rbuf, cbuf, cbuf_len)

                flen = hton.unpack_int16(frb_read(rbuf, 2))
                if flen != 1:
                    raise RuntimeError(
                        f'invalid number of columns: expected 1 got {flen}')

                buflen = hton.unpack_int32(frb_read(rbuf, 4))
                if frb_get_len(rbuf) != buflen:
                    raise RuntimeError('invalid buffer length')
            else:
                # EdgeDB returns rows with one column; Postgres' rows
                # are encoded as follows:
                #   2 bytes - int16 - number of columns
                #   4 bytes - int32 - every column is prefixed with its length
                # so we want to skip first 6 bytes:
                frb_init(rbuf, cbuf + 6, cbuf_len - 6)

            row = decoder(out_dc, rbuf)
            result.append(row)

            if frb_get_len(rbuf):
                raise RuntimeError(
                    f'unexpected trailing data in buffer after '
                    f'data message decoding: {frb_get_len(rbuf)}')

    cdef parse_command_complete_message(self):
        assert self.buffer.get_message_type() == COMMAND_COMPLETE_MSG
        self.ignore_headers()
        self.last_capabilities = enums.Capability(self.buffer.read_int64())
        self.last_status = self.buffer.read_len_prefixed_bytes()
        self.buffer.read_bytes(16)  # state type id
        self.buffer.read_len_prefixed_bytes()  # state
        self.buffer.finish_message()

    cdef parse_sync_message(self):
        cdef char status

        assert self.buffer.get_message_type() == READY_FOR_COMMAND_MSG

        self.ignore_headers()

        status = self.buffer.read_byte()

        if status == TRANS_STATUS_IDLE:
            self.xact_status = TRANS_IDLE
        elif status == TRANS_STATUS_INTRANS:
            self.xact_status = TRANS_INTRANS
        elif status == TRANS_STATUS_ERROR:
            self.xact_status = TRANS_INERROR
        else:
            self.xact_status = TRANS_UNKNOWN

        self.buffer.finish_message()

    cdef _amend_parse_error(
        self,
        exc,
        OutputFormat output_format,
        bint expect_one,
        bint required_one,
    ):
        if expect_one and exc.get_code() == result_cardinality_mismatch_code:
            assert output_format != OutputFormat.NONE
            methname = _QUERY_SINGLE_METHOD[required_one][output_format]
            new_exc = errors.InterfaceError(
                f'query cannot be executed with {methname}() as it '
                f'may return more than one element')
            new_exc.__cause__ = exc
            exc = new_exc

        return exc

    cdef dict parse_error_headers(self):
        cdef:
            dict attrs
            uint16_t num_fields
            uint16_t key
            bytes value

        attrs = {}
        num_fields = <uint16_t>self.buffer.read_int16()
        while num_fields:
            key = <uint16_t>self.buffer.read_int16()
            value = self.buffer.read_len_prefixed_bytes()
            attrs[key] = value
            num_fields -= 1
        return attrs

    cdef parse_error_message(self):
        cdef:
            uint32_t code
            int16_t num_fields
            uint8_t severity
            str msg

        assert self.buffer.get_message_type() == ERROR_RESPONSE_MSG

        severity = <uint8_t>self.buffer.read_byte()
        code = <uint32_t>self.buffer.read_int32()
        msg = self.buffer.read_len_prefixed_utf8()
        attrs = self.parse_error_headers()

        # It's safe to always map error codes as we don't reuse them
        code = OLD_ERROR_CODES.get(code, code)

        exc = errors.EdgeDBError._from_code(code, msg)
        exc._attrs = attrs
        return exc


## etc

cdef result_cardinality_mismatch_code = \
    errors.ResultCardinalityMismatchError._code
cdef parameter_type_mismatch_code = errors.ParameterTypeMismatchError._code

cdef bytes SYNC_MESSAGE = bytes(
    WriteBuffer.new_message(SYNC_MSG).end_message())
cdef bytes FLUSH_MESSAGE = bytes(
    WriteBuffer.new_message(FLUSH_MSG).end_message())


include "protocol_v0.pyx"
