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

from libc.stdint cimport int16_t, int32_t, uint16_t, \
                         uint32_t, int64_t, uint64_t

from gel.pgproto.pgproto cimport (
    WriteBuffer,
    ReadBuffer,
    FRBuffer,
)

from gel.pgproto cimport pgproto
from gel.pgproto.debug cimport PG_DEBUG


include "./lru.pxd"
include "./codecs/codecs.pxd"


ctypedef object (*decode_row_method)(BaseCodec, FRBuffer *buf)


cpdef enum InputLanguage:
    EDGEQL = 0x45  # b'E'
    SQL = 0x53  # b'S'


cpdef enum OutputFormat:
    BINARY = 98  # b'b'
    JSON = 106  # b'j'
    JSON_ELEMENTS = 74  # b'J'
    NONE = 110  # b'n'


cdef enum TransactionStatus:
    TRANS_IDLE = 0                 # connection idle
    TRANS_ACTIVE = 1               # command in progress
    TRANS_INTRANS = 2              # idle, within transaction block
    TRANS_INERROR = 3              # idle, within failed transaction
    TRANS_UNKNOWN = 4              # cannot determine status


cdef enum EdgeParseFlags:
    PARSE_HAS_RESULT = 1 << 0
    PARSE_SINGLETON_RESULT = 1 << 1


cdef enum AuthenticationStatuses:
    AUTH_OK = 0
    AUTH_SASL = 10
    AUTH_SASL_CONTINUE = 11
    AUTH_SASL_FINAL = 12


cdef class ExecuteContext:
    cdef:
        # Input arguments
        str query
        object args
        object kwargs
        CodecsRegistry reg
        LRUMapping qc
        InputLanguage input_language
        OutputFormat output_format
        bint expect_one
        bint required_one
        int implicit_limit
        bint inline_typenames
        bint inline_typeids
        uint64_t allow_capabilities
        object state
        object annotations

        # Contextual variables
        readonly bytes cardinality
        readonly BaseCodec in_dc
        readonly BaseCodec out_dc
        readonly uint64_t capabilities
        readonly tuple warnings
        readonly tuple unsafe_isolation_dangers

    cdef inline bint has_na_cardinality(self)
    cdef bint load_from_cache(self)
    cdef inline store_to_cache(self)
    cdef prefers_repeatable_read(self)


cdef class SansIOProtocol:

    cdef:
        ReadBuffer buffer

        readonly bint connected
        readonly bint cancelled

        object con
        readonly object con_params

        object backend_secret

        TransactionStatus xact_status

        CodecsRegistry internal_reg
        dict server_settings

        readonly bytes last_status
        readonly bytes last_details
        readonly object last_capabilities
        readonly tuple protocol_version

        readonly bint is_legacy

        bytes state_type_id
        BaseCodec state_codec
        object state_cache

    cdef encode_args(self, BaseCodec in_dc, WriteBuffer buf, args, kwargs)
    cdef encode_state(self, state)

    cdef parse_data_messages(self, BaseCodec out_dc, result)
    cdef parse_sync_message(self)
    cdef parse_command_complete_message(self)
    cdef parse_describe_type_message(self, ExecuteContext ctx)
    cdef parse_describe_state_message(self)
    cdef parse_type_data(self, CodecsRegistry reg)
    cdef _amend_parse_error(
        self,
        exc,
        OutputFormat output_format,
        bint expect_one,
        bint required_one,
    )

    cdef inline ignore_headers(self)
    cdef inline dict read_headers(self)
    cdef dict parse_error_headers(self)
    cdef write_annotations(self, ExecuteContext ctx, WriteBuffer buf)

    cdef parse_error_message(self)

    cdef write(self, WriteBuffer buf)
    cpdef abort(self)

    cdef reset_status(self)

    cdef parse_system_config(self, BaseCodec codec, bytes data)
    cdef parse_server_settings(self, str name, bytes val)

    cdef fallthrough(self)

    cdef ensure_connected(self)

    cdef WriteBuffer encode_parse_params(self, ExecuteContext ctx, dict state)


include "protocol_v0.pxd"
