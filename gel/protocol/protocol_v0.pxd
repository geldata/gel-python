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

cdef class SansIOProtocolBackwardsCompatible(SansIOProtocol):
    cdef parse_legacy_describe_type_message(self, CodecsRegistry reg)
    cdef parse_legacy_command_complete_message(self)
    cdef legacy_write_headers(self, WriteBuffer buf, dict headers)
    cdef legacy_write_execute_headers(
        self,
        WriteBuffer buf,
        int implicit_limit,
        bint inline_typenames,
        bint inline_typeids,
        uint64_t allow_capabilities,
    )
    cdef dict legacy_parse_headers(self)
    cdef _legacy_parse_type_data(self, CodecsRegistry reg)
    cdef _legacy_parse_data_messages(self, BaseCodec out_dc, result)
