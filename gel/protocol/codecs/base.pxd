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


cdef class BaseCodec:

    cdef:
        bytes   tid
        str     name
        str     type_name

    cdef inline bytes get_tid(self):
        return self.tid

    cdef encode(self, WriteBuffer buf, object obj)
    cdef decode(self, object return_type, FRBuffer *buf)

    cdef dump(self, int level=?)


cdef class CodecPythonOverride(BaseCodec):

    cdef:
        BaseCodec codec
        object encoder
        object decoder

    @staticmethod
    cdef BaseCodec new(bytes tid,
                       BaseCodec basecodec,
                       object encoder,
                       object decoder)


cdef class BaseRecordCodec(BaseCodec):

    cdef:
        tuple fields_codecs
        uint64_t encoder_flags

    cdef _check_encoder(self)


cdef class EmptyTupleCodec(BaseCodec):

    cdef:
        object empty_tup


cdef class NullCodec(BaseCodec):
    pass


cdef class BaseNamedRecordCodec(BaseRecordCodec):

    cdef:
        object descriptor


@cython.final
cdef class EdegDBCodecContext(pgproto.CodecContext):

    cdef:
        object _codec
