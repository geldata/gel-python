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


@cython.final
cdef class ScalarCodec(BaseCodec):

    def __cinit__(self):
        self.c_encoder = NULL
        self.c_decoder = NULL

    cdef encode(self, WriteBuffer buf, object obj):
        self.c_encoder(DEFAULT_CODEC_CONTEXT, buf, obj)

    cdef decode(self, object return_type, FRBuffer *buf):
        return self.c_decoder(DEFAULT_CODEC_CONTEXT, buf)

    cdef derive(self, bytes tid):
        cdef ScalarCodec rv
        rv = ScalarCodec.new(tid, self.name, self.c_encoder, self.c_decoder)
        rv.base_codec = self
        return rv

    @staticmethod
    cdef BaseCodec new(bytes tid, str name,
                       pgproto.encode_func encoder,
                       pgproto.decode_func decoder):
        cdef:
            ScalarCodec codec

        codec = ScalarCodec.__new__(ScalarCodec)

        codec.tid = tid
        codec.name = name
        codec.c_encoder = encoder
        codec.c_decoder = decoder

        return codec

    def make_type(self, describe_context):
        if self.base_codec is None:
            return describe.BaseScalarType(
                desc_id=uuid.UUID(bytes=self.tid),
                name=self.name,
            )
        else:
            return describe.ScalarType(
                desc_id=uuid.UUID(bytes=self.tid),
                name=self.type_name,
                base_type=self.base_codec.make_type(describe_context),
            )
