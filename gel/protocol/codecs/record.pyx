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
cdef class RecordCodec(BaseNamedRecordCodec):

    cdef encode(self, WriteBuffer buf, object obj):
        raise NotImplementedError

    cdef decode(self, object return_type, FRBuffer *buf):
        cdef:
            object result
            Py_ssize_t elem_count
            Py_ssize_t i
            int32_t elem_len
            BaseCodec elem_codec
            FRBuffer elem_buf
            tuple fields_codecs = (<BaseRecordCodec>self).fields_codecs
            descriptor = (<BaseNamedRecordCodec>self).descriptor

        elem_count = <Py_ssize_t><uint16_t>hton.unpack_int32(frb_read(buf, 4))

        if elem_count != len(fields_codecs):
            raise RuntimeError(
                f'cannot decode Record: expected {len(fields_codecs)} '
                f'elements, got {elem_count}')

        result = datatypes.record_new(descriptor)

        for i in range(elem_count):
            frb_read(buf, 4)  # reserved
            elem_len = hton.unpack_int32(frb_read(buf, 4))

            if elem_len == -1:
                elem = None
            else:
                elem_codec = <BaseCodec>fields_codecs[i]
                elem = elem_codec.decode(
                    None,
                    frb_slice_from(&elem_buf, buf, elem_len)
                )
                if frb_get_len(&elem_buf):
                    raise RuntimeError(
                        f'unexpected trailing data in buffer after '
                        f'record element decoding: {frb_get_len(&elem_buf)}')

            datatypes.record_set(result, i, elem)

        return result

    @staticmethod
    cdef BaseCodec new(bytes tid, tuple names, tuple codecs):
        cdef:
            RecordCodec codec

        codec = RecordCodec.__new__(RecordCodec)

        codec.tid = tid
        codec.name = 'Record'
        codec.descriptor = datatypes.record_desc_new(
            names, <object>NULL, <object>NULL)
        codec.fields_codecs = codecs

        return codec

