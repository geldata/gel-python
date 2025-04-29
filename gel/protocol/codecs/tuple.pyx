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
cdef class TupleCodec(BaseRecordCodec):

    cdef decode(self, object return_type, FRBuffer *buf):
        cdef:
            object result
            Py_ssize_t elem_count
            Py_ssize_t i
            int32_t elem_len
            BaseCodec elem_codec
            FRBuffer elem_buf
            tuple fields_codecs = (<BaseRecordCodec>self).fields_codecs

        elem_count = <Py_ssize_t><uint32_t>hton.unpack_int32(frb_read(buf, 4))

        if elem_count != len(fields_codecs):
            raise RuntimeError(
                f'cannot decode Tuple: expected {len(fields_codecs)} '
                f'elements, got {elem_count}')

        result = cpython.PyTuple_New(elem_count)

        for i in range(elem_count):
            frb_read(buf, 4)  # reserved
            elem_len = hton.unpack_int32(frb_read(buf, 4))

            if elem_len == -1:
                elem = None
            else:
                elem_codec = <BaseCodec>fields_codecs[i]
                elem = elem_codec.decode(
                    None, # XXX
                    frb_slice_from(&elem_buf, buf, elem_len)
                )
                if frb_get_len(&elem_buf):
                    raise RuntimeError(
                        f'unexpected trailing data in buffer after '
                        f'tuple element decoding: {frb_get_len(&elem_buf)}')

            cpython.Py_INCREF(elem)
            cpython.PyTuple_SET_ITEM(result, i, elem)

        return result

    cdef dump(self, int level = 0):
        buf = [f'{level * " "}{self.name}']
        for codec in self.fields_codecs:
            buf.append((<BaseCodec>codec).dump(level + 1))
        return '\n'.join(buf)

    @staticmethod
    cdef BaseCodec new(bytes tid, tuple fields_codecs):
        cdef:
            TupleCodec codec

        codec = TupleCodec.__new__(TupleCodec)

        codec.tid = tid
        codec.name = 'Tuple'
        codec.fields_codecs = fields_codecs

        return codec

    def make_type(self, describe_context):
        return describe.TupleType(
            desc_id=uuid.UUID(bytes=self.tid),
            name=self.type_name,
            element_types=tuple(
                codec.make_type(describe_context)
                for codec in self.fields_codecs
            )
        )
