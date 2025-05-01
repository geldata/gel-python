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
cdef class SetCodec(BaseArrayCodec):

    @staticmethod
    cdef BaseCodec new(bytes tid, BaseCodec sub_codec):
        cdef:
            SetCodec codec

        codec = SetCodec.__new__(SetCodec)

        codec.tid = tid
        codec.name = 'Set'
        codec.sub_codec = sub_codec

        return codec

    cdef decode(self, object return_type, FRBuffer *buf):
        if type(self.sub_codec) is ArrayCodec:
            # This is a set of arrays encoded as an array
            # of single-element records.
            return self._decode_array_set(return_type, buf)
        else:
            # Set of non-arrays.
            return self._decode_array(True, return_type, buf)

    cdef inline _decode_array_set(self, object return_type, FRBuffer *buf):
        cdef:
            object result
            object elem
            Py_ssize_t elem_count
            Py_ssize_t recsize
            Py_ssize_t i
            int32_t elem_len
            int32_t ndims = hton.unpack_int32(frb_read(buf, 4))
            BaseCodec sub_codec = <BaseCodec>self.sub_codec
            FRBuffer elem_buf

        frb_read(buf, 4)  # ignore flags
        frb_read(buf, 4)  # ignore reserved

        if ndims == 0:
            # Special case for an empty set.
            return []
        elif ndims > 1:
            raise RuntimeError('expected a two-dimensional array for a '
                               'set of arrays')

        elem_count = <Py_ssize_t><uint32_t>hton.unpack_int32(frb_read(buf, 4))
        frb_read(buf, 4)  # Ignore the lower bound information

        result = cpython.PyList_New(elem_count)
        for i in range(elem_count):
            frb_read(buf, 4)  # ignore array element size

            recsize = <Py_ssize_t><uint32_t>hton.unpack_int32(frb_read(buf, 4))
            if recsize != 1:
                raise RuntimeError(
                    'expected a record with a single element as an array set '
                    'element envelope')

            frb_read(buf, 4)  # reserved

            elem_len = hton.unpack_int32(frb_read(buf, 4))
            if elem_len == -1:
                raise RuntimeError(
                    'unexpected NULL value in array set element ')

            frb_slice_from(&elem_buf, buf, elem_len)
            elem = sub_codec.decode(return_type, &elem_buf)
            if frb_get_len(&elem_buf):
                raise RuntimeError(
                    f'unexpected trailing data in buffer after '
                    f'set element decoding: {frb_get_len(&elem_buf)}')
            cpython.Py_INCREF(elem)
            cpython.PyList_SET_ITEM(result, i, elem)

        return result

    def make_type(self, describe_context):
        return describe.ArrayType(
            desc_id=uuid.UUID(bytes=self.tid),
            name=None,
            element_type=self.sub_codec.make_type(describe_context),
        )
