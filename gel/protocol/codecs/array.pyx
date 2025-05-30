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


from collections.abc import (Iterable as IterableABC,
                             Mapping as MappingABC,
                             Sized as SizedABC)


cdef class BaseArrayCodec(BaseCodec):

    # Base codec for arrays & sets.

    def __cinit__(self):
        self.sub_codec = None
        self.cardinality = -1

    cdef encode(self, WriteBuffer buf, object obj):
        cdef:
            WriteBuffer elem_data
            WriteBuffer tuple_elem_data
            int32_t ndims = 1
            Py_ssize_t objlen
            Py_ssize_t i

        if not isinstance(
            self.sub_codec,
            (
                ScalarCodec,
                TupleCodec,
                NamedTupleCodec,
                EnumCodec,
                RangeCodec,
                MultiRangeCodec,
                ArrayCodec,
            )
        ):
            raise TypeError(
                'only arrays of scalars are supported (got type {!r})'.format(
                    type(self.sub_codec).__name__
                )
            )

        if not _is_array_iterable(obj):
            raise TypeError(
                'a sized iterable container expected (got type {!r})'.format(
                    type(obj).__name__))

        objlen = len(obj)
        if objlen > _MAXINT32:
            raise ValueError('too many elements in array value')

        elem_data = WriteBuffer.new()
        for i in range(objlen):
            item = obj[i]
            if item is None:
                raise ValueError(
                    "invalid array element at index {}: "
                    "None is not allowed".format(i)
                )
            else:
                try:
                    if isinstance(self.sub_codec, ArrayCodec):
                        # This is an array of array.
                        # Wrap the inner array with a tuple.
                        tuple_elem_data = WriteBuffer.new()
                        self.sub_codec.encode(tuple_elem_data, item)

                        elem_data.write_int32(4 + 4 + tuple_elem_data.len()) # buffer length
                        elem_data.write_int32(1) # tuple_elem_count
                        elem_data.write_int32(0) # reserved
                        elem_data.write_buffer(tuple_elem_data)

                    else:
                        self.sub_codec.encode(elem_data, item)

                except TypeError as e:
                    raise ValueError(
                        'invalid array element: {}'.format(
                            e.args[0])) from None

        buf.write_int32(12 + 8 * ndims + elem_data.len())  # buffer length
        buf.write_int32(ndims)  # number of dimensions
        buf.write_int32(0)  # flags
        buf.write_int32(0)  # reserved

        buf.write_int32(<int32_t>objlen)
        buf.write_int32(1)

        buf.write_buffer(elem_data)

    cdef decode(self, FRBuffer *buf):
        return self._decode_array(buf)

    cdef inline _decode_array(self, FRBuffer *buf):
        cdef:
            Py_ssize_t elem_count
            int32_t ndims = hton.unpack_int32(frb_read(buf, 4))
            object result
            Py_ssize_t i
            int32_t elem_len
            FRBuffer elem_buf

        frb_read(buf, 4)  # ignore flags
        frb_read(buf, 4)  # reserved

        if ndims > 1:
            raise RuntimeError('only 1-dimensional arrays are supported')

        if ndims == 0:
            return []

        assert ndims == 1

        elem_count = <Py_ssize_t><uint32_t>hton.unpack_int32(frb_read(buf, 4))
        if self.cardinality != -1 and elem_count != self.cardinality:
            raise ValueError(
                f'invalid array size: received {elem_count}, '
                f'expected {self.cardinality}'
            )

        frb_read(buf, 4)  # Ignore the lower bound information

        result = cpython.PyList_New(elem_count)
        for i in range(elem_count):
            elem_len = hton.unpack_int32(frb_read(buf, 4))
            if elem_len == -1:
                elem = None
            else:

                if isinstance(self.sub_codec, ArrayCodec):
                    # This is an array of array
                    # Unwrap the tuple from the inner array.
                    tuple_elem_count = <Py_ssize_t><uint32_t>hton.unpack_int32(frb_read(buf, 4))
                    if tuple_elem_count != 1:
                        raise RuntimeError(
                            f'cannot decode inner array: expected 1 '
                            f'element, got {tuple_elem_count}')

                    frb_read(buf, 4)  # reserved
                    tuple_elem_len = hton.unpack_int32(frb_read(buf, 4))

                    elem = self.sub_codec.decode(
                        frb_slice_from(&elem_buf, buf, tuple_elem_len)
                    )

                else:
                    frb_slice_from(&elem_buf, buf, elem_len)
                    elem = self.sub_codec.decode(&elem_buf)

                if frb_get_len(&elem_buf):
                    raise RuntimeError(
                        f'unexpected trailing data in buffer after '
                        f'array element decoding: {frb_get_len(&elem_buf)}')

            cpython.Py_INCREF(elem)
            cpython.PyList_SET_ITEM(result, i, elem)

        return result

    cdef dump(self, int level = 0):
        return f'{level * " "}{self.name}\n{self.sub_codec.dump(level + 1)}'


@cython.final
cdef class ArrayCodec(BaseArrayCodec):

    @staticmethod
    cdef BaseCodec new(bytes tid, BaseCodec sub_codec, int32_t cardinality):
        cdef:
            ArrayCodec codec

        codec = ArrayCodec.__new__(ArrayCodec)

        codec.tid = tid
        codec.name = 'Array'
        codec.sub_codec = sub_codec
        codec.cardinality = cardinality

        return codec

    def make_type(self, describe_context):
        return describe.ArrayType(
            desc_id=uuid.UUID(bytes=self.tid),
            name=self.type_name,
            element_type=self.sub_codec.make_type(describe_context),
        )


cdef inline bint _is_trivial_container(object obj):
    return cpython.PyUnicode_Check(obj) or cpython.PyBytes_Check(obj) or \
            cpythonx.PyByteArray_Check(obj) or cpythonx.PyMemoryView_Check(obj)


cdef inline _is_array_iterable(object obj):
    return (
        cpython.PyTuple_Check(obj) or
        cpython.PyList_Check(obj) or
        (
            isinstance(obj, IterableABC) and
            isinstance(obj, SizedABC) and
            not _is_trivial_container(obj) and
            not isinstance(obj, MappingABC)
        )
    )
