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


from gel.datatypes import range as range_mod


cdef uint8_t RANGE_EMPTY = 0x01
cdef uint8_t RANGE_LB_INC = 0x02
cdef uint8_t RANGE_UB_INC = 0x04
cdef uint8_t RANGE_LB_INF = 0x08
cdef uint8_t RANGE_UB_INF = 0x10


@cython.final
cdef class RangeCodec(BaseCodec):

    def __cinit__(self):
        self.sub_codec = None

    @staticmethod
    cdef BaseCodec new(bytes tid, BaseCodec sub_codec):
        cdef:
            RangeCodec codec

        codec = RangeCodec.__new__(RangeCodec)

        codec.tid = tid
        codec.name = 'Range'
        codec.sub_codec = sub_codec

        return codec

    @staticmethod
    cdef encode_range(WriteBuffer buf, object obj, BaseCodec sub_codec):
        cdef:
            uint8_t flags = 0
            WriteBuffer sub_data
            object lower = obj.lower
            object upper = obj.upper
            bint inc_lower = obj.inc_lower
            bint inc_upper = obj.inc_upper
            bint empty = obj.is_empty()

        if not isinstance(sub_codec, ScalarCodec):
            raise TypeError(
                'only scalar ranges are supported (got type {!r})'.format(
                    type(sub_codec).__name__
                )
            )

        if empty:
            flags |= RANGE_EMPTY
        else:
            if lower is None:
                flags |= RANGE_LB_INF
            elif inc_lower:
                flags |= RANGE_LB_INC
            if upper is None:
                flags |= RANGE_UB_INF
            elif inc_upper:
                flags |= RANGE_UB_INC

        sub_data = WriteBuffer.new()
        if lower is not None:
            try:
                sub_codec.encode(sub_data, lower)
            except TypeError as e:
                raise ValueError(
                    'invalid range lower bound: {}'.format(
                        e.args[0])) from None
        if upper is not None:
            try:
                sub_codec.encode(sub_data, upper)
            except TypeError as e:
                raise ValueError(
                    'invalid range upper bound: {}'.format(
                        e.args[0])) from None

        buf.write_int32(1 + sub_data.len())
        buf.write_byte(<int8_t>flags)
        buf.write_buffer(sub_data)

    @staticmethod
    cdef decode_range(object return_type, FRBuffer *buf, BaseCodec sub_codec):
        cdef:
            uint8_t flags = <uint8_t>frb_read(buf, 1)[0]
            bint empty = (flags & RANGE_EMPTY) != 0
            bint inc_lower = (flags & RANGE_LB_INC) != 0
            bint inc_upper = (flags & RANGE_UB_INC) != 0
            bint has_lower = (flags & (RANGE_EMPTY | RANGE_LB_INF)) == 0
            bint has_upper = (flags & (RANGE_EMPTY | RANGE_UB_INF)) == 0
            object lower = None
            object upper = None
            int32_t sub_len
            FRBuffer sub_buf

        if has_lower:
            sub_len = hton.unpack_int32(frb_read(buf, 4))
            if sub_len != -1:
                frb_slice_from(&sub_buf, buf, sub_len)
                lower = sub_codec.decode(return_type, &sub_buf)
                if frb_get_len(&sub_buf):
                    raise RuntimeError(
                        f'unexpected trailing data in buffer after '
                        f'range bound decoding: {frb_get_len(&sub_buf)}')

        if has_upper:
            sub_len = hton.unpack_int32(frb_read(buf, 4))
            if sub_len != -1:
                frb_slice_from(&sub_buf, buf, sub_len)
                upper = sub_codec.decode(return_type, &sub_buf)
                if frb_get_len(&sub_buf):
                    raise RuntimeError(
                        f'unexpected trailing data in buffer after '
                        f'range bound decoding: {frb_get_len(&sub_buf)}')

        return range_mod.Range(
            lower,
            upper,
            inc_lower=inc_lower,
            inc_upper=inc_upper,
            empty=empty,
        )

    cdef encode(self, WriteBuffer buf, object obj):
        RangeCodec.encode_range(buf, obj, self.sub_codec)

    cdef decode(self, object return_type, FRBuffer *buf):
        return RangeCodec.decode_range(return_type, buf, self.sub_codec)

    cdef dump(self, int level = 0):
        return f'{level * " "}{self.name}\n{self.sub_codec.dump(level + 1)}'

    def make_type(self, describe_context):
        return describe.RangeType(
            desc_id=uuid.UUID(bytes=self.tid),
            name=self.type_name,
            value_type=self.sub_codec.make_type(describe_context),
        )


@cython.final
cdef class MultiRangeCodec(BaseCodec):

    def __cinit__(self):
        self.sub_codec = None

    @staticmethod
    cdef BaseCodec new(bytes tid, BaseCodec sub_codec):
        cdef:
            MultiRangeCodec codec

        codec = MultiRangeCodec.__new__(MultiRangeCodec)

        codec.tid = tid
        codec.name = 'MultiRange'
        codec.sub_codec = sub_codec

        return codec

    cdef encode(self, WriteBuffer buf, object obj):
        cdef:
            WriteBuffer elem_data
            Py_ssize_t objlen
            Py_ssize_t elem_data_len

        if not isinstance(self.sub_codec, ScalarCodec):
            raise TypeError(
                f'only scalar multiranges are supported (got type '
                f'{type(self.sub_codec).__name__!r})'
            )

        if not _is_array_iterable(obj):
            raise TypeError(
                f'a sized iterable container expected (got type '
                f'{type(obj).__name__!r})'
            )

        objlen = len(obj)
        if objlen > _MAXINT32:
            raise ValueError('too many elements in multirange value')

        elem_data = WriteBuffer.new()
        for item in obj:
            try:
                RangeCodec.encode_range(elem_data, item, self.sub_codec)
            except TypeError as e:
                raise ValueError(
                    f'invalid multirange element: {e.args[0]}') from None

        elem_data_len = elem_data.len()
        if elem_data_len > _MAXINT32 - 4:
            raise OverflowError(
                f'size of encoded multirange datum exceeds the maximum '
                f'allowed {_MAXINT32 - 4} bytes')

        # Datum length
        buf.write_int32(4 + <int32_t>elem_data_len)
        # Number of elements in multirange
        buf.write_int32(<int32_t>objlen)
        buf.write_buffer(elem_data)

    cdef decode(self, object return_type, FRBuffer *buf):
        cdef:
            Py_ssize_t elem_count = <Py_ssize_t><uint32_t>hton.unpack_int32(
                frb_read(buf, 4))
            object result
            Py_ssize_t i
            int32_t elem_len
            FRBuffer elem_buf

        result = cpython.PyList_New(elem_count)
        for i in range(elem_count):
            elem_len = hton.unpack_int32(frb_read(buf, 4))
            if elem_len == -1:
                raise RuntimeError(
                    'unexpected NULL element in multirange value')
            else:
                frb_slice_from(&elem_buf, buf, elem_len)
                elem = RangeCodec.decode_range(
                    return_type, &elem_buf, self.sub_codec
                )
                if frb_get_len(&elem_buf):
                    raise RuntimeError(
                        f'unexpected trailing data in buffer after '
                        f'multirange element decoding: '
                        f'{frb_get_len(&elem_buf)}')

            cpython.Py_INCREF(elem)
            cpython.PyList_SET_ITEM(result, i, elem)

        return range_mod.MultiRange(result)

    cdef dump(self, int level = 0):
        return f'{level * " "}{self.name}\n{self.sub_codec.dump(level + 1)}'

    def make_type(self, describe_context):
        return describe.MultiRangeType(
            desc_id=uuid.UUID(bytes=self.tid),
            name=self.type_name,
            value_type=self.sub_codec.make_type(describe_context),
        )
