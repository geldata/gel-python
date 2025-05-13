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


import codecs

from collections.abc import Mapping as MappingABC


cdef uint64_t RECORD_ENCODER_CHECKED = 1 << 0
cdef uint64_t RECORD_ENCODER_INVALID = 1 << 1

cdef bytes NULL_CODEC_ID = b'\x00' * 16
cdef bytes EMPTY_TUPLE_CODEC_ID = TYPE_IDS.get('empty-tuple').bytes

EMPTY_NULL_DATA = b'\x00\x00\x00\x00'
EMPTY_RECORD_DATA = b'\x00\x00\x00\x04\x00\x00\x00\x00'


cdef class BaseCodec:

    def __init__(self):
        raise RuntimeError(
            'codecs are not supposed to be instantiated directly')

    def __cinit__(self):
        self.tid = None
        self.name = None

    cdef encode(self, WriteBuffer buf, object obj):
        raise NotImplementedError

    cdef decode(self, object return_type, FRBuffer *buf):
        raise NotImplementedError

    cdef dump(self, int level = 0):
        return f'{level * " "}{self.name}'

    def make_type(self, describe_context):
        raise NotImplementedError


cdef class CodecPythonOverride(BaseCodec):

    def __cinit__(self):
        self.codec = None
        self.encoder = None
        self.decoder = None

    cdef encode(self, WriteBuffer buf, object obj):
        self.codec.encode(buf, self.encoder(obj))

    cdef decode(self, object return_type, FRBuffer *buf):
        return self.decoder(self.codec.decode(return_type, buf))

    cdef dump(self, int level = 0):
        return f'{level * " "}<Python override>{self.name}'

    @staticmethod
    cdef BaseCodec new(bytes tid,
                       BaseCodec basecodec,
                       object encoder,
                       object decoder):

        cdef:
            CodecPythonOverride codec

        codec = CodecPythonOverride.__new__(CodecPythonOverride)
        codec.tid = tid
        codec.name = basecodec.name
        codec.codec = basecodec
        codec.encoder = encoder
        codec.decoder = decoder
        return codec

    def make_type(self, describe_context):
        return self.codec.make_type(describe_context)


cdef class EmptyTupleCodec(BaseCodec):

    def __cinit__(self):
        self.tid = EMPTY_TUPLE_CODEC_ID
        self.name = 'no-input'
        self.empty_tup = None

    cdef encode(self, WriteBuffer buf, object obj):
        if type(obj) is not tuple:
            raise RuntimeError(
                f'cannot encode empty Tuple: expected a tuple, '
                f'got {type(obj).__name__}')
        if len(obj) != 0:
            raise RuntimeError(
                f'cannot encode empty Tuple: expected 0 elements, '
                f'got {len(obj)}')
        buf.write_bytes(EMPTY_RECORD_DATA)

    cdef decode(self, object return_type, FRBuffer *buf):
        elem_count = <Py_ssize_t><uint32_t>hton.unpack_int32(frb_read(buf, 4))
        if elem_count != 0:
            raise RuntimeError(
                f'cannot decode empty Tuple: expected 0 elements, '
                f'got {elem_count}')

        if self.empty_tup is None:
            self.empty_tup = cpython.PyTuple_New(0)
        return self.empty_tup

    def make_type(self, describe_context):
        return describe.TupleType(
            desc_id=uuid.UUID(bytes=self.tid),
            name=None,
            element_types=()
        )


cdef class NullCodec(BaseCodec):

    def __cinit__(self):
        self.tid = NULL_CODEC_ID
        self.name = 'null-codec'

    def make_type(self, describe_context):
        return None


cdef class BaseRecordCodec(BaseCodec):

    def __cinit__(self):
        self.fields_codecs = ()
        self.encoder_flags = 0

    cdef _check_encoder(self):
        if not (self.encoder_flags & RECORD_ENCODER_CHECKED):
            for codec in self.fields_codecs:
                if not isinstance(
                    codec,
                    (ScalarCodec, ArrayCodec, TupleCodec, NamedTupleCodec,
                     EnumCodec, RangeCodec, MultiRangeCodec),
                ):
                    self.encoder_flags |= RECORD_ENCODER_INVALID
                    break
            self.encoder_flags |= RECORD_ENCODER_CHECKED

        if self.encoder_flags & RECORD_ENCODER_INVALID:
            raise TypeError(
                'argument tuples do not support objects')

    cdef encode(self, WriteBuffer buf, object obj):
        cdef:
            WriteBuffer elem_data
            Py_ssize_t objlen
            Py_ssize_t i
            BaseCodec sub_codec

        self._check_encoder()

        if not _is_array_iterable(obj):
            raise TypeError(
                'a sized iterable container expected (got type {!r})'.format(
                    type(obj).__name__))

        objlen = len(obj)
        if objlen == 0:
            buf.write_bytes(EMPTY_RECORD_DATA)
            return

        if objlen > _MAXINT32:
            raise ValueError('too many elements for a tuple')

        if objlen != len(self.fields_codecs):
            raise ValueError(
                f'expected {len(self.fields_codecs)} elements in the tuple, '
                f'got {objlen}')

        elem_data = WriteBuffer.new()
        for i in range(objlen):
            item = obj[i]
            elem_data.write_int32(0)  # reserved bytes
            if item is None:
                elem_data.write_int32(-1)
            else:
                sub_codec = <BaseCodec>(self.fields_codecs[i])
                try:
                    sub_codec.encode(elem_data, item)
                except (TypeError, ValueError) as e:
                    value_repr = repr(item)
                    if len(value_repr) > 40:
                        value_repr = value_repr[:40] + '...'
                    raise errors.InvalidArgumentError(
                        'invalid input for query argument'
                        ' ${n}: {v} ({msg})'.format(
                            n=i, v=value_repr, msg=e)) from e

        buf.write_int32(4 + elem_data.len())  # buffer length
        buf.write_int32(<int32_t><uint32_t>objlen)
        buf.write_buffer(elem_data)


cdef class BaseNamedRecordCodec(BaseRecordCodec):

    def __cinit__(self):
        self.descriptor = None

    cdef dump(self, int level = 0):
        buf = [f'{level * " "}{self.name}']
        for pos, codec in enumerate(self.fields_codecs):
            name = datatypes.record_desc_pointer_name(self.descriptor, pos)
            buf.append('{}{} := {}'.format(
                (level + 1) * " ",
                name,
                (<BaseCodec>codec).dump(level + 1).strip()))
        return '\n'.join(buf)

    cdef encode(self, WriteBuffer buf, object obj):
        cdef:
            WriteBuffer elem_data
            Py_ssize_t objlen
            Py_ssize_t i
            BaseCodec sub_codec
            Py_ssize_t is_dict
            Py_ssize_t is_namedtuple

        self._check_encoder()

        # We check in this order (dict, _is_array_iterable,
        # MappingABC) so that in the common case of dict or tuple, we
        # never do an ABC check.
        if cpython.PyDict_Check(obj):
            is_dict = True
        elif _is_array_iterable(obj):
            is_dict = False
        elif isinstance(obj, MappingABC):
            is_dict = True
        else:
            raise TypeError(
                'a sized iterable container or mapping '
                'expected (got type {!r})'.format(
                    type(obj).__name__))
        is_namedtuple = not is_dict and hasattr(obj, '_fields')

        objlen = len(obj)
        if objlen == 0:
            buf.write_bytes(EMPTY_RECORD_DATA)
            return

        if objlen > _MAXINT32:
            raise ValueError('too many elements for a tuple')

        if objlen != len(self.fields_codecs):
            raise ValueError(
                f'expected {len(self.fields_codecs)} elements in the tuple, '
                f'got {objlen}')

        elem_data = WriteBuffer.new()
        for i in range(objlen):
            if is_dict:
                name = datatypes.record_desc_pointer_name(self.descriptor, i)
                try:
                    item = obj[name]
                except KeyError:
                    raise ValueError(
                        f"named tuple dict is missing '{name}' key",
                    ) from None
            elif is_namedtuple:
                name = datatypes.record_desc_pointer_name(self.descriptor, i)
                try:
                    item = getattr(obj, name)
                except AttributeError:
                    raise ValueError(
                        f"named tuple is missing '{name}' attribute",
                    ) from None
            else:
                item = obj[i]

            elem_data.write_int32(0)  # reserved bytes
            if item is None:
                elem_data.write_int32(-1)
            else:
                sub_codec = <BaseCodec>(self.fields_codecs[i])
                try:
                    sub_codec.encode(elem_data, item)
                except (TypeError, ValueError) as e:
                    value_repr = repr(item)
                    if len(value_repr) > 40:
                        value_repr = value_repr[:40] + '...'
                    raise errors.InvalidArgumentError(
                        'invalid input for query argument'
                        ' ${n}: {v} ({msg})'.format(
                            n=i, v=value_repr, msg=e)) from e

        buf.write_int32(4 + elem_data.len())  # buffer length
        buf.write_int32(<int32_t><uint32_t>objlen)
        buf.write_buffer(elem_data)

@cython.final
cdef class EdegDBCodecContext(pgproto.CodecContext):

    def __cinit__(self):
        self._codec = codecs.lookup('utf-8')

    cpdef get_text_codec(self):
        return self._codec

    cdef is_encoding_utf8(self):
        return True


cdef EdegDBCodecContext DEFAULT_CODEC_CONTEXT = EdegDBCodecContext()
