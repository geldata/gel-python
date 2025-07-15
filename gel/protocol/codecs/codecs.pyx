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


import array
import inspect
import decimal
import uuid
import datetime
from gel import describe
from gel import enums
from gel.datatypes import datatypes

from libc.string cimport memcpy
from cpython.bytes cimport PyBytes_FromStringAndSize

from gel._internal import _tracked_list
cdef DLIST_READ_WRITE = _tracked_list.Mode.ReadWrite

include "./edb_types.pxi"


include "./base.pyx"
include "./scalar.pyx"
include "./tuple.pyx"
include "./namedtuple.pyx"
include "./object.pyx"
include "./array.pyx"
include "./range.pyx"
include "./set.pyx"
include "./enum.pyx"
include "./record.pyx"


DEF CTYPE_SET = 0
DEF CTYPE_SHAPE = 1
DEF CTYPE_BASE_SCALAR = 2
DEF CTYPE_SCALAR = 3
DEF CTYPE_TUPLE = 4
DEF CTYPE_NAMEDTUPLE = 5
DEF CTYPE_ARRAY = 6
DEF CTYPE_ENUM = 7
DEF CTYPE_INPUT_SHAPE = 8
DEF CTYPE_RANGE = 9
DEF CTYPE_OBJECT = 10
DEF CTYPE_COMPOUND = 11
DEF CTYPE_MULTIRANGE = 12
DEF CTYPE_SQL_ROW = 13
DEF CTYPE_ANNO_TYPENAME = 255

DEF _CODECS_BUILD_CACHE_SIZE = 200

DEF NBASE = 10000
DEF NUMERIC_POS = 0x0000
DEF NUMERIC_NEG = 0x4000

cdef BaseCodec NULL_CODEC = NullCodec.__new__(NullCodec)
cdef BaseCodec EMPTY_TUPLE_CODEC = EmptyTupleCodec.__new__(EmptyTupleCodec)


cdef class CodecsRegistry:

    def __init__(self, *, cache_size=1000):
        self.codecs_build_cache = LRUMapping(maxsize=_CODECS_BUILD_CACHE_SIZE)
        self.codecs = LRUMapping(maxsize=cache_size)
        self.base_codec_overrides = {}

    def clear_cache(self):
        self.codecs.clear()
        self.codecs_build_cache.clear()

    def set_type_codec(self, typeid, *, encoder, decoder, format):
        if format != 'python':
            raise ValueError('"python" is the only valid format')
        if not isinstance(typeid, uuid.UUID):
            raise TypeError('typeid must be a UUID')
        basecodec = BASE_SCALAR_CODECS.get(typeid.bytes)
        if basecodec is None:
            raise ValueError(
                f'{typeid} does not correspond to any known base type')
        self.base_codec_overrides[typeid.bytes] = CodecPythonOverride.new(
            typeid.bytes,
            basecodec,
            encoder,
            decoder,
        )

    cdef BaseCodec _build_codec(self, FRBuffer *spec, list codecs_list,
                                protocol_version):
        cdef:
            uint32_t desc_len = 0
            uint8_t t
            bytes tid
            uint16_t els
            uint16_t i
            uint32_t str_len
            uint16_t pos
            int32_t dim_len
            BaseCodec res
            BaseCodec sub_codec

        if protocol_version >= (2, 0):
            desc_len = frb_get_len(spec) - 16 - 1

        t = <uint8_t>(frb_read(spec, 1)[0])
        tid = frb_read(spec, 16)[:16]

        res = self.codecs.get(tid, None)
        if res is None:
            res = self.codecs_build_cache.get(tid, None)
        if res is not None:
            # We have a codec for this "tid"; advance the buffer
            # so that we can process the next codec.
            if desc_len > 0:
                frb_read(spec, desc_len)
                return res

            if t == CTYPE_SET:
                frb_read(spec, 2)

            elif t == CTYPE_SHAPE or t == CTYPE_INPUT_SHAPE:
                els = <uint16_t>hton.unpack_int16(frb_read(spec, 2))
                for i in range(els):
                    frb_read(spec, 4)  # flags
                    frb_read(spec, 1)  # cardinality
                    str_len = hton.unpack_uint32(frb_read(spec, 4))
                    # read the <str> (`str_len` bytes) and <pos> (2 bytes)
                    frb_read(spec, str_len + 2)

            elif t == CTYPE_BASE_SCALAR:
                pass

            elif t == CTYPE_SCALAR:
                frb_read(spec, 2)

            elif t == CTYPE_TUPLE:
                # tuple
                els = <uint16_t>hton.unpack_int16(frb_read(spec, 2))
                for i in range(els):
                    frb_read(spec, 2)

            elif t == CTYPE_NAMEDTUPLE:
                els = <uint16_t>hton.unpack_int16(frb_read(spec, 2))
                for i in range(els):
                    str_len = hton.unpack_uint32(frb_read(spec, 4))
                    frb_read(spec, str_len + 2)

            elif t == CTYPE_ARRAY:
                frb_read(spec, 2)
                els = <uint16_t>hton.unpack_int16(frb_read(spec, 2))
                if els != 1:
                    raise NotImplementedError(
                        'cannot handle arrays with more than one dimension')
                # First dimension length.
                frb_read(spec, 4)

            elif t == CTYPE_RANGE:
                frb_read(spec, 2)

            elif t == CTYPE_MULTIRANGE:
                frb_read(spec, 2)

            elif t == CTYPE_SQL_ROW:
                els = <uint16_t>hton.unpack_int16(frb_read(spec, 2))
                for i in range(els):
                    str_len = hton.unpack_uint32(frb_read(spec, 4))
                    frb_read(spec, str_len + 2)

            elif t == CTYPE_ENUM:
                els = <uint16_t>hton.unpack_int16(frb_read(spec, 2))
                for i in range(els):
                    str_len = hton.unpack_uint32(frb_read(spec, 4))
                    frb_read(spec, str_len)

            elif t == CTYPE_ANNO_TYPENAME:
                str_len = hton.unpack_uint32(frb_read(spec, 4))
                res.type_name = cpythonx.PyUnicode_FromStringAndSize(
                    frb_read(spec, str_len), str_len)
                return None

            elif 0x80 & t == 0x80:
                # Ignore all other type annotations.
                str_len = hton.unpack_uint32(frb_read(spec, 4))
                frb_read(spec, str_len)
                return None

            else:
                raise NotImplementedError(
                    f'no codec implementation for EdgeDB data class {t}')

            return res

        if t == CTYPE_SET:
            pos = <uint16_t>hton.unpack_int16(frb_read(spec, 2))
            sub_codec = <BaseCodec>codecs_list[pos]
            res = SetCodec.new(tid, sub_codec)

        elif t == CTYPE_SHAPE:
            if protocol_version >= (2, 0):
                ephemeral_free_shape = <bint>frb_read(spec, 1)[0]
                objtype_pos = <uint16_t>hton.unpack_int16(frb_read(spec, 2))

            els = <uint16_t>hton.unpack_int16(frb_read(spec, 2))
            codecs = cpython.PyTuple_New(els)
            names = cpython.PyTuple_New(els)
            flags = cpython.PyTuple_New(els)
            cards = cpython.PyTuple_New(els)
            source_types = cpython.PyTuple_New(els)
            for i in range(els):
                flag = hton.unpack_uint32(frb_read(spec, 4))  # flags
                cardinality = <uint8_t>frb_read(spec, 1)[0]

                str_len = hton.unpack_uint32(frb_read(spec, 4))
                name = cpythonx.PyUnicode_FromStringAndSize(
                    frb_read(spec, str_len), str_len)
                pos = <uint16_t>hton.unpack_int16(frb_read(spec, 2))

                if flag & datatypes._EDGE_POINTER_IS_LINKPROP:
                    name = "@" + name
                cpython.Py_INCREF(name)
                cpython.PyTuple_SetItem(names, i, name)

                sub_codec = codecs_list[pos]
                cpython.Py_INCREF(sub_codec)
                cpython.PyTuple_SetItem(codecs, i, sub_codec)

                cpython.Py_INCREF(flag)
                cpython.PyTuple_SetItem(flags, i, flag)

                cpython.Py_INCREF(cardinality)
                cpython.PyTuple_SetItem(cards, i, cardinality)

                if protocol_version >= (2, 0):
                    source_type_pos = <uint16_t>hton.unpack_int16(
                        frb_read(spec, 2))
                    source_type = codecs_list[source_type_pos]
                    cpython.Py_INCREF(source_type)
                    cpython.PyTuple_SetItem(source_types, i, source_type)
                else:
                    cpython.Py_INCREF(None)
                    cpython.PyTuple_SetItem(source_types, i, None)

            res = ObjectCodec.new(
                tid, names, flags, cards, codecs, source_types,
                t == CTYPE_INPUT_SHAPE,
            )

        elif t == CTYPE_INPUT_SHAPE:
            els = <uint16_t>hton.unpack_int16(frb_read(spec, 2))
            codecs = cpython.PyTuple_New(els)
            names = cpython.PyTuple_New(els)
            flags = cpython.PyTuple_New(els)
            cards = cpython.PyTuple_New(els)
            for i in range(els):
                flag = hton.unpack_uint32(frb_read(spec, 4))  # flags
                cardinality = <uint8_t>frb_read(spec, 1)[0]

                str_len = hton.unpack_uint32(frb_read(spec, 4))
                name = cpythonx.PyUnicode_FromStringAndSize(
                    frb_read(spec, str_len), str_len)
                pos = <uint16_t>hton.unpack_int16(frb_read(spec, 2))

                if flag & datatypes._EDGE_POINTER_IS_LINKPROP:
                    name = "@" + name
                cpython.Py_INCREF(name)
                cpython.PyTuple_SetItem(names, i, name)

                sub_codec = codecs_list[pos]
                cpython.Py_INCREF(sub_codec)
                cpython.PyTuple_SetItem(codecs, i, sub_codec)

                cpython.Py_INCREF(flag)
                cpython.PyTuple_SetItem(flags, i, flag)

                cpython.Py_INCREF(cardinality)
                cpython.PyTuple_SetItem(cards, i, cardinality)

            res = ObjectCodec.new(
                tid, names, flags, cards, codecs, None,
                t == CTYPE_INPUT_SHAPE,
            )

        elif t == CTYPE_BASE_SCALAR:
            if tid in self.base_codec_overrides:
                return self.base_codec_overrides[tid]
            else:
                res = <BaseCodec>BASE_SCALAR_CODECS[tid]

        elif t == CTYPE_SCALAR:
            if protocol_version >= (2, 0):
                str_len = hton.unpack_uint32(frb_read(spec, 4))
                type_name = cpythonx.PyUnicode_FromStringAndSize(
                    frb_read(spec, str_len), str_len)
                schema_defined = <bint>frb_read(spec, 1)[0]

                ancestor_count = <uint16_t>hton.unpack_int16(frb_read(spec, 2))
                ancestors = []
                for _ in range(ancestor_count):
                    ancestor_pos = <uint16_t>hton.unpack_int16(
                        frb_read(spec, 2))
                    ancestor_codec = codecs_list[ancestor_pos]
                    if type(ancestor_codec) is not ScalarCodec:
                        raise RuntimeError(
                            f'a scalar codec expected for base scalar type, '
                            f'got {type(ancestor_codec).__name__}')
                    ancestors.append(ancestor_codec)

                if ancestor_count == 0:
                    if tid in self.base_codec_overrides:
                        res = self.base_codec_overrides[tid]
                    else:
                        res = <BaseCodec>BASE_SCALAR_CODECS[tid]
                else:
                    fundamental_codec = ancestors[-1]
                    if type(fundamental_codec) is not ScalarCodec:
                        raise RuntimeError(
                            f'a scalar codec expected for base scalar type, '
                            f'got {type(fundamental_codec).__name__}')
                    res = (<ScalarCodec>fundamental_codec).derive(tid)
                res.type_name = type_name
            else:
                fundamental_pos = <uint16_t>hton.unpack_int16(
                    frb_read(spec, 2))
                fundamental_codec = codecs_list[fundamental_pos]
                if type(fundamental_codec) is not ScalarCodec:
                    raise RuntimeError(
                        f'a scalar codec expected for base scalar type, '
                        f'got {type(fundamental_codec).__name__}')
                res = (<ScalarCodec>fundamental_codec).derive(tid)

        elif t == CTYPE_TUPLE:
            if protocol_version >= (2, 0):
                str_len = hton.unpack_uint32(frb_read(spec, 4))
                type_name = cpythonx.PyUnicode_FromStringAndSize(
                    frb_read(spec, str_len), str_len)
                schema_defined = <bint>frb_read(spec, 1)[0]
                ancestor_count = <uint16_t>hton.unpack_int16(frb_read(spec, 2))
                for _ in range(ancestor_count):
                    ancestor_pos = <uint16_t>hton.unpack_int16(
                        frb_read(spec, 2))
                    ancestor_codec = codecs_list[ancestor_pos]
            else:
                type_name = None
            els = <uint16_t>hton.unpack_int16(frb_read(spec, 2))
            codecs = cpython.PyTuple_New(els)
            for i in range(els):
                pos = <uint16_t>hton.unpack_int16(frb_read(spec, 2))

                sub_codec = codecs_list[pos]
                cpython.Py_INCREF(sub_codec)
                cpython.PyTuple_SetItem(codecs, i, sub_codec)

            res = TupleCodec.new(tid, codecs)
            res.type_name = type_name

        elif t == CTYPE_NAMEDTUPLE:
            if protocol_version >= (2, 0):
                str_len = hton.unpack_uint32(frb_read(spec, 4))
                type_name = cpythonx.PyUnicode_FromStringAndSize(
                    frb_read(spec, str_len), str_len)
                schema_defined = <bint>frb_read(spec, 1)[0]
                ancestor_count = <uint16_t>hton.unpack_int16(frb_read(spec, 2))
                for _ in range(ancestor_count):
                    ancestor_pos = <uint16_t>hton.unpack_int16(
                        frb_read(spec, 2))
                    ancestor_codec = codecs_list[ancestor_pos]
            else:
                type_name = None
            els = <uint16_t>hton.unpack_int16(frb_read(spec, 2))
            codecs = cpython.PyTuple_New(els)
            names = cpython.PyTuple_New(els)
            for i in range(els):
                str_len = hton.unpack_uint32(frb_read(spec, 4))
                name = cpythonx.PyUnicode_FromStringAndSize(
                    frb_read(spec, str_len), str_len)
                pos = <uint16_t>hton.unpack_int16(frb_read(spec, 2))

                cpython.Py_INCREF(name)
                cpython.PyTuple_SetItem(names, i, name)

                sub_codec = codecs_list[pos]
                cpython.Py_INCREF(sub_codec)
                cpython.PyTuple_SetItem(codecs, i, sub_codec)

            res = NamedTupleCodec.new(tid, names, codecs)
            res.type_name = type_name

        elif t == CTYPE_SQL_ROW:
            els = <uint16_t>hton.unpack_int16(frb_read(spec, 2))
            codecs = cpython.PyTuple_New(els)
            names = cpython.PyTuple_New(els)
            for i in range(els):
                str_len = hton.unpack_uint32(frb_read(spec, 4))
                name = cpythonx.PyUnicode_FromStringAndSize(
                    frb_read(spec, str_len), str_len)
                pos = <uint16_t>hton.unpack_int16(frb_read(spec, 2))

                cpython.Py_INCREF(name)
                cpython.PyTuple_SetItem(names, i, name)

                sub_codec = codecs_list[pos]
                cpython.Py_INCREF(sub_codec)
                cpython.PyTuple_SetItem(codecs, i, sub_codec)

            res = RecordCodec.new(tid, names, codecs)

        elif t == CTYPE_ENUM:
            if protocol_version >= (2, 0):
                str_len = hton.unpack_uint32(frb_read(spec, 4))
                type_name = cpythonx.PyUnicode_FromStringAndSize(
                    frb_read(spec, str_len), str_len)
                schema_defined = <bint>frb_read(spec, 1)[0]
                ancestor_count = <uint16_t>hton.unpack_int16(frb_read(spec, 2))
                for _ in range(ancestor_count):
                    ancestor_pos = <uint16_t>hton.unpack_int16(
                        frb_read(spec, 2))
                    ancestor_codec = codecs_list[ancestor_pos]
            else:
                type_name = None
            els = <uint16_t>hton.unpack_int16(frb_read(spec, 2))
            names = cpython.PyTuple_New(els)
            for i in range(els):
                str_len = hton.unpack_uint32(frb_read(spec, 4))
                name = cpythonx.PyUnicode_FromStringAndSize(
                    frb_read(spec, str_len), str_len)

                cpython.Py_INCREF(name)
                cpython.PyTuple_SetItem(names, i, name)

            res = EnumCodec.new(tid, names)
            res.type_name = type_name

        elif t == CTYPE_ARRAY:
            if protocol_version >= (2, 0):
                str_len = hton.unpack_uint32(frb_read(spec, 4))
                type_name = cpythonx.PyUnicode_FromStringAndSize(
                    frb_read(spec, str_len), str_len)
                schema_defined = <bint>frb_read(spec, 1)[0]
                ancestor_count = <uint16_t>hton.unpack_int16(frb_read(spec, 2))
                for _ in range(ancestor_count):
                    ancestor_pos = <uint16_t>hton.unpack_int16(
                        frb_read(spec, 2))
                    ancestor_codec = codecs_list[ancestor_pos]
            else:
                type_name = None
            pos = <uint16_t>hton.unpack_int16(frb_read(spec, 2))
            els = <uint16_t>hton.unpack_int16(frb_read(spec, 2))
            if els != 1:
                raise NotImplementedError(
                    'cannot handle arrays with more than one dimension')
            # First dimension length.
            dim_len = hton.unpack_int32(frb_read(spec, 4))
            sub_codec = <BaseCodec>codecs_list[pos]
            res = ArrayCodec.new(tid, sub_codec, dim_len)
            res.type_name = type_name

        elif t == CTYPE_RANGE:
            if protocol_version >= (2, 0):
                str_len = hton.unpack_uint32(frb_read(spec, 4))
                type_name = cpythonx.PyUnicode_FromStringAndSize(
                    frb_read(spec, str_len), str_len)
                schema_defined = <bint>frb_read(spec, 1)[0]
                ancestor_count = <uint16_t>hton.unpack_int16(frb_read(spec, 2))
                for _ in range(ancestor_count):
                    ancestor_pos = <uint16_t>hton.unpack_int16(
                        frb_read(spec, 2))
                    ancestor_codec = codecs_list[ancestor_pos]
            else:
                type_name = None
            pos = <uint16_t>hton.unpack_int16(frb_read(spec, 2))
            sub_codec = <BaseCodec>codecs_list[pos]
            res = RangeCodec.new(tid, sub_codec)
            res.type_name = type_name

        elif t == CTYPE_MULTIRANGE:
            if protocol_version >= (2, 0):
                str_len = hton.unpack_uint32(frb_read(spec, 4))
                type_name = cpythonx.PyUnicode_FromStringAndSize(
                    frb_read(spec, str_len), str_len)
                schema_defined = <bint>frb_read(spec, 1)[0]
                ancestor_count = <uint16_t>hton.unpack_int16(frb_read(spec, 2))
                for _ in range(ancestor_count):
                    ancestor_pos = <uint16_t>hton.unpack_int16(
                        frb_read(spec, 2))
                    ancestor_codec = codecs_list[ancestor_pos]
            else:
                type_name = None
            pos = <uint16_t>hton.unpack_int16(frb_read(spec, 2))
            sub_codec = <BaseCodec>codecs_list[pos]
            res = MultiRangeCodec.new(tid, sub_codec)
            res.type_name = type_name

        elif t == CTYPE_OBJECT and protocol_version >= (2, 0):
            str_len = hton.unpack_uint32(frb_read(spec, 4))
            name = cpythonx.PyUnicode_FromStringAndSize(
                frb_read(spec, str_len), str_len)
            schema_defined = <bint>frb_read(spec, 1)[0]
            res = ObjectTypeNullCodec.new(tid, name, schema_defined)

        elif t == CTYPE_COMPOUND and protocol_version >= (2, 0):
            str_len = hton.unpack_uint32(frb_read(spec, 4))
            name = cpythonx.PyUnicode_FromStringAndSize(
                frb_read(spec, str_len), str_len)
            schema_defined = <bint>frb_read(spec, 1)[0]
            op = <uint8_t>frb_read(spec, 1)[0]
            els = <uint16_t>hton.unpack_int16(frb_read(spec, 2))
            components = cpython.PyTuple_New(els)
            for i in range(els):
                cmp_pos = <uint16_t>hton.unpack_int16(
                    frb_read(spec, 2))
                cmp = codecs_list[cmp_pos]
                cpython.Py_INCREF(cmp)
                cpython.PyTuple_SetItem(components, i, cmp)
            res = CompoundTypeNullCodec.new(
                tid, name, schema_defined, op, components)

        else:
            raise NotImplementedError(
                f'no codec implementation for EdgeDB data class {t}')

        self.codecs_build_cache[tid] = res
        return res

    cdef has_codec(self, bytes type_id):
        return (
            type_id in self.codecs or
            type_id in {NULL_CODEC_ID, EMPTY_TUPLE_CODEC_ID}
        )

    cdef BaseCodec get_codec(self, bytes type_id):
        try:
            return <BaseCodec>self.codecs[type_id]
        except KeyError:
            pass

        if type_id == NULL_CODEC_ID:
            return NULL_CODEC

        if type_id == EMPTY_TUPLE_CODEC_ID:
            return EMPTY_TUPLE_CODEC

        raise LookupError

    cdef BaseCodec build_codec(self, bytes spec, protocol_version):
        cdef:
            FRBuffer buf
            FRBuffer elem_buf
            BaseCodec res
            list codecs_list

        frb_init(
            &buf,
            cpython.PyBytes_AsString(spec),
            cpython.Py_SIZE(spec))

        codecs_list = []
        while frb_get_len(&buf):
            if protocol_version >= (2, 0):
                desc_len = <uint32_t>hton.unpack_int32(frb_read(&buf, 4))
                frb_slice_from(&elem_buf, &buf, desc_len)
                res = self._build_codec(
                    &elem_buf, codecs_list, protocol_version)
                if frb_get_len(&elem_buf):
                    raise RuntimeError(
                        f'unexpected trailing data in type descriptor datum')
            else:
                res = self._build_codec(&buf, codecs_list, protocol_version)
            if res is None:
                # An annotation; ignore.
                continue
            codecs_list.append(res)
            self.codecs[res.tid] = res

        if not codecs_list:
            raise RuntimeError(f'cannot not build codec; empty type desc')

        return codecs_list[-1]


cdef dict BASE_SCALAR_CODECS = {}


cdef register_base_scalar_codec(
        str name,
        pgproto.encode_func encoder,
        pgproto.decode_func decoder,
        object tid = None):

    cdef:
        BaseCodec codec

    if tid is None:
        tid = TYPE_IDS.get(name)
        if tid is None:
            raise RuntimeError(f'cannot find known ID for type {name!r}')
    tid = tid.bytes

    if tid in BASE_SCALAR_CODECS:
        raise RuntimeError(f'base scalar codec for {id} is already registered')

    codec = ScalarCodec.new(tid, name, encoder, decoder)
    BASE_SCALAR_CODECS[tid] = codec


cdef time_encode(pgproto.CodecContext settings, WriteBuffer buf, obj):
    if getattr(obj, 'tzinfo', None) is not None:
        raise TypeError(
            f'a naive time object (tzinfo is None) was expected')

    pgproto.time_encode(settings, buf, obj)


cdef date_encode(pgproto.CodecContext settings, WriteBuffer buf, obj):
    # Python `datetime.date` object does not have a `tzinfo` attribute
    # nor it is timezone-aware.  But since we're accepting duck types
    # let's ensure it doesn't have tzinfo anyways.
    if getattr(obj, 'tzinfo', None) is not None:
        raise TypeError(
            f'a naive date object (tzinfo is None) was expected')

    pgproto.date_encode(settings, buf, obj)


cdef timestamp_encode(pgproto.CodecContext settings, WriteBuffer buf, obj):
    if not cpython.datetime.PyDateTime_Check(obj):
        raise TypeError(
            f'a datetime.datetime object was expected, got {obj!r}')

    if getattr(obj, 'tzinfo', None) is not None:
        raise TypeError(
            f'a naive datetime object (tzinfo is None) was expected')

    pgproto.timestamp_encode(settings, buf, obj)


cdef timestamptz_encode(pgproto.CodecContext settings, WriteBuffer buf, obj):
    if not cpython.datetime.PyDateTime_Check(obj):
        raise TypeError(
            f'a datetime.datetime object was expected')

    if getattr(obj, 'tzinfo', None) is None:
        raise TypeError(
            f'a timezone-aware datetime object (tzinfo is not None) '
            f'was expected')

    pgproto.timestamptz_encode(settings, buf, obj)


cdef duration_encode(pgproto.CodecContext settings, WriteBuffer buf,
                     object obj):

    cdef microseconds = obj / datetime.timedelta(microseconds=1)

    buf.write_int32(16)
    buf.write_int64(microseconds)
    buf.write_int32(0)
    buf.write_int32(0)


cdef duration_decode(pgproto.CodecContext settings, FRBuffer *buf):
    cdef:
        int32_t days
        int32_t months
        int64_t microseconds

    microseconds = hton.unpack_int64(frb_read(buf, 8))
    days = hton.unpack_int32(frb_read(buf, 4))
    months = hton.unpack_int32(frb_read(buf, 4))
    if days != 0 or months != 0:
        raise RuntimeError(f'non-zero reserved bytes in duration')

    return datetime.timedelta(microseconds=microseconds)


cdef relative_duration_encode(pgproto.CodecContext settings, WriteBuffer buf,
                              object obj):

    cdef:
        microseconds = obj.microseconds
        days = obj.days
        months = obj.months

    buf.write_int32(16)
    buf.write_int64(microseconds)
    buf.write_int32(days)
    buf.write_int32(months)


cdef relative_duration_decode(pgproto.CodecContext settings, FRBuffer *buf):
    cdef:
        int32_t days
        int32_t months
        int64_t microseconds

    microseconds = hton.unpack_int64(frb_read(buf, 8))
    days = hton.unpack_int32(frb_read(buf, 4))
    months = hton.unpack_int32(frb_read(buf, 4))

    return datatypes.RelativeDuration(
        microseconds=microseconds, days=days, months=months)


cdef date_duration_encode(pgproto.CodecContext settings, WriteBuffer buf,
                          object obj):

    cdef:
        days = obj.days
        months = obj.months

    buf.write_int32(16)
    buf.write_int64(0)
    buf.write_int32(days)
    buf.write_int32(months)


cdef date_duration_decode(pgproto.CodecContext settings, FRBuffer *buf):
    cdef:
        int32_t days
        int32_t months
        int64_t microseconds

    microseconds = hton.unpack_int64(frb_read(buf, 8))
    days = hton.unpack_int32(frb_read(buf, 4))
    months = hton.unpack_int32(frb_read(buf, 4))

    if microseconds != 0:
        raise ValueError("date duration has non-zero microseconds")

    return datatypes.DateDuration(days=days, months=months)


cdef config_memory_encode(pgproto.CodecContext settings,
                          WriteBuffer buf,
                          object obj):
    cdef:
        bytes = obj._bytes

    buf.write_int32(8)
    buf.write_int64(bytes)


cdef config_memory_decode(pgproto.CodecContext settings, FRBuffer *buf):
    cdef:
        int64_t bytes

    bytes = hton.unpack_int64(frb_read(buf, 8))

    return datatypes.ConfigMemory(bytes=bytes)


DEF PGVECTOR_MAX_DIM = (1 << 16) - 1


cdef pgvector_encode_memview(pgproto.CodecContext settings, WriteBuffer buf,
                             float[:] obj):
    cdef:
        Py_ssize_t objlen
        Py_ssize_t i

    objlen = len(obj)
    if objlen > PGVECTOR_MAX_DIM:
        raise ValueError('too many elements in vector value')

    buf.write_int32(4 + objlen*4)
    buf.write_int16(objlen)
    buf.write_int16(0)
    for i in range(objlen):
        buf.write_float(obj[i])


cdef pgvector_encode(pgproto.CodecContext settings, WriteBuffer buf,
                     object obj):
    cdef:
        Py_ssize_t objlen
        float[:] memview
        Py_ssize_t i

    # If we can take a typed memview of the object, we use that.
    # That is good, because it means we can consume array.array and
    # numpy.ndarray without needing to unbox.
    # Otherwise we take the slow path, indexing into the array using
    # the normal protocol.
    try:
        memview = obj
    except (ValueError, TypeError) as e:
        pass
    else:
        pgvector_encode_memview(settings, buf, memview)
        return

    if not _is_array_iterable(obj):
        raise TypeError(
            'a sized iterable container expected (got type {!r})'.format(
                type(obj).__name__))

    # Annoyingly, this is literally identical code to the fast path...
    # but the types are different in critical ways.
    objlen = len(obj)
    if objlen > PGVECTOR_MAX_DIM:
        raise ValueError('too many elements in vector value')

    buf.write_int32(4 + objlen*4)
    buf.write_int16(objlen)
    buf.write_int16(0)
    for i in range(objlen):
        buf.write_float(obj[i])


cdef object ONE_EL_F32_ARRAY = array.array('f', [0.0])


cdef pgvector_decode(pgproto.CodecContext settings, FRBuffer *buf):
    cdef:
        int32_t dim
        Py_ssize_t size
        Py_buffer view
        char *p
        float[:] array_view

    dim = hton.unpack_uint16(frb_read(buf, 2))
    frb_read(buf, 2)

    size = dim * 4
    p = frb_read(buf, size)

    # Create a float array with size dim
    val = ONE_EL_F32_ARRAY * dim

    # And fill it with the buffer contents
    array_view = val
    memcpy(&array_view[0], p, size)
    val.byteswap()

    return val


# Half-floats or float16 types often don't have a native implementation, so in
# order to cast to and from it we need to implement the cast ourselves. The
# algorithm to do so is taken from here:
# http://www.fox-toolkit.org/ftp/fasthalffloatconversion.pdf
#
# It is a paper titled "Fast Half Float Conversions" by Jeroen van der Zijp,
# referenced by wiki as well as many articles on float conversion.

# Needed to perform bit manipulations to convert from float16 to float32
cdef union IntFloatSwap:
    uint32_t i
    float f


# Optimization to convert f16 to f32
cdef:
    uint32_t[2048] MANTISSA_TABLE
    uint32_t[64] EXP_TABLE
    uint32_t[64] OFFSET_TABLE
    uint32_t[512] BASE_TABLE
    uint32_t[512] SHIFT_TABLE


cdef convertmantissa(uint32_t n):
    cdef:
        uint32_t m
        uint32_t e

    m = n << 13                 # Zero pad mantissa bits
    e = 0                       # Zero exponent
    # While not normalized
    while (m & 0x00800000) == 0:
        e -= 0x00800000         # Decrement exponent (1<<23)
        m <<= 1                 # Shift mantissa

    m &= ~0x00800000            # Clear leading 1 bit
    e += 0x38800000             # Adjust bias ((127-14)<<23)

    return m | e


cdef generate_tables():
    cdef:
        uint32_t i
        int32_t e

    # Generate the f16 to f32 conversion tables
    MANTISSA_TABLE[0] = 0
    for i in range(1, 1024):
        MANTISSA_TABLE[i] = convertmantissa(i)
    for i in range(1024, 2048):
        MANTISSA_TABLE[i] = 0x38000000 + ((i - 1024) << 13)

    EXP_TABLE[0] = 0
    EXP_TABLE[31]= 0x47800000
    EXP_TABLE[32]= 0x80000000
    EXP_TABLE[63]= 0xC7800000
    for i in range(1, 31):
        EXP_TABLE[i] = i << 23
    for i in range(33, 63):
        EXP_TABLE[i] = 0x80000000 + ((i - 32) << 23)

    for i in range(64):
        if i == 0 or i == 32:
            OFFSET_TABLE[i] = 0
        else:
            OFFSET_TABLE[i] = 1024

    # Generate the f32 to f16 conversion tables
    for i in range(256):
        e = i - 127

        # Very small numbers map to zero
        if e < -24:
            BASE_TABLE[i] = 0
            BASE_TABLE[i + 256] = 0x8000
            SHIFT_TABLE[i] = 24
            SHIFT_TABLE[i + 256] = 24

        # Small numbers map to denorms
        elif e < -14:
            BASE_TABLE[i] = 0x0400 >> (-e - 14)
            BASE_TABLE[i + 256] = (0x0400 >> (-e - 14)) | 0x8000
            SHIFT_TABLE[i] = -e - 1
            SHIFT_TABLE[i + 256] = -e - 1

        # Normal numbers just lose precision
        elif e <= 15:
            BASE_TABLE[i] = (e + 15) << 10
            BASE_TABLE[i + 256] = ((e + 15) << 10) | 0x8000
            SHIFT_TABLE[i] = 13
            SHIFT_TABLE[i + 256] = 13

        # Large numbers map to Infinity
        elif e < 128:
            BASE_TABLE[i] = 0x7C00
            BASE_TABLE[i + 256] = 0xFC00
            SHIFT_TABLE[i] = 24
            SHIFT_TABLE[i + 256] = 24

        # Infinity and NaN's stay Infinity and NaN's
        else:
            BASE_TABLE[i] = 0x7C00
            BASE_TABLE[i + 256] = 0xFC00
            SHIFT_TABLE[i] = 13
            SHIFT_TABLE[i + 256] = 13


generate_tables()


cdef pgvector_hv_encode(pgproto.CodecContext settings, WriteBuffer buf,
                        object obj):
    cdef:
        Py_ssize_t objlen
        float[:] memview
        Py_ssize_t i
        IntFloatSwap swap

    if not _is_array_iterable(obj):
        raise TypeError(
            'a sized iterable container expected (got type {!r})'.format(
                type(obj).__name__))

    objlen = len(obj)
    if objlen > PGVECTOR_MAX_DIM:
        raise ValueError('too many elements in vector value')

    buf.write_int32(4 + objlen * 2)
    buf.write_int16(objlen)
    buf.write_int16(0)
    for i in range(objlen):
        swap.f = obj[i]
        swap.i = (
            BASE_TABLE[(swap.i >> 23) & 0x1ff]
            + ((swap.i & 0x007fffff) >> SHIFT_TABLE[(swap.i >> 23) & 0x1ff])
        )

        if swap.i == BASE_TABLE[255] or swap.i == BASE_TABLE[511]:
            raise ValueError(
                '{!r} is out of range for type halfvec'.format(obj[i]))

        buf.write_int16(swap.i)


cdef object ONE_EL16_ARRAY = array.array('H', [0])


cdef pgvector_hv_decode(pgproto.CodecContext settings, FRBuffer *buf):
    cdef:
        int32_t dim
        Py_ssize_t size
        Py_buffer view
        char *p
        unsigned short[:] tmp_array_view
        float[:] array_view
        Py_ssize_t i
        IntFloatSwap swap

    dim = hton.unpack_uint16(frb_read(buf, 2))
    frb_read(buf, 2)

    # We'll read float16 values as an array of uint16 and then convert since
    # plain vanilla Python doesn't have half-floats.
    #
    # TODO: Potentially we might handle it differently if numpy is present since
    # numpy has half floats since 1.60.
    size = dim * 2
    p = frb_read(buf, size)
    tmp = ONE_EL16_ARRAY * dim

    # And fill it with the buffer contents
    tmp_array_view = tmp
    memcpy(&tmp_array_view[0], p, size)
    tmp.byteswap()

    # Create a float array with size dim
    val = ONE_EL_F32_ARRAY * dim
    for i in range(dim):
        swap.i = tmp[i]
        swap.i = MANTISSA_TABLE[
            OFFSET_TABLE[swap.i >> 10] + (swap.i & 0x000003ff)
        ] + EXP_TABLE[swap.i >> 10]
        val[i] = swap.f

    return val


cdef pgvector_sv_encode(pgproto.CodecContext settings, WriteBuffer buf,
                        object obj):
    cdef:
        int32_t dim
        Py_ssize_t nnz

    if not (cpython.PyDict_Check(obj) or isinstance(obj, MappingABC)):
        raise TypeError(
            'a dict or Mapping expected (got type {!r})'.format(
                type(obj).__name__))

    if 'dim' not in obj:
        raise TypeError('"dim" key is missing')

    dim = obj['dim']

    # One of the keys is 'dim' so nnz is one less than the length
    nnz = len(obj) - 1
    if nnz > PGVECTOR_MAX_DIM or nnz > dim:
        raise ValueError('too many elements in vector value')

    buf.write_int32(4 * (3 + 2 * nnz))
    buf.write_int32(dim)
    buf.write_int32(nnz)
    buf.write_int32(0)

    for key in obj.keys():
        if key != 'dim':
            ensure_is_int(key)
            buf.write_int32(key)

    for key, val in obj.items():
        if key != 'dim':
            if val == 0:
                raise ValueError('elements in a sparse vector cannot be 0')
            buf.write_float(<float>val)


cdef object ONE_EL_I32_ARRAY = array.array('l', [0])


cdef pgvector_sv_decode(pgproto.CodecContext settings, FRBuffer *buf):
    cdef:
        int32_t dim
        int32_t nnz
        Py_ssize_t size
        char *p
        float[:] array_view
        Py_ssize_t i

    dim = hton.unpack_int32(frb_read(buf, 4))
    nnz = hton.unpack_int32(frb_read(buf, 4))
    frb_read(buf, 4)

    # Create an int array with size nnz (for indexes)
    keys = ONE_EL_I32_ARRAY * nnz
    for i in range(nnz):
        keys[i] = hton.unpack_int32(frb_read(buf, 4))

    size = nnz * 4
    p = frb_read(buf, size)

    # Create a float array with size nnz (for values)
    vals = ONE_EL_F32_ARRAY * nnz

    # And fill it with the buffer contents
    array_view = vals
    memcpy(&array_view[0], p, size)
    vals.byteswap()

    res = {'dim': dim}
    res.update(zip(keys, vals))

    return res


cdef checked_decimal_encode(
    pgproto.CodecContext settings, WriteBuffer buf, obj
):
    if not isinstance(obj, decimal.Decimal) and not isinstance(obj, int):
        raise TypeError('expected a Decimal or an int')
    pgproto.numeric_encode_binary(settings, buf, obj)


cdef decimal_decode(pgproto.CodecContext settings, FRBuffer *buf):
    return pgproto.numeric_decode_binary_ex(settings, buf, True)


cdef ensure_is_int(obj):
    if type(obj) is not int and not isinstance(obj, int):
        raise TypeError('expected an int')

cdef checked_int2_encode(pgproto.CodecContext settings, WriteBuffer buf, obj):
    ensure_is_int(obj)
    pgproto.int2_encode(settings, buf, obj)


cdef checked_int4_encode(pgproto.CodecContext settings, WriteBuffer buf, obj):
    ensure_is_int(obj)
    pgproto.int4_encode(settings, buf, obj)


cdef checked_int8_encode(pgproto.CodecContext settings, WriteBuffer buf, obj):
    ensure_is_int(obj)
    pgproto.int8_encode(settings, buf, obj)


cdef bigint_encode(pgproto.CodecContext settings, WriteBuffer buf, obj):
    cdef:
        uint16_t sign = NUMERIC_POS
        list digits = []

    ensure_is_int(obj)

    if obj == 0:
        buf.write_int32(8)  # len
        buf.write_int32(0)  # ndigits + weight
        buf.write_int16(NUMERIC_POS)  # sign
        buf.write_int16(0)  # dscale
        return

    if obj < 0:
        sign = NUMERIC_NEG
        obj = -obj

    while obj:
        mod = obj % NBASE
        obj //= NBASE
        digits.append(mod)

    buf.write_int32(8 + len(digits) * 2)  # len
    buf.write_int16(len(digits))  # ndigits
    buf.write_int16(len(digits) - 1)  # weight
    buf.write_int16(sign)  # sign
    buf.write_int16(0)  # dscale
    for dig in reversed(digits):
        buf.write_int16(dig)


cdef bigint_decode(pgproto.CodecContext settings, FRBuffer *buf):
    cdef:
        uint16_t ndigits = <uint16_t>hton.unpack_int16(frb_read(buf, 2))
        uint16_t weight = <uint16_t>hton.unpack_int16(frb_read(buf, 2))
        uint16_t sign = <uint16_t>hton.unpack_int16(frb_read(buf, 2))
        uint16_t dscale = <uint16_t>hton.unpack_int16(frb_read(buf, 2))
        result = ''
        int32_t i = <int32_t>weight
        uint16_t d = 0

    if sign == NUMERIC_NEG:
        result = '-'
    elif sign != NUMERIC_POS:
        raise ValueError("bad bigint sign data")

    if dscale != 0:
        raise ValueError("bigint data has fractional part")

    if ndigits == 0:
        return 0

    while i >= 0:
        if i <= weight and d < ndigits:
            num = str(<uint16_t>hton.unpack_int16(frb_read(buf, 2)))
            result += num.zfill(4)
            d += 1
        else:
            result += '0000'
        i -= 1

    return int(result)


cdef geometry_encode(pgproto.CodecContext settings, WriteBuffer buf, obj):
    buf.write_int32(len(obj.wkb))
    buf.write_bytes(obj.wkb)


cdef geometry_decode(pgproto.CodecContext settings, FRBuffer *buf):
    cdef:
        object result
        ssize_t buf_len

    # Just wrap the bytes into a named tuple with a single field `wkb`
    descriptor = datatypes.record_desc_new(
        ('wkb',), <object>NULL, <object>NULL)
    result = datatypes.namedtuple_new(
        datatypes.namedtuple_type_new(descriptor))

    # frb_read_all adjusts buf.len, so we need to read it out first
    buf_len = buf.len
    elem = PyBytes_FromStringAndSize(frb_read_all(buf), buf_len)
    cpython.Py_INCREF(elem)
    cpython.PyTuple_SET_ITEM(result, 0, elem)

    return result


cdef register_base_scalar_codecs():
    register_base_scalar_codec(
        'std::uuid',
        pgproto.uuid_encode,
        pgproto.uuid_decode)

    register_base_scalar_codec(
        'std::str',
        pgproto.text_encode,
        pgproto.text_decode)

    register_base_scalar_codec(
        'std::bytes',
        pgproto.bytea_encode,
        pgproto.bytea_decode)

    register_base_scalar_codec(
        'std::int16',
        checked_int2_encode,
        pgproto.int2_decode)

    register_base_scalar_codec(
        'std::int32',
        checked_int4_encode,
        pgproto.int4_decode)

    register_base_scalar_codec(
        'std::int64',
        checked_int8_encode,
        pgproto.int8_decode)

    register_base_scalar_codec(
        'std::float32',
        pgproto.float4_encode,
        pgproto.float4_decode)

    register_base_scalar_codec(
        'std::float64',
        pgproto.float8_encode,
        pgproto.float8_decode)

    register_base_scalar_codec(
        'std::decimal',
        checked_decimal_encode,
        decimal_decode)

    register_base_scalar_codec(
        'std::bigint',
        bigint_encode,
        bigint_decode)

    register_base_scalar_codec(
        'std::bool',
        pgproto.bool_encode,
        pgproto.bool_decode)

    register_base_scalar_codec(
        'std::datetime',
        timestamptz_encode,
        pgproto.timestamptz_decode)

    register_base_scalar_codec(
        'std::pg::timestamptz',
        timestamptz_encode,
        pgproto.timestamptz_decode)

    register_base_scalar_codec(
        'cal::local_datetime',
        timestamp_encode,
        pgproto.timestamp_decode)

    register_base_scalar_codec(
        'std::pg::timestamp',
        timestamp_encode,
        pgproto.timestamp_decode)

    register_base_scalar_codec(
        'cal::local_date',
        date_encode,
        pgproto.date_decode)

    register_base_scalar_codec(
        'std::pg::date',
        date_encode,
        pgproto.date_decode)

    register_base_scalar_codec(
        'cal::local_time',
        time_encode,
        pgproto.time_decode)

    register_base_scalar_codec(
        'std::duration',
        duration_encode,
        duration_decode)

    register_base_scalar_codec(
        'std::json',
        pgproto.jsonb_encode,
        pgproto.jsonb_decode)

    register_base_scalar_codec(
        'std::pg::json',
        pgproto.json_encode,
        pgproto.json_decode)

    register_base_scalar_codec(
        'cal::relative_duration',
        relative_duration_encode,
        relative_duration_decode)

    register_base_scalar_codec(
        'std::pg::interval',
        relative_duration_encode,
        relative_duration_decode)

    register_base_scalar_codec(
        'cal::date_duration',
        date_duration_encode,
        date_duration_decode)

    register_base_scalar_codec(
        'cfg::memory',
        config_memory_encode,
        config_memory_decode)

    register_base_scalar_codec(
        'fts::language',
        pgproto.text_encode,
        pgproto.text_decode)

    register_base_scalar_codec(
        'ext::pgvector::vector',
        pgvector_encode,
        pgvector_decode,
        uuid.UUID('9565dd88-04f5-11ee-a691-0b6ebe179825'),
    )

    register_base_scalar_codec(
        'ext::pgvector::halfvec',
        pgvector_hv_encode,
        pgvector_hv_decode,
        uuid.UUID('4ba84534-188e-43b4-a7ce-cea2af0f405b'),
    )

    register_base_scalar_codec(
        'ext::pgvector::sparsevec',
        pgvector_sv_encode,
        pgvector_sv_decode,
        uuid.UUID('003e434d-cac2-430a-b238-fb39d73447d2'),
    )

    register_base_scalar_codec(
        'ext::postgis::geometry',
        geometry_encode,
        geometry_decode,
        uuid.UUID('44c901c0-d922-4894-83c8-061bd05e4840'),
    )

    register_base_scalar_codec(
        'ext::postgis::geography',
        geometry_encode,
        geometry_decode,
        uuid.UUID('4d738878-3a5f-4821-ab76-9d8e7d6b32c4'),
    )

    register_base_scalar_codec(
        'ext::postgis::box2d',
        geometry_encode,
        geometry_decode,
        uuid.UUID('7fae5536-6311-4f60-8eb9-096a5d972f48'),
    )

    register_base_scalar_codec(
        'ext::postgis::box3d',
        geometry_encode,
        geometry_decode,
        uuid.UUID('c1a50ff8-fded-48b0-85c2-4905a8481433'),
    )


register_base_scalar_codecs()
