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

import dataclasses
import inspect
import typing

from gel.datatypes import datatypes
from gel._internal import _dlist


cdef dict CARDS_MAP = {
    datatypes.EdgeFieldCardinality.NO_RESULT: enums.Cardinality.NO_RESULT,
    datatypes.EdgeFieldCardinality.AT_MOST_ONE: enums.Cardinality.AT_MOST_ONE,
    datatypes.EdgeFieldCardinality.ONE: enums.Cardinality.ONE,
    datatypes.EdgeFieldCardinality.MANY: enums.Cardinality.MANY,
    datatypes.EdgeFieldCardinality.AT_LEAST_ONE: enums.Cardinality.AT_LEAST_ONE,
}

cdef DLIST_READ_WRITE = _dlist.Mode.ReadWrite


@cython.final
cdef class ObjectCodec(BaseNamedRecordCodec):

    cdef encode(self, WriteBuffer buf, object obj):
        cdef:
            WriteBuffer elem_data
            Py_ssize_t objlen = 0
            Py_ssize_t i
            BaseCodec sub_codec
            descriptor = (<BaseNamedRecordCodec>self).descriptor

        if not self.is_sparse:
            raise NotImplementedError

        elem_data = WriteBuffer.new()
        for name, arg in obj.items():
            try:
                i = descriptor.get_pos(name)
            except LookupError:
                raise self._make_missing_args_error_message(obj) from None
            objlen += 1
            elem_data.write_int32(i)
            if arg is not None:
                sub_codec = <BaseCodec>(self.fields_codecs[i])
                try:
                    sub_codec.encode(elem_data, arg)
                except (TypeError, ValueError) as e:
                    value_repr = repr(arg)
                    if len(value_repr) > 40:
                        value_repr = value_repr[:40] + '...'
                    raise errors.InvalidArgumentError(
                        'invalid input for state argument '
                        f' {name} := {value_repr} ({e})') from e
            else:
                elem_data.write_int32(-1)

        buf.write_int32(4 + elem_data.len())  # buffer length
        buf.write_int32(<int32_t><uint32_t>objlen)
        buf.write_buffer(elem_data)

    cdef encode_args(self, WriteBuffer buf, dict obj):
        cdef:
            WriteBuffer elem_data
            Py_ssize_t objlen
            Py_ssize_t i
            BaseCodec sub_codec
            descriptor = (<BaseNamedRecordCodec>self).descriptor

        if self.is_sparse:
            raise NotImplementedError

        self._check_encoder()

        objlen = len(obj)
        if objlen != len(self.fields_codecs):
            raise self._make_missing_args_error_message(obj)

        elem_data = WriteBuffer.new()
        for i in range(objlen):
            name = datatypes.record_desc_pointer_name(descriptor, i)
            try:
                arg = obj[name]
            except KeyError:
                raise self._make_missing_args_error_message(obj) from None

            card = datatypes.record_desc_pointer_card(descriptor, i)

            elem_data.write_int32(0)  # reserved bytes
            if arg is None:
                if card in {datatypes.EdgeFieldCardinality.ONE,
                            datatypes.EdgeFieldCardinality.AT_LEAST_ONE}:
                    raise errors.InvalidArgumentError(
                        f'argument ${name} is required, but received None'
                    )
                elem_data.write_int32(-1)
            else:
                sub_codec = <BaseCodec>(self.fields_codecs[i])
                try:
                    sub_codec.encode(elem_data, arg)
                except (TypeError, ValueError) as e:
                    value_repr = repr(arg)
                    if len(value_repr) > 40:
                        value_repr = value_repr[:40] + '...'
                    raise errors.InvalidArgumentError(
                        'invalid input for query argument'
                        f' ${name}: {value_repr} ({e})') from e

        buf.write_int32(4 + elem_data.len())  # buffer length
        buf.write_int32(<int32_t><uint32_t>objlen)
        buf.write_buffer(elem_data)

    def _make_missing_args_error_message(self, args):
        cdef descriptor = (<BaseNamedRecordCodec>self).descriptor

        required_args = set()

        for i in range(len(self.fields_codecs)):
            name = datatypes.record_desc_pointer_name(descriptor, i)
            required_args.add(name)

        passed_args = set(args.keys())
        missed_args = required_args - passed_args
        extra_args = passed_args - required_args
        required = 'acceptable' if self.is_sparse else 'expected'

        error_message = f'{required} {required_args} arguments'

        passed_args_repr = repr(passed_args) if passed_args else 'nothing'
        error_message += f', got {passed_args_repr}'

        if not self.is_sparse:
            missed_args = set(required_args) - set(passed_args)
            if missed_args:
                error_message += f', missed {missed_args}'

        extra_args = set(passed_args) - set(required_args)
        if extra_args:
            error_message += f', extra {extra_args}'

        return errors.QueryArgumentError(error_message)

    cdef _decode_plain(self, FRBuffer *buf, Py_ssize_t elem_count):
        cdef:
            object result
            Py_ssize_t i
            int32_t elem_len
            object elem
            BaseCodec elem_codec
            FRBuffer elem_buf
            tuple fields_codecs = (<BaseRecordCodec>self).fields_codecs
            descriptor = (<BaseNamedRecordCodec>self).descriptor

        result = datatypes.object_new(descriptor)

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
                        f'object element decoding: {frb_get_len(&elem_buf)}')

            datatypes.object_set(result, i, elem)

        return result

    cdef decode(self, object return_type, FRBuffer *buf):
        cdef:
            object result, lprops
            Py_ssize_t elem_count
            Py_ssize_t i
            Py_ssize_t tid_index
            Py_ssize_t start_reading_from = 0
            int32_t elem_len
            BaseCodec elem_codec
            FRBuffer elem_buf
            tuple fields_codecs = (<BaseRecordCodec>self).fields_codecs
            tuple fields_types
            tuple names = self.names
            tuple flags = self.flags
            tuple dlists
            dict tid_map
            object return_type_proxy
            Py_ssize_t fields_codecs_len = len(fields_codecs)
            descriptor = (<BaseNamedRecordCodec>self).descriptor
            dict lprops_dict
            dict result_dict
            bint is_polymorphic
            char *started_at

        if self.is_sparse:
            raise NotImplementedError

        self.adapt_to_return_type(return_type)
        tid_map = self.cached_tid_map
        tid_index = self.cached_tid_index
        is_polymorphic = tid_map is not None and len(tid_map) > 1
        fields_types = self.cached_return_type_subcodecs
        return_type = self.cached_return_type
        return_type_proxy = self.cached_return_type_proxy
        dlists = self.cached_return_type_dlists
        origins = self.cached_field_origins

        elem_count = <Py_ssize_t><uint32_t>hton.unpack_int32(frb_read(buf, 4))

        if elem_count != fields_codecs_len:
            raise RuntimeError(
                f'cannot decode Object: expected {fields_codecs_len} '
                f'elements, got {elem_count}')

        if return_type is None:
            return self._decode_plain(buf, elem_count)

        current_ret_type = return_type
        result_dict = {}
        if return_type_proxy is not None:
            lprops_dict = {}
        else:
            lprops_dict = None

        tid = None
        if is_polymorphic:
            # We need to read __tid__ and adapt the decoding types to it.
            # Unfortunately, __tid__ isn't always a first field, so for
            # certain queries we have to workaround and fish for it before
            # we start reading any other data.
            if tid_index == 0:
                frb_read(buf, 4)  # reserved
                elem_len = hton.unpack_int32(frb_read(buf, 4))
                if elem_len != 16:
                    raise RuntimeError('__tid__ has wrong length')
                tid = frb_read(buf, 16)[:16]
                start_reading_from = 1
            else:
                started_at = buf.buf
                for i in range(elem_count):
                    frb_read(buf, 4)  # reserved
                    elem_len = hton.unpack_int32(frb_read(buf, 4))
                    if i == tid_index:
                        if elem_len != 16:
                            raise RuntimeError('__tid__ has wrong length')
                        tid = frb_read(buf, 16)[:16]
                        buf.buf = started_at  # reset the buffer
                    else:
                        if elem_len != -1:
                            frb_read(buf, elem_len)

            if tid is None:
                raise RuntimeError('__tid__ is unexepectedly empty')
            try:
                current_ret_type = tid_map[tid]
            except KeyError:
                pass

        for i in range(start_reading_from, elem_count):
            frb_read(buf, 4)  # reserved
            elem_len = hton.unpack_int32(frb_read(buf, 4))

            if i == tid_index:
                if elem_len != -1:
                    # Discard the value; we've parsed it earlier.
                    frb_read(buf, elem_len)
                continue
            else:
                if elem_len == -1:
                    elem = None
                else:
                    elem_codec = <BaseCodec>fields_codecs[i]
                    elem = elem_codec.decode(
                        fields_types[i],
                        frb_slice_from(&elem_buf, buf, elem_len)
                    )
                    if frb_get_len(&elem_buf):
                        raise RuntimeError(
                            f'unexpected trailing data in buffer after '
                            f'object element decoding: {frb_get_len(&elem_buf)}')

            name = names[i]
            if flags[i] & datatypes._EDGE_POINTER_IS_LINKPROP:
                assert name[0] == '@' # XXX fix this
                lprops_dict[name[1:]] = elem
            else:
                dlist_factory = self.cached_return_type_dlists[i]
                if dlist_factory is tuple:
                    elem = tuple(elem)
                elif dlist_factory is not None:
                    elem = dlist_factory(
                        elem,
                        __wrap_list__=True,
                        __mode__=DLIST_READ_WRITE,
                    )
                result_dict[name] = elem

        if return_type_proxy is not None:
            nested = current_ret_type.__gel_model_construct__(result_dict)
            result = return_type_proxy.__gel_proxy_construct__(nested, lprops_dict)
        else:
            result = current_ret_type.__gel_model_construct__(result_dict)

        return result

    cdef adapt_to_return_type(self, object return_type):
        cdef:
            tuple names = self.names
            tuple flags = self.flags
            tuple fields_codecs = (<BaseRecordCodec>self).fields_codecs
            Py_ssize_t fields_codecs_len = len(fields_codecs)
            Py_ssize_t i

        if return_type is None:
            self.cached_tid_map = None
            self.cached_return_type = None
            self.cached_return_type_subcodecs = (None,) * fields_codecs_len
            self.cached_return_type_dlists = (None,) * fields_codecs_len
            self.cached_return_type_proxy = None
            return

        if return_type is self.cached_orig_return_type:
            # return_type should always be the same in the overwhelming
            # number of scenarios, so we should only do the expensive task
            # of introspecting the return_type and tailoring to it once
            # per Object codec's entire lifespan.
            return

        refl = getattr(return_type, "__gel_reflection__", None)
        if (
            refl is None
            or not hasattr(return_type, '__gel_model_construct__')
        ):
            raise TypeError(
                'only GelModel subclasses are supported in the decoding pipeline'
            )

        prefl = getattr(refl, "pointers", None)
        if prefl is None:
            raise TypeError(
                'only GelModel subclasses are supported in the decoding pipeline'
            )

        lprops_type = None
        proxy = getattr(return_type, '__proxy_of__', None)
        if proxy is not None:
            self.cached_return_type_proxy = return_type
            self.cached_return_type = proxy
            lprops_type = self.cached_return_type_proxy.__lprops__
        else:
            self.cached_return_type = return_type
            self.cached_return_type_proxy = None

        # Store base type's tid in the mapping *also* to exclude
        # subclasses of base type, e.g. if we have this:
        #
        #    class CustomContent(default.Content):
        #        pass
        #
        # then default.Content.__subclasses__() will contain
        # CustomContent, which we don't want to be there.

        tid_map = {refl.id.bytes: return_type}

        for ch in return_type.__subclasses__():
            try:
                refl = ch.__gel_reflection__
            except AttributeError:
                pass
            else:
                if refl.id.bytes not in tid_map:
                    tid_map[refl.id.bytes] = ch

        subs = []
        dlists = []
        origins = []
        ptrtypes = return_type.__gel_pointers__()
        for i, name in enumerate(names):
            if flags[i] & datatypes._EDGE_POINTER_IS_LINKPROP:
                subs.append(None)
                dlists.append(None)
                origins.append(return_type)
            elif name in {"__tname__", "__tid__"}:
                subs.append(None)
                dlists.append(None)
                self.cached_tid_index = i
                origins.append(return_type)
            else:
                origin = return_type
                if isinstance(self.source_types[i], ObjectTypeNullCodec):
                    tid = self.source_types[i].get_tid()
                    try:
                        origin = tid_map[tid]
                    except KeyError:
                        pass

                origins.append(origin)

                sub = getattr(origin, name)
                subs.append(sub.__gel_origin__)

                dlist_factory = None

                ptr = prefl.get(name)
                ptrtype = ptrtypes.get(name)
                if (
                    ptr is not None
                    and ptr.cardinality.is_multi()
                    and ptrtype is not None
                ):
                    if isinstance(ptrtype, typing.GenericAlias):
                        ptrtype = typing.get_origin(ptrtype)

                    if (
                        isinstance(ptrtype, type)
                        and (
                            issubclass(
                                ptrtype,
                                (_dlist.AbstractTrackedList, tuple),
                            )
                        )
                    ):
                        dlist_factory = ptrtype
                dlists.append(dlist_factory)

        self.cached_return_type_subcodecs = tuple(subs)
        self.cached_return_type_dlists = tuple(dlists)

        self.cached_field_origins = tuple(origins)

        self.cached_tid_map = tid_map
        self.cached_orig_return_type = return_type

    def get_dataclass_fields(self):
        cdef descriptor = (<BaseNamedRecordCodec>self).descriptor

        rv = self.cached_dataclass_fields
        if rv is None:
            rv = {}

            for i in range(len(self.fields_codecs)):
                name = datatypes.record_desc_pointer_name(descriptor, i)
                field = rv[name] = dataclasses.field()
                field.name = name
                field._field_type = dataclasses._FIELD

            self.cached_dataclass_fields = rv
        return rv

    @staticmethod
    cdef BaseCodec new(bytes tid, tuple names, tuple flags, tuple cards,
                       tuple codecs, tuple source_types, bint is_sparse):
        cdef:
            ObjectCodec codec

        codec = ObjectCodec.__new__(ObjectCodec)

        codec.tid = tid
        if is_sparse:
            codec.name = 'SparseObject'
        else:
            codec.name = 'Object'

        codec.cached_return_type_proxy = None
        codec.cached_return_type = None
        codec.cached_return_type_subcodecs = (None,) * len(names)
        codec.cached_orig_return_type = None
        codec.cached_tid_map = None
        codec.cached_return_type_dlists = None
        codec.cached_field_origins = None

        codec.flags = flags
        codec.is_sparse = is_sparse
        codec.descriptor = datatypes.record_desc_new(names, flags, cards)
        codec.descriptor.set_dataclass_fields_func(codec.get_dataclass_fields)
        codec.fields_codecs = codecs
        codec.names = names
        codec.source_types = source_types
        return codec

    def make_type(self, describe_context):
        cdef descriptor = (<BaseNamedRecordCodec>self).descriptor

        elements = {}
        for i, codec in enumerate(self.fields_codecs):
            name = datatypes.record_desc_pointer_name(descriptor, i)
            is_implicit = datatypes.record_desc_pointer_is_implicit(
                descriptor, i
            )
            if is_implicit and name == "__tname__":
                continue
            elements[name] = describe.Element(
                type=codec.make_type(describe_context),
                cardinality=CARDS_MAP[
                    datatypes.record_desc_pointer_card(descriptor, i)
                ],
                is_implicit=is_implicit,
                kind=(
                    enums.ElementKind.LINK
                    if datatypes.record_desc_pointer_is_link(descriptor, i)
                    else (
                        enums.ElementKind.LINK_PROPERTY
                        if datatypes.record_desc_pointer_is_link_prop(
                            descriptor, i
                        )
                        else enums.ElementKind.PROPERTY
                    )
                )
            )

        return describe.ObjectType(
            desc_id=uuid.UUID(bytes=self.tid),
            name=None,
            elements=elements,
        )


@cython.final
cdef class ObjectTypeNullCodec(BaseCodec):

    @staticmethod
    cdef BaseCodec new(bytes tid, str name, bint schema_defined):
        cdef:
            ObjectTypeNullCodec codec

        codec = ObjectTypeNullCodec.__new__(ObjectTypeNullCodec)
        codec.tid = tid
        codec.name = name
        codec.schema_defined = schema_defined
        return codec

    def get_tid(self):
        return self.tid

    def __repr__(self):
        return f'<ObjectType tid={self.tid} name={self.name}>'

@cython.final
cdef class CompoundTypeNullCodec(BaseCodec):

    @staticmethod
    cdef BaseCodec new(bytes tid, str name, bint schema_defined,
                       int op, tuple components):
        cdef:
            CompoundTypeNullCodec codec

        codec = CompoundTypeNullCodec.__new__(CompoundTypeNullCodec)

        codec.tid = tid
        codec.name = name
        codec.schema_defined = schema_defined
        codec.op = op
        codec.components = components
        return codec
