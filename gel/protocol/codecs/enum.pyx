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

import enum


@cython.final
cdef class EnumCodec(BaseCodec):

    cdef encode(self, WriteBuffer buf, object obj):
        if not isinstance(obj, (self.cls, str)):
            try:
                obj = self.cls._try_from(obj)
            except (TypeError, ValueError):
                obj = None
            if obj is None:
                raise TypeError(
                    f'a str or gel.EnumValue(__tid__={self.cls.__tid__}) '
                    f'is expected as a valid enum argument, '
                    f'got {type(obj).__name__}') from None
        pgproto.text_encode(DEFAULT_CODEC_CONTEXT, buf, str(obj))

    cdef decode(self, object return_type, FRBuffer *buf):
        label = pgproto.text_decode(DEFAULT_CODEC_CONTEXT, buf)
        return self.cls(label)

    @staticmethod
    cdef BaseCodec new(bytes tid, tuple enum_labels):
        cdef:
            EnumCodec codec

        codec = EnumCodec.__new__(EnumCodec)

        codec.tid = tid
        codec.name = 'Enum'
        cls = "DerivedEnumValue"
        bases = (datatypes.EnumValue,)
        classdict = enum.EnumMeta.__prepare__(cls, bases)
        classdict["__module__"] = "gel"
        classdict["__qualname__"] = "gel.DerivedEnumValue"
        classdict["__tid__"] = pgproto.UUID(tid)
        for label in enum_labels:
            classdict[label.upper()] = label
        codec.cls = enum.EnumMeta(cls, bases, classdict)
        for index, label in enumerate(enum_labels):
            codec.cls(label)._index_ = index

        return codec

    def make_type(self, describe_context):
        return describe.EnumType(
            desc_id=uuid.UUID(bytes=self.tid),
            name=self.type_name,
            members=tuple(x.value for x in self.cls.__members__.values())
        )
