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


cimport cython
cimport cpython

include "./abstract.pyx"
include "./relative_duration.pyx"
include "./date_duration.pyx"
include "./enum.pyx"
include "./config_memory.pyx"


_RecordDescriptor = EdgeRecordDesc_InitType()
Tuple = tuple
NamedTuple = EdgeNamedTuple_InitType()
Object = EdgeObject_InitType()
Record = EdgeRecord_InitType()
Set = list
Array = list

cdef str at_sign = "@"
at_sign_ptr = <cpython.PyObject*>at_sign

_EDGE_POINTER_IS_IMPLICIT = EDGE_POINTER_IS_IMPLICIT
_EDGE_POINTER_IS_LINKPROP = EDGE_POINTER_IS_LINKPROP
_EDGE_POINTER_IS_LINK = EDGE_POINTER_IS_LINK


def get_object_descriptor(obj):
    return EdgeObject_GetRecordDesc(obj)


def create_object_factory(**pointers):
    import dataclasses

    flags = ()
    names = ()
    fields = {}
    for pname, ptype in pointers.items():
        if not isinstance(ptype, set):
            ptype = {ptype}

        flag = 0
        is_linkprop = False
        for pt in ptype:
            if pt == 'link':
                flag |= EDGE_POINTER_IS_LINK
            elif pt == 'property':
                pass
            elif pt == 'link-property':
                flag |= EDGE_POINTER_IS_LINKPROP
                is_linkprop = True
            elif pt == 'implicit':
                flag |= EDGE_POINTER_IS_IMPLICIT
            else:
                raise ValueError(f'unknown pointer type {pt}')
        if is_linkprop:
            names += ("@" + pname,)
        else:
            names += (pname,)
            field = dataclasses.field()
            field.name = pname
            field._field_type = dataclasses._FIELD
            fields[pname] = field

        flags += (flag,)

    desc = EdgeRecordDesc_New(names, flags, <object>NULL)
    size = len(pointers)
    desc.set_dataclass_fields_func(lambda: fields)

    def factory(*items):
        if len(items) != size:
            raise ValueError

        o = EdgeObject_New(desc)
        for i in range(size):
            EdgeObject_SetItem(o, i, items[i])

        return o

    return factory


cdef record_desc_new(object names, object flags, object cards):
    return EdgeRecordDesc_New(names, flags, cards)


cdef record_desc_pointer_name(object desc, Py_ssize_t pos):
    return EdgeRecordDesc_PointerName(desc, pos)


cdef record_desc_pointer_card(object desc, Py_ssize_t pos):
    return EdgeRecordDesc_PointerCardinality(desc, pos)


cdef record_desc_pointer_is_link_prop(object desc, Py_ssize_t pos):
    return EdgeRecordDesc_PointerIsLinkProp(desc, pos)


cdef record_desc_pointer_is_link(object desc, Py_ssize_t pos):
    return EdgeRecordDesc_PointerIsLink(desc, pos)


cdef record_desc_pointer_is_implicit(object desc, Py_ssize_t pos):
    return EdgeRecordDesc_PointerIsImplicit(desc, pos)


cdef namedtuple_new(object namedtuple_type):
    return EdgeNamedTuple_New(namedtuple_type)


cdef namedtuple_type_new(object desc):
    return EdgeNamedTuple_Type_New(desc)


cdef object_new(object desc):
    return EdgeObject_New(desc)

cdef object_set(object obj, Py_ssize_t pos, object elem):
    EdgeObject_SetItem(obj, pos, elem)

cdef record_new(object desc):
    return EdgeRecord_New(desc)

cdef record_set(object obj, Py_ssize_t pos, object elem):
    EdgeRecord_SetItem(obj, pos, elem)
