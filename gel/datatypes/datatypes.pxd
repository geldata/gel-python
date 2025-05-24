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


include "./abstract.pxd"
include "./relative_duration.pxd"
include "./date_duration.pxd"
include "./config_memory.pxd"


cdef extern from "datatypes.h":

    int EDGE_POINTER_IS_IMPLICIT
    int EDGE_POINTER_IS_LINKPROP
    int EDGE_POINTER_IS_LINK

    ctypedef enum EdgeFieldCardinality:
        UNKNOWN
        NO_RESULT
        AT_MOST_ONE
        ONE
        MANY
        AT_LEAST_ONE

    object EdgeRecordDesc_InitType()
    object EdgeRecordDesc_New(object, object, object)
    object EdgeRecordDesc_PointerName(object, Py_ssize_t pos)
    EdgeFieldCardinality EdgeRecordDesc_PointerCardinality(
        object, Py_ssize_t pos)
    int EdgeRecordDesc_PointerIsLinkProp(object, Py_ssize_t pos)
    int EdgeRecordDesc_PointerIsLink(object, Py_ssize_t pos)
    int EdgeRecordDesc_PointerIsImplicit(object, Py_ssize_t pos)

    object EdgeNamedTuple_InitType()
    object EdgeNamedTuple_New(object)
    object EdgeNamedTuple_Type_New(object)

    object EdgeObject_InitType()
    object EdgeObject_New(object);
    int EdgeObject_SetItem(object, Py_ssize_t, object) except -1
    object EdgeObject_GetRecordDesc(object)

    object EdgeRecord_InitType()
    object EdgeRecord_New(object);
    int EdgeRecord_SetItem(object, Py_ssize_t, object) except -1
    object EdgeRecord_GetRecordDesc(object)

cdef record_desc_new(object names, object flags, object cards)
cdef record_desc_pointer_name(object desc, Py_ssize_t pos)
cdef record_desc_pointer_card(object desc, Py_ssize_t pos)
cdef record_desc_pointer_is_link_prop(object desc, Py_ssize_t pos)
cdef record_desc_pointer_is_link(object desc, Py_ssize_t pos)
cdef record_desc_pointer_is_implicit(object desc, Py_ssize_t pos)
cdef namedtuple_new(object namedtuple_type)
cdef namedtuple_type_new(object desc)
cdef object_new(object desc)
cdef object_set(object tuple, Py_ssize_t pos, object elem)
cdef record_new(object desc)
cdef record_set(object obj, Py_ssize_t pos, object elem)

cdef extern cpython.PyObject* at_sign_ptr
