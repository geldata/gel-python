#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2019-present MagicStack Inc. and the EdgeDB authors.
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


# IMPORTANT: this private API is subject to change.


import functools
import typing

from gel.datatypes import datatypes as dt
from gel.enums import ElementKind


class PointerDescription(typing.NamedTuple):
    name: str
    kind: ElementKind
    implicit: bool


class ObjectDescription(typing.NamedTuple):
    pointers: typing.Tuple[PointerDescription, ...]


@functools.lru_cache()
def _introspect_object_desc(desc) -> ObjectDescription:
    pointers = []
    # Call __dir__ directly as dir() scrambles the order.
    for name in desc.__dir__():
        if desc.is_link(name):
            kind = ElementKind.LINK
        elif desc.is_linkprop(name):
            continue
        else:
            kind = ElementKind.PROPERTY

        pointers.append(
            PointerDescription(
                name=name, kind=kind, implicit=desc.is_implicit(name)
            )
        )

    return ObjectDescription(pointers=tuple(pointers))


def introspect_object(obj) -> ObjectDescription:
    return _introspect_object_desc(dt.get_object_descriptor(obj))
