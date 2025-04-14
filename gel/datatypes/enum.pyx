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

import enum


class EnumValue(enum.Enum):

    def __str__(self):
        return self._value_

    def __repr__(self):
        return f'<gel.EnumValue {self._value_!r}>'

    @classmethod
    def _try_from(cls, value):
        if isinstance(value, EnumValue):
            return value
        elif isinstance(value, enum.Enum):
            return cls(value.value)
        else:
            return None

    def __lt__(self, other):
        other = self._try_from(other)
        if not other or self.__tid__ != other.__tid__:
            return NotImplemented
        return self._index_ < other._index_

    def __gt__(self, other):
        other = self._try_from(other)
        if not other or self.__tid__ != other.__tid__:
            return NotImplemented
        return self._index_ > other._index_

    def __le__(self, other):
        other = self._try_from(other)
        if not other or self.__tid__ != other.__tid__:
            return NotImplemented
        return self._index_ <= other._index_

    def __ge__(self, other):
        other = self._try_from(other)
        if not other or self.__tid__ != other.__tid__:
            return NotImplemented
        return self._index_ >= other._index_

    def __eq__(self, other):
        other = self._try_from(other)
        return self is other

    def __hash__(self):
        return hash(self._value_)
