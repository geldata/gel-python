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


from libc.stdint cimport int64_t, int32_t


DEF MONTHS_PER_YEAR         = 12


cdef class DateDuration(CustomType):

    def __init__(self, *, int32_t days=0, int32_t months=0):
        self.days = days
        self.months = months

    def __eq__(self, other):
        if type(other) is not DateDuration:
            return NotImplemented

        return (
            self.days == (<DateDuration>other).days and
            self.months == (<DateDuration>other).months
        )

    def __hash__(self):
        return hash((DateDuration, self.days, self.months))

    def __repr__(self):
        return f'<gel.DateDuration "{self}">'

    @cython.cdivision(True)
    def __str__(self):
        # This is closely modeled after interval_out().
        cdef:
            int64_t year = self.months / MONTHS_PER_YEAR
            int64_t mon = self.months % MONTHS_PER_YEAR
            list buf = []

        if not self.months and not self.days:
            return 'P0D'

        buf.append('P')

        if year:
            buf.append(f'{year}Y')

        if mon:
            buf.append(f'{mon}M')

        if self.days:
            buf.append(f'{self.days}D')

        return ''.join(buf)


cdef new_date_duration(int32_t days, int32_t months):
    cdef DateDuration dur = DateDuration.__new__(DateDuration)

    dur.days = days
    dur.months = months

    return dur
