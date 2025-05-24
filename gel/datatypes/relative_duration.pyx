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
DEF USECS_PER_HOUR          = 3600000000
DEF USECS_PER_MINUTE        = 60000000
DEF USECS_PER_SEC           = 1000000
DEF MAX_INTERVAL_PRECISION  = 6


cdef class RelativeDuration(CustomType):

    def __init__(self, *, int64_t microseconds=0,
                 int32_t days=0, int32_t months=0):
        self.microseconds = microseconds
        self.days = days
        self.months = months

    def __eq__(self, other):
        if type(other) is not RelativeDuration:
            return NotImplemented

        return (
            self.microseconds == (<RelativeDuration>other).microseconds and
            self.days == (<RelativeDuration>other).days and
            self.months == (<RelativeDuration>other).months
        )

    def __hash__(self):
        return hash((RelativeDuration, self.microseconds, self.days, self.months))

    def __repr__(self):
        return f'<gel.RelativeDuration "{self}">'

    @cython.cdivision(True)
    def __str__(self):
        # This is closely modeled after interval_out().
        cdef:
            int64_t year = self.months / MONTHS_PER_YEAR
            int64_t mon = self.months % MONTHS_PER_YEAR
            int64_t time = self.microseconds
            int64_t tfrac
            int64_t hour
            int64_t min
            int64_t sec
            int64_t fsec
            list buf = []
            bint neg
            bint is_before = False
            bint is_first = True

        if not self.months and not self.days and not time:
            return 'PT0S'

        tfrac = time / <int64_t>USECS_PER_HOUR
        time -= tfrac * <int64_t>USECS_PER_HOUR
        hour = tfrac

        if (hour < 0) != (tfrac < 0):
            raise ValueError('interval out of range')

        tfrac = time / <int64_t>USECS_PER_MINUTE
        time -= tfrac * <int64_t>USECS_PER_MINUTE
        min = tfrac
        sec = time / USECS_PER_SEC
        fsec = time - sec * USECS_PER_SEC

        buf.append('P')

        if year:
            buf.append(f'{year}Y')

        if mon:
            buf.append(f'{mon}M')

        if self.days:
            buf.append(f'{self.days}D')

        if not self.microseconds:
            return ''.join(buf)

        buf.append('T')

        if hour:
            buf.append(f'{hour}H')

        if min:
            buf.append(f'{min}M')

        if sec or fsec:
            # If the original microseconds are negative we expect '-' in front
            # of all non-zero hour/min/second components. The hour/min sign
            # can be taken as is, but seconds are constructed out of sec and
            # fsec parts, both of which have their own sign and thus we cannot
            # just use their string representations directly.
            sign = '-' if self.microseconds < 0 else ''
            buf.append(f'{sign}{abs(sec)}')

            if fsec:
                buf.append(f'.{abs(fsec):0>6}'.rstrip('0'))

            buf.append('S')

        return ''.join(buf)


cdef new_duration(int64_t microseconds, int32_t days, int32_t months):
    cdef RelativeDuration dur = RelativeDuration.__new__(RelativeDuration)

    dur.microseconds = microseconds
    dur.days = days
    dur.months = months

    return dur
