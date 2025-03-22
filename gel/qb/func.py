from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from edgeql_qb.operators import OperationsMixin


@dataclass(slots=True, frozen=True)
class Function:
    module: str
    name: str

    def __call__(self, *args: Any, **kwargs: Any) -> FuncInvocation:
        return FuncInvocation(func=self, args=args, arity=len(args))


@dataclass(slots=True, frozen=True)
class _Math:
    abs = Function(module='math', name='abs')
    ceil = Function(module='math', name='ceil')
    floor = Function(module='math', name='floor')
    ln = Function(module='math', name='log')
    mean = Function(module='math', name='mean')
    stddev = Function(module='math', name='stddev')
    stddev_pop = Function(module='math', name='stddev_pop')
    var = Function(module='math', name='var')
    var_pop = Function(module='math', name='var_pop')


@dataclass(slots=True, frozen=True)
class _Std:
    all = Function(module='std', name='all')
    any = Function(module='std', name='any')
    array_agg = Function(module='std', name='array_agg')
    array_fill = Function(module='std', name='array_fill')
    array_get = Function(module='std', name='array_get')
    array_join = Function(module='std', name='array_join')
    array_replace = Function(module='std', name='array_replace')
    array_unpack = Function(module='std', name='array_unpack')
    assert_distinct = Function(module='std', name='assert_distinct')
    assert_exists = Function(module='std', name='assert_exists')
    assert_single = Function(module='std', name='assert_single')
    bit_and = Function(module='std', name='bit_and')
    bit_lshift = Function(module='std', name='bit_lshift')
    bit_not = Function(module='std', name='bit_not')
    bit_or = Function(module='std', name='bit_or')
    bit_rshift = Function(module='std', name='bit_rshift')
    bit_xor = Function(module='std', name='bit_xor')
    bytes_get_bit = Function(module='std', name='bytes_get_bit')
    contains = Function(module='std', name='contains')
    count = Function(module='std', name='count')
    datetime_current = Function(module='std', name='datetime_current')
    datetime_get = Function(module='std', name='datetime_get')
    datetime_of_statement = Function(module='std', name='datetime_of_statement')
    datetime_of_transaction = Function(module='std', name='datetime_of_transaction')
    datetime_truncate = Function(module='std', name='datetime_truncate')
    duration_get = Function(module='std', name='duration_get')
    duration_truncate = Function(module='std', name='duration_truncate')
    enumerate = Function(module='std', name='enumerate')
    find = Function(module='std', name='find')
    len = Function(module='std', name='len')
    max = Function(module='std', name='max')
    min = Function(module='std', name='min')
    overlaps = Function(module='std', name='overlaps')
    random = Function(module='std', name='random')
    range_get_lower = Function(module='std', name='range_get_lower')
    range_get_upper = Function(module='std', name='range_get_upper')
    range_is_empty = Function(module='std', name='range_is_empty')
    range_is_inclusive_lower = Function(module='std', name='range_is_inclusive_lower')
    range_is_inclusive_upper = Function(module='std', name='range_is_inclusive_upper')
    range_unpack = Function(module='std', name='range_unpack')
    re_match = Function(module='std', name='re_match')
    re_match_all = Function(module='std', name='re_match_all')
    re_replace = Function(module='std', name='re_replace')
    re_test = Function(module='std', name='re_test')
    sequence_next = Function(module='std', name='sequence_next')
    sequence_reset = Function(module='std', name='sequence_reset')
    str_lower = Function(module='std', name='str_lower')
    str_pad_end = Function(module='std', name='str_pad_end')
    str_pad_start = Function(module='std', name='str_pad_start')
    str_repeat = Function(module='std', name='str_repeat')
    str_replace = Function(module='std', name='str_replace')
    str_reverse = Function(module='std', name='str_reverse')
    str_split = Function(module='std', name='str_split')
    str_title = Function(module='std', name='str_title')
    str_trim = Function(module='std', name='str_trim')
    str_trim_end = Function(module='std', name='str_trim_end')
    str_trim_start = Function(module='std', name='str_trim_start')
    str_upper = Function(module='std', name='str_upper')
    sum = Function(module='std', name='sum')
    to_bigint = Function(module='std', name='to_bigint')
    to_datetime = Function(module='std', name='to_datetime')
    to_decimal = Function(module='std', name='to_decimal')
    to_duration = Function(module='std', name='to_duration')
    to_float32 = Function(module='std', name='to_float32')
    to_float64 = Function(module='std', name='to_float64')
    to_int16 = Function(module='std', name='to_int16')
    to_int32 = Function(module='std', name='to_int32')
    to_int64 = Function(module='std', name='to_int64')
    to_str = Function(module='std', name='to_str')
    uuid_generate_v1mc = Function(module='std', name='uuid_generate_v1mc')
    uuid_generate_v4 = Function(module='std', name='uuid_generate_v4')


@dataclass(slots=True, frozen=True)
class _Sys:
    get_version = Function(module='sys', name='get_version')
    get_version_as_str = Function(module='sys', name='get_version_as_str')
    get_current_database = Function(module='sys', name='get_current_database')


@dataclass(slots=True, frozen=True)
class _Cal:
    local_datetime = Function(module='std', name='local_datetime')
    local_date = Function(module='std', name='local_date')
    local_time = Function(module='std', name='local_time')
    relative_duration = Function(module='std', name='relative_duration')
    date_duration = Function(module='std', name='date_duration')
    to_local_datetime = Function(module='std', name='to_local_datetime')
    to_local_date = Function(module='std', name='to_local_date')
    to_local_time = Function(module='std', name='to_local_time')
    to_relative_duration = Function(module='std', name='to_relative_duration')
    to_date_duration = Function(module='std', name='to_date_duration')
    time_get = Function(module='std', name='time_get')
    date_get = Function(module='std', name='date_get')
    duration_normalize_hours = Function(module='std', name='duration_normalize_hours')
    duration_normalize_days = Function(module='std', name='duration_normalize_days')


math = _Math()
std = _Std()
sys = _Sys()
cal = _Cal()


@dataclass(slots=True, frozen=True)
class FuncInvocation(OperationsMixin):
    func: Function
    args: tuple[Any, ...]
    arity: int
