# This source file is part of the EdgeDB open source project.
#
# Copyright 2025-present MagicStack Inc. and the EdgeDB authors.
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

import re


_re_ident = re.compile(
    r"""(?x)
    [^\W\d]\w*  # alphanumeric identifier
"""
)

_re_ident_or_num = re.compile(
    r"""(?x)
    [^\W\d]\w*  # alphanumeric identifier
    |
    ([1-9]\d* | 0)  # purely integer identifier
"""
)

_reserved_keyword = {
    "like",
    "do",
    "listen",
    "grant",
    "anytype",
    "ilike",
    "single",
    "__edgedbsys__",
    "end",
    "set",
    "never",
    "typeof",
    "start",
    "configure",
    "rollback",
    "when",
    "__edgedbtpl__",
    "or",
    "__source__",
    "filter",
    "global",
    "case",
    "introspect",
    "__default__",
    "not",
    "begin",
    "over",
    "if",
    "lock",
    "refresh",
    "else",
    "alter",
    "notify",
    "distinct",
    "and",
    "module",
    "offset",
    "drop",
    "is",
    "discard",
    "anyobject",
    "import",
    "group",
    "__subject__",
    "limit",
    "match",
    "anyarray",
    "insert",
    "get",
    "administer",
    "delete",
    "__old__",
    "exists",
    "true",
    "select",
    "analyze",
    "by",
    "move",
    "load",
    "deallocate",
    "partition",
    "with",
    "window",
    "in",
    "false",
    "raise",
    "revoke",
    "anytuple",
    "commit",
    "update",
    "for",
    "describe",
    "variadic",
    "fetch",
    "__specified__",
    "optional",
    "explain",
    "__new__",
    "create",
    "prepare",
    "check",
    "extending",
    "detached",
    "on",
}


def escape_string(s: str) -> str:
    # characters escaped according to
    # https://www.edgedb.com/docs/reference/edgeql/lexical#strings
    result = s

    # escape backslash first
    result = result.replace("\\", "\\\\")

    result = result.replace("'", "\\'")
    result = result.replace("\b", "\\b")
    result = result.replace("\f", "\\f")
    result = result.replace("\n", "\\n")
    result = result.replace("\r", "\\r")
    result = result.replace("\t", "\\t")

    return result


def quote_literal(string: str) -> str:
    return "'" + escape_string(string) + "'"


def needs_quoting(string: str, allow_reserved: bool, allow_num: bool) -> bool:
    if not string or string.startswith("@") or "::" in string:
        # some strings are illegal as identifiers and as such don't
        # require quoting
        return False

    r = _re_ident_or_num if allow_num else _re_ident
    isalnum = r.fullmatch(string)

    string = string.lower()

    is_reserved = string in _reserved_keyword

    return not isalnum or (not allow_reserved and is_reserved)


def _quote_ident(string: str) -> str:
    return "`" + string.replace("`", "``") + "`"


def quote_ident(
    string: str,
    *,
    force: bool = False,
    allow_reserved: bool = False,
    allow_num: bool = False,
) -> str:
    if force or needs_quoting(string, allow_reserved, allow_num):
        return _quote_ident(string)
    else:
        return string
