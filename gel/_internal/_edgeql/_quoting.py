# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.


from __future__ import annotations

import re

from . import _keywords


_re_ident = re.compile(r"""(?x)
    [^\W\d]\w*  # alphanumeric identifier
""")

_re_ident_or_num = re.compile(r"""(?x)
    [^\W\d]\w*  # alphanumeric identifier
    |
    ([1-9]\d* | 0)  # purely integer identifier
""")


def escape_string(s: str) -> str:
    # characters escaped according to
    # https://docs.geldata.com/reference/edgeql/literals#strings
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


def dollar_quote_literal(text: str) -> str:
    quote = "$$"
    qq = 0

    while quote in text:
        if qq % 16 < 10:  # noqa: PLR2004
            qq += 10 - qq % 16

        quote = f"${qq:x}$"[::-1]
        qq += 1

    return quote + text + quote


def needs_quoting(
    string: str,
    *,
    allow_reserved: bool,
    allow_num: bool,
) -> bool:
    if not string or string.startswith("@") or "::" in string:
        # some strings are illegal as identifiers and as such don't
        # require quoting
        return False

    r = _re_ident_or_num if allow_num else _re_ident
    isalnum = r.fullmatch(string)

    string = string.lower()

    is_reserved = (
        string not in {"__type__", "__std__"}
        and string in _keywords.RESERVED_KEYWRORDS
    )

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
    if force or needs_quoting(
        string,
        allow_reserved=allow_reserved,
        allow_num=allow_num,
    ):
        return _quote_ident(string)
    else:
        return string
