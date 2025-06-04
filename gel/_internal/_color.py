# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.


import typing
from collections.abc import Callable

import functools
import os
import sys
import warnings


class Color(typing.NamedTuple):
    HEADER: str = ""
    BLUE: str = ""
    CYAN: str = ""
    GREEN: str = ""
    WARNING: str = ""
    FAIL: str = ""
    ENDC: str = ""
    BOLD: str = ""
    UNDERLINE: str = ""


@functools.cache
def get_color() -> Color:
    if type(USE_COLOR) is bool:
        use_color = USE_COLOR
    else:
        try:
            use_color = USE_COLOR()
        except Exception:
            use_color = False
    if use_color:
        color = Color(
            HEADER="\033[95m",
            BLUE="\033[94m",
            CYAN="\033[96m",
            GREEN="\033[92m",
            WARNING="\033[93m",
            FAIL="\033[91m",
            ENDC="\033[0m",
            BOLD="\033[1m",
            UNDERLINE="\033[4m",
        )
    else:
        color = Color()

    return color


try:
    USE_COLOR: bool | Callable[[], bool] = {
        "default": sys.stderr.isatty,
        "auto": sys.stderr.isatty,
        "enabled": True,
        "disabled": False,
    }[os.getenv("EDGEDB_COLOR_OUTPUT", "default")]
except KeyError:
    warnings.warn(
        "EDGEDB_COLOR_OUTPUT can only be one of: "
        "default, auto, enabled or disabled",
        stacklevel=1,
    )
    USE_COLOR = False
