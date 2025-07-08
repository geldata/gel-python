# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

"""enum.IntEnum polyfill"""

import sys

if sys.version_info >= (3, 11):
    from enum import IntEnum
else:
    from ._intenum_impl import IntEnum


__all__ = ("IntEnum",)
