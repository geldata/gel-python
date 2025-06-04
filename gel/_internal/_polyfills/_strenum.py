# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

"""enum.StrEnum polyfill"""

import sys

if sys.version_info >= (3, 11):
    from enum import StrEnum
else:
    from ._strenum_impl import StrEnum


__all__ = ("StrEnum",)
