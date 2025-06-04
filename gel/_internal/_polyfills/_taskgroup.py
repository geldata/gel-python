# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

"""asyncio.TaskGroup polyfill"""

import sys


if sys.version_info >= (3, 11):
    from asyncio import TaskGroup
else:
    from ._taskgroup_impl import TaskGroup


__all__ = ("TaskGroup",)
