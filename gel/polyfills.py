# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

"""
Polyfills for modern Python features used in the driver or generated code.
"""

from typing import (
    Any,
)

try:
    from enum import StrEnum as StrEnum  # type: ignore
except ImportError:
    import enum as _enum

    class StrEnum(str, _enum.Enum):
        @staticmethod
        def _generate_next_value_(
            name: str, start: int, count: int, last_values: list[Any]
        ) -> str:
            return name.lower()
