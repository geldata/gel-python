# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

from typing import (
    Any,
)

from pydantic_core import core_schema


def serialization_info_to_dump_kwargs(
    info: core_schema.SerializationInfo,
) -> dict[str, Any]:
    """Convert SerializationInfo to kwargs suitable for model_dump()"""

    kwargs = {}

    for attr in ["mode", "include", "exclude", "context", "by_alias"]:
        value = getattr(info, attr, None)
        if value is not None:
            kwargs[attr] = value

    for flag in [
        "exclude_unset",
        "exclude_defaults",
        "exclude_none",
        "round_trip",
        "warnings",
    ]:
        if getattr(info, flag, False):
            kwargs[flag] = True

    return kwargs
