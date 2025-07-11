# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

import uuid

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


def validate_id(value: Any) -> uuid.UUID:
    # standard Pydantic allows a few different ways of passing
    # uuids into models. We have to replicate it all because
    # we can't use Pydantic's validation as `id` is a special
    # frozen field that we have a lot of custom logic for.

    if type(value) is uuid.UUID or isinstance(value, uuid.UUID):
        return value

    if value is None:
        raise ValueError("id argument can't be None")

    if isinstance(value, str):
        try:
            return uuid.UUID(value)
        except ValueError as e:
            raise ValueError(
                f"id argument is a string value {value!r} "
                f"that can't cast to uuid"
            ) from e

    if isinstance(value, bytes):
        if len(value) == 16:
            return uuid.UUID(bytes=value)

        if len(value) != 36:
            raise ValueError(
                f"id argument is a bytes value {value!r} "
                f"that can't cast to uuid"
            )

        value_str = value.decode("latin-1")
        try:
            return uuid.UUID(value_str)
        except ValueError as e:
            raise ValueError(
                f"id argument is a bytes value {value!r} "
                f"that can't cast to uuid"
            ) from e

    raise ValueError(
        f"id argument has wrong type: expected uuid.UUID, "
        f"got {type(value).__name__}"
    )
