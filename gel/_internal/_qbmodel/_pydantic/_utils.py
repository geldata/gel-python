# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.


from __future__ import annotations

import uuid

from typing import (
    Any,
    TypeAlias,
    TypedDict,
    Literal,
    TYPE_CHECKING,
)

from collections.abc import (
    Callable,
    Mapping,
)


if TYPE_CHECKING:
    from pydantic import BaseModel
    from pydantic_core import core_schema

    from ._models import GelModel


# TypeAliasType does not support recursive types. mypy errors out with:
#    error: Cannot resolve name "IncEx" (possible cyclic definition)  [misc]
#
# ...so we use TypeAlias instead, which does not bother mypy (and that's
# how this type is defined in pydantic itself.)
IncEx: TypeAlias = (
    set[int]
    | set[str]
    | Mapping[int, "IncEx | bool"]
    | Mapping[str, "IncEx | bool"]
)
IncExStr: TypeAlias = set[str] | Mapping[str, "IncExStr | bool"]


class GelDumpContext(TypedDict, total=False):
    gel_exclude_computeds: bool
    gel_allow_unsaved: bool


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


# Overload to add `context` typed as `GelDumpContext`
def model_dump_signature(
    self: BaseModel,
    *,
    mode: Literal["json", "python"] | str = "python",  # noqa: PYI051
    include: IncEx | None = None,
    exclude: IncEx | None = None,
    context: GelDumpContext | None = None,
    by_alias: bool | None = None,
    exclude_unset: bool = False,
    exclude_defaults: bool = False,
    exclude_none: bool = False,
    round_trip: bool = False,
    warnings: bool | Literal["none", "warn", "error"] = True,
    fallback: Callable[[Any], Any] | None = None,
    serialize_as_any: bool = False,
) -> dict[str, Any]:
    raise NotImplementedError


# Overload to add `context` typed as `GelDumpContext`
def model_dump_json_signature(
    self: BaseModel,
    *,
    indent: int | None = None,
    include: IncEx | None = None,
    exclude: IncEx | None = None,
    context: GelDumpContext | None = None,
    by_alias: bool | None = None,
    exclude_unset: bool = False,
    exclude_defaults: bool = False,
    exclude_none: bool = False,
    round_trip: bool = False,
    warnings: bool | Literal["none", "warn", "error"] = True,
    fallback: Callable[[Any], Any] | None = None,
    serialize_as_any: bool = False,
) -> str:
    raise NotImplementedError


def massage_model_dump_kwargs(
    model: GelModel,
    /,
    *,
    caller: Literal["model_dump", "model_dump_json"],
    context: GelDumpContext | None = None,
    kwargs: dict[str, Any],
) -> None:
    if (
        not model.__pydantic_computed_fields__
        and not model.__gel_changed_fields__
    ):
        return

    allow_unsaved = False
    exclude_computeds = False
    if context is not None:
        allow_unsaved = context.get("gel_allow_unsaved", False)
        exclude_computeds = context.get("gel_exclude_computeds", False)

    if not allow_unsaved and (
        model.__gel_changed_fields__
        or (model.__gel_has_id_field__ and model.__gel_new__)
    ):
        raise ValueError(
            f"Cannot dump an unsaved model: usually it is a sign of a bug "
            f"caused by forgetting to call `client.save()` on the model "
            f"to persist the changes to the database.\n\n"
            f"If you have to dump an unsaved model, pass "
            f'`context={{"gel_allow_unsaved": True}}` '
            f"to `{caller}()`."
        )

    # This is a model-level `exclude`, so it's either a set of str
    # or a dict of str to bools.
    exclude: IncExStr
    try:
        exclude = kwargs["exclude"]
    except KeyError:
        exclude = kwargs["exclude"] = set()

    if model.__gel_has_id_field__ and model.__gel_new__:
        # Exclude unset `id` field from the dump or the serializer
        # will crash with an
        if isinstance(exclude, set):
            exclude.add("id")
        else:
            assert isinstance(exclude, dict)
            exclude["id"] = True

    if model.__pydantic_computed_fields__:
        to_exclude: set[str]

        if exclude_computeds:
            # Exclude all computed fields.
            to_exclude = set(model.__pydantic_computed_fields__)
        else:
            # Pydantic assumes computed fields are always set, but that's
            # not true for GelModel; they might not be fetched. Attempting
            # to getattr an unfectched computed would result in an
            # `AttributeError`, so we exclude them to prevent `model_dump()`
            # from crashing.
            to_exclude = set()
            for fn in model.__pydantic_computed_fields__:
                if fn not in model.__dict__:
                    to_exclude.add(fn)

        if to_exclude:
            if isinstance(exclude, set):
                exclude.update(to_exclude)
            else:
                assert isinstance(exclude, dict)
                for fn in to_exclude:
                    exclude[fn] = True
