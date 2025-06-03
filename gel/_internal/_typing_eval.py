# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

# ruff: noqa: A001, A002

from typing import (
    Annotated,
    Any,
    ForwardRef,
    Union,
)
from typing_extensions import (
    TypeVar,
    TypeVarTuple,
    ParamSpec,
    evaluate_forward_ref,
)

from collections.abc import (
    Iterable,
    Mapping,
)

import sys
import typing
import types

from . import _typing_inspect


def resolve_type(
    value: Any,
    *,
    owner: types.ModuleType | type[Any] | None = None,
    globals: Mapping[str, Any] | None = None,
    locals: Mapping[str, Any] | None = None,
    type_params: Iterable[TypeVar | ParamSpec | TypeVarTuple] | None = None,
) -> Any:
    if isinstance(value, str):
        value = ForwardRef(value)

    if _typing_inspect.is_forward_ref(value):
        value = evaluate_forward_ref(
            value,
            owner=owner,
            globals=globals,
            locals=locals,
            type_params=type_params,
        )

    if _typing_inspect.is_type_alias(value):
        # PEP 695 TypeAliasType -> unwrap its __value__
        module = sys.modules[value.__module__] if value.__module__ else None
        value = value.__value__
        if isinstance(value, str):
            globals = module.__dict__ if module is not None else {}
            value = resolve_type(
                ForwardRef(value),
                globals=globals,
            )
        return resolve_type(value)
    elif _typing_inspect.is_generic_alias(value):
        origin = typing.get_origin(value)

        # typing.Annotated[...] -> drop metadata
        if origin is Annotated:  # type: ignore [comparison-overlap]
            return resolve_type(
                typing.get_args(value)[0],
                owner=owner,
                globals=globals,
                locals=locals,
            )

        # typing.Union[...] -> rebuild as PEP 604 UnionType (int|str)
        elif origin is Union:  # type: ignore [comparison-overlap]
            args = typing.get_args(value)
            resolved = resolve_type(
                args[0],
                owner=owner,
                globals=globals,
                locals=locals,
            )
            for arg in args[1:]:
                resolved |= resolve_type(
                    arg,
                    owner=owner,
                    globals=globals,
                    locals=locals,
                )
            return resolved

        # other typing generics (e.g. list[int])
        else:
            resolved_base = resolve_type(
                origin,
                owner=owner,
                globals=globals,
                locals=locals,
            )
            resolved_args = tuple(
                resolve_type(
                    a,
                    owner=owner,
                    globals=globals,
                    locals=locals,
                )
                for a in typing.get_args(value)
            )

            return resolved_base[resolved_args]
    else:
        # everything else is already a runtime type
        return value


def try_resolve_type(
    value: Any,
    *,
    owner: types.ModuleType | type[Any] | None = None,
    globals: Mapping[str, Any] | None = None,
    locals: Mapping[str, Any] | None = None,
    type_params: Iterable[TypeVar | ParamSpec | TypeVarTuple] | None = None,
) -> Any:
    try:
        return resolve_type(
            value,
            owner=owner,
            globals=globals,
            locals=locals,
            type_params=type_params,
        )
    except NameError:
        return None
