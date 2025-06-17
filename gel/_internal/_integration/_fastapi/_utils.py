# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

from __future__ import annotations
from typing import (
    Any,
    cast,
    Generic,
    Optional,
    overload,
    Protocol,
    TYPE_CHECKING,
    TypeVar,
)
from typing_extensions import Self

import asyncio
import contextlib
import functools
import os
import re
import traceback
import warnings

import fastapi
from fastapi import exceptions, responses, routing, types, utils
from fastapi.datastructures import Default, DefaultPlaceholder
from fastapi.dependencies import models, utils as dep_utils
from starlette import concurrency

if TYPE_CHECKING:
    from collections.abc import Callable
    from fastapi._compat import ModelField


class ConfigSubject(Protocol):
    installed: bool


S = TypeVar("S", bound=ConfigSubject)
T = TypeVar("T")
T_contra = TypeVar("T_contra", contravariant=True)
DEBUG_DEPTH = int(os.environ.get("GEL_PYTHON_DEBUG_ACCESS_STACK", "0"))


class Handler(Protocol[T_contra]):
    def __call__(self, result: T_contra, *args: Any, **kwargs: Any) -> Any: ...


class Hook(Generic[T]):
    _key: str
    _hook_name: str

    def __init__(self, key: str):
        self._key = key

    def __set_name__(self, owner: type[Any], name: str) -> None:
        self._hook_name = name

    @overload
    def __get__(self, instance: None, owner: type[Any]) -> Self: ...

    @overload
    def __get__(self, instance: S, owner: type[S]) -> HookInstance[T, S]: ...

    def __get__(
        self, instance: Optional[S], owner: type[S]
    ) -> Self | HookInstance[T, S]:
        if instance is None:
            return self
        return self._get(instance)

    def __set__(self, instance: S, value: Handler[T]) -> None:
        self._get(instance)(value)

    def _get(self, instance: S) -> HookInstance[T, S]:
        prop = f"_{self._hook_name}"
        rv: Optional[HookInstance[T, S]] = getattr(instance, prop, None)
        if rv is None:
            cls_key = f"{self._key}_default_response_class"
            if hasattr(instance, cls_key):
                default_response_class = getattr(instance, cls_key).value
            else:
                default_response_class = responses.JSONResponse
            code_key = f"{self._key}_default_status_code"
            if hasattr(instance, code_key):
                default_status_code = getattr(instance, code_key).value
            else:
                default_status_code = None
            rv = HookInstance(
                instance,
                path=getattr(instance, f"{self._key}_path").value,
                name=getattr(instance, f"{self._key}_name").value + prop,
                default_response_class=default_response_class,
                default_status_code=default_status_code,
            )
            setattr(instance, prop, rv)
        return rv


class HookInstance(Generic[T, S]):
    _subject: S
    _is_set: bool = False
    _path: str
    _name: str

    _func: Handler[T]
    _is_coroutine: bool
    _dependant: models.Dependant

    _status_code: Optional[int]
    _response_field: Optional[ModelField]
    _response_model_include: Optional[types.IncEx]
    _response_model_exclude: Optional[types.IncEx]
    _response_model_by_alias: bool
    _response_model_exclude_unset: bool
    _response_model_exclude_defaults: bool
    _response_model_exclude_none: bool
    _default_response_class: type[fastapi.Response]
    _default_status_code: Optional[int]
    _response_class: type[fastapi.Response]

    def __init__(
        self,
        subject: S,
        *,
        path: str,
        name: str,
        default_response_class: type[fastapi.Response],
        default_status_code: Optional[int],
    ) -> None:
        self._subject = subject
        self._path = path
        self._name = name
        self._default_response_class = default_response_class
        self._default_status_code = default_status_code

    @overload
    def __call__(
        self,
        *,
        response_model: Any = ...,
        status_code: Optional[int] = ...,
        response_model_include: Optional[types.IncEx] = ...,
        response_model_exclude: Optional[types.IncEx] = ...,
        response_model_by_alias: bool = ...,
        response_model_exclude_unset: bool = ...,
        response_model_exclude_defaults: bool = ...,
        response_model_exclude_none: bool = ...,
        response_class: type[fastapi.Response] = ...,
    ) -> Callable[[Handler[T]], Handler[T]]: ...

    @overload
    def __call__(
        self,
        f: Handler[T],
        *,
        response_model: Any = ...,
        status_code: Optional[int] = ...,
        response_model_include: Optional[types.IncEx] = ...,
        response_model_exclude: Optional[types.IncEx] = ...,
        response_model_by_alias: bool = ...,
        response_model_exclude_unset: bool = ...,
        response_model_exclude_defaults: bool = ...,
        response_model_exclude_none: bool = ...,
        response_class: type[fastapi.Response] = ...,
    ) -> Handler[T]: ...

    def __call__(
        self,
        f: Optional[Handler[T]] = None,
        *,
        response_model: Any = Default(None),  # noqa: B008
        status_code: Optional[int] = None,
        response_model_include: Optional[types.IncEx] = None,
        response_model_exclude: Optional[types.IncEx] = None,
        response_model_by_alias: bool = True,
        response_model_exclude_unset: bool = False,
        response_model_exclude_defaults: bool = False,
        response_model_exclude_none: bool = False,
        response_class: type[fastapi.Response] = Default(  # noqa: B008
            responses.JSONResponse
        ),
    ) -> Handler[T] | Callable[[Handler[T]], Handler[T]]:
        if self._subject.installed:
            raise ValueError("cannot set a hook handler after installation")
        if self._is_set:
            warnings.warn(
                f"overwriting an existing hook handler: {self._func}",
                stacklevel=2,
            )

        def wrapper(func: Handler[T]) -> Handler[T]:
            call = functools.partial(func, cast("T", None))
            # __globals__ is used by FastAPI get_typed_signature()
            call.__globals__ = func.__globals__  # type: ignore [attr-defined]
            dependant = dep_utils.get_dependant(
                path=self._path,
                name=self._name,
                call=call,
            )

            if dependant.path_params:
                raise ValueError("cannot depend on path parameters here")
            if dependant.query_params:
                raise ValueError("cannot depend on query parameters here")
            if dependant.header_params:
                raise ValueError("cannot depend on header parameters here")
            if dependant.cookie_params:
                raise ValueError("cannot depend on cookie parameters here")
            if dependant.body_params:
                raise ValueError("cannot depend on body parameters here")

            is_coroutine = asyncio.iscoroutinefunction(func)
            if response_model:
                assert utils.is_body_allowed_for_status_code(status_code), (
                    f"Status code {status_code} must not have a response body"
                )
                response_name = "Response_" + re.sub(r"\W", "_", self._name)
                response_field = dep_utils.create_model_field(  # type: ignore [attr-defined]
                    name=response_name,
                    type_=response_model,
                    mode="serialization",
                )
                secure_cloned_response_field: Optional[ModelField] = (
                    utils.create_cloned_field(response_field)
                )
            else:
                secure_cloned_response_field = None
            current_response_class = utils.get_value_or_default(
                response_class, self._default_response_class
            )
            if isinstance(current_response_class, DefaultPlaceholder):
                actual_response_class: type[fastapi.Response] = (
                    current_response_class.value
                )
            else:
                actual_response_class = current_response_class

            self._is_set = True
            self._func = func
            self._is_coroutine = is_coroutine
            self._dependant = dependant
            self._status_code = status_code
            self._response_field = secure_cloned_response_field
            self._response_model_include = response_model_include
            self._response_model_exclude = response_model_exclude
            self._response_model_by_alias = response_model_by_alias
            self._response_model_exclude_unset = response_model_exclude_unset
            self._response_model_exclude_defaults = (
                response_model_exclude_defaults
            )
            self._response_model_exclude_none = response_model_exclude_none
            self._response_class = actual_response_class
            return func

        if f is None:
            return wrapper
        else:
            return wrapper(f)

    def is_set(self) -> bool:
        return self._is_set

    async def call(
        self, request: fastapi.Request, result: T
    ) -> fastapi.Response:
        response: Optional[fastapi.Response] = None
        async with contextlib.AsyncExitStack() as async_exit_stack:
            solved_result = await dep_utils.solve_dependencies(
                request=request,
                dependant=self._dependant,
                async_exit_stack=async_exit_stack,
                embed_body_fields=False,
            )
            assert not solved_result.errors
            if self._default_status_code is not None:
                solved_result.response.status_code = self._default_status_code
            if self._is_coroutine:
                raw_response = await self._func(result, **solved_result.values)
            else:
                raw_response = await concurrency.run_in_threadpool(
                    self._func, result, **solved_result.values
                )
            if isinstance(raw_response, fastapi.Response):
                if raw_response.background is None:
                    raw_response.background = solved_result.background_tasks
                response = raw_response
            else:
                response_args: dict[str, Any] = {
                    "background": solved_result.background_tasks
                }
                if solved_result.response.status_code:
                    response_args["status_code"] = (
                        solved_result.response.status_code
                    )
                content = await routing.serialize_response(
                    field=self._response_field,
                    response_content=raw_response,
                    include=self._response_model_include,
                    exclude=self._response_model_exclude,
                    by_alias=self._response_model_by_alias,
                    exclude_unset=self._response_model_exclude_unset,
                    exclude_defaults=self._response_model_exclude_defaults,
                    exclude_none=self._response_model_exclude_none,
                    is_coroutine=self._is_coroutine,
                )
                response = self._response_class(content, **response_args)
                if not utils.is_body_allowed_for_status_code(
                    response.status_code
                ):
                    response.body = b""
                response.headers.raw.extend(solved_result.response.headers.raw)

        if response is None:
            raise exceptions.FastAPIError(
                "No response object was returned. There's a high chance that "
                "the application code is raising an exception and a "
                "dependency with yield has a block with a bare except, or a "
                "block with except Exception, and is not raising the "
                "exception again. Read more about it in the docs: "
                "https://fastapi.tiangolo.com/tutorial/dependencies/"
                "dependencies-with-yield/#dependencies-with-yield-and-except"
            )
        return response


class Config(Generic[T]):
    _default: T
    _config_name: str

    def __init__(self, default: T) -> None:
        self._default = default

    def __set_name__(self, owner: type[Any], name: str) -> None:
        self._config_name = name

    @overload
    def __get__(self, instance: None, owner: type[Any]) -> Self: ...

    @overload
    def __get__(self, instance: S, owner: type[S]) -> ConfigInstance[T, S]: ...

    def __get__(
        self, instance: Optional[S], owner: type[S]
    ) -> Self | ConfigInstance[T, S]:
        if instance is None:
            return self
        return self._get(instance)

    def __set__(self, instance: S, value: T) -> None:
        self._get(instance)(value)

    def _get(self, instance: S) -> ConfigInstance[T, S]:
        prop = f"_{self._config_name}"
        rv: Optional[ConfigInstance[T, S]] = getattr(instance, prop, None)
        if rv is None:
            rv = ConfigInstance(self, instance)
            setattr(instance, prop, rv)
        return rv

    @property
    def default(self) -> T:
        return self._default


class ConfigInstance(Generic[T, S]):
    _default: Callable[[], T]
    _subject: S
    _value: T
    _froze_by: Optional[str] = None

    def __init__(self, config: Config[T], subject: S) -> None:
        self._default = lambda: config.default
        self._subject = subject

    def __call__(self, value: T) -> S:
        if self._subject.installed:
            raise ValueError("cannot set config value after installation")
        if self._froze_by is not None:
            raise ValueError(
                f"cannot set config value after reading it: \n{self._froze_by}"
            )
        self._value = value
        return self._subject

    @property
    def value(self) -> T:
        if self._froze_by is None:
            if DEBUG_DEPTH > 0:
                self._froze_by = "".join(
                    traceback.format_list(
                        traceback.extract_stack(limit=DEBUG_DEPTH + 1)[:-1]
                    )
                )
            else:
                self._froze_by = (
                    "  Set GEL_PYTHON_DEBUG_ACCESS_STACK=3 "
                    "to see the stack trace"
                )
        try:
            return self._value
        except AttributeError:
            return self._default()
