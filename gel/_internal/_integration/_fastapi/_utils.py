# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

from __future__ import annotations
from typing import (
    Annotated,
    Any,
    cast,
    Generic,
    get_args,
    get_origin,
    Optional,
    overload,
    Protocol,
    TYPE_CHECKING,
    TypeVar,
)
from typing_extensions import Self, TypeVarTuple, Unpack

import asyncio
import contextlib
import importlib
import inspect
import logging
import os
import re
import traceback
import warnings

import fastapi
from fastapi import exceptions, params, responses, routing, types, utils
from fastapi.datastructures import Default, DefaultPlaceholder
from fastapi.dependencies import models, utils as dep_utils
from fastapi.openapi import utils as openapi_utils
from starlette import concurrency
from starlette.routing import Match

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence
    from fastapi._compat import ModelField
    from starlette.routing import BaseRoute
    from starlette.types import Scope, Receive, Send


class ConfigSubject(Protocol):
    installed: bool


C = TypeVar("C")
S = TypeVar("S", bound=ConfigSubject)
T = TypeVar("T")
Ts = TypeVarTuple("Ts")
Handler = TypeVar("Handler", bound="Callable[..., Any]")
DEBUG_DEPTH = int(os.environ.get("GEL_PYTHON_DEBUG_ACCESS_STACK", "0"))


class Hook(Generic[Unpack[Ts]]):
    _key: str
    _hook_name: str

    def __init__(self, key: str):
        self._key = key

    def __set_name__(self, owner: type[Any], name: str) -> None:
        self._hook_name = name

    @overload
    def __get__(self, instance: None, owner: type[Any]) -> Self: ...

    @overload
    def __get__(
        self, instance: S, owner: type[S]
    ) -> HookInstance[S, Unpack[Ts]]: ...

    def __get__(
        self, instance: Optional[S], owner: type[S]
    ) -> Self | HookInstance[S, Unpack[Ts]]:
        if instance is None:
            return self
        return self._get(instance)

    def __set__(
        self, instance: ConfigSubject, value: Callable[..., Any]
    ) -> None:
        self._get(instance)(value)

    def _get(self, instance: S) -> HookInstance[S, Unpack[Ts]]:
        prop = f"_{self._hook_name}"
        rv: Optional[HookInstance[S, Unpack[Ts]]] = getattr(
            instance, prop, None
        )
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
                param_types=get_args(
                    dep_utils.get_typed_annotation(
                        instance.__annotations__[self._hook_name],
                        vars(importlib.import_module(instance.__module__)),
                    )
                ),
            )
            setattr(instance, prop, rv)
        return rv


class ParamProvider:
    def __init__(
        self,
        param_types: tuple[tuple[type[Any], Callable[[], None]], ...],
        args: tuple[Any, ...],
    ):
        self.dependency_overrides = {
            handle: lambda v=arg: v
            for (_type, handle), arg in zip(param_types, args, strict=True)
        }


class HookInstance(Generic[S, Unpack[Ts]]):
    _subject: S
    _is_set: bool = False
    _path: str
    _name: str

    _func: Callable[..., Any]
    _is_coroutine: bool
    _dependant: models.Dependant
    _param_types: tuple[tuple[type[Any], Callable[[], None]], ...]

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
        param_types: tuple[type[Any], ...],
    ) -> None:
        self._subject = subject
        self._path = path
        self._name = name
        self._default_response_class = default_response_class
        self._default_status_code = default_status_code
        self._param_types = tuple((t, lambda: None) for t in param_types)

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
    ) -> Callable[[Handler], Handler]: ...

    @overload
    def __call__(
        self,
        f: Handler,
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
    ) -> Handler: ...

    def __call__(
        self,
        f: Optional[Handler] = None,
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
    ) -> Handler | Callable[[Handler], Handler]:
        if self._subject.installed:
            raise ValueError("cannot set a hook handler after installation")
        if self._is_set:
            warnings.warn(
                f"overwriting an existing hook handler: {self._func}",
                stacklevel=2,
            )

        def wrapper(func: Handler) -> Handler:
            sig = inspect.signature(func)
            parameters = []
            globalns = getattr(func, "__globals__", {})
            for param in sig.parameters.values():
                anno = dep_utils.get_typed_annotation(
                    param.annotation, globalns
                )
                if get_origin(anno) is None:
                    for param_type, handle in self._param_types:
                        if issubclass(anno, param_type):
                            anno = Annotated[anno, fastapi.Depends(handle)]
                            break
                parameters.append(param.replace(annotation=anno))
            orig_sig = getattr(func, "__signature__", None)
            func.__signature__ = sig.replace(parameters=parameters)  # type: ignore [attr-defined]
            try:
                dependant = dep_utils.get_dependant(
                    path=self._path,
                    name=self._name,
                    call=func,
                )
            finally:
                if orig_sig is None:
                    delattr(func, "__signature__")
                else:
                    func.__signature__ = orig_sig  # type: ignore [attr-defined]

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
        self,
        request: fastapi.Request,
        *args: Unpack[Ts],
    ) -> fastapi.Response:
        assert self._is_set
        response: Optional[fastapi.Response] = None
        async with contextlib.AsyncExitStack() as async_exit_stack:
            solved_result = await dep_utils.solve_dependencies(
                request=request,
                dependant=self._dependant,
                async_exit_stack=async_exit_stack,
                embed_body_fields=False,
                dependency_overrides_provider=ParamProvider(
                    self._param_types, args
                ),
            )
            assert not solved_result.errors
            if self._default_status_code is not None:
                solved_result.response.status_code = self._default_status_code
            if self._is_coroutine:
                raw_response = await self._func(**solved_result.values)
            else:
                raw_response = await concurrency.run_in_threadpool(
                    self._func, **solved_result.values
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

    def __set__(self, instance: ConfigSubject, value: T) -> None:
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


class BaseConfigInstance(Generic[T, S]):
    _default: Callable[[], T]
    _subject: S
    _value: T
    _froze_by: Optional[str] = None

    def _call(self, value: T) -> None:
        if self._subject.installed:
            raise ValueError("cannot set config value after installation")
        if self._froze_by is not None:
            raise ValueError(
                f"cannot set config value after reading it: \n{self._froze_by}"
            )
        self._value = value

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

    def is_set(self) -> bool:
        return hasattr(self, "_value")


class ConfigInstance(BaseConfigInstance[T, S], Generic[T, S]):
    def __init__(self, config: Config[T], subject: S) -> None:
        self._default = lambda: config.default
        self._subject = subject

    def __call__(self, value: T) -> S:
        self._call(value)
        return self._subject


class ConfigDecorator(Generic[T]):
    _default: T
    _config_name: str

    def __init__(self, default: T) -> None:
        self._default = default

    def __set_name__(self, owner: type[Any], name: str) -> None:
        self._config_name = name

    @overload
    def __get__(self, instance: None, owner: type[Any]) -> Self: ...

    @overload
    def __get__(
        self, instance: S, owner: type[S]
    ) -> DecoratorInstance[T, S]: ...

    def __get__(
        self, instance: Optional[S], owner: type[S]
    ) -> Self | DecoratorInstance[T, S]:
        if instance is None:
            return self
        return self._get(instance)

    def __set__(self, instance: ConfigSubject, value: T) -> None:
        self._get(instance)(value)

    def _get(self, instance: S) -> DecoratorInstance[T, S]:
        prop = f"_{self._config_name}"
        rv: Optional[DecoratorInstance[T, S]] = getattr(instance, prop, None)
        if rv is None:
            rv = DecoratorInstance(self, instance)
            setattr(instance, prop, rv)
        return rv

    @property
    def default(self) -> T:
        return self._default


class DecoratorInstance(BaseConfigInstance[T, S], Generic[T, S]):
    def __init__(self, config: ConfigDecorator[T], subject: S) -> None:
        self._default = lambda: config.default
        self._subject = subject

    def __call__(self, value: C) -> C:
        self._call(cast("T", value))
        return value


class OneOf:
    bodies: dict[str, params.Body]

    def __init__(self, *bodies: params.Body):
        self.bodies = {}
        skipped_forms = False
        for body in bodies:
            if body.media_type in self.bodies:
                raise ValueError(
                    f"OneOf bodies must have unique media types, "
                    f"got {body.media_type}"
                )

            if isinstance(body, params.Form):
                if skipped_forms:
                    continue
                logger = logging.getLogger("fastapi")
                orig_level = logger.level
                logger.setLevel(logging.CRITICAL)
                try:
                    dep_utils.ensure_multipart_is_installed()
                except Exception:
                    skipped_forms = True
                    continue
                finally:
                    logger.setLevel(orig_level)

            self.bodies[body.media_type] = body

        if not self.bodies:
            if skipped_forms:
                # Form is the only body type, but multipart is not installed
                dep_utils.ensure_multipart_is_installed()
            else:
                raise ValueError("OneOf must have at least one body")


class ContentTypeRoute(routing.APIRoute):
    routes: dict[str, routing.APIRoute]

    def __init__(
        self, path: str, endpoint: Callable[..., Any], **kwargs: Any
    ) -> None:
        self.routes = {}
        explicit_sig = getattr(endpoint, "__signature__", None)
        orig_sig = dep_utils.get_typed_signature(endpoint)
        orig_params = orig_sig.parameters

        # Find all media types from OneOf annotations
        is_accept = False
        media_types: dict[str, params.Body] = {}  # actually an ordered set
        for param in orig_params.values():
            anno = param.annotation
            if get_origin(anno) is not Annotated:
                continue
            for arg in reversed(get_args(anno)[1:]):
                if isinstance(arg, OneOf):
                    is_accept = True
                    accept_types = arg.bodies
                    break
                elif isinstance(arg, params.Body):
                    accept_types = {arg.media_type: arg}
                    break
            else:
                continue
            if media_types:
                if set(media_types) != set(accept_types):
                    raise ValueError(
                        "OneOf bodies must have the same media types, "
                        f"got {set(media_types)} and {set(accept_types)}"
                    )
            else:
                media_types = accept_types

        if not is_accept:
            super().__init__(path, endpoint, **kwargs)
            return

        sig_replaced = False
        try:
            # Create routes for each media type
            extras = []
            for i, media_type in enumerate(media_types):
                new_params = []
                for param in orig_params.values():
                    anno = param.annotation
                    if get_origin(anno) is not Annotated:
                        new_params.append(param)
                    else:
                        args = get_args(anno)
                        new_args = []
                        is_accept = False
                        for arg in reversed(args[1:]):
                            if not is_accept and isinstance(arg, OneOf):
                                is_accept = True
                                new_args.append(arg.bodies[media_type])
                            else:
                                new_args.append(arg)
                        if is_accept:
                            new_args.append(args[0])
                            new_args.reverse()
                            new_params.append(
                                param.replace(
                                    annotation=Annotated[tuple(new_args)]
                                )
                            )
                        else:
                            new_params.append(param)
                endpoint.__signature__ = orig_sig.replace(  # type: ignore [attr-defined]
                    parameters=new_params
                )
                sig_replaced = True
                if i == 0:
                    super().__init__(path, endpoint, **kwargs)
                    self.routes[media_type.lower()] = self
                else:
                    route = routing.APIRoute(path, endpoint, **kwargs)
                    self.routes[media_type.lower()] = route
                    extras.append(route.body_field)
            if self.body_field is not None:
                self.body_field.__gel_extras__ = extras  # type: ignore [attr-defined]

        finally:
            if sig_replaced:
                # Restore the original signature if it was set
                if explicit_sig is None:
                    delattr(endpoint, "__signature__")
                else:
                    endpoint.__signature__ = explicit_sig  # type: ignore [attr-defined]

    def matches(self, scope: Scope) -> tuple[Match, Scope]:
        if self.routes:
            for k, v in scope["headers"]:
                if k == b"content-type":
                    route = self.routes.get(v.decode("latin-1").lower(), None)
                    if route is self:
                        return super().matches(scope)
                    elif route is None:
                        return Match.NONE, {}
                    else:
                        return route.matches(scope)
        return super().matches(scope)

    async def handle(self, scope: Scope, receive: Receive, send: Send) -> None:
        route = scope.get("route", self)
        if route is self:
            await super().handle(scope, receive, send)
        else:
            await route.handle(scope, receive, send)


orig_get_openapi_operation_request_body = (
    openapi_utils.get_openapi_operation_request_body
)


def get_openapi_operation_request_body(
    *,
    body_field: Optional[ModelField],
    **kwargs: Any,
) -> Optional[dict[str, Any]]:
    rv = orig_get_openapi_operation_request_body(
        body_field=body_field, **kwargs
    )
    if rv is None:
        return None
    extras = getattr(body_field, "__gel_extras__", None)
    if extras:
        for extra_body_field in extras:
            doc = orig_get_openapi_operation_request_body(
                body_field=extra_body_field, **kwargs
            )
            if doc:
                rv["content"].update(doc["content"])
    return rv


openapi_utils.get_openapi_operation_request_body = (
    get_openapi_operation_request_body
)


orig_get_fields_from_routes = openapi_utils.get_fields_from_routes


def get_fields_from_routes(routes: Sequence[BaseRoute]) -> list[ModelField]:
    all_routes: list[BaseRoute] = []
    for route in routes:
        if isinstance(route, ContentTypeRoute):
            if route.routes:
                all_routes.extend(route.routes.values())
            else:
                all_routes.append(route)
        else:
            all_routes.append(route)
    return orig_get_fields_from_routes(all_routes)


openapi_utils.get_fields_from_routes = get_fields_from_routes
