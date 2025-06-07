# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

from __future__ import annotations
from typing import (
    Any,
    Annotated,
    cast,
    Concatenate,
    Optional,
    ParamSpec,
    TYPE_CHECKING,
    TypeVar,
)

from starlette import concurrency
from typing_extensions import Self

import asyncio
import importlib.util
import inspect

import fastapi
import gel
from fastapi import params

if TYPE_CHECKING:
    import types
    from collections.abc import Callable, Iterator

    from .auth import GelAuth


GEL_STATE_NAMES_STATE = "_gel_state_names"
P = ParamSpec("P")
Lifespan_T = TypeVar("Lifespan_T", bound="GelLifespan")


class Extension:
    _lifespan: GelLifespan

    def __init__(self, lifespan: GelLifespan) -> None:
        self._lifespan = lifespan
        self._post_init()

    def _post_init(self) -> None:
        pass

    async def on_startup(self, app: fastapi.FastAPI) -> None:
        pass

    async def on_shutdown(self, app: fastapi.FastAPI) -> None:
        pass


class GelLifespan:
    _state_name: str = "gel_client"
    _bio_state_name: str = "gel_blocking_io_client"
    _shutdown_timeout: Optional[float]
    _app: fastapi.FastAPI
    _client: gel.AsyncIOClient
    _bio_client: gel.Client

    _auth: Optional[GelAuth] = None
    _auto_auth: bool = True

    def __init__(
        self,
        app: fastapi.FastAPI,
        client: gel.AsyncIOClient,
        bio_client: gel.Client,
    ) -> None:
        self._app = app
        self._client = client
        self._bio_client = bio_client
        self._shutdown_timeout = None

        self._auth = None

    async def __aenter__(self) -> dict[str, Any]:
        await self._client.ensure_connected()
        await concurrency.run_in_threadpool(self._bio_client.ensure_connected)

        if (
            self._auto_auth
            and self._auth is None
            and all(
                [
                    importlib.util.find_spec("httpx"),
                    importlib.util.find_spec("python_multipart"),
                    importlib.util.find_spec("jwt"),
                ]
            )
        ):
            from ._auth import GelAuth  # noqa: PLC0415

            self._auth = GelAuth(self)

        for ext in [self._auth]:
            if ext is not None:
                await ext.on_startup(self._app)

        return {
            self._state_name: self._client,
            self._bio_state_name: self._bio_client,
            GEL_STATE_NAMES_STATE: (self._state_name, self._bio_state_name),
        }

    async def __aexit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[types.TracebackType],
    ) -> None:
        for ext in [self._auth]:
            if ext is not None:
                await ext.on_shutdown(self._app)
        if hasattr(asyncio, "timeout"):
            async with asyncio.timeout(self._shutdown_timeout):
                await self._client.aclose()
        else:
            await asyncio.wait_for(
                self._client.aclose(), timeout=self._shutdown_timeout
            )
        await concurrency.run_in_threadpool(
            self._bio_client.close, timeout=self._shutdown_timeout
        )

    def __call__(self, app: fastapi.FastAPI) -> Self:
        return self

    @property
    def client(self) -> gel.AsyncIOClient:
        return self._client

    @property
    def blocking_io_client(self) -> gel.Client:
        return self._bio_client

    def shutdown_timeout(self, timeout: Optional[float]) -> Self:
        self._shutdown_timeout = timeout
        return self

    def state_name(self, name: str, *, is_async: bool = True) -> Self:
        if is_async:
            self._state_name = name
        else:
            self._bio_state_name = name
        return self

    def install(self) -> None:
        self._app.include_router(fastapi.APIRouter(lifespan=self))

    def with_global(
        self, name: str
    ) -> Callable[[Callable[P, Optional[str]]], params.Depends]:
        def decorator(func: Callable[P, Optional[str]]) -> params.Depends:
            def wrapper(
                request: fastapi.Request, *args: P.args, **kwargs: P.kwargs
            ) -> Iterator[None]:
                value = func(*args, **kwargs)
                if value is None:
                    yield
                else:
                    state_name, bio_state_name = getattr(
                        request.state, GEL_STATE_NAMES_STATE
                    )
                    old_client = getattr(request.state, state_name)
                    old_bio_client = getattr(request.state, bio_state_name)
                    new_client = self._client.with_globals({name: value})
                    new_bio_client = self._bio_client.with_globals(
                        {name: value}
                    )
                    setattr(request.state, state_name, new_client)
                    setattr(request.state, bio_state_name, new_bio_client)
                    try:
                        yield
                    finally:
                        setattr(request.state, state_name, old_client)
                        setattr(request.state, bio_state_name, old_bio_client)

            sig = inspect.signature(wrapper)
            wrapper.__signature__ = sig.replace(  # type: ignore[attr-defined]
                parameters=[
                    next(iter(sig.parameters.values())),
                    *inspect.signature(func).parameters.values(),
                ]
            )
            return cast("params.Depends", fastapi.Depends(wrapper))

        return decorator

    @property
    def auth(self) -> GelAuth:
        if self._auth is None:
            from ._auth import GelAuth  # noqa: PLC0415

            self._auth = GelAuth(self)

        return self._auth

    def without_auth(self) -> Self:
        self._auth = None
        self._auto_auth = False
        return self


def _make_gelify(
    client_creator: Callable[P, gel.AsyncIOClient],
    bio_client_creator: Callable[P, gel.Client],
    lifespan_class: Callable[
        [fastapi.FastAPI, gel.AsyncIOClient, gel.Client], Lifespan_T
    ],
) -> Callable[Concatenate[fastapi.FastAPI, P], Lifespan_T]:
    def gelify(
        app: fastapi.FastAPI,
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Lifespan_T:
        lifespan = lifespan_class(
            app,
            client_creator(*args, **kwargs),
            bio_client_creator(*args, **kwargs),
        )
        lifespan.install()
        return lifespan

    return gelify


def _get_client(request: fastapi.Request) -> gel.AsyncIOClient:
    state_name, _ = getattr(request.state, GEL_STATE_NAMES_STATE)
    return cast("gel.AsyncIOClient", getattr(request.state, state_name))


def _get_bio_client(request: fastapi.Request) -> gel.Client:
    _, state_name = getattr(request.state, GEL_STATE_NAMES_STATE)
    return cast("gel.Client", getattr(request.state, state_name))


gelify = _make_gelify(gel.create_async_client, gel.create_client, GelLifespan)
Client = Annotated[gel.AsyncIOClient, fastapi.Depends(_get_client)]
BlockingIOClient = Annotated[gel.Client, fastapi.Depends(_get_bio_client)]
