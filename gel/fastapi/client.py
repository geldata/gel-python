#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2025-present MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from __future__ import annotations
from typing import (
    Any,
    Annotated,
    Callable,
    cast,
    Concatenate,
    Iterator,
    Optional,
    ParamSpec,
    TYPE_CHECKING,
    TypeVar,
)
from typing_extensions import Self

import asyncio
import inspect
import types

import fastapi
import gel
from fastapi import params

if TYPE_CHECKING:
    from .auth import GelAuth


GEL_STATE_NAME_STATE = "_gel_state_name"
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
    _shutdown_timeout: Optional[float]
    _app: fastapi.FastAPI
    _client: gel.AsyncIOClient

    _auth: Optional[GelAuth] = None
    _auto_auth: bool = True

    def __init__(
        self, app: fastapi.FastAPI, client: gel.AsyncIOClient
    ) -> None:
        self._app = app
        self._client = client
        self._shutdown_timeout = None

        self._auth = None

    async def __aenter__(self) -> dict[str, Any]:
        await self._client.ensure_connected()

        if self._auto_auth and self._auth is None:
            try:
                import httpx, jwt
            except ImportError:
                pass
            else:
                from .auth import GelAuth

                self._auth = GelAuth(self)

        for ext in [self._auth]:
            if ext is not None:
                await ext.on_startup(self._app)

        return {
            self._state_name: self._client,
            GEL_STATE_NAME_STATE: self._state_name,
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

    def __call__(self, app: fastapi.FastAPI) -> Self:
        return self

    @property
    def client(self) -> gel.AsyncIOClient:
        return self._client

    def shutdown_timeout(self, timeout: Optional[float]) -> Self:
        self._shutdown_timeout = timeout
        return self

    def state_name(self, name: str) -> Self:
        self._state_name = name
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
                state_name = getattr(request.state, GEL_STATE_NAME_STATE)
                old_client = getattr(request.state, state_name)
                value = func(*args, **kwargs)
                if value is None:
                    yield
                else:
                    new_client = self._client.with_globals({name: value})
                    setattr(request.state, state_name, new_client)
                    try:
                        yield
                    finally:
                        setattr(request.state, state_name, old_client)

            sig = inspect.signature(wrapper)
            wrapper.__signature__ = sig.replace(  # type: ignore[attr-defined]
                parameters=[
                    next(iter(sig.parameters.values())),
                    *inspect.signature(func).parameters.values(),
                ]
            )
            return cast(params.Depends, fastapi.Depends(wrapper))

        return decorator

    @property
    def auth(self) -> GelAuth:
        if self._auth is None:
            from .auth import GelAuth

            self._auth = GelAuth(self)

        return self._auth

    def without_auth(self) -> Self:
        self._auth = None
        self._auto_auth = False
        return self


def _make_gelify(
    client_creator: Callable[P, gel.AsyncIOClient],
    lifespan_class: Callable[[fastapi.FastAPI, gel.AsyncIOClient], Lifespan_T],
) -> Callable[Concatenate[fastapi.FastAPI, P], Lifespan_T]:
    def gelify(
        app: fastapi.FastAPI,
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Lifespan_T:
        lifespan = lifespan_class(app, client_creator(*args, **kwargs))
        lifespan.install()
        return lifespan

    return gelify


def _get_client(request: fastapi.Request) -> Any:
    state_name = getattr(request.state, GEL_STATE_NAME_STATE)
    return getattr(request.state, state_name)


gelify = _make_gelify(gel.create_async_client, GelLifespan)
Client = Annotated[gel.AsyncIOClient, fastapi.Depends(_get_client)]
