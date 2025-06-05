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
    Generic,
    Optional,
    TypeVar,
    Union,
)

try:
    # Python < 3.10
    from typing_extensions import ParamSpec, Concatenate
except ImportError:
    from typing import ParamSpec, Concatenate  # type: ignore
try:
    # Python < 3.11
    from typing_extensions import Self
except ImportError:
    from typing import Self  # type: ignore

import asyncio
import functools

import fastapi
import gel
from starlette import concurrency


GEL_STATE_NAME_STATE = "_gel_state_name"
P = ParamSpec("P")
Client_T = TypeVar("Client_T", bound=Union[gel.AsyncIOClient, gel.Client])
Lifespan_T = TypeVar("Lifespan_T", bound="GelLifespan[Client_T]")


class GelLifespan(Generic[Client_T]):
    _state_name: str
    _shutdown_timeout: Optional[float]
    _client: Client_T

    def __init__(self, client: Client_T) -> None:
        self._client = client
        self._shutdown_timeout = None

    async def __aenter__(self) -> dict[str, Client_T]:
        await self._ensure_connected()
        return {
            self._state_name: self._client,
            GEL_STATE_NAME_STATE: self._state_name,
        }

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._shutdown(self._shutdown_timeout)

    def __call__(self, app: fastapi.FastAPI) -> Self:
        return self

    async def _ensure_connected(self) -> None:
        raise NotImplementedError

    async def _shutdown(self, timeout: Optional[float]) -> None:
        raise NotImplementedError

    @property
    def client(self) -> Client_T:
        return self._client

    def shutdown_timeout(self, timeout: Optional[float]) -> Self:
        self._shutdown_timeout = timeout
        return self

    def state_name(self, name: str) -> Self:
        self._state_name = name
        return self

    def install(self, app: fastapi.FastAPI) -> None:
        app.include_router(fastapi.APIRouter(lifespan=self))

    def with_global(
        self, name: str
    ) -> Callable[[Callable[P, str]], Callable[P, Client_T]]:
        def decorator(func: Callable[P, str]) -> Callable[P, Client_T]:
            @functools.wraps(func)
            def wrapper(*args: P.args, **kwargs: P.kwargs) -> Client_T:
                return self._client.with_globals({name: func(*args, **kwargs)})

            wrapper.__annotations__["return"] = type(self._client)
            return wrapper

        return decorator


class BlockingIOLifespan(GelLifespan[gel.Client]):
    _state_name = "gel_blocking_client"

    async def _ensure_connected(self) -> None:
        await concurrency.run_in_threadpool(self._client.ensure_connected)

    async def _shutdown(self, timeout: Optional[float]) -> None:
        await concurrency.run_in_threadpool(
            self._client.close, timeout=timeout
        )


class AsyncIOLifespan(GelLifespan[gel.AsyncIOClient]):
    _state_name = "gel_client"

    async def _ensure_connected(self) -> None:
        await self._client.ensure_connected()

    async def _shutdown(self, timeout: Optional[float]) -> None:
        if hasattr(asyncio, "timeout"):
            async with asyncio.timeout(timeout):
                await self._client.aclose()
        else:
            await asyncio.wait_for(self._client.aclose(), timeout=timeout)


def make_gelify(
    client_creator: Callable[P, Client_T],
    lifespan_class: Callable[[Client_T], Lifespan_T],
) -> Callable[Concatenate[fastapi.FastAPI, P], Lifespan_T]:
    def gelify(
        app: fastapi.FastAPI,
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Lifespan_T:
        lifespan = lifespan_class(client_creator(*args, **kwargs))
        lifespan.install(app)
        return lifespan

    return gelify


gelify = make_gelify(gel.create_async_client, AsyncIOLifespan)
gelify_blocking = make_gelify(gel.create_client, BlockingIOLifespan)


def get_client(request: fastapi.Request) -> Any:
    state_name = getattr(request.state, GEL_STATE_NAME_STATE)
    return getattr(request.state, state_name)


Client = Annotated[gel.AsyncIOClient, fastapi.Depends(get_client)]
BlockingClient = Annotated[gel.Client, fastapi.Depends(get_client)]
