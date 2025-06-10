# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

from __future__ import annotations
from typing import (
    Any,
    Annotated,
    cast,
    Generic,
    Optional,
    overload,
    ParamSpec,
    Protocol,
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

from . import _utils as utils

if TYPE_CHECKING:
    import types
    from collections.abc import Callable, Iterator
    from ._auth import GelAuth


GEL_STATE_NAMES_STATE = "_gel_state_names"
P = ParamSpec("P")
C = ParamSpec("C")
R = TypeVar("R", covariant=True)


class LifespanCreator(Protocol[R]):
    def __call__(self, app: fastapi.FastAPI, **kwargs: Any) -> R: ...


class ClientCreator(Protocol[C, R]):
    @overload
    def __call__(self) -> R: ...

    @overload
    def __call__(self, *args: C.args, **kwargs: C.kwargs) -> R: ...

    def __call__(self, *args: C.args, **kwargs: C.kwargs) -> R: ...


class Extension(Generic[C]):
    installed: bool = False
    _lifespan: GelLifespan[C]

    def __init__(self, lifespan: GelLifespan[C]) -> None:
        self._lifespan = lifespan
        self._post_init()

    def _post_init(self) -> None:
        pass

    async def on_startup(self, app: fastapi.FastAPI) -> None:
        self.installed = True

    async def on_shutdown(self, app: fastapi.FastAPI) -> None:
        pass


class GelLifespan(Generic[C]):
    installed: bool = False
    state_name = utils.Config("gel_client")
    blocking_io_state_name = utils.Config("gel_blocking_io_client")
    shutdown_timeout: utils.Config[Optional[float]] = utils.Config(None)

    _app: fastapi.FastAPI
    _client_creator: ClientCreator[C, gel.AsyncIOClient]
    _client: gel.AsyncIOClient
    _bio_client_creator: ClientCreator[C, gel.Client]
    _bio_client: gel.Client

    _auth: Optional[GelAuth[C]] = None
    _auto_auth: bool = True

    def __init__(
        self,
        app: fastapi.FastAPI,
        *,
        client_creator: ClientCreator[C, gel.AsyncIOClient],
        bio_client_creator: ClientCreator[C, gel.Client],
        **kwargs: Any,
    ) -> None:
        self._app = app
        self._auth = None

        self._client_creator = client_creator
        self._client = client_creator()
        self._bio_client_creator = bio_client_creator
        self._bio_client = bio_client_creator()

        for key, value in kwargs.items():
            if hasattr(self, key):
                getattr(self, key)(value)
            else:
                raise ValueError(f"Unknown configuration option: {key}")
        self._install_lifespan()

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

        self.installed = True
        return {
            self.state_name.value: self._client,
            self.blocking_io_state_name.value: self._bio_client,
            GEL_STATE_NAMES_STATE: (
                self.state_name.value,
                self.blocking_io_state_name.value,
            ),
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
        timeout = self.shutdown_timeout.value
        if hasattr(asyncio, "timeout"):
            async with asyncio.timeout(timeout):
                await self._client.aclose()
        else:
            await asyncio.wait_for(self._client.aclose(), timeout=timeout)
        await concurrency.run_in_threadpool(
            self._bio_client.close, timeout=timeout
        )

    def __call__(self, app: fastapi.FastAPI) -> Self:
        return self

    @property
    def client(self) -> gel.AsyncIOClient:
        return self._client

    @property
    def blocking_io_client(self) -> gel.Client:
        return self._bio_client

    def _install_lifespan(self) -> None:
        self._app.include_router(fastapi.APIRouter(lifespan=self))

    def with_client_options(self, *args: C.args, **kwargs: C.kwargs) -> Self:
        if self.installed:
            raise ValueError("Cannot change client options after installation")

        self._client = self._client_creator(*args, **kwargs)
        self._bio_client = self._bio_client_creator(*args, **kwargs)
        return self

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
    def auth(self) -> GelAuth[C]:
        if self._auth is None:
            if self.installed:
                raise ValueError("Cannot enable auth after installation")

            from ._auth import GelAuth  # noqa: PLC0415

            self._auth = GelAuth(self)

        return self._auth

    def with_auth(self, **kwargs: Any) -> Self:
        auth = self.auth
        for key, value in kwargs.items():
            getattr(auth, key)(value)
        return self

    def without_auth(self) -> Self:
        if self.installed:
            raise ValueError("Cannot disable auth after installation")

        self._auth = None
        self._auto_auth = False
        return self


def _make_gelify(
    client_creator: ClientCreator[C, gel.AsyncIOClient],
    bio_client_creator: ClientCreator[C, gel.Client],
) -> LifespanCreator[GelLifespan[C]]:
    def gelify(app: fastapi.FastAPI, **kwargs: Any) -> GelLifespan[C]:
        return GelLifespan(
            app,
            client_creator=client_creator,
            bio_client_creator=bio_client_creator,
            **kwargs,
        )

    return gelify


def _get_client(request: fastapi.Request) -> gel.AsyncIOClient:
    state_name, _ = getattr(request.state, GEL_STATE_NAMES_STATE)
    return cast("gel.AsyncIOClient", getattr(request.state, state_name))


def _get_bio_client(request: fastapi.Request) -> gel.Client:
    _, state_name = getattr(request.state, GEL_STATE_NAMES_STATE)
    return cast("gel.Client", getattr(request.state, state_name))


gelify = _make_gelify(gel.create_async_client, gel.create_client)
Client = Annotated[gel.AsyncIOClient, fastapi.Depends(_get_client)]
BlockingIOClient = Annotated[gel.Client, fastapi.Depends(_get_bio_client)]
