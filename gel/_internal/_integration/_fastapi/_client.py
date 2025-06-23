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
    ParamSpec,
    TYPE_CHECKING,
    TypeVar,
)

from starlette import concurrency
from typing_extensions import Self

import asyncio
import importlib.util
import inspect
import logging

import fastapi
import gel
from fastapi import params

from . import _cli
from . import _utils as utils

if TYPE_CHECKING:
    import types
    from collections.abc import Callable, Iterator, Sequence
    from ._auth import GelAuth


_logger = logging.getLogger("gel.fastapi")
GEL_STATE_NAMES_STATE = "_gel_state_names"
P = ParamSpec("P")
T = TypeVar("T")
Extension_T = TypeVar("Extension_T", bound="Extension")


class Extension:
    installed: bool = False
    _lifespan: GelLifespan

    def __init__(self, lifespan: GelLifespan) -> None:
        self._lifespan = lifespan

    async def on_startup(self, app: fastapi.FastAPI) -> None:
        self.installed = True

    async def on_shutdown(self, app: fastapi.FastAPI) -> None:
        pass


class ExtensionShell(Generic[Extension_T]):
    extension: Optional[Extension_T] = None
    intro_query = """
        select exists(select schema::Extension filter .name = <str>$name)
    """

    def __init__(
        self,
        *,
        package: str,
        cls: str,
        extension_name: str,
        requires: Sequence[str],
    ) -> None:
        self._package = package
        self._cls = cls
        self._extension_name = extension_name
        self._requires = requires
        self._disabled = False

    def ensure_init(
        self, lifespan: GelLifespan, *, enable: bool = False
    ) -> Extension_T:
        if self.extension is None:
            if lifespan.installed:
                raise ValueError(
                    f'Cannot enable "{self._cls}" after installation'
                )
            cls = getattr(importlib.import_module(self._package), self._cls)
            self.extension = cls(lifespan)
        if enable:
            self._disabled = False
        return self.extension

    async def auto_init(self, lifespan: GelLifespan) -> Optional[Extension_T]:
        if self._disabled:
            return None

        elif self.extension is None:
            if await lifespan.client.query_required_single(
                self.intro_query, name=self._extension_name
            ):
                missing_deps = [
                    dep
                    for dep in self._requires
                    if not importlib.util.find_spec(dep)
                ]
                if missing_deps:
                    _logger.warning(
                        "Required dependencies %r are not installed. "
                        "Please install them to use the %r extension, "
                        "or disable the extension to avoid this warning.",
                        missing_deps,
                        self._cls,
                    )
                    return None
                else:
                    return self.ensure_init(lifespan)
            else:
                return None

        else:
            if await lifespan.client.query_required_single(
                self.intro_query, name=self._extension_name
            ):
                return self.extension
            else:
                raise RuntimeError(
                    f'Extension "{self._extension_name}" is not installed, '
                    f"add `use extension {self._extension_name};` to your Gel "
                    f"schema to enable it.",
                )

    def disable(self, lifespan: GelLifespan) -> None:
        if lifespan.installed:
            raise ValueError(
                f'Cannot disable "{self._cls}" after installation'
            )
        self._disabled = True
        self.extension = None


class GelLifespan:
    installed: bool = False
    state_name = utils.Config("gel_client")
    blocking_io_state_name = utils.Config("gel_blocking_io_client")
    shutdown_timeout: utils.Config[Optional[float]] = utils.Config(None)

    _app: fastapi.FastAPI
    _client_creator: Callable[..., gel.AsyncIOClient]
    _client: gel.AsyncIOClient
    _bio_client_creator: Callable[..., gel.Client]
    _bio_client: gel.Client
    _client_accessed: bool = False

    _auth: ExtensionShell[GelAuth]

    def __init__(
        self,
        app: fastapi.FastAPI,
        *,
        client_creator: Callable[..., gel.AsyncIOClient],
        bio_client_creator: Callable[..., gel.Client],
    ) -> None:
        self._app = app
        self._auth = ExtensionShell(
            package="gel._internal._integration._fastapi._auth",
            cls="GelAuth",
            extension_name="auth",
            requires=["httpx", "python_multipart", "jwt"],
        )
        self._shells = [self._auth]

        self._client_creator = client_creator
        self._client = client_creator()
        self._bio_client_creator = bio_client_creator
        self._bio_client = bio_client_creator()

        self._install_lifespan()

    async def __aenter__(self) -> dict[str, Any]:
        await self._client.ensure_connected()
        await concurrency.run_in_threadpool(self._bio_client.ensure_connected)

        for shell in self._shells:
            ext = await shell.auto_init(self)
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
        for shell in self._shells:
            if shell.extension is not None:
                await shell.extension.on_shutdown(self._app)
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
        self._client_accessed = True
        return self._client

    @property
    def blocking_io_client(self) -> gel.Client:
        self._client_accessed = True
        return self._bio_client

    def _install_lifespan(self) -> None:
        self._app.include_router(fastapi.APIRouter(lifespan=self))

    def with_client_options(
        self,
        *,
        dsn: Optional[str] = None,
        max_concurrency: Optional[int] = None,
        host: Optional[str] = None,
        port: Optional[int] = None,
        credentials: Optional[str] = None,
        credentials_file: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        secret_key: Optional[str] = None,
        database: Optional[str] = None,
        branch: Optional[str] = None,
        tls_ca: Optional[str] = None,
        tls_ca_file: Optional[str] = None,
        tls_security: Optional[str] = None,
        wait_until_available: int = 30,
        timeout: int = 10,
    ) -> Self:
        if self.installed:
            raise ValueError("Cannot change client options after installation")
        if self._client_accessed:
            raise ValueError(
                "Cannot change client options "
                "after the client has been accessed"
            )

        self._client = self._client_creator(
            dsn=dsn,
            max_concurrency=max_concurrency,
            host=host,
            port=port,
            credentials=credentials,
            credentials_file=credentials_file,
            user=user,
            password=password,
            secret_key=secret_key,
            database=database,
            branch=branch,
            tls_ca=tls_ca,
            tls_ca_file=tls_ca_file,
            tls_security=tls_security,
            wait_until_available=wait_until_available,
            timeout=timeout,
        )
        self._bio_client = self._bio_client_creator(
            dsn=dsn,
            max_concurrency=max_concurrency,
            host=host,
            port=port,
            credentials=credentials,
            credentials_file=credentials_file,
            user=user,
            password=password,
            secret_key=secret_key,
            database=database,
            branch=branch,
            tls_ca=tls_ca,
            tls_ca_file=tls_ca_file,
            tls_security=tls_security,
            wait_until_available=wait_until_available,
            timeout=timeout,
        )
        return self

    def with_global(
        self, name: str
    ) -> Callable[[Callable[P, T]], params.Depends]:
        def decorator(func: Callable[P, T]) -> params.Depends:
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
        return self._auth.ensure_init(self)

    def with_auth(self, **kwargs: Any) -> Self:
        auth = self._auth.ensure_init(self, enable=True)
        for key, value in kwargs.items():
            getattr(auth, key)(value)
        return self

    def without_auth(self) -> Self:
        self._auth.disable(self)
        return self


def gelify(app: fastapi.FastAPI, **kwargs: Any) -> GelLifespan:
    rv = GelLifespan(
        app,
        client_creator=gel.create_async_client,
        bio_client_creator=gel.create_client,
    )
    for key, value in kwargs.items():
        if hasattr(rv, key):
            getattr(rv, key)(value)
        else:
            raise ValueError(f"Unknown configuration option: {key}")
    _cli.maybe_patch_fastapi_cli()
    return rv


def _get_client(request: fastapi.Request) -> gel.AsyncIOClient:
    state_name, _ = getattr(request.state, GEL_STATE_NAMES_STATE)
    return cast("gel.AsyncIOClient", getattr(request.state, state_name))


def _get_bio_client(request: fastapi.Request) -> gel.Client:
    _, state_name = getattr(request.state, GEL_STATE_NAMES_STATE)
    return cast("gel.Client", getattr(request.state, state_name))


Client = Annotated[gel.AsyncIOClient, fastapi.Depends(_get_client)]
BlockingIOClient = Annotated[gel.Client, fastapi.Depends(_get_bio_client)]
