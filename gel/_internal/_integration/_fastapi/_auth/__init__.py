# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

from __future__ import annotations
from typing import Any, cast, Optional, TYPE_CHECKING
from typing_extensions import Self

import contextlib
import datetime

import fastapi
import jwt
import uuid  # noqa: TC003  # for runtime type annotations
from fastapi import security, params

from gel import auth as core  # noqa: TC001  # for runtime type annotations

from .. import _client as client_mod
from .. import _utils as utils

if TYPE_CHECKING:
    import enum
    from collections.abc import Callable, Iterator

    import gel
    from ._email_password import EmailPassword
    from ._builtin_ui import BuiltinUI


class Installable:
    installed: bool = False

    async def install(self, router: fastapi.APIRouter) -> None:
        self.installed = True


class _NoopResponse(fastapi.Response):
    pass


class GelAuth(client_mod.Extension):
    auto_detection = utils.Config(True)  # noqa: FBT003
    auth_path_prefix = utils.Config("/auth")
    client_token_global = utils.Config("ext::auth::client_token")
    auth_cookie_name = utils.Config("gel_auth_token")
    auth_cookie_description = utils.Config(
        "The cookie as the authentication token"
    )
    auth_cookie_domain: utils.Config[Optional[str]] = utils.Config(None)
    verifier_cookie_name = utils.Config("gel_verifier")
    tags: utils.Config[list[str | enum.Enum]] = utils.Config(["Gel Auth"])
    secure_cookie = utils.Config(True)  # noqa: FBT003
    redirect_to: utils.Config[Optional[str]] = utils.Config("/")
    redirect_to_page_name: utils.Config[Optional[str]] = utils.Config(None)

    _email_password: Optional[EmailPassword] = None
    _auto_email_password: bool = True
    _builtin_ui: Optional[BuiltinUI] = None
    _auto_builtin_ui: bool = True

    _on_new_identity_path = utils.Config("/")
    _on_new_identity_name = utils.Config("gel.fastapi.auth.on_new_identity")
    _on_new_identity_default_response_class: utils.Config[
        type[fastapi.Response]
    ] = utils.Config(_NoopResponse)
    on_new_identity: utils.Hook[uuid.UUID, Optional[core.TokenData]] = (
        utils.Hook("_on_new_identity")
    )

    _pkce_verifier: Optional[security.APIKeyCookie] = None
    _maybe_auth_token: params.Depends
    _auth_token: params.Depends

    def get_unchecked_exp(self, token: str) -> Optional[datetime.datetime]:
        jwt_payload = jwt.decode(token, options={"verify_signature": False})
        if "exp" not in jwt_payload:
            return None
        return datetime.datetime.fromtimestamp(
            jwt_payload["exp"], tz=datetime.timezone.utc
        )

    def set_auth_cookie(self, token: str, response: fastapi.Response) -> None:
        exp = self.get_unchecked_exp(token)
        response.set_cookie(
            key=self.auth_cookie_name.value,
            value=token,
            httponly=True,
            secure=self.secure_cookie.value,
            samesite="lax",
            expires=exp,
            domain=self.auth_cookie_domain.value,
        )

    def set_verifier_cookie(
        self, verifier: str, response: fastapi.Response
    ) -> None:
        response.set_cookie(
            key=self.verifier_cookie_name.value,
            value=verifier,
            httponly=True,
            secure=self.secure_cookie.value,
            samesite="lax",
            expires=int(datetime.timedelta(days=7).total_seconds()),
        )

    @property
    def pkce_verifier(self) -> security.APIKeyCookie:
        if self._pkce_verifier is None:
            self._pkce_verifier = security.APIKeyCookie(
                name=self.verifier_cookie_name.value,
                description="The cookie as the PKCE verifier",
                auto_error=False,
            )
        return self._pkce_verifier

    @property
    def client(self) -> gel.AsyncIOClient:
        return self._lifespan.client

    @property
    def blocking_io_client(self) -> gel.Client:
        return self._lifespan.blocking_io_client

    @property
    def maybe_auth_token(self) -> params.Depends:
        if not hasattr(self, "_maybe_auth_token"):
            auth_token_cookie = security.APIKeyCookie(
                name=self.auth_cookie_name.value,
                description=self.auth_cookie_description.value,
                auto_error=False,
            )

            def get_auth_token(
                auth_token: Optional[str] = fastapi.Depends(auth_token_cookie),
            ) -> Optional[str]:
                return auth_token

            self._maybe_auth_token = self._lifespan.with_global(
                self.client_token_global.value
            )(get_auth_token)
        return self._maybe_auth_token

    @property
    def auth_token(self) -> params.Depends:
        if not hasattr(self, "_auth_token"):
            auth_token_cookie = security.APIKeyCookie(
                name=self.auth_cookie_name.value,
                description=self.auth_cookie_description.value,
                auto_error=True,
            )

            def get_auth_token(
                auth_token: str = fastapi.Depends(auth_token_cookie),
            ) -> str:
                return auth_token

            self._auth_token = self._lifespan.with_global(
                self.client_token_global.value
            )(get_auth_token)
        return self._auth_token

    def with_auth_token(
        self, auth_token: str, request: fastapi.Request
    ) -> contextlib.AbstractContextManager[None]:
        dec = self._lifespan.with_global(self.client_token_global.value)
        dep = dec(lambda: auth_token).dependency
        call = cast("Callable[[fastapi.Request], Iterator[None]]", dep)
        return contextlib.contextmanager(call)(request)

    async def handle_new_identity(
        self,
        request: fastapi.Request,
        identity_id: uuid.UUID,
        token_data: Optional[core.TokenData],
    ) -> Optional[fastapi.Response]:
        if self.on_new_identity.is_set():
            if token_data is None:
                response = await self.on_new_identity.call(
                    request, identity_id, token_data
                )
            else:
                with self.with_auth_token(token_data.auth_token, request):
                    response = await self.on_new_identity.call(
                        request, identity_id, token_data
                    )
            if not isinstance(response, _NoopResponse):
                return response

        return None

    @property
    def email_password(self) -> EmailPassword:
        if self._email_password is None:
            if self.installed:
                raise ValueError(
                    "Cannot enable email_password after installation"
                )

            from ._email_password import EmailPassword  # noqa: PLC0415

            self._email_password = EmailPassword(self)

        return self._email_password

    def with_email_password(self, **kwargs: Any) -> Self:
        ep = self.email_password
        for key, value in kwargs.items():
            getattr(ep, key)(value)
        return self

    def without_email_password(self) -> Self:
        if self.installed:
            raise ValueError(
                "Cannot disable email_password after installation"
            )

        self._email_password = None
        self._auto_email_password = False
        return self

    @property
    def builtin_ui(self) -> BuiltinUI:
        if self._builtin_ui is None:
            if self.installed:
                raise ValueError("Cannot enable builtin_ui after installation")

            from ._builtin_ui import BuiltinUI  # noqa: PLC0415

            self._builtin_ui = BuiltinUI(self)

        return self._builtin_ui

    def with_builtin_ui(self, **kwargs: Any) -> Self:
        ui = self.builtin_ui
        for key, value in kwargs.items():
            getattr(ui, key)(value)
        return self

    def without_builtin_ui(self) -> Self:
        if self.installed:
            raise ValueError("Cannot disable builtin_ui after installation")

        self._builtin_ui = None
        self._auto_builtin_ui = False
        return self

    async def on_startup(self, app: fastapi.FastAPI) -> None:
        router = fastapi.APIRouter(
            prefix=self.auth_path_prefix.value,
            tags=self.tags.value,
            route_class=utils.ContentTypeRoute,
        )
        insts: list[Optional[Installable]] = []
        if self.auto_detection.value:
            config = await self._lifespan.client.query_single(
                """
                select assert_single(
                    cfg::Config.extensions[is ext::auth::AuthConfig]
                ) {
                    providers: { id, name },
                    ui,
                }
                """
            )
            if config:
                for provider in config.providers:
                    match provider.name:
                        case "builtin::local_emailpassword":
                            if (
                                self._auto_email_password
                                and self._email_password is None
                            ):
                                _ = self.email_password

                if (
                    config.ui is not None
                    and self._auto_builtin_ui
                    and self._builtin_ui is None
                ):
                    _ = self.builtin_ui

        insts.extend([self._email_password, self._builtin_ui])
        for inst in insts:
            if inst is not None:
                await inst.install(router)

        app.include_router(router)
        await super().on_startup(app)
