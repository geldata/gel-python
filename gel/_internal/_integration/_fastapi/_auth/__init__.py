# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

from __future__ import annotations
from typing import cast, Optional, TYPE_CHECKING

import contextlib
import datetime

import fastapi
import jwt
from fastapi import security, params


from .. import _client as client_mod
from .. import _utils as utils

if TYPE_CHECKING:
    import enum
    import uuid
    from collections.abc import Callable, Iterator

    import gel
    from gel import auth as core
    from .email_password import EmailPassword
    from .builtin_ui import BuiltinUI


class Installable:
    def install(self, router: fastapi.APIRouter) -> None:
        raise NotImplementedError


class _NoopResponse(fastapi.Response):
    pass


class GelAuth(client_mod.Extension):
    auth_path_prefix: str = "/auth"
    auth_cookie_name: str = "gel_auth_token"
    verifier_cookie_name: str = "gel_verifier"
    tags: list[str | enum.Enum]
    secure_cookie: bool = True
    email_password: EmailPassword
    builtin_ui: BuiltinUI

    _on_new_identity_path: str = "/"
    _on_new_identity_name: str = "gel.fastapi.auth.on_new_identity"
    _on_new_identity_default_response_class = _NoopResponse
    on_new_identity: utils.Hook[tuple[uuid.UUID, Optional[core.TokenData]]] = (
        utils.Hook("_on_new_identity")
    )

    _pkce_verifier: Optional[security.APIKeyCookie] = None
    _maybe_auth_token: Optional[security.APIKeyCookie] = None
    _auth_token: Optional[security.APIKeyCookie] = None
    _insts: dict[str, Installable]

    def _post_init(self) -> None:
        from .email_password import EmailPassword  # noqa: PLC0415
        from .builtin_ui import BuiltinUI  # noqa: PLC0415

        self.tags = ["Gel Auth"]
        self.email_password = EmailPassword(self)
        self.builtin_ui = BuiltinUI(self)

        self._insts = {
            "builtin::local_emailpassword": self.email_password,
        }

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
            key=self.auth_cookie_name,
            value=token,
            httponly=True,
            secure=self.secure_cookie,
            samesite="lax",
            expires=exp,
        )

    def set_verifier_cookie(
        self, verifier: str, response: fastapi.Response
    ) -> None:
        response.set_cookie(
            key=self.verifier_cookie_name,
            value=verifier,
            httponly=True,
            secure=self.secure_cookie,
            samesite="lax",
            expires=int(datetime.timedelta(days=7).total_seconds()),
        )

    @property
    def pkce_verifier(self) -> security.APIKeyCookie:
        if self._pkce_verifier is None:
            self._pkce_verifier = security.APIKeyCookie(
                name=self.verifier_cookie_name,
                description="The cookie as the PKCE verifier",
                auto_error=False,
            )
        return self._pkce_verifier

    @property
    def client(self) -> gel.AsyncIOClient:
        return self._lifespan.client

    def _make_auth_token_dependency(
        self, *, auto_error: bool, cache_name: str
    ) -> params.Depends:
        auth_token_cookie = getattr(self, cache_name, None)
        if auth_token_cookie is None:
            auth_token_cookie = security.APIKeyCookie(
                name=self.auth_cookie_name,
                description="The cookie as the authentication token",
                auto_error=auto_error,
            )
            setattr(self, cache_name, auth_token_cookie)

        def get_auth_token(
            auth_token: Optional[str] = fastapi.Depends(auth_token_cookie),
        ) -> Optional[str]:
            return auth_token

        return self._lifespan.with_global("ext::auth::client_token")(
            get_auth_token
        )

    @property
    def maybe_auth_token(self) -> params.Depends:
        return self._make_auth_token_dependency(
            auto_error=False, cache_name="_maybe_auth_token"
        )

    @property
    def auth_token(self) -> params.Depends:
        return self._make_auth_token_dependency(
            auto_error=True, cache_name="_auth_token"
        )

    async def handle_new_identity(
        self,
        request: fastapi.Request,
        identity_id: uuid.UUID,
        token_data: Optional[core.TokenData],
    ) -> Optional[fastapi.Response]:
        if self.on_new_identity.is_set():
            result = (identity_id, token_data)
            if token_data is None:
                response = await self.on_new_identity.call(request, result)
            else:
                dec = self._lifespan.with_global("ext::auth::client_token")
                dep = dec(lambda: token_data.auth_token).dependency
                call = cast("Callable[[fastapi.Request], Iterator[None]]", dep)
                ctx = contextlib.contextmanager(call)
                with ctx(request):
                    response = await self.on_new_identity.call(request, result)
            if not isinstance(response, _NoopResponse):
                return response

        return None

    async def on_startup(self, app: fastapi.FastAPI) -> None:
        router = fastapi.APIRouter(
            prefix=self.auth_path_prefix,
            tags=self.tags,
        )
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
                inst = self._insts.get(provider.name)
                if inst is not None:
                    inst.install(router)

            if config.ui is not None:
                self.builtin_ui.install(router)

        app.include_router(router)
