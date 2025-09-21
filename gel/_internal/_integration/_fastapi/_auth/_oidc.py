# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

from __future__ import annotations
from typing import Annotated, Optional

import http

import fastapi
from fastapi import responses
from starlette import concurrency

from gel.auth import oauth
from gel._internal._auth._oauth import TokenData  # noqa: TC001

from . import GelAuth, Installable
from .. import _utils as utils


class OpenIDConnect(Installable):
    _auth: GelAuth
    _core: oauth.AsyncOAuth
    _blocking_io_core: oauth.OAuth

    install_endpoints = utils.Config(True)  # noqa: FBT003

    # Authorize
    authorize_path = utils.Config("/{provider_name}/authorize")
    authorize_name = utils.Config("gel.auth.oidc.{provider_name}.authorize")
    authorize_summary = utils.Config(
        "Authorize with OpenID Connect provider: {provider_name}"
    )
    authorize_status_code = utils.Config(http.HTTPStatus.SEE_OTHER)

    # Callback
    callback_path = utils.Config("/{provider_name}/callback")
    callback_name = utils.Config("gel.auth.oidc.{provider_name}.callback")
    callback_summary = utils.Config(
        "Handle the OpenID Connect callback from provider: {provider_name}"
    )
    callback_default_response_class = utils.Config(responses.RedirectResponse)
    callback_default_status_code = utils.Config(http.HTTPStatus.SEE_OTHER)
    on_sign_in_complete: utils.Hook[TokenData] = utils.Hook("callback")
    on_sign_up_complete: utils.Hook[TokenData] = utils.Hook("callback")

    def __init__(self, auth: GelAuth, *, provider_name: str) -> None:
        self._auth = auth
        self._provider_name = provider_name

    def _redirect_success(self, request: fastapi.Request) -> fastapi.Response:
        response_class = self.callback_default_response_class.value
        response_code = self.callback_default_status_code.value
        redirect_to = self._auth.redirect_to.value
        redirect_to_page_name = self._auth.redirect_to_page_name.value
        if redirect_to_page_name is not None:
            return response_class(
                url=request.url_for(redirect_to_page_name),
                status_code=response_code,
            )
        elif redirect_to is not None:
            return response_class(url=redirect_to, status_code=response_code)
        else:
            raise RuntimeError(
                "GelAuth should have either redirect_to or "
                "redirect_to_page_name set"
            )

    def _redirect_error(
        self, request: fastapi.Request, **query_params: str
    ) -> fastapi.Response:
        response_class = self.callback_default_response_class.value
        return response_class(
            url=request.url_for(
                self._auth.error_page_name.value
            ).include_query_params(**query_params),
            status_code=self.callback_default_status_code.value,
        )

    def __install_authorize(self, router: fastapi.APIRouter) -> None:
        callback_name = self.callback_name.value.format(
            provider_name=self._provider_name
        )

        @router.get(
            self.authorize_path.value.format(
                provider_name=self._provider_name
            ),
            name=self.authorize_name.value.format(
                provider_name=self._provider_name
            ),
            summary=self.authorize_summary.value.format(
                provider_name=self._provider_name.title()
            ),
            response_class=responses.RedirectResponse,
            status_code=self.authorize_status_code.value,
        )
        async def authorize(
            request: fastapi.Request, response: fastapi.Response
        ) -> str:
            callback_url = request.url_for(callback_name)
            auth_data = self._core.authorize(
                redirect_to=str(callback_url),
                redirect_to_on_signup=str(
                    callback_url.replace_query_params(isSignUp=True)
                ),
            )
            self._auth.set_verifier_cookie(auth_data.verifier, response)
            return auth_data.redirect_url

        @router.get(
            self.callback_path.value.format(provider_name=self._provider_name),
            name=callback_name,
            summary=self.callback_summary.value.format(
                provider_name=self._provider_name.title()
            ),
        )
        async def callback(
            request: fastapi.Request,
            *,
            code: Optional[str] = None,
            error: Optional[str] = None,
            error_description: Optional[str] = None,
            verifier: str = fastapi.Depends(self._auth.pkce_verifier),
            is_sign_up: Annotated[
                bool, fastapi.Query(alias="isSignUp")
            ] = False,
        ) -> fastapi.Response:
            if code is None:
                assert error is not None
                args = {"error": error}
                if error_description is not None:
                    args["error_description"] = error_description
                return self._redirect_error(request, **args)

            token_data = await self._core.get_token(
                verifier=verifier, code=code
            )
            if is_sign_up:
                response = await self._auth.handle_new_identity(
                    request, token_data.identity_id, token_data
                )
                if response is None:
                    if self.on_sign_up_complete.is_set():
                        with self._auth.with_auth_token(
                            token_data.auth_token, request
                        ):
                            response = await self.on_sign_up_complete.call(
                                request, token_data
                            )
                    else:
                        response = self._redirect_success(request)
            else:
                if self.on_sign_in_complete.is_set():
                    with self._auth.with_auth_token(
                        token_data.auth_token, request
                    ):
                        response = await self.on_sign_in_complete.call(
                            request, token_data
                        )
                else:
                    response = self._redirect_success(request)
            self._auth.set_auth_cookie(
                token_data.auth_token, response=response
            )
            return response

    @property
    def blocking_io_core(self) -> oauth.OAuth:
        return self._blocking_io_core

    @property
    def core(self) -> oauth.AsyncOAuth:
        return self._core

    async def install(self, router: fastapi.APIRouter) -> None:
        self._core = await oauth.make_async(
            self._auth.client, provider_name=self._provider_name
        )
        self._blocking_io_core = await concurrency.run_in_threadpool(
            oauth.make,
            self._auth.blocking_io_client,
            provider_name=self._provider_name,
        )

        if self.install_endpoints.value:
            self.__install_authorize(router)

        await super().install(router)
