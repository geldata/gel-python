# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

from __future__ import annotations
from typing import Annotated, Optional

import http
import logging

import fastapi
import pydantic
from fastapi import responses
from starlette import concurrency

from gel.auth import magic_link as core, TokenData

from . import GelAuth, Installable
from .. import _utils as utils


logger = logging.getLogger("gel.auth")


class RequestBody(pydantic.BaseModel):
    email: str


class AuthenticatedBody(pydantic.BaseModel):
    email: str
    code: str


class MagicLink(Installable):
    sign_in_page_name = utils.Config("sign_in_page")

    _auth: GelAuth
    _core: (
        core.AsyncLegacyMagicLink | core.AsyncMagicLink | core.AsyncMagicCode
    )
    _blocking_io_core: core.LegacyMagicLink | core.MagicLink | core.MagicCode

    install_endpoints = utils.Config(True)  # noqa: FBT003

    # Request for magic link/code
    request_body: utils.ConfigDecorator[type[RequestBody]] = (
        utils.ConfigDecorator(RequestBody)
    )
    request_path = utils.Config("/magic-link")
    request_name = utils.Config("gel.auth.magic_link.request")
    request_summary = utils.Config("Request for magic link or code")
    request_default_response_class = utils.Config(responses.RedirectResponse)
    request_default_status_code = utils.Config(http.HTTPStatus.SEE_OTHER)
    on_magic_link_sent: utils.Hook[RequestBody, core.MagicLinkSentResponse] = (
        utils.Hook("request")
    )
    on_magic_link_failed: utils.Hook[
        RequestBody, core.MagicLinkFailedResponse
    ] = utils.Hook("request")
    on_magic_code_sent: utils.Hook[RequestBody, core.MagicCodeSentResponse] = (
        utils.Hook("request")
    )
    on_magic_code_failed: utils.Hook[
        RequestBody, core.MagicCodeFailedResponse
    ] = utils.Hook("request")

    # Callback
    authenticate_body: utils.ConfigDecorator[type[AuthenticatedBody]] = (
        utils.ConfigDecorator(AuthenticatedBody)
    )
    authenticate_path = utils.Config("/magic-link")
    authenticate_name = utils.Config("gel.auth.magic_link.authenticate")
    authenticate_summary = utils.Config("Handle the magic link authentication")
    authenticate_default_response_class = utils.Config(
        responses.RedirectResponse
    )
    authenticate_default_status_code = utils.Config(http.HTTPStatus.SEE_OTHER)
    on_authenticated: utils.Hook[TokenData] = utils.Hook("authenticate")

    def __init__(self, auth: GelAuth):
        self._auth = auth

    def _redirect_success(
        self,
        request: fastapi.Request,
        key: str,
    ) -> fastapi.Response:
        response_class: type[responses.RedirectResponse] = getattr(
            self, f"{key}_default_response_class"
        ).value
        response_code = getattr(self, f"{key}_default_status_code").value
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
        self,
        request: fastapi.Request,
        key: str,
        **query_params: str,
    ) -> fastapi.Response:
        response_class: type[responses.RedirectResponse] = getattr(
            self, f"{key}_default_response_class"
        ).value
        return response_class(
            url=request.url_for(
                self._auth.error_page_name.value
            ).include_query_params(**query_params),
            status_code=getattr(self, f"{key}_default_status_code").value,
        )

    def _redirect_sign_in(
        self,
        request: fastapi.Request,
        key: str,
        **query_params: str,
    ) -> fastapi.Response:
        response_class: type[responses.RedirectResponse] = getattr(
            self, f"{key}_default_response_class"
        ).value
        return response_class(
            url=request.url_for(
                self.sign_in_page_name.value
            ).include_query_params(**query_params),
            status_code=getattr(self, f"{key}_default_status_code").value,
        )

    async def handle_request_link_result(
        self,
        request: fastapi.Request,
        body: RequestBody,
        result: core.MagicLinkSentResponse | core.MagicLinkFailedResponse,
    ) -> fastapi.Response:
        match result:
            case core.MagicLinkSentResponse(email_sent=email, signup=signup):
                if signup:
                    # The Gel server is not returning the identity_id on
                    # sign-up magic link request, so we need to fetch it here.
                    identity_id = (
                        await self._auth.client.query_required_single(
                            """
                            select (
                                select ext::auth::MagicLinkFactor
                                filter .email = <str>$email
                            ).identity.id;
                            """,
                            email=email,
                        )
                    )
                    id_response = await self._auth.handle_new_identity(
                        request, identity_id, None
                    )
                else:
                    id_response = None
                if id_response is not None:
                    response = id_response
                elif self.on_magic_link_sent.is_set():
                    response = await self.on_magic_link_sent.call(
                        request, body, result
                    )
                else:
                    response = self._redirect_sign_in(
                        request, "request", incomplete="magic_link_sent"
                    )

            case core.MagicLinkFailedResponse():
                logger.info(
                    "[%d] failed requesting for magic link: %s",
                    result.status_code,
                    result.message,
                )
                logger.debug("%r", result)

                if self.on_magic_link_failed.is_set():
                    response = await self.on_magic_link_failed.call(
                        request, body, result
                    )
                else:
                    response = self._redirect_error(
                        request, "request", error=result.message
                    )

            case _:
                raise AssertionError("request returned unknown response")

        self._auth.set_verifier_cookie(result.verifier, response)
        return response

    async def handle_pkce_code_with_verifier(
        self,
        code: str,
        *,
        request: fastapi.Request,
        verifier: Optional[str] = None,
    ) -> fastapi.Response:
        try:
            token_data = await self._core.get_token(
                verifier=verifier, code=code
            )
        except Exception as e:
            response = self._redirect_error(
                request, "authenticate", error=str(e)
            )
        else:
            if self.on_authenticated.is_set():
                with self._auth.with_auth_token(
                    token_data.auth_token, request
                ):
                    response = await self.on_authenticated.call(
                        request, token_data
                    )
            else:
                response = self._redirect_success(request, "on_authenticated")
            self._auth.set_auth_cookie(
                token_data.auth_token, response=response
            )
        return response

    def __install_legacy_request_link(
        self,
        router: fastapi.APIRouter,
        magic_link: core.AsyncLegacyMagicLink,
    ) -> None:
        async def request_link(
            request_body: Annotated[
                RequestBody, utils.OneOf(fastapi.Form(), fastapi.Body())
            ],
            is_sign_up: Annotated[
                bool,
                fastapi.Query(alias="isSignUp", default=False),
            ],
            request: fastapi.Request,
        ) -> fastapi.Response:
            callback_url = str(request.url_for(self.authenticate_name.value))
            redirect_on_failure = str(
                request.url_for(self._auth.error_page_name.value)
            )
            if is_sign_up:
                result = await magic_link.sign_up(
                    request_body.email,
                    callback_url=callback_url,
                    redirect_on_failure=redirect_on_failure,
                )
            else:
                result = await magic_link.sign_in(
                    request_body.email,
                    callback_url=callback_url,
                    redirect_on_failure=redirect_on_failure,
                )
            return await self.handle_request_link_result(
                request, request_body, result
            )

        request_link.__globals__["RequestBody"] = self.request_body.value

        router.post(
            self.request_path.value,
            name=self.request_name.value,
            summary=self.request_summary.value,
        )(request_link)

    def __install_legacy_authenticate_link(
        self, router: fastapi.APIRouter
    ) -> None:
        @router.get(
            self.authenticate_path.value,
            name=self.authenticate_name.value,
            summary=self.authenticate_summary.value,
        )
        async def authenticate(
            request: fastapi.Request,
            code: str,
            verifier: Optional[str] = fastapi.Depends(
                self._auth.pkce_verifier
            ),
        ) -> fastapi.Response:
            return await self.handle_pkce_code_with_verifier(
                code, request=request, verifier=verifier
            )

    def __install_request_link(
        self,
        router: fastapi.APIRouter,
        magic_link: core.AsyncMagicLink,
    ) -> None:
        async def request_link(
            request_body: Annotated[
                RequestBody, utils.OneOf(fastapi.Form(), fastapi.Body())
            ],
            is_sign_up: Annotated[
                bool,
                fastapi.Query(alias="isSignUp", default=False),
            ],
            request: fastapi.Request,
        ) -> fastapi.Response:
            callback_url = str(request.url_for(self.authenticate_name.value))
            redirect_on_failure = str(
                request.url_for(self._auth.error_page_name.value)
            )
            if is_sign_up:
                result = await magic_link.sign_up(
                    request_body.email,
                    callback_url=callback_url,  # not used in favor of link_url
                    redirect_on_failure=redirect_on_failure,
                    link_url=callback_url,
                )
            else:
                result = await magic_link.sign_in(
                    request_body.email,
                    callback_url=callback_url,  # not used in favor of link_url
                    redirect_on_failure=redirect_on_failure,
                    link_url=callback_url,
                )
            return await self.handle_request_link_result(
                request, request_body, result
            )

        request_link.__globals__["RequestBody"] = self.request_body.value

        router.post(
            self.request_path.value,
            name=self.request_name.value,
            summary=self.request_summary.value,
        )(request_link)

    def __install_authenticate_link(
        self, router: fastapi.APIRouter, magic_link: core.AsyncMagicLink
    ) -> None:
        @router.get(
            self.authenticate_path.value,
            name=self.authenticate_name.value,
            summary=self.authenticate_summary.value,
        )
        async def authenticate(
            request: fastapi.Request,
            token: str,
            verifier: Optional[str] = fastapi.Depends(
                self._auth.pkce_verifier
            ),
        ) -> fastapi.Response:
            response = await magic_link.authenticate(token)
            match response:
                case core.AuthenticateLinkResultResponse(code=code):
                    return await self.handle_pkce_code_with_verifier(
                        code, request=request, verifier=verifier
                    )

                case core.AuthenticateLinkFailedResponse():
                    logger.info(
                        "[%d] failed authenticating magic link: %s",
                        response.status_code,
                        response.message,
                    )
                    return self._redirect_error(
                        request, "authenticate", error=response.message
                    )

                case _:
                    raise AssertionError(
                        "authenticate returned unknown response"
                    )

    async def _install_link(
        self, router: fastapi.APIRouter, server_major_version: int
    ) -> None:
        self._blocking_io_core = await concurrency.run_in_threadpool(
            core.make,
            self._auth.blocking_io_core,
            server_major_version=server_major_version,
            verification_method=core.VerificationMethod.LINK,
        )
        if server_major_version == 5:
            self._core = legacy_magic_link = await core.make_async(
                self._auth.client,
                server_major_version=5,
            )
            if self.install_endpoints.value:
                self.__install_legacy_request_link(router, legacy_magic_link)
                self.__install_legacy_authenticate_link(router)
        else:
            self._core = magic_link = await core.make_async(
                self._auth.client,
                server_major_version=server_major_version,
                verification_method=core.VerificationMethod.LINK,
            )
            if self.install_endpoints.value:
                self.__install_request_link(router, magic_link)
                self.__install_authenticate_link(router, magic_link)

    def __install_request_code(
        self,
        router: fastapi.APIRouter,
        magic_code: core.AsyncMagicCode,
    ) -> None:
        async def request_code(
            request_body: Annotated[
                RequestBody, utils.OneOf(fastapi.Form(), fastapi.Body())
            ],
            is_sign_up: Annotated[
                bool,
                fastapi.Query(alias="isSignUp", default=False),
            ],
            request: fastapi.Request,
        ) -> fastapi.Response:
            if is_sign_up:
                result = await magic_code.sign_up(request_body.email)
            else:
                result = await magic_code.sign_in(request_body.email)
            match result:
                case core.MagicCodeSentResponse(signup=signup, email=email):
                    if signup:
                        # The Gel server is not returning the identity_id on
                        # sign-up magic code request, so we need to fetch it.
                        identity_id = (
                            await self._auth.client.query_required_single(
                                """
                                select (
                                    select ext::auth::MagicCodeFactor
                                    filter .email = <str>$email
                                ).identity.id;
                                """,
                                email=email,
                            )
                        )
                        id_response = await self._auth.handle_new_identity(
                            request, identity_id, None
                        )
                    else:
                        id_response = None
                    if id_response is not None:
                        return id_response
                    elif self.on_magic_code_sent.is_set():
                        return await self.on_magic_code_sent.call(
                            request, request_body, result
                        )
                    else:
                        return self._redirect_sign_in(
                            request, "request", incomplete="magic_code_sent"
                        )

                case core.MagicCodeFailedResponse():
                    logger.info(
                        "[%d] failed requesting for magic code: %s",
                        result.status_code,
                        result.message,
                    )
                    logger.debug("%r", result)

                    if self.on_magic_code_failed.is_set():
                        return await self.on_magic_code_failed.call(
                            request, request_body, result
                        )
                    else:
                        return self._redirect_error(
                            request, "request", error=result.message
                        )

                case _:
                    raise AssertionError("request returned unknown response")

        request_code.__globals__["RequestBody"] = self.request_body.value

        router.post(
            self.request_path.value,
            name=self.request_name.value,
            summary=self.request_summary.value,
        )(request_code)

    def __install_authenticate_code(
        self, router: fastapi.APIRouter, magic_code: core.AsyncMagicCode
    ) -> None:
        @router.put(
            self.authenticate_path.value,
            name=self.authenticate_name.value,
            summary=self.authenticate_summary.value,
        )
        async def authenticate(
            request: fastapi.Request,
            body: Annotated[
                AuthenticatedBody,
                utils.OneOf(fastapi.Form(), fastapi.Body()),
            ],
        ) -> fastapi.Response:
            result = await magic_code.authenticate(body.email, body.code)
            match result:
                case core.AuthenticateCodeResultResponse():
                    return await self.handle_pkce_code_with_verifier(
                        result.code, request=request, verifier=result.verifier
                    )

                case core.AuthenticateCodeFailedResponse():
                    logger.info(
                        "[%d] failed authenticating magic code: %s",
                        result.status_code,
                        result.message,
                    )
                    logger.debug("%r", result)
                    return self._redirect_error(
                        request, "authenticate", error=result.message
                    )

                case _:
                    raise AssertionError(
                        "authenticate returned unknown response"
                    )

    async def _install_code(
        self, router: fastapi.APIRouter, server_major_version: int
    ) -> None:
        self._blocking_io_core = await concurrency.run_in_threadpool(
            core.make,
            self._auth.blocking_io_core,
            server_major_version=server_major_version,
            verification_method=core.VerificationMethod.CODE,
        )
        self._core = magic_code = await core.make_async(
            self._auth.client,
            server_major_version=server_major_version,
            verification_method=core.VerificationMethod.CODE,
        )
        if self.install_endpoints.value:
            self.__install_request_code(router, magic_code)
            self.__install_authenticate_code(router, magic_code)

    async def install(self, router: fastapi.APIRouter) -> None:
        config = await self._auth.client.query_required_single(
            """
            select assert_single(
                cfg::Config.extensions[is ext::auth::AuthConfig]
                .providers[is ext::auth::MagicLinkProviderConfig]
            ) {
                *,
                server_major_version := (select sys::get_version()).major,
            }
            """
        )
        server_major_version = config.server_major_version
        if hasattr(config, "verification_method"):
            method = core.VerificationMethod(config.verification_method)
        else:
            method = core.VerificationMethod.LINK
        if method == core.VerificationMethod.LINK:
            await self._install_link(router, server_major_version)
        else:
            await self._install_code(router, server_major_version)

        await super().install(router)
