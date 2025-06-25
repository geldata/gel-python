# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

from __future__ import annotations
from typing import Annotated, Optional

import http
import logging

import pydantic
import fastapi
from fastapi import responses
from starlette import concurrency

from gel.auth import email_password as core

from . import GelAuth, Installable
from .. import _utils as utils


logger = logging.getLogger("gel.auth")


class SignUpBody(pydantic.BaseModel):
    email: str
    password: str


class SignInBody(pydantic.BaseModel):
    email: str
    password: str


class VerifyBody(pydantic.BaseModel):
    verification_token: str


class SendPasswordResetBody(pydantic.BaseModel):
    email: str


class ResetPasswordBody(pydantic.BaseModel):
    reset_token: str
    password: str


class EmailPassword(Installable):
    error_page_name = utils.Config("error_page")
    sign_in_page_name = utils.Config("sign_in_page")
    reset_password_page_name = utils.Config("reset_password_page")

    _auth: GelAuth
    _core: core.AsyncEmailPassword
    _blocking_io_core: core.EmailPassword

    # Sign-up
    install_sign_up = utils.Config(True)  # noqa: FBT003
    sign_up_path = utils.Config("/register")
    sign_up_name = utils.Config("gel.auth.email_password.sign_up")
    sign_up_summary = utils.Config("Sign up with email and password")
    sign_up_default_response_class = utils.Config(responses.RedirectResponse)
    sign_up_default_status_code = utils.Config(http.HTTPStatus.SEE_OTHER)
    on_sign_up_complete: utils.Hook[core.SignUpCompleteResponse] = utils.Hook(
        "sign_up"
    )
    on_sign_up_verification_required: utils.Hook[
        core.SignUpVerificationRequiredResponse
    ] = utils.Hook("sign_up")
    on_sign_up_failed: utils.Hook[core.SignUpFailedResponse] = utils.Hook(
        "sign_up"
    )

    # Sign-in
    install_sign_in = utils.Config(True)  # noqa: FBT003
    sign_in_path = utils.Config("/authenticate")
    sign_in_name = utils.Config("gel.auth.email_password.sign_in")
    sign_in_summary = utils.Config("Sign in with email and password")
    sign_in_default_response_class = utils.Config(responses.RedirectResponse)
    sign_in_default_status_code = utils.Config(http.HTTPStatus.SEE_OTHER)
    on_sign_in_complete: utils.Hook[core.SignInCompleteResponse] = utils.Hook(
        "sign_in"
    )
    on_sign_in_verification_required: utils.Hook[
        core.SignInVerificationRequiredResponse
    ] = utils.Hook("sign_in")
    on_sign_in_failed: utils.Hook[core.SignInFailedResponse] = utils.Hook(
        "sign_in"
    )

    # Email verification
    install_email_verification = utils.Config(True)  # noqa: FBT003
    email_verification_path = utils.Config("/verify")
    email_verification_name = utils.Config(
        "gel.auth.email_password.email_verification"
    )
    email_verification_summary = utils.Config("Verify the email address")
    email_verification_default_response_class = utils.Config(
        responses.RedirectResponse
    )
    email_verification_default_status_code = utils.Config(
        http.HTTPStatus.SEE_OTHER
    )
    on_email_verification_complete: utils.Hook[
        core.EmailVerificationCompleteResponse
    ] = utils.Hook("email_verification")
    on_email_verification_missing_proof: utils.Hook[
        core.EmailVerificationMissingProofResponse
    ] = utils.Hook("email_verification")
    on_email_verification_failed: utils.Hook[
        core.EmailVerificationFailedResponse
    ] = utils.Hook("email_verification")

    # Send password reset
    install_send_password_reset = utils.Config(True)  # noqa: FBT003
    send_password_reset_email_path = utils.Config("/send-password-reset")
    send_password_reset_email_name = utils.Config(
        "gel.auth.email_password.send_password_reset"
    )
    send_password_reset_email_summary = utils.Config(
        "Send a password reset email"
    )
    send_password_reset_email_default_response_class = utils.Config(
        responses.RedirectResponse
    )
    send_password_reset_email_default_status_code = utils.Config(
        http.HTTPStatus.SEE_OTHER
    )
    on_send_password_reset_email_complete: utils.Hook[
        core.SendPasswordResetEmailCompleteResponse
    ] = utils.Hook("send_password_reset_email")
    on_send_password_reset_email_failed: utils.Hook[
        core.SendPasswordResetEmailFailedResponse
    ] = utils.Hook("send_password_reset_email")

    # Reset password
    install_reset_password = utils.Config(True)  # noqa: FBT003
    reset_password_path = utils.Config("/reset-password")
    reset_password_name = utils.Config(
        "gel.auth.email_password.reset_password"
    )
    reset_password_summary = utils.Config("Reset the password")
    reset_password_default_response_class = utils.Config(
        responses.RedirectResponse
    )
    reset_password_default_status_code = utils.Config(
        http.HTTPStatus.SEE_OTHER
    )
    on_reset_password_complete: utils.Hook[
        core.PasswordResetCompleteResponse
    ] = utils.Hook("reset_password")
    on_reset_password_missing_proof: utils.Hook[
        core.PasswordResetMissingProofResponse
    ] = utils.Hook("reset_password")
    on_reset_password_failed: utils.Hook[core.PasswordResetFailedResponse] = (
        utils.Hook("reset_password")
    )

    def __init__(self, auth: GelAuth):
        self._auth = auth

    def _not_implemented(self, method: str) -> fastapi.Response:
        return responses.JSONResponse(
            status_code=http.HTTPStatus.NOT_IMPLEMENTED,
            content={"error": "not implemented", "method": method},
        )

    def _redirect_success(
        self,
        request: fastapi.Request,
        key: str,
        *,
        method: str,
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
            return self._not_implemented(method)

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
                self.error_page_name.value
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

    async def handle_sign_up_complete(
        self,
        request: fastapi.Request,
        result: core.SignUpCompleteResponse,
    ) -> fastapi.Response:
        response = await self._auth.handle_new_identity(
            request, result.identity_id, result.token_data
        )
        if response is None:
            if self.on_sign_up_complete.is_set():
                with self._auth.with_auth_token(
                    result.token_data.auth_token, request
                ):
                    response = await self.on_sign_up_complete.call(
                        request, result
                    )
            else:
                response = self._redirect_success(
                    request, "sign_up", method="on_sign_up_complete"
                )
        self._auth.set_auth_cookie(result.token_data.auth_token, response)
        return response

    async def handle_sign_up_verification_required(
        self,
        request: fastapi.Request,
        result: core.SignUpVerificationRequiredResponse,
    ) -> fastapi.Response:
        if result.identity_id:
            response = await self._auth.handle_new_identity(
                request, result.identity_id, None
            )
        else:
            response = None
        if response is None:
            if self.on_sign_up_verification_required.is_set():
                response = await self.on_sign_up_verification_required.call(
                    request, result
                )
            else:
                response = self._redirect_sign_in(
                    request, "sign_up", incomplete="verification_required"
                )
        self._auth.set_verifier_cookie(result.verifier, response)
        return response

    async def handle_sign_up_failed(
        self,
        request: fastapi.Request,
        result: core.SignUpFailedResponse,
    ) -> fastapi.Response:
        logger.info(
            "[%d] sign up failed: %s", result.status_code, result.message
        )
        logger.debug("%r", result)

        if self.on_sign_up_failed.is_set():
            response = await self.on_sign_up_failed.call(request, result)
        else:
            response = self._redirect_error(
                request, "sign_up", error=result.message
            )
        self._auth.set_verifier_cookie(result.verifier, response)
        return response

    def __install_sign_up(self, router: fastapi.APIRouter) -> None:
        @router.post(
            self.sign_up_path.value,
            name=self.sign_up_name.value,
            summary=self.sign_up_summary.value,
        )
        async def sign_up(
            sign_up_body: Annotated[
                SignUpBody, utils.OneOf(fastapi.Form(), fastapi.Body())
            ],
            request: fastapi.Request,
        ) -> fastapi.Response:
            result = await self._core.sign_up(
                sign_up_body.email,
                sign_up_body.password,
                verify_url=str(
                    request.url_for(self.email_verification_name.value)
                ),
            )
            match result:
                case core.SignUpCompleteResponse():
                    return await self.handle_sign_up_complete(request, result)
                case core.SignUpVerificationRequiredResponse():
                    return await self.handle_sign_up_verification_required(
                        request, result
                    )
                case core.SignUpFailedResponse():
                    return await self.handle_sign_up_failed(request, result)
                case _:
                    raise AssertionError("Invalid sign up response")

    async def handle_sign_in_complete(
        self,
        request: fastapi.Request,
        result: core.SignInCompleteResponse,
    ) -> fastapi.Response:
        if self.on_sign_in_complete.is_set():
            with self._auth.with_auth_token(
                result.token_data.auth_token, request
            ):
                response = await self.on_sign_in_complete.call(request, result)
        else:
            response = self._redirect_success(
                request, "sign_in", method="on_sign_in_complete"
            )
        self._auth.set_auth_cookie(result.token_data.auth_token, response)
        return response

    async def handle_sign_in_verification_required(
        self,
        request: fastapi.Request,
        result: core.SignInVerificationRequiredResponse,
    ) -> fastapi.Response:
        if self.on_sign_in_verification_required.is_set():
            response = await self.on_sign_in_verification_required.call(
                request, result
            )
        else:
            response = self._redirect_sign_in(
                request, "sign_in", incomplete="verification_required"
            )
        self._auth.set_verifier_cookie(result.verifier, response)
        return response

    async def handle_sign_in_failed(
        self,
        request: fastapi.Request,
        result: core.SignInFailedResponse,
    ) -> fastapi.Response:
        logger.info(
            "[%d] sign in failed: %s", result.status_code, result.message
        )
        logger.debug("%r", result)

        if self.on_sign_in_failed.is_set():
            response = await self.on_sign_in_failed.call(request, result)
        else:
            response = self._redirect_error(
                request, "sign_in", error=result.message
            )
        self._auth.set_verifier_cookie(result.verifier, response)
        return response

    def __install_sign_in(self, router: fastapi.APIRouter) -> None:
        @router.post(
            self.sign_in_path.value,
            name=self.sign_in_name.value,
            summary=self.sign_in_summary.value,
        )
        async def sign_in(
            sign_in_body: Annotated[
                SignInBody, utils.OneOf(fastapi.Form(), fastapi.Body())
            ],
            request: fastapi.Request,
        ) -> fastapi.Response:
            result = await self._core.sign_in(
                sign_in_body.email, sign_in_body.password
            )
            match result:
                case core.SignInCompleteResponse():
                    return await self.handle_sign_in_complete(request, result)
                case core.SignInVerificationRequiredResponse():
                    return await self.handle_sign_in_verification_required(
                        request, result
                    )
                case core.SignInFailedResponse():
                    return await self.handle_sign_in_failed(request, result)
                case _:
                    raise AssertionError("Invalid sign in response")

    async def handle_email_verification_complete(
        self,
        request: fastapi.Request,
        result: core.EmailVerificationCompleteResponse,
    ) -> fastapi.Response:
        if self.on_email_verification_complete.is_set():
            with self._auth.with_auth_token(
                result.token_data.auth_token, request
            ):
                response = await self.on_email_verification_complete.call(
                    request, result
                )
        else:
            response = self._redirect_success(
                request,
                "email_verification",
                method="on_email_verification_complete",
            )
        self._auth.set_auth_cookie(result.token_data.auth_token, response)
        return response

    async def handle_email_verification_missing_proof(
        self,
        request: fastapi.Request,
        result: core.EmailVerificationMissingProofResponse,
    ) -> fastapi.Response:
        if self.on_email_verification_missing_proof.is_set():
            return await self.on_email_verification_missing_proof.call(
                request, result
            )
        else:
            return self._redirect_sign_in(
                request, "email_verification", incomplete="verify"
            )

    async def handle_email_verification_failed(
        self,
        request: fastapi.Request,
        result: core.EmailVerificationFailedResponse,
    ) -> fastapi.Response:
        logger.info(
            "[%d] email verification failed: %s",
            result.status_code,
            result.message,
        )
        logger.debug("%r", result)

        if self.on_email_verification_failed.is_set():
            return await self.on_email_verification_failed.call(
                request, result
            )
        else:
            return self._redirect_error(
                request, "email_verification", error=result.message
            )

    def __install_email_verification(self, router: fastapi.APIRouter) -> None:
        @router.get(
            self.email_verification_path.value,
            name=self.email_verification_name.value,
            summary=self.email_verification_summary.value,
        )
        async def verify(
            request: fastapi.Request,
            verify_body: Annotated[VerifyBody, fastapi.Query()],
            verifier: Optional[str] = fastapi.Depends(
                self._auth.pkce_verifier
            ),
        ) -> fastapi.Response:
            result = await self._core.verify_email(
                verify_body.verification_token, verifier
            )
            match result:
                case core.EmailVerificationCompleteResponse():
                    return await self.handle_email_verification_complete(
                        request, result
                    )
                case core.EmailVerificationMissingProofResponse():
                    return await self.handle_email_verification_missing_proof(
                        request, result
                    )
                case core.EmailVerificationFailedResponse():
                    return await self.handle_email_verification_failed(
                        request, result
                    )
                case _:
                    raise AssertionError("Invalid email verification response")

    async def handle_send_password_reset_email_complete(
        self,
        request: fastapi.Request,
        result: core.SendPasswordResetEmailCompleteResponse,
    ) -> fastapi.Response:
        if self.on_send_password_reset_email_complete.is_set():
            response = await self.on_send_password_reset_email_complete.call(
                request, result
            )
        else:
            response = self._redirect_sign_in(
                request,
                "send_password_reset_email",
                incomplete="password_reset_sent",
            )
        self._auth.set_verifier_cookie(result.verifier, response)
        return response

    async def handle_send_password_reset_email_failed(
        self,
        request: fastapi.Request,
        result: core.SendPasswordResetEmailFailedResponse,
    ) -> fastapi.Response:
        logger.info(
            "[%d] send password reset email failed: %s",
            result.status_code,
            result.message,
        )
        logger.debug("%r", result)

        if self.on_send_password_reset_email_failed.is_set():
            response = await self.on_send_password_reset_email_failed.call(
                request, result
            )
        else:
            response = self._redirect_error(
                request, "send_password_reset_email", error=result.message
            )
        self._auth.set_verifier_cookie(result.verifier, response)
        return response

    def __install_send_password_reset(self, router: fastapi.APIRouter) -> None:
        @router.post(
            self.send_password_reset_email_path.value,
            name=self.send_password_reset_email_name.value,
            summary=self.send_password_reset_email_summary.value,
        )
        async def send_password_reset(
            send_password_reset_body: Annotated[
                SendPasswordResetBody,
                utils.OneOf(fastapi.Form(), fastapi.Body()),
            ],
            request: fastapi.Request,
        ) -> fastapi.Response:
            result = await self._core.send_password_reset_email(
                send_password_reset_body.email,
                reset_url=str(
                    request.url_for(self.reset_password_page_name.value)
                ),
            )
            match result:
                case core.SendPasswordResetEmailCompleteResponse():
                    return (
                        await self.handle_send_password_reset_email_complete(
                            request, result
                        )
                    )
                case core.SendPasswordResetEmailFailedResponse():
                    return await self.handle_send_password_reset_email_failed(
                        request, result
                    )
                case _:
                    raise AssertionError(
                        "Invalid send password reset response"
                    )

    async def handle_reset_password_complete(
        self,
        request: fastapi.Request,
        result: core.PasswordResetCompleteResponse,
    ) -> fastapi.Response:
        if self.on_reset_password_complete.is_set():
            with self._auth.with_auth_token(
                result.token_data.auth_token, request
            ):
                response = await self.on_reset_password_complete.call(
                    request, result
                )
        else:
            response = self._redirect_success(
                request, "reset_password", method="on_reset_password_complete"
            )
        self._auth.set_auth_cookie(result.token_data.auth_token, response)
        return response

    async def handle_reset_password_missing_proof(
        self,
        request: fastapi.Request,
        result: core.PasswordResetMissingProofResponse,
    ) -> fastapi.Response:
        if self.on_reset_password_missing_proof.is_set():
            return await self.on_reset_password_missing_proof.call(
                request, result
            )
        else:
            return self._redirect_sign_in(
                request, "reset_password", incomplete="reset_password"
            )

    async def handle_reset_password_failed(
        self,
        request: fastapi.Request,
        result: core.PasswordResetFailedResponse,
    ) -> fastapi.Response:
        logger.info(
            "[%d] password reset failed: %s",
            result.status_code,
            result.message,
        )
        logger.debug("%r", result)

        if self.on_reset_password_failed.is_set():
            return await self.on_reset_password_failed.call(request, result)
        else:
            return self._redirect_error(
                request, "reset_password", error=result.message
            )

    def __install_reset_password(self, router: fastapi.APIRouter) -> None:
        @router.post(
            self.reset_password_path.value,
            name=self.reset_password_name.value,
            summary=self.reset_password_summary.value,
        )
        async def reset_password(
            request: fastapi.Request,
            reset_password_body: Annotated[
                ResetPasswordBody, utils.OneOf(fastapi.Form(), fastapi.Body())
            ],
            verifier: Optional[str] = fastapi.Depends(
                self._auth.pkce_verifier
            ),
        ) -> fastapi.Response:
            result = await self._core.reset_password(
                reset_token=reset_password_body.reset_token,
                verifier=verifier,
                password=reset_password_body.password,
            )
            match result:
                case core.PasswordResetCompleteResponse():
                    return await self.handle_reset_password_complete(
                        request, result
                    )
                case core.PasswordResetMissingProofResponse():
                    return await self.handle_reset_password_missing_proof(
                        request, result
                    )
                case core.PasswordResetFailedResponse():
                    return await self.handle_reset_password_failed(
                        request, result
                    )
                case _:
                    raise AssertionError("Invalid reset password response")

    @property
    def blocking_io_core(self) -> core.EmailPassword:
        return self._blocking_io_core

    @property
    def core(self) -> core.AsyncEmailPassword:
        return self._core

    async def install(self, router: fastapi.APIRouter) -> None:
        self._core = await core.make_async(self._auth.client)
        self._blocking_io_core = await concurrency.run_in_threadpool(
            core.make, self._auth.blocking_io_client
        )
        if self.install_sign_up.value:
            self.__install_sign_up(router)
        if self.install_sign_in.value:
            self.__install_sign_in(router)
        if self.install_email_verification.value:
            self.__install_email_verification(router)
        if self.install_send_password_reset.value:
            self.__install_send_password_reset(router)
        if self.install_reset_password.value:
            self.__install_reset_password(router)
        await super().install(router)
