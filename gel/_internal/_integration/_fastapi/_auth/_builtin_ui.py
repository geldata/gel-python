# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

from __future__ import annotations
from typing import Annotated, Any, Literal, Optional, TYPE_CHECKING

import datetime  # noqa: TC003  # for runtime type annotations
import http
import inspect
import itertools
import logging
import urllib.parse
import uuid  # noqa: TC003  # for runtime type annotations

import fastapi
import pydantic
from fastapi import responses
from starlette import concurrency

from gel.auth import builtin_ui as core

from . import GelAuth
from . import Installable
from .. import _utils as utils

if TYPE_CHECKING:
    from collections.abc import Callable
    from gel._internal._auth._token_data import TokenData


_logger = logging.getLogger("gel.fastapi")
InstallMode = Literal["no", "strict", "permissive"]


class BuiltinUI(Installable):
    _auth: GelAuth
    _core: core.AsyncBuiltinUI
    _blocking_io_core: core.BuiltinUI

    install_sign_in_page = utils.Config(True)  # noqa: FBT003
    sign_in_path = utils.Config("/sign-in")
    sign_in_name = utils.Config("gel.auth.builtin_ui.sign_in")
    sign_in_summary = utils.Config(
        "Redirect to the sign-in page of the built-in UI"
    )
    sign_in_default_response_class = utils.Config(responses.RedirectResponse)
    sign_in_default_status_code = utils.Config(http.HTTPStatus.SEE_OTHER)

    install_sign_up_page = utils.Config(True)  # noqa: FBT003
    sign_up_path = utils.Config("/sign-up")
    sign_up_name = utils.Config("gel.auth.builtin_ui.sign_up")
    sign_up_summary = utils.Config(
        "Redirect to the sign-up page of the built-in UI"
    )
    sign_up_default_response_class = utils.Config(responses.RedirectResponse)
    sign_up_default_status_code = utils.Config(http.HTTPStatus.SEE_OTHER)

    install_callback: utils.Config[InstallMode] = utils.Config("strict")
    is_sign_up_alias = utils.Config("isSignUp")
    callback_path = utils.Config("")
    callback_name = utils.Config("gel.auth.builtin_ui.callback")
    callback_summary = utils.Config("Built-in UI callback handler")
    callback_default_response_class = utils.Config(responses.RedirectResponse)
    callback_default_status_code = utils.Config(http.HTTPStatus.SEE_OTHER)
    on_sign_up_verification_required: utils.Hook[
        Optional[datetime.datetime]
    ] = utils.Hook("callback")
    on_sign_in_complete: utils.Hook[TokenData] = utils.Hook("callback")
    on_sign_up_complete: utils.Hook[TokenData] = utils.Hook("callback")

    def __init__(self, auth: GelAuth) -> None:
        self._auth = auth

    def __install_sign_in_page(self, router: fastapi.APIRouter) -> None:
        @router.get(
            self.sign_in_path.value,
            name=self.sign_in_name.value,
            summary=self.sign_in_summary.value,
            response_class=self.sign_in_default_response_class.value,
            status_code=self.sign_in_default_status_code.value,
        )
        async def sign_in(response: fastapi.Response) -> str:
            result = self._core.start_sign_in()
            self._auth.set_verifier_cookie(result.verifier, response)
            return str(result.redirect_url)

    def __install_sign_up_page(self, router: fastapi.APIRouter) -> None:
        @router.get(
            self.sign_up_path.value,
            name=self.sign_up_name.value,
            summary=self.sign_up_summary.value,
            response_class=self.sign_up_default_response_class.value,
            status_code=self.sign_up_default_status_code.value,
        )
        async def sign_up(response: fastapi.Response) -> str:
            result = self._core.start_sign_up()
            self._auth.set_verifier_cookie(result.verifier, response)
            return str(result.redirect_url)

    def _redirect_success(
        self,
        request: fastapi.Request,
        *,
        method: str,
    ) -> fastapi.Response:
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
            return responses.JSONResponse(
                status_code=http.HTTPStatus.NOT_IMPLEMENTED,
                content={"error": "not implemented", "method": method},
            )

    def _redirect_error(self, message: str) -> fastapi.Response:
        result = self._core.start_sign_in(error=message)
        response = self.callback_default_response_class.value(
            url=str(result.redirect_url),
            status_code=self.callback_default_status_code.value,
        )
        self._auth.set_verifier_cookie(result.verifier, response)
        return response

    def make_auth_callback_handler(
        self,
        *,
        for_sign_in: bool = True,
        for_sign_up: bool = True,
    ) -> Callable[..., Any]:
        if not (for_sign_in or for_sign_up):
            raise ValueError(
                "At least one of for_sign_in or for_sign_up must be True"
            )

        pkce_verifier = self._auth.pkce_verifier
        is_sign_up_alias = self.is_sign_up_alias.value

        async def auth_callback(
            verifier: Annotated[str, fastapi.Depends(pkce_verifier)],
            request: fastapi.Request,
            *,
            is_sign_up: Annotated[
                bool, fastapi.Query(alias=is_sign_up_alias)
            ] = not for_sign_in,
            code: Optional[str] = None,
            identity_id: Optional[uuid.UUID] = None,
            verification_email_sent_at: Optional[datetime.datetime] = None,
        ) -> fastapi.Response:
            if code is None:
                if identity_id is None:
                    return self._redirect_error(
                        "Bad request: missing identity_id"
                    )

                response = await self._auth.handle_new_identity(
                    request, identity_id, None
                )
                if response is not None:
                    return response
                elif self.on_sign_up_verification_required.is_set():
                    return await self.on_sign_up_verification_required.call(
                        request, verification_email_sent_at
                    )
                else:
                    return self._redirect_error("Email verification required")

            else:
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
                            response = self._redirect_success(
                                request, method="on_sign_up_complete"
                            )
                else:
                    if self.on_sign_in_complete.is_set():
                        with self._auth.with_auth_token(
                            token_data.auth_token, request
                        ):
                            response = await self.on_sign_in_complete.call(
                                request, token_data
                            )
                    else:
                        response = self._redirect_success(
                            request, method="on_sign_in_complete"
                        )
                self._auth.set_auth_cookie(
                    token_data.auth_token, response=response
                )
                return response

        if not for_sign_in or not for_sign_up:
            sig = inspect.signature(auth_callback)
            auth_callback.__signature__ = sig.replace(  # type: ignore [attr-defined]
                parameters=[
                    param
                    for name, param in sig.parameters.items()
                    if name != "is_sign_up"
                ]
            )
        auth_callback.__globals__.update(
            dict(
                pkce_verifier=pkce_verifier,
                is_sign_up_alias=is_sign_up_alias,
            )
        )
        return auth_callback

    async def install_callback_handlers(
        self, router: fastapi.APIRouter
    ) -> None:
        config = await self._auth.client.query_required_single(
            """
            select assert_single(
                cfg::Config.extensions[is ext::auth::AuthConfig].ui
            ) { redirect_to, redirect_to_on_signup }
            """
        )
        redirect_to = urllib.parse.urlsplit(config.redirect_to)
        on_signup = urllib.parse.urlsplit(config.redirect_to_on_signup)

        # Check for issues with the redirect URLs: (msg, args, is_fatal)
        issues: list[tuple[str, tuple[Any, ...], bool]] = []
        if redirect_to.scheme != on_signup.scheme:
            issues.append(
                (
                    "inconsistent scheme, redirect_to is %r but "
                    "redirect_to_on_signup is %r",
                    (redirect_to.scheme, on_signup.scheme),
                    False,
                )
            )
        if redirect_to.netloc != on_signup.netloc:
            issues.append(
                (
                    "inconsistent netloc, redirect_to has %r but "
                    "redirect_to_on_signup has %r",
                    (redirect_to.netloc, on_signup.netloc),
                    False,
                )
            )
        if not redirect_to.netloc:
            issues.append(("redirect_to is relative", (), False))
        if not on_signup.netloc:
            issues.append(("redirect_to_on_signup is relative", (), False))
        if redirect_to.path == on_signup.path:
            # Single handler for all callbacks, on_signup must have the query
            # argument `is_sign_up_alias` set to true, even in permissive mode.
            adt = pydantic.TypeAdapter(bool)
            arg = self.is_sign_up_alias.value
            default_args = urllib.parse.parse_qs(
                redirect_to.query, keep_blank_values=True
            )
            val = default_args.get(arg, ["False"])[0]
            try:
                if adt.validate_python(val) is not False:
                    issues.append(
                        (
                            'redirect_to has query "%s=%s", '
                            'but it must be "false" or omitted',
                            (arg, val),
                            True,
                        )
                    )
            except pydantic.ValidationError:
                issues.append(
                    (
                        'redirect_to has invalid query value "%s=%s", '
                        'but it must be "false" or omitted',
                        (arg, val),
                        True,
                    )
                )
            signup_args = urllib.parse.parse_qs(
                on_signup.query, keep_blank_values=True
            )
            if arg in signup_args:
                val = signup_args[arg][0]
                try:
                    if adt.validate_python(val) is not True:
                        issues.append(
                            (
                                'redirect_to_on_signup has query "%s=%s", '
                                'but it must be "true"',
                                (arg, val),
                                True,
                            )
                        )
                except pydantic.ValidationError:
                    issues.append(
                        (
                            "redirect_to_on_signup has invalid query value "
                            '"%s=%s", but it must be "true"',
                            (arg, val),
                            True,
                        )
                    )
            else:
                issues.append(
                    (
                        'redirect_to_on_signup must have query "%s=true" '
                        "when redirect_to shares the same path",
                        (arg,),
                        True,
                    )
                )
            if (
                self.callback_path.is_set()
                and self.callback_path.value != redirect_to.path
            ):
                issues.append(
                    (
                        "redirect_to %r does not match the configured "
                        "callback_path %r",
                        (redirect_to.path, self.callback_path.value),
                        True,
                    )
                )
        elif self.callback_path.is_set():
            if self.callback_path.value != redirect_to.path:
                issues.append(
                    (
                        "redirect_to %r does not match the configured "
                        "callback_path %r",
                        (redirect_to.path, self.callback_path.value),
                        True,
                    )
                )
            if self.callback_path.value != on_signup.path:
                issues.append(
                    (
                        "redirect_to_on_signup %r does not match the "
                        "configured callback_path %r",
                        (on_signup.path, self.callback_path.value),
                        True,
                    )
                )

        if issues:
            if any(is_fatal for _msg, _args, is_fatal in issues):
                reason = "\n\t* ".join(
                    msg % args for msg, args, is_fatal in issues if is_fatal
                )
                raise RuntimeError(
                    f"detected issues with the redirect URLs of the built-in "
                    f"UI configuration of the Gel auth extension: \n"
                    f"\t* {reason}"
                )
            else:
                reason = "\n\t* ".join(msg for msg, _args, _is_fatal in issues)
                if self.install_callback.value == "permissive":
                    msg = (
                        "The built-in UI callback handlers are still "
                        "installed, please double check to ensure that they "
                        "work as expected."
                    )
                else:
                    msg = (
                        "The built-in UI callback handlers will not be "
                        "installed because of this. If you are sure that the "
                        "URLs are correct, you can install them by setting:\n"
                        "\tg.auth.builtin_ui.install_callback = 'permissive'"
                    )
                _logger.warning(
                    "detected issues with "  # noqa: G003  # reason has args
                    "the redirect URLs of the built-in UI configuration of "
                    "the Gel auth extension: \n\t* " + reason + "\n%s",
                    *itertools.chain(
                        *(args for _msg, args, _is_fatal in issues)
                    ),
                    msg,
                )
                if self.install_callback.value != "permissive":
                    return

        # Now, let's actually install the callback handlers
        if not self.callback_path.is_set():
            # For hook dependants, doesn't really matter much
            self.callback_path = redirect_to.path
        if redirect_to.path == on_signup.path:
            # Single handler for all callbacks
            router.get(
                redirect_to.path,
                name=self.callback_name.value,
                summary=self.callback_summary.value,
            )(self.make_auth_callback_handler())
        else:
            router.get(
                redirect_to.path,
                name=self.callback_name.value,
                summary=self.callback_summary.value,
            )(self.make_auth_callback_handler(for_sign_up=False))
            router.get(
                on_signup.path,
                name=self.callback_name.value,
                summary=self.callback_summary.value,
            )(self.make_auth_callback_handler(for_sign_in=False))

    @property
    def blocking_io_core(self) -> core.BuiltinUI:
        return self._blocking_io_core

    @property
    def core(self) -> core.AsyncBuiltinUI:
        return self._core

    async def install(self, router: fastapi.APIRouter) -> None:
        self._core = await core.make_async(self._auth.client)
        self._blocking_io_core = await concurrency.run_in_threadpool(
            core.make, self._auth.blocking_io_client
        )
        if self.install_sign_in_page.value:
            self.__install_sign_in_page(router)
        if self.install_sign_up_page.value:
            self.__install_sign_up_page(router)
        if self.install_callback.value != "no":
            prefix = router.prefix
            router.prefix = ""
            try:
                await self.install_callback_handlers(router)
            finally:
                router.prefix = prefix
        await super().install(router)
