# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

from __future__ import annotations
from typing import Any, Optional, TypeVar, overload, Literal, TYPE_CHECKING

import dataclasses
import http
import logging

import httpx

import gel
from gel import blocking_client
from gel._internal._polyfills._strenum import StrEnum

from . import _base as base
from . import _pkce as pkce_mod
from . import _token_data as td_mod

logger = logging.getLogger("gel.auth")
C = TypeVar("C", bound=httpx.Client | httpx.AsyncClient)


class VerificationMethod(StrEnum):
    LINK = "Link"
    CODE = "Code"


@dataclasses.dataclass
class MagicLinkSentResponse:
    email_sent: str
    verifier: str
    signup: bool


@dataclasses.dataclass
class MagicLinkFailedResponse(base.BaseServerFailedResponse):
    verifier: str


MagicLinkResponse = MagicLinkSentResponse | MagicLinkFailedResponse


@dataclasses.dataclass
class MagicCodeSentResponse:
    signup: bool
    email: str


@dataclasses.dataclass
class MagicCodeFailedResponse(base.BaseServerFailedResponse):
    pass


MagicCodeResponse = MagicCodeSentResponse | MagicCodeFailedResponse


@dataclasses.dataclass
class AuthenticateLinkResultResponse:
    code: str


@dataclasses.dataclass
class AuthenticateLinkFailedResponse(base.BaseServerFailedResponse):
    pass


AuthenticateLinkResponse = (
    AuthenticateLinkResultResponse | AuthenticateLinkFailedResponse
)


@dataclasses.dataclass
class AuthenticateCodeResultResponse:
    code: str
    verifier: str


@dataclasses.dataclass
class AuthenticateCodeFailedResponse(base.BaseServerFailedResponse):
    verifier: str


AuthenticateCodeResponse = (
    AuthenticateCodeResultResponse | AuthenticateCodeFailedResponse
)


class BaseMagicLink(base.BaseClient[C]):
    def __init__(
        self,
        *,
        connection_info: gel.ConnectionInfo,
        **kwargs: Any,
    ) -> None:
        self.provider = "builtin::local_magic_link"
        super().__init__(connection_info=connection_info, **kwargs)

    async def _request_link(
        self,
        email: str,
        *,
        is_sign_up: bool,
        callback_url: str,
        redirect_on_failure: str,
        link_url: Optional[str] = None,
        parse_redirect_as_error: bool = False,
    ) -> MagicLinkResponse:
        title = "register" if is_sign_up else "sign in"
        logger.info("signing %s user: %s", "up" if is_sign_up else "in", email)
        pkce = self._generate_pkce()
        data = {
            "provider": self.provider,
            "email": email,
            "challenge": pkce.challenge,
            "callback_url": callback_url,
            "redirect_on_failure": redirect_on_failure,
        }
        if link_url is not None:
            data["link_url"] = link_url
        register_response = await self._http_request(
            "POST",
            "/magic-link/register" if is_sign_up else "/magic-link/email",
            json=data,
            headers={"Accept": "application/json"},
        )
        if parse_redirect_as_error and register_response.has_redirect_location:
            # On Gel 5.x/6.x, the /magic-link/email endpoint does not return
            # JSON error responses, but instead redirects to the failure URL
            # with an error query parameter. This bug is fixed in Gel 7.0+.
            failure_url = httpx.URL(register_response.headers["Location"])
            error = failure_url.params.get("error", "unknown error")
            logger.error("%s error: %s", title, error)
            return MagicLinkFailedResponse(
                verifier=pkce.verifier,
                status_code=http.HTTPStatus.BAD_REQUEST,
                message=error,
            )
        try:
            register_response.raise_for_status()
        except httpx.HTTPStatusError as e:
            logger.error("%s error: %s", title, e)
            return MagicLinkFailedResponse(
                verifier=pkce.verifier,
                status_code=e.response.status_code,
                message=e.response.text,
            )
        register_json = register_response.json()
        if "error" in register_json:
            error = register_json["error"]
            logger.error("%s error: %s", title, error)
            return MagicLinkFailedResponse(
                verifier=pkce.verifier,
                status_code=register_response.status_code,
                message=error,
            )
        else:
            email_sent = register_json["email_sent"]
            signup = (
                register_json.get("signup", str(is_sign_up)).lower() == "true"
            )
            logger.info("the magic link is sent to: %r", email_sent)
            logger.debug(
                "Sign-up: %s, PKCE verifier: %s", signup, pkce.verifier
            )
            return MagicLinkSentResponse(
                email_sent=email_sent,
                verifier=pkce.verifier,
                signup=signup,
            )

    async def _request_code(
        self,
        email: str,
        *,
        is_sign_up: bool,
    ) -> MagicCodeResponse:
        title = "register" if is_sign_up else "sign in"
        logger.info("signing %s user: %s", "up" if is_sign_up else "in", email)
        data = {
            "provider": self.provider,
            "email": email,
        }
        register_response = await self._http_request(
            "POST",
            "/magic-link/register" if is_sign_up else "/magic-link/email",
            json=data,
            headers={"Accept": "application/json"},
        )
        try:
            register_response.raise_for_status()
        except httpx.HTTPStatusError as e:
            logger.error("%s error: %s", title, e)
            return MagicCodeFailedResponse(
                status_code=e.response.status_code,
                message=e.response.text,
            )
        register_json = register_response.json()
        if "error" in register_json:
            error = register_json["error"]
            logger.error("%s error: %s", title, error)
            return MagicCodeFailedResponse(
                status_code=register_response.status_code,
                message=error,
            )
        else:
            email = register_json["email"]
            signup = (
                register_json.get("signup", str(is_sign_up)).lower() == "true"
            )
            logger.info("the magic code is sent to: %r", email)
            logger.debug("Sign-up: %s", signup)
            return MagicCodeSentResponse(email=email, signup=signup)

    async def _authenticate_link(self, token: str) -> AuthenticateLinkResponse:
        logger.info("authenticating magic link token")
        logger.debug("token: %s", token)
        response = await self._http_request(
            "GET",
            httpx.URL("/magic-link/authenticate").copy_add_param(
                "token", token
            ),
            headers={"Accept": "application/json"},
        )
        if response.has_redirect_location:
            # /magic-link/authenticate redirects to the callback URL, but we
            # want to return the code directly.
            redirect_url = httpx.URL(response.headers["Location"])
            code = redirect_url.params.get("code")
            if code is not None:
                logger.info("authentication succeeded")
                logger.debug("code: %s", code)
                return AuthenticateLinkResultResponse(code=code)
            else:
                logger.error("authentication failed: missing code")
                return AuthenticateLinkFailedResponse(
                    status_code=http.HTTPStatus.BAD_GATEWAY,
                    message="missing code in redirect URL",
                )
        elif response.is_success:
            logger.error("authentication failed: expected redirect")
            return AuthenticateLinkFailedResponse(
                status_code=response.status_code,
                message="expected redirect but got response",
            )
        else:
            logger.error(
                "authentication failed: [%d] %s",
                response.status_code,
                response.text,
            )
            return AuthenticateLinkFailedResponse(
                status_code=response.status_code,
                message=response.text,
            )

    async def _authenticate_code(
        self, email: str, code: str
    ) -> AuthenticateCodeResponse:
        logger.info("authenticating by magic code")
        logger.debug("email: %r, code: %s", email, code)
        pkce = self._generate_pkce()
        response = await self._http_request(
            "POST",
            "/magic-link/authenticate",
            json={"email": email, "code": code, "challenge": pkce.challenge},
            headers={"Accept": "application/json"},
        )
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            logger.error("authentication failed: %s", e)
            return AuthenticateCodeFailedResponse(
                verifier=pkce.verifier,
                status_code=e.response.status_code,
                message=e.response.text,
            )
        response_json = response.json()
        if "error" in response_json:
            error = response_json["error"]
            logger.error("authentication failed: %s", error)
            return AuthenticateCodeFailedResponse(
                verifier=pkce.verifier,
                status_code=response.status_code,
                message=error,
            )
        elif "code" in response_json:
            pkce_code = response_json["code"]
            logger.info("authentication succeeded")
            logger.debug("PKCE code: %s", pkce_code)
            return AuthenticateCodeResultResponse(
                code=pkce_code, verifier=pkce.verifier
            )
        else:
            logger.error("authentication failed: missing code")
            return AuthenticateCodeFailedResponse(
                verifier=pkce.verifier,
                status_code=http.HTTPStatus.BAD_GATEWAY,
                message="missing code in response",
            )

    async def _get_token(
        self, *, verifier: Optional[str], code: str
    ) -> td_mod.TokenData:
        if verifier is None:
            raise ValueError("verifier is required to get token")
        pkce = self._pkce_from_verifier(verifier)
        logger.info("exchanging code for token: %s", code)
        return await pkce.internal_exchange_code_for_token(code)


class BaseBlockingIOMagicLink(BaseMagicLink[httpx.Client]):
    def _init_http_client(self, **kwargs: Any) -> httpx.Client:
        return httpx.Client(**kwargs)

    def _generate_pkce(self) -> pkce_mod.PKCE:
        return pkce_mod.generate_pkce(self._client)

    def _pkce_from_verifier(self, verifier: str) -> pkce_mod.PKCE:
        return pkce_mod.PKCE(self._client, verifier)

    async def _send_http_request(
        self, request: httpx.Request
    ) -> httpx.Response:
        return self._client.send(request)

    def get_token(
        self, *, verifier: Optional[str], code: str
    ) -> td_mod.TokenData:
        return blocking_client.iter_coroutine(
            self._get_token(verifier=verifier, code=code)
        )


class LegacyMagicLink(BaseBlockingIOMagicLink):
    def sign_up(
        self,
        email: str,
        *,
        callback_url: str,
        redirect_on_failure: str,
    ) -> MagicLinkResponse:
        return blocking_client.iter_coroutine(
            self._request_link(
                email,
                is_sign_up=True,
                callback_url=callback_url,
                redirect_on_failure=redirect_on_failure,
            )
        )

    def sign_in(
        self,
        email: str,
        *,
        callback_url: str,
        redirect_on_failure: str,
    ) -> MagicLinkResponse:
        return blocking_client.iter_coroutine(
            self._request_link(
                email,
                is_sign_up=False,
                callback_url=callback_url,
                redirect_on_failure=redirect_on_failure,
                parse_redirect_as_error=True,
            )
        )


class MagicLink(BaseBlockingIOMagicLink):
    def __init__(
        self,
        *,
        parse_redirect_as_error: bool = False,
        connection_info: gel.ConnectionInfo,
        **kwargs: Any,
    ) -> None:
        self.parse_redirect_as_error = parse_redirect_as_error
        super().__init__(connection_info=connection_info, **kwargs)

    def sign_up(
        self,
        email: str,
        *,
        callback_url: str,
        redirect_on_failure: str,
        link_url: Optional[str] = None,
    ) -> MagicLinkResponse:
        return blocking_client.iter_coroutine(
            self._request_link(
                email,
                is_sign_up=True,
                callback_url=callback_url,
                redirect_on_failure=redirect_on_failure,
                link_url=link_url,
            )
        )

    def sign_in(
        self,
        email: str,
        *,
        callback_url: str,
        redirect_on_failure: str,
        link_url: Optional[str] = None,
    ) -> MagicLinkResponse:
        return blocking_client.iter_coroutine(
            self._request_link(
                email,
                is_sign_up=False,
                callback_url=callback_url,
                redirect_on_failure=redirect_on_failure,
                link_url=link_url,
                parse_redirect_as_error=self.parse_redirect_as_error,
            )
        )

    def authenticate(self, token: str) -> AuthenticateLinkResponse:
        return blocking_client.iter_coroutine(self._authenticate_link(token))


class MagicCode(BaseBlockingIOMagicLink):
    def sign_up(self, email: str) -> MagicCodeResponse:
        return blocking_client.iter_coroutine(
            self._request_code(email, is_sign_up=True)
        )

    def sign_in(self, email: str) -> MagicCodeResponse:
        return blocking_client.iter_coroutine(
            self._request_code(email, is_sign_up=False)
        )

    def authenticate(self, email: str, code: str) -> AuthenticateCodeResponse:
        return blocking_client.iter_coroutine(
            self._authenticate_code(email, code)
        )


def _validate_server_version(
    server_major_version: int,
    verification_method: VerificationMethod,
) -> None:
    if server_major_version < 5:
        raise gel.UnsupportedFeatureError(
            "Magic link is not supported on Gel < 5.0"
        )
    if (
        server_major_version < 7
        and verification_method == VerificationMethod.CODE
    ):
        raise gel.UnsupportedFeatureError(
            "Magic code verification is not supported on Gel < 7.0"
        )


if TYPE_CHECKING:
    AnyMagicLink = LegacyMagicLink | MagicLink | MagicCode


@overload
def make(
    client: gel.Client, *, server_major_version: Literal[5]
) -> LegacyMagicLink: ...


@overload
def make(
    client: gel.Client,
    *,
    server_major_version: int,
    verification_method: Literal[VerificationMethod.LINK],
) -> MagicLink: ...


@overload
def make(
    client: gel.Client,
    *,
    server_major_version: int,
    verification_method: Literal[VerificationMethod.CODE],
) -> MagicCode: ...


@overload
def make(
    client: gel.Client,
    *,
    server_major_version: Optional[int] = None,
    verification_method: VerificationMethod = VerificationMethod.LINK,
    cls: Optional[type[AnyMagicLink]] = None,
) -> AnyMagicLink: ...


def make(
    client: gel.Client,
    *,
    server_major_version: Optional[int] = None,
    verification_method: VerificationMethod = VerificationMethod.LINK,
    cls: Optional[type[AnyMagicLink]] = None,
) -> AnyMagicLink:
    if server_major_version is None:
        server_major_version = client.query_required_single(
            "select sys::get_version().major"
        )
        assert isinstance(server_major_version, int)
    _validate_server_version(server_major_version, verification_method)
    args = {}
    if cls is None:
        if server_major_version < 6:
            cls = LegacyMagicLink
        elif server_major_version < 7:
            cls = MagicLink
            args["parse_redirect_as_error"] = True
        elif verification_method == VerificationMethod.LINK:
            cls = MagicLink
        else:
            cls = MagicCode
    return cls(connection_info=client.check_connection(), **args)


class BaseAsyncMagicLink(BaseMagicLink[httpx.AsyncClient]):
    def _init_http_client(self, **kwargs: Any) -> httpx.AsyncClient:
        return httpx.AsyncClient(**kwargs)

    def _generate_pkce(self) -> pkce_mod.AsyncPKCE:
        return pkce_mod.generate_async_pkce(self._client)

    def _pkce_from_verifier(self, verifier: str) -> pkce_mod.AsyncPKCE:
        return pkce_mod.AsyncPKCE(self._client, verifier)

    async def _send_http_request(
        self, request: httpx.Request
    ) -> httpx.Response:
        return await self._client.send(request)

    async def get_token(
        self, *, verifier: Optional[str], code: str
    ) -> td_mod.TokenData:
        return await self._get_token(verifier=verifier, code=code)


class AsyncLegacyMagicLink(BaseAsyncMagicLink):
    async def sign_up(
        self,
        email: str,
        *,
        callback_url: str,
        redirect_on_failure: str,
    ) -> MagicLinkResponse:
        return await self._request_link(
            email,
            is_sign_up=True,
            callback_url=callback_url,
            redirect_on_failure=redirect_on_failure,
        )

    async def sign_in(
        self,
        email: str,
        *,
        callback_url: str,
        redirect_on_failure: str,
    ) -> MagicLinkResponse:
        return await self._request_link(
            email,
            is_sign_up=False,
            callback_url=callback_url,
            redirect_on_failure=redirect_on_failure,
            parse_redirect_as_error=True,
        )


class AsyncMagicLink(BaseAsyncMagicLink):
    def __init__(
        self,
        *,
        parse_redirect_as_error: bool = False,
        connection_info: gel.ConnectionInfo,
        **kwargs: Any,
    ) -> None:
        self.parse_redirect_as_error = parse_redirect_as_error
        super().__init__(connection_info=connection_info, **kwargs)

    async def sign_up(
        self,
        email: str,
        *,
        callback_url: str,
        redirect_on_failure: str,
        link_url: Optional[str] = None,
    ) -> MagicLinkResponse:
        return await self._request_link(
            email,
            is_sign_up=True,
            callback_url=callback_url,
            redirect_on_failure=redirect_on_failure,
            link_url=link_url,
        )

    async def sign_in(
        self,
        email: str,
        *,
        callback_url: str,
        redirect_on_failure: str,
        link_url: Optional[str] = None,
    ) -> MagicLinkResponse:
        return await self._request_link(
            email,
            is_sign_up=False,
            callback_url=callback_url,
            redirect_on_failure=redirect_on_failure,
            link_url=link_url,
            parse_redirect_as_error=self.parse_redirect_as_error,
        )

    async def authenticate(self, token: str) -> AuthenticateLinkResponse:
        return await self._authenticate_link(token)


class AsyncMagicCode(BaseAsyncMagicLink):
    async def sign_up(self, email: str) -> MagicCodeResponse:
        return await self._request_code(email, is_sign_up=True)

    async def sign_in(self, email: str) -> MagicCodeResponse:
        return await self._request_code(email, is_sign_up=False)

    async def authenticate(
        self, email: str, code: str
    ) -> AuthenticateCodeResponse:
        return await self._authenticate_code(email, code)


if TYPE_CHECKING:
    AnyAsyncMagicLink = AsyncLegacyMagicLink | AsyncMagicLink | AsyncMagicCode


@overload
async def make_async(
    client: gel.AsyncIOClient, *, server_major_version: Literal[5]
) -> AsyncLegacyMagicLink: ...


@overload
async def make_async(
    client: gel.AsyncIOClient,
    *,
    server_major_version: int,
    verification_method: Literal[VerificationMethod.LINK],
) -> AsyncMagicLink: ...


@overload
async def make_async(
    client: gel.AsyncIOClient,
    *,
    server_major_version: int,
    verification_method: Literal[VerificationMethod.CODE],
) -> AsyncMagicCode: ...


@overload
async def make_async(
    client: gel.AsyncIOClient,
    *,
    server_major_version: Optional[int] = None,
    verification_method: VerificationMethod = VerificationMethod.LINK,
    cls: Optional[type[AnyAsyncMagicLink]] = None,
) -> AnyAsyncMagicLink: ...


async def make_async(
    client: gel.AsyncIOClient,
    *,
    server_major_version: Optional[int] = None,
    verification_method: VerificationMethod = VerificationMethod.LINK,
    cls: Optional[type[AnyAsyncMagicLink]] = None,
) -> AnyAsyncMagicLink:
    if server_major_version is None:
        server_major_version = await client.query_required_single(
            "select sys::get_version().major"
        )
        assert isinstance(server_major_version, int)
    _validate_server_version(server_major_version, verification_method)
    args = {}
    if cls is None:
        if server_major_version < 6:
            cls = AsyncLegacyMagicLink
        elif server_major_version < 7:
            cls = AsyncMagicLink
            args["parse_redirect_as_error"] = True
        elif verification_method == VerificationMethod.LINK:
            cls = AsyncMagicLink
        else:
            cls = AsyncMagicCode
    return cls(connection_info=await client.check_connection(), **args)
