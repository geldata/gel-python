import os
import pathlib
import typing
import json

from gel._internal import _platform


class RequiredCredentials(typing.TypedDict, total=True):
    port: int
    user: str


class Credentials(RequiredCredentials, total=False):
    host: typing.Optional[str]
    password: typing.Optional[str]
    # It's OK for database and branch to appear in credentials, as long as
    # they match.
    database: typing.Optional[str]
    branch: typing.Optional[str]
    tls_ca: typing.Optional[str]
    tls_security: typing.Optional[str]
    secret_key: typing.Optional[str]


def get_credentials_path(instance_name: str) -> pathlib.Path:
    return _platform.search_config_dir("credentials", instance_name + ".json")


def read_credentials(path: os.PathLike) -> Credentials:
    try:
        with open(path, encoding="utf-8") as f:
            credentials = json.load(f)
        return validate_credentials(credentials)
    except Exception as e:
        raise RuntimeError(f"cannot read credentials at {path}") from e


def validate_credentials(data: dict) -> Credentials:
    result = {}

    port = data.get("port")
    if port is None:
        port = 5656
    if not isinstance(port, int) or port < 1 or port > 65535:
        raise ValueError("invalid `port` value")
    result["port"] = port

    user = data.get("user")
    if user is not None:
        if not isinstance(user, str):
            raise ValueError("`user` must be a string")
        result["user"] = user
    else:
        # Backwards compatibility with the old EdgeDB versions.
        # In Gel, `egdedb` is changed to `main`.
        # https://github.com/geldata/gel/pull/8010
        result["user"] = "edgedb"

    host = data.get("host")
    if host is not None:
        if not isinstance(host, str):
            raise ValueError("`host` must be a string")
        result["host"] = host

    database = data.get("database")
    if database is not None:
        if not isinstance(database, str):
            raise ValueError("`database` must be a string")
        result["database"] = database

    branch = data.get("branch")
    if branch is not None:
        if not isinstance(branch, str):
            raise ValueError("`branch` must be a string")
        if (
            database is not None
            and branch != database
            and branch != "__default__"
        ):
            raise ValueError(f"`database` and `branch` cannot be different")
        result["branch"] = branch

    password = data.get("password")
    if password is not None:
        if not isinstance(password, str):
            raise ValueError("`password` must be a string")
        result["password"] = password

    ca = data.get("tls_ca")
    if ca is not None:
        if not isinstance(ca, str):
            raise ValueError("`tls_ca` must be a string")
        result["tls_ca"] = ca

    cert_data = data.get("tls_cert_data")
    if cert_data is not None:
        if not isinstance(cert_data, str):
            raise ValueError("`tls_cert_data` must be a string")
        if ca is not None and ca != cert_data:
            raise ValueError(
                f"tls_ca and tls_cert_data are both set and disagree"
            )
        result["tls_ca"] = cert_data

    verify = data.get("tls_verify_hostname")
    if verify is not None:
        if not isinstance(verify, bool):
            raise ValueError("`tls_verify_hostname` must be a bool")
        result["tls_security"] = "strict" if verify else "no_host_verification"

    tls_security = data.get("tls_security")
    if tls_security is not None:
        if not isinstance(tls_security, str):
            raise ValueError("`tls_security` must be a string")
        result["tls_security"] = tls_security

    secret_key = data.get("secret_key")
    if secret_key is not None:
        if not isinstance(secret_key, str):
            raise ValueError("`secret_key` must be a string")
        result["secret_key"] = secret_key

    missmatch = ValueError(
        f"tls_verify_hostname={verify} and "
        f"tls_security={tls_security} are incompatible"
    )
    if tls_security == "strict" and verify is False:
        raise missmatch
    if tls_security in {"no_host_verification", "insecure"} and verify is True:
        raise missmatch

    return result
