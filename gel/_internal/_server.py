# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.
#
# ruff: noqa: T201

from __future__ import annotations
from typing import TYPE_CHECKING, Any, NamedTuple, Optional, overload
from typing_extensions import Self, TypeAliasType, TypeVar

import asyncio
import atexit
import contextlib
import enum
import json
import os
import pathlib
import re
import socket
import subprocess
import shutil
import sys
import tempfile
import textwrap
import time

import gel
from gel import asyncio_client, blocking_client
from gel import cli
from gel._internal import _edgeql
from gel._internal import _version

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator, Iterable, Mapping


class InstanceError(Exception):
    pass


_debug_enabled = os.environ.get("EDGEDB_DEBUG_SERVER", "").strip().lower() in {
    "1",
    "true",
    "t",
    "yes",
    "y",
}


def _debug(*msg: str) -> None:
    if _debug_enabled:
        print("[gel.captive]", *msg, file=sys.stderr, flush=True)


if sys.platform == "win32":
    _WSL_CMD = ["wsl", "-u", "edgedb"]

    def _find_server_executable() -> pathlib.Path | None:
        which_res = subprocess.run(
            [*_WSL_CMD, "which", "gel-server"],
            capture_output=True,
            check=False,
            text=True,
        )
        if which_res.returncode != 0:
            which_res = subprocess.run(
                [*_WSL_CMD, "which", "edgedb-server"],
                capture_output=True,
                check=False,
                text=True,
            )
        if which_res.returncode != 0:
            return None
        else:
            try:
                return pathlib.Path(which_res.stdout.strip())
            except ValueError:
                return None
else:

    def _find_server_executable() -> pathlib.Path | None:
        path = shutil.which("gel-server")
        if not path:
            path = shutil.which("edgedb-server")
        if not path:
            return None
        else:
            return pathlib.Path(path)


def _get_wsl_path(win_path: str) -> str:
    return (
        re.sub(r"^([A-Z]):", lambda m: f"/mnt/{m.group(1)}", win_path)
        .replace("\\", "/")
        .lower()
    )


def _get_cli_managed_server_path(
    cli_bin: str,
    min_server_version: VersionConstraint | None,
) -> pathlib.Path | None:
    cmd = [cli_bin, "server", "info", "--get", "bin-path"]
    if min_server_version is not None:
        cmd.extend(
            [
                "--version",
                f">={Version(*min_server_version)}",  # pyright: ignore [reportArgumentType]
            ]
        )
    else:
        cmd.append("--latest")

    proc = subprocess.run(
        cmd,
        check=False,
        capture_output=True,
        text=True,
    )
    if proc.returncode == 0:
        path = proc.stdout.strip()
        if not path:
            return None
        else:
            try:
                return pathlib.Path(path)
            except ValueError:
                return None
    else:
        return None


def _download_server(
    cli_bin: str,
    min_server_version: VersionConstraint | None,
) -> bool:
    cmd = [cli_bin, "server", "install"]
    if min_server_version is not None:
        cmd.extend(
            [
                "--version",
                f">={Version(*min_server_version)}",  # pyright: ignore [reportArgumentType]
            ]
        )

    proc = subprocess.run(
        cmd,
        check=False,
        text=True,
    )

    return proc.returncode == 0


def _get_server_venv_site(
    executable_path: pathlib.Path,
) -> tuple[pathlib.Path | None, tuple[str, ...]]:
    if len(executable_path.parts) < 4:
        # Needs to be at least /venv/bin/gel-server
        return None, ()
    bin_dir = executable_path.parent
    venv_root = bin_dir.parent
    pyvenv_cfg_path = venv_root / "pyvenv.cfg"
    if not pyvenv_cfg_path.exists():
        return None, ()

    exe_suffix = ".exe" if sys.platform == "win32" else ""
    python = bin_dir / f"python{exe_suffix}"
    sitescript = "import json, site; print(json.dumps(site.getsitepackages()))"
    proc = subprocess.run(
        [python, "-E", "-c", sitescript],
        check=False,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        return None, ()

    try:
        site_packages: tuple[str, ...] = tuple(json.loads(proc.stdout))
    except ValueError:
        return None, ()

    return python, site_packages


def _is_devmode_server(executable_path: pathlib.Path) -> pathlib.Path | None:
    if sys.platform == "win32":
        return None

    python, server_venv_site = _get_server_venv_site(executable_path)
    if not server_venv_site:
        return None

    org = _version.get_direct_url_origin("gel_server", path=server_venv_site)
    if org is None or not org.editable:
        return None

    return python


_SERVER_VERSION_LINE_RE = re.compile(
    r"""
    ^
    (?:gel|edgedb)-server,\s+version\s+
    (?P<release>[0-9]+(?:\.[0-9]+)*)
    (?P<pre>
        [-\.]?
        (?P<pre_l>(a|b|c|rc|alpha|beta|dev))
        [\.]?
        (?P<pre_n>[0-9]+)?
    )?
    (?:\+(?P<local>[a-z0-9]+(?:[\.][a-z0-9]+)*))?
    $
""",
    re.VERBOSE,
)

_VERSION_RE = re.compile(
    r"""
    ^
    (?P<release>[0-9]+(?:\.[0-9]+)*)
    (?P<pre>
        [-\.]?
        (?P<pre_l>(a|b|c|rc|alpha|beta|dev))
        [\.]?
        (?P<pre_n>[0-9]+)?
    )?
    (?:\+(?P<local>[a-z0-9]+(?:[\.][a-z0-9]+)*))?
    $
""",
    re.VERBOSE,
)


class VersionStage(enum.IntEnum):
    DEV = 0
    ALPHA = 10
    BETA = 20
    RC = 30
    FINAL = 40

    def __str__(self) -> str:
        return self.name.lower()


class Version(NamedTuple):
    major: int
    minor: int
    stage: VersionStage = VersionStage.FINAL
    stage_no: int = 0
    local: tuple[str, ...] = ()

    def to_str(self, *, include_local: bool = True) -> str:
        ver = f"{self.major}.{self.minor}"
        if self.stage is not VersionStage.FINAL:
            ver += f"-{self.stage.name.lower()}.{self.stage_no}"
        if self.local and include_local:
            ver += f'{("+" + ".".join(self.local)) if self.local else ""}'
        return ver

    def __str__(self) -> str:
        return self.to_str()

    @classmethod
    def parse_server_line(cls, ver: str) -> Self:
        v = _SERVER_VERSION_LINE_RE.match(ver)
        if v is None:
            raise ValueError(f"malformed version string: {ver}")
        if v.group("pre"):
            pre_l = v.group("pre_l")
            if pre_l in {"a", "alpha"}:
                stage = VersionStage.ALPHA
            elif pre_l in {"b", "beta"}:
                stage = VersionStage.BETA
            elif pre_l in {"c", "rc"}:
                stage = VersionStage.RC
            elif pre_l == "dev":
                stage = VersionStage.DEV
            else:
                raise ValueError(f"cannot determine release stage from {ver}")

            stage_no = int(v.group("pre_n"))
        else:
            stage = VersionStage.FINAL
            stage_no = 0

        local: list[str] = []
        if v.group("local"):
            local.extend(v.group("local").split("."))

        release = [int(r) for r in v.group("release").split(".")]

        return cls(
            major=release[0],
            minor=release[1],
            stage=stage,
            stage_no=stage_no,
            local=tuple(local),
        )

    @classmethod
    def parse(cls, ver: str) -> Self:
        v = _VERSION_RE.match(ver)
        if v is None:
            raise ValueError(f"cannot parse version: {ver}")
        local: list[str] = []
        if v.group("pre"):
            pre_l = v.group("pre_l")
            if pre_l in {"a", "alpha"}:
                stage = VersionStage.ALPHA
            elif pre_l in {"b", "beta"}:
                stage = VersionStage.BETA
            elif pre_l in {"c", "rc"}:
                stage = VersionStage.RC
            elif pre_l == "dev":
                stage = VersionStage.DEV
            else:
                raise ValueError(f"cannot determine release stage from {ver}")

            stage_no = int(v.group("pre_n"))
        else:
            stage = VersionStage.FINAL
            stage_no = 0
        if v.group("local"):
            local.extend(v.group("local").split("."))

        release = [int(r) for r in v.group("release").split(".")]

        return cls(
            major=release[0],
            minor=release[1],
            stage=stage,
            stage_no=stage_no,
            local=tuple(local),
        )


VersionConstraint = TypeAliasType(
    "VersionConstraint",
    tuple[int, int] | tuple[int, int, VersionStage, int] | Version,
)


def _server_env(env: Mapping[str, str] | None = None, /) -> dict[str, str]:
    res = {k: v for k, v in os.environ.items() if not k.startswith("PYTHON")}
    if env is not None:
        res |= env
    return res


def _get_server_version(
    executable: pathlib.Path,
) -> tuple[Version, pathlib.Path | None]:
    devmode_python = _is_devmode_server(executable)
    cmd = []
    if devmode_python is not None:
        cmd.extend([str(devmode_python), "-I"])
    cmd.extend([str(executable), "--version"])
    env = _server_env({"__EDGEDB_DEVMODE": "1"} if devmode_python else None)
    if sys.platform == "win32":
        cmd = [*_WSL_CMD, *cmd]
    proc = subprocess.run(
        cmd,
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )
    if proc.returncode != 0:
        raise InstanceError(
            f"could not check version of server located at {executable}: "
            f"server exited with code {proc.returncode}\n"
            f"stderr: {proc.stderr}"
        )

    version_line = proc.stdout.strip()

    try:
        return Version.parse_server_line(version_line), devmode_python
    except ValueError:
        raise InstanceError(
            f"could not check version of server located at {executable}: "
            f"server responded with malformed version: {version_line}"
        ) from None


class ServerInfo(NamedTuple):
    executable: pathlib.Path
    version: Version
    devmode_python: pathlib.Path | None
    context: str


def _check_server(
    executable: pathlib.Path,
    min_server_version: VersionConstraint | None = None,
    *,
    context: str,
) -> ServerInfo:
    try:
        version, devmode_python = _get_server_version(executable)
    except Exception as e:
        raise InstanceError(
            f"{context} at `{executable}` appears to "
            f"be broken: checking for version raised this: {e}",
        ) from None
    else:
        if min_server_version is not None and version < min_server_version:
            raise InstanceError(
                f"{context} at `{executable}`, but it is of "
                f"version {version}, which is older than the minimum "
                f"required {min_server_version}",
            )

    return ServerInfo(
        executable=executable,
        version=version,
        devmode_python=devmode_python,
        context=context,
    )


def _server_from_arg(
    executable: pathlib.Path | None,
    min_server_version: VersionConstraint | None = None,
) -> ServerInfo | None:
    if executable is None:
        return None
    else:
        return _check_server(
            executable,
            min_server_version,
            context="specified server",
        )


def _server_from_env(
    min_server_version: VersionConstraint | None = None,
) -> ServerInfo | None:
    for var in ["GEL_SERVER_BINARY", "EDGEDB_SERVER_BINARY"]:
        if path := os.environ.get(var):
            return _check_server(
                pathlib.Path(path),
                min_server_version,
                context=(
                    f"server specified in the {var} environment variable"
                ),
            )

    return None


def _server_from_path(
    min_server_version: VersionConstraint | None = None,
) -> ServerInfo | None:
    executable = _find_server_executable()
    if executable is None:
        return None

    try:
        return _check_server(
            executable,
            min_server_version,
            context="server found in PATH",
        )
    except InstanceError as e:
        # We found _some_ server in PATH but it seems to be broken
        print(e.args[0], file=sys.stderr)
        return None


def _server_from_cli(
    min_server_version: VersionConstraint | None = None,
) -> ServerInfo | None:
    cli_bin = cli.get_cli()
    executable = _get_cli_managed_server_path(cli_bin, min_server_version)
    if executable is None and _download_server(cli_bin, min_server_version):
        executable = _get_cli_managed_server_path(cli_bin, min_server_version)

    if executable is None:
        return None

    try:
        return _check_server(
            executable,
            min_server_version,
            context="server installed by the Gel CLI",
        )
    except InstanceError as e:
        # We found _some_ server in PATH but it seems to be broken
        print(e.args[0], file=sys.stderr)
        return None


def _ensure_server(
    executable: pathlib.Path | None,
    min_server_version: VersionConstraint | None = None,
) -> ServerInfo:
    server = (
        _server_from_arg(executable, min_server_version)
        or _server_from_env(min_server_version)
        or _server_from_path(min_server_version)
        or _server_from_cli(min_server_version)
    )

    if server is None:
        raise InstanceError(
            "could not find appropriate gel-server binary in PATH; "
            "and attempts to install it failed; try modifying PATH "
            "or specify the binary directly via the "
            "GEL_SERVER_BINARY environment variable",
        )

    return server


_AsyncIOClient_T = TypeVar(
    "_AsyncIOClient_T",
    bound=asyncio_client.AsyncIOClient,
    default=asyncio_client.AsyncIOClient,
)
_BlockingIOClient_T = TypeVar(
    "_BlockingIOClient_T",
    bound=blocking_client.Client,
    default=blocking_client.Client,
)


class BaseInstance:
    def get_connect_args(self) -> dict[str, Any]:
        raise NotImplementedError("get_connect_args")

    def get_server_version(self) -> Version:
        raise NotImplementedError("get_server_version")

    async def start(
        self,
        wait: int = 60,
        *,
        port: Optional[int] = None,
        **settings: Any,
    ) -> None:
        raise NotImplementedError("start")

    def stop(self, wait: int = 60) -> None:
        raise NotImplementedError("stop")

    @overload
    def create_blocking_client(
        self,
        *,
        client_class: type[_BlockingIOClient_T],
        connection_class: type[
            blocking_client.BlockingIOConnection
        ] = blocking_client.BlockingIOConnection,
        **kwargs: Any,
    ) -> _BlockingIOClient_T: ...

    @overload
    def create_blocking_client(
        self,
        *,
        connection_class: type[
            blocking_client.BlockingIOConnection
        ] = blocking_client.BlockingIOConnection,
        **kwargs: Any,
    ) -> blocking_client.Client: ...

    def create_blocking_client(
        self,
        *,
        client_class: type[blocking_client.Client] = blocking_client.Client,
        connection_class: type[
            blocking_client.BlockingIOConnection
        ] = blocking_client.BlockingIOConnection,
        **kwargs: Any,
    ) -> blocking_client.Client:
        args = self.get_connect_args() | {"max_concurrency": 1} | kwargs
        return client_class(connection_class=connection_class, **args)

    @overload
    def create_async_client(
        self,
        *,
        client_class: type[_AsyncIOClient_T],
        connection_class: type[
            asyncio_client.AsyncIOConnection
        ] = asyncio_client.AsyncIOConnection,
        **kwargs: Any,
    ) -> _AsyncIOClient_T: ...

    @overload
    def create_async_client(
        self,
        *,
        connection_class: type[
            asyncio_client.AsyncIOConnection
        ] = asyncio_client.AsyncIOConnection,
        **kwargs: Any,
    ) -> asyncio_client.AsyncIOClient: ...

    def create_async_client(
        self,
        *,
        client_class: type[
            asyncio_client.AsyncIOClient
        ] = asyncio_client.AsyncIOClient,
        connection_class: type[
            asyncio_client.AsyncIOConnection
        ] = asyncio_client.AsyncIOConnection,
        **kwargs: Any,
    ) -> asyncio_client.AsyncIOClient:
        args = self.get_connect_args() | {"max_concurrency": 1} | kwargs
        return client_class(connection_class=connection_class, **args)

    @contextlib.contextmanager
    def client(self, /, **kwargs: Any) -> Iterator[gel.Client]:
        client = self.create_blocking_client(**kwargs).ensure_connected()
        try:
            yield client
        finally:
            client.close()

    @contextlib.asynccontextmanager
    async def aclient(
        self, /, **kwargs: Any
    ) -> AsyncIterator[gel.AsyncIOClient]:
        client = self.create_async_client(**kwargs)
        await client.ensure_connected()
        try:
            yield client
        finally:
            await client.aclose()


class ManagedInstance(BaseInstance):
    def __init__(
        self,
        *,
        data_dir: pathlib.Path | None = None,
        min_server_version: VersionConstraint | None = None,
        executable: pathlib.Path | None = None,
        runstate_dir: pathlib.Path | None = None,
        port: int = 0,
        env: Optional[Mapping[str, str]] = None,
        instance_config: Mapping[str, str] | None = None,
        backend_dsn: str | None = None,
        superuser_password: str | None = None,
        trust_all_connections: bool = False,
        testmode: bool = False,
        log_level: Optional[str] = None,
        data_tarball: pathlib.Path | None = None,
    ) -> None:
        server = _ensure_server(executable, min_server_version)
        self._env = {**env} if env is not None else {}
        self._server_cmd: list[str] = []
        self._data_tarball = data_tarball
        if server.devmode_python is not None:
            self._env["__EDGEDB_DEVMODE"] = "1"
            self._server_cmd.extend([str(server.devmode_python), "-I"])

        self._server_cmd.extend(
            [
                str(server.executable),
                "--tls-cert-mode=generate_self_signed",
                "--jose-key-mode=generate",
            ]
        )

        if backend_dsn is not None:
            self._server_cmd.append(f"--backend-dsn={backend_dsn}")

        self._backend_dsn = backend_dsn
        self._data_dir = data_dir
        self._temp_data_dir: tempfile.TemporaryDirectory[str] | None = None

        if runstate_dir:
            self._server_cmd.append(f"--runstate-dir={runstate_dir}")

        if log_level:
            self._server_cmd.append(f"--log-level={log_level}")

        if testmode:
            self._server_cmd.append("--testmode")

        # The default role became admin in nightly build 9024 for 6.0
        if server.version >= (6, 0, VersionStage.DEV, 9024):
            self._default_role = "admin"
        else:
            self._default_role = "edgedb"
        self._server_version = server.version
        self._log_level = log_level
        self._runstate_dir = runstate_dir
        self._daemon_process: Optional[subprocess.Popen[str]] = None
        self._port = port
        self._effective_port = None
        self._tls_cert_file = None
        self._instance_config = instance_config
        self._superuser_password = superuser_password
        self._trust_all_connections = trust_all_connections

    def set_data_tarball(self, path: pathlib.Path) -> None:
        if self.running():
            raise InstanceError("cannot change the data of a running instance")
        self._data_tarball = path

    def get_server_version(self) -> Version:
        return self._server_version

    def get_connect_args(self) -> dict[str, Any]:
        args = {
            "host": "localhost",
            "port": self._effective_port,
            "tls_ca_file": self._tls_cert_file,
        }

        if self._superuser_password is not None:
            args["password"] = self._superuser_password

        return args

    async def start(
        self,
        wait: int = 60,
        *,
        port: Optional[int] = None,
        **settings: Any,
    ) -> None:
        if port is None:
            port = self._port

        cmd_port = "auto" if not port else str(port)
        extra_args = [
            "--{}={}".format(k.replace("_", "-"), v)
            for k, v in settings.items()
        ]
        extra_args.append(f"--port={cmd_port}")
        status_r = status_w = None
        status_r, status_w = socket.socketpair()
        extra_args.append(f"--emit-server-status=fd://{status_w.fileno()}")

        if self._data_tarball is not None:
            if self._data_dir is None:
                self._temp_data_dir = tempfile.TemporaryDirectory(
                    prefix="gel-data-",
                    ignore_cleanup_errors=True,
                )
                self._data_dir = pathlib.Path(self._temp_data_dir.name)

            # We shell out to tar with subprocess instead of using
            # tarfile because it is quite a bit faster.
            subprocess.check_call(
                (
                    "tar",
                    "--extract",
                    "--file",
                    self._data_tarball,
                    "--strip-components=1",
                ),
                cwd=self._data_dir,
            )

        if self._data_dir:
            extra_args.append(f"--data-dir={self._data_dir}")
        else:
            extra_args.append("--temp-dir")

        env = _server_env(self._env)
        server_stdout = None if _debug_enabled else subprocess.DEVNULL

        with self._bootstrap_command_file() as bcmdf:
            args = [
                *self._server_cmd,
                *extra_args,
                f"--bootstrap-command-file={bcmdf}",
            ]
            _debug("running", " ".join(args))
            self._daemon_process = subprocess.Popen(
                args,
                env=env,
                text=True,
                pass_fds=(status_w.fileno(),) if status_w is not None else (),
                stdout=server_stdout,
                stderr=subprocess.STDOUT,
            )

            if status_w is not None:
                status_w.close()

            try:
                await self._wait_for_server(timeout=wait, status_sock=status_r)
            except Exception:
                self.stop()
                raise

        self.post_start()

    def post_start(self) -> None:
        pass

    def running(self) -> bool:
        return (
            self._daemon_process is not None
            and self._daemon_process.returncode is None
        )

    def stop(self, wait: int = 60) -> None:
        if (
            self._daemon_process is not None
            and self._daemon_process.returncode is None
        ):
            self._daemon_process.terminate()
            self._daemon_process.wait(wait)

        if sys.platform == "win32":
            if self._tls_cert_file and os.path.exists(self._tls_cert_file):
                try:
                    os.unlink(self._tls_cert_file)
                except OSError:
                    pass

    def destroy(self) -> None:
        if self.running():
            self.stop()

        if self._temp_data_dir is not None:
            self._temp_data_dir.cleanup()
            self._temp_data_dir = None

        if self._data_dir is not None and self._data_dir.exists():
            shutil.rmtree(self._data_dir)

    def backup(self, path: pathlib.Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        try:
            subprocess.run(
                (
                    "tar",
                    "--create",
                    "--zstd",
                    "--options",
                    "zstd:compression-level=1,zstd:threads=0",
                    "--file",
                    path,
                    ".",
                ),
                cwd=self._data_dir,
                capture_output=True,
                text=True,
                check=True,
            )
        except subprocess.CalledProcessError as e:
            raise InstanceError(
                f"could not backup instance, "
                f"tar exited with code {e.returncode}:\n{e.output}"
            ) from None

    def __del__(self) -> None:
        self.destroy()

    @contextlib.contextmanager
    def _bootstrap_command_file(self) -> Iterator[str]:
        fd, fname = tempfile.mkstemp()
        try:
            cmd = None
            if (pw := self._superuser_password) is not None:
                cmd = (
                    f"ALTER ROLE {self._default_role}"
                    f" SET password := {_edgeql.quote_literal(pw)}"
                )

            elif self._trust_all_connections:
                cmd = textwrap.dedent("""\
                    CONFIGURE INSTANCE INSERT Auth {
                        priority := 0,
                        method := (INSERT Trust),
                    }
                    """)

            if not cmd:
                cmd = "SELECT 1"

            with open(fd, mode="w+", encoding="utf-8") as f:
                f.write(cmd)

            yield fname
        finally:
            os.unlink(fname)

    def _extra_bootstrap_commands(
        self,
        script: Iterable[str] = (),
    ) -> Iterator[str]:
        if (
            self._trust_all_connections
            and self._superuser_password is not None
        ):
            yield textwrap.dedent("""\
                CONFIGURE INSTANCE INSERT Auth {
                    priority := 0,
                    method := (INSERT Trust),
                }
                """)

        if self._instance_config:
            for k, v in self._instance_config.items():
                yield f"CONFIGURE INSTANCE SET {k} := {v}"

        if script:
            yield from script

    async def _wait_for_server(
        self,
        timeout: float = 30.0,
        status_sock: Optional[socket.socket] = None,
    ) -> None:
        async def _read_server_status(
            stream: asyncio.StreamReader,
        ) -> dict[str, Any]:
            while True:
                line = await stream.readline()
                if not line:
                    raise InstanceError("Gel server terminated")
                if line.startswith(b"READY="):
                    break

            _, _, dataline = line.decode().partition("=")
            try:
                data: dict[str, Any] = json.loads(dataline)
            except Exception as e:
                raise InstanceError(
                    f"Gel server returned invalid status line: "
                    f"{dataline!r} ({e})"
                ) from e

            return data

        async def test() -> None:
            stat_reader, stat_writer = await asyncio.open_connection(
                sock=status_sock,
            )
            try:
                data = await asyncio.wait_for(
                    _read_server_status(stat_reader), timeout=timeout
                )
            except asyncio.TimeoutError:
                raise InstanceError(
                    f"Gel server did not initialize within {timeout} seconds"
                ) from None
            finally:
                stat_writer.close()

            self._effective_port = data["port"]
            self._tls_cert_file = data["tls_cert_file"]
            if self._data_dir is None:
                self._data_dir = pathlib.Path(data["socket_dir"])
            if sys.platform == "win32":
                wsl_tls_cert_file = self._tls_cert_file
                fd, self._tls_cert_file = tempfile.mkstemp()
                os.close(fd)
                subprocess.check_call(
                    [
                        *_WSL_CMD,
                        "cp",
                        wsl_tls_cert_file,
                        _get_wsl_path(self._tls_cert_file),
                    ]
                )

        left = timeout
        if status_sock is not None:
            started = time.monotonic()
            await test()
            left -= time.monotonic() - started

        async with self.aclient(
            wait_until_available=f"{max(1, int(left))}s",
        ) as client:
            for cmd in self._extra_bootstrap_commands():
                _debug(f"executing query: {cmd}")
                try:
                    await client.execute(cmd)
                except gel.ConstraintViolationError:
                    pass
            await client.execute("select 1")

    def has_create_database(self) -> bool:
        return True

    def has_create_role(self) -> bool:
        return True

    def get_data_dir(self) -> pathlib.Path | None:
        return self._data_dir

    def set_data_dir(self, data_dir: pathlib.Path) -> None:
        if self.running():
            raise InstanceError(
                "cannot change the data directory of a running instance"
            )
        self._data_dir = data_dir


class PersistentInstance(ManagedInstance):
    def __init__(
        self,
        *,
        data_dir: pathlib.Path,
        min_server_version: VersionConstraint | None = None,
        executable: pathlib.Path | None = None,
        runstate_dir: pathlib.Path,
        port: int = 5656,
        env: Optional[Mapping[str, str]] = None,
        testmode: bool = False,
        log_level: Optional[str] = None,
    ) -> None:
        self._runstate_dir = runstate_dir
        super().__init__(
            data_dir=data_dir,
            executable=executable,
            min_server_version=min_server_version,
            port=port,
            env=env,
            testmode=testmode,
            log_level=log_level,
        )
        self._server_cmd.append(f"--runstate-dir={self._runstate_dir}")

    def get_data_dir(self) -> pathlib.Path:
        assert self._data_dir is not None
        return self._data_dir


class TempInstance(ManagedInstance):
    def __init__(
        self,
        *,
        data_dir: pathlib.Path | None = None,
        min_server_version: VersionConstraint | None = None,
        executable: pathlib.Path | None = None,
        env: Optional[Mapping[str, str]] = None,
        instance_config: Mapping[str, str] | None = None,
        backend_dsn: str | None = None,
        superuser_password: str | None = None,
        trust_all_connections: bool = False,
        testmode: bool = False,
        log_level: Optional[str] = None,
        cleanup_atexit: bool = True,
        data_tarball: pathlib.Path | None = None,
    ) -> None:
        super().__init__(
            data_dir=data_dir,
            min_server_version=min_server_version,
            executable=executable,
            env=env,
            instance_config=instance_config,
            backend_dsn=backend_dsn,
            superuser_password=superuser_password,
            trust_all_connections=trust_all_connections,
            testmode=testmode,
            log_level=log_level,
            data_tarball=data_tarball,
        )
        if cleanup_atexit:
            self._server_cmd.append("--auto-shutdown-after=5")
        self._cleanup_atexit = cleanup_atexit

    def post_start(self) -> None:
        super().post_start()
        if self._cleanup_atexit:
            self._client = self.create_blocking_client(
                client_class=blocking_client.Client
            ).ensure_connected()
            atexit.register(self._client.close)


class TestInstance(TempInstance):
    def __init__(
        self,
        *,
        data_dir: pathlib.Path | None = None,
        min_server_version: VersionConstraint | None = None,
        executable: pathlib.Path | None = None,
        env: Optional[Mapping[str, str]] = None,
        instance_config: dict[str, str] | None = None,
        backend_dsn: str | None = None,
        superuser_password: str | None = "test",  # noqa: S107
        trust_all_connections: bool = True,
        testmode: bool = True,
        log_level: Optional[str] = None,
        cleanup_atexit: bool = True,
        data_tarball: pathlib.Path | None = None,
    ) -> None:
        default_instance_config = {
            "session_idle_transaction_timeout": "<duration>'5 minutes'",
            "session_idle_timeout": "<duration>'0 seconds'",
        }
        if instance_config is None:
            instance_config = default_instance_config
        else:
            instance_config = default_instance_config | instance_config

        super().__init__(
            data_dir=data_dir,
            min_server_version=min_server_version,
            executable=executable,
            env=env,
            instance_config=instance_config,
            backend_dsn=backend_dsn,
            superuser_password=superuser_password,
            trust_all_connections=trust_all_connections,
            testmode=testmode,
            log_level=log_level,
            cleanup_atexit=cleanup_atexit,
            data_tarball=data_tarball,
        )


class RunningInstance(BaseInstance):
    def __init__(
        self,
        *,
        server_version: Version | None = None,
        conn_args: dict[str, Any],
    ) -> None:
        self.conn_args = conn_args
        self._server_version = server_version

    def ensure_initialized(self) -> bool:
        return False

    def get_connect_args(self) -> dict[str, Any]:
        return dict(self.conn_args)

    def get_server_version(self) -> Version:
        if self._server_version is None:
            raise InstanceError(
                "server version of running instance is not yet known, "
                "please call start() first"
            )
        return self._server_version

    async def get_status(self) -> str:
        return "running"

    async def start(
        self,
        wait: int = 60,
        *,
        port: Optional[int] = None,
        **settings: Any,
    ) -> None:
        if port is not None:
            raise InstanceError(
                "running instance cannot have its port changed"
            )

        if self._server_version is None:
            async with self.aclient() as client:
                version = await client.query_required_single(
                    "select sys::get_version()",
                )

            self._server_version = Version(
                major=version.major,
                minor=version.minor,
                stage=getattr(
                    VersionStage,
                    str(version.stage).upper(),
                    VersionStage.FINAL,
                ),
                stage_no=version.stage_no,
            )

    def stop(self, wait: int = 60) -> None:
        pass

    def destroy(self) -> None:
        pass

    def has_create_database(self) -> bool:
        return os.environ.get("EDGEDB_TEST_CASES_SET_UP") != "inplace"

    def has_create_role(self) -> bool:
        return os.environ.get("EDGEDB_TEST_HAS_CREATE_ROLE") == "True"
