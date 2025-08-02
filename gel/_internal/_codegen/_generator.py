# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.


from __future__ import annotations
from typing import (
    TYPE_CHECKING,
    Any,
    TextIO,
)

import getpass
import io
import json
import os
import pathlib
import sys
import typing

import gel
from gel.con_utils import find_gel_project_dir
from gel._internal._color import get_color

if TYPE_CHECKING:
    import argparse


C = get_color()


class AbstractCodeGenerator:
    def __init__(
        self,
        args: argparse.Namespace,
        *,
        project_dir: pathlib.Path | None = None,
        client: gel.Client | None = None,
        interactive: bool = True,
    ):
        self._no_cache = False
        self._quiet = False
        self._default_module = "default"
        self._async = False

        self._interactive = interactive
        self._stderr: TextIO
        if not interactive:
            self._stderr = io.StringIO()
        else:
            self._stderr = sys.stderr

        if project_dir is None:
            try:
                self._project_dir = pathlib.Path(find_gel_project_dir())
            except gel.ClientConnectionError:
                self.print_error(
                    "Cannot find gel.toml: "
                    "codegen must be run inside a Gel project directory"
                )
                self.abort(2)

            if not self._quiet:
                self.print_msg(
                    f"Found Gel project: {C.BOLD}{self._project_dir}{C.ENDC}"
                )
        else:
            self._project_dir = project_dir

        self._apply_env_config()
        self._apply_cli_config(args)

        if client is None:
            self._client = gel.create_client(**self._get_conn_args(args))
        else:
            self._client = client

    def _apply_cli_config(self, args: argparse.Namespace) -> None:
        if args.no_cache is not None:
            self._no_cache = args.no_cache
        if args.quiet is not None:
            self._quiet = args.quiet

    def _apply_env_config(self) -> None:
        """
        Apply environment configuration.

        _GEL_MANIFEST is a JSON string from the Gel CLI in the form:
        {
            "generate-config": {
                "no_cache": {
                    "value": true,
                    "source": {
                        "span": [<start>, <end>],
                        "manifest": "project"
                    }
                },
                "some_other_config": {
                    "value": ...,
                    "source": {
                        "span": [<start>, <end>],
                        "manifest": "project"
                    }
                }
            },
            "manifests": {
                "project": "path/to/gel.toml"
            }
        }
        """
        manifest_str = os.getenv("_GEL_MANIFEST")
        if not manifest_str:
            return
        manifest = json.loads(manifest_str)
        if not isinstance(manifest, dict):
            raise ValueError("_GEL_MANIFEST must be a JSON object")
        config = manifest.get("generate-config")
        if not config:
            return
        if not isinstance(config, dict):
            raise ValueError("generate-config must be a JSON object")
        for key, value in config.items():
            if not isinstance(value, dict):
                raise ValueError(
                    f"Invalid generate-config value for {key!r}: "
                    f"expected a JSON object, got {type(value).__name__}"
                )
            m = getattr(self, f"_apply_env_{key}", None)
            if m is None:
                if not self._quiet:
                    self.print_msg(
                        f"{C.WARNING}Skipping unknown environment config: "
                        f"{key}{C.ENDC}"
                    )
                continue
            try:
                m(value["value"])
            except ValueError as e:
                source = value.get("source")
                manifests = manifest.get("manifests")
                if source and manifests:
                    try:
                        self._print_env_config_error(e, source, manifests)
                    except Exception as print_err:
                        raise RuntimeError(
                            "Failed to print error of environment config"
                        ) from print_err
                    else:
                        sys.exit(22)
                else:
                    raise

    def _print_env_config_error(
        self, error: ValueError, source: Any, manifests: Any
    ) -> None:
        self.print_error(f"{type(error).__name__}: {error}")
        if self._quiet:
            return
        if not isinstance(source, dict):
            raise ValueError("source must be a JSON object")
        manifest = source.get("manifest")
        if not isinstance(manifest, str):
            raise ValueError("manifest must be a string")
        if not isinstance(manifests, dict):
            raise ValueError("manifests must be a JSON object")
        path = manifests.get(manifest)
        if not path:
            return
        if not isinstance(path, str):
            raise ValueError("manifest-path must be a string")
        span = source.get("span")
        if not isinstance(span, list) or len(span) != 2:
            raise ValueError("span must be a list of two integers")
        start, end = span
        if not isinstance(start, int) or not isinstance(end, int):
            raise ValueError("span must be a list of two integers")
        src = pathlib.Path(path).read_text(encoding="utf8")
        marking = False
        lines = src.splitlines(keepends=True)
        left = len(str(len(lines))) + 1
        offset = 0
        for no, line in enumerate(lines):
            llen = len(line.encode("utf8"))
            if not marking and offset < start < offset + llen:
                self.print_msg(
                    f"{'':>{left}}{C.BLUE}-->{C.ENDC} "
                    f"gel.toml:{no + 1}:{start - offset + 1}"
                )
                self.print_msg(f"{'':>{left}}{C.BLUE} |{C.ENDC}")
                marking = True
            if marking:
                self.print_msg(
                    f"{C.BLUE}{no + 1:>{left}} | {C.ENDC}{line.rstrip()}"
                )
                n_spaces = max(0, start - offset)
                mark_len = min(llen - n_spaces, end - offset - n_spaces)
                self.print_msg(
                    f"{'':>{left}}{C.BLUE} | {C.ENDC}"
                    f"{'':>{n_spaces}}"
                    f"{C.FAIL}{'':^>{mark_len}}{C.ENDC}"
                )
                if offset < end < offset + llen:
                    break
            offset += llen
        self.print_msg(f"{'':>{left}}{C.BLUE} |{C.ENDC}")

    def _apply_env_no_cache(self, value: Any) -> None:
        if not isinstance(value, bool):
            raise ValueError('"no_cache" must be a boolean')
        self._no_cache = value

    def get_error_output(self) -> str:
        if isinstance(self._stderr, io.StringIO):
            return self._stderr.getvalue()
        else:
            raise RuntimeError("Cannot get error output in non-silent mode")

    def abort(self, code: int) -> typing.NoReturn:
        if self._interactive:
            raise RuntimeError(f"aborting codegen, code={code}")
        else:
            sys.exit(code)

    def print_msg(self, msg: str) -> None:
        print(msg, file=self._stderr)

    def print_error(self, msg: str) -> None:
        print(
            f"{C.BOLD}{C.FAIL}error: {C.ENDC}{C.BOLD}{msg}{C.ENDC}",
            file=self._stderr,
        )

    def _get_conn_args(self, args: argparse.Namespace) -> dict[str, Any]:
        if args.password_from_stdin:
            if args.password:
                self.print_error(
                    "--password and --password-from-stdin "
                    "are mutually exclusive",
                )
                self.abort(22)
            if sys.stdin.isatty():
                password = getpass.getpass()
            else:
                password = sys.stdin.read().strip()
        else:
            password = args.password
        if args.dsn and args.instance:
            self.print_error("--dsn and --instance are mutually exclusive")
            self.abort(22)
        return dict(
            dsn=args.dsn or args.instance,
            credentials_file=args.credentials_file,
            host=args.host,
            port=args.port,
            database=args.database,
            user=args.user,
            password=password,
            tls_ca_file=args.tls_ca_file,
            tls_security=args.tls_security,
        )
