#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2022-present MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from typing import (
    Any,
    Iterator,
    Literal,
    Optional,
    TypeAlias,
    Union,
)

import argparse
import collections
import contextlib
import enum
import getpass
import io
import os
import pathlib
import sys
import textwrap
from collections import defaultdict

import gel
from gel.con_utils import find_gel_project_dir
from gel.color import get_color


C = get_color()
if pyver_env := os.environ.get("GEL_PYTHON_CODEGEN_PY_VER"):
    try:
        SYS_VERSION_INFO = tuple(map(int, pyver_env.split(".")))[:2]
    except ValueError:
        raise ValueError(
            f"invalid version in GEL_PYTHON_CODEGEN_PY_VER: {pyver_env}"
        )
else:
    SYS_VERSION_INFO = sys.version_info[:2]

TYPE_MAPPING = {
    "std::str": "str",
    "std::float32": "float",
    "std::float64": "float",
    "std::int16": "int",
    "std::int32": "int",
    "std::int64": "int",
    "std::bigint": "int",
    "std::bool": "bool",
    "std::uuid": "uuid.UUID",
    "std::bytes": "bytes",
    "std::decimal": "decimal.Decimal",
    "std::datetime": "datetime.datetime",
    "std::duration": "datetime.timedelta",
    "std::json": "str",
    "cal::local_date": "datetime.date",
    "cal::local_time": "datetime.time",
    "cal::local_datetime": "datetime.datetime",
    "cal::relative_duration": "gel.RelativeDuration",
    "cal::date_duration": "gel.DateDuration",
    "cfg::memory": "gel.ConfigMemory",
    "ext::pgvector::vector": "array.array",
}

TYPE_IMPORTS = {
    "std::uuid": "uuid",
    "std::decimal": "decimal",
    "std::datetime": "datetime",
    "std::duration": "datetime",
    "cal::local_date": "datetime",
    "cal::local_time": "datetime",
    "cal::local_datetime": "datetime",
    "ext::pgvector::vector": "array",
}

INPUT_TYPE_MAPPING = TYPE_MAPPING.copy()
INPUT_TYPE_MAPPING.update(
    {
        "ext::pgvector::vector": "typing.Sequence[float]",
    }
)

INPUT_TYPE_IMPORTS = TYPE_IMPORTS.copy()
INPUT_TYPE_IMPORTS.update(
    {
        "ext::pgvector::vector": "typing",
    }
)

PYDANTIC_MIXIN = """\
class NoPydanticValidation:
    @classmethod
    def __get_pydantic_core_schema__(cls, _source_type, _handler):
        # Pydantic 2.x
        from pydantic_core.core_schema import any_schema
        return any_schema()

    @classmethod
    def __get_validators__(cls):
        # Pydantic 1.x
        from pydantic.dataclasses import dataclass as pydantic_dataclass
        _ = pydantic_dataclass(cls)
        cls.__pydantic_model__.__get_validators__ = lambda: []
        return []\
"""


def print_msg(msg: str) -> None:
    print(msg, file=sys.stderr)


def print_error(msg: str) -> None:
    print_msg(f"{C.BOLD}{C.FAIL}error: {C.ENDC}{C.BOLD}{msg}{C.ENDC}")


def _get_conn_args(args: argparse.Namespace) -> dict[str, Any]:
    if args.password_from_stdin:
        if args.password:
            print_error(
                "--password and --password-from-stdin are mutually exclusive",
            )
            sys.exit(22)
        if sys.stdin.isatty():
            password = getpass.getpass()
        else:
            password = sys.stdin.read().strip()
    else:
        password = args.password
    if args.dsn and args.instance:
        print_error("--dsn and --instance are mutually exclusive")
        sys.exit(22)
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


class Generator:
    def __init__(self, args: argparse.Namespace):
        self._default_module = "default"
        self._async = False
        try:
            self._project_dir = pathlib.Path(find_gel_project_dir())
        except gel.ClientConnectionError:
            print(
                "Cannot find gel.toml: "
                "codegen must be run inside a Gel project directory"
            )
            sys.exit(2)
        print_msg(f"Found Gel project: {C.BOLD}{self._project_dir}{C.ENDC}")

        self._client = gel.create_client(**_get_conn_args(args))


class _ImportSection(enum.Enum):
    local = enum.auto()
    lib = enum.auto()
    std = enum.auto()


class _ImportTime(enum.Enum):
    runtime = enum.auto()
    typecheck = enum.auto()


class _ImportKind(enum.Enum):
    names = enum.auto()
    self = enum.auto()


_ImportKey: TypeAlias = tuple[_ImportSection, _ImportTime, _ImportKind]
_Imports: TypeAlias = defaultdict[_ImportKey, defaultdict[str, set[str]]]


class GeneratedModule:
    INDENT = " " * 4

    def __init__(self, preamble: str) -> None:
        self._comment_preamble = preamble
        self._indent_level = 0
        self._chunks: list[str] = []
        self._imports: _Imports = defaultdict(lambda: defaultdict(set))

    def has_content(self) -> bool:
        return len(self._chunks) > 0

    def indent(self, levels: int = 1) -> None:
        self._indent_level += levels

    def dedent(self, levels: int = 1) -> None:
        if self._indent_level > 0:
            self._indent_level -= levels

    def _import_module(
        self,
        module: str,
        imports: dict[_ImportKind, list[str]],
        import_time: _ImportTime,
    ) -> None:
        if module == "gel" or module.startswith("gel."):
            section = _ImportSection.lib
        elif module.startswith("."):
            section = _ImportSection.local
        else:
            section = _ImportSection.std

        for import_kind, import_strings in imports.items():
            key = section, import_time, import_kind
            self._imports[key][module].update(import_strings)

    def _import_names(
        self,
        module: str,
        names: tuple[str, ...],
        aliases: dict[str, str],
        *,
        import_time: _ImportTime,
    ) -> None:
        all_names: list[str] = []
        all_self_aliases: list[str] = []
        for n in names:
            if n == ".":
                all_self_aliases.append("")
            else:
                all_names.append(n)

        for k, v in aliases.items():
            if v == ".":  # module import
                all_self_aliases.append(k)
            else:
                all_names.append(f"{v} as {k}")

        if not all_names and not all_self_aliases:
            all_self_aliases.append("")

        self._import_module(
            module,
            {_ImportKind.names: all_names, _ImportKind.self: all_self_aliases},
            import_time=import_time,
        )

    def import_names(self, module: str, *names: str, **aliases: str) -> None:
        self._import_names(
            module, names, aliases, import_time=_ImportTime.runtime)

    def import_type_names(
        self,
        module: str,
        *names: str,
        **aliases: str,
    ) -> None:
        self.import_names("typing")
        self._import_names(
            module, names, aliases, import_time=_ImportTime.typecheck)

    @contextlib.contextmanager
    def indented(self) -> Iterator[None]:
        self._indent_level += 1
        try:
            yield
        finally:
            self._indent_level -= 1

    def reset_indent(self) -> None:
        self._indent_level = 0

    def write(self, text: str = "") -> None:
        chunk = textwrap.indent(text, prefix=self.INDENT * self._indent_level)
        self._chunks.append(chunk)

    def write_section_break(self, size: int = 2) -> None:
        self._chunks.extend([""] * size)

    def get_comment_preamble(self) -> str:
        return self._comment_preamble

    def render_imports(self) -> str:
        sections = ["from __future__ import annotations"]
        for section_key in _ImportSection.__members__.values():
            key = (section_key, _ImportTime.runtime)
            sections.append(self._render_imports(*key))

        typecheck_sections = []
        for section_key in _ImportSection.__members__.values():
            key = (section_key, _ImportTime.typecheck)
            typecheck_sections.append(
                self._render_imports(*key, indent="    "))

        if any(typecheck_sections):
            sections.append("if typing.TYPE_CHECKING:")
            sections.extend(typecheck_sections)

        return "\n\n".join(filter(None, sections))

    def _render_imports(
        self,
        section: _ImportSection,
        import_time: _ImportTime,
        *,
        indent: str = "",
    ) -> str:
        output = []
        imports = self._imports[section, import_time, _ImportKind.self]
        mods = sorted(imports.items(), key=lambda kv: (len(kv[1]) == 0, kv[0]))
        for modname, aliases in mods:
            for alias in aliases:
                if modname.startswith("."):
                    relative, _, modname = modname.rpartition(".")
                    import_line = f"from .{relative} import {modname}"
                    if alias:
                        import_line += f" as {alias}"
                else:
                    if alias:
                        import_line = f"import {modname} as {alias}"
                    else:
                        import_line = f"import {modname}"
                output.append(import_line)

        imports = self._imports[section, import_time, _ImportKind.names]
        mods = sorted(imports.items(), key=lambda kv: (len(kv[1]) == 0, kv[0]))
        for modname, names in mods:
            if not names:
                continue
            import_line = f"from {modname} import "
            names_list = list(names)
            names_list.sort()
            names_part = ", ".join(names_list)
            if len(import_line) + len(names_part) > 79:
                names_part = "(\n    " + ",\n    ".join(names_list) + "\n)"
            import_line += names_part
            output.append(import_line)

        result = "\n".join(output)
        if indent:
            result = textwrap.indent(result, indent)
        return result

    def output(self, out: io.TextIOWrapper) -> None:
        out.write(self.get_comment_preamble())
        out.write("\n\n")
        out.write(self.render_imports())
        out.write("\n\n\n")
        out.write("\n".join(self._chunks))
        out.write("\n")
