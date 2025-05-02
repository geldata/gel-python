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
    TypeAlias,
)
from collections.abc import Iterator, Mapping

import argparse
import collections
import collections.abc
import contextlib
import enum
import getpass
import io
import os
import pathlib
import re
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
        ) from None
else:
    SYS_VERSION_INFO = sys.version_info[:2]

TYPE_MAPPING: dict[str, str | tuple[str, str]] = {
    "std::str": "str",
    "std::float32": "float",
    "std::float64": "float",
    "std::int16": "int",
    "std::int32": "int",
    "std::int64": "int",
    "std::bigint": "int",
    "std::bool": "bool",
    "std::uuid": ("uuid", "UUID"),
    "std::bytes": "bytes",
    "std::decimal": ("decimal", "Decimal"),
    "std::datetime": ("datetime", "datetime"),
    "std::duration": ("datetime", "timedelta"),
    "std::json": "str",
    "cal::local_date": ("datetime", "date"),
    "cal::local_time": ("datetime", "time"),
    "cal::local_datetime": ("datetime", "datetime"),
    "cal::relative_duration": ("gel", "RelativeDuration"),
    "cal::date_duration": ("gel", "DateDuration"),
    "cfg::memory": ("gel", "ConfigMemory"),
    "ext::pgvector::vector": ("array", "array"),
}

INPUT_TYPE_MAPPING = dict(TYPE_MAPPING)
INPUT_TYPE_MAPPING.update(
    {
        "ext::pgvector::vector": ("typing", "Sequence[float]"),
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

MAX_LINE_LENGTH = 79


def print_msg(msg: str) -> None:
    print(msg, file=sys.stderr)  # noqa: T201


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
            print_error(
                "Cannot find gel.toml: "
                "codegen must be run inside a Gel project directory"
            )
            sys.exit(2)
        print_msg(f"Found Gel project: {C.BOLD}{self._project_dir}{C.ENDC}")

        self._client = gel.create_client(**_get_conn_args(args))


class _ImportSource(enum.Enum):
    local = enum.auto()
    lib = enum.auto()
    std = enum.auto()


class ImportTime(enum.Enum):
    runtime = enum.auto()
    late_runtime = enum.auto()
    typecheck = enum.auto()


class _ImportKind(enum.Enum):
    names = enum.auto()
    self = enum.auto()


class CodeSection(enum.Enum):
    main = enum.auto()
    after_late_import = enum.auto()


_Imports: TypeAlias = defaultdict[
    _ImportSource,
    defaultdict[_ImportKind, defaultdict[str, set[str]]],
]


def _new_imports_map() -> _Imports:
    return defaultdict(lambda: defaultdict(lambda: defaultdict(set)))


class GeneratedModule:
    INDENT = " " * 4

    def __init__(self, preamble: str) -> None:
        self._comment_preamble = preamble
        self._indent_level = 0
        self._content: defaultdict[CodeSection, list[str]] = defaultdict(list)
        self._code_section = CodeSection.main
        self._code = self._content[self._code_section]
        self._imports: defaultdict[ImportTime, _Imports] = defaultdict(
            _new_imports_map
        )
        self._globals: set[str] = set()
        self._exports: set[str] = set()
        self._imported_names: dict[
            tuple[str, str, str | None, ImportTime], str
        ] = {}

    def has_content(self) -> bool:
        return any(
            self.section_has_content(section) for section in self._content
        ) or bool(self._exports)

    def section_has_content(self, section: CodeSection) -> bool:
        return bool(self._content[section])

    def indent(self, levels: int = 1) -> None:
        self._indent_level += levels

    def dedent(self, levels: int = 1) -> None:
        if self._indent_level > 0:
            self._indent_level -= levels

    def has_global(self, name: str) -> bool:
        return name in self._globals

    def add_global(self, name: str) -> None:
        self._globals.add(name)

    def udpate_globals(self, names: collections.abc.Iterable[str]) -> None:
        self._globals.update(names)

    def _get_import_strings(
        self,
        module: str,
        names: tuple[str, ...],
        aliases: dict[str, str],
    ) -> dict[_ImportKind, list[str]]:
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

        return {
            _ImportKind.names: all_names,
            _ImportKind.self: all_self_aliases,
        }

    def _get_import_source(self, module: str) -> _ImportSource:
        if module == "gel" or module.startswith("gel."):
            source = _ImportSource.lib
        elif module.startswith("."):
            source = _ImportSource.local
        else:
            source = _ImportSource.std

        return source

    def _update_import_maps(
        self,
        module: str,
        names: tuple[str, ...],
        aliases: dict[str, str],
        *,
        import_maps: _Imports,
    ) -> None:
        source = self._get_import_source(module)
        import_lines = self._get_import_strings(module, names, aliases)
        for import_kind, import_strings in import_lines.items():
            import_maps[source][import_kind][module].update(import_strings)

    def _disambiguate_import_name(self, name: str) -> str:
        if name not in self._globals:
            return name

        ctr = 0

        def _mangle(name: str) -> str:
            nonlocal ctr
            if ctr == 0:
                return f"__{name}__"
            else:
                return f"__{name}_{ctr}__"

        mangled = _mangle(name)
        while mangled in self._globals:
            ctr += 1
            mangled = _mangle(name)

        return mangled

    def _import_name(
        self,
        module: str,
        name: str,
        *,
        alias: str | None = None,
        import_maps: _Imports,
    ) -> tuple[str, str]:
        if alias is not None:
            imported = self._disambiguate_import_name(alias)
            self._update_import_maps(
                module,
                (),
                {imported: name},
                import_maps=import_maps,
            )
            new_global = imported
        elif name != "." and name not in self._globals:
            self._update_import_maps(
                module,
                (name,),
                {},
                import_maps=import_maps,
            )
            new_global = imported = name
        elif all(c == "." for c in module):
            imported_as = self._disambiguate_import_name(name)
            self._update_import_maps(
                module,
                (),
                {imported_as: name},
                import_maps=import_maps,
            )
            imported = new_global = imported_as
        else:
            parent_module, _, tail_module = module.rpartition(".")
            if parent_module:
                imported_as = self._disambiguate_import_name(tail_module)
                self._update_import_maps(
                    parent_module,
                    (),
                    {imported_as: tail_module},
                    import_maps=import_maps,
                )

            else:
                imported_as = self._disambiguate_import_name(module)
                self._update_import_maps(
                    module,
                    (),
                    {imported_as: "."},
                    import_maps=import_maps,
                )

            new_global = imported_as
            imported = imported_as if name == "." else f"{imported_as}.{name}"

        return imported, new_global

    def _do_import_name(
        self,
        module: str,
        name: str,
        *,
        alias: str | None = None,
        import_time: ImportTime = ImportTime.runtime,
    ) -> str:
        key = (module, name, alias, import_time)
        imported = self._imported_names.get(key)
        if imported is not None:
            return imported

        if import_time is ImportTime.late_runtime:
            # See if there was a previous eager import for same name
            early_key = (module, name, alias, ImportTime.runtime)
            imported = self._imported_names.get(early_key)
            if imported is not None:
                self._imported_names[key] = imported
                return imported

        imported, new_global = self._import_name(
            module,
            name,
            alias=alias,
            import_maps=self._imports[import_time],
        )

        self._globals.add(new_global)
        self._imported_names[key] = imported

        return imported

    def import_name(
        self,
        module: str,
        name: str,
        *,
        alias: str | None = None,
        import_time: ImportTime = ImportTime.runtime,
        directly: bool = True,
    ) -> str:
        if directly:
            return self._do_import_name(
                module, name, alias=alias, import_time=import_time
            )
        else:
            mod = self._do_import_name(
                module, ".", alias=alias, import_time=import_time
            )
            return f"{mod}.{name}"

    def render_name_import(
        self,
        module: str,
        name: str,
        *,
        alias: str | None = None,
    ) -> tuple[str, str]:
        import_maps = _new_imports_map()
        imported, _ = self._import_name(
            module,
            name,
            alias=alias,
            import_maps=import_maps,
        )

        import_chunks = self._render_imports(import_maps)
        import_code = "\n\n".join(import_chunks)

        return imported, import_code

    def export(self, *names: str) -> None:
        self._exports.update(names)

    def current_indentation(self) -> str:
        return self.INDENT * self._indent_level

    @contextlib.contextmanager
    def indented(self) -> Iterator[None]:
        self._indent_level += 1
        try:
            yield
        finally:
            self._indent_level -= 1

    @contextlib.contextmanager
    def type_checking(self) -> Iterator[None]:
        tc = self.import_name("typing", "TYPE_CHECKING")
        self.write(f"if {tc}:")
        with self.indented():
            yield

    @contextlib.contextmanager
    def not_type_checking(self) -> Iterator[None]:
        tc = self.import_name("typing", "TYPE_CHECKING")
        self.write(f"if not {tc}:")
        with self.indented():
            yield

    @contextlib.contextmanager
    def code_section(self, section: CodeSection) -> Iterator[None]:
        orig_section = self._code_section
        self._code_section = section
        self._code = self._content[self._code_section]
        try:
            yield
        finally:
            self._code_section = orig_section
            self._code = self._content[self._code_section]

    def reset_indent(self) -> None:
        self._indent_level = 0

    def write(self, text: str = "") -> None:
        chunk = textwrap.indent(text, prefix=self.INDENT * self._indent_level)
        self._code.append(chunk)

    def write_section_break(self, size: int = 2) -> None:
        self._code.extend([""] * size)

    def get_comment_preamble(self) -> str:
        return self._comment_preamble

    def render_exports(self) -> str:
        if self._exports:
            return "\n".join(
                [
                    "__all__ = (",
                    *(f"    {ex!r}," for ex in sorted(self._exports)),
                    ")",
                ]
            )
        else:
            return ""

    def render_imports(self) -> str:
        typecheck_sections = self._render_imports(
            self._imports[ImportTime.typecheck],
            indent="    ",
        )

        tc = None
        if any(typecheck_sections):
            tc = self.import_name("typing", "TYPE_CHECKING")

        sections = ["from __future__ import annotations"]
        sections.extend(
            self._render_imports(self._imports[ImportTime.runtime])
        )

        if any(typecheck_sections):
            assert tc
            sections.append(f"if {tc}:")
            sections.extend(typecheck_sections)

        return "\n\n".join(filter(None, sections))

    def render_late_imports(self) -> str:
        sections = self._render_imports(
            self._imports[ImportTime.late_runtime],
            noqa=["E402", "F403"],
        )

        return "\n\n".join(filter(None, sections))

    def _render_imports(
        self,
        imports: _Imports,
        *,
        indent: str = "",
        noqa: list[str] | None = None,
    ) -> list[str]:
        blocks = []
        for source in _ImportSource.__members__.values():
            block = self._render_imports_source_block(
                imports[source],
                indent=indent,
                noqa=noqa,
            )
            if block:
                blocks.append(block)
        return blocks

    def _render_imports_source_block(
        self,
        imports: Mapping[_ImportKind, Mapping[str, set[str]]],
        *,
        indent: str = "",
        noqa: list[str] | None = None,
    ) -> str:
        output = []
        self_imports = imports[_ImportKind.self]
        mods = sorted(
            self_imports.items(),
            key=lambda kv: (len(kv[1]) == 0, kv[0]),
        )
        for modname, aliases in mods:
            for alias in aliases:
                if modname.startswith("."):
                    match = re.match(r"^(\.+)(.*)", modname)
                    assert match
                    relative = match.group(1)
                    rest = match.group(2)
                    pkg, _, name = rest.rpartition(".")
                    import_line = f"from {relative}{pkg} import {name}"
                    if alias and alias != name:
                        import_line += f" as {alias}"
                elif alias:
                    import_line = f"import {modname} as {alias}"
                else:
                    import_line = f"import {modname}"
                if noqa:
                    import_line += f"  # noqa: {' '.join(noqa)}"
                output.append(import_line)

        name_imports = imports[_ImportKind.names]
        mods = sorted(
            name_imports.items(),
            key=lambda kv: (len(kv[1]) == 0, kv[0]),
        )

        noqa_suf = f"  # noqa: {' '.join(noqa)}" if noqa else ""

        for modname, names in mods:
            if not names:
                continue
            import_line = f"from {modname} import "
            names_list = list(names)
            names_list.sort()
            names_part = ", ".join(names_list)
            if len(import_line) + len(names_part) > MAX_LINE_LENGTH:
                import_line += (
                    f"({noqa_suf}\n    " + ",\n    ".join(names_list) + "\n)"
                )
            else:
                import_line += names_part + noqa_suf
            output.append(import_line)

        result = "\n".join(output)
        if indent:
            result = textwrap.indent(result, indent)
        return result

    def output(self, out: io.TextIOWrapper) -> None:
        out.write(self.get_comment_preamble())
        out.write("\n\n")
        out.write(self.render_imports())
        main_code = self._content[CodeSection.main]
        if main_code:
            out.write("\n\n\n")
            out.write("\n".join(main_code))
        late_imports = self.render_late_imports()
        if late_imports:
            out.write("\n\n\n")
            out.write(late_imports)
        late_code = self._content[CodeSection.after_late_import]
        if late_code:
            out.write("\n\n\n")
            out.write("\n".join(late_code))
        exports = self.render_exports()
        if exports:
            out.write("\n\n\n")
            out.write(exports)
        out.write("\n")
