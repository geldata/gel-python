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

import argparse
import io
import os
import pathlib
import sys
import textwrap
import typing

import gel
from gel import abstract
from gel import describe
from gel.con_utils import find_gel_project_dir
from gel.color import get_color
from gel.compatibility.clihelper import print_msg, print_error, _get_conn_args


C = get_color()
SYS_VERSION_INFO = os.getenv("EDGEDB_PYTHON_CODEGEN_PY_VER")
if SYS_VERSION_INFO:
    SYS_VERSION_INFO = tuple(map(int, SYS_VERSION_INFO.split(".")))[:2]
else:
    SYS_VERSION_INFO = sys.version_info[:2]

INDENT = "    "
SUFFIXES = [
    ("async", "_async_edgeql.py", True),
    ("blocking", "_edgeql.py", False),
]
FILE_MODE_OUTPUT_FILE = "generated"

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


class Generator:
    def __init__(self, args: argparse.Namespace):
        self._default_module = "default"
        self._targets = args.target
        self._skip_pydantic_validation = args.skip_pydantic_validation
        self._async = False
        try:
            self._project_dir = pathlib.Path(find_gel_project_dir())
        except gel.ClientConnectionError:
            print(
                "Cannot find gel.toml: "
                "codegen must be run under an EdgeDB project dir"
            )
            sys.exit(2)
        print_msg(f"Found EdgeDB project: {C.BOLD}{self._project_dir}{C.ENDC}")
        self._client = gel.create_client(**_get_conn_args(args))
        self._single_mode_files = args.file
        self._search_dirs = []
        for search_dir in args.dir or []:
            search_dir = pathlib.Path(search_dir).absolute()
            if (
                search_dir == self._project_dir
                or self._project_dir in search_dir.parents
            ):
                self._search_dirs.append(search_dir)
            else:
                print(
                    f"--dir '{search_dir}' is not under "
                    f"the project directory: {self._project_dir}"
                )
                sys.exit(1)
        self._method_names = set()
        self._describe_results = []

        self._cache = {}
        self._imports = set()
        self._aliases = {}
        self._defs = {}
        self._names = set()
        self._use_pydantic = False

    def _new_file(self):
        self._cache.clear()
        self._imports.clear()
        self._aliases.clear()
        self._defs.clear()
        self._names.clear()
        self._use_pydantic = False

    def run(self):
        try:
            self._client.ensure_connected()
        except gel.EdgeDBError as e:
            print(f"Failed to connect to EdgeDB instance: {e}")
            sys.exit(61)
        with self._client:
            if self._search_dirs:
                for search_dir in self._search_dirs:
                    self._process_dir(search_dir)
            else:
                self._process_dir(self._project_dir)
        for target, suffix, is_async in SUFFIXES:
            if target in self._targets:
                self._async = is_async
                if self._single_mode_files:
                    self._generate_single_file(suffix)
                else:
                    self._generate_files(suffix)
                self._new_file()
        print_msg(f"{C.GREEN}{C.BOLD}Done.{C.ENDC}")

    def _process_dir(self, dir_: pathlib.Path):
        for file_or_dir in dir_.iterdir():
            if not file_or_dir.exists():
                continue
            if file_or_dir.is_dir():
                if file_or_dir.relative_to(self._project_dir) != pathlib.Path(
                    "dbschema/migrations"
                ):
                    self._process_dir(file_or_dir)
            elif file_or_dir.suffix.lower() == ".edgeql":
                self._process_file(file_or_dir)

    def _process_file(self, source: pathlib.Path):
        print_msg(f"{C.BOLD}Processing{C.ENDC} {C.BLUE}{source}{C.ENDC}")
        with source.open() as f:
            query = f.read()
        name = source.stem
        if self._single_mode_files:
            if name in self._method_names:
                print_error(f"Conflict method names: {name}")
                sys.exit(17)
            self._method_names.add(name)
        dr = self._client._describe_query(query, inject_type_names=True)
        self._describe_results.append((name, source, query, dr))

    def _generate_files(self, suffix: str):
        for name, source, query, dr in self._describe_results:
            target = source.parent / f"{name}{suffix}"
            print_msg(f"{C.BOLD}Generating{C.ENDC} {C.BLUE}{target}{C.ENDC}")
            self._new_file()
            content = self._generate(name, query, dr)
            buf = io.StringIO()
            self._write_comments(buf, [source])
            self._write_definitions(buf)
            buf.write(content)
            with target.open("w") as f:
                f.write(buf.getvalue())

    def _generate_single_file(self, suffix: str):
        print_msg(f"{C.BOLD}Generating single file output...{C.ENDC}")
        buf = io.StringIO()
        output = []
        sources = []
        for name, source, query, dr in sorted(self._describe_results):
            sources.append(source)
            output.append(self._generate(name, query, dr))
        self._write_comments(buf, sources)
        self._write_definitions(buf)
        for i, o in enumerate(output):
            buf.write(o)
            if i < len(output) - 1:
                print(file=buf)
                print(file=buf)

        for target in self._single_mode_files:
            if target:
                target = pathlib.Path(target).absolute()
            else:
                target = self._project_dir / f"{FILE_MODE_OUTPUT_FILE}{suffix}"
            print_msg(f"{C.BOLD}Writing{C.ENDC} {C.BLUE}{target}{C.ENDC}")
            with target.open("w") as f:
                f.write(buf.getvalue())

    def _write_comments(
        self, f: io.TextIOBase, src: typing.List[pathlib.Path]
    ):
        src_str = map(
            lambda p: repr(p.relative_to(self._project_dir).as_posix()), src
        )
        if len(src) > 1:
            print("# AUTOGENERATED FROM:", file=f)
            for s in src_str:
                print(f"#     {s}", file=f)
            print("# WITH:", file=f)
        else:
            print(f"# AUTOGENERATED FROM {next(src_str)} WITH:", file=f)
        cmd = []
        if sys.argv[0].endswith("__main__.py"):
            cmd.append(pathlib.Path(sys.executable).name)
            cmd.extend(["-m", "gel.codegen"])
        else:
            cmd.append(pathlib.Path(sys.argv[0]).name)
        cmd.extend(sys.argv[1:])
        cmd = " ".join(cmd)
        print(f"#     $ {cmd}", file=f)
        print(file=f)
        print(file=f)

    def _write_definitions(self, f: io.TextIOBase):
        print("from __future__ import annotations", file=f)
        for m in sorted(self._imports):
            print(f"import {m}", file=f)
        print(file=f)
        print(file=f)

        if self._aliases:
            for _, a in sorted(self._aliases.items()):
                print(a, file=f)
            print(file=f)
            print(file=f)

        if self._use_pydantic:
            print(PYDANTIC_MIXIN, file=f)
            print(file=f)
            print(file=f)

        for _, d in sorted(self._defs.items()):
            print(d, file=f)
            print(file=f)
            print(file=f)

    def _generate(
        self, name: str, query: str, dr: abstract.DescribeResult
    ) -> str:
        buf = io.StringIO()

        name_hint = f"{self._snake_to_camel(name)}Result"
        out_type = self._generate_code(dr.output_type, name_hint)
        if dr.output_cardinality.is_multi():
            if SYS_VERSION_INFO >= (3, 9):
                out_type = f"list[{out_type}]"
            else:
                self._imports.add("typing")
                out_type = f"typing.List[{out_type}]"
        elif dr.output_cardinality == gel.Cardinality.AT_MOST_ONE:
            if SYS_VERSION_INFO >= (3, 10):
                out_type = f"{out_type} | None"
            else:
                self._imports.add("typing")
                out_type = f"typing.Optional[{out_type}]"

        args = {}
        kw_only = False
        if isinstance(dr.input_type, describe.ObjectType):
            if "".join(dr.input_type.elements.keys()).isdecimal():
                for el_name, el in dr.input_type.elements.items():
                    args[int(el_name)] = self._generate_code_with_cardinality(
                        el.type, f"arg{el_name}", el.cardinality, is_input=True
                    )
                args = {f"arg{i}": v for i, v in sorted(args.items())}
            else:
                kw_only = True
                for el_name, el in dr.input_type.elements.items():
                    args[el_name] = self._generate_code_with_cardinality(
                        el.type,
                        el_name,
                        el.cardinality,
                        keyword_argument=True,
                        is_input=True
                    )

        if self._async:
            print(f"async def {name}(", file=buf)
        else:
            print(f"def {name}(", file=buf)
        self._imports.add("gel")
        if self._async:
            print(f"{INDENT}executor: gel.AsyncIOExecutor,", file=buf)
        else:
            print(f"{INDENT}executor: gel.Executor,", file=buf)
        if kw_only:
            print(f"{INDENT}*,", file=buf)
        for name, arg in args.items():
            print(f"{INDENT}{name}: {arg},", file=buf)
        print(f") -> {out_type}:", file=buf)
        if dr.output_cardinality.is_multi():
            method = "query"
            rt = "return "
        elif dr.output_cardinality == gel.Cardinality.NO_RESULT:
            method = "execute"
            rt = ""
        else:
            method = "query_single"
            rt = "return "

        if self._async:
            print(f"{INDENT}{rt}await executor.{method}(", file=buf)
        else:
            print(f"{INDENT}{rt}executor.{method}(", file=buf)
        print(f'{INDENT}{INDENT}"""\\', file=buf)
        print(
            textwrap.indent(
                textwrap.dedent(query).strip(), f"{INDENT}{INDENT}"
            )
            + "\\",
            file=buf,
        )
        print(f'{INDENT}{INDENT}""",', file=buf)
        for name in args:
            if kw_only:
                print(f"{INDENT}{INDENT}{name}={name},", file=buf)
            else:
                print(f"{INDENT}{INDENT}{name},", file=buf)
        print(f"{INDENT})", file=buf)
        return buf.getvalue()

    def _generate_code(
        self,
        type_: typing.Optional[describe.AnyType],
        name_hint: str,
        is_input: bool = False,
    ) -> str:
        if type_ is None:
            return "None"

        if (type_.desc_id, is_input) in self._cache:
            return self._cache[(type_.desc_id, is_input)]

        imports = INPUT_TYPE_IMPORTS if is_input else TYPE_IMPORTS
        mapping = INPUT_TYPE_MAPPING if is_input else TYPE_MAPPING

        if isinstance(type_, describe.BaseScalarType):
            if import_str := imports.get(type_.name):
                self._imports.add(import_str)
            rv = mapping[type_.name]

        elif isinstance(type_, describe.SequenceType):
            el_type = self._generate_code(
                type_.element_type, f"{name_hint}Item", is_input
            )
            if SYS_VERSION_INFO >= (3, 9):
                rv = f"list[{el_type}]"
            else:
                self._imports.add("typing")
                rv = f"typing.List[{el_type}]"

        elif isinstance(type_, describe.TupleType):
            elements = ", ".join(
                self._generate_code(el_type, f"{name_hint}Item", is_input)
                for el_type in type_.element_types
            )
            if SYS_VERSION_INFO >= (3, 9):
                rv = f"tuple[{elements}]"
            else:
                self._imports.add("typing")
                rv = f"typing.Tuple[{elements}]"

        elif isinstance(type_, describe.ScalarType):
            rv = self._find_name(type_.name or name_hint)
            base_type_name = type_.base_type.name
            if import_str := imports.get(base_type_name):
                self._imports.add(import_str)
            value = mapping[base_type_name]
            self._aliases[rv] = f"{rv} = {value}"

        elif isinstance(type_, describe.ObjectType):
            rv = self._find_name(name_hint)
            buf = io.StringIO()
            self._imports.add("dataclasses")
            print("@dataclasses.dataclass", file=buf)
            if self._skip_pydantic_validation:
                print(f"class {rv}(NoPydanticValidation):", file=buf)
                self._use_pydantic = True
            else:
                print(f"class {rv}:", file=buf)
            link_props = []
            for el_name, element in type_.elements.items():
                if element.is_implicit and el_name != "id":
                    continue
                name_hint = f"{rv}{self._snake_to_camel(el_name)}"
                el_code = self._generate_code_with_cardinality(
                    element.type, name_hint, element.cardinality
                )
                if element.kind == gel.ElementKind.LINK_PROPERTY:
                    link_props.append((el_name, el_code))
                else:
                    print(f"{INDENT}{el_name}: {el_code}", file=buf)
            if link_props:
                print(file=buf)
                self._imports.add("typing")
                typing_literal = "typing.Literal"
                for el_name, el_code in link_props:
                    print(f"{INDENT}@typing.overload", file=buf)
                    print(
                        f'{INDENT}def __getitem__'
                        f'(self, key: {typing_literal}["{el_name}"]) '
                        f'-> {el_code}:',
                        file=buf,
                    )
                    print(f"{INDENT}{INDENT}...", file=buf)
                    print(file=buf)
                print(
                    f"{INDENT}def __getitem__(self, key: str) -> typing.Any:",
                    file=buf,
                )
                print(
                    f"{INDENT}{INDENT}raise NotImplementedError", file=buf
                )

            self._defs[rv] = buf.getvalue().strip()

        elif isinstance(type_, describe.NamedTupleType):
            rv = self._find_name(name_hint)
            buf = io.StringIO()
            self._imports.add("typing")
            print(f"class {rv}(typing.NamedTuple):", file=buf)
            for el_name, el_type in type_.element_types.items():
                el_code = self._generate_code(
                    el_type, f"{rv}{self._snake_to_camel(el_name)}", is_input
                )
                print(f"{INDENT}{el_name}: {el_code}", file=buf)
            self._defs[rv] = buf.getvalue().strip()

        elif isinstance(type_, describe.EnumType):
            rv = self._find_name(type_.name or name_hint)
            buf = io.StringIO()
            self._imports.add("enum")
            print(f"class {rv}(enum.Enum):", file=buf)
            for member, member_id in self._to_unique_idents(type_.members):
                print(f'{INDENT}{member_id.upper()} = "{member}"', file=buf)
            self._defs[rv] = buf.getvalue().strip()

        elif isinstance(type_, describe.RangeType):
            value = self._generate_code(type_.value_type, name_hint, is_input)
            rv = f"gel.Range[{value}]"

        else:
            rv = "??"

        self._cache[(type_.desc_id, is_input)] = rv
        return rv

    def _generate_code_with_cardinality(
        self,
        type_: typing.Optional[describe.AnyType],
        name_hint: str,
        cardinality: gel.Cardinality,
        keyword_argument: bool = False,
        is_input: bool = False,
    ):
        rv = self._generate_code(type_, name_hint, is_input)
        if cardinality == gel.Cardinality.AT_MOST_ONE:
            if SYS_VERSION_INFO >= (3, 10):
                rv = f"{rv} | None"
            else:
                self._imports.add("typing")
                rv = f"typing.Optional[{rv}]"
            if keyword_argument:
                rv = f"{rv} = None"
        return rv

    def _find_name(self, name: str) -> str:
        default_prefix = f"{self._default_module}::"
        if name.startswith(default_prefix):
            name = name[len(default_prefix) :]
        mod, _, name = name.rpartition("::")
        name = self._snake_to_camel(name)
        name = mod.title() + name
        if name in self._names:
            for i in range(2, 100):
                new = f"{name}{i:02d}"
                if new not in self._names:
                    name = new
                    break
            else:
                print_error(f"Failed to find a unique name for: {name}")
                sys.exit(17)
        self._names.add(name)
        return name

    def _snake_to_camel(self, name: str) -> str:
        parts = name.split("_")
        if len(parts) > 1 or name.islower():
            return "".join(map(str.title, parts))
        else:
            return name

    def _to_unique_idents(
        self, names: typing.Iterable[typing.Tuple[str, str]]
    ) -> typing.Iterator[str]:
        dedup = set()
        for name in names:
            if name.isidentifier():
                name_id = name
                sep = name.endswith("_")
            else:
                sep = True
                result = []
                for i, c in enumerate(name):
                    if c.isdigit():
                        if i == 0:
                            result.append("e_")
                        result.append(c)
                        sep = False
                    elif c.isidentifier():
                        result.append(c)
                        sep = c == "_"
                    elif not sep:
                        result.append("_")
                        sep = True
                name_id = "".join(result)
            rv = name_id
            if not sep:
                name_id = name_id + "_"
            i = 1
            while rv in dedup:
                rv = f"{name_id}{i}"
                i += 1
            dedup.add(rv)
            yield name, rv
