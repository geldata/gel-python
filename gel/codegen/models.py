#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2025-present MagicStack Inc. and the EdgeDB authors.
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
    Generator,
    Mapping,
    NamedTuple,
    TypedDict,
    Union,
)

import argparse
import enum
import getpass
import io
import os
import pathlib
import sys
import textwrap
import typing
import uuid

from collections import defaultdict
from contextlib import contextmanager

import gel
from gel import abstract
from gel import describe
from gel.con_utils import find_gel_project_dir
from gel.color import get_color

from gel.orm import introspection
from gel.orm.introspection import FilePrinter, get_mod_and_name
from gel._internal import _reflection as reflection

from . import base
from .base import C, GeneratedModule


COMMENT = """\
#
# Automatically generated from Gel schema.
#
# Do not edit directly as re-generating this file will overwrite any changes.
#\
"""


class IntrospectedModule(TypedDict):
    imports: dict[str, str]
    object_types: dict[str, reflection.ObjectType]
    scalar_types: dict[str, reflection.ScalarType]


class ModelsGenerator(base.Generator):
    def __init__(self, args: argparse.Namespace):
        super().__init__(args)

    def run(self) -> None:
        try:
            self._client.ensure_connected()
        except gel.EdgeDBError as e:
            print(f"could not connect to Gel instance: {e}")
            sys.exit(61)

        with self._client:
            std_gen = SchemaGenerator(self._client, reflection.SchemaPart.STD)
            std_gen.run()

        base.print_msg(f"{C.GREEN}{C.BOLD}Done.{C.ENDC}")


class SchemaGenerator:
    def __init__(self, client: gel.Client, schema_part: reflection.SchemaPart) -> None:
        self._client = client
        self._schema_part = schema_part
        self._basemodule = "models"
        self._outdir = pathlib.Path("models")
        self._modules: dict[str, IntrospectedModule] = {}
        self._types: dict[uuid.UUID, reflection.AnyType] = {}
        self._named_tuples: dict[uuid.UUID, reflection.NamedTupleType] = {}
        self._wrapped_types: set[str] = set()

    def run(self) -> None:
        self.get_schema()

        self._generate_common_types()
        for modname, content in self._modules.items():
            if not content:
                # skip apparently empty modules
                continue

            module = GeneratedSchemaModule(modname, self._types)
            module.process(content)

            with self.open_module(modname) as f:
                module.output(f)

    def get_comment_preamble(self) -> str:
        return COMMENT

    def _generate_common_types(self) -> None:
        module = GeneratedGlobalModule("__types__", self._named_tuples)
        module.process()

        with self.open_module("__types__") as f:
            module.output(f)

    @contextmanager
    def open_module(self, mod: str) -> Generator[io.TextIOWrapper, None, None]:
        if any(m.startswith(f"{mod}::") for m in self._modules):
            # This is a prefix in another module, thus it is part of a nested
            # module structure.
            dirpath = mod.split("::")
            filename = "__init__.py"
        else:
            # This is a leaf module, so we just need to create a corresponding
            # <mod>.py file.
            *dirpath, filename = mod.split("::")
            filename = f"{filename}.py"

        # Along the dirpath we need to ensure that all packages are created
        path = self._outdir
        self.init_dir(path)
        for el in dirpath:
            path = path / el
            self.init_dir(path)

        with open(path / filename, "wt") as f:
            try:
                yield f
            finally:
                pass

    def get_schema(self) -> None:
        for mod in reflection.fetch_modules(self._client, self._schema_part):
            self._modules[mod] = {
                "scalar_types": {},
                "object_types": {},
                "imports": {},
            }

        self._types = reflection.fetch_types(self._client, self._schema_part)

        for t in self._types.values():
            if t.kind == reflection.TypeKind.Object.value:
                mod, name = reflection.parse_name(t.name)
                self._modules[mod]["object_types"][name] = t
            elif t.kind == reflection.TypeKind.Scalar.value:
                mod, name = reflection.parse_name(t.name)
                self._modules[mod]["scalar_types"][name] = t
            elif t.kind == reflection.TypeKind.NamedTuple.value:
                self._named_tuples[t.id] = t

    def init_dir(self, dirpath: pathlib.Path) -> None:
        if not dirpath:
            # nothing to initialize
            return

        path = dirpath.resolve()

        # ensure `path` directory exists
        if not path.exists():
            path.mkdir(parents=True)
        elif not path.is_dir():
            raise NotADirectoryError(
                f"{path!r} exists, but it is not a directory"
            )

        # ensure `path` directory contains `__init__.py`
        (path / "__init__.py").touch()


class GeneratedSchemaModule(base.GeneratedModule):
    def __init__(
        self,
        modname: str,
        all_types: dict[uuid.UUID, reflection.AnyType],
    ) -> None:
        super().__init__()
        self._modname = modname
        self._modpath = pathlib.Path(*modname.split("::"))
        self._types = all_types

    def get_comment_preamble(self) -> str:
        return COMMENT

    def process(self, mod: IntrospectedModule) -> None:
        self.write_scalar_types(mod["scalar_types"])
        self.write_object_types(mod["object_types"])

    def write_description(
        self,
        type: Union[reflection.ScalarType, reflection.ObjectType],
    ) -> None:
        if not type.description:
            return

        desc = textwrap.wrap(
            textwrap.dedent(type.description).strip(),
            break_long_words=False,
        )
        self.write('"""')
        self.write("\n".join(desc))
        self.write('"""')

    def write_scalar_types(
        self,
        scalar_types: dict[str, reflection.ScalarType],
    ) -> None:
        for type_name, type in scalar_types.items():
            if type.enum_values:
                self.import_("gel.polyfills", "StrEnum")
                self.import_("gel.models", gexpr="expr")
                self.write(
                    f"class {type_name}(StrEnum, gexpr.Expression[StrEnum]):")
                with self.indented():
                    self.write_description(type)
                    for value in type.enum_values:
                        self.write(f"{value} = {value!r}")
                self.write_section_break()

    def write_object_types(
        self,
        object_types: dict[str, reflection.ObjectType],
    ) -> None:
        if not object_types:
            return

        self.import_(
            "typing",
            "TYPE_CHECKING",
            "Annotated",
            "Any",
            "Optional",
            "ParamSpec",
            "Type",
            "TypeAlias",
            "TypeVar",
            "Union",
        )

        self.import_("gel.models", gm="pydantic")
        self.import_("gel.models", gexpr="expr")

        for name, type in sorted(object_types.items(), key=lambda kv: kv[0]):
            self.write_object_type(name, type)

    def write_object_type(
        self,
        name: str,
        objtype: reflection.ObjectType,
    ) -> None:
        self.write()
        self.write()
        self.write("#")
        self.write(f"# type {objtype.name}")
        self.write("#")

        for prop in objtype.pointers:
            orig_prop_type = self.get_prop_type(prop)
            prop_type = orig_prop_type
            req_t = f"{name}__p__{prop.name}_req_t"
            opt_t = f"{name}__p__{prop.name}"
            self.write(f"class {req_t}(")
            self.write(f"    {prop_type},")
            self.write(f"    gexpr.Expression[{prop_type}],")
            self.write("):")
            self.write("    pass")
            self.write(f"{opt_t} = Optional[{req_t}]")
            self.write(f'"""{objtype.name}.{prop.name} ({orig_prop_type})"""')
            self.write(f"{name}__p__{prop.name}_selector = Union[")
            with self.indented():
                self.write(f"{req_t},")
                self.write(f"{opt_t},")
            self.write("]")

        if len(objtype.pointers):
            self.write(f"{name}__pointers = TypeVar(")
            with self.indented():
                self.write(f"'{name}__pointers',")
                self.write("bound=Union[")
                with self.indented():
                    for prop in objtype.pointers:
                        self.write(f"{name}__p__{prop.name}_selector,")
                self.write("]")
            self.write(")")

        self.write()
        self.write(f"{name}_T = TypeVar('{name}_T', bound='{name}')")
        self.write(f"class {name}(gm.GelModel):")
        with self.indented():
            self.write(
                f"__gel_metadata__ = "
                f"gm.GelMetadata(schema_name={objtype.name!r})",
            )

            self.write("class __gel_for__:")
            with self.indented():
                self.write("class Select(gm.GelModel):")
                with self.indented():
                    self.write(
                        f"__gel_metadata__ = "
                        f"gm.GelMetadata(schema_name={objtype.name!r})",
                    )

            if len(objtype.pointers) > 0:
                self.write()
                self.write("class __gel_fields__:")
                with self.indented():
                    for ptr in objtype.pointers:
                        orig_prop_type = self.get_prop_type(ptr)
                        self.write(
                            f"{ptr.name}: TypeAlias = {name}__p__{ptr.name}"
                        )

                self.write()
                for ptr in objtype.pointers:
                    orig_prop_type = self.get_prop_type(ptr)
                    self.write(f"{ptr.name}: {name}__p__{ptr.name}")
                    self.write(
                        f'"""{objtype.name}.{ptr.name} ({orig_prop_type})"""'
                    )

        self.write()

    def get_prop_type(self, prop: reflection.Pointer) -> str:
        type = self._types[prop.target_id]
        base_type = base.TYPE_MAPPING.get(type.name)
        if base_type is not None:
            base_import = base.TYPE_IMPORTS.get(type.name)
            if base_import is not None:
                self.import_(base_import)
            return base_type
        else:
            type_path = pathlib.Path(*type.name.split("::"))
            mod_path = self._modpath
            if type_path.parent == mod_path:
                return type_path.name
            else:
                common = pathlib.Path(
                    os.path.commonpath([type_path, mod_path])
                )
                if common:
                    relative_depth = len(mod_path.parts) - len(common.parts)
                    import_tail = type_path.parts[len(common.parts) :]
                else:
                    relative_depth = len(mod_path.parts)
                    import_tail = type_path.parts

                module = "." * relative_depth + ".".join(import_tail[:-1])
                alias = "_".join(type_path.parts)
                self.import_(module, **{alias: import_tail[-1]})
                return alias


class GeneratedGlobalModule(base.GeneratedModule):
    def __init__(
        self,
        modname: str,
        global_types: Mapping[uuid.UUID, reflection.AnyType],
    ) -> None:
        super().__init__()
        self._modname = modname
        self._modpath = pathlib.Path(*modname.split("::"))
        self._types = global_types

    def process(self) -> None:
        graph = defaultdict(set)
        for tid, t in self._types.items():
            if isinstance(t, reflection.NamedTupleType):
                pass
