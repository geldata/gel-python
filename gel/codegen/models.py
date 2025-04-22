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

from __future__ import annotations
from typing import (
    DefaultDict,
    Generator,
    Mapping,
    NamedTuple,
    Set,
    TypedDict,
    Union,
)

import argparse
import base64
import collections
import enum
import getpass
import graphlib
import functools
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

            usr_gen = SchemaGenerator(self._client, reflection.SchemaPart.USER)
            usr_gen.run()

        base.print_msg(f"{C.GREEN}{C.BOLD}Done.{C.ENDC}")


class SchemaGenerator:
    def __init__(self, client: gel.Client, schema_part: reflection.SchemaPart) -> None:
        self._client = client
        self._schema_part = schema_part
        self._basemodule = "models"
        self._outdir = pathlib.Path("models")
        self._modules: dict[reflection.SchemaPath, IntrospectedModule] = {}
        self._types: Mapping[uuid.UUID, reflection.AnyType] = {}
        self._named_tuples: dict[uuid.UUID, reflection.NamedTupleType] = {}
        self._wrapped_types: set[str] = set()

    def run(self) -> None:
        self.introspect_schema()

        self._generate_common_types()
        for modname, content in self._modules.items():
            if not content:
                # skip apparently empty modules
                continue

            as_pkg = self.has_submodules(modname)
            if (
                not as_pkg
                and self._schema_part is reflection.SchemaPart.STD
                and len(modname.parts) == 1
            ):
                as_pkg = True
            module = GeneratedSchemaModule(modname, self._types, as_pkg)
            module.process(content)

            with self.open_module(modname, as_pkg) as f:
                module.output(f)

    def has_submodules(self, mod: reflection.SchemaPath) -> bool:
        return any(m.parent.has_prefix(mod) for m in self._modules)

    def introspect_schema(self) -> None:
        for mod in reflection.fetch_modules(self._client, self._schema_part):
            self._modules[reflection.parse_name(mod)] = {
                "scalar_types": {},
                "object_types": {},
                "imports": {},
            }

        refl_types = reflection.fetch_types(self._client, self._schema_part)
        if self._schema_part is not reflection.SchemaPart.STD:
            std_types = reflection.fetch_types(
                self._client, reflection.SchemaPart.STD)
            self._types = collections.ChainMap(std_types, refl_types)
        else:
            self._types = refl_types

        for t in refl_types.values():
            if reflection.is_object_type(t):
                name = reflection.parse_name(t.name)
                self._modules[name.parent]["object_types"][name.name] = t
            elif reflection.is_scalar_type(t):
                name = reflection.parse_name(t.name)
                self._modules[name.parent]["scalar_types"][name.name] = t
            elif reflection.is_named_tuple_type(t):
                self._named_tuples[t.id] = t

    def get_comment_preamble(self) -> str:
        return COMMENT

    def _generate_common_types(self) -> None:
        mod = reflection.SchemaPath("__types__")
        if self._schema_part is reflection.SchemaPart.STD:
            mod = reflection.SchemaPath("std") / mod
        module = GeneratedGlobalModule(mod, self._types, False)
        module.process(self._named_tuples)

        with self.open_module(mod, False) as f:
            module.output(f)

    @contextmanager
    def open_module(
        self,
        mod: reflection.SchemaPath,
        as_pkg: bool,
    ) -> Generator[io.TextIOWrapper, None, None]:
        if as_pkg:
            # This is a prefix in another module, thus it is part of a nested
            # module structure.
            dirpath = mod
            filename = "__init__.py"
        else:
            # This is a leaf module, so we just need to create a corresponding
            # <mod>.py file.
            dirpath = mod.parent
            filename = f"{mod.name}.py"

        # Along the dirpath we need to ensure that all packages are created
        path = self._outdir
        self.init_dir(path)
        for el in dirpath.parts:
            path = path / el
            self.init_dir(path)

        with open(path / filename, "wt") as f:
            try:
                yield f
            finally:
                pass

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


class BaseGeneratedModule(base.GeneratedModule):
    def __init__(
        self,
        modname: reflection.SchemaPath,
        all_types: Mapping[uuid.UUID, reflection.AnyType],
        is_package: bool,
    ) -> None:
        super().__init__()
        self._modpath = modname
        self._types = all_types
        self._is_package = is_package

    def get_comment_preamble(self) -> str:
        return COMMENT

    def get_tuple_name(
        self,
        t: reflection.NamedTupleType,
    ) -> str:
        names = [elem.name.capitalize() for elem in t.tuple_elements]
        hash = base64.b64encode(t.id.bytes[:4], altchars=b"__").decode()
        return "".join(names) + "_Tuple_" + hash.rstrip("=")

    def get_type(
        self,
        type: reflection.AnyType,
        for_runtime: bool = False,
    ) -> str:
        base_type = base.TYPE_MAPPING.get(type.name)
        if base_type is not None:
            base_import = base.TYPE_IMPORTS.get(type.name)
            if base_import is not None:
                self.import_(base_import)
            return base_type

        if reflection.is_array_type(type):
            elem_type = self.get_type(self._types[type.array_element_id])
            return f"list[{elem_type}]"

        if reflection.is_pseudo_type(type):
            if type.name == "anyobject":
                self.import_("gel.models", gm="pydantic")
                return "gm.GelModel"
            elif type.name == "anytuple":
                return "tuple[typing.Any, ...]"
            else:
                raise AssertionError(f"unsupported pseudo-type: {type.name}")

        if reflection.is_named_tuple_type(type):
            mod = "__types__"
            if type.builtin:
                mod = f"std::{mod}"
            type_name = f"{mod}::{self.get_tuple_name(type)}"
            import_name = True
        else:
            type_name = type.name
            import_name = False

        type_path = reflection.parse_name(type_name)
        mod_path = self._modpath
        if type_path.parent == mod_path:
            return type_path.name
        else:
            common_parts = type_path.common_parts(mod_path)
            if common_parts:
                relative_depth = len(mod_path.parts) - len(common_parts)
                import_tail = type_path.parts[len(common_parts) :]
            else:
                relative_depth = len(mod_path.parts) + int(self._is_package)
                import_tail = type_path.parts

            module = "." * relative_depth + ".".join(import_tail[:-1])
            imported_name = import_tail[-1]
            do_import = self.import_ if for_runtime else self.import_type_names
            if import_name:
                do_import(module, imported_name)
                return imported_name
            else:
                alias = "_".join(type_path.parts[:-1])
                if all(c == "." for c in module):
                    do_import(f".{module}", **{alias: type_path.parts[-2]})
                else:
                    do_import(module, **{alias: "."})
                return f"{alias}.{imported_name}"


class GeneratedSchemaModule(BaseGeneratedModule):
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
                self.write(
                    f"class {type_name}(StrEnum):")
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

        graph: dict[uuid.UUID, Set[uuid.UUID]] = {}
        for t in object_types.values():
            graph[t.id] = set()
            t_name = reflection.parse_name(t.name)

            for base_ref in t.bases:
                base = self._types[base_ref.id]
                base_name = reflection.parse_name(base.name)
                if t_name.parent == base_name.parent:
                    graph[t.id].add(base.id)

        self.import_("typing")
        self.import_("typing_extensions", "TypeAliasType")

        self.import_("gel.models", gm="pydantic")
        self.import_("gel.models", gexpr="expr")

        for tid in graphlib.TopologicalSorter(graph).static_order():
            objtype = self._types[tid]
            assert reflection.is_object_type(objtype)
            self.write_object_type(objtype)

    def write_object_type(
        self,
        objtype: reflection.ObjectType,
    ) -> None:
        self.write()
        self.write()
        self.write("#")
        self.write(f"# type {objtype.name}")
        self.write("#")

        type_name = reflection.parse_name(objtype.name)
        name = type_name.name

        base_types = [
            self.get_type(self._types[base.id], for_runtime=True)
            for base in objtype.bases
        ]
        if not base_types:
            bases = ["gm.GelModel"]
        else:
            bases = base_types
        bases_string = ", ".join(bases)
        class_string = f"class {name}({bases_string}):"
        if len(class_string) > 79:
            bases_string = ",\n    ".join(bases)
            class_string = f"class {name}(\n    {bases_string}\n):"

        self.write()
        self.write(f"{name}_T = typing.TypeVar('{name}_T', bound='{name}')")
        self.write(class_string)
        with self.indented():
            if objtype.pointers:
                self.write()
                for ptr in objtype.pointers:
                    ptr_type = self.get_ptr_type(ptr)
                    self.write(f"{ptr.name}: {ptr_type}")
                    self.write(f'"""{objtype.name}.{ptr.name}"""')

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

            all_pointers = self._get_pointer_origins(objtype)
            if all_pointers:
                self.write()
                if base_types:
                    bases_string = ",\n    ".join(
                        f"{b}.__typeof__" for b in base_types)
                    self.write(f"class __typeof__(\n    {bases_string}\n):")
                else:
                    self.write(f"class __typeof__:")
                with self.indented():
                    for ptr, origin in all_pointers:
                        if origin is objtype:
                            ptr_t = self.get_ptr_type(ptr)
                        else:
                            origin_t = self.get_type(origin)
                            ptr_t = f"{origin_t}.__typeof__.{ptr.name}"

                        defn = f"TypeAliasType('{ptr.name}', '{ptr_t}')"
                        self.write(f"{ptr.name} = {defn}")

        self.write()

    def _get_pointer_origins(
        self,
        objtype: reflection.ObjectType,
    ) -> list[tuple[reflection.Pointer, reflection.ObjectType]]:
        pointers: dict[str, tuple[reflection.Pointer, reflection.ObjectType]] = {}
        for ancestor_ref in reversed(objtype.ancestors):
            ancestor = self._types[ancestor_ref.id]
            assert reflection.is_object_type(ancestor)
            for ptr in ancestor.pointers:
                pointers[ptr.name] = (ptr, ancestor)

        for ptr in objtype.pointers:
            pointers[ptr.name] = (ptr, objtype)

        return list(pointers.values())

    def get_ptr_type(self, prop: reflection.Pointer) -> str:
        return self.get_type(self._types[prop.target_id])


class GeneratedGlobalModule(BaseGeneratedModule):
    def process(self, types: Mapping[uuid.UUID, reflection.AnyType]) -> None:
        graph: DefaultDict[uuid.UUID, Set[uuid.UUID]] = defaultdict(set)

        @functools.singledispatch
        def type_dispatch(t: reflection.AnyType, ref_t: uuid.UUID) -> None:
            if reflection.is_named_tuple_type(t):
                graph[ref_t].add(t.id)
                for elem in t.tuple_elements:
                    type_dispatch(self._types[elem.type_id], t.id)
            elif reflection.is_tuple_type(t):
                for elem in t.tuple_elements:
                    type_dispatch(self._types[elem.type_id], ref_t)
            elif reflection.is_array_type(t):
                type_dispatch(self._types[t.array_element_id], ref_t)

        for t in types.values():
            if reflection.is_named_tuple_type(t):
                graph[t.id] = set()
                for elem in t.tuple_elements:
                    type_dispatch(self._types[elem.type_id], t.id)

        for tid in graphlib.TopologicalSorter(graph).static_order():
            t = self._types[tid]
            assert reflection.is_named_tuple_type(t)
            self.write_named_tuple_type(t)

    def write_named_tuple_type(
        self,
        t: reflection.NamedTupleType,
    ) -> None:
        self.import_("typing", "NamedTuple")

        self.write("#")
        self.write(f"# tuple type {t.name}")
        self.write("#")
        self.write(f"class {self.get_tuple_name(t)}(NamedTuple):")
        for elem in t.tuple_elements:
            elem_type = self.get_type(self._types[elem.type_id])
            self.write(f"    {elem.name}: {elem_type}")
        self.write()
