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
    Callable,
    DefaultDict,
    Generator,
    Iterator,
    Mapping,
    NamedTuple,
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
from collections.abc import (
    Collection,
    Set,
)
from contextlib import contextmanager

import gel
from gel import abstract
from gel import describe
from gel._internal._reflection._types import is_scalar_type
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

            module = GeneratedSchemaModule(
                modname, self._types, self._modules, self._schema_part)
            module.process(content)
            module.write_files(self._outdir)

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
        module = GeneratedGlobalModule(
            mod, self._types, self._modules, self._schema_part)
        module.process(self._named_tuples)
        module.write_files(self._outdir)


class BaseGeneratedModule:
    def __init__(
        self,
        modname: reflection.SchemaPath,
        all_types: Mapping[uuid.UUID, reflection.AnyType],
        modules: Collection[reflection.SchemaPath],
        schema_part: reflection.SchemaPart,
    ) -> None:
        super().__init__()
        self._modpath = modname
        self._types = all_types
        self._modules = frozenset(modules)
        self._schema_part = schema_part
        self._is_package = self.mod_is_package(modname, schema_part)
        self._py_files = {
            "main":  base.GeneratedModule(COMMENT),
            "variants": base.GeneratedModule(COMMENT),
        }
        self._current_py_file = self._py_files["main"]
        self._type_import_cache: dict[tuple[str, bool, bool, bool], str] = {}

    def get_mod_schema_part(
        self,
        mod: reflection.SchemaPath,
    ) -> reflection.SchemaPart:
        if self._schema_part is reflection.SchemaPart.STD:
            return reflection.SchemaPart.STD
        elif mod not in self._modules:
            return reflection.SchemaPart.STD
        else:
            return reflection.SchemaPart.USER

    @classmethod
    def mod_is_package(
        cls,
        mod: reflection.SchemaPath,
        schema_part: reflection.SchemaPart,
    ) -> bool:
        return (
            bool(mod.parent.parts)
            or (
                schema_part is reflection.SchemaPart.STD
                and len(mod.parts) == 1
            )
        )

    @property
    def py_file(self) -> base.GeneratedModule:
        return self._current_py_file

    @property
    def py_files(self) -> Mapping[str, base.GeneratedModule]:
        return self._py_files

    @contextmanager
    def another_py_file(self, pyfile: str) -> Iterator[None]:
        prev_py_file = self._current_py_file

        try:
            self._current_py_file = self._py_files[pyfile]
            yield
        finally:
            self._current_py_file = prev_py_file

    @property
    def modpath(self) -> reflection.SchemaPath:
        return self._modpath

    @property
    def variants_modpath(self) -> reflection.SchemaPath:
        if self.is_package:
            modpath = self.modpath / "__variants__"
        else:
            modpath = self.modpath.parent / "__variants__" / self.modpath.name

        return modpath

    @property
    def is_package(self) -> bool:
        return self._is_package

    @contextmanager
    def _open_py_file(
        self,
        dir: pathlib.Path,
        modpath: reflection.SchemaPath,
        as_pkg: bool,
    ) -> Generator[io.TextIOWrapper, None, None]:
        if as_pkg:
            # This is a prefix in another module, thus it is part of a nested
            # module structure.
            dirpath = modpath
            filename = "__init__.py"
        else:
            # This is a leaf module, so we just need to create a corresponding
            # <mod>.py file.
            dirpath = modpath.parent
            filename = f"{modpath.name}.py"

        # Along the dirpath we need to ensure that all packages are created
        path = dir
        self._init_dir(path)
        for el in dirpath.parts:
            path = path / el
            self._init_dir(path)

        with open(path / filename, "wt") as f:
            try:
                yield f
            finally:
                pass

    def _init_dir(self, dirpath: pathlib.Path) -> None:
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

    def write_files(self, dir: pathlib.Path) -> None:
        main = self._py_files["main"]
        with self._open_py_file(dir, self.modpath, self.is_package) as f:
            main.output(f)

        variants = self._py_files["variants"]
        if not variants.has_content():
            return

        with self._open_py_file(dir, self.variants_modpath, self.is_package) as f:
            variants.output(f)

    def import_names(self, module: str, *names: str, **aliases: str) -> None:
        self.py_file.import_names(module, *names, **aliases)

    def import_type_names(
        self,
        module: str,
        *names: str,
        **aliases: str,
    ) -> None:
        self.py_file.import_type_names(module, *names, **aliases)

    @contextmanager
    def indented(self) -> Iterator[None]:
        with self.py_file.indented():
            yield

    def reset_indent(self) -> None:
        self.py_file.reset_indent()

    def write(self, text: str = "") -> None:
        self.py_file.write(text)

    def write_section_break(self, size: int = 2) -> None:
        self.py_file.write_section_break(size)

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
        *,
        for_runtime: bool = False,
        variants: bool = False,
        from_variants: bool | None = None,
    ) -> str:
        base_type = base.TYPE_MAPPING.get(type.name)
        if base_type is not None:
            base_import = base.TYPE_IMPORTS.get(type.name)
            if base_import is not None:
                self.import_names(base_import)
            return base_type

        if reflection.is_array_type(type):
            elem_type = self.get_type(
                self._types[type.array_element_id],
                for_runtime=for_runtime,
                variants=variants,
                from_variants=from_variants,
            )
            return f"list[{elem_type}]"

        if reflection.is_pseudo_type(type):
            if type.name == "anyobject":
                self.import_names("gel.models", gm="pydantic")
                return "gm.GelModel"
            elif type.name == "anytuple":
                return "tuple[typing.Any, ...]"
            else:
                raise AssertionError(f"unsupported pseudo-type: {type.name}")

        if from_variants is None:
            from_variants = variants

        if reflection.is_named_tuple_type(type):
            mod = "__types__"
            if type.builtin:
                mod = f"std::{mod}"
            type_name = f"{mod}::{self.get_tuple_name(type)}"
            import_name = True
        else:
            type_name = type.name
            import_name = False

        if reflection.is_scalar_type(type):
            variants = True
        elif reflection.is_named_tuple_type(type):
            variants = False

        cache_key = (type_name, variants, from_variants, import_name)
        result = self._type_import_cache.get(cache_key)
        if result is not None and "NameExpr" in result:
            return result

        mod_path = self.variants_modpath if from_variants else self.modpath
        type_path = reflection.parse_name(type_name)
        type_mod = type_path.parent
        import_alias = None
        if variants:
            if type_mod == mod_path:
                import_alias = "__"
            type_mod_is_pkg = self.mod_is_package(
                type_mod,
                self.get_mod_schema_part(type_mod),
            )
            if type_mod_is_pkg:
                type_path = type_mod / "__variants__" / type_path.name
            else:
                type_path = (
                    type_mod.parent / "__variants__" / type_mod.name
                    / type_path.name
                )

        if type_path.parent == mod_path:
            result = type_path.name
        else:
            common_parts = type_path.common_parts(mod_path)
            if common_parts:
                relative_depth = len(mod_path.parts) - len(common_parts)
                import_tail = type_path.parts[len(common_parts) :]
            else:
                relative_depth = len(mod_path.parts)
                import_tail = type_path.parts

            if self._is_package:
                relative_depth += 1

            if str(mod_path) == "std" and "__variants__" in str(type_path):
                print(type_path, common_parts, relative_depth, import_tail)

            module = "." * relative_depth + ".".join(import_tail[:-1])
            imported_name = import_tail[-1]
            if for_runtime:
                do_import = self.import_names
            else:
                do_import = self.import_type_names
            if import_name:
                do_import(module, imported_name)
                result = imported_name
            else:
                if import_alias is not None:
                    alias = import_alias
                else:
                    alias = "_".join(type_path.parts[:-1])
                if all(c == "." for c in module):
                    do_import(f".{module}", **{alias: type_path.parts[-2]})
                else:
                    do_import(module, **{alias: "."})
                result = f"{alias}.{imported_name}"

        self._type_import_cache[cache_key] = result
        return result

    def format_list(self, tpl: str, values: list[str]) -> str:
        list_string = ", ".join(values)
        output_string = tpl.format(list=list_string)

        if len(output_string) > 79:
            list_string = ",\n    ".join(values)
            list_string = f"\n    {list_string}\n"
            output_string = tpl.format(list=list_string)

        return output_string


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
        with self.another_py_file("variants"):
            for type_name, type in scalar_types.items():
                if type.enum_values:
                    self.import_names("gel.polyfills", "StrEnum")
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

        graph: dict[uuid.UUID, set[uuid.UUID]] = {}
        for t in object_types.values():
            graph[t.id] = set()
            t_name = reflection.parse_name(t.name)

            for base_ref in t.bases:
                base = self._types[base_ref.id]
                base_name = reflection.parse_name(base.name)
                if t_name.parent == base_name.parent:
                    graph[t.id].add(base.id)

        objtypes = []
        for tid in graphlib.TopologicalSorter(graph).static_order():
            objtype = self._types[tid]
            assert reflection.is_object_type(objtype)
            objtypes.append(objtype)

        self.import_names("typing")

        self.import_names("gel.models", gm="pydantic")
        self.import_names("gel.models", gexpr="expr")

        for objtype in objtypes:
            self.write_object_type(objtype)

        with self.another_py_file("variants"):
            self.import_names("typing")
            self.import_names("typing_extensions", "TypeAliasType")

            self.import_names("gel.models", gm="pydantic")
            self.import_names("gel.models", gexpr="expr")

            for objtype in objtypes:
                self.write_object_type_variants(objtype)

    def _format_class_line(
        self,
        class_name: str,
        base_types: list[str],
        *,
        fixed_bases: list[str] | None = None,
        transform: None | Callable[[str], str] = None,
    ) -> str:
        if transform is not None:
            bases = []
            for b in base_types:
                mod, _, name = b.rpartition(".")
                name = transform(name)
                bases.append(f"{mod}.{name}" if mod else name)
        else:
            bases = base_types

        all_bases = fixed_bases + bases if fixed_bases is not None else bases
        return self.format_list(f"class {class_name}({{list}}):", all_bases)

    def _write_class_line(
        self,
        class_name: str,
        base_types: list[str],
        *,
        fixed_bases: list[str] | None = None,
        transform: None | Callable[[str], str] = None,
    ) -> None:
        class_line = self._format_class_line(
            class_name,
            base_types,
            fixed_bases=fixed_bases,
            transform=transform,
        )
        self.write(class_line)

    def write_object_type_variants(
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

        def _mangle_typeof(name: str) -> str:
            return f"__{name}_typeof__"

        base_types = [
            self.get_type(self._types[base.id], for_runtime=True, variants=True)
            for base in objtype.bases
        ]
        typeof_class = _mangle_typeof(name)
        self._write_class_line(
            typeof_class,
            base_types,
            transform=_mangle_typeof,
        )
        all_pointers = self._get_pointer_origins(objtype)
        with self.indented():
            if not all_pointers:
                self.write("pass")
            else:
                for ptr, origin in all_pointers:
                    if origin is objtype:
                        ptr_t = self.get_ptr_type(ptr, variants=True)
                    else:
                        origin_t = self.get_type(origin, variants=True)
                        ptr_t = f"{origin_t}.__typeof__.{ptr.name}"

                    defn = f"TypeAliasType('{ptr.name}', '{ptr_t}')"
                    self.write(f"{ptr.name} = {defn}")

        self.write()
        self.write()
        self._write_class_line(
            name,
            base_types,
        )
        with self.indented():
            self._write_class_line(
                "__typeof__",
                base_types,
                fixed_bases=[typeof_class],
                transform=lambda s: f"{s}.__typeof__",
            )
            with self.indented():
                self.write("pass")

            self._write_class_line(
                "__variants__",
                base_types,
                transform=lambda s: f"{s}.__variants__",
            )
            with self.indented():
                self._write_class_line(
                    "Empty",
                    base_types,
                    transform=lambda s: f"{s}.__variants__.Empty",
                )
                with self.indented():
                    self.write("pass")

        self.write()

    def write_object_type(
        self,
        objtype: reflection.ObjectType,
    ) -> None:
        self.write()
        self.write("#")
        self.write(f"# type {objtype.name}")
        self.write("#")

        type_name = reflection.parse_name(objtype.name)
        name = type_name.name

        base = self.get_type(
            objtype, for_runtime=True, variants=True, from_variants=False)
        base_types = [base]
        base_types.extend([
            self.get_type(self._types[base.id], for_runtime=True)
            for base in objtype.bases
        ])
        class_string = self.format_list(f"class {name}({{list}}):", base_types)
        self.write(f"{name}_T = typing.TypeVar('{name}_T', bound='{name}')")
        self.write(class_string)
        with self.indented():
            if objtype.pointers:
                for ptr in objtype.pointers:
                    ptr_type = self.get_ptr_type(
                        ptr, variants=True, from_variants=False)
                    self.write(f"{ptr.name}: {ptr_type}")
                    self.write(f'"""{objtype.name}.{ptr.name}"""')
                    self.write()
            else:
                self.write("pass")
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

    def get_ptr_type(
        self,
        prop: reflection.Pointer,
        variants: bool = False,
        from_variants: bool | None = None,
    ) -> str:
        ptr_type = self.get_type(
            self._types[prop.target_id],
            variants=variants,
            from_variants=from_variants,
        )
        if prop.card in {
            reflection.Cardinality.AtLeastOne._value_,
            reflection.Cardinality.Many._value_,
        }:
            return f"list[{ptr_type}]"
        else:
            return ptr_type


class GeneratedGlobalModule(BaseGeneratedModule):
    def process(self, types: Mapping[uuid.UUID, reflection.AnyType]) -> None:
        graph: DefaultDict[uuid.UUID, set[uuid.UUID]] = defaultdict(set)

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
        self.import_names("typing", "NamedTuple")

        self.write("#")
        self.write(f"# tuple type {t.name}")
        self.write("#")
        self.write(f"class {self.get_tuple_name(t)}(NamedTuple):")
        for elem in t.tuple_elements:
            elem_type = self.get_type(self._types[elem.type_id])
            self.write(f"    {elem.name}: {elem_type}")
        self.write()
