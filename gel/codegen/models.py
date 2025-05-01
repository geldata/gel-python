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
from re import L
from typing import (
    Callable,
    DefaultDict,
    Generator,
    Iterable,
    Iterator,
    Literal,
    Mapping,
    NamedTuple,
    Sequence,
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

from pydantic.main import _private_setattr_handler

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
from .base import C, GeneratedModule, ImportTime, CodeSection


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
    def __init__(
        self,
        client: abstract.ReadOnlyExecutor,
        schema_part: reflection.SchemaPart,
        outdir: pathlib.Path = pathlib.Path("models"),
    ) -> None:
        self._client = client
        self._schema_part = schema_part
        self._basemodule = "models"
        self._outdir = outdir
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
                modname, self._types, self._modules, self._schema_part
            )
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
                self._client, reflection.SchemaPart.STD
            )
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
            mod, self._types, self._modules, self._schema_part
        )
        module.process(self._named_tuples)
        module.write_files(self._outdir)


class ModuleAspect(enum.Enum):
    MAIN = enum.auto()
    VARIANTS = enum.auto()
    LATE = enum.auto()


class Import(NamedTuple):
    module: str
    name: str
    alias: str | None


@functools.cache
def get_modpath(
    modpath: reflection.SchemaPath,
    aspect: ModuleAspect,
    mod_is_package: bool,
) -> reflection.SchemaPath:
    if aspect is ModuleAspect.MAIN:
        pass
    elif aspect is ModuleAspect.VARIANTS:
        modpath = reflection.SchemaPath("__variants__") / modpath
    elif aspect is ModuleAspect.LATE:
        modpath = reflection.SchemaPath("__variants__") / "__late__" / modpath

    return modpath


def _map_name(
    transform: Callable[[str], str],
    classnames: Iterable[str],
) -> list[str]:
    result = []
    for classname in classnames:
        mod, _, name = classname.rpartition(".")
        name = transform(name)
        result.append(f"{mod}.{name}" if mod else name)
    return result


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
        for t in all_types.values():
            if t.name == "schema::ObjectType":
                assert reflection.is_object_type(t)
                self._schema_object_type = t
                break
        else:
            raise RuntimeError(
                "schema::ObjectType type not found in schema reflection"
            )
        self._modules = frozenset(modules)
        self._schema_part = schema_part
        self._is_package = self.mod_is_package(modname, schema_part)
        self._py_files = {
            ModuleAspect.MAIN: base.GeneratedModule(COMMENT),
            ModuleAspect.VARIANTS: base.GeneratedModule(COMMENT),
            ModuleAspect.LATE: base.GeneratedModule(COMMENT),
        }
        self._current_py_file = self._py_files[ModuleAspect.MAIN]
        self._current_aspect = ModuleAspect.MAIN
        self._type_import_cache: dict[
            tuple[
                str, ModuleAspect, ModuleAspect, bool, str | None, ImportTime
            ],
            str,
        ] = {}

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
        return bool(mod.parent.parts) or (
            schema_part is reflection.SchemaPart.STD and len(mod.parts) == 1
        )

    @property
    def py_file(self) -> base.GeneratedModule:
        return self._current_py_file

    @property
    def py_files(self) -> Mapping[ModuleAspect, base.GeneratedModule]:
        return self._py_files

    @property
    def current_aspect(self) -> ModuleAspect:
        return self._current_aspect

    @contextmanager
    def aspect(self, aspect: ModuleAspect) -> Iterator[None]:
        prev_aspect = self._current_aspect

        try:
            self._current_py_file = self._py_files[aspect]
            self._current_aspect = aspect
            yield
        finally:
            self._current_py_file = self._py_files[prev_aspect]
            self._current_aspect = prev_aspect

    @property
    def canonical_modpath(self) -> reflection.SchemaPath:
        return self._modpath

    @property
    def current_modpath(self) -> reflection.SchemaPath:
        return self.modpath(self._current_aspect)

    def modpath(self, aspect: ModuleAspect) -> reflection.SchemaPath:
        return get_modpath(self._modpath, aspect, self.is_package)

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
        for aspect, py_file in self.py_files.items():
            if not py_file.has_content():
                continue

            with self._open_py_file(
                dir,
                self.modpath(aspect),
                self.is_package,
            ) as f:
                py_file.output(f)

    def import_name(
        self,
        module: str,
        name: str,
        *,
        alias: str | None = None,
        import_time: ImportTime = ImportTime.runtime,
    ) -> str:
        return self.py_file.import_name(
            module,
            name,
            alias=alias,
            import_time=import_time,
        )

    def current_indentation(self) -> str:
        return self.py_file.current_indentation()

    @contextmanager
    def indented(self) -> Iterator[None]:
        with self.py_file.indented():
            yield

    @contextmanager
    def type_checking(self) -> Iterator[None]:
        with self.py_file.type_checking():
            yield

    @contextmanager
    def not_type_checking(self) -> Iterator[None]:
        with self.py_file.not_type_checking():
            yield

    @contextmanager
    def code_section(self, section: CodeSection) -> Iterator[None]:
        with self.py_file.code_section(section):
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

    def _resolve_rel_import(
        self,
        imp_path: reflection.SchemaPath,
        aspect: ModuleAspect,
        import_name: bool = False,
    ) -> Import | None:
        imp_mod_canon = imp_mod = imp_path.parent
        imp_name = imp_path.name
        cur_mod = self.current_modpath
        if aspect is not ModuleAspect.MAIN:
            imp_schema_part = self.get_mod_schema_part(imp_mod)
            imp_mod_is_pkg = self.mod_is_package(imp_mod, imp_schema_part)
            imp_mod = get_modpath(imp_mod, aspect, imp_mod_is_pkg)

        if imp_mod == cur_mod and aspect is self.current_aspect:
            # It's this module, no need to import
            return None
        else:
            common_parts = imp_mod.common_parts(cur_mod)
            if common_parts:
                relative_depth = len(cur_mod.parts) - len(common_parts)
                import_tail = imp_mod.parts[len(common_parts) :]
            else:
                relative_depth = len(cur_mod.parts)
                import_tail = imp_mod.parts

            if self._is_package:
                relative_depth += 1

            py_mod = "." * relative_depth + ".".join(import_tail)
            if import_name:
                result = Import(
                    module=py_mod,
                    name=imp_name,
                    alias=None,
                )
            else:
                if (
                    imp_mod_canon == self.canonical_modpath
                    and self.current_aspect is ModuleAspect.MAIN
                ):
                    alias = "__"
                else:
                    alias = "_".join(imp_path.parts[:-1])
                if all(c == "." for c in py_mod):
                    result = Import(
                        module=f".{py_mod}",
                        name=imp_path.parts[-2],
                        alias=alias,
                    )
                else:
                    result = Import(
                        module=py_mod,
                        name=".",
                        alias=alias,
                    )

        return result

    def get_type(
        self,
        type: reflection.AnyType,
        *,
        import_time: ImportTime = ImportTime.runtime,
        aspect: ModuleAspect = ModuleAspect.MAIN,
        rename_as: str | None = None,
    ) -> str:
        base_type = base.TYPE_MAPPING.get(type.name)
        if base_type is not None:
            if isinstance(base_type, str):
                if self.py_file.has_global(base_type):
                    # Schema shadows a builtin, disambiguate.
                    base_type = ("builtins", base_type)
                else:
                    # Unshadowed Python builtin.
                    return base_type

            return self.import_name(
                *base_type,
                import_time=import_time,
            )

        if reflection.is_array_type(type):
            elem_type = self.get_type(
                self._types[type.array_element_id],
                import_time=import_time,
                aspect=aspect,
            )
            return f"list[{elem_type}]"

        if reflection.is_pseudo_type(type):
            if type.name == "anyobject":
                return self.import_name("gel.models.pydantic", "GelModel")
            elif type.name == "anytuple":
                any = self.import_name("typing", "Any")
                return f"tuple[{any}, ...]"
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

        if reflection.is_scalar_type(type):
            aspect = ModuleAspect.VARIANTS
        elif reflection.is_named_tuple_type(type):
            aspect = ModuleAspect.MAIN

        cur_aspect = self.current_aspect
        cache_key = (
            type_name,
            aspect,
            cur_aspect,
            import_name,
            rename_as,
            import_time,
        )
        result = self._type_import_cache.get(cache_key)
        if result is not None:
            return result

        type_path = reflection.parse_name(type_name)
        type_name = type_path.name
        if rename_as is not None:
            imported_name = rename_as
            imp_path = type_path.parent / imported_name
        else:
            imported_name = type_name
            imp_path = type_path

        rel_import = self._resolve_rel_import(imp_path, aspect, import_name)
        if rel_import is None:
            result = imported_name
        else:
            result = self.import_name(
                rel_import.module,
                rel_import.name,
                alias=rel_import.alias,
                import_time=import_time,
            )

            if rel_import.alias is not None:
                result = f"{result}.{imported_name}"

        self._type_import_cache[cache_key] = result
        return result

    def format_list(self, tpl: str, values: list[str]) -> str:
        list_string = ", ".join(values)
        output_string = tpl.format(list=list_string)

        if len(output_string) + len(self.current_indentation()) > 79:
            list_string = ",\n    ".join(values)
            list_string = f"\n    {list_string},\n"
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
        with self.aspect(ModuleAspect.VARIANTS):
            for type_name, type in scalar_types.items():
                if type.enum_values:
                    strenum = self.import_name("gel.polyfills", "StrEnum")
                    self.write(f"class {type_name}({strenum}):")
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
            type_name = reflection.parse_name(objtype.name)
            self.py_file.add_global(type_name.name)

        for objtype in objtypes:
            self.write_object_type(objtype)

        with self.aspect(ModuleAspect.LATE):
            for objtype in objtypes:
                self.write_object_type_link_variants(objtype, local=False)

        with self.aspect(ModuleAspect.VARIANTS):
            for objtype in objtypes:
                type_name = reflection.parse_name(objtype.name)
                self.py_file.add_global(type_name.name)

            for objtype in objtypes:
                self.write_object_type_variants(objtype)

            for objtype in objtypes:
                self.write_object_type_link_variants(objtype, local=True)

            if self.py_files[ModuleAspect.LATE].has_content():
                rel_import = self._resolve_rel_import(
                    self.canonical_modpath / "*",
                    ModuleAspect.LATE,
                    import_name=True,
                )
                assert rel_import is not None
                self.import_name(
                    rel_import.module,
                    rel_import.name,
                    alias=rel_import.alias,
                    import_time=ImportTime.late_runtime,
                )

            with self.code_section(CodeSection.after_late_import):
                with self._func_def("__gel_type_reflection_register__"):
                    ot_imp = self._resolve_rel_import(
                        reflection.parse_name(self._schema_object_type.name),
                        aspect=ModuleAspect.MAIN,
                    )
                    assert ot_imp is not None
                    ot_ref, import_code = self.py_file.render_name_import(
                        ot_imp.module,
                        ot_imp.name,
                        alias=ot_imp.alias,
                    )
                    self.write(import_code)
                    for objtype in objtypes:
                        type_name = reflection.parse_name(objtype.name)
                        self.write_object_type_reflection(
                            objtype,
                            type_name.name,
                            f"{ot_ref}.ObjectType",
                        )

                late_reg = ["schema", "sys"]
                this_modpath = str(self.canonical_modpath)
                if this_modpath == "std":
                    for mod in late_reg:
                        rel_import = self._resolve_rel_import(
                            reflection.SchemaPath(mod),
                            ModuleAspect.VARIANTS,
                            import_name=True,
                        )
                        assert rel_import is not None
                        mod_v = self.import_name(
                            rel_import.module,
                            rel_import.name,
                            alias=rel_import.alias,
                            import_time=ImportTime.late_runtime,
                        )
                        self.write(
                            f"{mod_v}.__gel_type_reflection_register__()"
                        )

                if this_modpath not in late_reg:
                    self.write("__gel_type_reflection_register__()")

    def _format_class_line(
        self,
        class_name: str,
        bases: Iterable[str],
        *,
        class_kwargs: dict[str, str] | None = None,
    ) -> str:
        args = list(bases)
        if class_kwargs:
            args.extend(f"{k}={v}" for k, v in class_kwargs.items())
        if args:
            return self.format_list(f"class {class_name}({{list}}):", args)
        else:
            return f"class {class_name}:"

    @contextmanager
    def _class_def(
        self,
        class_name: str,
        base_types: Iterable[str],
        *,
        class_kwargs: dict[str, str] | None = None,
    ) -> Iterator[None]:
        class_line = self._format_class_line(
            class_name,
            base_types,
            class_kwargs=class_kwargs,
        )
        self.write(class_line)
        with self.indented():
            yield

    @contextmanager
    def _func_def(
        self,
        func_name: str,
        params: Iterable[str] = (),
        return_type: str = "None",
        *,
        kind: Literal["classmethod", "method", "property", "func"] = "func",
    ) -> Iterator[None]:
        if kind == "classmethod":
            self.write("@classmethod")
        elif kind == "property":
            self.write("@property")
        params = list(params)
        if kind == "classmethod":
            params = ["cls"] + params
        elif kind == "method" or kind == "property":
            params = ["self"] + params
        def_line = self.format_list(
            f"def {func_name}({{list}}) -> {return_type}:",
            params,
        )
        self.write(def_line)
        with self.indented():
            yield

    @contextmanager
    def _classmethod_def(
        self,
        func_name: str,
        params: Iterable[str] = (),
        return_type: str = "None",
        *,
        kind: Literal["classmethod", "method", "property", "func"] = "func",
    ) -> Iterator[None]:
        with self._func_def(
            func_name,
            params,
            return_type,
            kind="classmethod",
        ):
            yield

    @contextmanager
    def _property_def(
        self,
        func_name: str,
        params: Iterable[str] = (),
        return_type: str = "None",
        *,
        kind: Literal["classmethod", "method", "property", "func"] = "func",
    ) -> Iterator[None]:
        with self._func_def(
            func_name,
            params,
            return_type,
            kind="property",
        ):
            yield

    @contextmanager
    def _method_def(
        self,
        func_name: str,
        params: Iterable[str] = (),
        return_type: str = "None",
        *,
        kind: Literal["classmethod", "method", "property", "func"] = "func",
    ) -> Iterator[None]:
        with self._func_def(
            func_name,
            params,
            return_type,
            kind="method",
        ):
            yield

    def write_object_type_reflection(
        self, objtype: reflection.ObjectType, classname: str, refl_t: str
    ) -> None:
        if objtype.name == "std::FreeObject":
            return
        uuid_t = self.import_name("uuid", "UUID")
        self.write(f"type({classname}).__gel_type_reflection_register__(")
        with self.indented():
            self.write(f"{refl_t}(")
            with self.indented():
                self.write(f"id={uuid_t}({str(objtype.id)!r}),")
                self.write(f"name={objtype.name!r},")
                self.write(f"builtin={objtype.builtin!r},")
                self.write(f"internal={objtype.internal!r},")
                self.write(f"abstract={objtype.abstract!r},")
                self.write(f"final={objtype.final!r},")
                self.write(f"compound_type={objtype.compound_type!r},")
            self.write("),")
        self.write(")")

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
            self.get_type(
                self._types[base.id],
                aspect=ModuleAspect.VARIANTS,
            )
            for base in objtype.bases
        ]
        typeof_class = _mangle_typeof(name)
        if base_types:
            typeof_bases = _map_name(_mangle_typeof, base_types)
        else:
            gmm = self.import_name("gel.models.pydantic", "GelModelMetadata")
            typeof_bases = [gmm]
        pointers = objtype.pointers
        otr = self.import_name("gel.models.pydantic", "ObjectTypeReflection")
        uuid = self.import_name("uuid", "UUID")
        with self._class_def(typeof_class, typeof_bases):
            self.write(f"__gel_type_reflection__ = {otr}(")
            with self.indented():
                self.write(f"id={uuid}({str(objtype.id)!r}),")
                self.write(f"name={objtype.name!r},")
            self.write(")")
            self.write()

            with self._class_def(
                "__typeof__",
                _map_name(
                    lambda s: f"{_mangle_typeof(s)}.__typeof__",
                    base_types,
                ),
            ):
                if not pointers:
                    self.write("pass")
                else:
                    type_alias = self.import_name(
                        "typing_extensions", "TypeAliasType"
                    )
                    for ptr in pointers:
                        ptr_t = self.get_ptr_type(objtype, ptr)
                        defn = f"{type_alias}('{ptr.name}', '{ptr_t}')"
                        self.write(f"{ptr.name} = {defn}")

        self.write()
        self.write()
        class_kwargs = {}
        if objtype.name == "std::BaseObject":
            gel_meta = self.import_name("gel.models.pydantic", "GelModelMeta")
            uuid = self.import_name("uuid", "UUID")
            classvar = self.import_name("typing", "ClassVar")
            refl_t = self.get_type(
                self._schema_object_type,
                import_time=base.ImportTime.typecheck,
                aspect=ModuleAspect.MAIN,
            )
            with self._class_def("_BaseObjectMeta", [gel_meta]):
                assert refl_t is not None
                registry = "__gel_type_reflection_registry__"
                self.write(
                    f"{registry}: {classvar}[dict[{uuid}, {refl_t}]] = {{}}"
                )
                self.write()

                with self._classmethod_def(
                    "__gel_type_reflection_register__",
                    [f"rtype: {refl_t}"],
                    "None",
                ):
                    self.write(f"cls.{registry}[rtype.id] = rtype")

                self.write()
                with self._classmethod_def(
                    "get_type_reflection",
                    [f"tid: {uuid}"],
                    refl_t,
                ):
                    self.write(f"return cls.{registry}[tid]")

                self.write()

            class_kwargs["metaclass"] = "_BaseObjectMeta"
            self.write()
            self.write()

        if not base_types:
            gel_model = self.import_name("gel.models.pydantic", "GelModel")
            vbase_types = [gel_model]
        else:
            vbase_types = base_types
        with self._class_def(
            name,
            [typeof_class] + vbase_types,
            class_kwargs=class_kwargs,
        ):
            self._write_base_object_type_body(objtype, typeof_class)
            self.write()

            with self._class_def(
                "__variants__",
                _map_name(lambda s: f"{s}.__variants__", base_types),
            ):
                base_bases = [typeof_class]
                if base_types:
                    base_bases.extend(
                        _map_name(
                            lambda s: f"{s}.__variants__.Empty",
                            base_types,
                        )
                    )
                else:
                    gel_model = self.import_name(
                        "gel.models.pydantic", "GelModel"
                    )
                    base_bases.append(gel_model)
                with self._class_def(
                    "Empty",
                    base_bases,
                    class_kwargs=class_kwargs,
                ):
                    self._write_base_object_type_body(objtype, typeof_class)

                self.write()
                typevar = self.import_name("typing", "TypeVar")
                self.write(f'Any = {typevar}("Any", bound="{name} | Empty")')

            proplinks = self._get_links_with_props(objtype)
            if base_types:
                links_bases = _map_name(lambda s: f"{s}.__links__", base_types)
            else:
                lns = self.import_name(
                    "gel.models.pydantic", "LinkClassNamespace")
                links_bases = [lns]

            with self._class_def("__links__", links_bases):
                if proplinks:
                    with self.type_checking():
                        self.write_object_type_link_variants(
                            objtype,
                            target_aspect=ModuleAspect.MAIN,
                            is_forward_decl=True,
                            local=None,
                        )
                else:
                    self.write("pass")

        self.write()
        with self.not_type_checking():
            self.write(f"{name}.__variants__.Empty = {name}")

        self.write()

    def _write_base_object_type_body(
        self,
        objtype: reflection.ObjectType,
        typeof_class: str,
    ) -> None:
        if objtype.name == "std::BaseObject":
            priv_attr = self.import_name("gel.models.pydantic", "PrivateAttr")
            comp_f = self.import_name("gel.models.pydantic", "computed_field")
            for ptr in objtype.pointers:
                if ptr.name == "__type__":
                    ptr_type = self.get_ptr_type(
                        objtype,
                        ptr,
                        aspect=ModuleAspect.MAIN,
                    )
                    with self._property_def(ptr.name, [], ptr_type):
                        self.write("cls = type(self)")
                        self.write("tid = cls.__gel_type_reflection__.id")
                        self.write(
                            f"return type(cls).get_type_reflection(tid)"
                        )
                elif ptr.name == "id":
                    ptr_type = self.get_ptr_type(objtype, ptr)
                    self.write(f"_p__{ptr.name}: {ptr_type} = {priv_attr}()")
                    self.write(f"@{comp_f}  # type: ignore[prop-decorator]")
                    with self._property_def(ptr.name, [], ptr_type):
                        self.write(f"return self._p__{ptr.name}")
            self.write()

        def _filter(
            v: tuple[reflection.Pointer, reflection.ObjectType],
        ) -> bool:
            ptr, owning_objtype = v
            if ptr.name == "__type__":
                return False
            if not objtype.name.startswith("schema::") and ptr.name == "id":
                return False
            if (
                owning_objtype.name.startswith("schema::")
                and ptr.name.startswith("is_")
                and ptr.is_computed
            ):
                # Skip deprecated schema props (is_-prefixed).
                return False
            return True

        reg_pointers = list(
            filter(_filter, self._get_pointer_origins(objtype))
        )
        args = []
        if reg_pointers:
            args.extend(["/", "*"])
        for ptr, org_objtype in reg_pointers:
            init_ptr_t = self.get_ptr_type(
                org_objtype,
                ptr,
                style="arg",
                prefer_broad_target_type=True,
            )
            args.append(f"{ptr.name}: {init_ptr_t}")

        with self.type_checking():
            with self._method_def("__init__", args):
                self.write(
                    f'"""Create a new {objtype.name} instance '
                    "from keyword arguments."
                )
                self.write()
                self.write(
                    "Call db.save() on the returned object to persist it "
                    "in the database."
                )
                self.write('"""')
                self.write("...")
                self.write()
        if objtype.name.startswith("schema::"):
            with self.not_type_checking():
                with self._method_def("__init__", ["/", "**kwargs: Any"]):
                    self.write('_id = kwargs.pop("id", None)')
                    self.write("super().__init__(**kwargs)")
                    self.write("self._p__id = _id")
            self.write()

    def _get_links_with_props(
        self,
        objtype: reflection.ObjectType,
        local: bool | None = None,
    ) -> list[reflection.Pointer]:
        type_name = reflection.parse_name(objtype.name)

        def _filter(ptr: reflection.Pointer) -> bool:
            if not reflection.is_link(ptr):
                return False
            if not ptr.pointers:
                return False

            target_type = self._types[ptr.target_id]
            target_type_name = reflection.parse_name(target_type.name)

            if (
                local is not None
                and local != (target_type_name.parent == type_name.parent)
            ):
                return False

            return True

        return list(filter(_filter, objtype.pointers))

    def write_object_type_link_variants(
        self,
        objtype: reflection.ObjectType,
        local: bool | None = False,
        target_aspect: ModuleAspect | None = None,
        is_forward_decl: bool = False,
    ) -> None:
        pointers = self._get_links_with_props(objtype, local=local)
        if not pointers:
            return

        if is_forward_decl:
            self._write_object_type_link_variants(
                objtype,
                pointers=pointers,
                target_aspect=target_aspect,
                is_forward_decl=is_forward_decl,
            )
        else:
            with self.not_type_checking():
                type_name = reflection.parse_name(objtype.name)
                obj_class = type_name.name
                classes = self._write_object_type_link_variants(
                    objtype,
                    pointers=pointers,
                    target_aspect=target_aspect,
                    is_forward_decl=is_forward_decl,
                )
                for ptrname, classname in classes.items():
                    self.write(f"{classname}.__name__ = {ptrname!r}")
                    qualname = f"{obj_class}.{ptrname}"
                    self.write(f"{classname}.__qualname__ = {qualname!r}")
                    self.write(
                        f"{obj_class}.__links__.{ptrname} = {classname}")
                    self.write(f"del {classname}")

    def _write_object_type_link_variants(
        self,
        objtype: reflection.ObjectType,
        pointers: Sequence[reflection.Pointer],
        local: bool | None = False,
        target_aspect: ModuleAspect | None = None,
        is_forward_decl: bool = False,
    ) -> Mapping[str, str]:
        type_name = reflection.parse_name(objtype.name)
        name = type_name.name

        self_t = self.import_name("typing", "Self")
        ProxyModel = self.import_name("gel.models.pydantic", "ProxyModel")
        all_ptr_origins = self._get_all_pointer_origins(objtype)

        if target_aspect is None:
            target_aspect = self.current_aspect

        classnames = {}
        for ptr in pointers:
            ptr_origins = [
                self.get_type(
                    ptr,
                    import_time=ImportTime.typecheck,
                    aspect=self.current_aspect,
                )
                for ptr in all_ptr_origins[ptr.name]
            ]

            target_type = self._types[ptr.target_id]
            target = self.get_type(
                target_type,
                aspect=target_aspect,
            )

            if is_forward_decl:
                classname = ptr.name
            else:
                classname = f"{name}__{ptr.name}"
            classnames[ptr.name] = classname
            with self._class_def(
                classname,
                (
                    [f"{s}.__links__.{ptr.name}" for s in ptr_origins]
                    + [target, f"{ProxyModel}[{target}]"]
                ),
            ):
                self.write(
                    f'"""link {objtype.name}.{ptr.name}: {target_type.name}"""'
                )

                if ptr_origins:
                    lprops_bases = _map_name(
                        lambda s: f"{s}.__links__.{ptr.name}.__lprops__",
                        ptr_origins,
                    )
                else:
                    b = self.import_name("gel.models.pydantic", "GelLinkModel")
                    lprops_bases = [b]

                opt = self.import_name("typing", "Optional")
                with self._class_def("__lprops__", lprops_bases):
                    assert ptr.pointers
                    lprops = []
                    lprop_assign = []
                    for lprop in ptr.pointers:
                        if lprop.name in {"source", "target"}:
                            continue
                        ptr_type = self.get_type(
                            self._types[lprop.target_id],
                            import_time=ImportTime.typecheck,
                        )
                        lprop_assign.append(f"{lprop.name}={lprop.name}")
                        lprop_line = f"{lprop.name}: {opt}[{ptr_type}]"
                        self.write(lprop_line)
                        lprops.append(f"{lprop_line} = None")

                self.write()
                priv_attr = self.import_name(
                    "gel.models.pydantic", "PrivateAttr")

                self.write(f"_p__obj__: {target} = {priv_attr}()")
                self.write(f"_p__lprops__: __lprops__ = {priv_attr}()")

                self.write()
                args = [f"obj: {target}", "/", "*"] + lprops
                with self._method_def("__init__", args):
                    if is_forward_decl:
                        self.write("...")
                    else:
                        self.write("self._p__obj__ = obj")
                        self.write(self.format_list(
                            "self._p__lprops__ = self.__lprops__({list})",
                            lprop_assign,
                        ))

                self.write()
                args = [f"obj: {target}", "/", "*"] + lprops
                with self._classmethod_def("link", args, self_t):
                    if is_forward_decl:
                        self.write("...")
                    else:
                        self.write(
                            self.format_list(
                                "return cls({list})",
                                ["obj"] + lprop_assign,
                            ),
                        )

                self.write()

        return classnames

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
            objtype,
            aspect=ModuleAspect.VARIANTS,
        )
        base_types = [base]
        base_types.extend(
            [self.get_type(self._types[base.id]) for base in objtype.bases]
        )
        with self._class_def(name, base_types):
            pointers = [
                ptr
                for ptr in objtype.pointers
                if ptr.name not in {"id", "__type__"}
            ]
            if objtype.name.startswith("schema::"):
                pointers = [
                    ptr
                    for ptr in objtype.pointers
                    if not ptr.name.startswith("is_") or not ptr.is_computed
                ]

            if pointers:
                for ptr in pointers:
                    ptr_type = self.get_ptr_type(
                        objtype,
                        ptr,
                        style="property",
                        aspect=ModuleAspect.MAIN,
                    )
                    self.write(f"{ptr.name}: {ptr_type}")
            else:
                self.write("pass")
                self.write()

    def _get_pointer_origins(
        self,
        objtype: reflection.ObjectType,
    ) -> list[tuple[reflection.Pointer, reflection.ObjectType]]:
        pointers: dict[
            str, tuple[reflection.Pointer, reflection.ObjectType]
        ] = {}
        for ancestor_ref in reversed(objtype.ancestors):
            ancestor = self._types[ancestor_ref.id]
            assert reflection.is_object_type(ancestor)
            for ptr in ancestor.pointers:
                pointers[ptr.name] = (ptr, ancestor)

        for ptr in objtype.pointers:
            pointers[ptr.name] = (ptr, objtype)

        return list(pointers.values())

    def _get_all_pointer_origins(
        self,
        objtype: reflection.ObjectType,
    ) -> dict[str, list[reflection.ObjectType]]:
        pointers: dict[str, list[reflection.ObjectType]] = defaultdict(list)
        for ancestor_ref in reversed(objtype.ancestors):
            ancestor = self._types[ancestor_ref.id]
            assert reflection.is_object_type(ancestor)
            for ptr in ancestor.pointers:
                pointers[ptr.name].append(ancestor)

        return pointers

    def _py_container_for_multiprop(
        self,
        prop: reflection.Pointer,
    ) -> str:
        if reflection.is_link(prop):
            pytype = self.import_name("gel.models.pydantic", "DistinctList")
        else:
            pytype = "list"

        return pytype

    def get_ptr_type(
        self,
        objtype: reflection.ObjectType,
        prop: reflection.Pointer,
        *,
        style: Literal[
            "annotation", "property", "typeddict", "arg"
        ] = "annotation",
        prefer_broad_target_type: bool = False,
        aspect: ModuleAspect | None = None,
    ) -> str:
        if aspect is None:
            aspect = ModuleAspect.VARIANTS

        target_type = self._types[prop.target_id]
        bare_ptr_type = ptr_type = self.get_type(
            target_type,
            aspect=aspect,
            import_time=ImportTime.late_runtime,
        )

        if (
            reflection.is_link(prop)
            and prop.pointers
            and not prefer_broad_target_type
        ):
            objtype_name = reflection.parse_name(objtype.name)
            if self.current_aspect is ModuleAspect.VARIANTS:
                target_type = self._types[prop.target_id]
                target_name = reflection.parse_name(target_type.name)
                if target_name.parent != objtype_name.parent:
                    aspect = ModuleAspect.LATE
            ptr_type = f"{objtype_name.name}.__links__.{prop.name}"
            link_type = True
        else:
            link_type = False

        card = reflection.Cardinality(prop.card)
        if card.is_optional():
            if style == "annotation":
                if card.is_multi():
                    pytype = self._py_container_for_multiprop(prop)
                    return f"{pytype}[{ptr_type}]"
                else:
                    optdef = self.import_name(
                        "gel.models.pydantic", "OptionalWithDefault"
                    )
                    return f"{optdef}[{ptr_type}]"
            elif style == "typeddict":
                not_required = self.import_name(
                    "typing_extensions", "NotRequired"
                )
                if card.is_multi():
                    pytype = self._py_container_for_multiprop(prop)
                    return f"{not_required}[{pytype}[{ptr_type}]]"
                else:
                    return f"{not_required}[{ptr_type}]"
            elif style == "arg":
                if card.is_multi():
                    deflist = self.import_name("typing", "Iterable")
                    return f"Iterable[{ptr_type}] = []"
                else:
                    opt = self.import_name("typing", "Optional")
                    return f"{opt}[{ptr_type}] = None"
            elif style == "property":
                if card.is_multi():
                    pytype = self._py_container_for_multiprop(prop)
                    return f"{pytype}[{ptr_type}]"
                elif link_type:
                    link = self.import_name("gel.models.pydantic", "OptionalLink")
                    return f"{link}[{ptr_type}, {bare_ptr_type}] = {link}(default=None)"
                else:
                    opt = self.import_name("typing", "Optional")
                    return f"{opt}[{ptr_type}] = None"
            else:
                raise AssertionError(
                    f"unexpected type rendering style: {style!r}"
                )

        elif card.is_multi():
            deflist = self.import_name("gel.models.pydantic", "DistinctList")
            return f"{deflist}[{ptr_type}]"
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
        namedtuple = self.import_name("typing", "NamedTuple")

        self.write("#")
        self.write(f"# tuple type {t.name}")
        self.write("#")
        self.write(f"class {self.get_tuple_name(t)}({namedtuple}):")
        for elem in t.tuple_elements:
            elem_type = self.get_type(
                self._types[elem.type_id],
                import_time=ImportTime.typecheck,
            )
            self.write(f"    {elem.name}: {elem_type}")
        self.write()
