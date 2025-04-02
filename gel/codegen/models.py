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
    TypedDict,
    NamedTuple,
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
from .base import C


INTRO_QUERY = """
with module schema
select ObjectType {
    name,
    links: {
        name,
        readonly,
        required,
        cardinality,
        exclusive := exists (
            select .constraints
            filter .name = 'std::exclusive'
        ),
        target: {name},
        constraints: {
            name,
            params: {name, @value},
        },

        properties: {
            name,
            readonly,
            required,
            cardinality,
            exclusive := exists (
                select .constraints
                filter .name = 'std::exclusive'
            ),
            target: {name},
            constraints: {
                name,
                params: {name, @value},
            },
        },
    } filter .name != '__type__' and not exists .expr,
    properties: {
        name,
        readonly,
        required,
        cardinality,
        exclusive := exists (
            select .constraints
            filter .name = 'std::exclusive'
        ),
        target: {name},
        constraints: {
            name,
            params: {name, @value},
        },
    } filter not exists .expr,
    backlinks := <array<str>>[],
}
filter
    not .internal
    and
    not .from_alias
    and
    not re_test('^(std|cfg|sys|schema)::', .name)
    and
    not any(re_test('^(cfg|sys|schema)::', .ancestors.name));
"""

MODULE_QUERY = """
with
    module schema,
    m := (select `Module`)
select _ := m.name order by _;
"""

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


class ModelsGenerator(base.Generator, FilePrinter):
    def __init__(self, args: argparse.Namespace):
        base.Generator.__init__(self, args)
        FilePrinter.__init__(self)

        self._basemodule = "models"
        self._outdir = pathlib.Path("models")
        self._modules: dict[str, IntrospectedModule] = {}
        self._types: dict[uuid.UUID, reflection.AnyType] = {}
        self._wrapped_types: set[str] = set()

    def run(self) -> None:
        try:
            self._client.ensure_connected()
        except gel.EdgeDBError as e:
            print(f"Failed to connect to EdgeDB instance: {e}")
            sys.exit(61)

        self.get_schema()

        with self._client:
            for mod, maps in self._modules.items():
                if not maps:
                    # skip apparently empty modules
                    continue

                with self.init_module(mod):
                    self.write_types(mod, maps)

        base.print_msg(f"{C.GREEN}{C.BOLD}Done.{C.ENDC}")

    def introspect_modules(self) -> list[str]:
        return self._client.query(MODULE_QUERY)  # type: ignore [no-any-return]

    def get_schema(self) -> None:
        for mod in self.introspect_modules():
            self._modules[mod] = {
                "scalar_types": {},
                "object_types": {},
                "imports": {},
            }

        self._types = reflection.fetch_types(self._client)

        for t in self._types.values():
            if t.kind not in {
                reflection.TypeKind.Scalar.value,
                reflection.TypeKind.Object.value,
            }:
                continue
            mod, name = get_mod_and_name(t.name)
            if t.kind == reflection.TypeKind.Object.value:
                self._modules[mod]["object_types"][t.name] = t
            elif t.kind == reflection.TypeKind.Scalar.value:
                self._modules[mod]["scalar_types"][t.name] = t

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

    @contextmanager
    def init_module(self, mod: str) -> Generator[io.TextIOBase, None, None]:
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
                self.out = f
                self.write(f"{COMMENT}\n")
                yield f
            finally:
                self.out = None

    def write_types(self, modname: str, maps: IntrospectedModule) -> None:
        self.write("from __future__ import annotations")

        self.write("import uuid")

        scalar_types = maps["scalar_types"]
        for type_name, type in scalar_types.items():
            if type.enum_values:
                self.write(f"class {type_name}(StrEnum):")
                with self.indented():
                    for value in type.enum_values:
                        self.write(f"{value} = {value!r}")
            else:
                if type.material_id is not None:
                    print(type_name)

        object_types = maps["object_types"]

        if object_types:
            self.write(
                f"from typing import TYPE_CHECKING, Optional, Annotated, TypeVar, Union, Type, ParamSpec, Any, TypeAlias"
            )
            self.write(f"from gel.models import pydantic as gm")
            self.write(f"from gel.models import expr as gexpr")

        objects = sorted(object_types.values(), key=lambda x: x.name)
        for obj in objects:
            self.render_type(modname, obj)

    def render_type(
        self, modname: str, objtype: reflection.ObjectType
    ) -> None:
        mod, name = get_mod_and_name(objtype.name)

        # for prop in objtype.pointers:
        #     prop_type = self.get_prop_type(prop)
        #     if prop_type not in self._wrapped_types:
        #         type_wrapper = f"_{prop_type.replace('.', '_')}_Subtype"
        #         self.write(
        #             f"class {type_wrapper}({prop_type}, gm.ValidatedType[{prop_type}]):"
        #         )
        #         self.write("    pass")
        #         self._wrapped_types.add(prop_type)

        self.write()
        self.write()
        self.write("#")
        self.write(f"# type {objtype.name}")
        self.write("#")

        modpath = tuple(modname.split("::"))

        for prop in objtype.pointers:
            orig_prop_type = self.get_prop_type(modpath, prop)
            prop_type = f"_{orig_prop_type.replace('.', '_')}_Subtype"
            self.write(
                f"class {name}__p__{prop.name}_req_t({prop_type}, gexpr.Expression[{prop_type}]):"
            )
            self.write("    pass")
            self.write(
                f"{name}__p__{prop.name} = Optional[{name}__p__{prop.name}_req_t]"
            )
            self.write(f'"""{objtype.name}.{prop.name} ({orig_prop_type})"""')
            self.write(f"{name}__p__{prop.name}_selector = Union[")
            with self.indented():
                self.write(f"{name}__p__{prop.name}_req_t,")
                self.write(f"{name}__p__{prop.name},")
            self.write("]")

        # for link in objtype.links:
        #     non_opt = self.render_prop_type(prop, optional=False)
        #     opt = self.render_prop_type(prop, optional=True)
        #     self.write(f"{name}__p__{prop.name}_req = {non_opt}")
        #     self.write(f"{name}__p__{prop.name}_opt = {opt}")
        #     self.write(f"{name}__p__{prop.name}_selector = Union[")
        #     with self.indented():
        #         self.write(f"{name}__p__{prop.name}_req,")
        #         self.write(f"{name}__p__{prop.name}_opt,")
        #     self.write("]")

        if len(objtype.properties):
            self.write(f"{name}__pointers = TypeVar(")
            with self.indented():
                self.write(f"'{name}__pointers',")
                self.write("bound=Union[")
                with self.indented():
                    for prop in objtype.properties:
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

            if len(objtype.properties) > 0:
                self.write()
                self.write("# Property types:")
                self.write("class __gel_fields__:")
                with self.indented():
                    for prop in objtype.properties:
                        orig_prop_type = self.get_prop_type(prop)
                        self.write(
                            f"{prop.name}: TypeAlias = {name}__p__{prop.name}"
                        )

                self.write()
                self.write("# Properties:")
                for prop in objtype.properties:
                    orig_prop_type = self.get_prop_type(prop)
                    self.write(f"{prop.name}: {name}__p__{prop.name}")
                    self.write(
                        f'"""{objtype.name}.{prop.name} ({orig_prop_type})"""'
                    )

            if len(objtype.links) > 0:
                self.write()
                self.write("# Links:")
                for link in objtype.links:
                    self.render_link(link, mod)

        self.write()

    def get_prop_type(
        self,
        modpath: tuple[str, ...],
        prop: reflection.Pointer,
    ) -> str:
        type = self._types[prop.target_id]
        type_path = pathlib.Path(*type.name.split("::"))
        mod_path = pathlib.Path(*modpath)
        common = pathlib.Path(os.path.commonpath([type_path, mod_path]))
        if common:
            relative_depth = len(mod_path.parts) - len(common.parts)
            import_tail = type_path.parts[len(common.parts):]
        else:
            relative_depth = len(mod_path.parts)
            import_tail = type_path.parts

        module = ".." * relative_depth + ".".join(import_tail[:-1])
        name = import_tail[-1]
        print(module, name)
        1 / 0
        return relative.parts
