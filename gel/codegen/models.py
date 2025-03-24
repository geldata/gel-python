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

from collections import defaultdict
from contextlib import contextmanager

import gel
from gel import abstract
from gel import describe
from gel.con_utils import find_gel_project_dir
from gel.color import get_color

from gel.orm.introspection import FilePrinter, get_mod_and_name

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
    not .builtin
    and
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
    m := (select `Module` filter not .builtin)
select _ := m.name order by _;
"""

COMMENT = """\
#
# Automatically generated from Gel schema.
#
# Do not edit directly as re-generating this file will overwrite any changes.
#\
"""


class StrEnum(str, enum.Enum):
    pass


class Cardinality(StrEnum):
    One = "One"
    Many = "Many"


class TypeRef(NamedTuple):
    name: str


class ConstraintParam(NamedTuple):
    name: str
    value: str


class IntrospectedConstraint(NamedTuple):
    name: str
    params: list[NamedTuple]


class IntrospectedProperty(NamedTuple):
    name: str
    readonly: bool
    required: bool
    cardinality: Cardinality
    exclusive: bool
    target: TypeRef
    constraints: list[IntrospectedConstraint]


class IntrospectedLink(IntrospectedProperty):
    properties: list[IntrospectedProperty]


class IntrospectedType(NamedTuple):
    name: str
    links: list[IntrospectedLink]
    properties: list[IntrospectedProperty]


class IntrospectedModule(TypedDict):
    object_types: dict[str, IntrospectedType]
    scalar_types: dict[str, IntrospectedType]


class ModelsGenerator(base.Generator, FilePrinter):
    def __init__(self, args: argparse.Namespace):
        base.Generator.__init__(self, args)
        FilePrinter.__init__(self)

        self._basemodule = "models"
        self._outdir = pathlib.Path("models")
        self._modules: dict[str, IntrospectedModule] = {}
        self._types: dict[str, IntrospectedType] = {}

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
                    self.write_types(maps)

        base.print_msg(f"{C.GREEN}{C.BOLD}Done.{C.ENDC}")

    def introspect_types(self) -> list[IntrospectedType]:
        return self._client.query(INTRO_QUERY)  # type: ignore [no-any-return]

    def introspect_modules(self) -> list[str]:
        return self._client.query(MODULE_QUERY)  # type: ignore [no-any-return]

    def get_schema(self) -> None:
        for mod in self.introspect_modules():
            self._modules[mod] = {
                "object_types": {},
                "scalar_types": {},
            }

        for t in self.introspect_types():
            mod, name = get_mod_and_name(t.name)
            self._types[t.name] = t
            self._modules[mod]["object_types"][t.name] = t

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

    def write_types(self, maps: IntrospectedModule) -> None:
        object_types = maps["object_types"]

        if object_types:
            self.write(
                f"from typing import Optional, Annotated, TypeVar, Union, Type"
            )
            self.write(f"from gel.models import pydantic as gm")

        self.write("import uuid")

        objects = sorted(object_types.values(), key=lambda x: x.name)
        for obj in objects:
            self.render_type(obj)

    def render_type(self, objtype: IntrospectedType) -> None:
        mod, name = get_mod_and_name(objtype.name)

        self.write()
        self.write()
        self.write("#")
        self.write(f"# type {objtype.name}")
        self.write("#")

        for prop in objtype.properties:
            non_opt = self.render_prop_type(prop, optional=False)
            opt = self.render_prop_type(prop, optional=True)
            self.write(f"{name}__p__{prop.name}_req = {non_opt}")
            self.write(f"{name}__p__{prop.name}_opt = {opt}")
            self.write(f"{name}__p__{prop.name}_selector = Union[")
            with self.indented():
                self.write(f"{name}__p__{prop.name}_req,")
                self.write(f"{name}__p__{prop.name}_opt,")
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
        self.write(f"class {name}(gm.BaseGelModel):")
        with self.indented():
            self.write(
                f"__gel_metadata__ = "
                f"gm.GelMetadata(schema_name={objtype.name!r})",
            )

            if len(objtype.properties) > 0:
                self.write()
                self.write("# Properties:")
                for prop in objtype.properties:
                    self.write(f"{prop.name}: {name}__p__{prop.name}_opt")

            if len(objtype.links) > 0:
                self.write()
                self.write("# Links:")
                for link in objtype.links:
                    self.render_link(link, mod)

            self.write("@classmethod")
            self.write(f"def select(cls, *ptrs: Type[{name}__pointers]):")
            with self.indented():
                self.write("pass")

        self.write()

    def render_prop_type(
        self,
        prop: IntrospectedProperty,
        optional: bool,
    ) -> str:
        gel_type = prop.target.name
        pytype = base.TYPE_MAPPING.get(gel_type)
        if not pytype:
            raise NotImplementedError(f"unsupported Gel type: {gel_type}")
        if optional:
            pytype = f"Optional[{pytype}]"
        annotated = [
            pytype,
            f"gm.GelMetadata(schema_name={prop.name!r})",
        ]
        if prop.exclusive:
            annotated.append("gm.Exclusive")
        annotations = textwrap.indent(",\n".join(annotated), "    ")
        return f"Annotated[\n{annotations},\n]"

    def render_prop(self, prop: IntrospectedProperty, curmod: str) -> None:
        pytype = base.TYPE_MAPPING.get(prop.target.name)
        defval = ""
        if not pytype:
            # skip
            return

        # FIXME: need to also handle multi

        if not prop.required:
            pytype = f"Optional[{pytype}]"
            # A value does not need to be supplied
            defval = " = None"

        if prop.exclusive:
            pytype = f"Annotated[{pytype}, gm.Exclusive]"

        self.write(f"{prop.name}: {pytype}{defval}")

    def render_link(self, link: IntrospectedLink, curmod: str) -> None:
        mod, name = get_mod_and_name(link.target.name)
        if curmod == mod:
            pytype = name
        else:
            pytype = link.target.name.replace("::", ".")

        # FIXME: need to also handle multi

        if link.required:
            self.write(f"{link.name}: {pytype!r}")
        else:
            # A value does not need to be supplied
            self.write(f"{link.name}: Optional[{pytype!r}] = None")
