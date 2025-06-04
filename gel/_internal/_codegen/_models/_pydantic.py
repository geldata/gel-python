# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

from __future__ import annotations
from typing import (
    TYPE_CHECKING,
    Literal,
    NamedTuple,
    TypedDict,
    TypeVar,
)

import base64
import collections
import enum
import functools
import graphlib
import keyword
import logging
import operator
import textwrap

from collections import defaultdict
from contextlib import contextmanager

import gel
from gel import abstract
from gel._internal import _reflection as reflection
from gel._internal._qbmodel import _abstract as _qbmodel

from .._generator import C, AbstractCodeGenerator
from .._module import ImportTime, CodeSection, GeneratedModule

if TYPE_CHECKING:
    import io
    import pathlib
    import uuid

    from collections.abc import (
        Callable,
        Collection,
        Generator,
        Iterable,
        Iterator,
        Mapping,
    )


COMMENT = """\
#
# Automatically generated from Gel schema.
#
# Do not edit directly as re-generating this file will overwrite any changes.
#\
"""

logger = logging.getLogger(__name__)


@functools.cache
def ident(s: str) -> str:
    if keyword.iskeyword(s):
        return f"{s}_"
    elif s.isidentifier():
        return s
    else:
        result = "".join(
            c if c.isidentifier() or c.isdigit() else "_" for c in s
        )
        if result and result[0].isdigit():
            result = f"_{result}"

        return result


class IntrospectedModule(TypedDict):
    imports: dict[str, str]
    object_types: dict[str, reflection.ObjectType]
    scalar_types: dict[str, reflection.ScalarType]
    functions: list[reflection.Function]


class PydanticModelsGenerator(AbstractCodeGenerator):
    def run(self) -> None:
        try:
            self._client.ensure_connected()
        except gel.EdgeDBError:
            logger.exception("could not connect to Gel instance")
            self.abort(61)

        with self._client:
            std_gen = SchemaGenerator(
                self._client,
                reflection.SchemaPart.STD,
                self._project_dir / "models",
            )
            std_gen.run()

            usr_gen = SchemaGenerator(
                self._client,
                reflection.SchemaPart.USER,
                self._project_dir / "models",
            )
            usr_gen.run()

        self.print_msg(f"{C.GREEN}{C.BOLD}Done.{C.ENDC}")


class SchemaGenerator:
    def __init__(
        self,
        client: abstract.ReadOnlyExecutor,
        schema_part: reflection.SchemaPart,
        outdir: pathlib.Path,
    ) -> None:
        self._client = client
        self._schema_part = schema_part
        self._basemodule = "models"
        self._outdir = outdir
        self._modules: dict[reflection.SchemaPath, IntrospectedModule] = {}
        self._std_modules: list[reflection.SchemaPath] = []
        self._types: Mapping[uuid.UUID, reflection.AnyType] = {}
        self._casts: reflection.CastMatrix
        self._operators: reflection.OperatorMatrix
        self._functions: list[reflection.Function]
        self._named_tuples: dict[uuid.UUID, reflection.NamedTupleType] = {}
        self._wrapped_types: set[str] = set()

    def run(self) -> None:
        self.introspect_schema()

        self._generate_common_types()
        modules: dict[reflection.SchemaPath, GeneratedSchemaModule] = {}
        order = sorted(
            self._modules.items(),
            key=operator.itemgetter(0),
            reverse=True,
        )
        for modname, content in order:
            if not content:
                # skip apparently empty modules
                continue

            module = GeneratedSchemaModule(
                modname,
                all_types=self._types,
                all_casts=self._casts,
                all_operators=self._operators,
                modules=self._modules,
                schema_part=self._schema_part,
            )
            module.process(content)
            module.write_submodules(
                [
                    k
                    for k, v in modules.items()
                    if k.is_relative_to(modname)
                    and len(k.parts) == len(modname.parts) + 1
                    and v.has_content()
                ]
            )
            module.write_files(self._outdir)
            modules[modname] = module

        all_modules = list(self._modules)
        if self._schema_part is not reflection.SchemaPart.STD:
            all_modules += [m for m in self._std_modules if len(m.parts) == 1]
        module = GeneratedSchemaModule(
            reflection.SchemaPath(),
            all_types=self._types,
            all_casts=self._casts,
            all_operators=self._operators,
            modules=all_modules,
            schema_part=self._schema_part,
        )
        module.write_submodules([m for m in all_modules if len(m.parts) == 1])
        default_module = modules.get(reflection.SchemaPath("default"))
        if default_module is not None:
            module.reexport_module(default_module)
        module.write_files(self._outdir)

    def introspect_schema(self) -> None:
        for mod in reflection.fetch_modules(self._client, self._schema_part):
            self._modules[reflection.parse_name(mod)] = {
                "scalar_types": {},
                "object_types": {},
                "functions": [],
                "imports": {},
            }

        this_part = self._schema_part
        std_part = reflection.SchemaPart.STD

        self._types = reflection.fetch_types(self._client, this_part)
        these_types = self._types
        self._casts = reflection.fetch_casts(self._client, this_part)
        self._operators = reflection.fetch_operators(self._client, this_part)
        these_funcs = reflection.fetch_functions(self._client, this_part)
        self._functions = these_funcs

        if self._schema_part is not std_part:
            std_types = reflection.fetch_types(self._client, std_part)
            self._types = collections.ChainMap(std_types, these_types)
            std_casts = reflection.fetch_casts(self._client, std_part)
            self._casts = self._casts.chain(std_casts)
            std_operators = reflection.fetch_operators(self._client, std_part)
            self._operators = self._operators.chain(std_operators)
            self._functions = these_funcs + reflection.fetch_functions(
                self._client, std_part
            )
            self._std_modules = [
                reflection.parse_name(mod)
                for mod in reflection.fetch_modules(self._client, std_part)
            ]
        else:
            self._std_modules = list(self._modules)

        for t in these_types.values():
            if reflection.is_object_type(t):
                name = reflection.parse_name(t.name)
                self._modules[name.parent]["object_types"][name.name] = t
            elif reflection.is_scalar_type(t):
                name = reflection.parse_name(t.name)
                self._modules[name.parent]["scalar_types"][name.name] = t
            elif reflection.is_named_tuple_type(t):
                self._named_tuples[t.id] = t

        for f in these_funcs:
            name = reflection.parse_name(f.name)
            self._modules[name.parent]["functions"].append(f)

    def get_comment_preamble(self) -> str:
        return COMMENT

    def _generate_common_types(self) -> None:
        mod = reflection.SchemaPath("__types__")
        if self._schema_part is reflection.SchemaPart.STD:
            mod = reflection.SchemaPath("std") / mod
        module = GeneratedGlobalModule(
            mod,
            all_types=self._types,
            all_casts=self._casts,
            all_operators=self._operators,
            modules=self._modules,
            schema_part=self._schema_part,
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
    module_alias: str | None


@functools.cache
def get_modpath(
    modpath: reflection.SchemaPath,
    aspect: ModuleAspect,
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


BASE_IMPL = "gel.models.pydantic"
CORE_OBJECTS = {
    "std::BaseObject",
    "std::Object",
    "std::FreeObject",
}


class BaseGeneratedModule:
    def __init__(
        self,
        modname: reflection.SchemaPath,
        *,
        all_types: Mapping[uuid.UUID, reflection.AnyType],
        all_casts: reflection.CastMatrix,
        all_operators: reflection.OperatorMatrix,
        modules: Collection[reflection.SchemaPath],
        schema_part: reflection.SchemaPart,
    ) -> None:
        super().__init__()
        self._modpath = modname
        self._types = all_types
        self._types_by_name: dict[str, reflection.AnyType] = {}
        self._casts = all_casts
        self._operators = all_operators
        schema_obj_type = None
        for t in all_types.values():
            self._types_by_name[t.name] = t
            if t.name == "schema::ObjectType":
                assert reflection.is_object_type(t)
                schema_obj_type = t

        if schema_obj_type is None:
            raise RuntimeError(
                "schema::ObjectType type not found in schema reflection"
            )
        self._schema_object_type = schema_obj_type
        self._modules = frozenset(modules)
        self._submodules = sorted(
            m
            for m in self._modules
            if m.is_relative_to(modname)
            and len(m.parts) == len(modname.parts) + 1
        )
        self._schema_part = schema_part
        self._is_package = self.mod_is_package(modname, schema_part)
        self._py_files = {
            ModuleAspect.MAIN: GeneratedModule(COMMENT),
            ModuleAspect.VARIANTS: GeneratedModule(COMMENT),
            ModuleAspect.LATE: GeneratedModule(COMMENT),
        }
        self._current_py_file = self._py_files[ModuleAspect.MAIN]
        self._current_aspect = ModuleAspect.MAIN
        self._type_import_cache: dict[
            tuple[str, ModuleAspect, ModuleAspect, bool, ImportTime],
            str,
        ] = {}

    def get_mod_schema_part(
        self,
        mod: reflection.SchemaPath,
    ) -> reflection.SchemaPart:
        if (
            self._schema_part is reflection.SchemaPart.STD
            or mod not in self._modules
        ):
            return reflection.SchemaPart.STD
        else:
            return reflection.SchemaPart.USER

    def mod_is_package(
        self,
        mod: reflection.SchemaPath,
        schema_part: reflection.SchemaPart,
    ) -> bool:
        return (
            not mod.parts
            or bool(self._submodules)
            or (
                schema_part is reflection.SchemaPart.STD
                and len(mod.parts) == 1
            )
        )

    @property
    def py_file(self) -> GeneratedModule:
        return self._current_py_file

    @property
    def py_files(self) -> Mapping[ModuleAspect, GeneratedModule]:
        return self._py_files

    @property
    def current_aspect(self) -> ModuleAspect:
        return self._current_aspect

    def has_content(self) -> bool:
        return self.py_files[ModuleAspect.MAIN].has_content()

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
        return get_modpath(self._modpath, aspect)

    @property
    def is_package(self) -> bool:
        return self._is_package

    @contextmanager
    def _open_py_file(
        self,
        path: pathlib.Path,
        modpath: reflection.SchemaPath,
        *,
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
        self._init_dir(path)
        for el in dirpath.parts:
            path /= el
            self._init_dir(path)

        with open(path / filename, "w", encoding="utf8") as f:
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

    def write_files(self, path: pathlib.Path) -> None:
        for aspect, py_file in self.py_files.items():
            if not py_file.has_content() and (
                self._schema_part is not reflection.SchemaPart.STD
                or aspect is not ModuleAspect.MAIN
            ):
                continue

            with self._open_py_file(
                path,
                self.modpath(aspect),
                as_pkg=self.is_package,
            ) as f:
                py_file.output(f)

    def import_name(
        self,
        module: str,
        name: str,
        *,
        suggested_module_alias: str | None = None,
        import_time: ImportTime = ImportTime.runtime,
        directly: bool = True,
        localns: frozenset[str] | None = None,
    ) -> str:
        return self.py_file.import_name(
            module,
            name,
            suggested_module_alias=suggested_module_alias,
            import_time=import_time,
            directly=directly,
            localns=localns,
        )

    def export(self, *name: str) -> None:
        self.py_file.export(*name)

    @property
    def exports(self) -> set[str]:
        return self.py_file.exports

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

    @property
    def in_type_checking(self) -> bool:
        return self.py_file.in_type_checking

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
        digest = base64.b64encode(t.id.bytes[:4], altchars=b"__").decode()
        return "".join(names) + "_Tuple_" + digest.rstrip("=")

    def _resolve_rel_import(
        self,
        imp_path: reflection.SchemaPath,
        aspect: ModuleAspect,
        *,
        import_directly: bool = False,
    ) -> Import | None:
        imp_mod_canon = imp_mod = imp_path.parent
        cur_mod = self.current_modpath
        if aspect is not ModuleAspect.MAIN:
            imp_mod = get_modpath(imp_mod, aspect)

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
            if (
                imp_mod_canon == self.canonical_modpath
                and self.current_aspect is ModuleAspect.MAIN
            ):
                module_alias = "base"
            else:
                module_alias = "_".join(imp_path.parts[:-1])

            imp_name = imp_path.name

            if not import_directly and all(c == "." for c in py_mod):
                if len(imp_path.parts) == 1:
                    py_mod = f"{py_mod}{imp_path.parts[0]}"
                    imp_name = "."
                else:
                    py_mod = f"{py_mod}.{imp_path.parts[-2]}"

            return Import(
                module=py_mod,
                name=imp_name,
                module_alias=module_alias,
            )

    def get_type(
        self,
        stype: reflection.AnyType,
        *,
        import_time: ImportTime | None = None,
        aspect: ModuleAspect = ModuleAspect.MAIN,
        import_directly: bool | None = None,
        allow_typevars: bool = True,
        localns: frozenset[str] | None = None,
    ) -> str:
        if import_time is None:
            import_time = (
                ImportTime.typecheck
                if self.in_type_checking
                else ImportTime.runtime
            )

        if reflection.is_array_type(stype):
            arr = self.import_name(BASE_IMPL, "Array")
            elem_type = self.get_type(
                self._types[stype.array_element_id],
                import_time=import_time,
                import_directly=import_directly,
                aspect=aspect,
                localns=localns,
            )
            return f"{arr}[{elem_type}]"

        elif reflection.is_tuple_type(stype):
            tup = self.import_name(BASE_IMPL, "Tuple")
            elem_types = [
                self.get_type(
                    self._types[elem.type_id],
                    import_time=import_time,
                    import_directly=import_directly,
                    aspect=aspect,
                    localns=localns,
                )
                for elem in stype.tuple_elements
            ]
            return f"{tup}[{', '.join(elem_types)}]"

        elif reflection.is_range_type(stype):
            rang = self.import_name(BASE_IMPL, "Range")
            elem_type = self.get_type(
                self._types[stype.range_element_id],
                import_time=import_time,
                import_directly=import_directly,
                aspect=aspect,
                localns=localns,
            )
            return f"{rang}[{elem_type}]"

        elif reflection.is_multi_range_type(stype):
            rang = self.import_name(BASE_IMPL, "MultiRange")
            elem_type = self.get_type(
                self._types[stype.multirange_element_id],
                import_time=import_time,
                import_directly=import_directly,
                aspect=aspect,
                localns=localns,
            )
            return f"{rang}[{elem_type}]"

        elif reflection.is_pseudo_type(stype):
            if stype.name == "anyobject":
                return self.import_name(BASE_IMPL, "GelModel")
            elif stype.name == "anytuple":
                return f"tuple[{self.import_name('typing', 'Any')}, ...]"
            elif stype.name == "anytype":
                basetype = "GelType_T" if allow_typevars else "GelType"
                return self.import_name(BASE_IMPL, basetype)
            else:
                raise AssertionError(f"unsupported pseudo-type: {stype.name}")

        elif reflection.is_named_tuple_type(stype):
            mod = "__types__"
            if stype.builtin:
                mod = f"std::{mod}"
            type_name = f"{mod}::{self.get_tuple_name(stype)}"
            if import_directly is None:
                import_directly = True
            # Named tuples are always imported from __types__,
            # which has only the MAIN aspect.
            aspect = ModuleAspect.MAIN

        else:
            type_name = stype.name
            if import_directly is None:
                import_directly = False

        type_path = reflection.parse_name(type_name)

        if (
            self._schema_part is reflection.SchemaPart.STD
            and reflection.is_scalar_type(stype)
        ):
            # std modules have complex cyclic deps,
            # especially where scalars are involved.
            aspect = ModuleAspect.VARIANTS

        if (
            self._schema_part is not reflection.SchemaPart.STD
            and type_path.parent not in self._modules
            and not reflection.is_named_tuple_type(stype)
            and import_time is ImportTime.late_runtime
        ):
            import_time = ImportTime.runtime

        cur_aspect = self.current_aspect
        cache_key = (
            type_name,
            aspect,
            cur_aspect,
            import_directly,
            import_time,
        )
        result = self._type_import_cache.get(cache_key)
        if result is not None:
            return result

        type_name = type_path.name
        imp_path = type_path

        rel_import = self._resolve_rel_import(
            imp_path, aspect, import_directly=import_directly
        )
        if rel_import is None:
            result = type_name
        else:
            result = self.import_name(
                rel_import.module,
                rel_import.name,
                suggested_module_alias=rel_import.module_alias,
                import_time=import_time,
                directly=import_directly,
            )

        self._type_import_cache[cache_key] = result
        return result

    def format_list(
        self,
        tpl: str,
        values: list[str],
        *,
        first_line_comment: str | None = None,
    ) -> str:
        return self.py_file.format_list(
            tpl,
            values,
            first_line_comment=first_line_comment,
        )

    def _format_class_line(
        self,
        class_name: str,
        bases: Iterable[str],
        *,
        class_kwargs: dict[str, str] | None = None,
        first_line_comment: str | None = None,
    ) -> str:
        args = list(bases)
        if class_kwargs:
            args.extend(f"{k}={v}" for k, v in class_kwargs.items())
        if args:
            return self.format_list(
                f"class {class_name}({{list}}):",
                args,
                first_line_comment=first_line_comment,
            )
        else:
            line = f"class {class_name}:"
            if first_line_comment:
                line = f"{line}  # {first_line_comment}"
            return line

    @contextmanager
    def _class_def(
        self,
        class_name: str,
        base_types: Iterable[str],
        *,
        class_kwargs: dict[str, str] | None = None,
        line_comment: str | None = None,
    ) -> Iterator[None]:
        class_line = self._format_class_line(
            class_name,
            base_types,
            class_kwargs=class_kwargs,
            first_line_comment=line_comment,
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
        overload: bool = False,
        stub: bool = False,
        decorators: Iterable[str] = (),
        line_comment: str | None = None,
        implicit_param: bool = True,
    ) -> Iterator[None]:
        if overload:
            over = self.import_name("typing", "overload")
            self.write(f"@{over}")
        for decorator in decorators:
            self.write(f"@{decorator}")
        if kind == "classmethod":
            self.write("@classmethod")
        elif kind == "property":
            self.write("@property")
        params = list(params)

        if kind == "classmethod" and implicit_param:
            params = ["cls", *params]
        elif kind in {"method", "property"} and implicit_param:
            params = ["self", *params]

        tpl = f"def {func_name}({{list}}) -> {return_type}:"
        if stub:
            tpl += " ..."
        def_line = self.format_list(
            tpl,
            params,
            first_line_comment=line_comment,
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
        overload: bool = False,
        decorators: Iterable[str] = (),
        line_comment: str | None = None,
    ) -> Iterator[None]:
        with self._func_def(
            func_name,
            params,
            return_type,
            kind="classmethod",
            overload=overload,
            decorators=decorators,
            line_comment=line_comment,
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
        overload: bool = False,
        decorators: Iterable[str] = (),
        line_comment: str | None = None,
    ) -> Iterator[None]:
        with self._func_def(
            func_name,
            params,
            return_type,
            kind="property",
            overload=overload,
            decorators=decorators,
            line_comment=line_comment,
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
        overload: bool = False,
        decorators: Iterable[str] = (),
        line_comment: str | None = None,
        implicit_param: bool = True,
    ) -> Iterator[None]:
        with self._func_def(
            func_name,
            params,
            return_type,
            kind="method",
            overload=overload,
            decorators=decorators,
            line_comment=line_comment,
            implicit_param=implicit_param,
        ):
            yield


InheritingType_T = TypeVar("InheritingType_T", bound=reflection.InheritingType)


class GeneratedSchemaModule(BaseGeneratedModule):
    def process(self, mod: IntrospectedModule) -> None:
        self.write_scalar_types(mod["scalar_types"])
        self.write_object_types(mod["object_types"])
        self.write_functions(mod["functions"])

    def reexport_module(self, mod: GeneratedSchemaModule) -> None:
        exports = sorted(mod.exports)
        if not exports:
            return

        rel_imp = self._resolve_rel_import(
            mod.canonical_modpath / exports[0],
            aspect=ModuleAspect.MAIN,
            import_directly=True,
        )
        if rel_imp is None:
            raise RuntimeError(
                f"could not resolve module import: {mod.canonical_modpath}"
            )

        for export in exports:
            self.import_name(
                rel_imp.module,
                export,
                suggested_module_alias=rel_imp.module_alias,
            )

            self.export(export)

    def write_submodules(self, mods: list[reflection.SchemaPath]) -> None:
        if not mods:
            return

        builtins_str = self.import_name(
            "builtins", "str", import_time=ImportTime.typecheck
        )
        any_ = self.import_name(
            "typing", "Any", import_time=ImportTime.typecheck
        )
        implib = self.import_name("importlib", ".")

        for mod in mods:
            self.import_name(
                "." + mod.name, ".", import_time=ImportTime.typecheck
            )

        with self.not_type_checking():
            with self._func_def(
                "__getattr__", [f"name: {builtins_str}"], any_
            ):
                self.write(
                    self.format_list(
                        "mods = frozenset([{list}])",
                        [f'"{m.name}"' for m in mods],
                    )
                )
                self.write("if name in mods:")
                with self.indented():
                    self.write(
                        f'return {implib}.import_module("." + name, __name__)'
                    )
                self.write(
                    'e = f"module {__name__!r} has no attribute {name!r}"'
                )
                self.write("raise AttributeError(e)")

        for mod in mods:
            self.export(mod.name)

    def write_description(
        self,
        stype: reflection.ScalarType | reflection.ObjectType,
    ) -> None:
        if not stype.description:
            return

        desc = textwrap.wrap(
            textwrap.dedent(stype.description).strip(),
            break_long_words=False,
        )
        self.write('"""')
        self.write("\n".join(desc))
        self.write('"""')

    def _sorted_types(
        self,
        types: Iterable[InheritingType_T],
    ) -> Iterator[InheritingType_T]:
        graph: dict[uuid.UUID, set[uuid.UUID]] = {}
        for t in types:
            graph[t.id] = set()
            t_name = reflection.parse_name(t.name)

            for base_ref in t.bases:
                base = self._types[base_ref.id]
                base_name = reflection.parse_name(base.name)
                if t_name.parent == base_name.parent:
                    graph[t.id].add(base.id)

        for tid in graphlib.TopologicalSorter(graph).static_order():
            stype = self._types[tid]
            yield stype  # type: ignore [misc]

    def _get_pybase_for_this_scalar(
        self,
        stype: reflection.ScalarType,
        *,
        require_subclassable: bool = False,
        consider_abstract: bool = True,
        import_time: ImportTime = ImportTime.runtime,
    ) -> list[str] | None:
        base_type = _qbmodel.get_py_type_for_scalar(
            stype.name,
            require_subclassable=require_subclassable,
            consider_abstract=consider_abstract,
        )
        if not base_type:
            return None
        else:
            return sorted(
                self.import_name(*t, import_time=import_time)
                for t in base_type
            )

    def _get_scalar_hierarchy(
        self,
        stype: reflection.ScalarType,
    ) -> list[str]:
        return [
            stype.name,
            *(self._types[a.id].name for a in reversed(stype.ancestors)),
        ]

    def _get_pybase_for_scalar(
        self,
        stype: reflection.ScalarType,
        *,
        consider_abstract: bool = True,
        import_time: ImportTime = ImportTime.runtime,
        localns: frozenset[str] | None = None,
    ) -> list[str]:
        base_type = _qbmodel.get_py_type_for_scalar_hierarchy(
            self._get_scalar_hierarchy(stype),
            consider_abstract=consider_abstract,
        )
        if not base_type:
            raise AssertionError(
                f"could not find Python base type for scalar type {stype.name}"
            )
        else:
            return sorted(
                self.import_name(*t, import_time=import_time, localns=localns)
                for t in base_type
            )

    def _get_pybase_for_primitive_type(
        self,
        stype: reflection.PrimitiveType,
        *,
        import_time: ImportTime = ImportTime.runtime,
        localns: frozenset[str] | None = None,
    ) -> str:
        if reflection.is_scalar_type(stype):
            if stype.enum_values:
                return self.import_name("builtins", "str")
            else:
                return " | ".join(
                    self._get_pybase_for_scalar(
                        stype,
                        import_time=import_time,
                        localns=localns,
                    )
                )
        elif reflection.is_array_type(stype):
            el_type = self._types[stype.array_element_id]
            if reflection.is_primitive_type(el_type):
                el = self._get_pybase_for_primitive_type(
                    el_type,
                    import_time=import_time,
                    localns=localns,
                )
            else:
                el = self.get_type(el_type, import_time=import_time)
            lst = self.import_name("builtins", "list")
            return f"{lst}[{el}]"
        elif reflection.is_range_type(stype):
            el_type = self._types[stype.range_element_id]
            if reflection.is_primitive_type(el_type):
                el = self._get_pybase_for_primitive_type(
                    el_type, import_time=import_time, localns=localns
                )
            else:
                el = self.get_type(el_type, import_time=import_time)
            rng = self.import_name("gel", "Range")
            return f"{rng}[{el}]"
        elif reflection.is_multi_range_type(stype):
            el_type = self._types[stype.multirange_element_id]
            if reflection.is_primitive_type(el_type):
                el = self._get_pybase_for_primitive_type(
                    el_type, import_time=import_time, localns=localns
                )
            else:
                el = self.get_type(el_type, import_time=import_time)
            rng = self.import_name("gel", "MultiRange")
            return f"{rng}[{el}]"
        elif reflection.is_named_tuple_type(stype) or reflection.is_tuple_type(
            stype
        ):
            elems = []
            for elem in stype.tuple_elements:
                el_type = self._types[elem.type_id]
                if reflection.is_primitive_type(el_type):
                    el = self._get_pybase_for_primitive_type(
                        el_type, import_time=import_time, localns=localns
                    )
                else:
                    el = self.get_type(
                        el_type, import_time=import_time, localns=localns
                    )
                elems.append(el)
            tup = self.import_name("builtins", "tuple")
            tup_vars = self.format_list("[{list}]", elems)
            return f"{tup}{tup_vars}"

        raise AssertionError(f"unhandled primitive type: {stype.kind}")

    def write_scalar_types(
        self,
        scalar_types: dict[str, reflection.ScalarType],
    ) -> None:
        with self.aspect(ModuleAspect.VARIANTS):
            scalars: list[reflection.ScalarType] = []
            for scalar in self._sorted_types(scalar_types.values()):
                type_name = reflection.parse_name(scalar.name)
                scalars.append(scalar)
                self.py_file.add_global(type_name.name)

            for stype in scalars:
                self._write_scalar_type(stype)

        for stype in scalars:
            classname = self.get_type(
                stype,
                aspect=ModuleAspect.VARIANTS,
                import_time=ImportTime.late_runtime,
                import_directly=True,
            )
            self.export(classname)

    def _write_enum_scalar_type(
        self,
        stype: reflection.ScalarType,
    ) -> None:
        type_name = reflection.parse_name(stype.name)
        tname = type_name.name
        assert stype.enum_values
        anyenum = self.import_name(BASE_IMPL, "AnyEnum")
        with self._class_def(tname, [anyenum]):
            self.write_description(stype)
            for value in stype.enum_values:
                self.write(f"{ident(value)} = {value!r}")
        self.write_section_break()

    def _write_scalar_type(
        self,
        stype: reflection.ScalarType,
    ) -> None:
        if stype.enum_values:
            self._write_enum_scalar_type(stype)
        else:
            self._write_regular_scalar_type(stype)

    def _write_regular_scalar_type(
        self,
        stype: reflection.ScalarType,
    ) -> None:
        type_name = reflection.parse_name(stype.name)
        tname = type_name.name
        pybase = self._get_pybase_for_this_scalar(
            stype,
            require_subclassable=True,
            consider_abstract=False,
        )
        if pybase is not None:
            real_pybase = self._get_pybase_for_this_scalar(
                stype,
                consider_abstract=False,
            )
            assert real_pybase is not None
            assert len(real_pybase) == 1
            pts = self.import_name(BASE_IMPL, "PyTypeScalar")
            typecheck_parents = [f"{pts}[{real_pybase[0]}]"]
            runtime_parents = [*pybase, *typecheck_parents]
        else:
            typecheck_parents = []
            runtime_parents = []

        scalar_bases = [
            self.get_type(self._types[base.id]) for base in stype.bases
        ]

        typecheck_parents.extend(scalar_bases)
        runtime_parents.extend(scalar_bases)

        if not runtime_parents:
            typecheck_parents = [self.import_name(BASE_IMPL, "BaseScalar")]
            runtime_parents = typecheck_parents

        bin_ops = self._operators.binary_ops.get(stype.id, [])
        un_ops = self._operators.unary_ops.get(stype.id, [])

        self.write()
        if scalar_bases:
            meta_bases = _map_name(
                lambda n: f"__{n}_meta__",
                scalar_bases,
            )
        else:
            gel_type_meta = self.import_name(BASE_IMPL, "GelTypeMeta")
            meta_bases = [gel_type_meta]

        tmeta = f"__{tname}_meta__"
        with self._class_def(tmeta, meta_bases):
            if not bin_ops and not un_ops:
                self.write("pass")
            else:
                self.write_unary_operator_overloads(un_ops)
                self.write_binary_operator_overloads(bin_ops)

        with self.type_checking():
            with self._class_def(
                tname, typecheck_parents, class_kwargs={"metaclass": tmeta}
            ):
                self._write_type_reflection(stype)

        self.write()

        with self.not_type_checking():
            classvar = self.import_name(
                "typing", "ClassVar", import_time=ImportTime.typecheck
            )
            with self._class_def(tname, runtime_parents):
                self.write(f"__gel_type_class__: {classvar}[type] = {tmeta}")
                self.write()
                self._write_type_reflection(stype)

        self.write_section_break()

    def _write_type_reflection(
        self,
        stype: reflection.AnyType,
    ) -> None:
        uuid = self.import_name("uuid", "UUID")
        schemapath = self.import_name(BASE_IMPL, "SchemaPath")
        type_name = reflection.parse_name(stype.name)
        if isinstance(stype, reflection.InheritingType):
            base_types = [
                self.get_type(self._types[base.id]) for base in stype.bases
            ]
        else:
            base_types = []
        with self._class_def(
            "__gel_reflection__",
            _map_name(
                lambda s: f"{s}.__gel_reflection__",
                base_types,
            ),
        ):
            self.write(f"id = {uuid}(int={stype.id.int})")
            schema_path = ", ".join(repr(p) for p in type_name.parts)
            self.write(f"name = {schemapath}({schema_path})")

    def render_callable_return_type(
        self,
        tp: reflection.AnyType,
        typemod: reflection.TypeModifier,
        *,
        default: str | None = None,
        import_time: ImportTime = ImportTime.typecheck,
        allow_typevars: bool = True,
    ) -> str:
        result = self.get_type(
            tp,
            import_time=import_time,
            allow_typevars=allow_typevars,
        )
        type_ = self.import_name("builtins", "type", import_time=import_time)
        return f"{type_}[{result}]"

    def render_callable_runtime_return_type(
        self,
        tp: reflection.AnyType,
        typemod: reflection.TypeModifier,
        *,
        default: str | None = None,
        import_time: ImportTime = ImportTime.late_runtime,
    ) -> str:
        return self.get_type(
            tp,
            import_time=import_time,
            allow_typevars=False,
        )

    def render_callable_sig_type(
        self,
        tp: reflection.AnyType,
        typemod: reflection.TypeModifier,
        default: str | None = None,
        *,
        include_pybase: bool = False,
    ) -> str:
        result = self.get_type(tp, import_time=ImportTime.typecheck)
        type_ = self.import_name(
            "builtins", "type", import_time=ImportTime.typecheck
        )
        result = f"{type_}[{result}]"

        if include_pybase and reflection.is_primitive_type(tp):
            pybase = self._get_pybase_for_primitive_type(
                tp,
                import_time=ImportTime.typecheck,
            )
            result = f"{result} | {pybase}"

        if typemod == reflection.TypeModifier.Optional:
            result = f"{result} | None"
        if default is not None:
            unspec_t = self.import_name(BASE_IMPL, "UnspecifiedType")
            unspec = self.import_name(BASE_IMPL, "Unspecified")
            result = f"{result} | {unspec_t} = {unspec}"

        return result

    def write_unary_operator_overloads(
        self,
        ops: list[reflection.Operator],
    ) -> None:
        aexpr = self.import_name(BASE_IMPL, "AnnotatedExpr")
        pfxop = self.import_name(BASE_IMPL, "PrefixOp")
        for op in ops:
            if op.py_magic is None:
                raise AssertionError(f"expected {op} to have py_magic set")
            if op.operator_kind != reflection.OperatorKind.Prefix:
                raise AssertionError(f"expected {op} to be a prefix operator")
            ret_type = self._types[op.return_type.id]
            rtype = self.render_callable_return_type(
                ret_type, op.return_typemod
            )
            rtype_rt = self.render_callable_runtime_return_type(
                ret_type, op.return_typemod
            )
            with self._method_def(
                op.py_magic,
                ["cls"],
                rtype,
                implicit_param=False,
            ):
                name = reflection.parse_name(op.name)
                args = [
                    "expr=cls",
                    f'op="{name.name}"',
                    f"type_={self._render_obj_schema_path(ret_type)}",
                ]
                opexpr = self.format_list(f"{pfxop}({{list}})", args)
                self.write(
                    self.format_list(
                        f"return {aexpr}({{list}})",
                        [rtype_rt, opexpr],
                        first_line_comment="type: ignore [return-value]",
                    )
                )
            self.write()

    def _render_obj_schema_path(
        self,
        obj: reflection.AnyType,
    ) -> str:
        type_name = reflection.parse_name(obj.name)
        type_path = ", ".join(repr(p) for p in type_name.parts)
        schemapath = self.import_name(BASE_IMPL, "SchemaPath")
        return f"{schemapath}({type_path})"

    def write_binary_operator_overloads(
        self,
        ops: list[reflection.Operator],
    ) -> None:
        opmap: defaultdict[
            tuple[str, str],
            defaultdict[
                tuple[reflection.AnyType, reflection.TypeModifier],
                set[reflection.AnyType],
            ],
        ] = defaultdict(lambda: defaultdict(set))

        aexpr = self.import_name(BASE_IMPL, "AnnotatedExpr")
        infxop = self.import_name(BASE_IMPL, "InfixOp")
        type_ = self.import_name("builtins", "type")
        any_ = self.import_name("typing", "Any")

        explicit_rparams: defaultdict[str, set[uuid.UUID]] = defaultdict(set)
        for op in ops:
            opname = reflection.parse_name(op.name).name
            explicit_rparams[opname].add(op.params[1].type.id)

        for op in ops:
            if op.py_magic is None:
                raise AssertionError(f"expected {op} to have py_magic set")
            if op.operator_kind != reflection.OperatorKind.Infix:
                raise AssertionError(f"expected {op} to be an infix operator")

            opname = reflection.parse_name(op.name).name
            ret_type = self._types[op.return_type.id]
            right_param = op.params[1]
            right_type_id = right_param.type.id
            right_type = self._types[right_type_id]
            implicit_casts = self._casts.implicit_casts_to.get(right_type_id)
            union = [right_type]
            if implicit_casts:
                union.extend(
                    self._types[ic]
                    for ic in set(implicit_casts) - explicit_rparams[opname]
                )

            op_key = op.py_magic, opname
            r_key = ret_type, op.return_typemod
            opmap[op_key][r_key].update(union)

        py_cast_rankings: defaultdict[
            tuple[str, str],
            dict[
                tuple[str, str],
                tuple[tuple[int, int], int, reflection.ScalarType],
            ],
        ] = defaultdict(dict)

        for op_key, overloads in opmap.items():
            explicit = explicit_rparams[op_key[1]]
            py_cast_ranking = py_cast_rankings[op_key]

            for i, params in enumerate(overloads.values()):
                for stype in params:
                    if reflection.is_scalar_type(stype):
                        py_types = _qbmodel.get_py_type_for_scalar_hierarchy(
                            self._get_scalar_hierarchy(stype),
                            consider_abstract=True,
                        )
                        cast_rank = int(stype.id not in explicit)

                        for py_type in py_types:
                            scalar_rank = (
                                _qbmodel.get_py_type_scalar_match_rank(
                                    py_type, stype.name
                                )
                            )
                            if scalar_rank is None:
                                # No unabiguous cast from py to db,
                                # e.g `local_datetime -> datetime -> ?`
                                continue
                            assert scalar_rank is not None
                            rank = (cast_rank, scalar_rank)
                            prev = py_cast_ranking.get(py_type)
                            if prev is None or prev[0] > rank:
                                py_cast_ranking[py_type] = (rank, i, stype)

        py_casts: defaultdict[
            tuple[str, str],
            defaultdict[int, dict[tuple[str, str], reflection.ScalarType]],
        ] = defaultdict(lambda: defaultdict(dict))

        for op_key, py_cast_ranking in py_cast_rankings.items():
            for py_type, (
                _,
                overload_idx,
                canon_type,
            ) in py_cast_ranking.items():
                py_casts[op_key][overload_idx][py_type] = canon_type

        for op_key, overloads in opmap.items():
            meth, opname = op_key
            param_py_types_map = py_casts[op_key]
            overload = len(overloads) > 1
            for i, ((ret_type, ret_typemod), other_types) in enumerate(
                overloads.items()
            ):
                rtype = self.render_callable_return_type(
                    ret_type,
                    ret_typemod,
                )
                rtype_rt = self.render_callable_runtime_return_type(
                    ret_type,
                    ret_typemod,
                )
                other_type_union: list[str] = []
                param_py_types = param_py_types_map.get(i)
                for t in other_types:
                    tstr = self.get_type(t, import_time=ImportTime.typecheck)
                    other_type_union.append(f"{type_}[{tstr}]")
                    if reflection.is_primitive_type(t):
                        other_type_union.append(tstr)

                if param_py_types:
                    py_coerce_map: dict[str, str] = {}
                    for py_type, stype in param_py_types.items():
                        if proto := _qbmodel.maybe_get_protocol_for_py_type(
                            py_type
                        ):
                            ptype_sym = self.import_name(BASE_IMPL, proto)
                        else:
                            ptype_sym = self.import_name(
                                *py_type, directly=False
                            )

                        py_coerce_map[ptype_sym] = self.get_type(
                            stype, import_time=ImportTime.late_runtime
                        )
                    other_type_union.extend(py_coerce_map)
                    coerce_cases = {
                        f"{py_tname}()": f"other = {s_tname}(other)"
                        for py_tname, s_tname in py_coerce_map.items()
                    }
                else:
                    coerce_cases = None

                other_type_union.sort()
                other_type = " | ".join(other_type_union)

                with self._method_def(
                    meth,
                    ["cls", f"other: {other_type}"],
                    rtype,
                    overload=overload,
                    line_comment="type: ignore [override, unused-ignore]",
                    implicit_param=False,
                ):
                    if coerce_cases:
                        self.write("match other:")
                        with self.indented():
                            for cond, code in coerce_cases.items():
                                self.write(f"case {cond}:")
                                with self.indented():
                                    self.write(code)

                    args = [
                        "lexpr=cls",
                        f'op="{opname}"',
                        "rexpr=other",
                        f"type_={self._render_obj_schema_path(ret_type)}",
                    ]
                    self.write(
                        self.format_list(f"op = {infxop}({{list}})", args)
                    )
                    self.write(
                        self.format_list(
                            f"return {aexpr}({{list}})",
                            [rtype_rt, "op"],
                            first_line_comment="type: ignore [return-value]",
                        )
                    )
                self.write()

            if overload:
                dispatch = self.import_name(BASE_IMPL, "dispatch_overload")
                with self._method_def(
                    meth,
                    ["cls", f"*args: {any_}", f"**kwargs: {any_}"],
                    type_,
                    implicit_param=False,
                ):
                    self.write(
                        f"return {dispatch}(cls.{meth}, *args, **kwargs)"
                        f"  # type: ignore [no-any-return]"
                    )
                self.write()

    def write_object_types(
        self,
        object_types: dict[str, reflection.ObjectType],
    ) -> None:
        if not object_types:
            return

        objtypes = []
        for objtype in self._sorted_types(object_types.values()):
            objtypes.append(objtype)
            type_name = reflection.parse_name(objtype.name)
            if objtype.name not in CORE_OBJECTS:
                self.py_file.add_global(type_name.name)
            self.py_file.export(type_name.name)

        for objtype in objtypes:
            if objtype.name in CORE_OBJECTS:
                # Core objects are "base" by definition
                # so there is no reason to re-define them,
                # just import the base variant.
                self.get_type(
                    objtype,
                    aspect=ModuleAspect.VARIANTS,
                    import_time=ImportTime.late_runtime,
                    import_directly=True,
                )
            else:
                self.write_object_type(objtype)

        with self.aspect(ModuleAspect.VARIANTS):
            for objtype in objtypes:
                type_name = reflection.parse_name(objtype.name)
                self.py_file.add_global(type_name.name)

            for objtype in objtypes:
                self.write_object_type_variants(objtype)

            if self.py_files[ModuleAspect.LATE].has_content():
                rel_import = self._resolve_rel_import(
                    self.canonical_modpath / "*",
                    ModuleAspect.LATE,
                    import_directly=True,
                )
                assert rel_import is not None
                self.import_name(
                    rel_import.module,
                    rel_import.name,
                    suggested_module_alias=rel_import.module_alias,
                    import_time=ImportTime.late_runtime,
                )

    def _write_object_type_reflection(
        self,
        objtype: reflection.ObjectType,
        refl_t: str,
    ) -> None:
        uuid_t = self.import_name("uuid", "UUID")
        self.write(f"id={uuid_t}(int={objtype.id.int}),")
        self.write(f"name={objtype.name!r},")
        self.write(f"builtin={objtype.builtin!r},")
        self.write(f"internal={objtype.internal!r},")
        self.write(f"abstract={objtype.abstract!r},")
        self.write(f"final={objtype.final!r},")
        self.write(f"compound_type={objtype.compound_type!r},")

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
            gmm = self.import_name(BASE_IMPL, "GelTypeMetadata")
            typeof_bases = [gmm]

        pointers = objtype.pointers
        sp = self.import_name(BASE_IMPL, "SchemaPath")
        lazyclassproperty = self.import_name(BASE_IMPL, "LazyClassProperty")
        objecttype_t = self.get_type(
            self._schema_object_type,
            aspect=ModuleAspect.MAIN,
            import_time=ImportTime.typecheck,
        )
        objecttype_import = self._resolve_rel_import(
            reflection.parse_name(self._schema_object_type.name),
            aspect=ModuleAspect.MAIN,
        )
        assert objecttype_import is not None
        uuid = self.import_name("uuid", "UUID")
        with self._class_def(typeof_class, typeof_bases):
            with self._class_def(
                "__gel_reflection__",
                _map_name(
                    lambda s: f"{_mangle_typeof(s)}.__gel_reflection__",
                    base_types,
                ),
            ):
                self.write(f"id = {uuid}(int={objtype.id.int})")
                schema_path = ", ".join(repr(p) for p in type_name.parts)
                self.write(f"name = {sp}({schema_path})")
                with self._classmethod_def(
                    "object",
                    [],
                    objecttype_t,
                    decorators=(f'{lazyclassproperty}["{objecttype_t}"]',),
                ):
                    objecttype, import_code = self.py_file.render_name_import(
                        objecttype_import.module,
                        objecttype_import.name,
                        suggested_module_alias=objecttype_import.module_alias,
                    )
                    self.write(import_code)
                    self.write(f"return {objecttype}(")
                    with self.indented():
                        self._write_object_type_reflection(objtype, objecttype)
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
        class_r_kwargs = {"__gel_type_id__": f"{uuid}(int={objtype.id.int})"}
        if not base_types:
            gel_model = self.import_name(BASE_IMPL, "GelModel")
            vbase_types = [gel_model]
            bin_ops = self._operators.binary_ops.get(objtype.id, [])
            un_ops = self._operators.unary_ops.get(objtype.id, [])
            gel_model_meta = self.import_name(BASE_IMPL, "GelModelMeta")
            metaclass = f"__{name}_ops__"
            with self._class_def(metaclass, [gel_model_meta]):
                if not bin_ops and not un_ops:
                    self.write("pass")
                else:
                    self.write_unary_operator_overloads(un_ops)
                    self.write_binary_operator_overloads(bin_ops)
            with self.type_checking():
                self.write(f"__{name}_meta__ = __{name}_ops__")
            with self.not_type_checking():
                self.write(f"__{name}_meta__ = {gel_model_meta}")
            class_kwargs["metaclass"] = f"__{name}_meta__"
        else:
            vbase_types = base_types

        with self._class_def(
            name,
            [typeof_class, *vbase_types],
            class_kwargs={**class_kwargs, **class_r_kwargs},
        ):
            if not base_types:
                with self.not_type_checking():
                    self.write(f"__gel_type_class__ = __{name}_ops__")
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
                            lambda s: f"{s}.__variants__.Base",
                            base_types,
                        )
                    )
                else:
                    gel_model = self.import_name(BASE_IMPL, "GelModel")
                    base_bases.append(gel_model)

                with self._class_def(
                    "Base",
                    base_bases,
                    class_kwargs=class_kwargs,
                ):
                    self._write_base_object_type_body(objtype, typeof_class)

                self.write()
                typevar = self.import_name("typing", "TypeVar")
                self.write(f'Any = {typevar}("Any", bound="{name} | Base")')

            proplinks = self._get_links_with_props(objtype)
            if base_types:
                links_bases = _map_name(lambda s: f"{s}.__links__", base_types)
            else:
                lns = self.import_name(BASE_IMPL, "LinkClassNamespace")
                links_bases = [lns]

            with self._class_def("__links__", links_bases):
                if proplinks:
                    self.write_object_type_link_variants(objtype)
                else:
                    self.write("pass")

        self.write()
        with self.not_type_checking():
            self.write(f"{name}.__variants__.Base = {name}")

        self.write()

    def _write_base_object_type_body(
        self,
        objtype: reflection.ObjectType,
        typeof_class: str,
    ) -> None:
        if objtype.name == "std::BaseObject":
            gmm = self.import_name(BASE_IMPL, "GelModelMeta")
            for ptr in objtype.pointers:
                if ptr.name == "__type__":
                    ptr_type = self.get_ptr_type(
                        objtype,
                        ptr,
                        aspect=ModuleAspect.MAIN,
                        cardinality=reflection.Cardinality.One,
                    )
                    with self._property_def(ptr.name, [], ptr_type):
                        self.write(
                            "tid = self.__class__.__gel_reflection__.id"
                        )
                        self.write(f"actualcls = {gmm}.get_class_by_id(tid)")
                        self.write(
                            "return actualcls.__gel_reflection__.object"
                            "  # type: ignore [attr-defined, no-any-return]"
                        )
                elif ptr.name == "id":
                    priv_type = self.import_name("uuid", "UUID")
                    ptr_type = self.get_ptr_type(objtype, ptr)
                    desc = self.import_name(BASE_IMPL, "IdProperty")
                    self.write(
                        f"id: {desc}[{ptr_type}, {priv_type}]"
                        f" # type: ignore [assignment]"
                    )
                self.write()

        def _filter(
            v: tuple[reflection.Pointer, reflection.ObjectType],
        ) -> bool:
            ptr, owning_objtype = v
            if ptr.name == "__type__":
                return False
            # Skip deprecated schema props (is_-prefixed).
            return not (
                owning_objtype.name.startswith("schema::")
                and ptr.name.startswith("is_")
                and ptr.is_computed
            )

        reg_pointers = list(
            filter(_filter, self._get_pointer_origins(objtype))
        )
        init_pointers = [
            (ptr, obj)
            for ptr, obj in reg_pointers
            if (ptr.name != "id" and not ptr.is_computed)
            or objtype.name.startswith("schema::")
        ]
        args = []
        if init_pointers:
            args.extend(["/", "*"])
        for ptr, org_objtype in init_pointers:
            ptr_t = self.get_ptr_type(
                org_objtype,
                ptr,
                style="arg",
                prefer_broad_target_type=True,
            )
            args.append(f"{ptr.name}: {ptr_t}")

        std_bool = self.get_type(
            self._types_by_name["std::bool"],
            import_time=ImportTime.typecheck,
        )
        builtin_bool = self.import_name("builtins", "bool", directly=False)
        builtin_str = self.import_name("builtins", "str", directly=False)
        callable_ = self.import_name("collections.abc", "Callable")
        self_ = self.import_name("typing_extensions", "Self")
        type_ = self.import_name("builtins", "type")
        tuple_ = self.import_name("builtins", "tuple")
        type_self = f"{type_}[{self_}]"
        expr_proto = self.import_name(BASE_IMPL, "ExprCompatible")
        py_const = self.import_name(BASE_IMPL, "PyConstType")
        expr_closure = f"{callable_}[[{type_self}], {expr_proto}]"
        pathalias = self.import_name(BASE_IMPL, "PathAlias")
        filter_args = [
            "/",
            f"*exprs: {callable_}[[{type_self}], {type_}[{std_bool}]]",
        ]
        select_args = ["/", f"*exprs: {pathalias}"]
        update_args = []
        direction = (
            f"{self.import_name(BASE_IMPL, 'Direction')} | {builtin_str}"
        )
        empty_direction = (
            f"{self.import_name(BASE_IMPL, 'EmptyDirection')} | {builtin_str}"
        )
        order_expr = " | ".join(
            (
                expr_closure,
                f"{tuple_}[{expr_closure}, {direction}]",
                f"{tuple_}[{expr_closure}, {direction}, {empty_direction}]",
            )
        )
        order_args = ["/", f"*exprs: {order_expr}"]
        if reg_pointers:
            unspec_t = self.import_name(BASE_IMPL, "UnspecifiedType")
            unspec = self.import_name(BASE_IMPL, "Unspecified")

            order_kwarg_t = " | ".join(
                (
                    direction,
                    builtin_str,
                    builtin_bool,
                    f"{tuple_}[{direction}, {empty_direction}]",
                    unspec_t,
                )
            )

            for ptr, _ in reg_pointers:
                target_t = self._types[ptr.target_id]
                narrow_ptr_t = self.get_type(
                    target_t,
                    import_time=ImportTime.typecheck,
                )
                union = []
                select_union = [builtin_bool, expr_closure, expr_proto]
                if reflection.is_non_enum_scalar_type(target_t):
                    broad_ptr_t = self._get_pybase_for_scalar(target_t)
                    union.extend(broad_ptr_t)
                    order_args.append(
                        f"{ptr.name}: {order_kwarg_t} = {unspec}"
                    )
                union.extend((f"type[{narrow_ptr_t}]", unspec_t))
                select_union.extend((f"type[{narrow_ptr_t}]", unspec_t))
                ptr_t = f"{' | '.join(union)} = {unspec}"
                if not ptr.is_readonly and not ptr.is_computed:
                    update_args.append(f"{ptr.name}: {ptr_t}")
                filter_args.append(f"{ptr.name}: {ptr_t}")
                select_ptr_t = f"{' | '.join(select_union)} = {unspec}"
                select_args.append(f"{ptr.name}: {select_ptr_t}")

        select_args.append(
            f"**computed: {expr_closure} | {expr_proto} | {py_const}"
        )

        if update_args:
            update_args = ["/", "*", *update_args]

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

            self_ = self.import_name("typing_extensions", "Self")
            with self._classmethod_def(
                "update",
                update_args,
                f"type[{self_}]",
                # Ignore override errors, because we type select **computed
                # as type[GelType], which is incompatible with bool and
                # UnspecifiedType.
                line_comment="type: ignore [override, unused-ignore]",
            ):
                self.write(
                    f'"""Update {objtype.name} instances in the database.'
                )
                self.write('"""')
                self.write("...")
                self.write()

            self_ = self.import_name("typing_extensions", "Self")
            with self._classmethod_def(
                "select",
                select_args,
                f"type[{self_}]",
                # Ignore override errors, because we type select **computed
                # as type[GelType], which is incompatible with bool and
                # UnspecifiedType.
                line_comment="type: ignore [override, unused-ignore]",
            ):
                self.write(
                    f'"""Fetch {objtype.name} instances from the database.'
                )
                self.write('"""')
                self.write("...")
                self.write()

            with self._classmethod_def(
                "filter",
                filter_args,
                "type[Self]",
                line_comment="type: ignore [override, unused-ignore]",
            ):
                self.write(
                    f'"""Fetch {objtype.name} instances from the database.'
                )
                self.write('"""')
                self.write("...")
                self.write()

            with self._classmethod_def(
                "order_by",
                order_args,
                "type[Self]",
                line_comment="type: ignore [override, unused-ignore]",
            ):
                self.write('"""Specify the sort order for the selection"""')
                self.write("...")
                self.write()

            if objtype.name == "std::BaseObject":
                int64_t = self._types_by_name["std::int64"]
                assert reflection.is_scalar_type(int64_t)
                type_ = self.import_name("builtins", "type")
                std_int = self.get_type(
                    int64_t,
                    import_time=ImportTime.typecheck,
                )

                builtins_int = self._get_pybase_for_primitive_type(int64_t)

                splice_args = [f"value: {type_}[{std_int}] | {builtins_int}"]
                with self._classmethod_def("limit", splice_args, "type[Self]"):
                    self.write(
                        '"""Limit selection to a set number of entries."""'
                    )
                    self.write("...")
                    self.write()

                with self._classmethod_def(
                    "offset", splice_args, "type[Self]"
                ):
                    self.write('"""Start selection from a specific offset."""')
                    self.write("...")
                    self.write()

        if objtype.name == "schema::ObjectType":
            any_ = self.import_name("typing", "Any")
            with (
                self.not_type_checking(),
                self._method_def("__init__", ["/", f"**kwargs: {any_}"]),
            ):
                self.write('_id = kwargs.pop("id", None)')
                self.write("super().__init__(**kwargs)")
                self.write('object.__setattr__(self, "id", _id)')
            self.write()

    def _get_links_with_props(
        self,
        objtype: reflection.ObjectType,
        *,
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

            return local is None or local == (
                target_type_name.parent == type_name.parent
            )

        return list(filter(_filter, objtype.pointers))

    def write_object_type_link_variants(
        self,
        objtype: reflection.ObjectType,
        *,
        target_aspect: ModuleAspect = ModuleAspect.MAIN,
    ) -> None:
        pointers = self._get_links_with_props(objtype)
        if not pointers:
            return

        all_ptr_origins = self._get_all_pointer_origins(objtype)
        lazyclassprop = self.import_name(BASE_IMPL, "LazyClassProperty")
        type_ = self.import_name("builtins", "type")

        with self.type_checking():
            for pointer in pointers:
                self._write_object_type_link_variant(
                    objtype,
                    pointer=pointer,
                    ptr_origins=all_ptr_origins[pointer.name],
                    target_aspect=target_aspect,
                    is_forward_decl=True,
                )

        with self.not_type_checking():
            type_name = reflection.parse_name(objtype.name)
            obj_class = type_name.name
            for pointer in pointers:
                ptrname = pointer.name
                with self._classmethod_def(
                    ptrname,
                    [],
                    "type",
                    decorators=[f"{lazyclassprop}[{type_}]"],
                ):
                    classname = self._write_object_type_link_variant(
                        objtype,
                        pointer=pointer,
                        ptr_origins=all_ptr_origins[pointer.name],
                        target_aspect=target_aspect,
                        is_forward_decl=False,
                    )
                    self.write(f"{classname}.__name__ = {ptrname!r}")
                    qualname = f"{obj_class}.{ptrname}"
                    self.write(f"{classname}.__qualname__ = {qualname!r}")
                    self.write(f"return {classname}")

    def _write_object_type_link_variant(
        self,
        objtype: reflection.ObjectType,
        *,
        pointer: reflection.Pointer,
        ptr_origins: list[reflection.ObjectType],
        target_aspect: ModuleAspect | None = None,
        is_forward_decl: bool = False,
    ) -> str:
        type_name = reflection.parse_name(objtype.name)
        name = type_name.name

        self_t = self.import_name("typing_extensions", "Self")
        classvar_t = self.import_name("typing", "ClassVar")
        proxymodel_t = self.import_name(BASE_IMPL, "ProxyModel")
        prop_desc_t = self.import_name(BASE_IMPL, "LinkPropsDescriptor")
        priv_attr = self.import_name(BASE_IMPL, "PrivateAttr")

        if target_aspect is None:
            target_aspect = self.current_aspect

        ptr = pointer
        pname = ptr.name
        target_type = self._types[ptr.target_id]
        import_time = (
            ImportTime.typecheck
            if is_forward_decl
            else ImportTime.late_runtime
        )
        target = self.get_type(
            target_type,
            import_time=import_time,
            aspect=target_aspect,
        )

        ptr_origin_types = [
            self.get_type(
                origin,
                import_time=ImportTime.typecheck,
                aspect=self.current_aspect,
            )
            for origin in ptr_origins
        ]

        classname = pname if is_forward_decl else f"{name}__{pname}"
        with self._class_def(
            classname,
            (
                [f"{s}.__links__.{pname}" for s in ptr_origin_types]
                + [target, f"{proxymodel_t}[{target}]"]
            ),
        ):
            self.write(
                f'"""link {objtype.name}.{pname}: {target_type.name}"""'
            )

            if ptr_origin_types:
                lprops_bases = _map_name(
                    functools.partial(
                        lambda s, pn: f"{s}.__links__.{pn}.__lprops__",
                        pn=pname,
                    ),
                    ptr_origin_types,
                )
            else:
                b = self.import_name(BASE_IMPL, "GelLinkModel")
                lprops_bases = [b]

            with self._class_def("__lprops__", lprops_bases):
                assert ptr.pointers
                lprops = []
                lprop_assign = []
                for lprop in ptr.pointers:
                    if lprop.name in {"source", "target"}:
                        continue
                    lprop_assign.append(f"{lprop.name}={lprop.name}")
                    ttype = self._types[lprop.target_id]
                    assert reflection.is_scalar_type(ttype)
                    ptr_type = self.get_type(ttype, import_time=import_time)
                    pytype = " | ".join(self._get_pybase_for_scalar(ttype))
                    py_anno = self._py_anno_for_ptr(
                        lprop,
                        ptr_type,
                        pytype,
                        reflection.Cardinality(lprop.card),
                    )
                    lprop_line = f"{lprop.name}: {py_anno}"
                    self.write(lprop_line)
                    lprop_line = f"{lprop.name}: {pytype} | None = None"
                    lprops.append(lprop_line)

            self.write()
            self.write(
                f"__linkprops__: {classvar_t}[{prop_desc_t}[__lprops__]]"
                f" = {prop_desc_t}()"
            )
            self.write(f"_p__obj__: {target} = {priv_attr}()")
            self.write()
            args = [f"obj: {target}", "/", "*", *lprops]
            with self._method_def("__init__", args):
                if is_forward_decl:
                    self.write("...")
                else:
                    obj = self.import_name("builtins", "object")
                    self.write(f"{proxymodel_t}.__init__(self, obj)")
                    self.write(
                        self.format_list(
                            "lprops = self.__class__.__lprops__({list})",
                            lprop_assign,
                        )
                    )
                    self.write(
                        f'{obj}.__setattr__(self, "__linkprops__", lprops)'
                    )

            self.write()
            args = [f"obj: {target}", "/", "*", *lprops]
            with self._classmethod_def("link", args, self_t):
                if is_forward_decl:
                    self.write("...")
                else:
                    self.write(
                        self.format_list(
                            "return cls({list})",
                            ["obj", *lprop_assign],
                        ),
                    )

            self.write()

        return classname

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
        for base_ref in objtype.bases:
            base_type = self._types[base_ref.id]
            if base_type.name in CORE_OBJECTS:
                continue
            else:
                base_types.append(self.get_type(base_type))

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
                localns = frozenset(ptr.name for ptr in pointers)
                for ptr in pointers:
                    ptr_type = self.get_ptr_type(
                        objtype,
                        ptr,
                        aspect=ModuleAspect.MAIN,
                        localns=localns,
                    )
                    self.write(f"{ptr.name}: {ptr_type}")
            else:
                self.write("pass")
                self.write()

    def write_functions(self, functions: list[reflection.Function]) -> None:
        param_map: defaultdict[str, defaultdict[str, set[uuid.UUID]]] = (
            defaultdict(lambda: defaultdict(set))
        )
        funcmap: defaultdict[str, list[reflection.Function]] = defaultdict(
            list
        )
        for function in functions:
            for param in function.params:
                param_map[function.name][param.name].add(param.type.id)
            funcname = reflection.parse_name(function.name)
            funcmap[funcname.name].append(function)

        for fname, overloads in funcmap.items():
            self._write_function(fname, overloads, param_map)

    def _write_function(
        self,
        fname: str,
        overloads: list[reflection.Function],
        param_map: defaultdict[str, defaultdict[str, set[uuid.UUID]]],
    ) -> None:
        if len(overloads) > 1:
            for overload in overloads:
                self._write_function_overload(
                    overload, param_map, overload=True
                )
            any_ = self.import_name(
                "typing", "Any", import_time=ImportTime.typecheck
            )
            gel_type_t = self.import_name(
                BASE_IMPL, "GelType", import_time=ImportTime.typecheck
            )
            type_t = self.import_name(
                "builtins", "type", import_time=ImportTime.typecheck
            )
            dispatch = self.import_name(BASE_IMPL, "dispatch_overload")
            with self._func_def(
                fname,
                [f"*args: {any_}", f"**kwargs: {any_}"],
                f"{type_t}[{gel_type_t}]",
            ):
                self.write(
                    f"return {dispatch}({fname}, *args, **kwargs)"
                    f"  # type: ignore [no-any-return]"
                )
            self.write()
        else:
            self._write_function_overload(
                overloads[0],
                param_map,
                overload=False,
                allow_pybase_args=True,
            )

    def _write_function_overload(
        self,
        function: reflection.Function,
        param_map: defaultdict[str, defaultdict[str, set[uuid.UUID]]],
        *,
        overload: bool,
        generic_signature: bool = False,
        allow_pybase_args: bool = False,
    ) -> None:
        name = reflection.parse_name(function.name)
        args = []
        arg_names: list[str] = []
        kwargs = []
        kwarg_names: list[str] = []
        variadic = None
        for param in function.params:
            pt = self.render_callable_sig_type(
                self._types[param.type.id],
                param.typemod,
                param.default,
                include_pybase=allow_pybase_args,
            )
            param_decl = f"{param.name}: {pt}"
            if param.kind == reflection.CallableParamKind.Positional:
                args.append(param_decl)
                arg_names.append(param.name)
            elif param.kind == reflection.CallableParamKind.Variadic:
                if variadic is not None:
                    raise AssertionError(
                        f"multiple variadict parameters declared "
                        f"in function {name}"
                    )
                variadic = param_decl
            elif param.kind == reflection.CallableParamKind.NamedOnly:
                kwargs.append(param_decl)
                kwarg_names.append(param.name)
            else:
                raise AssertionError(
                    f"unexpected parameter kind in {name}: {param.kind}"
                )

        if variadic is None and kwargs:
            args.append("*")
        args.extend(kwargs)

        ret_type = self._types[function.return_type.id]
        rtype = self.render_callable_return_type(
            ret_type,
            function.return_typemod,
        )

        rtype_rt = self.render_callable_runtime_return_type(
            ret_type,
            function.return_typemod,
        )

        fname = name.name
        if keyword.iskeyword(fname):
            fname += "_"

        if overload:
            line_comment = (
                "type: ignore [overload-cannot-match, unused-ignore]"
            )
        else:
            line_comment = None

        with self._func_def(
            fname,
            args,
            rtype,
            overload=overload,
            line_comment=line_comment,
        ):
            aexpr = self.import_name(BASE_IMPL, "AnnotatedExpr")
            fcall = self.import_name(BASE_IMPL, "FuncCall")
            unsp = self.import_name(BASE_IMPL, "Unspecified")
            any_ = self.import_name("typing", "Any")
            dict_ = self.import_name("builtins", "dict")
            str_ = self.import_name("builtins", "str")
            list_ = self.import_name("builtins", "list")
            self.write(
                self.format_list(
                    f"args: {list_}[{any_}] = [{{list}}]",
                    arg_names,
                ),
            )
            self.write(
                self.format_list(
                    f"kw: {dict_}[{str_}, {any_}] = {{{{{{list}}}}}}",
                    [f'"{n}": {n}' for n in kwarg_names],
                )
            )
            self.write(f"return {aexpr}(  # type: ignore [return-value]")
            with self.indented():
                self.write(f"{rtype_rt},")
                self.write(f"{fcall}(")
                with self.indented():
                    self.write(f'fname="{function.name}",')
                    self.write(
                        f"args=[v for v in args if v is not {unsp}],",
                    )
                    self.write(
                        f"kwargs={{n: v for n, v in kw.items() "
                        f"if v is not {unsp}}},"
                    )
                    self.write(
                        f"type_={self._render_obj_schema_path(ret_type)},"
                    )
                self.write(")")
            self.write(")")

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

    def _py_anno_for_ptr(
        self,
        prop: reflection.Pointer,
        narrow_type: str,
        broad_type: str,
        cardinality: reflection.Cardinality,
    ) -> str:
        if reflection.is_link(prop):
            match (
                cardinality.is_multi(),
                cardinality.is_optional(),
                bool(prop.pointers),
                prop.is_computed,
            ):
                case True, _, True, False:
                    desc = self.import_name(BASE_IMPL, "MultiLinkWithProps")
                    pytype = f"{desc}[{narrow_type}, {broad_type}]"
                case True, _, True, True:
                    desc = self.import_name(
                        BASE_IMPL, "ComputedMultiLinkWithProps"
                    )
                    pytype = f"{desc}[{narrow_type}, {broad_type}]"
                case True, _, False, False:
                    desc = self.import_name(BASE_IMPL, "MultiLink")
                    pytype = f"{desc}[{narrow_type}]"
                case True, _, False, True:
                    desc = self.import_name(BASE_IMPL, "ComputedMultiLink")
                    pytype = f"{desc}[{narrow_type}]"
                case False, True, True, False:
                    desc = self.import_name(BASE_IMPL, "OptionalLinkWithProps")
                    pytype = f"{desc}[{narrow_type}, {broad_type}]"
                case False, True, True, True:
                    desc = self.import_name(
                        BASE_IMPL, "OptionalComputedLinkWithProps"
                    )
                    pytype = f"{desc}[{narrow_type}, {broad_type}]"
                case False, False, True, False:
                    # XXX
                    desc = self.import_name(BASE_IMPL, "RequiredLinkWithProps")
                    pytype = f"{desc}[{narrow_type}, {broad_type}]"
                case False, False, True, True:
                    # XXX
                    desc = self.import_name(
                        BASE_IMPL, "RequiredComputedLinkWithProps"
                    )
                    pytype = f"{desc}[{narrow_type}, {broad_type}]"
                case False, True, False, False:
                    desc = self.import_name(BASE_IMPL, "OptionalLink")
                    pytype = f"{desc}[{narrow_type}]"
                case False, True, False, True:
                    desc = self.import_name(BASE_IMPL, "OptionalComputedLink")
                    pytype = f"{desc}[{narrow_type}]"
                case False, False, False, False:
                    pytype = narrow_type
        elif cardinality.is_multi():
            pytype = f"list[{broad_type}]"  # XXX: this is wrong
        elif cardinality.is_optional():
            if prop.is_computed:
                desc = self.import_name(BASE_IMPL, "OptionalComputedProperty")
            else:
                desc = self.import_name(BASE_IMPL, "OptionalProperty")
            pytype = f"{desc}[{narrow_type}, {broad_type}]"
        elif prop.is_computed:
            desc = self.import_name(BASE_IMPL, "ComputedProperty")
            pytype = f"{desc}[{narrow_type}, {broad_type}]"
        else:
            pytype = narrow_type

        return pytype  # pyright: ignore [reportPossiblyUnboundVariable]  # pyright match block no bueno

    def get_ptr_type(
        self,
        objtype: reflection.ObjectType,
        prop: reflection.Pointer,
        *,
        style: Literal["annotation", "typeddict", "arg"] = "annotation",
        prefer_broad_target_type: bool = False,
        aspect: ModuleAspect | None = None,
        cardinality: reflection.Cardinality | None = None,
        localns: frozenset[str] | None = None,
    ) -> str:
        if aspect is None:
            aspect = ModuleAspect.VARIANTS

        target_type = self._types[prop.target_id]
        bare_ptr_type = ptr_type = self.get_type(
            target_type,
            aspect=aspect,
            import_time=ImportTime.late_runtime,
            localns=localns,
        )

        if reflection.is_primitive_type(target_type):
            bare_ptr_type = self._get_pybase_for_primitive_type(
                target_type,
                import_time=ImportTime.late_runtime,
                localns=localns,
            )
            union = {bare_ptr_type}
            assn_casts = self._casts.assignment_casts_to.get(
                target_type.id,
                [],
            )
            for type_id in assn_casts:
                assn_type = self._types[type_id]
                if reflection.is_primitive_type(assn_type):
                    assn_pytype = self._get_pybase_for_primitive_type(
                        assn_type,
                        import_time=ImportTime.late_runtime,
                        localns=localns,
                    )
                    union.add(assn_pytype)
            bare_ptr_type = " | ".join(sorted(union))

            if prefer_broad_target_type:
                ptr_type = bare_ptr_type

        if (
            reflection.is_link(prop)
            and prop.pointers
            and not prefer_broad_target_type
        ):
            objtype_name = reflection.parse_name(objtype.name)
            if self.current_aspect is ModuleAspect.VARIANTS:
                target_name = reflection.parse_name(target_type.name)
                if target_name.parent != objtype_name.parent:
                    aspect = ModuleAspect.LATE
            ptr_type = f"{objtype_name.name}.__links__.{prop.name}"

        if cardinality is None:
            cardinality = reflection.Cardinality(prop.card)
            # Unless explicitly requested, force link cardinality to be
            # optional, because links are not guaranteed to be fetched
            # under the standard reflection scenario.
            if reflection.is_link(prop) and not cardinality.is_optional():
                if cardinality.is_multi():
                    cardinality = reflection.Cardinality.Many
                else:
                    cardinality = reflection.Cardinality.AtMostOne

        match style:
            case "annotation":
                result = self._py_anno_for_ptr(
                    prop, ptr_type, bare_ptr_type, cardinality
                )
            case "typeddict":
                result = self._py_anno_for_ptr(
                    prop, ptr_type, bare_ptr_type, cardinality
                )
                if cardinality.is_optional():
                    nreq = self.import_name("typing_extensions", "NotRequired")
                    result = f"{nreq}[{result}]"
            case "arg":
                if cardinality.is_multi():
                    iterable = self.import_name("collections.abc", "Iterable")
                    result = f"{iterable}[{ptr_type}] = []"
                elif cardinality.is_optional():
                    result = f"{ptr_type} | None = None"
                else:
                    result = ptr_type
            case _:
                raise AssertionError(
                    f"unexpected type rendering style: {style!r}"
                )

        return result


class GeneratedGlobalModule(BaseGeneratedModule):
    def process(self, types: Mapping[uuid.UUID, reflection.AnyType]) -> None:
        graph: defaultdict[uuid.UUID, set[uuid.UUID]] = defaultdict(set)

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
        anytuple = self.import_name(BASE_IMPL, "AnyTuple")

        self.write("#")
        self.write(f"# tuple type {t.name}")
        self.write("#")
        classname = self.get_tuple_name(t)
        with self._class_def(f"_{classname}", [namedtuple]):
            for elem in t.tuple_elements:
                elem_type = self.get_type(
                    self._types[elem.type_id],
                    import_time=ImportTime.late_runtime,
                )
                self.write(f"{elem.name}: {elem_type}")
        self.write_section_break()
        with self._class_def(classname, [f"_{classname}", anytuple]):
            self.write("__slots__ = ()")
        self.write()
