# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

from __future__ import annotations
import contextlib
from typing import (
    TYPE_CHECKING,
    Literal,
    NamedTuple,
    TypedDict,
    TypeVar,
)

import base64
import collections
import dataclasses
import enum
import functools
import itertools
import json
import graphlib
import keyword
import logging
import operator
import pathlib
import tempfile
import textwrap

from collections import defaultdict
from collections.abc import MutableMapping  # noqa: TC003  # pydantic needs it
from contextlib import contextmanager

import gel
from gel import abstract
from gel import _version as _gel_py_ver
from gel._internal import _cache
from gel._internal import _dataclass_extras
from gel._internal import _dirsync
from gel._internal import _reflection as reflection
from gel._internal._qbmodel import _abstract as _qbmodel
from gel._internal._reflection._enums import SchemaPart

from .._generator import C, AbstractCodeGenerator
from .._module import ImportTime, CodeSection, GeneratedModule

if TYPE_CHECKING:
    import io

    from collections.abc import (
        Callable,
        Collection,
        Generator,
        Iterable,
        Iterator,
        Mapping,
        Sequence,
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


@dataclasses.dataclass(kw_only=True, frozen=True)
class Schema:
    types: MutableMapping[str, reflection.AnyType]
    casts: reflection.CastMatrix
    operators: reflection.OperatorMatrix
    functions: list[reflection.Function]


class PydanticModelsGenerator(AbstractCodeGenerator):
    def run(self) -> None:
        try:
            self._client.ensure_connected()
        except gel.EdgeDBError:
            logger.exception("could not connect to Gel instance")
            self.abort(61)

        models_root = self._project_dir / self._args.output
        tmp_models_root = tempfile.TemporaryDirectory(
            prefix=".~tmp.models.",
            dir=self._project_dir,
        )
        file_state = self._get_last_state()

        with tmp_models_root, self._client:
            db_state = reflection.fetch_branch_state(self._client)
            std_schema: Schema | None = None

            std_gen = SchemaGenerator(
                self._client,
                reflection.SchemaPart.STD,
            )

            outdir = pathlib.Path(tmp_models_root.name)
            need_dirsync = False

            if (
                file_state is None
                or file_state.server_version != db_state.server_version
                or self._no_cache
            ):
                std_schema, std_manifest = std_gen.run(outdir)
                self._save_std_schema_cache(
                    std_schema, db_state.server_version
                )
                need_dirsync = True
            else:
                std_schema = self._load_std_schema_cache(
                    db_state.server_version,
                )
                std_manifest = std_gen.dry_run_manifest()

            if (
                file_state is None
                or file_state.server_version != db_state.server_version
                or file_state.top_migration != db_state.top_migration
                or self._no_cache
            ):
                usr_gen = SchemaGenerator(
                    self._client,
                    reflection.SchemaPart.USER,
                    std_schema=std_schema,
                )
                usr_gen.run(outdir)
                need_dirsync = True

            self._write_state(db_state, outdir)

            if need_dirsync:
                for fn in list(std_manifest):
                    # Also keep the directories
                    std_manifest.update(fn.parents)

                _dirsync.dirsync(outdir, models_root, keep=std_manifest)

        self.print_msg(f"{C.GREEN}{C.BOLD}Done.{C.ENDC}")

    def _cache_key(self, suf: str, sv: reflection.ServerVersion) -> str:
        return f"gm-c-{_gel_py_ver.__version__}-s-{sv.major}.{sv.minor}-{suf}"

    def _save_std_schema_cache(
        self, schema: Schema, sv: reflection.ServerVersion
    ) -> None:
        _cache.save_json(
            self._cache_key("std.json", sv),
            dataclasses.asdict(schema),
        )

    def _load_std_schema_cache(
        self, sv: reflection.ServerVersion
    ) -> Schema | None:
        schema_data = _cache.load_json(self._cache_key("std.json", sv))
        if schema_data is None:
            return None

        if not isinstance(schema_data, dict):
            return None

        try:
            return _dataclass_extras.coerce_to_dataclass(Schema, schema_data)
        except Exception:
            return None

    def _get_last_state(self) -> reflection.BranchState | None:
        state_json = self._project_dir / "models" / "_state.json"
        try:
            with open(state_json, encoding="utf8") as f:
                state_data = json.load(f)
        except (OSError, ValueError, TypeError):
            return None

        try:
            server_version = state_data["server_version"]
            top_migration = state_data["top_migration"]
        except KeyError:
            return None

        if (
            not isinstance(server_version, list)
            or len(server_version) != 2
            or not all(isinstance(part, int) for part in server_version)
        ):
            return None

        if not isinstance(top_migration, str):
            return None

        return reflection.BranchState(
            server_version=reflection.ServerVersion(*server_version),
            top_migration=top_migration,
        )

    def _write_state(
        self,
        state: reflection.BranchState,
        outdir: pathlib.Path,
    ) -> None:
        state_json = outdir / "_state.json"
        try:
            with open(state_json, mode="w", encoding="utf8") as f:
                json.dump(dataclasses.asdict(state), f)
        except (OSError, ValueError, TypeError):
            return None


class SchemaGenerator:
    def __init__(
        self,
        client: abstract.ReadOnlyExecutor,
        schema_part: reflection.SchemaPart,
        std_schema: Schema | None = None,
    ) -> None:
        self._client = client
        self._schema_part = schema_part
        self._basemodule = "models"
        self._modules: dict[reflection.SchemaPath, IntrospectedModule] = {}
        self._std_modules: list[reflection.SchemaPath] = []
        self._types: Mapping[str, reflection.AnyType] = {}
        self._casts: reflection.CastMatrix
        self._operators: reflection.OperatorMatrix
        self._functions: list[reflection.Function]
        self._named_tuples: dict[str, reflection.NamedTupleType] = {}
        self._wrapped_types: set[str] = set()
        self._std_schema = std_schema
        if schema_part is not SchemaPart.STD and std_schema is None:
            raise ValueError(
                "must pass std_schema when reflecting user schemas"
            )

    def dry_run_manifest(self) -> set[pathlib.Path]:
        part = self._schema_part
        std_modules: dict[reflection.SchemaPath, bool] = dict.fromkeys(
            (
                reflection.parse_name(mod)
                for mod in reflection.fetch_modules(self._client, part)
            ),
            False,
        )

        for mod in list(std_modules):
            if mod.parent:
                std_modules[mod.parent] = True

        files = set()
        for mod, has_submodules in std_modules.items():
            modpath = get_modpath(mod, ModuleAspect.MAIN)
            as_pkg = mod_is_package(modpath, part) or has_submodules
            for aspect in ModuleAspect.__members__.values():
                modpath = get_modpath(mod, aspect)
                files.add(mod_filename(modpath, as_pkg=as_pkg))

        common_modpath = get_common_types_modpath(self._schema_part)
        as_pkg = mod_is_package(common_modpath, part)
        files.add(mod_filename(common_modpath, as_pkg=as_pkg))

        return files

    def run(self, outdir: pathlib.Path) -> tuple[Schema, set[pathlib.Path]]:
        schema = self.introspect_schema()
        written: set[pathlib.Path] = set()

        written.update(self._generate_common_types(outdir))
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
            written.update(module.write_files(outdir))
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
        written.update(module.write_files(outdir))

        return schema, written

    def introspect_schema(self) -> Schema:
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
            assert self._std_schema is not None
            std_types = self._std_schema.types
            self._types = collections.ChainMap(std_types, these_types)
            std_casts = self._std_schema.casts
            self._casts = self._casts.chain(std_casts)
            std_operators = self._std_schema.operators
            self._operators = self._operators.chain(std_operators)
            self._functions = these_funcs + self._std_schema.functions
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

        return Schema(
            types=self._types,
            casts=self._casts,
            operators=self._operators,
            functions=self._functions,
        )

    def get_comment_preamble(self) -> str:
        return COMMENT

    def _generate_common_types(
        self, outdir: pathlib.Path
    ) -> set[pathlib.Path]:
        mod = get_common_types_modpath(self._schema_part)
        module = GeneratedGlobalModule(
            mod,
            all_types=self._types,
            all_casts=self._casts,
            all_operators=self._operators,
            modules=self._modules,
            schema_part=self._schema_part,
        )
        module.process(self._named_tuples)
        return module.write_files(outdir)


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


def get_common_types_modpath(
    schema_part: reflection.SchemaPart,
) -> reflection.SchemaPath:
    mod = reflection.SchemaPath("__types__")
    if schema_part is reflection.SchemaPart.STD:
        mod = reflection.SchemaPath("std") / mod
    return mod


def mod_is_package(
    mod: reflection.SchemaPath,
    schema_part: reflection.SchemaPart,
) -> bool:
    return not mod.parts or (
        schema_part is reflection.SchemaPart.STD and len(mod.parts) == 1
    )


def mod_filename(
    modpath: reflection.SchemaPath,
    *,
    as_pkg: bool,
) -> pathlib.Path:
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

    return pathlib.Path(dirpath) / filename


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


def _filter_pointers(
    pointers: Iterable[tuple[reflection.Pointer, reflection.ObjectType]],
    filters: Iterable[
        Callable[[reflection.Pointer, reflection.ObjectType], bool]
    ] = (),
    *,
    owned_only: bool = True,
    exclude_id: bool = True,
    exclude_type: bool = True,
) -> list[tuple[reflection.Pointer, reflection.ObjectType]]:
    excluded = set()
    if exclude_id:
        excluded.add("id")
    if exclude_type:
        excluded.add("__type__")
    if excluded:
        filters = [lambda ptr, obj: ptr.name not in excluded, *filters]
    else:
        filters = list(filters)

    filters.append(
        lambda ptr, obj: (
            obj.schemapath.parts[0] != "schema"
            or not ptr.name.startswith("is_")
            or not ptr.is_computed
        )
    )

    return [
        (ptr, objtype)
        for ptr, objtype in pointers
        if all(f(ptr, objtype) for f in filters)
    ]


def _get_object_type_body(
    objtype: reflection.ObjectType,
    filters: Iterable[
        Callable[[reflection.Pointer, reflection.ObjectType], bool]
    ] = (),
) -> list[reflection.Pointer]:
    return [
        p
        for p, _ in _filter_pointers(
            ((ptr, objtype) for ptr in objtype.pointers),
            filters,
        )
    ]


class BaseGeneratedModule:
    def __init__(
        self,
        modname: reflection.SchemaPath,
        *,
        all_types: Mapping[str, reflection.AnyType],
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
        return mod_is_package(mod, schema_part) or bool(self._submodules)

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
        mod_fname = mod_filename(modpath, as_pkg=as_pkg)
        # Along the dirpath we need to ensure that all packages are created
        self._init_dir(path)
        for el in mod_fname.parent.parts:
            path /= el
            self._init_dir(path)

        with open(path / mod_fname.name, "w", encoding="utf8") as f:
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

    def should_write(
        self, py_file: GeneratedModule, aspect: ModuleAspect
    ) -> bool:
        return py_file.has_content() or aspect is ModuleAspect.MAIN

    def write_files(self, path: pathlib.Path) -> set[pathlib.Path]:
        written: set[pathlib.Path] = set()
        for aspect, py_file in self.py_files.items():
            if not self.should_write(py_file, aspect):
                continue

            with self._open_py_file(
                path,
                self.modpath(aspect),
                as_pkg=self.is_package,
            ) as f:
                py_file.output(f)
                written.add(pathlib.Path(f.name).relative_to(path))

        return written

    def write_type_reflection(
        self,
        stype: reflection.AnyType,
    ) -> None:
        uuid = self.import_name("uuid", "UUID")
        schemapath = self.import_name(BASE_IMPL, "SchemaPath")
        if isinstance(stype, reflection.InheritingType):
            base_types = [
                self.get_type(self._types[base.id]) for base in stype.bases
            ]
        else:
            gmm = self.import_name(BASE_IMPL, "GelTypeMetadata")
            base_types = [gmm]
        with self._class_def(
            "__gel_reflection__",
            _map_name(
                lambda s: f"{s}.__gel_reflection__",
                base_types,
            ),
        ):
            self.write(f"id = {uuid}(int={stype.uuid.int})")
            self.write(f"name = {stype.schemapath.as_code(schemapath)}")

    def write_object_type_reflection(
        self,
        objtype: reflection.ObjectType,
        base_types: list[str],
    ) -> None:
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
        uuid_ = self.import_name("uuid", "UUID")

        if base_types:
            class_bases = base_types
        else:
            class_bases = [
                self.import_name(BASE_IMPL, "GelObjectTypeMetadata")
            ]

        with self._class_def(
            "__gel_reflection__",
            _map_name(lambda s: f"{s}.__gel_reflection__", class_bases),
        ):
            self.write(f"id = {uuid_}(int={objtype.uuid.int})")
            self.write(f"name = {objtype.schemapath.as_code(sp)}")
            self._write_pointers_reflection(objtype.pointers, base_types)

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
                    self.write(f"id={uuid_}(int={objtype.uuid.int}),")
                    self.write(f"name={objtype.name!r},")
                    self.write(f"builtin={objtype.builtin!r},")
                    self.write(f"internal={objtype.internal!r},")
                    self.write(f"abstract={objtype.abstract!r},")
                    self.write(f"final={objtype.final!r},")
                    self.write(f"compound_type={objtype.compound_type!r},")
                self.write(")")
        self.write()

    def write_link_reflection(
        self,
        link: reflection.Pointer,
        bases: list[str],
    ) -> None:
        sp = self.import_name(BASE_IMPL, "SchemaPath")
        uuid_ = self.import_name("uuid", "UUID")

        class_bases = bases or [self.import_name(BASE_IMPL, "GelLinkModel")]
        with self._class_def(
            "__gel_reflection__",
            _map_name(lambda s: f"{s}.__gel_reflection__", class_bases),
        ):
            self.write(f"id = {uuid_}(int={link.uuid.int})")
            self.write(f"name = {sp}({link.name!r})")
            self._write_pointers_reflection(link.pointers, bases)

        self.write()

    def _write_pointers_reflection(
        self,
        pointers: Sequence[reflection.Pointer] | None,
        bases: list[str],
    ) -> None:
        dict_ = self.import_name(
            "builtins", "dict", import_time=ImportTime.typecheck
        )
        str_ = self.import_name(
            "builtins", "str", import_time=ImportTime.typecheck
        )
        gel_ptr_ref = self.import_name(
            BASE_IMPL,
            "GelPointerReflection",
            import_time=ImportTime.runtime
            if pointers
            else ImportTime.typecheck,
        )
        lazyclassproperty = self.import_name(BASE_IMPL, "LazyClassProperty")
        ptr_ref_t = f"{dict_}[{str_}, {gel_ptr_ref}]"
        with self._classmethod_def(
            "pointers",
            [],
            ptr_ref_t,
            decorators=(f'{lazyclassproperty}["{ptr_ref_t}"]',),
        ):
            if pointers:
                self.write(f"my_ptrs: {ptr_ref_t} = {{")
                classes = {
                    "SchemaPath": self.import_name(BASE_IMPL, "SchemaPath"),
                    "GelPointerReflection": gel_ptr_ref,
                    "Cardinality": self.import_name(BASE_IMPL, "Cardinality"),
                    "PointerKind": self.import_name(BASE_IMPL, "PointerKind"),
                }
                with self.indented():
                    for ptr in pointers:
                        r = self._reflect_pointer(ptr, classes)
                        self.write(f"{ptr.name!r}: {r},")
                self.write("}")
            else:
                self.write(f"my_ptrs: {ptr_ref_t} = {{}}")

            if bases:
                pp = "__gel_reflection__.pointers"
                ret = self.format_list(
                    "return ({list})",
                    [
                        "my_ptrs",
                        *_map_name(lambda s: f"{s}.{pp}", bases),
                    ],
                    separator=" | ",
                    carry_separator=True,
                )
            else:
                ret = "return my_ptrs"

            self.write(ret)

        self.write()

    def _reflect_pointer(
        self,
        ptr: reflection.Pointer,
        classes: dict[str, str],
    ) -> str:
        target_type = self._types[ptr.target_id]
        kwargs: dict[str, str] = {
            "name": repr(ptr.name),
            "type": target_type.schemapath.as_code(classes["SchemaPath"]),
            "typexpr": repr(target_type.edgeql),
            "kind": f"{classes['PointerKind']}({str(ptr.kind)!r})",
            "cardinality": f"{classes['Cardinality']}({str(ptr.card)!r})",
            "computed": str(ptr.is_computed),
            "readonly": str(ptr.is_readonly),
            "has_default": str(ptr.has_default),
        }

        if ptr.pointers is not None:
            kwargs["properties"] = self.format_list(
                "{{{list}}}",
                [
                    f"{prop.name!r}: {self._reflect_pointer(prop, classes)}"
                    for prop in ptr.pointers
                ],
                extra_indent=1,
            )
        else:
            kwargs["properties"] = "None"

        return self.format_list(
            f"{classes['GelPointerReflection']}({{list}})",
            [f"{k}={v}" for k, v in kwargs.items()],
        )

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
        if module is _qbmodel.MODEL_SUBSTRATE_MODULE:
            module = BASE_IMPL

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
        digest = base64.b64encode(t.uuid.bytes[:4], altchars=b"__").decode()
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
                return self.import_name(BASE_IMPL, "AnyTuple")
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
        extra_indent: int = 0,
        separator: str = ", ",
        carry_separator: bool = False,
    ) -> str:
        return self.py_file.format_list(
            tpl,
            values,
            first_line_comment=first_line_comment,
            extra_indent=extra_indent,
            separator=separator,
            carry_separator=carry_separator,
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
        self.write_non_magic_infix_operators(
            [
                op
                for op in itertools.chain.from_iterable(
                    self._operators.binary_ops.values()
                )
                if op.schemapath.parent == self.canonical_modpath
            ]
        )

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
        graph: dict[str, set[str]] = {}
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
        base_type = _qbmodel.get_py_base_for_scalar(
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

    def _get_pytype_for_this_scalar(
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

    def _get_pytype_for_scalar(
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

    def _get_pytype_for_primitive_type(
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
                    self._get_pytype_for_scalar(
                        stype,
                        import_time=import_time,
                        localns=localns,
                    )
                )
        elif reflection.is_array_type(stype):
            el_type = self._types[stype.array_element_id]
            if reflection.is_primitive_type(el_type):
                el = self._get_pytype_for_primitive_type(
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
                el = self._get_pytype_for_primitive_type(
                    el_type, import_time=import_time, localns=localns
                )
            else:
                el = self.get_type(el_type, import_time=import_time)
            rng = self.import_name("gel", "Range")
            return f"{rng}[{el}]"
        elif reflection.is_multi_range_type(stype):
            el_type = self._types[stype.multirange_element_id]
            if reflection.is_primitive_type(el_type):
                el = self._get_pytype_for_primitive_type(
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
                    el = self._get_pytype_for_primitive_type(
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
            real_pybase = self._get_pytype_for_this_scalar(
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
                self._write_prefix_operator_methods(
                    [op for op in un_ops if op.py_magic is not None]
                )
                self._write_infix_operator_methods(bin_ops)

        with self.type_checking():
            with self._class_def(
                tname, typecheck_parents, class_kwargs={"metaclass": tmeta}
            ):
                self.write_type_reflection(stype)

        self.write()

        with self.not_type_checking():
            classvar = self.import_name(
                "typing", "ClassVar", import_time=ImportTime.typecheck
            )
            with self._class_def(tname, runtime_parents):
                self.write(f"__gel_type_class__: {classvar}[type] = {tmeta}")
                self.write()
                self.write_type_reflection(stype)

        self.write_section_break()

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
            pybase = self._get_pytype_for_primitive_type(
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

    def _write_prefix_operator_methods(
        self,
        ops: list[reflection.Operator],
    ) -> None:
        if not ops:
            # Exit early, don't generate imports we won't use.
            return

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
            meth = op.py_magic[0]
            with self._method_def(
                meth,
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

    def write_non_magic_infix_operators(
        self,
        ops: list[reflection.Operator],
    ) -> None:
        self._write_binary_operators(
            [
                (
                    (
                        op.suggested_ident or op.schemapath.name,
                        op.suggested_ident or op.schemapath.name,
                    ),
                    op,
                )
                for op in ops
                if op.py_magic is None
            ],
            style="function",
        )

    def _write_infix_operator_methods(
        self,
        ops: list[reflection.Operator],
    ) -> None:
        self._write_binary_operators(
            [(op.py_magic, op) for op in ops if op.py_magic is not None],
            style="method",
        )

    def _write_binary_operators(
        self,
        ops: list[tuple[tuple[str, ...], reflection.Operator]],
        *,
        style: Literal["method", "function"],
    ) -> None:
        opmap: defaultdict[
            tuple[tuple[str, ...], str],
            defaultdict[
                tuple[
                    frozenset[reflection.AnyType],
                    reflection.AnyType,
                    reflection.TypeModifier,
                ],
                set[reflection.AnyType],
            ],
        ] = defaultdict(lambda: defaultdict(set))

        if not ops:
            # Exit early, don't generate imports we won't use.
            return

        aexpr = self.import_name(BASE_IMPL, "AnnotatedExpr")
        expr_compat = self.import_name(BASE_IMPL, "ExprCompatible")
        type_ = self.import_name("builtins", "type")
        any_ = self.import_name("typing", "Any")

        op_map = {
            "[]": "IndexOp",
        }

        op_classes_to_import: dict[str, str] = {}

        explicit_rparams: defaultdict[str, set[str]] = defaultdict(set)
        for _, op in ops:
            explicit_rparams[op.schemapath.name].add(op.params[1].type.id)

        for fnames, op in ops:
            if op.operator_kind != reflection.OperatorKind.Infix:
                raise AssertionError(f"expected {op} to be an infix operator")

            opname = op.schemapath.name
            op_classes_to_import[opname] = op_map.get(opname, "InfixOp")
            ret_type = self._types[op.return_type.id]
            left_type = self._types[op.params[0].type.id]
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

            op_key = fnames, opname

            if style == "function" and (ex := opmap[op_key]):
                ex_r_key, right_types = next(iter(ex.items()))
                r_key = (
                    ex_r_key[0] | frozenset((left_type,)),
                    ret_type,
                    op.return_typemod,
                )
                ex.clear()
                ex[r_key] = right_types | set(union)
            else:
                r_key = frozenset((left_type,)), ret_type, op.return_typemod
                opmap[op_key][r_key].update(union)

        op_classes: dict[str, str] = {}
        imported: dict[str, str] = {}
        for opname, opclsname in op_classes_to_import.items():
            opcls = imported.get(opname)
            if opcls is None:
                opcls = self.import_name(BASE_IMPL, opclsname)
                imported[opname] = opcls
            op_classes[opname] = opcls

        py_cast_rankings: defaultdict[
            tuple[tuple[str, ...], str],
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
            tuple[tuple[str, ...], str],
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
            fnames, opname = op_key
            opcls = op_classes[opname]
            meth = ident(fnames[0])
            param_py_types_map = py_casts[op_key]
            overload = len(overloads) > 1
            swapped_overloads: list[
                tuple[
                    dict[str, str],
                    tuple[
                        frozenset[reflection.AnyType],
                        reflection.AnyType,
                        str,
                        str,
                    ],
                ]
            ] = []
            for i, (
                (self_type, ret_type, ret_typemod),
                other_types,
            ) in enumerate(overloads.items()):
                rtype = self.render_callable_return_type(
                    ret_type,
                    ret_typemod,
                )
                rtype_rt = self.render_callable_runtime_return_type(
                    ret_type,
                    ret_typemod,
                )
                other_type_union: list[str] = []
                for t in other_types:
                    tstr = self.get_type(t, import_time=ImportTime.typecheck)
                    other_type_union.append(f"{type_}[{tstr}]")
                    if reflection.is_primitive_type(t):
                        other_type_union.append(tstr)

                param_py_types = param_py_types_map.get(i)
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

                    if len(fnames) > 1:
                        swapped_overloads.append(
                            (
                                py_coerce_map,
                                (self_type, ret_type, rtype, rtype_rt),
                            )
                        )
                else:
                    coerce_cases = None

                other_type_union.sort()
                other_type = " | ".join(other_type_union)

                if style == "method":
                    defn = self._method_def(
                        meth,
                        ["cls", f"other: {other_type}"],
                        rtype,
                        overload=overload,
                        line_comment="type: ignore [override, unused-ignore]",
                        implicit_param=False,
                    )
                else:
                    self_type_str = " | ".join(
                        sorted(
                            self.get_type(
                                st,
                                import_time=ImportTime.typecheck,
                            )
                            for st in self_type
                        )
                    )
                    defn = self._func_def(
                        meth,
                        [
                            f"cls: {type_}[{self_type_str}]",
                            f"other: {other_type}",
                        ],
                        rtype,
                        overload=overload or bool(swapped_overloads),
                    )

                with defn:
                    if coerce_cases:
                        self.write("match other:")
                        with self.indented():
                            for cond, code in coerce_cases.items():
                                self.write(f"case {cond}:")
                                with self.indented():
                                    self.write(code)
                    if any(reflection.is_tuple_type(ot) for ot in other_types):
                        self.write(
                            f"rexpr: {expr_compat} = other"
                            f"  # type: ignore [assignment]"
                        )
                    else:
                        self.write(f"rexpr: {expr_compat} = other")

                    args = [
                        "lexpr=cls",
                        f'op="{opname}"',
                        "rexpr=rexpr",
                        f"type_={self._render_obj_schema_path(ret_type)}",
                    ]
                    self.write(
                        self.format_list(f"op = {opcls}({{list}})", args)
                    )
                    self.write(
                        self.format_list(
                            f"return {aexpr}({{list}})",
                            [rtype_rt, "op"],
                            first_line_comment="type: ignore [return-value]",
                        )
                    )
                self.write()

            num_swapped_overloads = len(swapped_overloads)
            rmeth = ident(fnames[1]) if len(fnames) > 1 else meth
            if overload and (meth != rmeth or num_swapped_overloads == 0):
                dispatch = self.import_name(BASE_IMPL, "dispatch_overload")
                if style == "method":
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
                else:
                    with self._func_def(
                        meth,
                        [f"*args: {any_}", f"**kwargs: {any_}"],
                        type_,
                    ):
                        self.write(
                            f"return {dispatch}({meth}, *args, **kwargs)"
                            f"  # type: ignore [no-any-return]"
                        )

                self.write()

            if num_swapped_overloads > 0:
                expr_compat = self.import_name(BASE_IMPL, "ExprCompatible")
                swapped_overload = num_swapped_overloads > 1 or rmeth == meth
                for py_coerce_map, (
                    self_type,
                    ret_type,
                    rtype,
                    rtype_rt,
                ) in swapped_overloads:
                    rev_op_type = " | ".join(sorted(py_coerce_map))

                    if style == "method":
                        defn = self._method_def(
                            rmeth,
                            ["cls", f"other: {rev_op_type}"],
                            rtype,
                            overload=swapped_overload,
                            line_comment=(
                                "type: ignore [override, unused-ignore]"
                            ),
                            implicit_param=False,
                        )
                    else:
                        self_type_str = " | ".join(
                            sorted(
                                self.get_type(
                                    st,
                                    import_time=ImportTime.typecheck,
                                )
                                for st in self_type
                            )
                        )
                        defn = self._func_def(
                            rmeth,
                            [
                                f"other: {rev_op_type}",
                                f"cls: {type_}[{self_type_str}]",
                            ],
                            rtype,
                            overload=True,
                        )

                    with defn:
                        self.write(f"operand: {expr_compat}")
                        coerce_cases = {
                            f"{py_tname}()": f"operand = {s_tname}(other)"
                            for py_tname, s_tname in py_coerce_map.items()
                        }
                        coerce_cases["_"] = "operand = other"
                        self.write("match other:")
                        with self.indented():
                            for cond, code in coerce_cases.items():
                                self.write(f"case {cond}:")
                                with self.indented():
                                    self.write(code)

                        args = [
                            "lexpr=operand",
                            f'op="{opname}"',
                            "rexpr=cls",
                            f"type_={self._render_obj_schema_path(ret_type)}",
                        ]
                        self.write(
                            self.format_list(f"op = {opcls}({{list}})", args)
                        )
                        self.write(
                            self.format_list(
                                f"return {aexpr}({{list}})",
                                [rtype_rt, "op"],
                                first_line_comment=(
                                    "type: ignore [return-value]"
                                ),
                            )
                        )
                    self.write()

                if swapped_overload:
                    dispatch = self.import_name(BASE_IMPL, "dispatch_overload")
                    if style == "method":
                        with self._method_def(
                            rmeth,
                            ["cls", f"*args: {any_}", f"**kwargs: {any_}"],
                            type_,
                            implicit_param=False,
                        ):
                            self.write(
                                f"return {dispatch}(cls.{meth}, *args,"
                                f" **kwargs)  # type: ignore [no-any-return]"
                            )
                    else:
                        with self._func_def(
                            rmeth,
                            [f"*args: {any_}", f"**kwargs: {any_}"],
                            type_,
                        ):
                            self.write(
                                f"return {dispatch}({meth}, *args, **kwargs)"
                                f"  # type: ignore [no-any-return]"
                            )
                    self.write()

    def _render_obj_schema_path(
        self,
        obj: reflection.AnyType,
    ) -> str:
        schemapath = self.import_name(BASE_IMPL, "SchemaPath")
        return obj.schemapath.as_code(schemapath)

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

    def write_object_type_variants(
        self,
        objtype: reflection.ObjectType,
    ) -> None:
        self.write()
        self.write()
        self.write("#")
        self.write(f"# type {objtype.name}")
        self.write("#")

        type_name = objtype.schemapath
        name = type_name.name

        def _mangle_typeof_base(name: str) -> str:
            return f"__{name}_typeof_base__"

        base_types = [
            self.get_type(
                self._types[base.id],
                aspect=ModuleAspect.VARIANTS,
            )
            for base in objtype.bases
        ]
        typeof_base_class = _mangle_typeof_base(name)
        if base_types:
            typeof_base_bases = _map_name(_mangle_typeof_base, base_types)
            reflection_bases = typeof_base_bases
        else:
            gmm = self.import_name(BASE_IMPL, "GelObjectTypeMetadata")
            typeof_base_bases = [gmm]
            reflection_bases = []

        pointers = objtype.pointers
        objecttype_import = self._resolve_rel_import(
            reflection.parse_name(self._schema_object_type.name),
            aspect=ModuleAspect.MAIN,
        )
        assert objecttype_import is not None
        uuid = self.import_name("uuid", "UUID")
        with self._class_def(typeof_base_class, typeof_base_bases):
            self.write_object_type_reflection(objtype, reflection_bases)

        def _mangle_typeof(name: str) -> str:
            return f"__{name}_typeof__"

        typeof_class = _mangle_typeof(name)
        if base_types:
            typeof_bases = _map_name(_mangle_typeof, base_types)
        else:
            typeof_bases = []

        typeof_bases.append(typeof_base_class)

        with self._class_def(typeof_class, typeof_bases):
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

        def _mangle_typeof_partial(name: str) -> str:
            return f"__{name}_typeof_partial__"

        typeof_partial_class = _mangle_typeof_partial(name)
        if base_types:
            typeof_partial_bases = _map_name(
                _mangle_typeof_partial, base_types
            )
        else:
            typeof_partial_bases = []

        typeof_partial_bases.append(typeof_base_class)

        with self._class_def(typeof_partial_class, typeof_partial_bases):
            with self._class_def(
                "__typeof__",
                _map_name(
                    lambda s: f"{_mangle_typeof_partial(s)}.__typeof__",
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
                        ptr_t = self.get_ptr_type(
                            objtype,
                            ptr,
                            cardinality=ptr.card.as_optional(),
                            variants=frozenset({None, "Partial"}),
                        )
                        defn = f"{type_alias}('{ptr.name}', '{ptr_t}')"
                        self.write(f"{ptr.name} = {defn}")

        self.write()
        self.write()

        gel_model_meta = self.import_name(BASE_IMPL, "GelModelMeta")
        if not base_types:
            gel_model = self.import_name(BASE_IMPL, "GelModel")
            meta_base_types = [gel_model_meta]
            vbase_types = [gel_model]
        else:
            meta_base_types = _map_name(lambda s: f"__{s}_ops__", base_types)
            vbase_types = base_types

        class_kwargs = {}

        if not base_types:
            bin_ops = self._operators.binary_ops.get(objtype.id, [])
            un_ops = self._operators.unary_ops.get(objtype.id, [])
            un_ops = [op for op in un_ops if op.py_magic is not None]
            metaclass = f"__{name}_ops__"

            with self._class_def(metaclass, meta_base_types):
                if not bin_ops and not un_ops:
                    self.write("pass")
                else:
                    self._write_prefix_operator_methods(un_ops)
                    self._write_infix_operator_methods(bin_ops)
            with self.type_checking():
                self.write(f"__{name}_meta__ = __{name}_ops__")
            with self.not_type_checking():
                self.write(f"__{name}_meta__ = {gel_model_meta}")

            class_kwargs["metaclass"] = f"__{name}_meta__"

        class_r_kwargs = {"__gel_type_id__": f"{uuid}(int={objtype.uuid.int})"}
        with self._class_def(
            name,
            [typeof_class, *vbase_types],
            class_kwargs={**class_kwargs, **class_r_kwargs},
        ):
            if not base_types:
                with self.not_type_checking():
                    self.write(f"__gel_type_class__ = __{name}_ops__")
            self._write_base_object_type_body(objtype, typeof_class)
            with self.type_checking():
                self._write_object_type_qb_methods(objtype)
            self.write()

            with self._class_def(
                "__variants__",
                _map_name(lambda s: f"{s}.__variants__", base_types),
            ):
                variant_base_types = []
                for bt in base_types:
                    if bt in {"Base", "Required", "PartalBase", "Partial"}:
                        variant_base_types.append(f"___{bt}___")
                    else:
                        variant_base_types.append(bt)

                with self._object_type_variant(
                    objtype,
                    variant="Base",
                    base_types=variant_base_types,
                    static_bases=[typeof_class],
                    class_kwargs=class_kwargs,
                    inherit_from_base_variant=False,
                ):
                    self._write_base_object_type_body(objtype, typeof_class)
                    with self.type_checking():
                        self._write_object_type_qb_methods(objtype)

                with self._object_type_variant(
                    objtype,
                    variant="Required",
                    base_types=variant_base_types,
                    static_bases=[],
                    class_kwargs={},
                    inherit_from_base_variant=True,
                ):
                    ptrs = _get_object_type_body(
                        objtype,
                        filters=[lambda ptr, _: not ptr.card.is_optional()],
                    )
                    if ptrs:
                        localns = frozenset(ptr.name for ptr in ptrs)
                        for ptr in ptrs:
                            ptr_type = self.get_ptr_type(
                                objtype,
                                ptr,
                                aspect=ModuleAspect.MAIN,
                                localns=localns,
                            )
                            self.write(f"{ptr.name}: {ptr_type}")
                        self.write()
                    else:
                        self.write("pass")
                        self.write()

                with self._object_type_variant(
                    objtype,
                    variant="PartialBase",
                    base_types=variant_base_types,
                    static_bases=[typeof_partial_class],
                    class_kwargs={},
                    inherit_from_base_variant=True,
                    line_comment="type: ignore [misc, unused-ignore]",
                ):
                    self.write("pass")
                    self.write()

                with self._object_type_variant(
                    objtype,
                    variant="Partial",
                    base_types=variant_base_types,
                    static_bases=["PartialBase"],
                    class_kwargs={},
                    inherit_from_base_variant=False,
                    line_comment="type: ignore [misc, unused-ignore]",
                ):
                    ptrs = _get_object_type_body(objtype)
                    if ptrs:
                        localns = frozenset(ptr.name for ptr in ptrs)
                        for ptr in ptrs:
                            ptr_type = self.get_ptr_type(
                                objtype,
                                ptr,
                                aspect=ModuleAspect.MAIN,
                                localns=localns,
                                cardinality=ptr.card.as_optional(),
                                variants=frozenset({None, "Partial"}),
                            )
                            self.write(f"{ptr.name}: {ptr_type}")
                        self.write()
                    else:
                        self.write("pass")
                        self.write()

                self.write()
                typevar = self.import_name("typing", "TypeVar")
                self.write(
                    f'Any = {typevar}("Any", '
                    f'bound="{name} | Base | Required | Partial")'
                )

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

            if base_types:
                links_partial_bases = _map_name(
                    lambda s: f"{s}.__links_partial__", base_types
                )
            else:
                lns = self.import_name(BASE_IMPL, "LinkClassNamespace")
                links_partial_bases = [lns]

            with self._class_def("__links_partial__", links_partial_bases):
                if proplinks:
                    self.write_object_type_link_variants(
                        objtype,
                        variant="Partial",
                    )
                else:
                    self.write("pass")

        self.write()
        with self.not_type_checking():
            self.write(f"{name}.__variants__.Base = {name}")

        if name in {"Base", "Required", "PartalBase", "Partial"}:
            # alias classes that conflict with variant types
            self.write(f"___{name}___ = {name}")

        self.write()

    @contextlib.contextmanager
    def _object_type_variant(
        self,
        objtype: reflection.ObjectType,
        *,
        variant: str,
        base_types: list[str],
        static_bases: list[str],
        class_kwargs: dict[str, str],
        inherit_from_base_variant: bool,
        line_comment: str | None = None,
    ) -> Iterator[None]:
        variant_bases = list(static_bases)
        if inherit_from_base_variant:
            variant_bases.append("Base")

        if base_types:
            variant_bases.extend(
                _map_name(
                    lambda s: f"{s}.__variants__.{variant}",
                    base_types,
                )
            )
        elif not inherit_from_base_variant:
            gel_model = self.import_name(BASE_IMPL, "GelModel")
            variant_bases.append(gel_model)

        with self._class_def(
            variant,
            variant_bases,
            class_kwargs=class_kwargs,
            line_comment=line_comment,
        ):
            yield

    def _write_object_type_qb_methods(
        self,
        objtype: reflection.ObjectType,
    ) -> None:
        reg_pointers = _filter_pointers(
            self._get_pointer_origins(objtype), exclude_id=False
        )
        std_bool = self.get_type(
            self._types_by_name["std::bool"],
            import_time=ImportTime.typecheck,
        )
        type_ = self.import_name("builtins", "type")
        self_ = self.import_name("typing_extensions", "Self")
        type_self = f"{type_}[{self_}]"
        builtin_bool = self.import_name("builtins", "bool", directly=False)
        builtin_str = self.import_name("builtins", "str", directly=False)
        callable_ = self.import_name("collections.abc", "Callable")
        literal_ = self.import_name("typing", "Literal")
        literal_star = f'{literal_}["*"]'
        tuple_ = self.import_name("builtins", "tuple")
        expr_proto = self.import_name(BASE_IMPL, "ExprCompatible")
        py_const = self.import_name(BASE_IMPL, "PyConstType")
        expr_closure = f"{callable_}[[{type_self}], {expr_proto}]"
        pathalias = self.import_name(BASE_IMPL, "PathAlias")
        filter_args = [
            "/",
            f"*exprs: {callable_}[[{type_self}], {type_}[{std_bool}]]",
        ]
        select_args = ["/", f"*exprs: {pathalias} | {literal_star}"]
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
                    broad_ptr_t = self._get_pytype_for_scalar(target_t)
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

        with self._classmethod_def(
            "update",
            update_args,
            type_self,
            # Ignore override errors, because we type select **computed
            # as type[GelType], which is incompatible with bool and
            # UnspecifiedType.
            line_comment="type: ignore [misc, override, unused-ignore]",
        ):
            self.write(f'"""Update {objtype.name} instances in the database.')
            self.write('"""')
            self.write("...")
            self.write()

        with self._classmethod_def(
            "select",
            select_args,
            type_self,
            # Ignore override errors, because we type select **computed
            # as type[GelType], which is incompatible with bool and
            # UnspecifiedType.
            line_comment="type: ignore [misc, override, unused-ignore]",
        ):
            self.write(f'"""Fetch {objtype.name} instances from the database.')
            self.write('"""')
            self.write("...")
            self.write()

        with self._classmethod_def(
            "filter",
            filter_args,
            type_self,
            line_comment="type: ignore [misc, override, unused-ignore]",
        ):
            self.write(f'"""Fetch {objtype.name} instances from the database.')
            self.write('"""')
            self.write("...")
            self.write()

        with self._classmethod_def(
            "order_by",
            order_args,
            type_self,
            line_comment="type: ignore [misc, override, unused-ignore]",
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

            builtins_int = self._get_pytype_for_primitive_type(int64_t)

            splice_args = [f"value: {type_}[{std_int}] | {builtins_int}"]
            with self._classmethod_def(
                "limit",
                splice_args,
                type_self,
                line_comment="type: ignore [misc, override, unused-ignore]",
            ):
                self.write('"""Limit selection to a set number of entries."""')
                self.write("...")
                self.write()

            with self._classmethod_def(
                "offset",
                splice_args,
                type_self,
                line_comment="type: ignore [misc, override, unused-ignore]",
            ):
                self.write('"""Start selection from a specific offset."""')
                self.write("...")
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

        init_pointers = _filter_pointers(
            self._get_pointer_origins(objtype),
            filters=[
                lambda ptr, obj: (
                    (ptr.name != "id" and not ptr.is_computed)
                    or objtype.name.startswith("schema::")
                ),
            ],
            exclude_id=False,
        )
        init_args = []
        if init_pointers:
            init_args.extend(["/", "*"])
            for ptr, org_objtype in init_pointers:
                ptr_t = self.get_ptr_type(
                    org_objtype,
                    ptr,
                    style="arg",
                    prefer_broad_target_type=True,
                    consider_default=True,
                )
                init_args.append(f"{ptr.name}: {ptr_t}")

        with self.type_checking():
            with self._method_def("__init__", init_args):
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
        variant: str | None = None,
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
                    variant=variant,
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
                        variant=variant,
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
        variant: str | None = None,
    ) -> str:
        type_name = reflection.parse_name(objtype.name)
        name = type_name.name

        self_t = self.import_name("typing_extensions", "Self")
        proxymodel_t = self.import_name(BASE_IMPL, "ProxyModel")

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
        if variant is not None:
            target = f"{target}.__variants__.{variant}"

        ptr_origin_types = [
            self.get_type(
                origin,
                import_time=ImportTime.typecheck,
                aspect=self.current_aspect,
            )
            for origin in ptr_origins
        ]

        classname = pname if is_forward_decl else f"{name}__{pname}"
        if variant is not None:
            container = f"__links_{variant.lower()}__"
            line_comment = "type: ignore [misc]"
        else:
            container = "__links__"
            line_comment = None

        with self._class_def(
            classname,
            (
                [f"{s}.{container}.{pname}" for s in ptr_origin_types]
                + [target, f"{proxymodel_t}[{target}]"]
            ),
            line_comment=line_comment,
        ):
            self.write(
                f'"""link {objtype.name}.{pname}: {target_type.name}"""'
            )

            if ptr_origin_types:
                lprops_bases = _map_name(
                    functools.partial(
                        lambda s, pn: f"{s}.{container}.{pn}.__lprops__",
                        pn=pname,
                    ),
                    ptr_origin_types,
                )
                reflection_bases = lprops_bases
            else:
                b = self.import_name(BASE_IMPL, "GelLinkModel")
                lprops_bases = [b]
                reflection_bases = []

            with self._class_def("__lprops__", lprops_bases):
                self.write_link_reflection(pointer, reflection_bases)

                assert ptr.pointers
                lprops = []
                for lprop in ptr.pointers:
                    if lprop.name in {"source", "target"}:
                        continue
                    ttype = self._types[lprop.target_id]
                    assert reflection.is_scalar_type(ttype)
                    ptr_type = self.get_type(ttype, import_time=import_time)
                    pytype = " | ".join(self._get_pytype_for_scalar(ttype))
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

            self.write("__linkprops__: __lprops__")
            self.write()
            if is_forward_decl:
                args = [f"obj: {target}", "/", "*", *lprops]
                with self._method_def("__init__", args):
                    self.write("...")
            else:
                # It's important to just forward '**link_props' to
                # the constructor of `__lprops__` -- otherwise pydantic's
                # tracking and our's own tracking would assume that argument
                # defaults (Nones in this case) were explicitly set by the user
                # leading to inefficient queries in save().
                args = ["obj", "/", "**link_props"]
                with self._method_def("__init__", args):
                    obj = self.import_name("builtins", "object")
                    self.write(f"{proxymodel_t}.__init__(self, obj)")
                    self.write(
                        "lprops = self.__class__.__lprops__(**link_props)"
                    )
                    self.write(
                        f'{obj}.__setattr__(self, "__linkprops__", lprops)'
                    )

            self.write()
            if is_forward_decl:
                args = [f"obj: {target}", "/", "*", *lprops]
                with self._classmethod_def("link", args, self_t):
                    self.write("...")
            else:
                args = ["obj", "/", "**link_props"]
                with self._classmethod_def("link", args, self_t):
                    self.write(
                        self.format_list(
                            "return cls({list})",
                            ["obj", "**link_props"],
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
            pointers = _get_object_type_body(objtype)
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
        param_map: defaultdict[str, defaultdict[str, set[str]]] = defaultdict(
            lambda: defaultdict(set)
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
        param_map: defaultdict[str, defaultdict[str, set[str]]],
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
        param_map: defaultdict[str, defaultdict[str, set[str]]],
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

        fname = ident(name.name)

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
                prop.card in {"AtLeastOne", "Many"},  # is multi
                prop.card in {"AtMostOne", "Many", "Empty"},  # is optional
                bool(prop.pointers),
                prop.is_computed,
            ):
                case True, True, True, False:
                    desc = self.import_name(BASE_IMPL, "MultiLinkWithProps")
                    pytype = f"{desc}[{narrow_type}, {broad_type}]"
                case True, False, True, False:
                    desc = self.import_name(
                        BASE_IMPL, "RequiredMultiLinkWithProps"
                    )
                    pytype = f"{desc}[{narrow_type}, {broad_type}]"
                case True, _, True, True:
                    desc = self.import_name(
                        BASE_IMPL, "ComputedMultiLinkWithProps"
                    )
                    pytype = f"{desc}[{narrow_type}, {broad_type}]"
                case True, True, False, False:
                    desc = self.import_name(BASE_IMPL, "MultiLink")
                    pytype = f"{desc}[{narrow_type}]"
                case True, False, False, False:
                    desc = self.import_name(BASE_IMPL, "RequiredMultiLink")
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
                    desc = self.import_name(BASE_IMPL, "LinkWithProps")
                    pytype = f"{desc}[{narrow_type}, {broad_type}]"
                case False, False, True, True:
                    desc = self.import_name(BASE_IMPL, "ComputedLinkWithProps")
                    pytype = f"{desc}[{narrow_type}, {broad_type}]"
                case False, True, False, False:
                    desc = self.import_name(BASE_IMPL, "OptionalLink")
                    pytype = f"{desc}[{narrow_type}]"
                case False, True, False, True:
                    desc = self.import_name(BASE_IMPL, "OptionalComputedLink")
                    pytype = f"{desc}[{narrow_type}]"
                case False, False, False, False:
                    pytype = narrow_type
        else:
            match (
                cardinality.is_multi(),
                cardinality.is_optional(),
                prop.is_computed,
            ):
                case True, _, False:
                    desc = self.import_name(BASE_IMPL, "MultiProperty")
                    pytype = f"{desc}[{narrow_type}, {broad_type}]"
                case True, _, True:
                    desc = self.import_name(BASE_IMPL, "ComputedMultiProperty")
                    pytype = f"{desc}[{narrow_type}, {broad_type}]"
                case False, True, False:
                    desc = self.import_name(BASE_IMPL, "OptionalProperty")
                    pytype = f"{desc}[{narrow_type}, {broad_type}]"
                case False, True, True:
                    desc = self.import_name(
                        BASE_IMPL, "OptionalComputedProperty"
                    )
                    pytype = f"{desc}[{narrow_type}, {broad_type}]"
                case False, False, True:
                    desc = self.import_name(BASE_IMPL, "ComputedProperty")
                    pytype = f"{desc}[{narrow_type}, {broad_type}]"
                case False, False, False:
                    pytype = narrow_type

        return pytype  # pyright: ignore [reportPossiblyUnboundVariable]  # pyright match block no bueno

    def get_ptr_type(
        self,
        objtype: reflection.ObjectType,
        prop: reflection.Pointer,
        *,
        style: Literal["annotation", "typeddict", "arg"] = "annotation",
        prefer_broad_target_type: bool = False,
        consider_default: bool = False,
        aspect: ModuleAspect | None = None,
        cardinality: reflection.Cardinality | None = None,
        localns: frozenset[str] | None = None,
        variants: frozenset[str | None] | None = None,
    ) -> str:
        if aspect is None:
            aspect = ModuleAspect.VARIANTS

        objtype_name = objtype.schemapath
        target_type = self._types[prop.target_id]
        bare_ptr_type = ptr_type = self.get_type(
            target_type,
            aspect=aspect,
            import_time=ImportTime.late_runtime,
            localns=localns,
        )

        if reflection.is_primitive_type(target_type):
            bare_ptr_type = self._get_pytype_for_primitive_type(
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
                    assn_pytype = self._get_pytype_for_primitive_type(
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
            if self.current_aspect is ModuleAspect.VARIANTS:
                target_name = reflection.parse_name(target_type.name)
                if target_name.parent != objtype_name.parent:
                    aspect = ModuleAspect.LATE
            ptr_type = f"{objtype_name.name}.__links__.{prop.name}"
            has_lprops = True
        else:
            has_lprops = False

        if reflection.is_link(prop) and variants:
            union = set()
            for variant in variants:
                if variant is None:
                    union.add(ptr_type)
                elif has_lprops:
                    union.add(
                        f"{objtype_name.name}.__links_{variant.lower()}__"
                        f".{prop.name}"
                    )
                else:
                    union.add(f"{ptr_type}.__variants__.{variant}")
            ptr_type = " | ".join(sorted(union))

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
                    type_ = f"{iterable}[{ptr_type}]"
                    default = "[]"
                elif cardinality.is_optional():
                    type_ = f"{ptr_type} | None"
                    default = "None"
                else:
                    type_ = ptr_type
                    default = None

                if consider_default and prop.has_default:
                    defv_t = self.import_name(BASE_IMPL, "DefaultValue")
                    type_ = f"{type_} | {defv_t}"
                    default = self.import_name(BASE_IMPL, "DEFAULT_VALUE")

                result = type_
                if default is not None:
                    result = f"{type_} = {default}"
            case _:
                raise AssertionError(
                    f"unexpected type rendering style: {style!r}"
                )

        return result


class GeneratedGlobalModule(BaseGeneratedModule):
    def should_write(
        self, py_file: GeneratedModule, aspect: ModuleAspect
    ) -> bool:
        return py_file.has_content()

    def process(self, types: Mapping[str, reflection.AnyType]) -> None:
        graph: defaultdict[str, set[str]] = defaultdict(set)

        @functools.singledispatch
        def type_dispatch(t: reflection.AnyType, ref_t: str) -> None:
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
        anytuple = self.import_name(BASE_IMPL, "AnyNamedTuple")

        self.write("#")
        self.write(f"# tuple type {t.schemapath.as_schema_name()}")
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
            self.write_type_reflection(t)
            self.write()
            self.write("__slots__ = ()")
        self.write()
