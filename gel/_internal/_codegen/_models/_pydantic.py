# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

from __future__ import annotations
import contextlib
from typing import (
    TYPE_CHECKING,
    Any,
    Literal,
    NamedTuple,
    Protocol,
    TypedDict,
    TypeVar,
    cast,
)
from typing_extensions import TypeAliasType

import base64
import dataclasses
import enum
import functools
import itertools
import json
import graphlib
import logging
import operator
import pathlib
import tempfile
import textwrap
import uuid

from collections import defaultdict
from collections.abc import Mapping  # noqa: TC003 # coerce_to_dataclass needs it
from contextlib import contextmanager

import gel
from gel import abstract
from gel._internal import _cache
from gel._internal import _dataclass_extras
from gel._internal import _dirsync
from gel._internal import _reflection as reflection
from gel._internal import _version as _ver_utils
from gel._internal._collections_extras import ImmutableChainMap
from gel._internal._namespace import ident, dunder
from gel._internal._qbmodel import _abstract as _qbmodel
from gel._internal._reflection._enums import SchemaPart, TypeModifier
from gel._internal._schemapath import SchemaPath

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
        Sequence,
        Set as AbstractSet,
    )

    from gel._internal._reflection._callables import CallableParamKey
    from gel._internal._reflection._types import Indirection


COMMENT = """\
#
# Automatically generated from Gel schema.
#
# Do not edit directly as re-generating this file will overwrite any changes.
#

# fmt: off
# ruff: noqa
# flake8: noqa
# pylint: skip-file\
"""


def get_init_new_docstring(name: str) -> str:
    return f"""\
\"\"\"Create a new {name} object from keyword arguments.

Call `db.save()` on the returned object to persist it in the database.
\"\"\"\
"""


def get_init_for_update_docsting(name: str) -> str:
    return f"""\
\"\"\"Update an existing {name} object with the matching `id`.

All keyword arguments except `id` are optional. When provided, they
will update the corresponding fields of the existing object.

Call `db.save()` on the returned object to persist changes in the database.
\"\"\"\
"""


def get_single_link_for_proxy_docsting(
    *,
    source_type: str,
    link_name: str,
    target_type: str,
) -> str:
    return f"""\
\"\"\"Wrap {target_type} to add link properties of {source_type}.{link_name}.

This is useful to add link properties when setting the link, e.g.:

    obj = {source_type.replace("::", ".")}(...)

    obj.{link_name} = {source_type.replace("::", ".")}.link(
        {target_type.replace("::", ".")}(...),
        link_prop=value,
        ...
    )
\"\"\"\
"""


def get_multi_link_for_proxy_docsting(
    *,
    source_type: str,
    link_name: str,
    target_type: str,
) -> str:
    return f"""\
\"\"\"Wrap {target_type} to add link properties of {source_type}.{link_name}.

This is useful to add link properties when setting the link, e.g.:

    obj = {source_type.replace("::", ".")}(...)

    obj.{link_name}.append(
        {source_type.replace("::", ".")}.link(
            {target_type.replace("::", ".")}(...),
            link_prop=value,
            ...
        )
    )
\"\"\"\
"""


logger = logging.getLogger(__name__)


class IntrospectedModule(TypedDict):
    object_types: dict[str, reflection.ObjectType]
    scalar_types: dict[str, reflection.ScalarType]
    functions: list[reflection.Function]
    globals: list[reflection.Global]


@dataclasses.dataclass(kw_only=True, frozen=True)
class Schema:
    types: Mapping[str, reflection.AnyType]
    casts: reflection.CastMatrix
    operators: reflection.OperatorMatrix
    functions: list[reflection.Function]
    globals: list[reflection.Global]


@dataclasses.dataclass(kw_only=True, frozen=True)
class GeneratedState:
    db: reflection.BranchState
    client_version: str


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
            this_client_ver = _ver_utils.get_project_version_key()
            std_schema: Schema | None = None

            std_gen = SchemaGenerator(
                self._client,
                reflection.SchemaPart.STD,
            )

            outdir = pathlib.Path(tmp_models_root.name)
            need_dirsync = False

            if (
                file_state is None
                or file_state.db.server_version != db_state.server_version
                or file_state.client_version != this_client_ver
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
                or file_state.db.server_version != db_state.server_version
                or file_state.db.top_migration != db_state.top_migration
                or file_state.client_version != this_client_ver
                or self._no_cache
            ):
                usr_gen = SchemaGenerator(
                    self._client,
                    reflection.SchemaPart.USER,
                    std_schema=std_schema,
                )
                usr_gen.run(outdir)
                need_dirsync = True

            self._write_state(
                GeneratedState(db=db_state, client_version=this_client_ver),
                outdir,
            )

            if need_dirsync:
                for fn in list(std_manifest):
                    # Also keep the directories
                    std_manifest.update(fn.parents)

                _dirsync.dirsync(outdir, models_root, keep=std_manifest)

        if not self._quiet:
            self.print_msg(f"{C.GREEN}{C.BOLD}Done.{C.ENDC}")

    def _cache_key(self, suf: str, sv: reflection.ServerVersion) -> str:
        ver_key = _ver_utils.get_project_version_key()
        return f"gm-c-{ver_key}-s-{sv.major}.{sv.minor}-{suf}"

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

    def _get_last_state(self) -> GeneratedState | None:
        state_json = self._project_dir / "models" / "_state.json"
        try:
            with open(state_json, encoding="utf8") as f:
                state_data = json.load(f)
        except (OSError, ValueError, TypeError):
            return None

        try:
            server_version = state_data["db"]["server_version"]
            top_migration = state_data["db"]["top_migration"]
            client_version = state_data["client_version"]
        except KeyError:
            return None

        if (
            not isinstance(server_version, list)
            or len(server_version) != 2
            or not all(isinstance(part, int) for part in server_version)
        ):
            return None

        if not isinstance(client_version, str) or not client_version:
            return None

        if not isinstance(top_migration, str):
            return None

        return GeneratedState(
            db=reflection.BranchState(
                server_version=reflection.ServerVersion(*server_version),
                top_migration=top_migration,
            ),
            client_version=client_version,
        )

    def _write_state(
        self,
        state: GeneratedState,
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
        self._modules: dict[SchemaPath, IntrospectedModule] = {}
        self._std_modules: list[SchemaPath] = []
        self._types: Mapping[str, reflection.Type] = {}
        self._casts: reflection.CastMatrix
        self._operators: reflection.OperatorMatrix
        self._functions: list[reflection.Function]
        self._globals: list[reflection.Global]
        self._named_tuples: dict[str, reflection.NamedTupleType] = {}
        self._wrapped_types: set[str] = set()
        self._std_schema = std_schema
        if schema_part is not SchemaPart.STD and std_schema is None:
            raise ValueError(
                "must pass std_schema when reflecting user schemas"
            )

    def dry_run_manifest(self) -> set[pathlib.Path]:
        part = self._schema_part
        std_modules: dict[SchemaPath, bool] = dict.fromkeys(
            (
                SchemaPath(mod)
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
        modules: dict[SchemaPath, GeneratedSchemaModule] = {}
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
                all_globals=self._globals,
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
            SchemaPath(),
            all_types=self._types,
            all_casts=self._casts,
            all_operators=self._operators,
            all_globals=self._globals,
            modules=all_modules,
            schema_part=self._schema_part,
        )
        module.write_submodules([m for m in all_modules if len(m.parts) == 1])
        default_module = modules.get(SchemaPath("default"))
        if default_module is not None:
            module.reexport_module(default_module)
        written.update(module.write_files(outdir))

        return schema, written

    def introspect_schema(self) -> Schema:
        for mod in reflection.fetch_modules(self._client, self._schema_part):
            self._modules[SchemaPath(mod)] = {
                "scalar_types": {},
                "object_types": {},
                "functions": [],
                "globals": [],
            }

        this_part = self._schema_part
        std_part = reflection.SchemaPart.STD

        self._types = reflection.fetch_types(self._client, this_part)
        these_types = self._types
        self._casts = reflection.fetch_casts(self._client, this_part)
        self._operators = reflection.fetch_operators(self._client, this_part)
        these_funcs = reflection.fetch_functions(self._client, this_part)
        self._functions = these_funcs
        these_globals = reflection.fetch_globals(self._client, this_part)
        self._globals = these_globals

        if self._schema_part is not std_part:
            assert self._std_schema is not None
            std_types = self._std_schema.types
            self._types = ImmutableChainMap(std_types, these_types)
            std_casts = self._std_schema.casts
            self._casts = self._casts.chain(std_casts)
            std_operators = self._std_schema.operators
            self._operators = self._operators.chain(std_operators)
            self._functions = these_funcs + self._std_schema.functions
            self._globals = these_globals + self._std_schema.globals
            self._std_modules = [
                SchemaPath(mod)
                for mod in reflection.fetch_modules(self._client, std_part)
            ]
        else:
            self._std_modules = list(self._modules)

        for t in these_types.values():
            if reflection.is_object_type(t):
                name = t.schemapath
                self._modules[name.parent]["object_types"][name.name] = t
            elif reflection.is_scalar_type(t):
                name = t.schemapath
                self._modules[name.parent]["scalar_types"][name.name] = t
            elif reflection.is_named_tuple_type(t):
                self._named_tuples[t.id] = t

        for f in these_funcs:
            name = f.schemapath
            self._modules[name.parent]["functions"].append(f)

        for g in these_globals:
            name = g.schemapath
            self._modules[name.parent]["globals"].append(g)

        return Schema(
            types=cast("Mapping[str, reflection.AnyType]", self._types),
            casts=self._casts,
            operators=self._operators,
            functions=self._functions,
            globals=self._globals,
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
            all_globals=self._globals,
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
    module_alias: str | None


@functools.cache
def get_modpath(
    modpath: SchemaPath,
    aspect: ModuleAspect,
) -> SchemaPath:
    if aspect is ModuleAspect.MAIN:
        pass
    elif aspect is ModuleAspect.VARIANTS:
        modpath = SchemaPath("__variants__") / modpath
    elif aspect is ModuleAspect.LATE:
        modpath = SchemaPath("__variants__") / "__late__" / modpath

    return modpath


def get_common_types_modpath(
    schema_part: reflection.SchemaPart,
) -> SchemaPath:
    mod = SchemaPath("__types__")
    if schema_part is reflection.SchemaPart.STD:
        mod = SchemaPath("std") / mod
    return mod


def mod_is_package(
    mod: SchemaPath,
    schema_part: reflection.SchemaPart,
) -> bool:
    return not mod.parts or (
        schema_part is reflection.SchemaPart.STD and len(mod.parts) == 1
    )


def mod_filename(
    modpath: SchemaPath,
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

    return dirpath.as_pathlib_path() / filename


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


def _indirection_key(path: Indirection) -> tuple[str, ...]:
    return tuple(s if isinstance(s, str) else f"{s[0]}[{s[1]}]" for s in path)


BASE_IMPL = "gel.models.pydantic"
CORE_OBJECTS = frozenset(
    {
        "std::BaseObject",
        "std::Object",
        "std::FreeObject",
    }
)
GENERIC_TYPES = frozenset(
    {
        SchemaPath("std", "array"),
        SchemaPath("std", "tuple"),
        SchemaPath("std", "range"),
        SchemaPath("std", "multirange"),
    }
)

# Deprecated Pydantic attributes, allow shadowing
SHADOWED_PYDANTIC_ATTRIBUTES = frozenset(
    {
        "dict",
        "json",
        "parse_obj",
        "parse_row",
        "parse_file",
        "from_orm",
        "construct",
        "copy",
        "schema",
        "schema_json",
        "validate",
        "update_forward_refs",
        "_iter",
        "_copy_and_set_values",
        "_get_value",
        "_calculate_keys",
    }
)


def _filter_pointers(
    pointers: Iterable[tuple[reflection.Pointer, reflection.ObjectType]],
    filters: Iterable[
        Callable[[reflection.Pointer, reflection.ObjectType], bool]
    ] = (),
    *,
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
        modname: SchemaPath,
        *,
        all_types: Mapping[str, reflection.Type],
        all_casts: reflection.CastMatrix,
        all_operators: reflection.OperatorMatrix,
        all_globals: list[reflection.Global],
        modules: Collection[SchemaPath],
        schema_part: reflection.SchemaPart,
    ) -> None:
        super().__init__()
        self._modpath = modname
        self._types = all_types
        self._types_by_name: dict[str, reflection.Type] = {}
        self._casts = all_casts
        self._operators = all_operators
        self._globals = all_globals
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
            ModuleAspect.MAIN: GeneratedModule(
                COMMENT,
                BASE_IMPL,
                code_preamble=(
                    '__gel_default_variant__ = "Default"'
                    if self._schema_part is reflection.SchemaPart.USER
                    else None
                ),
            ),
            ModuleAspect.VARIANTS: GeneratedModule(COMMENT, BASE_IMPL),
            ModuleAspect.LATE: GeneratedModule(COMMENT, BASE_IMPL),
        }
        self._current_py_file = self._py_files[ModuleAspect.MAIN]
        self._current_aspect = ModuleAspect.MAIN
        self._type_import_cache: dict[
            tuple[str, ModuleAspect, ModuleAspect, bool, ImportTime],
            str,
        ] = {}
        self.__post_init__()

    def __post_init__(self) -> None:
        pass

    def get_mod_schema_part(
        self,
        mod: SchemaPath,
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
        mod: SchemaPath,
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
    def canonical_modpath(self) -> SchemaPath:
        return self._modpath

    @property
    def current_modpath(self) -> SchemaPath:
        return self.modpath(self._current_aspect)

    def modpath(self, aspect: ModuleAspect) -> SchemaPath:
        return get_modpath(self._modpath, aspect)

    @property
    def is_package(self) -> bool:
        return self._is_package

    @contextmanager
    def _open_py_file(
        self,
        path: pathlib.Path,
        modpath: SchemaPath,
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
        stype: reflection.Type,
        *,
        base_metadata_class: str = "GelTypeMetadata",
    ) -> None:
        return self.write_schema_reflection(
            stype, base_metadata_class=base_metadata_class
        )

    def write_schema_reflection(
        self,
        sobj: reflection.SchemaObject,
        *,
        base_metadata_class: str = "GelSchemaMetadata",
    ) -> None:
        uuid = self.import_name("uuid", "UUID")
        schemapath = self.import_name(BASE_IMPL, "SchemaPath")
        if isinstance(sobj, reflection.InheritingType):
            base_types = [
                self.get_type(self._types[base.id]) for base in sobj.bases
            ]
        else:
            gmm = self.import_name(BASE_IMPL, base_metadata_class)
            base_types = [gmm]
        with self._class_def(
            "__gel_reflection__",
            _map_name(
                lambda s: f"{s}.__gel_reflection__",
                base_types,
            ),
        ):
            self.write(f"id = {uuid}(int={sobj.uuid.int})")
            self.write(f"name = {sobj.schemapath.as_code(schemapath)}")

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
            self._schema_object_type.schemapath.parent,
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
            # Need a cheap at runtime way to check if the type is abstract
            # in GelModel.__new__
            self.write(f"abstract = {objtype.abstract!r}")
            self._write_pointers_reflection(objtype.pointers, base_types)

            with self._classmethod_def(
                "object",
                [],
                objecttype_t,
                decorators=(f'{lazyclassproperty}["{objecttype_t}"]',),
            ):
                objtype_mod = self.py_file.import_module(
                    objecttype_import.module,
                    suggested_module_alias=objecttype_import.module_alias,
                    import_time=ImportTime.local,
                )
                self.write(f"return {objtype_mod}.ObjectType(")
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

    def declare_typevar(
        self,
        name: str,
        *,
        bound: str | None,
    ) -> str:
        return self.py_file.declare_typevar(name, bound=bound)

    def disambiguate_name(self, name: str) -> str:
        return self.py_file.disambiguate_name(name)

    def import_name(
        self,
        module: str,
        name: str,
        *,
        suggested_module_alias: str | None = None,
        import_time: ImportTime = ImportTime.runtime,
        localns: frozenset[str] | None = None,
    ) -> str:
        if module is _qbmodel.MODEL_SUBSTRATE_MODULE:
            module = BASE_IMPL

        return self.py_file.import_name(
            module,
            name,
            suggested_module_alias=suggested_module_alias,
            import_time=import_time,
            localns=localns,
        )

    def import_qual_name(
        self,
        module: str,
        name: str,
        *,
        suggested_module_alias: str | None = None,
        import_time: ImportTime = ImportTime.runtime,
        localns: frozenset[str] | None = None,
    ) -> str:
        if module is _qbmodel.MODEL_SUBSTRATE_MODULE:
            module = BASE_IMPL

        return self.py_file.import_qual_name(
            module,
            name,
            suggested_module_alias=suggested_module_alias,
            import_time=import_time,
            localns=localns,
        )

    def import_module(
        self,
        module: str,
        *,
        suggested_module_alias: str | None = None,
        import_time: ImportTime = ImportTime.runtime,
        localns: frozenset[str] | None = None,
    ) -> str:
        if module is _qbmodel.MODEL_SUBSTRATE_MODULE:
            module = BASE_IMPL

        return self.py_file.import_module(
            module,
            suggested_module_alias=suggested_module_alias,
            import_time=import_time,
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
    def if_(self, cond: str) -> Iterator[None]:
        with self.py_file.if_(cond):
            yield

    @contextmanager
    def elif_(self, cond: str) -> Iterator[None]:
        with self.py_file.elif_(cond):
            yield

    @contextmanager
    def else_(self) -> Iterator[None]:
        with self.py_file.else_():
            yield

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
        module: SchemaPath,
        aspect: ModuleAspect,
    ) -> Import | None:
        imp_mod = module
        cur_mod = self.current_modpath
        if aspect is not ModuleAspect.MAIN:
            imp_mod = get_modpath(imp_mod, aspect)

        if imp_mod == cur_mod and aspect is self.current_aspect:
            # It's this module, no need to import
            return None
        else:
            if (
                module == self.canonical_modpath
                and self.current_aspect is ModuleAspect.MAIN
            ):
                module_alias = "base"
            else:
                module_alias = "_".join(module.parts)

            if aspect is ModuleAspect.VARIANTS:
                module_alias += "_variants"
            elif aspect is ModuleAspect.LATE:
                module_alias += "_late"

            cur_pkg = cur_mod if self._is_package else cur_mod.parent
            common_parts = imp_mod.common_parts(cur_pkg)
            import_tail = imp_mod.parts[len(common_parts) :]

            relative_depth = len(cur_pkg.parts) - len(common_parts) + 1
            if not import_tail:
                relative_depth += 1

            dots = "." * relative_depth
            if not import_tail:
                # Pure ancestor import
                py_mod = f"{dots}{module.name}"
            else:
                py_mod = dots + ".".join(import_tail)

            return Import(
                module=py_mod,
                module_alias=module_alias,
            )

    def get_type(
        self,
        stype: reflection.Type,
        *,
        import_time: ImportTime | None = None,
        aspect: ModuleAspect = ModuleAspect.MAIN,
        typevars: Mapping[reflection.Type, str] | None = None,
        localns: frozenset[str] | None = None,
    ) -> str:
        if (
            typevars is not None
            and (typevar := typevars.get(stype)) is not None
        ):
            return typevar

        if import_time is None:
            import_time = (
                ImportTime.typecheck
                if self.in_type_checking
                else ImportTime.runtime
            )

        if (
            import_time is ImportTime.typecheck_runtime
            or import_time is ImportTime.late_runtime
        ):
            foreign_import_time = ImportTime.runtime
        else:
            foreign_import_time = import_time

        if reflection.is_array_type(stype):
            arr = self.import_name(
                BASE_IMPL, "Array", import_time=foreign_import_time
            )
            elem_type = self.get_type(
                stype.get_element_type(self._types),
                import_time=import_time,
                aspect=aspect,
                localns=localns,
                typevars=typevars,
            )
            return f"{arr}[{elem_type}]"

        elif reflection.is_tuple_type(stype):
            tup = self.import_name(
                BASE_IMPL, "Tuple", import_time=foreign_import_time
            )
            elem_types = [
                self.get_type(
                    elem_type,
                    import_time=import_time,
                    aspect=aspect,
                    localns=localns,
                    typevars=typevars,
                )
                for elem_type in stype.get_element_types(self._types)
            ]
            return f"{tup}[{', '.join(elem_types)}]"

        elif reflection.is_range_type(stype):
            rang = self.import_name(
                BASE_IMPL, "Range", import_time=foreign_import_time
            )
            elem_type = self.get_type(
                stype.get_element_type(self._types),
                import_time=import_time,
                aspect=aspect,
                localns=localns,
                typevars=typevars,
            )
            return f"{rang}[{elem_type}]"

        elif reflection.is_multi_range_type(stype):
            rang = self.import_name(
                BASE_IMPL, "MultiRange", import_time=foreign_import_time
            )
            elem_type = self.get_type(
                stype.get_element_type(self._types),
                import_time=import_time,
                aspect=aspect,
                localns=localns,
                typevars=typevars,
            )
            return f"{rang}[{elem_type}]"

        elif reflection.is_pseudo_type(stype):
            if stype.name == "anyobject":
                return self.import_name(
                    BASE_IMPL, "GelModel", import_time=foreign_import_time
                )
            elif stype.name == "anytuple":
                return self.import_name(
                    BASE_IMPL, "AnyTuple", import_time=foreign_import_time
                )
            elif stype.name == "anytype":
                return self.import_name(
                    BASE_IMPL, "GelType", import_time=foreign_import_time
                )
            else:
                raise AssertionError(f"unsupported pseudo-type: {stype.name}")

        elif reflection.is_named_tuple_type(stype):
            mod = "__types__"
            if stype.builtin:
                mod = f"std::{mod}"
            type_path = SchemaPath(mod, self.get_tuple_name(stype))
            # Named tuples are always imported from __types__,
            # which has only the MAIN aspect.
            aspect = ModuleAspect.MAIN

        else:
            type_path = stype.schemapath

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
            str(type_path),
            aspect,
            cur_aspect,
            False,
            import_time,
        )
        result = self._type_import_cache.get(cache_key)
        if result is None:
            result = self.get_object(
                type_path,
                import_time=import_time,
                aspect=aspect,
                localns=localns,
            )
            if import_time is not ImportTime.local:
                self._type_import_cache[cache_key] = result

        return result

    def get_object(
        self,
        obj_path: SchemaPath,
        *,
        import_time: ImportTime | None = None,
        aspect: ModuleAspect = ModuleAspect.MAIN,
        localns: frozenset[str] | None = None,
    ) -> str:
        obj_name = obj_path.name
        rel_import = self._resolve_rel_import(obj_path.parent, aspect)
        if rel_import is None:
            return obj_name
        else:
            if import_time is None:
                import_time = (
                    ImportTime.typecheck
                    if self.in_type_checking
                    else ImportTime.runtime
                )
            return self.import_qual_name(
                rel_import.module,
                obj_name,
                suggested_module_alias=rel_import.module_alias,
                import_time=import_time,
                localns=localns,
            )

    def get_type_type(
        self,
        stype: reflection.Type,
        *,
        import_time: ImportTime | None = None,
        aspect: ModuleAspect = ModuleAspect.MAIN,
        typevars: Mapping[reflection.Type, str] | None = None,
        localns: frozenset[str] | None = None,
    ) -> str:
        tp = self.get_type(
            stype,
            import_time=import_time,
            aspect=aspect,
            typevars=typevars,
            localns=localns,
        )
        if import_time is None:
            import_time = (
                ImportTime.typecheck
                if self.in_type_checking
                else ImportTime.runtime
            )
        elif import_time is ImportTime.typecheck_runtime:
            import_time = ImportTime.runtime
        type_ = self.import_name("builtins", "type", import_time=import_time)
        return f"{type_}[{tp}]"

    def get_type_generics(
        self,
        stype: reflection.Type,
    ) -> set[reflection.Type]:
        if stype.generic:
            return {stype}
        elif isinstance(stype, reflection.HomogeneousCollectionType):
            return self.get_type_generics(stype.get_element_type(self._types))
        elif isinstance(stype, reflection.HeterogeneousCollectionType):
            return set(
                itertools.chain.from_iterable(
                    self.get_type_generics(el_type)
                    for el_type in stype.get_element_types(self._types)
                )
            )
        else:
            return set()

    def format_list(
        self,
        tpl: str,
        values: Iterable[str],
        *,
        first_line_comment: str | None = None,
        extra_indent: int = 0,
        separator: str = ", ",
        carry_separator: bool = False,
        trailing_separator: bool | None = None,
    ) -> str:
        return self.py_file.format_list(
            tpl,
            values,
            first_line_comment=first_line_comment,
            extra_indent=extra_indent,
            separator=separator,
            carry_separator=carry_separator,
            trailing_separator=trailing_separator,
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
                trailing_separator=False,
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
        type_ignore: Iterable[str] = (),
        line_comment: str | None = None,
        implicit_param: bool = True,
    ) -> Iterator[None]:
        type_ignore = list(type_ignore)
        if overload:
            # Must import from `typing_extensions` for `get_overloads`
            # to work on Python 3.10.
            over = self.import_name("typing_extensions", "overload")
            # Mypy sometimes complains about the `@overload` line,
            # not the `def` line, so restate the type: ignore comment
            # here (with unused-ignore).
            decor_line = f"@{over}"
            if type_ignore:
                ignores = sorted({*type_ignore, "unused-ignore"})
                type_ignore_comment = f"type: ignore [{', '.join(ignores)}]"
                decor_line = f"{decor_line}  # {type_ignore_comment}"
            self.write(decor_line)
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

        if type_ignore:
            type_ignore_comment = f"type: ignore [{', '.join(type_ignore)}]"
            if line_comment:
                line_comment = f"{type_ignore_comment}  # {line_comment}"
            else:
                line_comment = type_ignore_comment

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
        type_ignore: Iterable[str] = (),
        line_comment: str | None = None,
    ) -> Iterator[None]:
        with self._func_def(
            func_name,
            params,
            return_type,
            kind="classmethod",
            overload=overload,
            decorators=decorators,
            type_ignore=type_ignore,
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
        type_ignore: Iterable[str] = (),
        line_comment: str | None = None,
    ) -> Iterator[None]:
        with self._func_def(
            func_name,
            params,
            return_type,
            kind="property",
            overload=overload,
            decorators=decorators,
            type_ignore=type_ignore,
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
        type_ignore: Iterable[str] = (),
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
            type_ignore=type_ignore,
            line_comment=line_comment,
            implicit_param=implicit_param,
        ):
            yield


InheritingType_T = TypeVar("InheritingType_T", bound=reflection.InheritingType)
PyTypeName = TypeAliasType("PyTypeName", tuple[str, str])

_Callable_T = TypeVar("_Callable_T", bound=reflection.Callable)


class GeneratedSchemaModule(BaseGeneratedModule):
    def __post_init__(self) -> None:
        super().__post_init__()
        self._scalar_generics: dict[str, str] = {}

    def process(self, mod: IntrospectedModule) -> None:
        self.prepare_namespace(mod)
        self.write_scalar_types(mod["scalar_types"])
        self.write_generic_types(mod)
        self.write_object_types(mod["object_types"])
        # Write functions, but omit generic type constructors
        # (those would have already been written by write_generic_types())
        self.write_functions(
            [f for f in mod["functions"] if f.schemapath not in GENERIC_TYPES]
        )
        self.write_non_magic_infix_operators(
            [
                op
                for op in itertools.chain.from_iterable(
                    self._operators.binary_ops.values()
                )
                if op.schemapath.parent == self.canonical_modpath
            ]
        )
        self.write_non_magic_prefix_operators(
            [
                op
                for op in itertools.chain.from_iterable(
                    self._operators.unary_ops.values()
                )
                if op.schemapath.parent == self.canonical_modpath
            ]
        )
        self.write_globals(mod["globals"])

    def reexport_module(self, mod: GeneratedSchemaModule) -> None:
        exports = sorted(mod.exports)
        if not exports:
            return

        rel_imp = self._resolve_rel_import(
            mod.canonical_modpath,
            aspect=ModuleAspect.MAIN,
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

    def write_submodules(self, mods: list[SchemaPath]) -> None:
        if not mods:
            return

        builtins_str = self.import_name(
            "builtins", "str", import_time=ImportTime.typecheck
        )
        any_ = self.import_name(
            "typing", "Any", import_time=ImportTime.typecheck
        )
        implib = self.import_module("importlib")

        for mod in mods:
            self.import_module(
                "." + mod.name, import_time=ImportTime.typecheck
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
            t_name = t.schemapath

            for base_ref in t.bases:
                base = self._types[base_ref.id]
                base_name = base.schemapath
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
        consider_generic: bool = True,
        import_time: ImportTime = ImportTime.runtime,
    ) -> list[str] | None:
        base_type = _qbmodel.get_py_base_for_scalar(
            stype.name,
            require_subclassable=require_subclassable,
            consider_generic=consider_generic,
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
        consider_generic: bool = True,
        import_time: ImportTime = ImportTime.runtime,
    ) -> list[str] | None:
        base_type = _qbmodel.get_py_type_for_scalar(
            stype.name,
            require_subclassable=require_subclassable,
            consider_generic=consider_generic,
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
        consider_generic: bool = True,
        import_time: ImportTime = ImportTime.runtime,
        localns: frozenset[str] | None = None,
    ) -> list[str]:
        base_type = _qbmodel.get_py_type_for_scalar_hierarchy(
            self._get_scalar_hierarchy(stype),
            consider_generic=consider_generic,
        )
        if not base_type:
            raise AssertionError(
                f"could not find Python base type for scalar type {stype.name}"
            )
        else:
            if (
                import_time is ImportTime.typecheck_runtime
                or import_time is ImportTime.late_runtime
            ):
                import_time = ImportTime.runtime
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
            el_type = stype.get_element_type(self._types)
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
            el_type = stype.get_element_type(self._types)
            if reflection.is_primitive_type(el_type):
                el = self._get_pytype_for_primitive_type(
                    el_type, import_time=import_time, localns=localns
                )
            else:
                el = self.get_type(el_type, import_time=import_time)
            rng = self.import_name("gel", "Range")
            return f"{rng}[{el}]"
        elif reflection.is_multi_range_type(stype):
            el_type = stype.get_element_type(self._types)
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
            for el_type in stype.get_element_types(self._types):
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
                type_name = scalar.schemapath
                scalars.append(scalar)
                self.py_file.add_global(type_name.name)

            for stype in scalars:
                self._write_scalar_type(stype)

        with self.code_section(CodeSection.after_late_import):
            for stype in scalars:
                classname = self.get_type(
                    stype,
                    aspect=ModuleAspect.VARIANTS,
                    import_time=ImportTime.late_runtime,
                )
                stype_ident = ident(stype.schemapath.name)
                self.write(f"{stype_ident} = {classname}")
                self.export(stype_ident)

    def write_globals(
        self,
        globals_list: list[reflection.Global],
    ) -> None:
        for glob in globals_list:
            self._write_global(glob)

    def _write_global(
        self,
        glob: reflection.Global,
    ) -> None:
        type_ = self.get_type(
            glob.get_type(self._types),
            import_time=ImportTime.typecheck_runtime,
        )
        name = ident(glob.schemapath.name)

        with self.aspect(ModuleAspect.VARIANTS):
            global_ = self.import_name(BASE_IMPL, "Global")
            name = ident(glob.schemapath.name)
            with self._class_def(name, [global_]):
                self.write_schema_reflection(glob)

        impl = self.get_object(
            glob.schemapath,
            aspect=ModuleAspect.VARIANTS,
        )
        self.write(f"{name} = {impl}.global_({type_})")
        self.write()

    def prepare_namespace(self, mod: IntrospectedModule) -> None:
        with self.aspect(ModuleAspect.VARIANTS):
            if self.canonical_modpath == SchemaPath("std"):
                # Only std "defines" generic types at the moment.
                self.py_file.update_globals(gt.name for gt in GENERIC_TYPES)

        self.py_file.update_globals(
            ident(t.schemapath.name)
            for t in itertools.chain(
                mod["scalar_types"].values(),
                mod["object_types"].values(),
                mod["globals"],
            )
        )

    def write_generic_types(
        self,
        mod: IntrospectedModule,
    ) -> None:
        if self.canonical_modpath != SchemaPath("std"):
            # Only std "defines" generic types at the moment.
            return

        funcs = mod["functions"]
        with self.aspect(ModuleAspect.VARIANTS):
            anytype = self.get_type(
                self._types_by_name["anytype"],
                import_time=ImportTime.typecheck,
            )
            anypoint = self.get_type(
                self._types_by_name["std::anypoint"],
                import_time=ImportTime.typecheck,
            )
            typevartup = self.import_name("typing_extensions", "TypeVarTuple")
            unpack = self.import_name("typing_extensions", "Unpack")
            tup = self.import_name(BASE_IMPL, "Tuple")
            arr = self.import_name(BASE_IMPL, "Array")
            rang = self.import_name(BASE_IMPL, "Range")
            mrang = self.import_name(BASE_IMPL, "MultiRange")

            t_anytype = self.declare_typevar("_T_anytype", bound=anytype)
            t_anypt = self.declare_typevar("_T_anypoint", bound=anypoint)
            self.write(f'_Tt = {typevartup}("_Tt")')

            generics = {
                SchemaPath("std", "tuple"): f"{tup}[{unpack}[_Tt]]",
                SchemaPath("std", "array"): f"{arr}[{t_anytype}]",
                SchemaPath("std", "range"): f"{rang}[{t_anypt}]",
                SchemaPath("std", "multirange"): f"{mrang}[{t_anypt}]",
            }
            for gt, base in generics.items():
                with self._class_def(gt.name, [base]):
                    ctors = [f for f in funcs if f.schemapath == gt]
                    if ctors:
                        self.write_functions(
                            ctors,
                            style="constructor",
                            type_ignore=["misc"],
                        )
                    else:
                        self.write("pass")
                self.write_section_break()

        with self.code_section(CodeSection.after_late_import):
            for gt in sorted(GENERIC_TYPES):
                rel_import = self._resolve_rel_import(
                    gt.parent, ModuleAspect.VARIANTS
                )
                assert rel_import is not None
                imported_name = self.import_qual_name(
                    rel_import.module,
                    gt.name,
                    import_time=ImportTime.late_runtime,
                    suggested_module_alias=rel_import.module_alias,
                )
                type_ident = ident(gt.name)
                self.write(f"{type_ident} = {imported_name}")
                self.export(type_ident)

    def _write_enum_scalar_type(
        self,
        stype: reflection.ScalarType,
    ) -> None:
        type_name = stype.schemapath
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
        type_name = stype.schemapath
        tname = type_name.name

        # Find runtime base class for this scalar type, which might
        # or might not be the builtin Python type, because we are
        # necessarily wrapping some of them.
        #
        rt_py_base_names = _qbmodel.get_py_base_for_scalar(
            stype.name,
            require_subclassable=True,
            consider_generic=False,
        )
        if rt_py_base_names:
            rt_py_base = sorted(
                self.import_name(*t, import_time=ImportTime.runtime)
                for t in rt_py_base_names
            )
        else:
            rt_py_base = None

        typecheck_meta_parents: list[str] = []
        descriptor_get_overload: str | None = None

        if rt_py_base is not None:
            # Raw base class would always be the builtin/stdlib
            # unwrapped type.
            raw_py_base_names = _qbmodel.get_py_type_for_scalar(
                stype.name,
                consider_generic=False,
            )
            assert len(raw_py_base_names) == 1
            raw_py_base_name = raw_py_base_names[0]
            raw_py_base = self.import_name(
                *raw_py_base_name, import_time=ImportTime.runtime
            )
            py_type_scalar = self.import_name(BASE_IMPL, "PyTypeScalar")
            raw_py_base_wrapper = f"{py_type_scalar}[{raw_py_base}]"
            runtime_parents = [*rt_py_base, raw_py_base_wrapper]
            ambiguity = _qbmodel.get_scalar_type_disambiguation_for_py_type(
                raw_py_base_name
            )
            if ambiguity:
                descriptor_get_overload = raw_py_base
                typecheck_parents = [raw_py_base_wrapper]
            else:
                typecheck_parents = [*runtime_parents]
                typecheck_meta_parent_names = (
                    _qbmodel.get_py_type_typecheck_meta_bases(raw_py_base_name)
                )
                typecheck_meta_parents.extend(
                    self.import_name(*tn, import_time=ImportTime.typecheck)
                    for tn in typecheck_meta_parent_names
                )
        else:
            typecheck_parents = []
            runtime_parents = []

        scalar_bases = [
            self.get_type(self._types[base.id]) for base in stype.bases
        ]

        typecheck_parents.extend(scalar_bases)
        runtime_parents.extend(scalar_bases)

        if not runtime_parents:
            typecheck_parents = [self.import_name(BASE_IMPL, "GelScalarType")]
            runtime_parents = typecheck_parents

        self.write()

        class_kwargs = {}
        write_meta = self._schema_part is SchemaPart.STD

        if write_meta:
            if scalar_bases:
                meta_bases = _map_name(
                    lambda n: f"__{n}_meta__",
                    scalar_bases,
                )
            else:
                gel_type_meta = self.import_name(BASE_IMPL, "GelTypeMeta")
                meta_bases = [gel_type_meta]

            if typecheck_meta_parents:
                with self.type_checking():
                    if len(typecheck_meta_parents) > 1:
                        with self._class_def(
                            f"__{tname}_meta_base__", typecheck_meta_parents
                        ):
                            self.write("pass")
                    else:
                        self.write(
                            f"__{tname}_meta_base__ = "
                            f"{typecheck_meta_parents[0]}"
                        )
                with self.not_type_checking():
                    self.write(f"__{tname}_meta_base__ = type")

                meta_bases.append(f"__{tname}_meta_base__")

            tmeta = f"__{tname}_meta__"
            with self._class_def(tmeta, meta_bases):
                un_ops = self._write_prefix_operator_methods(stype)
                bin_ops = self._write_infix_operator_methods(stype)
                if not un_ops and not bin_ops:
                    self.write("pass")
            class_kwargs["metaclass"] = tmeta
        else:
            tmeta = None

        with self.type_checking():
            with self._class_def(
                tname, typecheck_parents, class_kwargs=class_kwargs
            ):
                self.write_type_reflection(stype)
                if descriptor_get_overload is not None:
                    any_ = self.import_name("typing", "Any")
                    type_ = self.import_name("builtins", "type")
                    self_ = self.import_name("typing_extensions", "Self")
                    type_self = f"{type_}[{self_}]"

                    with self._method_def(
                        "__get__",
                        [
                            "instance: None",
                            f"owner: {type_}[{any_}]",
                            "/",
                        ],
                        f"{type_}[{self_}]",
                        overload=True,
                        type_ignore=("override", "unused-ignore"),
                    ):
                        self.write("...")

                    with self._method_def(
                        "__get__",
                        [
                            f"instance: {any_}",
                            f"owner: {type_}[{any_}] | None = None",
                            "/",
                        ],
                        descriptor_get_overload,
                        overload=True,
                    ):
                        self.write("...")

                    with self._method_def(
                        "__get__",
                        [
                            f"instance: {any_}",
                            f"owner: {type_}[{any_}] | None = None",
                            "/",
                        ],
                        f"{descriptor_get_overload} | {type_self}",
                        type_ignore=("override", "unused-ignore"),
                    ):
                        self.write("...")

        self.write()

        with self.not_type_checking():
            classvar = self.import_name(
                "typing", "ClassVar", import_time=ImportTime.typecheck
            )
            with self._class_def(tname, runtime_parents):
                if tmeta:
                    self.write(
                        f"__gel_type_class__: {classvar}[type] = {tmeta}"
                    )
                    self.write()
                self.write_type_reflection(stype)

        self.write_section_break()

    def render_callable_return_type(
        self,
        tp: reflection.Type,
        typemod: reflection.TypeModifier,
        *,
        import_time: ImportTime = ImportTime.typecheck,
        typevars: Mapping[reflection.Type, str] | None = None,
    ) -> str:
        result = self.get_type_type(
            tp,
            import_time=import_time,
            typevars=typevars,
        )
        if typemod is reflection.TypeModifier.Optional:
            result = f"{result} | None"

        return result

    def render_callable_sig_type(
        self,
        tp: reflection.Type,
        typemod: reflection.TypeModifier,
        *,
        default: str | None = None,
        typevars: Mapping[reflection.Type, str] | None = None,
        import_time: ImportTime = ImportTime.typecheck,
    ) -> str:
        result = self.get_type_type(
            tp,
            import_time=import_time,
            typevars=typevars,
        )

        return self._render_callable_sig_type(
            result,
            typemod=typemod,
            default=default,
        )

    def _render_callable_sig_type(
        self,
        typ: str,
        typemod: reflection.TypeModifier,
        default: str | None = None,
    ) -> str:
        result = typ
        if typemod is reflection.TypeModifier.Optional:
            result = f"{result} | None"

        if default is not None:
            unspec_t = self.import_name(BASE_IMPL, "UnspecifiedType")
            unspec = self.import_name(BASE_IMPL, "Unspecified")
            result = f"{result} | {unspec_t} = {unspec}"

        return result

    def _write_prefix_op_method_node_ctor(
        self,
        op: reflection.Operator,
    ) -> None:
        """Generate the query node constructor for a prefix operator method.

        Creates the code that builds a PrefixOp query node for unary operator
        methods like __neg__. The operator is applied to 'self' as the
        operand.

        Args:
            op: The operator reflection object containing metadata
        """
        op_name = op.schemapath.name
        node_cls = self.import_name(BASE_IMPL, "PrefixOp")

        args = [
            "expr=self",  # The operand is always 'self' for method calls
            f'op="{op_name}"',  # Gel operator name (e.g., "-", "+")
            "type_=__rtype__.__gel_reflection__.name",  # Result type info
        ]

        self.write(self.format_list(f"{node_cls}({{list}}),", args))

    def _write_prefix_operator_methods(
        self,
        stype: reflection.Type,
    ) -> bool:
        """Generate Python magic methods for unary operators on a type.

        Creates unary operator methods (__neg__, __pos__, etc.) for all prefix
        operators that can be applied to the given type. Only generates methods
        for operators that have Python magic method equivalents.

        Args:
            stype: The type to generate unary operator methods for

        Returns:
            True if any operator methods were generated, False otherwise
        """
        # Find unary operators for this type that have Python magic methods
        un_ops = [
            op
            for op in self._operators.unary_ops.get(stype.id, [])
            if op.py_magic is not None
        ]
        if not un_ops:
            return False
        else:
            self._write_callables(
                un_ops,
                style="method",
                type_ignore=("override", "unused-ignore"),
                node_ctor=self._write_prefix_op_method_node_ctor,
                param_getter=lambda f: f.params[1:],  # Skip first param (self)
            )
            return True

    def _write_prefix_op_func_node_ctor(
        self,
        op: reflection.Operator,
    ) -> None:
        """Generate the query node constructor for a prefix operator function.

        Creates the code that builds a PrefixOp query node for unary operator
        functions. Unlike method versions, this takes the operand from function
        arguments and applies special type casting for tuple parameters.

        Args:
            op: The operator reflection object containing metadata
        """
        op_name = op.schemapath.name
        node_cls = self.import_name(BASE_IMPL, "PrefixOp")
        expr_compat = self.import_name(BASE_IMPL, "ExprCompatible")
        cast_ = self.import_name("typing", "cast")

        other = "__args__[0]"
        # Tuple parameters need ExprCompatible casting
        # due to a possible mypy bug.
        if reflection.is_tuple_type(op.params[0].get_type(self._types)):
            other = f"{cast_}({expr_compat!r}, {other})"

        args = [
            f"expr={other}",
            f'op="{op_name}"',  # Gel operator name (e.g., "-", "+")
            "type_=__rtype__.__gel_reflection__.name",  # Result type info
        ]

        self.write(self.format_list(f"{node_cls}({{list}}),", args))

    def write_non_magic_prefix_operators(
        self,
        ops: list[reflection.Operator],
    ) -> bool:
        """Generate standalone functions for unary operators without Python
        magic methods.

        Creates function-style operators for prefix operations that don't have
        corresponding Python magic methods. Unlike magic method operators,
        these are generated as standalone functions rather than methods on
        types.

        Args:
            ops: List of operator reflection objects to process

        Returns:
            True if any operator functions were generated, False otherwise
        """
        # Filter to unary operators without Python magic method equivalents
        un_ops = [op for op in ops if op.py_magic is None]
        if not un_ops:
            return False
        else:
            self._write_callables(
                un_ops,
                style="function",
                type_ignore=("override", "unused-ignore"),
                node_ctor=self._write_prefix_op_func_node_ctor,
            )
            return True

    class InfixOpNodeConstructor(Protocol):
        def __call__(
            self,
            op: reflection.Operator,
            *,
            swapped: bool = False,
        ) -> None: ...

    def _write_infix_op_func_node_ctor(
        self,
        op: reflection.Operator,
        *,
        swapped: bool = False,
    ) -> None:
        op_name = op.schemapath.name
        node_cls = self.import_name(BASE_IMPL, "InfixOp")
        expr_compat = self.import_name(BASE_IMPL, "ExprCompatible")
        cast_ = self.import_name("typing", "cast")

        if not swapped:
            this_idx = 0
            other_idx = 1
        else:
            this_idx = 1
            other_idx = 0

        this = f"__args__[{this_idx}]"
        other = f"__args__[{other_idx}]"

        if reflection.is_tuple_type(
            op.params[other_idx].get_type(self._types)
        ):
            other = f"{cast_}({expr_compat!r}, {other})"

        args = [
            f"lexpr={this}",
            f'op="{op_name}"',
            f"rexpr={other}",
            "type_=__rtype__.__gel_reflection__.name",
        ]

        self.write(self.format_list(f"{node_cls}({{list}}),", args))

    def write_non_magic_infix_operators(
        self,
        ops: list[reflection.Operator],
    ) -> bool:
        """Generate standalone functions for operators without Python magic
        methods.

        Creates function-style operators for binary operations that don't have
        corresponding Python magic methods. Unlike magic method operators,
        these are generated as standalone functions rather than methods
        on types.

        Args:
            ops: List of operator reflection objects to process

        Returns:
            True if any operator functions were generated, False otherwise
        """
        # Filter to operators without Python magic method equivalents
        bin_ops = [op for op in ops if op.py_magic is None]
        if not bin_ops:
            return False
        else:
            # Create swapped versions for all non-magic operators since they
            # don't have inherent left/right precedence like Python operators
            swapped_ops = [
                dataclasses.replace(op, id=str(uuid.uuid4())) for op in bin_ops
            ]
            self._write_infix_operators(
                bin_ops,
                swapped_ops,
                style="function",
                node_ctor=self._write_infix_op_func_node_ctor,
            )
            return True

    def _write_infix_op_method_node_ctor(
        self,
        op: reflection.Operator,
        *,
        swapped: bool = False,
    ) -> None:
        """Generate the query node constructor for an infix operator method.

        Creates the code that builds the appropriate qb query node (InfixOp
        or IndexOp) for a binary operator method like __add__, __getitem__,
        etc.

        Args:
            op: The operator reflection object containing metadata
            swapped: If True, generates right-hand operation (e.g., __radd__)
                where 'other' becomes left operand and 'self' becomes right
        """
        op_name = op.schemapath.name
        # Map special operators to their specific node classes
        node_cls_map = {
            "[]": "IndexOp",  # Array/container indexing gets special handling
        }

        node_cls_name = node_cls_map.get(op_name, "InfixOp")
        node_cls = self.import_name(BASE_IMPL, node_cls_name)
        expr_compat = self.import_name(BASE_IMPL, "ExprCompatible")
        cast_ = self.import_name("typing", "cast")

        other = "__args__[0]"  # The other operand from method arguments
        # Tuple parameters need ExprCompatible casting due to type limits
        if reflection.is_tuple_type(op.params[0].get_type(self._types)):
            other = f"{cast_}({expr_compat!r}, {other})"

        args = [
            f'op="{op_name}"',  # Gel operator name (e.g., "+", "[]")
            "type_=__rtype__.__gel_reflection__.name",  # Result type info
        ]

        if swapped:
            args.extend([f"lexpr={other}", "rexpr=self"])
        else:
            args.extend(["lexpr=self", f"rexpr={other}"])

        self.write(self.format_list(f"{node_cls}({{list}}),", args))

    def _write_infix_operator_methods(
        self,
        stype: reflection.Type,
    ) -> bool:
        """Generate Python magic methods for binary operators on a type.

        Creates both normal operator methods (__add__, __getitem__, etc.) and
        their right-hand counterparts (__radd__, etc.) for all binary operators
        that can be applied to the given type. Only generates methods for
        operators that have Python magic method equivalents.

        Args:
            stype: The type to generate operator methods for

        Returns:
            True if any operator methods were generated, False otherwise
        """
        # Find binary operators for this type that have Python magic methods
        bin_ops = [
            op
            for op in self._operators.binary_ops.get(stype.id, [])
            if op.py_magic is not None
        ]
        if not bin_ops:
            return False
        else:
            # Create swapped versions for operators where the right operand
            # matches our type (enables right-hand methods like __radd__)
            swapped_ops = [
                dataclasses.replace(
                    op,
                    id=str(uuid.uuid4()),
                    py_magic=(op.swapped_infix_ident,),  # __radd__ vs __add__
                )
                for op in bin_ops
                if op.swapped_infix_ident is not None
                and op.params[1].get_type(self._types) == stype
            ]

            self._write_infix_operators(
                bin_ops,
                swapped_ops,
                style="method",
                node_ctor=self._write_infix_op_method_node_ctor,
            )
            return True

    def _write_infix_operators(
        self,
        bin_ops: list[reflection.Operator],
        swapped_ops: list[reflection.Operator],
        *,
        style: Literal["method", "function"],
        node_ctor: InfixOpNodeConstructor,
    ) -> None:
        """Generate code for binary operators and their swapped counterparts.

        Generates both normal binary operators (like __add__) and their
        right-hand counterparts (like __radd__) while avoiding overload
        overlaps. Uses exclusion lists to prevent swapped operators from
        accepting parameter types that are already handled by explicit normal
        operators.

        Args:
            bin_ops: List of normal binary operators to generate
            swapped_ops: List of swapped operators to generate
            style: Whether to generate as methods or standalone functions
            node_ctor: Factory function to create query nodes for operators
        """
        if swapped_ops:
            # Build exclusion map to prevent type conflicts between normal and
            # swapped operators. If A + B is explicitly defined, then
            # B.__radd__ should not accept type A to avoid ambiguous dispatch.
            explicit = frozenset(
                op.params[0].get_type(self._types) for op in bin_ops
            )
            excluded_param_types: dict[
                reflection.Operator,
                dict[reflection.CallableParamKey, frozenset[reflection.Type]],
            ] = {
                op: {
                    0: frozenset(
                        other_op.params[0].get_type(self._types)
                        for other_op in self._operators.binary_ops_by_name.get(
                            op.name, frozenset()
                        )
                        if other_op.params[0].get_type(self._types)
                        not in explicit
                    ),
                }
                for op in swapped_ops
            }
        else:
            excluded_param_types = {}

        if style == "method":

            def param_getter(
                f: reflection.Operator,
            ) -> Iterable[reflection.CallableParam]:
                # Swapped ops take only first param (self is implicit left
                # operand) Normal ops skip first param (self becomes explicit
                # left operand)
                return f.params[:1] if f in swapped_ops else f.params[1:]
        else:
            # For functions, use all parameters as-is
            def param_getter(
                f: reflection.Operator,
            ) -> Iterable[reflection.CallableParam]:
                return f.params

        self._write_callables(
            itertools.chain(bin_ops, swapped_ops),
            style=style,
            type_ignore=("override", "unused-ignore"),
            param_getter=param_getter,
            node_ctor=lambda op: node_ctor(op, swapped=op in swapped_ops),
            excluded_param_types=excluded_param_types,
        )

    def _partition_nominal_overloads(
        self,
        callables: Iterable[_Callable_T],
    ) -> dict[str, list[_Callable_T]]:
        """Partition a sequence of callables into a dictionary of lists of
        callables by name."""
        result: defaultdict[str, list[_Callable_T]] = defaultdict(list)
        for cb in callables:
            result[cb.ident].append(cb)
        return result

    def _partition_potentially_overlapping_overloads(
        self,
        callables: Sequence[_Callable_T],
    ) -> dict[reflection.CallableSignature, list[_Callable_T]]:
        """Partition a sequence of callables into a dictionary of lists of
        callables that _can_ overlap."""
        result: defaultdict[
            reflection.CallableSignature, list[_Callable_T]
        ] = defaultdict(list)

        # Group callables by overlapping signatures
        for cb in callables:
            sig = cb.signature

            # Find if this signature overlaps with any existing signature
            overlapping_sig = None
            for existing_sig in result:
                if sig.overlaps(existing_sig):
                    overlapping_sig = existing_sig
                    break

            if overlapping_sig is not None:
                # Add to existing overlapping group
                result[overlapping_sig].append(cb)
            else:
                # Create new group for this signature
                result[sig].append(cb)

        return result

    def _write_callables(
        self,
        callables: Iterable[_Callable_T],
        *,
        style: Literal["method", "function", "constructor"],
        type_ignore: Sequence[str] = (),
        param_getter: reflection.CallableParamGetter[
            _Callable_T
        ] = operator.attrgetter("params"),
        node_ctor: Callable[[_Callable_T], None] | None = None,
        excluded_param_types: Mapping[
            _Callable_T,
            dict[reflection.CallableParamKey, frozenset[reflection.Type]],
        ]
        | None = None,
    ) -> None:
        partitions = self._partition_nominal_overloads(callables)
        for overloads in partitions.values():
            self._write_nominal_overloads(
                overloads,
                style=style,
                type_ignore=type_ignore,
                param_getter=param_getter,
                node_ctor=node_ctor,
                excluded_param_types=excluded_param_types,
            )

    def _write_nominal_overloads(
        self,
        overloads: Sequence[_Callable_T],
        *,
        style: Literal["method", "function", "constructor"],
        type_ignore: Sequence[str] = (),
        param_getter: reflection.CallableParamGetter[_Callable_T],
        node_ctor: Callable[[_Callable_T], None] | None = None,
        excluded_param_types: Mapping[
            _Callable_T,
            dict[reflection.CallableParamKey, frozenset[reflection.Type]],
        ]
        | None = None,
    ) -> None:
        """Generate code for a group of function overloads with the same name.

        Processes overloads by minimizing and sorting them, partitioning them
        into potentially overlapping groups, and generating both the individual
        overload implementations and a dispatcher function when multiple
        overloads exist.

        Args:
            overloads: Sequence of callable objects to generate overloads for
            style: Code generation style (method, function, or constructor)
            type_ignore: Additional mypy type ignore codes to include
            param_getter: Function to extract parameters from callables
            node_ctor: Custom node constructor for query building
            excluded_param_types: Parameter types to exclude from specific
                overloads to prevent type conflicts
        """
        if len(overloads) > 1:
            # Remove redundant overloads
            overloads = self._minimize_overloads(
                overloads,
                param_getter=param_getter,
            )

        # Group overloads that might have overlapping parameter types
        partitions = self._partition_potentially_overlapping_overloads(
            overloads
        )
        num_generated_total = 0
        for overlapping in partitions.values():
            generated = self._write_potentially_overlapping_overloads(
                overlapping,
                style=style,
                nominal_overloads_total=len(overloads),
                type_ignore=type_ignore,
                param_getter=param_getter,
                node_ctor=node_ctor,
                excluded_param_types=excluded_param_types,
            )
            num_generated_total += len(generated)

        if num_generated_total > 1:
            # Generate dispatcher that delegates to appropriate overload
            self._write_function_overload_dispatcher(
                overloads[0],
                style=style,
            )

    def _write_potentially_overlapping_overloads(
        self,
        overloads: list[_Callable_T],
        *,
        style: Literal["method", "function", "constructor"],
        type_ignore: Sequence[str] = (),
        nominal_overloads_total: int,
        param_getter: reflection.CallableParamGetter[_Callable_T],
        node_ctor: Callable[[_Callable_T], None] | None = None,
        excluded_param_types: Mapping[
            _Callable_T,
            dict[reflection.CallableParamKey, frozenset[reflection.Type]],
        ]
        | None = None,
    ) -> list[_Callable_T]:
        """Generate overloads that may have overlapping parameter types.

        Performs type expansion by adding implicit casts and Python type
        coercions while avoiding conflicts. Each overload gets expanded to
        accept additional compatible types through implicit casting, but
        only if doing so wouldn't create ambiguous dispatch with other
        overloads in the same group.

        The process:
        1. Collect explicit parameter types from all overloads
        2. Add implicit casts where they don't create overlaps
        3. Add Python type coercions with proper ranking while also checking
           for overlaps
        4. Generate the final overload implementations

        Args:
            overloads: List of potentially overlapping overloads to process
            style: Code generation style (method, function, or constructor)
            type_ignore: Additional mypy type ignore codes
            nominal_overloads_total: Total number of nominal overloads
            param_getter: Function to extract parameters from callables
            node_ctor: Custom node constructor for query building
            excluded_param_types: Parameter types to exclude from overloads
        """
        overload_signatures: dict[
            _Callable_T, reflection.CallableParamTypeMap
        ] = {}

        # Signature union: all possible parameters with sets of overloads
        # that have them.
        param_overload_map: defaultdict[
            reflection.CallableParamKey, set[_Callable_T]
        ] = defaultdict(set)

        # Sort overloads by generality from least generic to most generic
        generality_key = functools.cmp_to_key(
            functools.partial(
                reflection.compare_callable_generality,
                schema=self._types,
            )
        )
        overloads = sorted(overloads, key=generality_key)

        for overload in overloads:
            overload_signatures[overload] = {}
            for param in param_getter(overload):
                param_overload_map[param.key].add(overload)
                param_type = param.get_type(self._types)
                # Unwrap the variadic type (it is reflected as an array of T)
                if param.kind is reflection.CallableParamKind.Variadic:
                    assert reflection.is_array_type(param_type)
                    param_type = param_type.get_element_type(self._types)
                # Start with the base parameter type
                overload_signatures[overload][param.key] = [param_type]

        overloads_specializations: dict[_Callable_T, list[_Callable_T]] = {}
        num_specializations = 0
        base_scalars = _qbmodel.get_base_scalars_backed_by_py_type()
        overlapping_py_types = {
            t: i for i, t in enumerate(_qbmodel.get_overlapping_py_types())
        }
        num_overlapping_py_types = len(overlapping_py_types)

        def specialization_sort_key(t: reflection.Type) -> int:
            return overlapping_py_types.get(
                base_scalars[t.name],
                num_overlapping_py_types,
            )

        for overload in overloads:
            generics = overload.generics(self._types)
            if (gen_return := generics.get("__return__")) is not None:
                overload_specializations = []
                gen_type_map: dict[reflection.Type, list[reflection.Type]] = {}
                in_params: defaultdict[
                    reflection.Type,
                    list[tuple[CallableParamKey, AbstractSet[Indirection]]],
                ] = defaultdict(list)
                for param_key, param_generics in generics.items():
                    if param_key != "__return__":
                        for type_, paths in param_generics.items():
                            in_params[type_].append((param_key, paths))

                for gen_t in gen_return:
                    if gen_t not in in_params:
                        continue

                    if isinstance(gen_t, reflection.ScalarType):
                        descendants = gen_t.descendants(self._types)
                        gen_type_map[gen_t] = sorted(
                            (
                                descendant
                                for descendant in descendants
                                if descendant.name in base_scalars
                            ),
                            key=specialization_sort_key,
                        )
                    elif gen_t.name == "anytype":
                        gen_type_map[gen_t] = sorted(
                            (
                                self._types_by_name[tn]
                                for tn in base_scalars
                                if tn in self._types_by_name
                            ),
                            key=specialization_sort_key,
                        )

                for gen_t, specializations in gen_type_map.items():
                    if not specializations:
                        continue

                    for specialization in specializations:
                        param_specializations = {
                            param_key: dict.fromkeys(
                                paths,
                                (gen_t, specialization),
                            )
                            for param_key, paths in (
                                ("__return__", gen_return[gen_t]),
                                *in_params[gen_t],
                            )
                        }

                        specialized = overload.specialize(
                            param_specializations, schema=self._types
                        )
                        spec_param_types: reflection.CallableParamTypeMap = {
                            param.key: [param.get_type(self._types)]
                            for param in param_getter(specialized)
                        }

                        if not self._would_cause_overlap(
                            overload,
                            spec_param_types,
                            overload_signatures,
                        ):
                            overload_specializations.append(specialized)
                            overload_signatures[specialized] = spec_param_types

                            for param in param_getter(specialized):
                                param_overload_map[param.key].add(specialized)

                overloads_specializations[overload] = overload_specializations
                num_specializations += len(overload_specializations)

        if num_specializations > 0:
            nominal_overloads_total += num_specializations

            # Splice generic specializations into the overloads list
            expanded_overloads: list[_Callable_T] = []
            for overload in overloads:
                if overload_specs := overloads_specializations[overload]:
                    expanded_overloads.extend(overload_specs)
                expanded_overloads.append(overload)
            overloads = expanded_overloads

        # Track which types are explicitly used to avoid creating overlaps
        # when adding implicit casts
        param_type_usages: defaultdict[
            reflection.CallableParamKey, set[reflection.Type]
        ] = defaultdict(set)
        for overload in overloads:
            for param in overload.params:
                param_type = param.get_type(self._types)
                # Unwrap the variadic type (it is reflected as an array of T)
                if param.kind is reflection.CallableParamKind.Variadic:
                    assert reflection.is_array_type(param_type)
                    param_type = param_type.get_element_type(self._types)
                param_type_usages[param.key].add(param_type)

        # Add implicit casts to each overload where they don't cause conflicts
        implicit_casts_map = self._casts.implicit_casts_to
        for param_key, param_overloads in param_overload_map.items():
            potentially_overlapping_overloads = {
                k: v
                for k, v in overload_signatures.items()
                if k in param_overloads
            }
            cast_rankings: defaultdict[
                reflection.Type, list[tuple[int, _Callable_T]]
            ] = defaultdict(list)
            for param_overload in param_overloads:
                param = param_overload.param_map[param_key]
                param_type = param.get_type(self._types)
                if icasts := implicit_casts_map.get(param_type.id):
                    for icast_type_id, icast_dist in icasts.items():
                        icast_type = self._types[icast_type_id]
                        cast_rankings[icast_type].append(
                            (icast_dist, param_overload)
                        )

            for icast_type, ranking in cast_rankings.items():
                ranking.sort(key=lambda v: (v[0], generality_key(v[1])))

                for _, param_overload in ranking:
                    overload_param_types = overload_signatures[param_overload]
                    param_types = overload_param_types[param_key]
                    param_types.append(icast_type)
                    # Remove the cast if it would cause overlap
                    if self._would_cause_overlap(
                        param_overload,
                        overload_param_types,
                        potentially_overlapping_overloads,
                    ):
                        param_types.pop()

        # Build mapping of Python types to overloads with ranking
        # (lower rank == higher priority for overload).
        param_py_overload_map: defaultdict[
            reflection.CallableParamKey,
            defaultdict[
                PyTypeName,
                list[tuple[int, _Callable_T, reflection.Type]],
            ],
        ] = defaultdict(lambda: defaultdict(list))

        # Collect Python type coercions for scalar parameters
        for overload, params in overload_signatures.items():
            if excluded_param_types is not None:
                ov_excluded = excluded_param_types.get(overload, {})
            else:
                ov_excluded = {}
            for param_key, param_types in params.items():
                excluded = ov_excluded.get(param_key)
                for stype in param_types:
                    # Only process scalar types that aren't excluded
                    if not isinstance(stype, reflection.ScalarType) or (
                        excluded is not None and stype in excluded
                    ):
                        continue

                    # Get corresponding Python types for this scalar type
                    py_types = _qbmodel.get_py_type_for_scalar_hierarchy(
                        self._get_scalar_hierarchy(stype),
                        consider_generic=True,
                    )
                    for py_type in py_types:
                        # Rank how well this Python type matches the scalar
                        scalar_rank = _qbmodel.get_py_type_scalar_match_rank(
                            py_type, stype.name
                        )
                        if scalar_rank is not None:
                            param_py_overload_map[param_key][py_type].append(
                                (scalar_rank, overload, stype)
                            )

        # Track which Python types are successfully added to each overload
        param_py_scalar_map: defaultdict[
            reflection.CallableParamKey,
            defaultdict[PyTypeName, dict[_Callable_T, reflection.Type]],
        ] = defaultdict(lambda: defaultdict(dict))

        overloads_type_ignores: dict[_Callable_T, list[str]] = {}

        # Add Python type coercions in ranking order, avoiding overlaps
        for param_key, param_py_ranks in param_py_overload_map.items():
            potentially_overlapping_py_overloads = {
                k: v
                for k, v in overload_signatures.items()
                if k in param_overload_map[param_key]
            }
            for py_type, ranked_overloads in param_py_ranks.items():
                # Process overloads in rank order (best matches first)
                ranked_overloads.sort(
                    key=lambda v: (v[0], generality_key(v[1]))
                )
                for _, overload, st in ranked_overloads:
                    overload_param_py_types = overload_signatures[overload]
                    param_py_types = overload_param_py_types[param_key]
                    param_py_types.append(py_type)
                    # Check if adding this Python type would cause overlap
                    overload_param_py_types_only = {
                        k: [
                            pt
                            for pt in v
                            if not isinstance(pt, reflection.Type)
                            or k != param_key
                        ]
                        for k, v in overload_param_py_types.items()
                    }
                    if self._would_cause_overlap(
                        overload,
                        overload_param_py_types_only,
                        potentially_overlapping_py_overloads,
                    ):
                        param_py_types.pop()
                    else:
                        if py_type in overlapping_py_types and (
                            overlapping_with := self._would_cause_overlap(
                                overload,
                                overload_param_py_types_only,
                                potentially_overlapping_py_overloads,
                                consider_py_inheritance=True,
                            )
                        ):
                            overloads_type_ignores[overlapping_with] = [
                                "overload-overlap",
                                "unused-ignore",
                            ]

                        # Successfully added - record the mapping
                        scalar_map = param_py_scalar_map[param_key][py_type]
                        if overload not in scalar_map:
                            scalar_map[overload] = st

        # Generate the final overload implementations with expanded types
        for overload in overloads:
            params = overload_signatures[overload]
            param_cast_map: dict[
                reflection.CallableParamKey, dict[str, reflection.Type]
            ] = {}
            param_type_unions: dict[
                reflection.CallableParamKey, list[reflection.Type]
            ] = {}
            if excluded_param_types is not None:
                overload_excluded_pt = excluded_param_types.get(overload)
            else:
                overload_excluded_pt = None
            for param_idx, param_types in params.items():
                param_type_union: list[reflection.Type] = []
                py_coerce_map: dict[str, reflection.Type] = {}
                if overload_excluded_pt is not None:
                    excluded = overload_excluded_pt.get(param_idx)
                else:
                    excluded = None
                for t in param_types:
                    if isinstance(t, reflection.Type):
                        if excluded is not None and t in excluded:
                            continue
                        param_type_union.append(t)
                    else:
                        # Handle Python type coercions
                        st = param_py_scalar_map[param_idx][t][overload]
                        py_type = t

                        # Import the Python type symbol
                        if proto := _qbmodel.maybe_get_protocol_for_py_type(
                            py_type
                        ):
                            ptype_sym = self.import_name(BASE_IMPL, proto)
                        else:
                            ptype_sym = self.import_name(*py_type)

                        # Map Python type to canonical Gel scalar type
                        py_coerce_map[ptype_sym] = st

                param_cast_map[param_idx] = py_coerce_map
                param_type_unions[param_idx] = param_type_union

            this_type_ignore: Sequence[str]
            if overload_type_ignores := overloads_type_ignores.get(overload):
                this_type_ignore = sorted(
                    {*overload_type_ignores, *type_ignore}
                )
            else:
                this_type_ignore = type_ignore

            # Generate the actual overload implementation
            self._write_function_overload(
                overload,
                num_overloads_total=nominal_overloads_total,
                param_types=param_type_unions,
                cast_map=param_cast_map,
                style=style,
                type_ignore=this_type_ignore,
                param_getter=param_getter,
                node_ctor=node_ctor,
            )

            self.write()

        return overloads

    def write_object_types(
        self,
        object_types: dict[str, reflection.ObjectType],
    ) -> None:
        if not object_types:
            return

        objtypes = []
        for objtype in self._sorted_types(object_types.values()):
            objtypes.append(objtype)
            type_name = objtype.schemapath
            if objtype.name not in CORE_OBJECTS:
                self.py_file.add_global(type_name.name)
            self.py_file.export(type_name.name)

        for objtype in objtypes:
            if objtype.name not in CORE_OBJECTS:
                # Core objects are "base" by definition
                # so there is no reason to re-define them,
                # just import the base variant.
                self.write_object_type(objtype)

        with self.code_section(CodeSection.after_late_import):
            for objtype in objtypes:
                if objtype.name in CORE_OBJECTS:
                    classname = self.get_type(
                        objtype,
                        aspect=ModuleAspect.VARIANTS,
                        import_time=ImportTime.late_runtime,
                    )
                    objtype_ident = ident(objtype.schemapath.name)
                    self.write(f"{objtype_ident} = {classname}")
                    self.export(objtype_ident)

        with self.aspect(ModuleAspect.VARIANTS):
            for objtype in objtypes:
                type_name = objtype.schemapath
                self.py_file.add_global(type_name.name)

            for objtype in objtypes:
                self.write_object_type_variants(objtype)

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
            self._schema_object_type.schemapath.parent,
            aspect=ModuleAspect.MAIN,
        )
        assert objecttype_import is not None
        uuid = self.import_name("uuid", "UUID")
        with self._class_def(typeof_base_class, typeof_base_bases):
            if not objtype.abstract:
                with self.type_checking():
                    with self._func_def(
                        "__gel_not_abstract__", ["self"], "None"
                    ):
                        self.write("...")

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

        proplinks = self._get_links_with_props(objtype)
        if proplinks:
            self.write_object_type_link_models(objtype)

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
            metaclass = f"__{name}_ops__"

            with self._class_def(metaclass, meta_base_types):
                un_ops = self._write_prefix_operator_methods(objtype)
                bin_ops = self._write_infix_operator_methods(objtype)
                if not un_ops and not bin_ops:
                    self.write("pass")
            with self.type_checking():
                self.write(f"__{name}_meta__ = __{name}_ops__")
            with self.not_type_checking():
                self.write(f"__{name}_meta__ = {gel_model_meta}")

            class_kwargs["metaclass"] = f"__{name}_meta__"

        class_r_kwargs = {
            "__gel_type_id__": f"{uuid}(int={objtype.uuid.int})",
            "__gel_variant__": '"Base"',
        }
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
                    class_kwargs=class_kwargs | {"__gel_variant__": '"Base"'},
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
                    class_kwargs={"__gel_variant__": '"Required"'},
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
                            self._write_model_attribute(ptr.name, ptr_type)
                        self.write()
                    else:
                        self.write("pass")
                        self.write()

                with self._object_type_variant(
                    objtype,
                    variant="PartialBase",
                    base_types=variant_base_types,
                    static_bases=[typeof_partial_class],
                    class_kwargs={"__gel_variant__": '"PartialBase"'},
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
                    class_kwargs={"__gel_variant__": '"Partial"'},
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
                            self._write_model_attribute(ptr.name, ptr_type)
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
        builtin_bool = self.import_name("builtins", "bool")
        builtin_str = self.import_name("builtins", "str")
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
                    select_union.extend(broad_ptr_t)
                union.extend((f"type[{narrow_ptr_t}]", unspec_t))
                select_union.extend((f"type[{narrow_ptr_t}]", unspec_t))
                ptr_t = f"{' | '.join(union)} = {unspec}"
                if not ptr.is_readonly and not ptr.is_computed:
                    update_args.append(f"{ptr.name}: {ptr_t}")
                filter_args.append(f"{ptr.name}: {ptr_t}")
                select_ptr_t = f"{' | '.join(select_union)} = {unspec}"
                select_args.append(f"{ptr.name}: {select_ptr_t}")
                if reflection.is_scalar_type(target_t):
                    order_args.append(
                        f"{ptr.name}: {order_kwarg_t} = {unspec}"
                    )

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

    def _generate_init_args(
        self,
        objtype: reflection.ObjectType,
        *,
        for_init_update: bool = False,
    ) -> list[str]:
        is_schema_type = objtype.name.startswith("schema::")

        init_pointers = _filter_pointers(
            self._get_pointer_origins(objtype),
            exclude_id=False,
        )

        id_ptr: reflection.Pointer | None = None
        id_objtype: reflection.ObjectType | None = None

        if not init_pointers:
            return []

        init_args: list[str] = ["/"]
        init_args_appended_star = False

        if for_init_update or is_schema_type:
            for ptr, arg_objtype in init_pointers:
                if ptr.name == "id":
                    id_ptr = ptr
                    id_objtype = arg_objtype
                    break

            assert id_ptr is not None
            assert id_objtype is not None

            ptr_t = self.get_ptr_type(
                id_objtype,
                id_ptr,
                style="arg_no_default",
                prefer_broad_target_type=True,
                consider_default=True,
            )
            init_args.append(f"id: {ptr_t}")

        for ptr, arg_objtype in init_pointers:
            if ptr.name == "id":
                continue

            if ptr.is_computed and not is_schema_type:
                continue

            if not init_args_appended_star:
                init_args.append("*")
                init_args_appended_star = True

            ptr_t = self.get_ptr_type(
                arg_objtype,
                ptr,
                style="arg" if not for_init_update else "unspec_arg",
                prefer_broad_target_type=True,
                consider_default=not for_init_update,
            )

            init_args.append(f"{ptr.name}: {ptr_t}")

        return init_args

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
                    self.write(f"id: {desc}[{ptr_type}, {priv_type}]")
                self.write()

        with self.type_checking():
            render_id_variant = False

            if (
                objtype.name != "std::FreeObject"
                and not objtype.name.startswith("schema::")
                and not objtype.name.startswith("std::")
            ):
                render_id_variant = True

            # Render ID-less variant first so that it's the first
            # overload suggested by tools (common case - new object.)
            init_args = self._generate_init_args(objtype)
            with self._method_def(
                "__init__",
                init_args,
                overload=render_id_variant,
                type_ignore=("misc", "override", "unused-ignore")
                if render_id_variant
                else (),
            ):
                self.write(get_init_new_docstring(objtype.name))
                self.write("...")
                self.write()

            if render_id_variant:
                init_with_id_args = self._generate_init_args(
                    objtype, for_init_update=True
                )
                with self._method_def(
                    "__init__",
                    init_with_id_args,
                    overload=True,
                    type_ignore=("misc", "override", "unused-ignore"),
                ):
                    self.write(get_init_for_update_docsting(objtype.name))
                    self.write("...")
                    self.write()

                any_ = self.import_name("typing", "Any")

                with self._method_def(
                    "__init__",
                    [f"*args: {any_}", f"**kwargs: {any_}"],
                    type_ignore=("misc", "override", "unused-ignore"),
                ):
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
        type_name = objtype.schemapath

        def _filter(ptr: reflection.Pointer) -> bool:
            if not reflection.is_link(ptr):
                return False
            if not ptr.pointers:
                return False

            target_type = self._types[ptr.target_id]
            target_type_name = target_type.schemapath

            return local is None or local == (
                target_type_name.parent == type_name.parent
            )

        return list(filter(_filter, objtype.pointers))

    def write_object_type_link_models(
        self,
        objtype: reflection.ObjectType,
    ) -> None:
        links = self._get_links_with_props(objtype)
        if not links:
            return

        all_ptr_origins = self._get_all_pointer_origins(objtype)
        for link in links:
            self._write_object_type_link_model(
                objtype,
                link=link,
                ptr_origins=all_ptr_origins[link.name],
            )

    def write_object_type_link_variants(
        self,
        objtype: reflection.ObjectType,
        *,
        target_aspect: ModuleAspect = ModuleAspect.MAIN,
        variant: str | None = None,
    ) -> None:
        links = self._get_links_with_props(objtype)
        if not links:
            return

        all_ptr_origins = self._get_all_pointer_origins(objtype)
        lazyclassprop = self.import_name(BASE_IMPL, "LazyClassProperty")
        type_ = self.import_name("builtins", "type")

        with self.type_checking():
            for link in links:
                self._write_object_type_link_variant(
                    objtype,
                    link=link,
                    ptr_origins=all_ptr_origins[link.name],
                    target_aspect=target_aspect,
                    is_forward_decl=True,
                    variant=variant,
                )

        with self.not_type_checking():
            type_name = objtype.schemapath
            obj_class = type_name.name
            for link in links:
                ptrname = link.name
                with self._classmethod_def(
                    ptrname,
                    [],
                    type_,
                    decorators=[f"{lazyclassprop}[{type_}]"],
                ):
                    classname = self._write_object_type_link_variant(
                        objtype,
                        link=link,
                        ptr_origins=all_ptr_origins[link.name],
                        target_aspect=target_aspect,
                        is_forward_decl=False,
                        variant=variant,
                    )
                    self.write(f"{classname}.__name__ = {ptrname!r}")
                    qualname = f"{obj_class}.__links__.{ptrname}"
                    self.write(f"{classname}.__qualname__ = {qualname!r}")
                    self.write(f"return {classname}")

    def _write_object_type_link_model(
        self,
        objtype: reflection.ObjectType,
        *,
        link: reflection.Pointer,
        ptr_origins: list[reflection.ObjectType],
    ) -> str:
        type_name = objtype.schemapath
        srcname = ident(type_name.name)
        linkname = ident(link.name)

        link_clsname = f"__{srcname}_{linkname}_link__"

        ptr_origin_types = [
            self.get_type(
                origin,
                import_time=ImportTime.runtime,
                aspect=self.current_aspect,
            )
            for origin in ptr_origins
        ]

        if ptr_origin_types:
            link_bases = _map_name(
                functools.partial(
                    lambda src, ln: f"__{src}_{ln}_link__",
                    ln=linkname,
                ),
                ptr_origin_types,
            )
            reflection_bases = link_bases
        else:
            b = self.import_name(BASE_IMPL, "GelLinkModel")
            link_bases = [b]
            reflection_bases = []

        # The link properties model class for this link
        with self._class_def(link_clsname, link_bases):
            self.write_link_reflection(link, reflection_bases)

            assert link.pointers
            for lprop in link.pointers:
                if lprop.name in {"source", "target"}:
                    continue
                ttype = self._types[lprop.target_id]
                assert reflection.is_scalar_type(ttype)
                ptr_type = self.get_type(ttype)
                pytype = " | ".join(self._get_pytype_for_scalar(ttype))
                py_anno = self._py_anno_for_ptr(
                    lprop,
                    ptr_type,
                    pytype,
                    reflection.Cardinality(lprop.card),
                )
                self._write_model_attribute(lprop.name, py_anno)

        self.write()

        return link_clsname

    def _write_object_type_link_variant(
        self,
        objtype: reflection.ObjectType,
        *,
        link: reflection.Pointer,
        ptr_origins: list[reflection.ObjectType],
        target_aspect: ModuleAspect | None = None,
        is_forward_decl: bool = False,
        variant: str | None = None,
    ) -> str:
        srcname = ident(objtype.schemapath.name)
        link_name = ident(link.name)

        self_t = self.import_name("typing_extensions", "Self")
        proxymodel_t = self.import_name(BASE_IMPL, "ProxyModel")
        lmdesc = self.import_name(BASE_IMPL, "GelLinkModelDescriptor")

        if target_aspect is None:
            target_aspect = self.current_aspect

        target_type = self._types[link.target_id]
        assert isinstance(target_type, reflection.ObjectType)
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

        classname = link_name if is_forward_decl else f"{srcname}__{link_name}"
        if variant is not None:
            container = f"__links_{variant.lower()}__"
            line_comment = "type: ignore [misc]"
        else:
            container = "__links__"
            line_comment = None

        link_desc = f"{lmdesc}[__{srcname}_{link_name}_link__]"

        assert link.pointers
        lprops = []
        for lprop in link.pointers:
            if lprop.name in {"source", "target"}:
                continue
            ttype = self._types[lprop.target_id]
            assert reflection.is_scalar_type(ttype)
            pytype = " | ".join(self._get_pytype_for_scalar(ttype))
            lprop_line = f"{lprop.name}: {pytype} | None = None"
            lprops.append(lprop_line)

        with self._class_def(
            classname,
            (
                [f"{s}.{container}.{link_name}" for s in ptr_origin_types]
                + [target, f"{proxymodel_t}[{target}]"]
            ),
            line_comment=line_comment,
        ):
            self.write(
                f'"""link {objtype.name}.{link.name}: {target_type.name}"""'
            )

            self.write(f"__linkprops__: {link_desc} = {link_desc}()")
            self.write()

            if is_forward_decl:
                args = [f"obj: {target}", "/", "*", *lprops]
                with self._classmethod_def(
                    "link",
                    args,
                    self_t,
                    line_comment="type: ignore [override]",
                ):
                    if link.card.is_multi():
                        self.write(
                            get_multi_link_for_proxy_docsting(
                                source_type=objtype.name,
                                link_name=link_name,
                                target_type=target_type.name,
                            )
                        )
                    else:
                        self.write(
                            get_single_link_for_proxy_docsting(
                                source_type=objtype.name,
                                link_name=link_name,
                                target_type=target_type.name,
                            )
                        )
                    self.write("...")

            self.write()

        return classname

    def write_object_type(
        self,
        objtype: reflection.ObjectType,
    ) -> None:
        self.write()
        self.write()

        type_name = objtype.schemapath
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

        with self._class_def(
            name,
            base_types,
            class_kwargs=(
                {}
                if self._schema_part is reflection.SchemaPart.USER
                else {"__gel_variant__": '"Default"'}
            ),
        ):
            self.write(f'"""type {objtype.name}"""')
            self.write()
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
                    self._write_model_attribute(ptr.name, ptr_type)
            else:
                self.write("pass")
                self.write()

    def _write_model_attribute(self, name: str, anno: str) -> None:
        decl = f"{name}: {anno}"
        if name in SHADOWED_PYDANTIC_ATTRIBUTES:
            decl += "  # type: ignore [assignment, override, unused-ignore]"
        self.write(decl)

    def write_functions(
        self,
        functions: list[reflection.Function],
        *,
        style: Literal["constructor", "method", "function"] = "function",
        type_ignore: Sequence[str] = (),
    ) -> None:
        self._write_callables(functions, style=style, type_ignore=type_ignore)

    def _get_type_generality_score(self, fn: reflection.Callable) -> int:
        """Calculate how generic a function is based on its parameter and
        return types. Higher score means more specific, lower score means
        more general.
        """
        score = 0

        # Score based on return type
        return_type = fn.get_return_type(self._types)
        # Generic types get a higher score
        if return_type.contained_generics(self._types):
            score -= 100

        # Score based on inheritance depth
        if isinstance(return_type, reflection.InheritingType):
            score += len(return_type.ancestors)

        # Score based on parameter types
        for param in fn.params:
            param_type = param.get_type(self._types)
            # Generic types get a higher score
            if param_type.contained_generics(self._types):
                score -= 100

            # Score based on inheritance depth
            if isinstance(param_type, reflection.InheritingType):
                score += len(param_type.ancestors)

        return score

    def _minimize_overloads(
        self,
        overloads: Sequence[_Callable_T],
        param_getter: reflection.CallableParamGetter[_Callable_T],
    ) -> Sequence[_Callable_T]:
        """Minimize the number of overloads by removing those that are subsumed
        by more general ones.
        """

        if len(overloads) <= 1:
            return overloads

        schema = self._types

        def assignable(a: _Callable_T, b: _Callable_T) -> bool:
            return a.assignable_from(
                b, schema=schema, param_getter=param_getter
            )

        # Remove overloads that can be assigned to more general overloads.
        minimized: list[_Callable_T] = []
        for fn in overloads:
            # Check if this function is subsumed by any function already in
            # minimized
            if not any(assignable(existing, fn) for existing in minimized):
                # If not subsumed, add it and remove any functions it subsumes
                minimized = [ex for ex in minimized if not assignable(fn, ex)]
                minimized.append(fn)

        return minimized

    def _write_function_overload_dispatcher(
        self,
        function: reflection.Callable,
        *,
        style: Literal["constructor", "method", "function"] = "function",
        type_ignore: Sequence[str] = (),
    ) -> None:
        any_ = self.import_name(
            "typing", "Any", import_time=ImportTime.typecheck
        )
        dispatch = self.import_name(BASE_IMPL, "dispatch_overload")
        fdef = (
            self._method_def
            if style in {"method", "constructor"}
            else self._func_def
        )
        fdef = (
            self._method_def
            if style in {"method", "constructor"}
            else self._func_def
        )
        params = [f"*args: {any_}", f"**kwargs: {any_}"]
        if style == "constructor":
            fdef_name = "__new__"
            params.insert(0, "cls")
            implicit_param = False
        else:
            fdef_name = ident(function.ident)
            implicit_param = True

        with fdef(
            fdef_name,
            params,
            any_,
            type_ignore=type_ignore,
            implicit_param=implicit_param,
            decorators=(dispatch,),
        ):
            self.write("pass")
        self.write()

    def _would_cause_overlap(
        self,
        overload: _Callable_T,
        overload_param_types: reflection.CallableParamTypeMap,
        other_overloads: dict[_Callable_T, reflection.CallableParamTypeMap],
        *,
        consider_py_inheritance: bool = False,
    ) -> _Callable_T | None:
        """Check if adding a type to a parameter union in a callable form
        would cause an overload overlap and if so, return the first other
        overload this overload would overlap with.
        """
        schema = self._types
        for other_overload, other_param_types in other_overloads.items():
            if overload != other_overload and overload.overlaps(
                other_overload,
                param_types=other_param_types,
                other_param_types=overload_param_types,
                schema=schema,
                consider_py_inheritance=consider_py_inheritance,
            ):
                return other_overload

        return None

    def _partition_function_params(
        self,
        params: list[str],
        omittable_params: set[str],
        nullable_params: Mapping[str, str],
        params_casts: Mapping[str, str],
    ) -> tuple[list[str], list[str]]:
        """Partition function parameters into mandatory and omittable groups
        with transformations.

        Takes a list of parameter names and applies various transformations
        based on their characteristics (nullable, requiring casts, etc.), then
        splits them into two groups for code generation purposes.

        Args:
            params: List of all parameter names to process
            omittable_params: Set of parameter names that can be omitted
            nullable_params: Map of parameter names that can be empty along
                             with their canonical type.
            params_casts: Mapping from parameter names to type strings for
                          parameters requiring casts

        Returns:
            Tuple of (mandatory_params, omittable_params) where each list
            contains the processed parameter expressions ready for code
            generation. Nullable parameters are wrapped with
            empty_set_if_none() calls, and parameters requiring casts are
            wrapped with typing.cast() calls.
        """
        mandatory = [a for a in params if a not in omittable_params]
        omittable = [a for a in params if a in omittable_params]

        if nullable_params:
            empty_set = self.import_name(BASE_IMPL, "empty_set_if_none")
            mandatory = [
                (
                    f"{empty_set}({a}, {st})"
                    if (st := nullable_params.get(a)) is not None
                    else a
                )
                for a in mandatory
            ]

            omittable = [
                (
                    f"{empty_set}({a}, {st})"
                    if (st := nullable_params.get(a)) is not None
                    else a
                )
                for a in omittable
            ]

        if params_casts:
            cast_ = self.import_name("typing", "cast")

            mandatory = [
                (
                    f"{cast_}({t!r}, {a})"
                    if (t := params_casts.get(a)) is not None
                    else a
                )
                for a in mandatory
            ]

            omittable = [
                (
                    f"{cast_}({t!r}, {a})"
                    if (t := params_casts.get(a)) is not None
                    else a
                )
                for a in omittable
            ]

        return mandatory, omittable

    def _write_function_positional_args_collection(
        self,
        params: list[str],
        omittable_params: set[str],
        nullable_params: Mapping[str, str],
        *,
        variadic_name: str | None,
        var: str,
        params_casts: Mapping[str, str],
    ) -> None:
        """Generate code to collect positional function arguments into a list.

        Creates a list variable containing all positional arguments, filtering
        out any omittable parameters that have Unspecified values at runtime.

        Args:
            params: Names of all positional parameters
            omittable_params: Set of parameter names that can be omitted
            nullable_params: Map of parameter names that can be empty along
                             with their canonical type.
            variadic_name: Name of variadic parameter if any.
            params_casts: Type checking workarounds for buggy situations.
            var: Name of the variable to assign the argument list to.
        """
        expr_compat = self.import_name(BASE_IMPL, "ExprCompatible")
        list_ = self.import_name("builtins", "list")

        args_def = f"{var}: {list_}[{expr_compat}]"
        mandatory, omittable = self._partition_function_params(
            params, omittable_params, nullable_params, params_casts
        )
        if omittable or variadic_name:
            unsp_t = self.import_name(BASE_IMPL, "UnspecifiedType")
            isinst = self.import_name("builtins", "isinstance")

            def write_omittable() -> None:
                # Filter out unspecified
                self.write("__v__")
                self.write(
                    self.format_list("for __v__ in ({list})", omittable)
                )
                self.write(f"if not {isinst}(__v__, {unsp_t})")

            self.write(f"{args_def} = [")
            with self.indented():
                # Include all mandatory parameters first
                if mandatory:
                    self.write(self.format_list("*({list}),", mandatory))
                if omittable:
                    self.write("*(")
                    with self.indented():
                        write_omittable()
                    self.write("),")
                if variadic_name:
                    # Note that we don't use variadic_name directly here
                    # because it might have been transformed by the
                    # type coercion block.
                    self.write("*__variadic__,")
            self.write("]")
        else:
            # Simple case: all parameters are mandatory
            self.write(
                self.format_list(f"{args_def} = [{{list}}]", mandatory),
            )

    def _write_function_keyword_args_collection(
        self,
        params: list[str],
        omittable_params: set[str],
        nullable_params: Mapping[str, str],
        *,
        var: str,
        params_casts: Mapping[str, str],
    ) -> None:
        """Generate code to collect keyword function arguments into a
        dictionary.

        Creates a dictionary variable containing all keyword arguments,
        filtering out any omittable parameters that have Unspecified values at
        runtime.

        Args:
            params: Names of all keyword parameters
            omittable_params: Set of parameter names that can be omitted
            nullable_params: Map of parameter names that can be empty along
                             with their canonical type.
            params_casts: Type checking workarounds for buggy situations.
            var: Name of the variable to assign the argument list to.
        """
        expr_compat = self.import_name(BASE_IMPL, "ExprCompatible")
        dict_ = self.import_name("builtins", "dict")
        str_ = self.import_name("builtins", "str")

        args_def = f"{var}: {dict_}[{str_}, {expr_compat}]"
        mandatory, omittable = self._partition_function_params(
            params, omittable_params, nullable_params, params_casts
        )
        if omittable:
            unsp_t = self.import_name(BASE_IMPL, "UnspecifiedType")
            isinst = self.import_name("builtins", "isinstance")

            def write_omittable() -> None:
                # Generate dict comprehension to filter out Unspecified values
                self.write("__k__: __v__")
                self.write(
                    self.format_list(
                        "for __k__, __v__ in {{{list}}}.items()",
                        [f'"{n}": {n}' for n in omittable],
                    )
                )
                self.write(f"if not {isinst}(__v__, {unsp_t})")

            self.write(f"{args_def} = {{")
            with self.indented():
                if mandatory:
                    # Include all mandatory parameters first
                    self.write(
                        self.format_list(
                            "**{{{list}}},",
                            [f'"{n}": {n}' for n in mandatory],
                        ),
                    )
                    self.write("**{")
                    with self.indented():
                        write_omittable()
                    self.write("},")
                else:
                    write_omittable()
            self.write("}")
        else:
            # Simple case: all parameters are mandatory
            self.write(
                self.format_list(
                    f"{args_def} = {{{{{{list}}}}}}",
                    [f'"{n}": {n}' for n in mandatory],
                ),
            )

    def _write_func_node_ctor(
        self,
        function: reflection.Callable,
    ) -> None:
        fcall = self.import_name(BASE_IMPL, "FuncCall")
        self.write(f"{fcall}(")
        sig = function.signature
        with self.indented():
            self.write(f'fname="{function.name}",')
            if sig.num_pos > 0 or sig.has_variadic:
                self.write("args=__args__,")
            if bool(sig.kwargs):
                self.write("kwargs=__kwargs__,")
            self.write("type_=__rtype__.__gel_reflection__.name,")
        self.write(")")

    def _write_function_overload(
        self,
        function: _Callable_T,
        *,
        num_overloads_total: int,
        param_types: Mapping[
            reflection.CallableParamKey,
            Sequence[reflection.Type],
        ]
        | None = None,
        cast_map: Mapping[
            reflection.CallableParamKey, dict[str, reflection.Type]
        ]
        | None = None,
        style: Literal["constructor", "method", "function"] = "function",
        type_ignore: Sequence[str] = (),
        param_getter: Callable[
            [_Callable_T], Iterable[reflection.CallableParam]
        ] = operator.attrgetter("params"),
        node_ctor: Callable[[_Callable_T], None] | None = None,
    ) -> None:
        """Generate a single function overload with proper type resolution.

        This is a complex code generator that handles Gel function overloads
        with support for generics, type casting, optional parameters, and
        runtime type inference. The generated code includes both static type
        annotations and runtime type resolution logic.

        Key behaviors:
        - Generic types are resolved at runtime by inspecting parameter types
        - Constructor-style functions get special generic type fallback
          handling
        - Tuple parameters get ExprCompatible casting to work around type
          system limitations
        - Optional parameters are handled with proper null checking and
          default values
        - Generated functions return AnnotatedExpr objects for Gel query
          building

        Args:
            function: The callable object to generate code for.
            num_overloads_total: Total number of overloads (affects @overload
                decorator usage).
            param_types: Override parameter types for this specific overload.
            cast_map: Additional type casts to apply to specific parameters.
            style: Style of callable to generate (method, function, ctor).
            type_ignore: Type ignore codes to annotate generated `def` with.
            param_getter:
                attribute)
            node_ctor: QB node constructor (default is _write_func_node_ctor).
        """
        params = []
        kwargs = []
        pos_names: list[str] = []
        kw_names: list[str] = []
        variadic: str | None = None

        generic_param_map = function.generics(self._types)
        # Create TypeVars for generic params
        typevars: dict[reflection.Type, str] = {}
        for typ in itertools.chain.from_iterable(generic_param_map.values()):
            typevar_name = f"_T_{ident(typ.schemapath.name)}"
            bound = self.get_type(
                typ,
                import_time=ImportTime.typecheck,
            )
            typevars[typ] = self.declare_typevar(
                typevar_name,
                bound=bound,
            )

        required_generic_params: set[str] = set()
        optional_generic_params: set[str] = set()
        omittable_params: set[str] = set()
        param_workaround_casts: dict[str, str] = {}
        nullable_param_types: dict[str, reflection.Type] = {}
        generic_params: defaultdict[
            reflection.Type,
            set[tuple[reflection.CallableParam, Indirection]],
        ] = defaultdict(set)
        for param in param_getter(function):
            # Track which parameters contribute to generic type resolution
            if (generics := generic_param_map.get(param.key)) is not None:
                for gt, paths in generics.items():
                    for path in paths:
                        generic_params[gt].add((param, path))
                # Required params used first for generic type inference
                # at runtime.
                if (
                    param.default is None
                    and param.typemod is not reflection.TypeModifier.Optional
                ):
                    required_generic_params.add(param.name)
                else:
                    optional_generic_params.add(param.name)

            if param.default is not None:
                omittable_params.add(param.name)

            if param_types is not None:
                param_type = param_types.get(param.key)
            else:
                param_type = None
            if param_type is None:
                param_type = [param.get_type(self._types)]

            if param.typemod is TypeModifier.Optional:
                nullable_param_types[param.name] = param_type[0]

            # Tuple types need special handling due to a possible mypy bug.
            if any(reflection.is_tuple_type(t) for t in param_type):
                expr_compat = self.import_name(BASE_IMPL, "ExprCompatible")
                param_workaround_casts[param.name] = expr_compat

            # Build the union type for parameter.
            union = []
            for item in param_type:
                # The type class (i.e another qb expression)
                pt_str = self.get_type_type(
                    item,
                    import_time=ImportTime.typecheck_runtime,
                    typevars=typevars,
                )
                union.append(pt_str)
                # For primitives, also accept the values
                if reflection.is_primitive_type(item):
                    pt_str = self.get_type(
                        item,
                        import_time=ImportTime.typecheck_runtime,
                        typevars=typevars,
                    )
                    union.append(pt_str)

            if cast_map is not None and (
                (param_casts := cast_map.get(param.key)) is not None
            ):
                union.extend(param_casts)

            pt = self._render_callable_sig_type(
                " | ".join(sorted(union)),
                param.typemod,
                param.default,
            )
            pident = ident(param.name)

            param_decl = f"{param.name}: {pt}"
            if param.kind == reflection.CallableParamKind.Positional:
                params.append(param_decl)
                pos_names.append(pident)
            elif param.kind == reflection.CallableParamKind.Variadic:
                # Gel functions should have at most one variadic parameter
                if variadic is not None:
                    raise AssertionError(
                        f"multiple variadic parameters "
                        f"declared in {function.name}"
                    )
                variadic = param_decl
            elif param.kind == reflection.CallableParamKind.NamedOnly:
                kwargs.append(param_decl)
                kw_names.append(pident)
            else:
                raise AssertionError(
                    f"unexpected parameter kind in {function.name}: "
                    f"{param.kind}"
                )

        if variadic is not None:
            params.append(f"*{variadic}")
        elif kwargs:
            params.append("*")
        params.extend(kwargs)

        ret_type = function.get_return_type(self._types)
        rtype = self.render_callable_return_type(
            ret_type,
            function.return_typemod,
            typevars=typevars,
            import_time=ImportTime.typecheck_runtime,
        )

        fname = ident(function.ident)
        fdef = (
            self._method_def
            if style in {"method", "constructor"}
            else self._func_def
        )
        def_kwargs: dict[str, Any] = {}
        # Constructor functions use __new__ and need explicit cls parameter
        if style == "constructor":
            fname = "__new__"
            params.insert(0, "cls")
            def_kwargs["implicit_param"] = False

        with fdef(
            fname,
            params,
            rtype,
            overload=num_overloads_total > 1,
            type_ignore=type_ignore,
            **def_kwargs,
        ):
            aexpr = self.import_name(BASE_IMPL, "AnnotatedExpr")
            unsp = self.import_name(BASE_IMPL, "Unspecified")
            expr_compat = self.import_name(BASE_IMPL, "ExprCompatible")
            list_ = self.import_name("builtins", "list")

            if cast_map is None:
                cast_map = {}

            # Build a `match` block for Python-to-Gel coercion of values.
            for param in function.params:
                if param_cast := cast_map.get(param.key):
                    pident = ident(param.name)
                    casts = {
                        py_type: self.get_type(
                            st, import_time=ImportTime.typecheck_runtime
                        )
                        for py_type, st in param_cast.items()
                    }
                    if param.is_variadic:
                        new_list = "__variadic__"
                        self.write(f"{new_list}: {list_}[{expr_compat}] = []")
                        it = f"{dunder(param.name)}el__"
                        self.write(f"for {it} in {pident}:")
                        with self.indented():
                            self.write(f"match {it}:")
                            with self.indented():
                                for py_type, cast_t in casts.items():
                                    self.write(f"case {py_type}():")
                                    with self.indented():
                                        self.write(f"{it} = {cast_t}({it})")
                            self.write(f"{new_list}.append({it})")
                    else:
                        self.write(f"match {pident}:")
                        with self.indented():
                            for py_type, cast_t in casts.items():
                                self.write(f"case {py_type}():")
                                with self.indented():
                                    self.write(
                                        f"{pident} = {cast_t}({pident})"
                                    )
                elif param.is_variadic:
                    self.write(f"__variadic__ = {ident(param.name)}")

            rt_generics = generic_param_map.get("__return__")
            if rt_generics is None and not optional_generic_params:
                # Simple case: no generic type inference needed
                rtype_rt = self.get_type(
                    ret_type,
                    import_time=ImportTime.typecheck_runtime,
                )
                nullable_params = {
                    n: self.get_type(
                        t,
                        import_time=ImportTime.typecheck_runtime,
                        typevars=typevars,
                    )
                    for n, t in nullable_param_types.items()
                }
            else:
                # Need runtime generic type inference
                isinstance_ = self.import_name("builtins", "isinstance")
                alias = self.import_name(BASE_IMPL, "BaseAlias")
                anytype = self.get_type_type(
                    self._types_by_name["anytype"],
                    import_time=ImportTime.typecheck,
                )
                rtypevars = {}
                for gt in typevars:
                    sources = sorted(
                        generic_params[gt],
                        key=lambda p: (p[0].sort_key, _indirection_key(p[1])),
                    )
                    gtvar = dunder(gt.name)  # e.g., __anytype__
                    rtypevars[gt] = gtvar
                    self.write(f"{gtvar}: {anytype}")

                    def resolve(
                        pn: str, path: Indirection, gtvar: str = gtvar
                    ) -> None:
                        """Navigate type indirection *path* and assign
                        the result to *gtvar* for a *pn* parameter."""

                        # Majority of non-value arguments would be other
                        # qb expressions aka ExprAlias.
                        self.write(
                            f"__t__ = {pn}.__gel_origin__ "
                            f"if {isinstance_}({pn}, {alias}) "
                            f"else {pn}"
                        )
                        # Navigate through type path (e.g., container elements)
                        if path:
                            t = "__t__." + ".".join(
                                f"{s[0]}[{s[1]}]"
                                if isinstance(s, tuple)
                                else s
                                for s in path
                            )
                        else:
                            t = "__t__"

                        self.write(
                            f"{gtvar} = {t}  "
                            f"# type: ignore [assignment, misc, unused-ignore]"
                        )

                    # Try to infer generic type from required params first
                    for param, path in sources:
                        if param.name in required_generic_params:
                            pn = ident(param.name)
                            resolve(pn, path)
                            break
                    else:
                        # Fall back to optional params with null/unspecified
                        # checks
                        for i, (param, path) in enumerate(sources):
                            pn = ident(param.name)
                            conds = []
                            if (
                                param.typemod
                                is reflection.TypeModifier.Optional
                            ):
                                conds.append(f"{pn} is not None")
                            if param.default is not None:
                                conds.append(f"{pn} is not {unsp}")
                            cond = " and ".join(conds)
                            ctl = self.if_ if i == 0 else self.elif_
                            with ctl(cond):
                                resolve(pn, path)

                        with self.else_():
                            # Special handling for single-generic constructors
                            # e.g std::range()
                            if style == "constructor" and len(typevars) == 1:
                                self.write("try:")
                                with self.indented():
                                    self.write(
                                        f"{gtvar} = cls.__element_type__"
                                    )
                                self.write("except AttributeError:")
                                with self.indented():
                                    fname = function.schemapath.name
                                    msg = (
                                        f"empty `{fname}` must be "
                                        f"explicitly parametrized, for example"
                                        f": `{fname}[std.int64]`"
                                    )
                                    self.write(
                                        f"raise TypeError({msg!r}) from None"
                                    )
                            else:
                                # Default to the generic type itself
                                # TODO: should we raise instead?
                                gt_t = self.get_type(gt)
                                self.write(f"{gtvar} = {gt_t}")

                # Use the resolved runtime type variables
                # for return type generic.
                rtype_rt = self.get_type(
                    ret_type,
                    typevars=rtypevars,
                    import_time=ImportTime.typecheck_runtime,
                )
                nullable_params = {
                    n: self.get_type(
                        t,
                        import_time=ImportTime.typecheck_runtime,
                        typevars=rtypevars,
                    )
                    for n, t in nullable_param_types.items()
                }

            # Generate positional argument collection code (__args__ tuple)
            if pos_names or variadic:
                self._write_function_positional_args_collection(
                    pos_names,
                    omittable_params,
                    nullable_params,
                    variadic_name=variadic,
                    params_casts=param_workaround_casts,
                    var="__args__",
                )

            # Generate keyword argument collection code (__kwargs__ dict)
            if kw_names:
                self._write_function_keyword_args_collection(
                    kw_names,
                    omittable_params,
                    nullable_params,
                    params_casts=param_workaround_casts,
                    var="__kwargs__",
                )

            self.write(
                f"__rtype__ = {rtype_rt}  "
                f"# type: ignore [valid-type, unused-ignore]"
            )
            # The result is always an ExprAlias with return type origin
            # and the expression in the metadata.
            self.write(f"return {aexpr}(  # type: ignore [return-value]")
            if node_ctor is None:
                node_ctor = self._write_func_node_ctor
            with self.indented():
                self.write("__rtype__,")
                # Generate the actual qb node (FuncCall, etc.)
                node_ctor(function)
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
        pytype: str

        if reflection.is_link(prop):
            is_multi = prop.card in {"AtLeastOne", "Many"}
            is_optional = prop.card in {"AtMostOne", "Many", "Empty"}
            match (
                is_multi,
                is_optional,
                bool(prop.pointers),
                prop.is_computed,
            ):
                case True, True, True, False:
                    desc = self.import_name(
                        BASE_IMPL, "OptionalMultiLinkWithProps"
                    )
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
                    desc = self.import_name(BASE_IMPL, "OptionalMultiLink")
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
                    desc = self.import_name(BASE_IMPL, "RequiredLinkWithProps")
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
                case False, False, False, True:
                    desc = self.import_name(BASE_IMPL, "ComputedLink")
                    pytype = f"{desc}[{narrow_type}]"
                case False, False, False, False:
                    pytype = narrow_type
                case _:
                    raise RuntimeError(
                        f"no handler for the combination of flags for "
                        f"the {prop.name!r} link: "
                        f"is_multi={is_multi} / "
                        f"is_optional={is_optional} / "
                        f"has_props={bool(prop.pointers)} / "
                        f"is_computed={prop.is_computed}"
                    )
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
                    if prop.name == "id":
                        # short circuit id -- it's wrapped in IdProperty
                        return narrow_type
                    desc = self.import_name(BASE_IMPL, "Property")
                    pytype = f"{desc}[{narrow_type}, {broad_type}]"
                case _:
                    raise RuntimeError(
                        f"no handler for the combination of flags for "
                        f"the {prop.name!r} property: "
                        f"is_multi={cardinality.is_multi()} / "
                        f"is_optional={cardinality.is_optional()} /"
                        f"is_computed={prop.is_computed}"
                    )

        return pytype

    def get_ptr_type(
        self,
        objtype: reflection.ObjectType,
        prop: reflection.Pointer,
        *,
        style: Literal[
            "annotation", "typeddict", "arg", "unspec_arg", "arg_no_default"
        ] = "annotation",
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
        import_time = ImportTime.late_runtime
        bare_ptr_type = ptr_type = self.get_type(
            target_type,
            aspect=aspect,
            import_time=import_time,
            localns=localns,
        )

        if reflection.is_primitive_type(target_type):
            bare_ptr_type = self._get_pytype_for_primitive_type(
                target_type,
                import_time=import_time,
                localns=localns,
            )
            union = {bare_ptr_type}
            assn_casts = self._casts.assignment_casts_to.get(
                target_type.id,
                {},
            )
            for type_id in assn_casts:
                assn_type = self._types[type_id]
                if reflection.is_primitive_type(assn_type):
                    assn_pytype = self._get_pytype_for_primitive_type(
                        assn_type,
                        import_time=import_time,
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
                target_name = target_type.schemapath
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
            case "arg" | "unspec_arg" | "arg_no_default":
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

                if style == "unspec_arg":
                    unspec_t = self.import_name(BASE_IMPL, "UnspecifiedType")
                    unspec = self.import_name(BASE_IMPL, "Unspecified")
                    result = f"{type_} | {unspec_t} = {unspec}"
                elif style == "arg_no_default":
                    result = f"{type_}"
                else:
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

    def process(self, types: Mapping[str, reflection.Type]) -> None:
        graph: defaultdict[str, set[str]] = defaultdict(set)

        @functools.singledispatch
        def type_dispatch(t: reflection.Type, ref_t: str) -> None:
            if reflection.is_named_tuple_type(t):
                graph[ref_t].add(t.id)
                for elem_type in t.get_element_types(self._types):
                    type_dispatch(elem_type, t.id)
            elif reflection.is_tuple_type(t):
                for elem_type in t.get_element_types(self._types):
                    type_dispatch(elem_type, ref_t)
            elif reflection.is_array_type(t):
                type_dispatch(t.get_element_type(self._types), ref_t)

        for t in types.values():
            if reflection.is_named_tuple_type(t):
                graph[t.id] = set()
                for elem_type in t.get_element_types(self._types):
                    type_dispatch(elem_type, t.id)

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
        self.write(f"# tuple type {t.schemapath}")
        self.write("#")
        classname = self.get_tuple_name(t)
        with self._class_def(f"_{classname}", [namedtuple]):
            for elem, elem_type in zip(
                t.tuple_elements,
                t.get_element_types(self._types),
                strict=True,
            ):
                elem_type_anno = self.get_type(
                    elem_type,
                    import_time=ImportTime.late_runtime,
                )
                self.write(f"{elem.name}: {elem_type_anno}")
        self.write_section_break()
        with self._class_def(classname, [f"_{classname}", anytuple]):
            self.write_type_reflection(t)
            self.write()
            self.write("__slots__ = ()")
        self.write()
