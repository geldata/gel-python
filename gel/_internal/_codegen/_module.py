# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.


from __future__ import annotations
from typing import (
    TYPE_CHECKING,
)

import collections
import collections.abc
import contextlib
import dataclasses
import enum
import operator
import re
import textwrap
from collections import defaultdict

from gel._internal._polyfills._intenum import IntEnum

if TYPE_CHECKING:
    import io

    from collections.abc import Iterator, Iterable
    from collections.abc import Set as AbstractSet


MAX_LINE_LENGTH = 79


class _ImportSource(enum.Enum):
    std = enum.auto()
    lib = enum.auto()
    local = enum.auto()


class ImportTime(IntEnum):
    typecheck = enum.auto()
    typecheck_runtime = enum.auto()
    late_runtime = enum.auto()
    runtime = enum.auto()
    local = enum.auto()


class CodeSection(enum.Enum):
    main = enum.auto()
    after_late_import = enum.auto()


def _disambiguate_name(
    name: str,
    globalns: AbstractSet[str],
    localns: AbstractSet[str] | None = None,
) -> str:
    if not _in_ns(name, globalns) and not _in_ns(name, localns):
        return name

    ctr = 0

    def _mangle(name: str) -> str:
        nonlocal ctr
        if ctr == 0:
            return f"{name}_"
        else:
            return f"{name}_{ctr}"

    mangled = _mangle(name)
    while _in_ns(mangled, globalns) or _in_ns(mangled, localns):
        ctr += 1
        mangled = _mangle(name)

    return mangled


@dataclasses.dataclass(kw_only=True)
class Import:
    module: str
    attr: str | None = None
    alias: str | None = None
    time: ImportTime
    ref_attrs: set[str] = dataclasses.field(default_factory=set)
    extra_aliases: set[str] = dataclasses.field(default_factory=set)

    @property
    def name(self) -> str:
        return self.alias or self.attr or self.module.rpartition(".")[-1]

    def non_conflicting_name(self, ns: AbstractSet[str] | None = None) -> str:
        name = self.name
        if ns is None:
            return name
        else:
            alias = _disambiguate_name(name, ns)
            if alias != name:
                self.extra_aliases.add(alias)
            return alias

    @property
    def source(self) -> _ImportSource:
        module = self.module
        if module == "gel" or module.startswith("gel."):
            source = _ImportSource.lib
        elif module.startswith("."):
            source = _ImportSource.local
        else:
            source = _ImportSource.std

        return source


class _ImportScope:
    imports: dict[ImportTime, dict[tuple[str, str | None], Import]]
    ns: set[str]

    def __init__(
        self, namespace: set[str], parent: _ImportScope | None = None
    ) -> None:
        self.imports = {
            ImportTime.typecheck: {},
            ImportTime.typecheck_runtime: {},
            ImportTime.runtime: {},
            ImportTime.late_runtime: {},
        }
        self.ns = namespace
        self.parent = parent

    def get(self, import_time: ImportTime) -> Iterable[Import]:
        return self.imports[import_time].values()

    def _maybe_make_visible(
        self,
        module: str,
        name: str | None,
        import_time: ImportTime,
    ) -> Import | None:
        key = (module, name)
        for imported_at, imports in self.imports.items():
            if (imported := imports.get(key)) is not None:
                if import_time > imported_at:
                    if import_time is ImportTime.late_runtime:
                        continue
                    # Promote import
                    imported.time = import_time
                    del imports[key]
                    self.imports[import_time][key] = imported

                return imported
            if (
                self.parent is not None
                and import_time <= imported_at
                and (imported := self.parent.imports[import_time].get(key))
                is not None
            ):
                return imported

        return None

    def import_(
        self,
        module: str,
        name: str | None,
        *,
        import_time: ImportTime = ImportTime.runtime,
        always_import_qualified: bool = False,
        suggested_module_alias: str | None = None,
        localns: frozenset[str] | None = None,
    ) -> str:
        if _is_all_dots(module):
            raise ValueError(
                f"import_name: bare relative imports are "
                f"not supported: {module!r}"
            )

        imported: Import | None = None
        ref_expr: str | None = None
        if name is not None and not always_import_qualified:
            if imported := self._maybe_make_visible(module, name, import_time):
                # Already imported
                return imported.non_conflicting_name(localns)
            if not _in_ns(name, self.ns) and not _in_ns(name, localns):
                # No conflict with namespace
                imported = Import(module=module, attr=name, time=import_time)
                ref_expr = imported.name
                if module == "builtins":
                    # Avoid rendering needless `from builtins` imports
                    return ref_expr

        if imported is None:
            # Module import (either direct or as a result of name conflict)
            parent_module, dot, tail_module = module.rpartition(".")
            if _is_all_dots(parent_module) or (not parent_module and dot):
                # Pure relative import
                parent_module += dot

            submod: str | None
            if parent_module:
                module = parent_module
                submod = tail_module
            else:
                submod = None

            if imported := self._maybe_make_visible(
                module, submod, import_time
            ):
                # Already imported
                if name:
                    imported.ref_attrs.add(name)
                    return f"{imported.name}.{name}"
                else:
                    return imported.non_conflicting_name(localns)

            import_name = submod or module
            alias = _disambiguate_name(
                suggested_module_alias or import_name,
                globalns=self.ns,
            )

            imported = Import(
                module=module,
                attr=submod,
                alias=alias if alias != import_name else None,
                time=import_time,
            )

            if name:
                imported.ref_attrs.add(name)
                ref_expr = f"{imported.name}.{name}"
            else:
                ref_expr = imported.non_conflicting_name(localns)

        self.imports[import_time][imported.module, imported.attr] = imported
        self.ns.add(imported.name)

        assert ref_expr is not None
        return ref_expr


def _is_all_dots(s: str) -> bool:
    return bool(s) and all(c == "." for c in s)


def _in_ns(imported: str, ns: AbstractSet[str] | None) -> bool:
    if ns is None:
        return False
    else:
        modname, _, attrname = imported.partition(".")
        return (modname or attrname) in ns


class GeneratedModule:
    INDENT = " " * 4

    def __init__(self, preamble: str, substrate_module: str) -> None:
        self._comment_preamble = preamble
        self._indent_level = 0
        self._in_type_checking = False
        self._content: defaultdict[CodeSection, list[str]] = defaultdict(list)
        self._code_section = CodeSection.main
        self._code = self._content[self._code_section]
        self._globals: set[str] = set()
        self._exports: set[str] = set()
        self._imports = _ImportScope(self._globals)
        self._imported_names: dict[tuple[str, str, ImportTime], str] = {}
        self._substrate_module = substrate_module
        self._typevars: defaultdict[str, dict[str | None, str]] = defaultdict(
            dict
        )

    def has_content(self) -> bool:
        return any(
            self.section_has_content(section) for section in self._content
        ) or bool(self._exports)

    def section_has_content(self, section: CodeSection) -> bool:
        return bool(self._content[section])

    def indent(self, levels: int = 1) -> None:
        self._indent_level += levels

    def dedent(self, levels: int = 1) -> None:
        if self._indent_level > 0:
            self._indent_level -= levels

    def has_global(self, name: str) -> bool:
        return name in self._globals

    def add_global(self, name: str) -> None:
        self._globals.add(name)

    def update_globals(self, names: collections.abc.Iterable[str]) -> None:
        self._globals.update(names)

    def declare_typevar(
        self,
        name: str,
        *,
        bound: str | None,
    ) -> str:
        typevar = self._typevars[name].get(bound)
        if typevar is None:
            typevar = self.disambiguate_name(name)
            self._typevars[name][bound] = typevar
            self.add_global(typevar)
        return typevar

    def disambiguate_name(self, name: str) -> str:
        return _disambiguate_name(name, self._globals, None)

    def _do_import_name(
        self,
        module: str,
        name: str | None,
        *,
        suggested_module_alias: str | None = None,
        always_import_qualified: bool = False,
        import_time: ImportTime = ImportTime.runtime,
        localns: frozenset[str] | None = None,
    ) -> str:
        if import_time is ImportTime.local:
            scope = _ImportScope(set(), parent=self._imports)
        else:
            scope = self._imports

        imported = scope.import_(
            module,
            name,
            suggested_module_alias=suggested_module_alias,
            always_import_qualified=always_import_qualified,
            import_time=ImportTime.runtime
            if import_time is ImportTime.local
            else import_time,
            localns=localns,
        )

        if import_time is ImportTime.local:
            self.write(self.render_imports(scope))

        return imported

    def import_name(
        self,
        module: str,
        name: str,
        *,
        suggested_module_alias: str | None = None,
        import_time: ImportTime = ImportTime.runtime,
        localns: frozenset[str] | None = None,
    ) -> str:
        if import_time is ImportTime.typecheck_runtime:
            raise ValueError(
                "typecheck/deferred import time does not "
                "support direct name imports"
            )
        return self._do_import_name(
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
        return self._do_import_name(
            module,
            name,
            suggested_module_alias=suggested_module_alias,
            always_import_qualified=True,
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
        return self._do_import_name(
            module,
            None,
            suggested_module_alias=suggested_module_alias,
            import_time=import_time,
            localns=localns,
        )

    def export(self, *names: str) -> None:
        self._exports.update(names)

    @property
    def exports(self) -> set[str]:
        return self._exports

    def current_indentation(self, extra: int = 0) -> str:
        return self.INDENT * (self._indent_level + extra)

    @contextlib.contextmanager
    def indented(self) -> Iterator[None]:
        self._indent_level += 1
        try:
            yield
        finally:
            self._indent_level -= 1

    @contextlib.contextmanager
    def if_(self, cond: str) -> Iterator[None]:
        self.write(f"if {cond}:")
        with self.indented():
            yield

    @contextlib.contextmanager
    def elif_(self, cond: str) -> Iterator[None]:
        self.write(f"elif {cond}:")
        with self.indented():
            yield

    @contextlib.contextmanager
    def else_(self) -> Iterator[None]:
        self.write("else:")
        with self.indented():
            yield

    @contextlib.contextmanager
    def type_checking(self) -> Iterator[None]:
        tc = self.import_name("typing", "TYPE_CHECKING")
        self.write(f"if {tc}:")
        old_in_tc = self._in_type_checking
        try:
            self._in_type_checking = True
            with self.indented():
                yield
        finally:
            self._in_type_checking = old_in_tc

    @contextlib.contextmanager
    def not_type_checking(self) -> Iterator[None]:
        if self._in_type_checking:
            raise AssertionError(
                "cannot enter `if not TYPE_CHECKING` context: "
                "already in `if TYPE_CHECKING`"
            )
        tc = self.import_name("typing", "TYPE_CHECKING")
        self.write(f"if not {tc}:")
        old_in_tc = self._in_type_checking
        try:
            self._in_type_checking = False
            with self.indented():
                yield
        finally:
            self._in_type_checking = old_in_tc

    @property
    def in_type_checking(self) -> bool:
        return self._in_type_checking

    @contextlib.contextmanager
    def code_section(self, section: CodeSection) -> Iterator[None]:
        orig_indent_level = self._indent_level
        self._indent_level = 0
        orig_section = self._code_section
        self._code_section = section
        self._code = self._content[self._code_section]
        try:
            yield
        finally:
            self._code_section = orig_section
            self._code = self._content[self._code_section]
            self._indent_level = orig_indent_level

    def reset_indent(self) -> None:
        self._indent_level = 0

    def write(self, text: str = "") -> None:
        chunk = textwrap.indent(text, prefix=self.INDENT * self._indent_level)
        self._code.append(chunk)

    def write_section_break(self, size: int = 2) -> None:
        self._code.extend([""] * size)

    def get_comment_preamble(self) -> str:
        return self._comment_preamble

    def _render_typevar(self, name: str, *, bound: str | None, tv: str) -> str:
        if bound is None:
            return f'{name} = {tv}("{name}")'
        else:
            return f'{name} = {tv}("{name}", bound={bound!r})'

    def render_typevars(self) -> str:
        if not self._typevars:
            return ""
        else:
            tv = self.import_name("typing", "TypeVar")
            return "\n".join(
                self._render_typevar(name, bound=bound, tv=tv)
                for bounds in self._typevars.values()
                for bound, name in bounds.items()
            )

    def render_exports(self) -> str:
        if self._exports:
            return "\n".join(
                [
                    "__all__ = (",
                    *(f"    {ex!r}," for ex in sorted(self._exports)),
                    ")",
                ]
            )
        else:
            return ""

    def render_imports(self, scope: _ImportScope | None = None) -> str:
        if scope is None:
            scope = self._imports

        tc_imps = [*scope.get(ImportTime.typecheck)]
        tc_runtime_imps = [*scope.get(ImportTime.typecheck_runtime)]
        tc_sections = []

        if tc_imps or tc_runtime_imps:
            tc = self.import_name("typing", "TYPE_CHECKING")
            tc_sections.append(f"if {tc}:")
            tc_sections.extend(
                self._render_imports(tc_imps, indent=self.INDENT)
            )
            if tc_runtime_imps:
                tc_sections.extend(
                    self._render_typecheck_faux_ns_imports(
                        tc_runtime_imps,
                        indent=self.INDENT,
                    )
                )
                tc_sections.append("else:")
                tc_sections.extend(
                    self._render_deferred_imports(
                        tc_runtime_imps,
                        indent=self.INDENT,
                        deferred_import=self.import_name(
                            self._substrate_module, "DeferredImport"
                        ),
                    )
                )

        sections = (
            self._render_imports(scope.get(ImportTime.runtime)) + tc_sections
        )

        return "\n\n".join(filter(None, sections))

    def render_late_imports(self) -> str:
        sections = self._render_imports(
            self._imports.get(ImportTime.late_runtime),
            noqa=["E402", "F403"],
        )

        return "\n\n".join(filter(None, sections))

    def _render_imports(
        self,
        imports: Iterable[Import],
        *,
        indent: str = "",
        noqa: list[str] | None = None,
    ) -> list[str]:
        blocks = []
        by_source: defaultdict[_ImportSource, list[Import]] = defaultdict(list)
        for imp in imports:
            by_source[imp.source].append(imp)
        for source in _ImportSource.__members__.values():
            block = self._render_imports_source_block(
                by_source[source],
                indent=indent,
                noqa=noqa,
            )
            if block:
                blocks.append(block)
        return blocks

    def _render_imports_source_block(
        self,
        imports: list[Import],
        *,
        indent: str = "",
        noqa: list[str] | None = None,
    ) -> str:
        output = []
        extra_aliases: list[str] = []
        mods = sorted(
            (imp for imp in imports if imp.attr is None),
            key=lambda imp: imp.name,
        )
        for imp in mods:
            modname = imp.module
            alias = imp.alias
            if modname.startswith("."):
                match = re.match(r"^(\.+)(.*)", modname)
                assert match
                relative = match.group(1)
                rest = match.group(2)
                pkg, _, name = rest.rpartition(".")
                import_line = f"from {relative}{pkg} import {name}"
                if alias and alias != name:
                    import_line += f" as {alias}"
            elif alias:
                import_line = f"import {modname} as {alias}"
            else:
                import_line = f"import {modname}"
            if noqa:
                import_line += f"  # noqa: {' '.join(noqa)}"
            output.append(import_line)
            extra_aliases.extend(
                f"{extra_alias} = {alias or modname}"
                for extra_alias in sorted(imp.extra_aliases)
            )

        import_lists: defaultdict[str, list[Import]] = defaultdict(list)
        for imp in imports:
            if imp.attr is not None:
                import_lists[imp.module].append(imp)

        noqa_suf = f"  # noqa: {' '.join(noqa)}" if noqa else ""

        name_imports = {
            modname: sorted(
                (imp for imp in names if imp.attr is not None),
                key=lambda imp: (
                    0
                    if imp.name.isupper()
                    else 1
                    if imp.name[0].isupper()
                    else 2,
                    imp.name,
                ),
            )
            for modname, names in import_lists.items()
        }
        for modname, names in sorted(
            name_imports.items(), key=operator.itemgetter(0)
        ):
            if not names:
                continue
            import_line = f"from {modname} import "
            names_list = [
                f"{imp.attr} as {imp.alias}" if imp.alias else imp.attr
                for imp in names
                if imp.attr is not None
            ]
            names_part = ", ".join(names_list)
            if len(import_line) + len(names_part) > MAX_LINE_LENGTH:
                import_line += (
                    f"({noqa_suf}\n    " + ",\n    ".join(names_list) + "\n)"
                )
            else:
                import_line += names_part + noqa_suf
            output.append(import_line)
            for imp in names:
                extra_aliases.extend(
                    f"{extra_alias} = {imp.name}"
                    for extra_alias in sorted(imp.extra_aliases)
                )

        if extra_aliases:
            output.extend(
                [
                    "",
                    "# disambiguation for clashes in local namespaces",
                    *extra_aliases,
                ]
            )

        result = "\n".join(output)
        if indent:
            result = textwrap.indent(result, indent)
        return result

    def _render_typecheck_faux_ns_imports(
        self,
        imports: list[Import],
        *,
        indent: str = "",
    ) -> list[str]:
        output: list[str] = []
        import_lists: defaultdict[str, list[Import]] = defaultdict(list)
        for imp in imports:
            if imp.ref_attrs:
                import_lists[imp.name].append(imp)

        name_imports = sorted(
            import_lists.items(),
            key=operator.itemgetter(0),
        )

        for modname, names in name_imports:
            output.append(f"class {modname}:")
            for imp in names:
                if imp.attr is None:
                    import_line = f"from {imp.module} import "
                else:
                    if _is_all_dots(imp.module):
                        mod = f"{imp.module}{imp.attr}"
                    else:
                        mod = f"{imp.module}.{imp.attr}"
                    import_line = f"from {mod} import "

                ref_attrs = sorted(
                    imp.ref_attrs,
                    key=lambda s: (
                        0 if s.isupper() else 1 if s[0].isupper() else 2,
                        s,
                    ),
                )

                nlist = [f"{s} as {s}" for s in ref_attrs]
                names_part = ", ".join(nlist)
                if len(import_line) + len(names_part) > MAX_LINE_LENGTH:
                    import_line += "(\n    " + ",\n    ".join(nlist) + "\n)"
                else:
                    import_line += names_part
                output.append(textwrap.indent(import_line, self.INDENT))

        result = "\n".join(output)
        if indent:
            result = textwrap.indent(result, indent)
        return [result]

    def _render_deferred_imports(
        self,
        imports: list[Import],
        *,
        indent: str = "",
        deferred_import: str,
    ) -> list[str]:
        blocks = []
        by_source: defaultdict[_ImportSource, list[Import]] = defaultdict(list)
        for imp in imports:
            by_source[imp.source].append(imp)
        for source in _ImportSource.__members__.values():
            block = self._render_deferred_imports_source_block(
                by_source[source],
                indent=indent,
                deferred_import=deferred_import,
            )
            if block:
                blocks.append(block)
        return blocks

    def _render_deferred_imports_source_block(
        self,
        imports: list[Import],
        *,
        indent: str = "",
        deferred_import: str,
    ) -> str:
        output: list[str] = []
        mods = sorted(
            (imp for imp in imports if imp.attr is None),
            key=lambda imp: imp.name,
        )
        for imp in mods:
            modname = imp.name
            maybe_alias = imp.alias
            if modname.startswith("."):
                match = re.match(r"^(\.+)(.*)", modname)
                assert match
                relative = match.group(1)
                rest = match.group(2)
                pkg, _, name = rest.rpartition(".")
                alias = maybe_alias or name
                import_line = (
                    f"{alias} = {deferred_import}("
                    f'"{relative}{pkg}", attr="{name}", '
                    f'alias="{alias}", package=__package__)'
                )
            else:
                alias = maybe_alias or modname
                import_line = (
                    f"{alias} = {deferred_import}("
                    f'"{modname}", alias="{alias}", package=__package__)'
                )
            output.append(import_line)

        import_lists: defaultdict[str, list[Import]] = defaultdict(list)
        for imp in imports:
            if imp.attr is not None:
                import_lists[imp.module].append(imp)

        name_imports = sorted(
            import_lists.items(),
            key=lambda kv: (len(kv[1]) == 0, kv[0]),
        )
        output.extend(
            (
                f"{imp.name} = {deferred_import}("
                f'"{imp.module}", attr="{imp.attr}", '
                f'alias="{imp.name}", package=__package__)'
            )
            for _, names in name_imports
            for imp in names
        )

        result = "\n".join(output)
        if indent:
            result = textwrap.indent(result, indent)
        return result

    def output(self, out: io.TextIOWrapper) -> None:
        typevars = self.render_typevars()
        out.write(self.get_comment_preamble())
        out.write("\n\n")
        out.write("from __future__ import annotations\n\n")
        out.write(self.render_imports())
        if typevars:
            out.write("\n\n\n")
            out.write(typevars)
        main_code = self._content[CodeSection.main]
        if main_code:
            out.write("\n\n\n")
            out.write("\n".join(main_code))
        late_imports = self.render_late_imports()
        if late_imports:
            out.write("\n\n\n")
            out.write(late_imports)
        late_code = self._content[CodeSection.after_late_import]
        if late_code:
            out.write("\n\n\n")
            out.write("\n".join(late_code))
        exports = self.render_exports()
        if exports:
            out.write("\n\n\n")
            out.write(exports)
        out.write("\n")

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
        vlist = list(values)
        if trailing_separator is None:
            trailing_separator = not carry_separator
        list_string = separator.join(vlist)
        if carry_separator:
            strip_sep = separator.lstrip()
        else:
            strip_sep = separator.rstrip()
        if list_string and trailing_separator:
            list_string += strip_sep
        output_string = tpl.format(list=list_string)
        line_length = len(output_string) + len(
            self.current_indentation(extra_indent)
        )
        if line_length > MAX_LINE_LENGTH:
            if carry_separator:
                line_sep = f"\n{self.INDENT}{strip_sep}"
            else:
                line_sep = f"{strip_sep}\n{self.INDENT}"
            list_string = line_sep.join(vlist)
            if list_string and trailing_separator:
                list_string += strip_sep
            if first_line_comment:
                list_string = f"  # {first_line_comment}\n    {list_string}\n"
            else:
                list_string = f"\n    {list_string}\n"
            output_string = tpl.format(list=list_string)
        elif first_line_comment:
            output_string += f"  # {first_line_comment}"

        return output_string
