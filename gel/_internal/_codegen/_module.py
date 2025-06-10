# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.


from __future__ import annotations
from typing import (
    TYPE_CHECKING,
    TypeAlias,
)

import collections
import collections.abc
import contextlib
import enum
import re
import textwrap
from collections import defaultdict

if TYPE_CHECKING:
    import io

    from collections.abc import Iterator, Mapping
    from collections.abc import Set as AbstractSet


MAX_LINE_LENGTH = 79


class _ImportSource(enum.Enum):
    local = enum.auto()
    lib = enum.auto()
    std = enum.auto()


class ImportTime(enum.Enum):
    runtime = enum.auto()
    late_runtime = enum.auto()
    typecheck = enum.auto()


class _ImportKind(enum.Enum):
    names = enum.auto()
    self = enum.auto()


class CodeSection(enum.Enum):
    main = enum.auto()
    after_late_import = enum.auto()


_Imports: TypeAlias = defaultdict[
    _ImportSource,
    defaultdict[_ImportKind, defaultdict[str, set[str]]],
]


def _new_imports_map() -> _Imports:
    return defaultdict(lambda: defaultdict(lambda: defaultdict(set)))


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

    def __init__(self, preamble: str) -> None:
        self._comment_preamble = preamble
        self._indent_level = 0
        self._in_type_checking = False
        self._content: defaultdict[CodeSection, list[str]] = defaultdict(list)
        self._code_section = CodeSection.main
        self._code = self._content[self._code_section]
        self._imports: defaultdict[ImportTime, _Imports] = defaultdict(
            _new_imports_map
        )
        self._globals: set[str] = set()
        self._exports: set[str] = set()
        self._imported_names: dict[tuple[str, str, ImportTime], str] = {}

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

    def udpate_globals(self, names: collections.abc.Iterable[str]) -> None:
        self._globals.update(names)

    def _get_import_strings(
        self,
        module: str,
        names: tuple[str, ...],
        aliases: dict[str, str],
    ) -> dict[_ImportKind, list[str]]:
        all_names: list[str] = []
        all_self_aliases: list[str] = []
        for n in names:
            if n == ".":
                all_self_aliases.append("")
            else:
                all_names.append(n)

        for k, v in aliases.items():
            if v == ".":  # module import
                all_self_aliases.append(k)
            else:
                all_names.append(f"{v} as {k}")

        if not all_names and not all_self_aliases:
            all_self_aliases.append("")

        return {
            _ImportKind.names: all_names,
            _ImportKind.self: all_self_aliases,
        }

    def _get_import_source(self, module: str) -> _ImportSource:
        if module == "gel" or module.startswith("gel."):
            source = _ImportSource.lib
        elif module.startswith("."):
            source = _ImportSource.local
        else:
            source = _ImportSource.std

        return source

    def _update_import_maps(
        self,
        module: str,
        names: tuple[str, ...],
        aliases: dict[str, str],
        *,
        import_maps: _Imports,
    ) -> None:
        source = self._get_import_source(module)
        import_lines = self._get_import_strings(module, names, aliases)
        for import_kind, import_strings in import_lines.items():
            import_maps[source][import_kind][module].update(import_strings)

    def _disambiguate_import_name(self, name: str) -> str:
        if name not in self._globals:
            return name

        ctr = 0

        def _mangle(name: str) -> str:
            nonlocal ctr
            if ctr == 0:
                return f"__{name}__"
            else:
                return f"__{name}_{ctr}__"

        mangled = _mangle(name)
        while mangled in self._globals:
            ctr += 1
            mangled = _mangle(name)

        return mangled

    def _import_name(
        self,
        module: str,
        name: str,
        *,
        suggested_module_alias: str | None = None,
        import_maps: _Imports,
        localns: frozenset[str] | None = None,
    ) -> tuple[str, str]:
        imported_names: tuple[str, ...] = ()
        imported_aliases = {}
        imported_module = module
        if _is_all_dots(module):
            raise ValueError(
                f"import_name: bare relative imports are "
                f"not supported: {module!r}"
            )
        if (
            name != "."
            and not _in_ns(name, self._globals)
            and not _in_ns(name, localns)
        ):
            imported = name
            new_global = imported
            imported_names += (name,)
        else:
            parent_module, dot, tail_module = module.rpartition(".")
            if _is_all_dots(parent_module) or (not parent_module and dot):
                # Pure relative import
                parent_module += dot

            if parent_module:
                imported_as = self._disambiguate_import_name(
                    suggested_module_alias or tail_module
                )
                if imported_as == tail_module:
                    imported_names += (imported_as,)
                else:
                    imported_aliases[imported_as] = tail_module
                imported_module = parent_module
            else:
                imported_as = self._disambiguate_import_name(
                    suggested_module_alias or module
                )
                imported_aliases[imported_as] = "."

            new_global = imported_as
            imported = imported_as if name == "." else f"{imported_as}.{name}"

        self._update_import_maps(
            imported_module,
            imported_names,
            imported_aliases,
            import_maps=import_maps,
        )
        return imported, new_global

    def _do_import_name(
        self,
        module: str,
        name: str,
        *,
        suggested_module_alias: str | None = None,
        import_time: ImportTime = ImportTime.runtime,
        localns: frozenset[str] | None = None,
    ) -> str:
        key = (module, name, import_time)
        imported = self._imported_names.get(key)
        cache_it = True
        if imported is not None:
            if not _in_ns(imported, localns):
                return imported
            else:
                cache_it = False

        if import_time is ImportTime.late_runtime:
            # See if there was a previous eager import for same name
            early_key = (module, name, ImportTime.runtime)
            imported = self._imported_names.get(early_key)
            if imported is not None:
                self._imported_names[key] = imported
                return imported

        imported, new_global = self._import_name(
            module,
            name,
            suggested_module_alias=suggested_module_alias,
            import_maps=self._imports[import_time],
            localns=localns,
        )

        self._globals.add(new_global)

        if cache_it:
            self._imported_names[key] = imported

        return imported

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
        if directly:
            return self._do_import_name(
                module,
                name,
                suggested_module_alias=suggested_module_alias,
                import_time=import_time,
                localns=localns,
            )
        else:
            mod = self._do_import_name(
                module,
                ".",
                suggested_module_alias=suggested_module_alias,
                import_time=import_time,
                localns=localns,
            )
            return f"{mod}.{name}"

    def render_name_import(
        self,
        module: str,
        name: str,
        *,
        suggested_module_alias: str | None = None,
    ) -> tuple[str, str]:
        import_maps = _new_imports_map()
        imported, _ = self._import_name(
            module,
            name,
            suggested_module_alias=suggested_module_alias,
            import_maps=import_maps,
        )

        import_chunks = self._render_imports(import_maps)
        import_code = "\n\n".join(import_chunks)

        return imported, import_code

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

    def render_imports(self) -> str:
        typecheck_sections = self._render_imports(
            self._imports[ImportTime.typecheck],
            indent="    ",
        )

        tc = None
        if any(typecheck_sections):
            tc = self.import_name("typing", "TYPE_CHECKING")

        sections = ["from __future__ import annotations"]
        sections.extend(
            self._render_imports(self._imports[ImportTime.runtime])
        )

        if any(typecheck_sections):
            assert tc
            sections.append(f"if {tc}:")
            sections.extend(typecheck_sections)

        return "\n\n".join(filter(None, sections))

    def render_late_imports(self) -> str:
        sections = self._render_imports(
            self._imports[ImportTime.late_runtime],
            noqa=["E402", "F403"],
        )

        return "\n\n".join(filter(None, sections))

    def _render_imports(
        self,
        imports: _Imports,
        *,
        indent: str = "",
        noqa: list[str] | None = None,
    ) -> list[str]:
        blocks = []
        for source in _ImportSource.__members__.values():
            block = self._render_imports_source_block(
                imports[source],
                indent=indent,
                noqa=noqa,
            )
            if block:
                blocks.append(block)
        return blocks

    def _render_imports_source_block(
        self,
        imports: Mapping[_ImportKind, Mapping[str, set[str]]],
        *,
        indent: str = "",
        noqa: list[str] | None = None,
    ) -> str:
        output = []
        self_imports = imports[_ImportKind.self]
        mods = sorted(
            self_imports.items(),
            key=lambda kv: (len(kv[1]) == 0, kv[0]),
        )
        for modname, aliases in mods:
            for alias in aliases:
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

        name_imports = imports[_ImportKind.names]
        mods = sorted(
            name_imports.items(),
            key=lambda kv: (len(kv[1]) == 0, kv[0]),
        )

        noqa_suf = f"  # noqa: {' '.join(noqa)}" if noqa else ""

        for modname, names in mods:
            if not names:
                continue
            import_line = f"from {modname} import "
            names_list = list(names)
            names_list.sort()
            names_part = ", ".join(names_list)
            if len(import_line) + len(names_part) > MAX_LINE_LENGTH:
                import_line += (
                    f"({noqa_suf}\n    " + ",\n    ".join(names_list) + "\n)"
                )
            else:
                import_line += names_part + noqa_suf
            output.append(import_line)

        result = "\n".join(output)
        if indent:
            result = textwrap.indent(result, indent)
        return result

    def output(self, out: io.TextIOWrapper) -> None:
        out.write(self.get_comment_preamble())
        out.write("\n\n")
        out.write(self.render_imports())
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
        values: list[str],
        *,
        first_line_comment: str | None = None,
        extra_indent: int = 0,
        separator: str = ", ",
        carry_separator: bool = False,
        trailing_separator: bool | None = None,
    ) -> str:
        list_string = separator.join(values)
        output_string = tpl.format(list=list_string)
        line_length = len(output_string) + len(
            self.current_indentation(extra_indent)
        )
        if trailing_separator is None:
            trailing_separator = not carry_separator
        if line_length > MAX_LINE_LENGTH:
            if carry_separator:
                strip_sep = separator.lstrip()
                line_sep = f"\n{self.INDENT}{strip_sep}"
            else:
                strip_sep = separator.rstrip()
                line_sep = f"{strip_sep}\n{self.INDENT}"
            list_string = line_sep.join(values)
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
