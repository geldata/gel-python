# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

from __future__ import annotations
from typing import TYPE_CHECKING, Any, NamedTuple

import importlib
import inspect
import sys
import weakref

if TYPE_CHECKING:
    import types


def _get_caller_module(stack_offset: int = 2) -> types.ModuleType | None:
    frame = inspect.currentframe()
    try:
        counter = 0
        while frame is not None and counter < stack_offset:
            frame = frame.f_back
            counter += 1

        if (
            frame is not None
            and (mod_name := frame.f_globals.get("__name__"))
            and (caller_mod := sys.modules.get(mod_name)) is not None
        ):
            return caller_mod
    finally:
        if frame is not None:
            # Break possible refcycle (of this frame onto itself via locals)
            del frame

    return None


def _possibly_circular_import_error(err: AttributeError) -> bool:
    """Check if a given AttribueError could have arisen from accessing
    a partially initialized module."""
    spec = getattr(err.obj, "__spec__", None)
    if spec is not None:
        if getattr(spec, "_initializing", False):
            return True
        uninit_submodules = getattr(spec, "_uninitialized_submodules", None)
        if uninit_submodules and err.name in uninit_submodules:
            return True

    return "circular import" in str(err.args[0])


class _Import(NamedTuple):
    module: str
    attr: str | None
    alias: str
    package: str | None


def _import(what: _Import) -> types.ModuleType:
    modname = what.module
    module = importlib.import_module(modname, what.package)
    if (attr := what.attr) is not None:
        if modname[-1] == ".":
            submod = f"{modname}{attr}"
        else:
            submod = f"{modname}.{attr}"

        module = importlib.import_module(submod, what.package)

    return module


class DeferredImport:
    """Proxy for an import that defers import until first attribute access.

    Upon import binds the module in the instantiating module's namespace.
    DeferredImport is primarily intended to be used to support
    runtime type evaluation for ForwardRefs containing types from other
    modules that potentially form import cycles.  Thus, DeferredImport will
    convert AttributeError exceptions caused by  accesses to partially
    initialized modules to NameError exceptions for compatibility with
    runtime type evaluation.
    """

    def __init__(
        self,
        module: str,
        attr: str | None = None,
        alias: str | None = None,
        package: str | None = None,
    ) -> None:
        """Initialize a DeferredImport proxy.

        Args:
            module: The module name to import. Can be a dotted name
                like "os.path".
            attr: Optional submodule name to import from the module.
                If provided, this will import the specific submodule rather
                than the entire module. Note: This parameter is only for
                importing submodules, not attributes like classes or functions.
            alias: Optional alias name to bind the imported object to in the
                caller's namespace. If not provided, defaults to:
                - The `attr` name if `attr` is specified
                - The last component of the module name otherwise
            package: Optional package name for relative imports. Used as the
                anchor for resolving relative module names.

        Examples:
            >>> # Equivalent to: import os
            >>> os = DeferredImport('os')

            >>> # Equivalent to: from os import path (path is a submodule)
            >>> path = DeferredImport('os', attr='path')

            >>> # Equivalent to: import os.path as pathname
            >>> pathname = DeferredImport('os.path', alias='pathname')

            >>> # Equivalent to: from .parent import submodule
            >>> submodule = DeferredImport('parent', attr='submodule',
                                           package=__package__)

        Note:
            The actual import is deferred until the first attribute access on
            the proxy object. Upon successful import, the imported object is
            automatically bound to the caller's module namespace using the
            determined alias name.

            This class is designed specifically for importing modules and
            submodules only, not individual attributes like classes or
            functions.
        """
        if not alias:
            if attr is not None:
                alias = attr
            else:
                _, _, alias = module.rpartition(".")

        self.__lm_import__ = _Import(
            module=module,
            attr=attr,
            alias=alias,
            package=package,
        )
        self.__lm_module__: weakref.ref[types.ModuleType] | None = None
        self.__lm_importing_module__: weakref.ref[types.ModuleType] | None
        if (caller_module := _get_caller_module()) is not None:
            self.__lm_importing_module__ = weakref.ref(caller_module)
        else:
            self.__lm_importing_module__ = None

    def __load(self) -> types.ModuleType:
        if (mod_ref := self.__lm_module__) is not None and (
            mod := mod_ref()
        ) is not None:
            return mod
        else:
            # do the real import
            mod = _import(self.__lm_import__)
            self.__lm_module__ = weakref.ref(mod)
            impmod_ref = self.__lm_importing_module__
            if impmod_ref is not None and (impmod := impmod_ref()) is not None:
                try:
                    setattr(impmod, self.__lm_import__.alias, mod)
                except (AttributeError, TypeError):
                    pass
            return mod

    def __getattr__(self, attr: str) -> Any:
        mod = self.__load()
        try:
            return getattr(mod, attr)
        except AttributeError as e:
            if _possibly_circular_import_error(e):
                # For attribute errors triggered by accessing stuff
                # from partially initialized modules we want NameErrors
                # instead to signal to the type-evaluation machinery that
                # the ForwardRef cannot be resolved yet.
                # This is understandably fragile, but I could not find
                # a less smelly way of doing this.
                raise NameError(attr) from e
            else:
                raise

    def __dir__(self) -> list[str]:
        return dir(self.__load())

    def __repr__(self) -> str:
        spec = self.__lm_import__
        if spec.attr is not None:
            import_line = f"from {spec.module} import {spec.attr}"
        else:
            import_line = f"import {spec.module}"
        if spec.alias is not None:
            import_line = f"{import_line} as {spec.alias}"
        package = spec.package if spec.package is not None else "__main__"
        return f"<LazyModule proxy for '{import_line}' in '{package}'>"
