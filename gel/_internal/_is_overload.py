# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

"""AST transformation to overload the `is` and `is not` operators.

This module provides a decorator that transform a function's source to
replace `is` and `is not` operations with calls to `__gel_is__` and
`__gel_is_not__` methods on the left-hand operand. This allows objects
to customize the behavior of identity checks, which is particularly
useful for query builder expressions where `is [not] None` needs to
trigger a TypeError.

The approach is to re-parse the function's source and replace the
identity checks with calls to methods via an ast.NodeTransformer.
This avoids complex bytecode inspection or manipulation and preserves
standard `is [not]` behavior for for objects that do not implement the
`__gel_is__` or `__gel_is_not__` methods.
"""

from __future__ import annotations
from typing import TYPE_CHECKING, Any, ParamSpec, TypeVar

import ast
import functools
import inspect
import textwrap

if TYPE_CHECKING:
    from collections.abc import Callable

_P = ParamSpec("_P")
_T = TypeVar("_T")


def maybe_overload_is_operator(func: Callable[_P, _T]) -> Callable[_P, _T]:
    """A decorator that overloads `is` and `is not` operators in a
    function.

    This decorator rewrites the function's bytecode to replace `is` and
    `is not` operations with calls to `__gel_is__` and `__gel_is_not__`
    on the left-hand operand.  This is done on a best effort basis, so
    if there is any issue accessing function source or tranforming or
    compiling it, the original function is returned unmodified.

    Args:
        func: The function to be transformed.

    Returns:
        The transformed function.
    """
    if not callable(func):
        return func

    if getattr(func, "__gel_is_overloaded__", False):
        # Already transfomed.
        return func

    try:
        source = inspect.getsource(func)
    except (OSError, TypeError):
        # If we can't get the source, we can't transform it, so
        # return the original function.
        return func

    source = textwrap.dedent(source)

    try:
        tree = ast.parse(source)
    except SyntaxError:
        # If parsing fails, return the original function.
        return func

    # `getsource` returns complete source lines, so if func is a
    # lambda, it might be just a _substring_ of source, so traverse
    # the AST until we find the first `def` or `lambda`.
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.Lambda)):
            func_tree = node
            break
    else:
        # This should not happen if the source is from a valid function.
        return func

    # Transform the AST to replace `is` and `is not` operations.
    transformer = _IsTransformer()
    def_tree: ast.stmt = transformer.visit(func_tree)
    if not transformer.num_transforms:
        # No `is` or `is not` operations were found, so no changes
        # are needed.
        return func

    # If the original function was a lambda, we need to assign it to a
    # variable to be able to extract the newly compiled function object.
    if isinstance(def_tree, ast.Lambda):
        func_name = "__gel_lambda__"
        def_tree = ast.Assign(
            targets=[ast.Name(id=func_name, ctx=ast.Store())],
            value=def_tree,
        )
    else:
        func_name = func.__name__

    tree = ast.Module(body=[def_tree], type_ignores=[])
    ast.fix_missing_locations(tree)

    try:
        code = compile(tree, func.__code__.co_filename, "exec")
    except Exception:
        return func

    # Reconstruct the closure namespace carefully.
    closure_vars = inspect.getclosurevars(func)
    globalns = {
        **func.__globals__,
        **closure_vars.globals,
        **closure_vars.nonlocals,
        "__gel_is__": _call_gel_is,
        "__gel_is_not__": _call_gel_is_not,
    }
    try:
        exec(code, globalns)  # noqa: S102
    except Exception:
        return func

    # Grab the new function and transplant original's metadata onto it.
    try:
        new_func: Callable[_P, _T] = globalns[func_name]
        new_func.__gel_is_overloaded__ = True  # type: ignore [attr-defined]
        return functools.update_wrapper(new_func, func)
    except Exception:
        return func


def _call_gel_is(obj: Any, other: Any) -> Any:
    """A helper function that is called instead of `is`.

    It tries to call `__gel_is__` on the object, and falls back to the
    standard `is` operator if the method is not defined.
    """
    if callable(gel_is := getattr(obj, "__gel_is__", None)):
        return gel_is(other)
    else:
        return obj is other


def _call_gel_is_not(obj: Any, other: Any) -> Any:
    """A helper function that is called instead of `is not`.

    It tries to call `__gel_is_not__` on the object, and falls back to
    the standard `is not` operator if the method is not defined.
    """
    if callable(gel_is_not := getattr(obj, "__gel_is_not__", None)):
        return gel_is_not(other)
    elif callable(gel_is := getattr(obj, "__gel_is__", None)):
        return not gel_is(other)
    else:
        return obj is not other


class _IsTransformer(ast.NodeTransformer):
    """An AST transformer that replaces `is` and `is not` operations.

    This transformer replaces `is` and `is not` operations with calls
    to `__gel_is__` and `__gel_is_not__` respectively. This allows us to
    overload these operators for our own objects.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._num_transforms = 0

    @property
    def num_transforms(self) -> int:
        """The total number of `is` and `is not` operations that were
        replaced.
        """
        return self._num_transforms

    def visit_Compare(self, node: ast.Compare) -> ast.AST:
        """Visit a `Compare` node and transform `is` and `is not`
        operations.
        """
        # Recursively transform any nested nodes first.
        self.generic_visit(node)

        # We only care about simple comparisons with a single operator.
        if len(node.ops) == 1 and len(node.comparators) == 1:
            op = node.ops[0]
            if isinstance(op, ast.Is):
                # Replace `x is y` with `__gel_is__(x, y)`.
                self._num_transforms += 1
                return ast.Call(
                    func=ast.Name(id="__gel_is__", ctx=ast.Load()),
                    args=[node.left, node.comparators[0]],
                    keywords=[],
                )
            elif isinstance(op, ast.IsNot):
                # Replace `x is not y` with `__gel_is_not__(x, y)`.
                self._num_transforms += 1
                return ast.Call(
                    func=ast.Name(id="__gel_is_not__", ctx=ast.Load()),
                    args=[node.left, node.comparators[0]],
                    keywords=[],
                )

        # Return the node unchanged if it's not a simple `is` or `is
        # not` comparison.
        return node
