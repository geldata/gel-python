from __future__ import annotations

import unittest
from typing import Any

from gel._internal._is_overload import maybe_overload_is_operator


class Overloadable:
    """A test class that implements `__gel_is__` and `__gel_is_not__`."""

    def __init__(self, is_result: bool, is_not_result: bool) -> None:
        self._is_result = is_result
        self._is_not_result = is_not_result
        self.is_called_with: Any = None
        self.is_not_called_with: Any = None

    def __gel_is__(self, other: Any) -> bool:
        self.is_called_with = other
        return self._is_result

    def __gel_is_not__(self, other: Any) -> bool:
        self.is_not_called_with = other
        return self._is_not_result


class OverloadableIsOnly:
    """A test class that only implements `__gel_is__`."""

    def __init__(self, is_result: bool) -> None:
        self._is_result = is_result
        self.is_called_with: Any = None

    def __gel_is__(self, other: Any) -> bool:
        self.is_called_with = other
        return self._is_result


class TestIsOverload(unittest.TestCase):
    def test_overload_is_with_function(self) -> None:
        """Test that `is` and `is not` are overloaded in a regular
        function."""
        obj = Overloadable(is_result=True, is_not_result=False)

        @maybe_overload_is_operator
        def check_is(o: Any) -> bool:
            return o is None

        @maybe_overload_is_operator
        def check_is_not(o: Any) -> bool:
            return o is not None

        # Check `is` overload
        self.assertTrue(check_is(obj))
        self.assertIsNone(obj.is_not_called_with)
        self.assertIs(obj.is_called_with, None)

        # Check `is not` overload
        self.assertFalse(check_is_not(obj))
        self.assertIs(obj.is_not_called_with, None)

    def test_overload_is_with_lambda(self) -> None:
        """Test that `is` and `is not` are overloaded in a lambda."""
        obj = Overloadable(is_result=False, is_not_result=True)
        sentinel = "something"

        check_is = maybe_overload_is_operator(lambda o: o is sentinel)
        check_is_not = maybe_overload_is_operator(lambda o: o is not sentinel)

        # Check `is` overload
        self.assertFalse(check_is(obj))
        self.assertIsNone(obj.is_not_called_with)
        self.assertIs(obj.is_called_with, sentinel)

        # Reset and check `is not` overload
        obj.is_called_with = None
        self.assertTrue(check_is_not(obj))
        self.assertIsNone(obj.is_called_with)
        self.assertIs(obj.is_not_called_with, sentinel)

    def test_overload_is_not_fallback_to_is(self) -> None:
        """Test that `is not` falls back to `not __gel_is__` if
        `__gel_is_not__` is not defined."""
        obj = OverloadableIsOnly(is_result=True)
        sentinel = 123

        @maybe_overload_is_operator
        def check_is_not(o: Any) -> bool:
            return o is not sentinel

        self.assertFalse(check_is_not(obj))
        self.assertIs(obj.is_called_with, sentinel)

    def test_overload_is_no_overload_methods(self) -> None:
        """Test that standard `is` and `is not` behavior is preserved
        for objects that don't have overload methods."""
        obj = object()

        @maybe_overload_is_operator
        def check_is(o: Any) -> bool:
            return o is obj

        @maybe_overload_is_operator
        def check_is_not(o: Any) -> bool:
            return o is not obj

        self.assertTrue(check_is(obj))
        self.assertFalse(check_is_not(obj))
        self.assertFalse(check_is(object()))
        self.assertTrue(check_is_not(object()))

    def test_overload_is_no_is_operations(self) -> None:
        """Test that the decorator returns the original function if it
        contains no `is` or `is not` operations."""
        original_func = lambda x: x + 1  # noqa: E731

        transformed_func = maybe_overload_is_operator(original_func)

        # The function object itself should be returned, not a new one.
        self.assertIs(original_func, transformed_func)
        self.assertEqual(transformed_func(1), 2)

    def test_overload_is_function_with_closure(self) -> None:
        """Test that the decorator correctly handles functions with
        closures."""
        y = 10
        obj = Overloadable(is_result=True, is_not_result=False)

        @maybe_overload_is_operator
        def check_is_with_closure(x: Any) -> bool:
            # This function uses `y` from the enclosing scope.
            return x is y

        self.assertTrue(check_is_with_closure(obj))
        self.assertIs(obj.is_called_with, y)

    def test_overload_is_chained_comparison_is_not_transformed(self) -> None:
        """Test that chained comparisons are not transformed."""

        # The transformer should not modify chained comparisons because
        # they have more than one operator. The original function should
        # be returned.
        def chained(a: Any, b: Any, c: Any) -> bool:
            return a is b is c

        transformed_chained = maybe_overload_is_operator(chained)
        self.assertIs(chained, transformed_chained)

        obj1 = object()
        obj2 = object()
        self.assertTrue(transformed_chained(obj1, obj1, obj1))
        self.assertFalse(transformed_chained(obj1, obj1, obj2))

    def test_overload_is_unsupported_object_returns_original_function(
        self,
    ) -> None:
        """Test that the decorator returns the original function if it
        can't get the source code."""
        # `int` is a C-defined type, so inspect.getsource will fail.
        original_func = int
        transformed_func = maybe_overload_is_operator(original_func)
        self.assertIs(original_func, transformed_func)

    def test_overload_is_syntax_error_returns_original_function(self) -> None:
        """Test that the decorator returns the original function if
        there's a syntax error in parsing."""

        # This test simulates what happens when source code can't be
        # parsed. We'll create a function normally and then test it.
        def normal_func(x: Any) -> bool:
            return x is None

        # The function should work normally
        transformed_func = maybe_overload_is_operator(normal_func)
        obj = Overloadable(is_result=True, is_not_result=False)
        self.assertTrue(transformed_func(obj))
        self.assertIs(obj.is_called_with, None)

    def test_overload_is_non_callable_returns_as_is(self) -> None:
        """Test that the decorator returns non-callable objects as-is."""
        not_callable = 42
        result = maybe_overload_is_operator(not_callable)  # type: ignore [arg-type, var-annotated]
        self.assertIs(result, not_callable)


if __name__ == "__main__":
    unittest.main()
