# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

"""
Comprehensive tests for gel._internal._typing_dispatch module.

Tests cover overload dispatch functionality, type checking, edge cases,
and various Python type system features.
"""

from typing import (
    Any,
    Annotated,
    Generic,
    Literal,
    TypeVar,
)
from typing_extensions import overload
from collections.abc import (
    Callable,
    Mapping,
    Sequence,
)

import unittest

from gel._internal._typing_dispatch import dispatch_overload


_T = TypeVar("_T")


class TestDispatchOverload(unittest.TestCase):
    """Test the dispatch_overload decorator."""

    def test_basic_overload_dispatch(self) -> None:
        """Test basic overload dispatch with different types."""

        @overload
        def process(x: int) -> str:
            return f"int: {x}"

        @overload
        def process(x: str) -> str:
            return f"str: {x}"

        @overload
        def process(x: str, y: int) -> str:
            return f"str: {x}, int: {y}"

        @dispatch_overload
        def process(x: int | str, y: int | None = None) -> str:
            raise NotImplementedError

        self.assertEqual(process(42), "int: 42")
        self.assertEqual(process("hello"), "str: hello")
        self.assertEqual(process("hello", 42), "str: hello, int: 42")

    def test_union_type_dispatch(self) -> None:
        """Test dispatch with Union types."""

        @overload
        def handle(x: int | float) -> str:
            return f"number: {x}"

        @overload
        def handle(x: str) -> str:
            return f"string: {x}"

        @dispatch_overload
        def handle(x: int | float | str) -> str:
            raise NotImplementedError

        self.assertEqual(handle(42), "number: 42")
        self.assertEqual(handle(3.14), "number: 3.14")
        self.assertEqual(handle("test"), "string: test")

    def test_optional_type_dispatch(self) -> None:
        """Test dispatch with Optional types."""

        @overload
        def maybe_process(x: int) -> str:
            return f"int: {x}"

        @overload
        def maybe_process(x: None) -> str:
            return "none"

        @dispatch_overload
        def maybe_process(x: int | None) -> str:
            raise NotImplementedError

        self.assertEqual(maybe_process(42), "int: 42")
        self.assertEqual(maybe_process(None), "none")

    def test_multiple_parameters(self) -> None:
        """Test dispatch with multiple parameters."""

        @overload
        def combine(x: int, y: int) -> str:
            return f"int+int: {x}+{y}"

        @overload
        def combine(x: str, y: str) -> str:
            return f"str+str: {x}+{y}"

        @dispatch_overload
        def combine(x: int | str, y: int | str) -> str:
            raise NotImplementedError

        self.assertEqual(combine(1, 2), "int+int: 1+2")
        self.assertEqual(combine("a", "b"), "str+str: a+b")

    def test_keyword_arguments(self) -> None:
        """Test dispatch with keyword arguments."""

        @overload
        def func(*, x: int, y: str = "default") -> str:
            return f"int: {x}, str: {y}"

        @overload
        def func(*, x: str, y: int = 42) -> str:
            return f"str: {x}, int: {y}"

        @dispatch_overload
        def func(*, x: int | str, y: str | int = None) -> str:  # type: ignore[assignment]
            raise NotImplementedError

        self.assertEqual(func(x=42), "int: 42, str: default")
        self.assertEqual(func(x=42, y="custom"), "int: 42, str: custom")
        self.assertEqual(func(x="hello"), "str: hello, int: 42")
        self.assertEqual(func(x="hello", y=99), "str: hello, int: 99")

    def test_no_matching_overload(self) -> None:
        """Test error when no overload matches."""

        @overload
        def strict_func(x: int) -> str:
            return f"int: {x}"

        @overload
        def strict_func(x: str) -> str:
            return f"str: {x}"

        @dispatch_overload
        def strict_func(x: int | str) -> str:
            raise NotImplementedError

        with self.assertRaises(TypeError) as cm:
            strict_func(3.14)  # type: ignore[call-overload]

        self.assertIn("no overload found", str(cm.exception))

    def test_bound_method_handling(self) -> None:
        """Test proper handling of bound methods."""

        class TestClass:
            def __init__(self, value: int) -> None:
                self.value = value

            @overload
            def add(self, x: int) -> int:
                return self.value + x

            @overload
            def add(self, x: str) -> str:
                return f"{self.value}{x}"

            @dispatch_overload
            def add(self, x: int | str) -> int | str:
                raise NotImplementedError

        obj = TestClass(10)
        self.assertEqual(obj.add(5), 15)
        self.assertEqual(obj.add("test"), "10test")

        # Test that bound method preserves self
        bound_method = obj.add
        self.assertEqual(bound_method(20), 30)
        self.assertEqual(bound_method("bound"), "10bound")

        # and unbound can still be called directly
        unbound_method = TestClass.add
        self.assertEqual(unbound_method(obj, 30), 40)
        self.assertEqual(unbound_method(obj, "unbound"), "10unbound")

    def test_static_method_dispatch(self) -> None:
        """Test dispatch on static methods."""

        class Utils:
            @overload
            @staticmethod
            def helper(x: int) -> str:
                return f"static int: {x}"

            @overload
            @staticmethod
            def helper(x: str) -> str:
                return f"static str: {x}"

            @dispatch_overload
            @staticmethod
            def helper(x: int | str) -> str:
                raise NotImplementedError

        self.assertEqual(Utils.helper(42), "static int: 42")
        self.assertEqual(Utils.helper("test"), "static str: test")

    def test_class_method_dispatch(self) -> None:
        """Test dispatch on class methods."""

        class Factory:
            @overload
            @classmethod
            def create(cls, x: int) -> str:
                return f"class int: {x}"

            @overload
            @classmethod
            def create(cls, x: str) -> str:
                return f"class str: {x}"

            @dispatch_overload
            @classmethod
            def create(cls, x: int | str) -> str:
                raise NotImplementedError

        self.assertEqual(Factory.create(42), "class int: 42")
        self.assertEqual(Factory.create("test"), "class str: test")

    def test_inheritance_dispatch(self) -> None:
        """Test dispatch with inheritance."""

        class Base:
            @overload
            def method(self, x: int) -> str:
                return f"base int: {x}"

            @overload
            def method(self, x: str) -> str:
                return f"base str: {x}"

            @dispatch_overload
            def method(self, x: int | str) -> str:
                raise NotImplementedError

        class Derived(Base):
            pass

        obj = Derived()
        self.assertEqual(obj.method(42), "base int: 42")
        self.assertEqual(obj.method("test"), "base str: test")

    def test_empty_overloads(self) -> None:
        """Test function with no overloads."""

        @dispatch_overload
        def no_overloads(x: int) -> str:
            return f"direct: {x}"

        # Should raise TypeError since no overloads exist
        with self.assertRaises(TypeError) as cm:
            no_overloads(42)

        self.assertIn("no overload found", str(cm.exception))

    def test_complex_type_matching(self) -> None:
        """Test complex type matching scenarios."""

        @overload
        def process(x: list[int]) -> str:
            return f"list of int: {x}"

        @overload
        def process(x: dict[str, int]) -> str:
            return f"dict str->int: {x}"

        @dispatch_overload
        def process(x: list[int] | dict[str, int]) -> str:
            raise NotImplementedError

        self.assertEqual(process([1, 2, 3]), "list of int: [1, 2, 3]")
        self.assertEqual(process({"a": 1}), "dict str->int: {'a': 1}")

    def test_literal_type_support(self) -> None:
        """Test Literal type support in overload dispatch."""

        @overload
        def handle_status(status: Literal["ok"]) -> str:
            return "All good"

        @overload
        def handle_status(status: Literal["error"]) -> str:
            return "Something went wrong"

        @dispatch_overload
        def handle_status(status: Literal["ok", "error"]) -> str:
            raise NotImplementedError

        self.assertEqual(handle_status("ok"), "All good")
        self.assertEqual(handle_status("error"), "Something went wrong")

        with self.assertRaises(TypeError):
            handle_status("unknown")  # type: ignore[call-overload]

    def test_any_type_support(self) -> None:
        """Test Any type support."""

        @overload
        def accept_anything(x: int) -> str:
            return f"specific int: {x}"

        @overload
        def accept_anything(x: Any) -> str:
            return f"any: {x}"

        @dispatch_overload
        def accept_anything(x: Any) -> str:
            raise NotImplementedError

        # int should match the specific overload
        self.assertEqual(accept_anything(42), "specific int: 42")
        # Other types should match Any
        self.assertEqual(accept_anything("hello"), "any: hello")

    def test_collections_abc_types(self) -> None:
        """Test collections.abc type checking."""

        @overload
        def process_container(x: Sequence[int]) -> str:
            return f"sequence: {x}"

        @overload
        def process_container(x: Mapping[str, int]) -> str:
            return f"mapping: {x}"

        @overload
        def process_container(x: Mapping[str, list[int]]) -> str:
            return "str_to_int_list"

        @overload
        def process_container(x: Mapping[int, list[str]]) -> str:
            return "int_to_str_list"

        @dispatch_overload
        def process_container(x: Any) -> str:
            raise NotImplementedError

        self.assertEqual(process_container([1, 2, 3]), "sequence: [1, 2, 3]")
        self.assertEqual(process_container({"a": 1}), "mapping: {'a': 1}")
        self.assertEqual(
            process_container({"key": [1, 2, 3]}), "str_to_int_list"
        )
        self.assertEqual(
            process_container({42: ["a", "b", "c"]}), "int_to_str_list"
        )

        with self.assertRaisesRegex(TypeError, "no overload found"):
            process_container("hello")  # type: ignore [arg-type]

    def test_collections_abc_bad_types(self) -> None:
        """Test collections.abc type checking."""

        @overload
        def process_container(x: Sequence[int]) -> str:
            return f"sequence: {x}"

        @overload
        def process_container(x: Mapping[str]) -> str:  # type: ignore [type-arg]
            return f"mapping: {x}"

        @dispatch_overload
        def process_container(
            x: Sequence[int] | Mapping[str],  # type: ignore [type-arg]
        ) -> str:
            raise NotImplementedError

        with self.assertRaisesRegex(
            TypeError,
            r"_isinstance\(\) argument 2 contains improperly typed "
            r"collections.abc.Mapping generic",
        ):
            process_container({"a": 1})

    def test_callable_type_checking(self) -> None:
        """Test Callable type checking."""

        @overload
        def handle_callable(x: Callable[[int], str]) -> str:
            return f"int->str callable: {x}"

        @overload
        def handle_callable(x: Callable[[str], int]) -> str:
            return f"str->int callable: {x}"

        @dispatch_overload
        def handle_callable(
            x: Callable[[int], str] | Callable[[str], int],
        ) -> str:
            raise NotImplementedError

        def sample_func(x: int) -> str:
            return str(x)

        # Note: Runtime callable type checking is limited
        result = handle_callable(sample_func)
        self.assertTrue(result.startswith("int->str callable:"))
        self.assertIn("sample_func", result)

    def test_type_with_inheritance_complex(self) -> None:
        """Test complex inheritance scenarios."""

        class BaseClass:
            pass

        class MiddleClass(BaseClass):
            pass

        class DerivedClass(MiddleClass):
            pass

        @overload
        def factory(x: type[DerivedClass]) -> str:
            return f"derived class: {x}"

        @overload
        def factory(x: type[BaseClass]) -> str:
            return f"base class: {x}"

        @dispatch_overload
        def factory(x: type[BaseClass] | type[DerivedClass]) -> str:
            raise NotImplementedError

        # More specific overload should win
        # (DerivedClass is more specific than BaseClass)
        result = factory(DerivedClass)
        self.assertTrue(result.startswith("derived class:"))
        self.assertIn("DerivedClass", result)

        class UnrelatedClass:
            pass

        # Test inheritance checking
        result = factory(MiddleClass)
        self.assertTrue(result.startswith("base class:"))
        self.assertIn("MiddleClass", result)

    def test_overload_ordering_priority(self) -> None:
        """Test overload ordering and priority."""

        @overload
        def prioritize(x: int) -> str:  # Most specific
            return f"int: {x}"

        @overload
        def prioritize(x: int | float) -> str:  # Less specific
            return f"number: {x}"

        @overload
        def prioritize(x: Any) -> str:  # Least specific
            return f"any: {x}"

        @dispatch_overload
        def prioritize(x: Any) -> str:
            raise NotImplementedError

        # Most specific should win
        self.assertEqual(prioritize(42), "int: 42")
        self.assertEqual(prioritize(3.14), "number: 3.14")
        self.assertEqual(prioritize("hello"), "any: hello")

    def test_function_attributes_preserved(self) -> None:
        """Test that function attributes are preserved."""

        @overload
        def documented_func(x: int) -> str:
            """Handle integers."""
            return f"int: {x}"

        @overload
        def documented_func(x: str) -> str:
            """Handle strings."""
            return f"str: {x}"

        @dispatch_overload
        def documented_func(x: int | str) -> str:
            """Main dispatcher function."""
            raise NotImplementedError

        # Check that function attributes are accessible
        self.assertEqual(documented_func.__name__, "documented_func")
        self.assertIsNotNone(documented_func.__doc__)
        self.assertEqual(documented_func(42), "int: 42")
        self.assertEqual(documented_func("test"), "str: test")

    def test_method_attributes_preserved(self) -> None:
        """Test that method attributes are preserved."""

        class TestClass:
            @overload
            def documented_method(self, x: int) -> str:
                """Handle integers."""
                return f"int: {x}"

            @overload
            def documented_method(self, x: str) -> str:
                """Handle strings."""
                return f"str: {x}"

            @dispatch_overload
            def documented_method(self, x: int | str) -> str:
                """Main dispatcher method."""
                raise NotImplementedError

        obj = TestClass()

        # Check that method attributes are accessible
        self.assertEqual(obj.documented_method.__name__, "documented_method")
        self.assertIsNotNone(obj.documented_method.__doc__)
        self.assertEqual(obj.documented_method(42), "int: 42")
        self.assertEqual(obj.documented_method("test"), "str: test")

    def test_classmethod_attributes_preserved(self) -> None:
        """Test that classmethod attributes are preserved."""

        class TestClass:
            @overload
            @classmethod
            def documented_classmethod(cls, x: int) -> str:
                """Handle integers."""
                return f"int: {x}"

            @overload
            @classmethod
            def documented_classmethod(cls, x: str) -> str:
                """Handle strings."""
                return f"str: {x}"

            @dispatch_overload
            @classmethod
            def documented_classmethod(cls, x: int | str) -> str:
                """Main dispatcher classmethod."""
                raise NotImplementedError

        # Check that classmethod attributes are accessible
        self.assertEqual(
            TestClass.documented_classmethod.__name__, "documented_classmethod"
        )
        self.assertIsNotNone(TestClass.documented_classmethod.__doc__)
        self.assertEqual(TestClass.documented_classmethod(42), "int: 42")
        self.assertEqual(TestClass.documented_classmethod("test"), "str: test")

    def test_staticmethod_attributes_preserved(self) -> None:
        """Test that staticmethod attributes are preserved."""

        class TestClass:
            @overload
            @staticmethod
            def documented_staticmethod(x: int) -> str:
                """Handle integers."""
                return f"int: {x}"

            @overload
            @staticmethod
            def documented_staticmethod(x: str) -> str:
                """Handle strings."""
                return f"str: {x}"

            @dispatch_overload
            @staticmethod
            def documented_staticmethod(x: int | str) -> str:
                """Main dispatcher staticmethod."""
                raise NotImplementedError

        # Check that staticmethod attributes are accessible
        self.assertEqual(
            TestClass.documented_staticmethod.__name__,
            "documented_staticmethod",
        )
        self.assertIsNotNone(TestClass.documented_staticmethod.__doc__)
        self.assertEqual(TestClass.documented_staticmethod(42), "int: 42")
        self.assertEqual(
            TestClass.documented_staticmethod("test"), "str: test"
        )

    def test_descriptor_access_from_class(self) -> None:
        """Test accessing dispatch method from class (not instance)."""

        class TestClass:
            @overload
            def method(self, x: int) -> str:
                return f"int: {x}"

            @overload
            def method(self, x: str) -> str:
                return f"str: {x}"

            @dispatch_overload
            def method(self, x: int | str) -> str:
                raise NotImplementedError

        # Accessing from class should return the descriptor itself
        descriptor = TestClass.method
        self.assertIsInstance(descriptor, type(TestClass.method))

        # Should be callable when bound to instance
        obj = TestClass()
        bound_method = TestClass.method.__get__(obj, TestClass)
        self.assertEqual(bound_method(42), "int: 42")

    def test_generic_origin_fallback(self) -> None:
        """Test fallback to origin type for unsupported generics."""

        class MyType(Generic[_T]):
            pass

        @overload
        def handle_generic_fallback(x: dict[str, Any]) -> str:
            return "dict"

        @overload
        def handle_generic_fallback(x: MyType[int]) -> str:
            return "MyType"

        @dispatch_overload
        def handle_generic_fallback(
            x: dict[str, Any] | MyType[int],
        ) -> str:
            raise NotImplementedError

        # These should work by falling back to origin type checking
        self.assertEqual(handle_generic_fallback({"key": "value"}), "dict")
        self.assertEqual(handle_generic_fallback(MyType[int]()), "MyType")

    def test_tuple_heterogeneous_exact_match(self) -> None:
        """Test heterogeneous tuples with exact element count checking."""

        @overload
        def process_exact_tuple(items: tuple[int, str]) -> str:
            return "int_str_pair"

        @overload
        def process_exact_tuple(items: tuple[str, int, bool]) -> str:
            return "str_int_bool_triple"

        @dispatch_overload
        def process_exact_tuple(
            items: tuple[int, str] | tuple[str, int, bool],
        ) -> str:
            raise NotImplementedError

        self.assertEqual(process_exact_tuple((42, "hello")), "int_str_pair")
        self.assertEqual(
            process_exact_tuple(("hello", 42, True)), "str_int_bool_triple"
        )

        # Wrong tuple length should not match
        with self.assertRaises(TypeError):
            process_exact_tuple((42, "hello", "extra"))  # type: ignore [arg-type]

    def test_tuple_length_mismatch_handling(self) -> None:
        """Test tuple type checking when lengths don't match."""

        @overload
        def strict_tuple(items: tuple[int, str, bool]) -> str:
            return "exact_triple"

        @overload
        def strict_tuple(items: tuple[str, int]) -> str:
            return "str_int_pair"

        @dispatch_overload
        def strict_tuple(
            items: tuple[int, str, bool] | tuple[str, int],
        ) -> str:
            raise NotImplementedError

        # Correct length should work
        self.assertEqual(strict_tuple((42, "test", True)), "exact_triple")

        # Wrong lengths should not match and raise TypeError
        with self.assertRaises(TypeError):
            strict_tuple((42, "test"))  # type: ignore [arg-type]  # Too short

        with self.assertRaises(TypeError):
            strict_tuple((42, "test", True, "extra"))  # type: ignore [arg-type]  # Too long

    def test_tuple_homogeneous_ellipsis(self) -> None:
        """Test tuple[T, ...] homogeneous type checking."""

        @overload
        def process_var_tuple(items: tuple[int, ...]) -> str:
            return "int_var_tuple"

        @overload
        def process_var_tuple(items: tuple[str, ...]) -> str:
            return "str_var_tuple"

        @dispatch_overload
        def process_var_tuple(
            items: tuple[int, ...] | tuple[str, ...],
        ) -> str:
            raise NotImplementedError

        # Variable length tuples should work
        self.assertEqual(process_var_tuple((1,)), "int_var_tuple")
        self.assertEqual(process_var_tuple((1, 2, 3, 4, 5)), "int_var_tuple")
        self.assertEqual(process_var_tuple(("a", "b")), "str_var_tuple")

    def test_empty_tuple_handling(self) -> None:
        """Test empty tuple handling."""

        @overload
        def handle_empty_tuple(items: tuple[int, ...]) -> str:
            return "empty_int_tuple"

        @overload
        def handle_empty_tuple(items: tuple[str, ...]) -> str:
            return "empty_str_tuple"

        @dispatch_overload
        def handle_empty_tuple(
            items: tuple[int, ...] | tuple[str, ...],
        ) -> str:
            raise NotImplementedError

        # Empty tuple should work without errors
        empty_tuple: tuple[int, ...] = ()
        self.assertEqual(handle_empty_tuple(empty_tuple), "empty_int_tuple")

    def test_tuple_no_match(self) -> None:
        """Test when argument is not a tuple"""

        @overload
        def process_var_tuple(items: tuple[int, ...]) -> str:
            return "int_var_tuple"

        @overload
        def process_var_tuple(items: tuple[str, ...]) -> str:
            return "str_var_tuple"

        @dispatch_overload
        def process_var_tuple(
            items: tuple[int, ...] | tuple[str, ...],
        ) -> str:
            raise NotImplementedError

        with self.assertRaisesRegex(TypeError, "no overload found"):
            process_var_tuple(1)  # type: ignore [call-overload]

    def test_mro_entries_support(self) -> None:
        """Test __mro_entries__ support for type checking."""

        @overload
        def handle_mro(x: type[int]) -> str:
            return "handles_int_type"

        @overload
        def handle_mro(x: type[str]) -> str:
            return "handles_str_type"

        @dispatch_overload
        def handle_mro(x: type[int] | type[str]) -> str:
            raise NotImplementedError

        # Test with actual type
        self.assertEqual(handle_mro(int), "handles_int_type")

        # Test with annotated type
        self.assertEqual(
            handle_mro(Annotated[str, "foo"]),  # type: ignore [call-overload]
            "handles_str_type",
        )

    def test_empty_collections_type_args(self) -> None:
        """Test empty collections with various type argument scenarios."""

        @overload
        def handle_collections(items: list[int]) -> str:
            return "int_list"

        @overload
        def handle_collections(items: set[str]) -> str:
            return "str_set"

        @overload
        def handle_collections(items: dict[str, int]) -> str:
            return "str_int_dict"

        @dispatch_overload
        def handle_collections(
            items: list[int] | set[str] | dict[str, int],
        ) -> str:
            raise NotImplementedError

        # Empty collections should dispatch to some overload without error
        empty_list: list[int] = []
        empty_set: set[str] = set()
        empty_dict: dict[str, int] = {}

        # These should all work without raising TypeError
        result1 = handle_collections(empty_list)
        result2 = handle_collections(empty_set)
        result3 = handle_collections(empty_dict)

        # Results should be one of the expected values
        self.assertIn(result1, ["int_list", "str_set", "str_int_dict"])
        self.assertIn(result2, ["int_list", "str_set", "str_int_dict"])
        self.assertIn(result3, ["int_list", "str_set", "str_int_dict"])

    def test_tuple_with_no_args(self) -> None:
        """Test tuple type with no type arguments."""

        @overload
        def handle_bare_tuple(items: tuple) -> str:  # type: ignore [type-arg]
            return "bare_tuple"

        @overload
        def handle_bare_tuple(items: list) -> str:  # type: ignore [type-arg]
            return "bare_list"

        @dispatch_overload
        def handle_bare_tuple(items: tuple | list) -> str:  # type: ignore [type-arg]
            raise NotImplementedError

        # Should work with any tuple
        self.assertEqual(handle_bare_tuple((1, 2, 3)), "bare_tuple")
        self.assertEqual(handle_bare_tuple(("a", "b")), "bare_tuple")
        self.assertEqual(handle_bare_tuple(()), "bare_tuple")

    def test_type_value_mix(self) -> None:
        @overload
        def handle_type_or_value(cls: type[str]) -> str:
            return "str_type"

        @overload
        def handle_type_or_value(cls: int) -> str:
            return "int_value"

        @dispatch_overload
        def handle_type_or_value(cls: type[str] | int) -> str:
            raise NotImplementedError

        class MyStr(str):
            pass

        self.assertEqual(handle_type_or_value(42), "int_value")
        self.assertEqual(handle_type_or_value(MyStr), "str_type")

    def test_invalid_type_error(self) -> None:
        @overload
        def bad_type(x: 10) -> None:  # type: ignore [valid-type]
            pass

        @overload
        def bad_type(x: int) -> None:  # type: ignore [overload-cannot-match]
            pass

        @dispatch_overload
        def bad_type(x: Any) -> None:
            pass

        # This should trigger the final TypeError
        with self.assertRaisesRegex(TypeError, "argument 2 is 10"):
            bad_type(10)


if __name__ == "__main__":
    unittest.main()
