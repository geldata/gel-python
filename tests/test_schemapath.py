#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2019-present MagicStack Inc. and the EdgeDB authors.
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

import unittest
import pathlib
from typing import Any

from gel._internal._schemapath import ParametricTypeName, SchemaPath


class TestSchemaPath(unittest.TestCase):
    """Test suite for SchemaPath class."""

    def test_schemapath_basic_construction(self):
        """Test basic construction from string."""
        path = SchemaPath("std::int64")
        self.assertEqual(str(path), "std::int64")
        self.assertEqual(path.parts, ("std", "int64"))
        self.assertEqual(path.name, "int64")

    def test_schemapath_single_segment(self):
        """Test construction with single segment."""
        path = SchemaPath("MyType")
        self.assertEqual(str(path), "MyType")
        self.assertEqual(path.parts, ("MyType",))
        self.assertEqual(path.name, "MyType")

    def test_schemapath_multiple_segments(self):
        """Test construction with multiple segments."""
        path = SchemaPath("std::cal::local_time")
        self.assertEqual(str(path), "std::cal::local_time")
        self.assertEqual(path.parts, ("std", "cal", "local_time"))
        self.assertEqual(path.name, "local_time")

    def test_schemapath_empty_string(self):
        """Test construction with empty string."""
        path = SchemaPath("")
        self.assertEqual(str(path), "")
        self.assertEqual(path.parts, ())

    def test_schemapath_from_segments(self):
        """Test from_segments class method."""
        path = SchemaPath.from_segments("std", "int64")
        self.assertEqual(str(path), "std::int64")
        self.assertEqual(path.parts, ("std", "int64"))
        self.assertEqual(path.name, "int64")

    def test_schemapath_from_segments_with_separator(self):
        """Test from_segments with segments containing separator."""
        # This is the key use case: collection types with :: in their name
        path = SchemaPath.from_segments("array<std::int64>")
        self.assertEqual(str(path), "array<std::int64>")
        self.assertEqual(path.parts, ("array<std::int64>",))
        self.assertEqual(path.name, "array<std::int64>")

    def test_schemapath_from_segments_multiple_with_separator(self):
        """Test from_segments with multiple segments containing separator."""
        path = SchemaPath.from_segments("std", "array<std::int64>")
        self.assertEqual(str(path), "std::array<std::int64>")
        self.assertEqual(path.parts, ("std", "array<std::int64>"))
        self.assertEqual(path.name, "array<std::int64>")

    def test_schemapath_multiple_arguments(self):
        """Test construction with multiple string arguments."""
        path = SchemaPath("std", "cal", "local_time")
        self.assertEqual(str(path), "std::cal::local_time")
        self.assertEqual(path.parts, ("std", "cal", "local_time"))

    def test_schemapath_mixed_arguments(self):
        """Test construction with mixed string and SchemaPath arguments."""
        path1 = SchemaPath("std::cal")
        path2 = SchemaPath(path1, "local_time")
        self.assertEqual(str(path2), "std::cal::local_time")
        self.assertEqual(path2.parts, ("std", "cal", "local_time"))

    def test_schemapath_schemapath_argument(self):
        """Test construction from another SchemaPath."""
        path1 = SchemaPath("std::int64")
        path2 = SchemaPath(path1)
        self.assertEqual(str(path2), "std::int64")
        self.assertEqual(path2.parts, ("std", "int64"))
        self.assertIsNot(path1, path2)

    def test_schemapath_invalid_argument_type(self):
        """Test construction with invalid argument type."""
        with self.assertRaises(TypeError):
            SchemaPath(123)  # type: ignore

    def test_schemapath_truediv_operator(self):
        """Test / operator with SchemaPath."""
        path1 = SchemaPath("std")
        path2 = SchemaPath("int64")
        result = path1 / path2
        self.assertEqual(str(result), "std::int64")
        self.assertEqual(result.parts, ("std", "int64"))

    def test_schemapath_truediv_operator_string(self):
        """Test / operator with string."""
        path = SchemaPath("std")
        result = path / "int64"
        self.assertEqual(str(result), "std::int64")
        self.assertEqual(result.parts, ("std", "int64"))

    def test_schemapath_rtruediv_operator(self):
        """Test reverse / operator."""
        path = SchemaPath("int64")
        result = "std" / path
        self.assertEqual(str(result), "std::int64")
        self.assertEqual(result.parts, ("std", "int64"))

    def test_schemapath_truediv_invalid_type(self):
        """Test / operator with invalid type."""
        path = SchemaPath("std")
        with self.assertRaises(TypeError):
            path / 123  # type: ignore

    def test_schemapath_equality(self):
        """Test equality comparison."""
        path1 = SchemaPath("std::int64")
        path2 = SchemaPath("std::int64")
        path3 = SchemaPath("std::str")

        self.assertEqual(path1, path2)
        self.assertNotEqual(path1, path3)
        self.assertNotEqual(path1, "std::int64")  # Different type

    def test_schemapath_hash(self):
        """Test hash function."""
        path1 = SchemaPath("std::int64")
        path2 = SchemaPath("std::int64")
        path3 = SchemaPath("std::str")

        self.assertEqual(hash(path1), hash(path2))
        self.assertNotEqual(hash(path1), hash(path3))

        # Test that paths can be used as dictionary keys
        d = {path1: "value1", path3: "value3"}
        self.assertEqual(d[path2], "value1")

    def test_schemapath_ordering(self):
        """Test ordering operations."""
        path1 = SchemaPath("a::b")
        path2 = SchemaPath("a::c")
        path3 = SchemaPath("b::a")

        self.assertLess(path1, path2)
        self.assertLessEqual(path1, path2)
        self.assertLessEqual(path1, path1)
        self.assertGreater(path3, path1)
        self.assertGreaterEqual(path3, path1)
        self.assertGreaterEqual(path1, path1)

    def test_schemapath_ordering_invalid_type(self):
        """Test ordering with invalid types."""
        path = SchemaPath("std::int64")

        self.assertEqual(path.__lt__("invalid"), NotImplemented)
        self.assertEqual(path.__le__("invalid"), NotImplemented)
        self.assertEqual(path.__gt__("invalid"), NotImplemented)
        self.assertEqual(path.__ge__("invalid"), NotImplemented)

    def test_schemapath_str_and_repr(self):
        """Test string representation."""
        path = SchemaPath("std::int64")
        self.assertEqual(str(path), "std::int64")
        self.assertEqual(repr(path), "SchemaPath('std::int64')")

    def test_schemapath_parent(self):
        """Test parent property."""
        path = SchemaPath("std::cal::local_time")
        parent = path.parent
        self.assertEqual(str(parent), "std::cal")
        self.assertEqual(parent.parts, ("std", "cal"))

    def test_schemapath_parent_single_segment(self):
        """Test parent of single segment path."""
        path = SchemaPath("MyType")
        parent = path.parent
        self.assertEqual(str(parent), "")
        self.assertEqual(parent.parts, ())

    def test_schemapath_parent_empty(self):
        """Test parent of empty path."""
        path = SchemaPath("MyType")
        parent = path.parent
        self.assertEqual(str(parent), "")
        self.assertEqual(parent.parts, ())

    def test_schemapath_parents(self):
        """Test parents property."""
        path = SchemaPath("std::cal::local_time")
        parents = path.parents

        self.assertEqual(len(parents), 3)
        self.assertEqual(str(parents[0]), "std::cal")
        self.assertEqual(str(parents[1]), "std")
        self.assertEqual(str(parents[2]), "")

    def test_schemapath_parents_indexing(self):
        """Test parents indexing including negative indices."""
        path = SchemaPath("a::b::c::d")
        parents = path.parents

        self.assertEqual(len(parents), 4)
        self.assertEqual(str(parents[0]), "a::b::c")
        self.assertEqual(str(parents[1]), "a::b")
        self.assertEqual(str(parents[2]), "a")
        self.assertEqual(str(parents[3]), "")

        # Test negative indexing
        self.assertEqual(str(parents[-1]), "")
        self.assertEqual(str(parents[-2]), "a")
        self.assertEqual(str(parents[-3]), "a::b")
        self.assertEqual(str(parents[-4]), "a::b::c")

    def test_schemapath_parents_slicing(self):
        """Test parents slicing."""
        path = SchemaPath("a::b::c::d")
        parents = path.parents

        slice_result = parents[1:3]
        self.assertEqual(len(slice_result), 2)
        self.assertEqual(str(slice_result[0]), "a::b")
        self.assertEqual(str(slice_result[1]), "a")

    def test_schemapath_parents_index_error(self):
        """Test parents index out of bounds."""
        path = SchemaPath("a::b")
        parents = path.parents

        with self.assertRaises(IndexError):
            parents[5]

        with self.assertRaises(IndexError):
            parents[-5]

    def test_schemapath_parents_repr(self):
        """Test parents repr."""
        path = SchemaPath("std::int64")
        parents = path.parents
        self.assertEqual(repr(parents), "<SchemaPath.parents>")

    def test_schemapath_common_parts(self):
        """Test common_parts method."""
        path1 = SchemaPath("std::cal::local_time")
        path2 = SchemaPath("std::cal::local_date")
        path3 = SchemaPath("std::str")
        path4 = SchemaPath("user::MyType")

        self.assertEqual(path1.common_parts(path2), ["std", "cal"])
        self.assertEqual(path1.common_parts(path3), ["std"])
        self.assertEqual(path1.common_parts(path4), [])

    def test_schemapath_is_relative_to(self):
        """Test is_relative_to method."""
        path = SchemaPath("std::cal::local_time")

        self.assertTrue(path.is_relative_to("std"))
        self.assertTrue(path.is_relative_to("std::cal"))
        self.assertTrue(path.is_relative_to("std::cal::local_time"))
        self.assertFalse(path.is_relative_to("std::cal::local_date"))
        self.assertFalse(path.is_relative_to("user"))

    def test_schemapath_is_relative_to_schemapath(self):
        """Test is_relative_to with SchemaPath argument."""
        path = SchemaPath("std::cal::local_time")
        base = SchemaPath("std::cal")

        self.assertTrue(path.is_relative_to(base))

    def test_schemapath_has_prefix(self):
        """Test has_prefix method."""
        path = SchemaPath("std::cal::local_time")

        self.assertTrue(path.has_prefix(SchemaPath("std")))
        self.assertTrue(path.has_prefix(SchemaPath("std::cal")))
        self.assertTrue(path.has_prefix(SchemaPath("std::cal::local_time")))
        self.assertFalse(path.has_prefix(SchemaPath("std::cal::local_date")))
        self.assertFalse(path.has_prefix(SchemaPath("user")))

    def test_schemapath_as_quoted_schema_name(self):
        """Test as_quoted_schema_name method."""
        path = SchemaPath("std::int64")
        # This test assumes quote_ident just quotes identifiers normally
        # The actual behavior depends on the _edgeql.quote_ident function
        quoted = path.as_quoted_schema_name()
        self.assertIn("::", quoted)
        self.assertTrue(quoted.startswith("std") or quoted.startswith("`std`"))

    def test_schemapath_as_code(self):
        """Test as_code method."""
        type_name = SchemaPath("std::int64")
        code = type_name.as_python_code()
        self.assertEqual(code, "SchemaPath.from_segments('std', 'int64')")

        type_name = ParametricTypeName(
            SchemaPath("std", "array"), [SchemaPath("std::int64")]
        )
        code = type_name.as_python_code()
        self.assertEqual(
            code,
            "ParametricTypeName("
            "SchemaPath.from_segments('std', 'array'), "
            "[SchemaPath.from_segments('std', 'int64')]"
            ")",
        )

        type_name = ParametricTypeName(
            SchemaPath("std", "tuple"),
            {'a': SchemaPath("std::int64"), 'b': SchemaPath("std::str")},
        )
        code = type_name.as_python_code()
        self.assertEqual(
            code,
            "ParametricTypeName("
            "SchemaPath.from_segments('std', 'tuple'), {"
            "'a': SchemaPath.from_segments('std', 'int64'), "
            "'b': SchemaPath.from_segments('std', 'str')"
            "}"
            ")",
        )

    def test_schemapath_as_code_custom_classname(self):
        """Test as_code method with custom class name."""
        type_name = SchemaPath("std::int64")
        code = type_name.as_python_code("MySchemaPath", "MyParametricTypeName")
        self.assertEqual(code, "MySchemaPath.from_segments('std', 'int64')")

        type_name = ParametricTypeName(
            SchemaPath("std", "array"), [SchemaPath("std::int64")]
        )
        code = type_name.as_python_code("MySchemaPath", "MyParametricTypeName")
        self.assertEqual(
            code,
            "MyParametricTypeName("
            "MySchemaPath.from_segments('std', 'array'), "
            "[MySchemaPath.from_segments('std', 'int64')]"
            ")",
        )

        type_name = ParametricTypeName(
            SchemaPath("std", "tuple"),
            {'a': SchemaPath("std::int64"), 'b': SchemaPath("std::str")},
        )
        code = type_name.as_python_code("MySchemaPath", "MyParametricTypeName")
        self.assertEqual(
            code,
            "MyParametricTypeName("
            "MySchemaPath.from_segments('std', 'tuple'), {"
            "'a': MySchemaPath.from_segments('std', 'int64'), "
            "'b': MySchemaPath.from_segments('std', 'str')"
            "}"
            ")",
        )

    def test_schemapath_as_pathlib_path(self):
        """Test as_pathlib_path method."""
        path = SchemaPath("std::cal::local_time")
        pathlib_path = path.as_pathlib_path()
        expected = pathlib.Path("std", "cal", "local_time")
        self.assertIsInstance(pathlib_path, pathlib.Path)
        self.assertEqual(pathlib_path, expected)

    def test_schemapath_caching(self):
        """Test that properties are properly cached."""
        path = SchemaPath("std::cal::local_time")

        # Access parts multiple times to ensure caching works
        parts1 = path.parts
        parts2 = path.parts
        self.assertIs(parts1, parts2)

        # Access string representation multiple times
        str1 = path._str
        str2 = path._str
        self.assertIs(str1, str2)

    def test_schemapath_edge_cases_leading_separator(self):
        """Test that leading separators are normalized (stripped)."""
        path = SchemaPath("::std::int64")
        self.assertEqual(str(path), "std::int64")
        self.assertEqual(path.parts, ("std", "int64"))

    def test_schemapath_edge_cases_trailing_separator(self):
        """Test that trailing separators are normalized (stripped)."""
        path = SchemaPath("std::int64::")
        self.assertEqual(str(path), "std::int64")
        self.assertEqual(path.parts, ("std", "int64"))

    def test_schemapath_edge_cases_consecutive_separators(self):
        """Test that consecutive separators are collapsed."""
        path = SchemaPath("std::::int64")
        self.assertEqual(str(path), "std::int64")
        self.assertEqual(path.parts, ("std", "int64"))

    def test_schemapath_normalization_both_separators(self):
        """Test normalization of both leading and trailing separators."""
        path = SchemaPath("::std::int64::")
        self.assertEqual(str(path), "std::int64")
        self.assertEqual(path.parts, ("std", "int64"))

        # Test that normalized paths are equal to non-normalized ones
        normal_path = SchemaPath("std::int64")
        self.assertEqual(path, normal_path)
        self.assertEqual(hash(path), hash(normal_path))

    def test_schemapath_normalization_multiple_separators(self):
        """Test normalization with multiple leading/trailing separators."""
        path = SchemaPath("::::std::int64::::")
        self.assertEqual(str(path), "std::int64")
        self.assertEqual(path.parts, ("std", "int64"))

    def test_schemapath_normalization_empty_path(self):
        """Test normalization of path with only separators."""
        path = SchemaPath("::")
        self.assertEqual(str(path), "")
        self.assertEqual(path.parts, ())

    def test_schemapath_consecutive_separator_collapsing(self):
        """Test that consecutive separators are collapsed to
        single separators."""
        # Test various forms of consecutive separators
        test_cases = [
            ("std::::int64", "std::int64"),
            ("::std::::int64::", "std::int64"),
            ("std::::::cal::::local_time", "std::cal::local_time"),
            ("a::::b::::c", "a::b::c"),
            (":::::::", ""),
        ]

        for input_path, expected in test_cases:
            with self.subTest(input_path=input_path):
                path = SchemaPath(input_path)
                self.assertEqual(str(path), expected)

    def test_schemapath_mixed_normalization(self):
        """Test complex cases with mixed leading/trailing/consecutive
        separators."""
        path = SchemaPath("::std::::cal::::local_time::")
        self.assertEqual(str(path), "std::cal::local_time")
        self.assertEqual(path.parts, ("std", "cal", "local_time"))

        # Test equality with normalized path
        normal_path = SchemaPath("std::cal::local_time")
        self.assertEqual(path, normal_path)
        self.assertEqual(hash(path), hash(normal_path))

    def test_schemapath_complex_collection_type(self):
        """Test complex collection type using from_segments."""
        # Test a complex nested collection type
        path = SchemaPath.from_segments(
            "std", "array<tuple<std::str, std::int64>>"
        )
        self.assertEqual(str(path), "std::array<tuple<std::str, std::int64>>")
        self.assertEqual(
            path.parts, ("std", "array<tuple<std::str, std::int64>>")
        )
        self.assertEqual(path.name, "array<tuple<std::str, std::int64>>")

    def test_schemapath_comparison_with_different_lengths(self):
        """Test comparison between paths of different lengths."""
        path1 = SchemaPath("std")
        path2 = SchemaPath("std::int64")

        self.assertLess(path1, path2)
        self.assertGreater(path2, path1)

    def test_schemapath_type_alias_compatibility(self):
        """Test that SchemaPath works with SchemaPathLike type alias."""

        def process_path(path: Any) -> str:
            if isinstance(path, str):
                return SchemaPath(path).name
            elif isinstance(path, SchemaPath):
                return path.name
            else:
                raise TypeError("Expected SchemaPathLike")

        # Test with string
        self.assertEqual(process_path("std::int64"), "int64")

        # Test with SchemaPath
        path = SchemaPath("std::int64")
        self.assertEqual(process_path(path), "int64")

    def test_schemapath_immutability(self):
        """Test that SchemaPath behaves as immutable."""
        path = SchemaPath("std::int64")

        # Accessing parts shouldn't modify the object
        parts = path.parts
        self.assertEqual(parts, ("std", "int64"))

        # Creating new paths from operations shouldn't modify original
        new_path = path / "extended"
        self.assertEqual(str(path), "std::int64")
        self.assertEqual(str(new_path), "std::int64::extended")

    def test_schemapath_from_parsed_parts_optimization(self):
        """Test that _from_parsed_parts properly optimizes construction."""
        # This tests the internal optimization where parts are pre-computed
        path = SchemaPath._from_parsed_parts(("std", "int64"))
        self.assertEqual(str(path), "std::int64")
        self.assertEqual(path.parts, ("std", "int64"))
        self.assertEqual(path.name, "int64")

    def test_schemapath_sorting(self):
        """Test that SchemaPath objects can be sorted."""
        paths = [
            SchemaPath("std::str"),
            SchemaPath("std::int64"),
            SchemaPath("std::cal::local_time"),
            SchemaPath("user::MyType"),
        ]

        sorted_paths = sorted(paths)
        expected_order = [
            "std::cal::local_time",
            "std::int64",
            "std::str",
            "user::MyType",
        ]

        self.assertEqual([str(p) for p in sorted_paths], expected_order)

    def test_schemapath_validation_empty_segments(self):
        """Test that empty segments in from_segments are preserved."""
        path = SchemaPath.from_segments("std", "", "int64")
        self.assertEqual(path.parts, ("std", "", "int64"))
        self.assertEqual(str(path), "std::::int64")

    def test_schemapath_validation_only_separator(self):
        """Test that path with only separators becomes empty."""
        path = SchemaPath("::")
        self.assertEqual(str(path), "")
        self.assertEqual(path.parts, ())

    def test_schemapath_permissive_behavior(self):
        """Test that various edge cases are handled permissively."""
        test_cases = [
            (":::", "", ()),
            ("std:::int64", "std:::int64", ("std", ":int64")),
            ("::std", "std", ("std",)),
            ("std::", "std", ("std",)),
            ("std:::", "std", ("std",)),
        ]

        for input_path, expected_str, expected_parts in test_cases:
            with self.subTest(input_path=input_path):
                path = SchemaPath(input_path)
                self.assertEqual(str(path), expected_str)
                self.assertEqual(path.parts, expected_parts)

    def test_schemapath_comprehensive_permissive_behavior(self):
        """Test comprehensive documentation of all permissive behaviors."""

        # Test 1: Empty paths are allowed
        empty_cases = [
            ("", "", ()),
            ("::", "", ()),
            ("::::", "", ()),
            ("::::::", "", ()),
        ]

        for input_path, expected_str, expected_parts in empty_cases:
            with self.subTest(category="empty", input_path=input_path):
                path = SchemaPath(input_path)
                self.assertEqual(str(path), expected_str)
                self.assertEqual(path.parts, expected_parts)

        # Test 2: Leading/trailing separators are normalized
        normalization_cases = [
            ("::std::int64", "std::int64", ("std", "int64")),
            ("std::int64::", "std::int64", ("std", "int64")),
            ("::std::int64::", "std::int64", ("std", "int64")),
            ("::::std::int64::::", "std::int64", ("std", "int64")),
        ]

        for input_path, expected_str, expected_parts in normalization_cases:
            with self.subTest(category="normalization", input_path=input_path):
                path = SchemaPath(input_path)
                self.assertEqual(str(path), expected_str)
                self.assertEqual(path.parts, expected_parts)

        # Test 3: Consecutive separators are collapsed
        collapse_cases = [
            ("std::::int64", "std::int64", ("std", "int64")),
            (
                "std::::::cal::::local_time",
                "std::cal::local_time",
                ("std", "cal", "local_time"),
            ),
            ("a::::b::::c::::d", "a::b::c::d", ("a", "b", "c", "d")),
        ]

        for input_path, expected_str, expected_parts in collapse_cases:
            with self.subTest(category="collapse", input_path=input_path):
                path = SchemaPath(input_path)
                self.assertEqual(str(path), expected_str)
                self.assertEqual(path.parts, expected_parts)

        # Test 4: Complex mixed cases
        mixed_cases = [
            (
                "::std::::cal::::local_time::",
                "std::cal::local_time",
                ("std", "cal", "local_time"),
            ),
            ("::::a::::b::::c::::", "a::b::c", ("a", "b", "c")),
            ("::::::std::::::int64::::::", "std::int64", ("std", "int64")),
        ]

        for input_path, expected_str, expected_parts in mixed_cases:
            with self.subTest(category="mixed", input_path=input_path):
                path = SchemaPath(input_path)
                self.assertEqual(str(path), expected_str)
                self.assertEqual(path.parts, expected_parts)

        # Test 5: Equality after normalization
        path1 = SchemaPath("std::int64")
        path2 = SchemaPath("::std::::int64::")
        self.assertEqual(path1, path2)
        self.assertEqual(hash(path1), hash(path2))


if __name__ == "__main__":
    unittest.main()
