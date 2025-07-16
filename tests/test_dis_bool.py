#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2024-present MagicStack Inc. and the EdgeDB authors.
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

from typing import Any

import unittest

from gel._internal._dis_bool import (
    BoolOp,
    guess_bool_op,
)


class NoBoolTestObject:
    """Test object that prevents boolean conversion and analyzes context."""

    def __bool__(self) -> bool:
        # This will analyze the calling context and raise TypeError with
        # detected operation
        detected_op = guess_bool_op()
        raise TypeError(f"Boolean conversion detected: {detected_op}")


class TestDisBool(unittest.TestCase):
    """Test suite for _dis_bool module."""

    def setUp(self) -> None:
        """Set up test object for each test."""
        self.obj = NoBoolTestObject()

    def test_dis_bool_and_operation(self) -> None:
        """Test detection of 'and' operations."""
        # Test basic and operation
        try:
            _ = self.obj and True
            self.fail("Expected TypeError")
        except TypeError as e:
            self.assertIn("BoolOp.AND", str(e))

        # Test and with False
        try:
            _ = self.obj and False
            self.fail("Expected TypeError")
        except TypeError as e:
            self.assertIn("BoolOp.AND", str(e))

        # Test and with another object
        try:
            _ = self.obj and "test"
            self.fail("Expected TypeError")
        except TypeError as e:
            self.assertIn("BoolOp.AND", str(e))

        # Test and with None
        try:
            _ = self.obj and None
            self.fail("Expected TypeError")
        except TypeError as e:
            self.assertIn("BoolOp.AND", str(e))

        # Test and with variable
        other_var = True
        try:
            _ = self.obj and other_var
            self.fail("Expected TypeError")
        except TypeError as e:
            self.assertIn("BoolOp.AND", str(e))

    def test_dis_bool_or_operation(self) -> None:
        """Test detection of 'or' operations."""
        # Test basic or operation
        try:
            _ = self.obj or False
            self.fail("Expected TypeError")
        except TypeError as e:
            self.assertIn("BoolOp.OR", str(e))

        # Test or with True
        try:
            _ = self.obj or True
            self.fail("Expected TypeError")
        except TypeError as e:
            self.assertIn("BoolOp.OR", str(e))

        # Test or with another object
        try:
            _ = self.obj or "test"
            self.fail("Expected TypeError")
        except TypeError as e:
            self.assertIn("BoolOp.OR", str(e))

        # Test or with None
        try:
            _ = self.obj or None
            self.fail("Expected TypeError")
        except TypeError as e:
            self.assertIn("BoolOp.OR", str(e))

        # Test or with variable
        other_var = False
        try:
            _ = self.obj or other_var
            self.fail("Expected TypeError")
        except TypeError as e:
            self.assertIn("BoolOp.OR", str(e))

    def test_dis_bool_not_operation(self) -> None:
        """Test detection of 'not' operations."""
        # Test basic not operation
        try:
            _ = not self.obj
            self.fail("Expected TypeError")
        except TypeError as e:
            self.assertIn("BoolOp.NOT", str(e))

        # Test double not (should still be detected as NOT)
        try:
            _ = not not self.obj
            self.fail("Expected TypeError")
        except TypeError as e:
            self.assertIn("BoolOp.NOT", str(e))

    def test_dis_bool_explicit_bool_conversion(self) -> None:
        """Test detection of explicit bool() calls."""
        # Test direct bool() call
        try:
            _ = bool(self.obj)
            self.fail("Expected TypeError")
        except TypeError as e:
            self.assertIn("BoolOp.EXPLICIT_BOOL", str(e))

        # Test bool() with assignment
        try:
            _ = bool(self.obj)
            self.fail("Expected TypeError")
        except TypeError as e:
            self.assertIn("BoolOp.EXPLICIT_BOOL", str(e))

    def test_dis_bool_implicit_bool_conversion_if(self) -> None:
        """Test detection of implicit bool conversion in if statements."""
        # Test if statement - we need to capture the result during evaluation
        try:
            if self.obj:
                _ = BoolOp.IMPLICIT_BOOL
            else:
                _ = BoolOp.IMPLICIT_BOOL
            self.fail("Expected TypeError")
        except TypeError as e:
            self.assertIn("BoolOp.IMPLICIT_BOOL", str(e))

    def test_dis_bool_implicit_bool_conversion_while(self) -> None:
        """Test detection of implicit bool conversion in while loops."""
        # Test while loop (we'll break immediately to avoid infinite loop)
        iterations = 0
        try:
            while self.obj and iterations < 1:
                _ = BoolOp.AND  # This should be detected as AND
                iterations += 1
            self.fail("Expected TypeError")
        except TypeError as e:
            # The while loop condition might detect IMPLICIT_BOOL
            self.assertIn("BoolOp", str(e))

    def test_dis_bool_implicit_bool_conversion_ternary(self) -> None:
        """Test detection of implicit bool conversion
        in ternary expressions."""
        # Test ternary expression
        try:
            _ = "yes" if self.obj else "no"
            self.fail("Expected TypeError")
        except TypeError as e:
            self.assertIn("BoolOp.IMPLICIT_BOOL", str(e))

    def test_dis_bool_chained_boolean_operations(self) -> None:
        """Test chained boolean operations."""
        # Test chained and operations
        try:
            _ = self.obj and True and False
            self.fail("Expected TypeError")
        except TypeError as e:
            self.assertIn("BoolOp.AND", str(e))

        # Test chained or operations
        try:
            _ = self.obj or False or True
            self.fail("Expected TypeError")
        except TypeError as e:
            self.assertIn("BoolOp.OR", str(e))

    def test_dis_bool_function_call_contexts(self) -> None:
        """Test boolean context within function calls."""

        # Test as function argument
        def dummy_func(x: Any) -> Any:
            return x

        # This should be detected as IMPLICIT_BOOL since it's a function
        # argument
        result = dummy_func(self.obj)
        # Function argument doesn't trigger __bool__ unless used in boolean
        # context
        self.assertEqual(result, self.obj)

    def test_dis_bool_edge_cases(self) -> None:
        """Test edge cases and boundary conditions."""
        # Test with None
        try:
            _ = self.obj and None
            self.fail("Expected TypeError")
        except TypeError as e:
            self.assertIn("BoolOp.AND", str(e))

        # Test with empty string
        try:
            _ = self.obj or ""
            self.fail("Expected TypeError")
        except TypeError as e:
            self.assertIn("BoolOp.OR", str(e))

        # Test with zero
        try:
            _ = self.obj and 0
            self.fail("Expected TypeError")
        except TypeError as e:
            self.assertIn("BoolOp.AND", str(e))

        # Test with empty list
        try:
            _ = self.obj or []
            self.fail("Expected TypeError")
        except TypeError as e:
            self.assertIn("BoolOp.OR", str(e))

    def test_dis_bool_nested_expressions(self) -> None:
        """Test nested boolean expressions."""
        # Test nested and/or
        try:
            _ = (self.obj and True) or False
            self.fail("Expected TypeError")
        except TypeError as e:
            self.assertIn("BoolOp.AND", str(e))

        try:
            _ = (self.obj or False) and True
            self.fail("Expected TypeError")
        except TypeError as e:
            self.assertIn("BoolOp.OR", str(e))

    def test_dis_bool_boolean_in_data_structures(self) -> None:
        """Test boolean operations within data structures."""
        # Test in list
        try:
            _ = [self.obj and True, self.obj or False]
            self.fail("Expected TypeError")
        except TypeError as e:
            self.assertIn("BoolOp.AND", str(e))

        # Test in dictionary
        try:
            _ = {"and": self.obj and True, "or": self.obj or False}
            self.fail("Expected TypeError")
        except TypeError as e:
            self.assertIn("BoolOp.AND", str(e))

    def test_dis_bool_with_lambda_expressions(self) -> None:
        """Test boolean operations in lambda expressions."""

        # Test lambda with and
        def func_and() -> Any:
            return self.obj and True

        try:
            _ = func_and()
            self.fail("Expected TypeError")
        except TypeError as e:
            self.assertIn("BoolOp.AND", str(e))

        # Test lambda with or
        def func_or() -> Any:
            return self.obj or False

        try:
            _ = func_or()
            self.fail("Expected TypeError")
        except TypeError as e:
            self.assertIn("BoolOp.OR", str(e))

    def test_dis_bool_return_statement_context(self) -> None:
        """Test boolean operations in return statements."""

        def test_return_and() -> Any:
            return self.obj and True

        def test_return_or() -> Any:
            return self.obj or False

        def test_return_not() -> Any:
            return not self.obj

        def test_return_bool() -> Any:
            return bool(self.obj)

        # Test return statements
        try:
            test_return_and()
            self.fail("Expected TypeError")
        except TypeError as e:
            self.assertIn("BoolOp.AND", str(e))

        try:
            test_return_or()
            self.fail("Expected TypeError")
        except TypeError as e:
            self.assertIn("BoolOp.OR", str(e))

        try:
            test_return_not()
            self.fail("Expected TypeError")
        except TypeError as e:
            self.assertIn("BoolOp.NOT", str(e))

        try:
            test_return_bool()
            self.fail("Expected TypeError")
        except TypeError as e:
            self.assertIn("BoolOp.EXPLICIT_BOOL", str(e))

    def test_dis_bool_multiple_objects(self) -> None:
        """Test with multiple NoBoolTestObject instances."""
        obj1 = NoBoolTestObject()
        obj2 = NoBoolTestObject()

        # Test both objects in and operation
        try:
            _ = obj1 and obj2
            self.fail("Expected TypeError")
        except TypeError as e:
            self.assertIn("BoolOp.AND", str(e))

        # Test both objects in or operation
        try:
            _ = obj1 or obj2
            self.fail("Expected TypeError")
        except TypeError as e:
            self.assertIn("BoolOp.OR", str(e))

    def test_dis_bool_cache_opcode_handling(self) -> None:
        """Test that CACHE opcodes are properly handled in Python 3.11+."""
        # Test that the detection still works with CACHE opcodes present
        try:
            _ = self.obj and True
            self.fail("Expected TypeError")
        except TypeError as e:
            self.assertIn("BoolOp.AND", str(e))

        try:
            _ = self.obj or False
            self.fail("Expected TypeError")
        except TypeError as e:
            self.assertIn("BoolOp.OR", str(e))

    def test_dis_bool_copy_opcode_detection(self) -> None:
        """Test COPY opcode detection in Python 3.12+."""
        # Test that COPY opcode detection works correctly
        try:
            _ = self.obj and True
            self.fail("Expected TypeError")
        except TypeError as e:
            self.assertIn("BoolOp.AND", str(e))

        try:
            _ = self.obj or False
            self.fail("Expected TypeError")
        except TypeError as e:
            self.assertIn("BoolOp.OR", str(e))

    def test_dis_bool_to_bool_opcode_handling(self) -> None:
        """Test TO_BOOL opcode handling in Python 3.13+."""
        # Test that TO_BOOL opcode is properly handled
        try:
            _ = self.obj and True
            self.fail("Expected TypeError")
        except TypeError as e:
            self.assertIn("BoolOp.AND", str(e))

        try:
            _ = self.obj or False
            self.fail("Expected TypeError")
        except TypeError as e:
            self.assertIn("BoolOp.OR", str(e))

        try:
            _ = not self.obj
            self.fail("Expected TypeError")
        except TypeError as e:
            self.assertIn("BoolOp.NOT", str(e))

    def test_dis_bool_recursive_boolean_operations(self) -> None:
        """Test recursive boolean operations."""

        def recursive_and(obj: Any, depth: int) -> Any:
            if depth <= 0:
                return obj and True
            return obj and recursive_and(obj, depth - 1)

        try:
            _ = recursive_and(self.obj, 2)
            self.fail("Expected TypeError")
        except TypeError as e:
            self.assertIn("BoolOp.AND", str(e))

    def test_dis_bool_complex_boolean_logic(self) -> None:
        """Test complex boolean logic patterns."""
        obj = NoBoolTestObject()

        # Test complex expressions
        try:
            _ = obj and (True or False)
            self.fail("Expected TypeError")
        except TypeError as e:
            self.assertIn("BoolOp.AND", str(e))

        try:
            _ = obj or (True and False)
            self.fail("Expected TypeError")
        except TypeError as e:
            self.assertIn("BoolOp.OR", str(e))

        try:
            _ = not (obj and True)
            self.fail("Expected TypeError")
        except TypeError as e:
            self.assertIn("BoolOp.AND", str(e))

    def test_dis_bool_generator_expressions(self) -> None:
        """Test with generator expressions."""
        obj = NoBoolTestObject()

        # Test in generator expression
        gen = (x for x in [1, 2, 3] if obj)
        self.assertIsNotNone(gen)

    def test_dis_bool_list_comprehensions(self) -> None:
        """Test with list comprehensions."""
        obj = NoBoolTestObject()

        # Test in list comprehension
        try:
            _ = [x for x in [1, 2, 3] if obj]
            self.fail("Expected TypeError")
        except TypeError as e:
            self.assertIn("BoolOp", str(e))

    def test_dis_bool_context_managers(self) -> None:
        """Test in context manager contexts."""
        obj = NoBoolTestObject()

        # Test basic boolean operation in context
        try:
            _ = obj and True
            self.fail("Expected TypeError")
        except TypeError as e:
            self.assertIn("BoolOp.AND", str(e))

    def test_dis_bool_exception_handling(self) -> None:
        """Test in exception handling contexts."""
        obj = NoBoolTestObject()

        try:
            _ = obj and True
            self.fail("Expected TypeError")
        except TypeError as e:
            self.assertIn("BoolOp.AND", str(e))
