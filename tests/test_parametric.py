# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

from __future__ import annotations
from typing import ClassVar, Generic
from typing_extensions import TypeVar, TypeVarTuple, Unpack

import sys
import unittest
import weakref
from unittest.mock import patch

from gel._internal._typing_parametric import (
    ParametricType,
    _PARAMETRIC_TYPES_CACHE,
)

# Global type variables for use in tests
T = TypeVar("T")
U = TypeVar("U")
V = TypeVar("V")
W = TypeVar("W", default=str)
Ts = TypeVarTuple("Ts")


class BaseTestCase(unittest.TestCase):
    def setUp(self) -> None:
        """Ensures the global cache is cleared before each test."""
        _PARAMETRIC_TYPES_CACHE.set(weakref.WeakValueDictionary())


class TestBasicUsage(BaseTestCase):
    """Tests the fundamental functionality of creating and specializing
    parametric types."""

    def test_single_parametric_type_specialization(self) -> None:
        """Tests specializing SingleParametricType and instantiating its
        concrete subclass."""

        class MyType(ParametricType, Generic[T]):
            type: ClassVar[type[T]]  # type: ignore [misc]

        Specialized = MyType[int]
        self.assertIs(Specialized.type, int)  # type: ignore [misc]
        self.assertIs(Specialized.__parametric_origin__, MyType)
        self.assertEqual(Specialized.__name__, "MyType[int]")

        class Concrete(Specialized):
            pass

        instance = Concrete()
        self.assertIsInstance(instance, MyType)
        self.assertIsInstance(instance, Specialized)
        self.assertIsInstance(instance, Concrete)

    def test_multi_param_type_specialization(self) -> None:
        """Tests a custom ParametricType with multiple parameters."""

        class Mapping(ParametricType, Generic[T, U]):
            key_type: ClassVar[type[T]]  # type: ignore [misc]
            value_type: ClassVar[type[U]]  # type: ignore [misc]

        StrIntMap = Mapping[str, int]
        self.assertIs(StrIntMap.key_type, str)  # type: ignore [misc]
        self.assertIs(StrIntMap.value_type, int)  # type: ignore [misc]

        class ConcreteMap(StrIntMap):
            pass

        instance = ConcreteMap()
        self.assertIsInstance(instance, Mapping)

    def test_type_var_tuple_unsupported(self) -> None:
        """Tests that TypeVarTuple is not currently supported as a ClassVar
        mapping."""
        with self.assertRaisesRegex(
            TypeError, "missing ClassVar for generic parameter Ts"
        ):

            class TupleContainer(ParametricType, Generic[Unpack[Ts]]):
                types: ClassVar[tuple[type, ...]]

    def test_type_var_with_default_value(self) -> None:
        """Tests that a TypeVar with a default is correctly applied."""

        class Duo(ParametricType, Generic[T, W]):
            item1: ClassVar[type[T]]  # type: ignore [misc]
            item2: ClassVar[type[W]]  # type: ignore [misc]

        Specialized = Duo[int]
        self.assertIs(Specialized.item1, int)  # type: ignore [misc]
        self.assertIs(Specialized.item2, str)  # type: ignore [misc]

        class ConcreteDuo(Specialized):
            pass

        instance = ConcreteDuo()
        self.assertIsInstance(instance, Duo)


class TestInheritance(BaseTestCase):
    """Tests various inheritance scenarios involving parametric types."""

    def test_simple_inheritance_from_specialized_type(self) -> None:
        """A simple class inheriting from a specialized type should be
        instantiable."""

        class Base(ParametricType, Generic[T]):
            type: ClassVar[type[T]]  # type: ignore [misc]

        class Child(Base[int]):
            pass

        self.assertIs(Child.type, int)  # type: ignore [misc]
        instance = Child()
        self.assertIsInstance(instance, Base)

    def test_multi_level_parametric_inheritance(self) -> None:
        """Tests a chain of inheritance where parameters are passed
        down."""

        class Grandparent(ParametricType, Generic[T, U]):
            p1: ClassVar[type[T]]  # type: ignore [misc]
            p2: ClassVar[type[U]]  # type: ignore [misc]

        class Parent(Grandparent[T, str], Generic[T]):
            pass

        class Child(Parent[bool]):
            pass

        self.assertIs(Child.p1, bool)  # type: ignore [misc]
        self.assertIs(Child.p2, str)  # type: ignore [misc]
        instance = Child()
        self.assertIsInstance(instance, Grandparent)
        self.assertIsInstance(instance, Parent)

    def test_multiple_inheritance_from_specialized_bases(self) -> None:
        """Tests inheriting from two different specialized parametric types."""

        class Base1(ParametricType, Generic[T]):
            type: ClassVar[type[T]]  # type: ignore [misc]

        class Base2(ParametricType, Generic[U, V]):
            p1: ClassVar[type[U]]  # type: ignore [misc]
            p2: ClassVar[type[V]]  # type: ignore [misc]

        class Child(Base1[int], Base2[str, bool]):
            pass

        self.assertIs(Child.type, int)  # type: ignore [misc]
        self.assertIs(Child.p1, str)  # type: ignore [misc]
        self.assertIs(Child.p2, bool)  # type: ignore [misc]
        instance = Child()
        self.assertIsInstance(instance, Base1)
        self.assertIsInstance(instance, Base2)

    def test_diamond_inheritance_pattern(self) -> None:
        """Tests that diamond inheritance resolves correctly."""

        class Top(ParametricType, Generic[T]):
            type: ClassVar[type[T]]  # type: ignore [misc]

        class Left(Top[T], Generic[T]):
            pass

        class Right(Top[T], Generic[T]):
            pass

        class Bottom(Left[float], Right[float]):
            pass

        self.assertIs(Bottom.type, float)  # type: ignore [misc]
        instance = Bottom()
        self.assertIsInstance(instance, Top)
        self.assertIsInstance(instance, Left)
        self.assertIsInstance(instance, Right)

    def test_diamond_inheritance_with_different_parametric_bases(self) -> None:
        """Tests diamond inheritance where different branches use different
        parametric bases."""

        class BaseA(ParametricType, Generic[T]):
            type: ClassVar[type[T]]  # type: ignore [misc]

        class BaseB(ParametricType, Generic[U, V]):
            key_type: ClassVar[type[U]]  # type: ignore [misc]
            value_type: ClassVar[type[V]]  # type: ignore [misc]

        # Create concrete specializations directly rather than generic
        # intermediates
        class DiamondBottom(BaseA[int], BaseB[str, bool]):
            pass

        # Check that the diamond bottom gets properties from both bases
        self.assertIs(DiamondBottom.type, int)  # type: ignore [misc]
        self.assertIs(DiamondBottom.key_type, str)  # type: ignore [misc]
        self.assertIs(DiamondBottom.value_type, bool)  # type: ignore [misc]

        instance = DiamondBottom()
        self.assertIsInstance(instance, BaseA)
        self.assertIsInstance(instance, BaseB)

    def test_complex_diamond_with_three_parametric_bases(self) -> None:
        """Tests complex diamond inheritance involving three different
        parametric base types."""

        class Source(ParametricType, Generic[T]):
            type: ClassVar[type[T]]  # type: ignore [misc]

        class Transform(ParametricType, Generic[T, U]):
            input_type: ClassVar[type[T]]  # type: ignore [misc]
            output_type: ClassVar[type[U]]  # type: ignore [misc]

        class Sink(ParametricType, Generic[V, W]):
            data_type: ClassVar[type[V]]  # type: ignore [misc]
            storage_type: ClassVar[type[W]]  # type: ignore [misc]

        # Create concrete pipeline that inherits from all three bases directly
        class FullPipeline(
            Source[int], Transform[int, float], Sink[float, str]
        ):
            pass

        # Verify all type parameters are correctly resolved
        self.assertIs(FullPipeline.type, int)  # type: ignore [misc]  # From Source
        self.assertIs(FullPipeline.input_type, int)  # type: ignore [misc]  # From Transform
        self.assertIs(FullPipeline.output_type, float)  # type: ignore [misc] # From Transform
        self.assertIs(FullPipeline.data_type, float)  # type: ignore [misc] #  From Sink
        self.assertIs(FullPipeline.storage_type, str)  # type: ignore [misc] # From Sink

        instance = FullPipeline()
        self.assertIsInstance(instance, Source)
        self.assertIsInstance(instance, Transform)
        self.assertIsInstance(instance, Sink)

    def test_diamond_inheritance_with_type_var_defaults(self) -> None:
        """Tests diamond inheritance where different branches have incompatible
        type arguments should fail."""

        class ConfigurableBase(
            ParametricType, Generic[T, W]
        ):  # W has default=str
            primary_type: ClassVar[type[T]]  # type: ignore [misc]
            secondary_type: ClassVar[type[W]]  # type: ignore [misc]

        class LeftBranch(ConfigurableBase[T, int], Generic[T]):
            pass

        class RightBranch(ConfigurableBase[T, bool], Generic[T]):
            pass

        # This should fail because LeftBranch[str] and RightBranch[str]
        # inherit from incompatible specializations of ConfigurableBase:
        # ConfigurableBase[str, int] vs ConfigurableBase[str, bool]
        with self.assertRaisesRegex(
            TypeError,
            r"Base classes.*are mutually incompatible|"
            r"incompatible.*specializations|"
            r"conflicting type arguments",
        ):

            class DiamondMerge(LeftBranch[str], RightBranch[str]):  # pyright: ignore [reportGeneralTypeIssues]
                pass

    def test_asymmetric_diamond_inheritance(self) -> None:
        """Tests diamond inheritance where branches have different numbers of
        type parameters."""

        class SimpleBase(ParametricType, Generic[T]):
            type: ClassVar[type[T]]  # type: ignore [misc]

        class ComplexBase(ParametricType, Generic[T, U, V]):
            type1: ClassVar[type[T]]  # type: ignore [misc]
            type2: ClassVar[type[U]]  # type: ignore [misc]
            type3: ClassVar[type[V]]  # type: ignore [misc]

        class SimpleBranch(SimpleBase[T], Generic[T]):
            pass

        class ComplexBranch(ComplexBase[T, U, V], Generic[T, U, V]):
            pass

        class AsymmetricMerge(
            SimpleBranch[int], ComplexBranch[int, str, bool]
        ):
            pass

        # Verify all type parameters are correctly resolved
        self.assertIs(AsymmetricMerge.type, int)  # type: ignore [misc] # From SimpleBase
        self.assertIs(AsymmetricMerge.type1, int)  # type: ignore [misc] # From ComplexBase
        self.assertIs(AsymmetricMerge.type2, str)  # type: ignore [misc] # From ComplexBase
        self.assertIs(AsymmetricMerge.type3, bool)  # type: ignore [misc] # From ComplexBase

        instance = AsymmetricMerge()
        self.assertIsInstance(instance, SimpleBase)
        self.assertIsInstance(instance, ComplexBase)
        self.assertIsInstance(instance, SimpleBranch)
        self.assertIsInstance(instance, ComplexBranch)

    def test_mixing_with_non_parametric_base(self) -> None:
        """Tests that a parametric type can be mixed with a regular class."""

        class Regular:
            def method(self) -> str:
                return "hello"

        class Base(ParametricType, Generic[T]):
            type: ClassVar[type[T]]  # type: ignore [misc]

        class Child(Regular, Base[bytes]):
            pass

        self.assertIs(Child.type, bytes)  # type: ignore [misc]
        instance = Child()
        self.assertEqual(instance.method(), "hello")


class TestCaching(BaseTestCase):
    """Tests the caching mechanism for specialized types."""

    def test_identical_specializations_are_cached(self) -> None:
        """Verifies that creating the same specialized type twice returns
        the same object."""

        class CacheTest(ParametricType, Generic[T]):
            type: ClassVar[type[T]]  # type: ignore [misc]

        specialized1 = CacheTest[int]
        specialized2 = CacheTest[int]

        self.assertIs(specialized1, specialized2)

    def test_different_specializations_are_not_the_same_object(self) -> None:
        """Verifies that different specializations produce different types."""

        class CacheTest(ParametricType, Generic[T]):
            type: ClassVar[type[T]]  # type: ignore [misc]

        specialized1 = CacheTest[int]
        specialized2 = CacheTest[str]

        self.assertIsNot(specialized1, specialized2)


class TestForwardReferences(BaseTestCase):
    """Tests the handling of forward string references in type parameters."""

    def test_forward_ref_in_same_scope_resolves(self) -> None:
        """Tests resolving a forward reference to a class in the same
        scope."""

        class Node(ParametricType, Generic[T]):
            type: ClassVar[type[T]]  # type: ignore [misc]

        with patch.dict(sys.modules[Node.__module__].__dict__, {"Node": Node}):
            LinkedList = Node["Node[int]"]

            class ConcreteNode(LinkedList):
                pass

            instance = ConcreteNode()
            self.assertIs(instance.type, Node[int])

    def test_forward_ref_to_class_defined_later(self) -> None:
        """Tests that a forward reference can be resolved after the class is
        defined."""

        class MyList(ParametricType, Generic[T]):
            type: ClassVar[type[T]]  # type: ignore [misc]

        SpecializedList = MyList["ListItem"]

        class ListItem:
            pass

        with patch.dict(globals(), {"ListItem": ListItem}):

            class ConcreteList(SpecializedList):
                pass

            instance = ConcreteList()
            self.assertIs(instance.type, ListItem)

    def test_unresolvable_forward_ref_raises_error(self) -> None:
        """Tests that instantiating with an unresolvable forward ref fails."""

        class Container(ParametricType, Generic[T]):
            type: ClassVar[type[T]]  # type: ignore [misc]

        Unresolvable = Container["SomeNonExistentType"]  # type: ignore [name-defined]

        class ConcreteUnresolvable(Unresolvable):
            pass

        with self.assertRaisesRegex(TypeError, "unresolved type parameters"):
            ConcreteUnresolvable()


class TestErrorConditions(BaseTestCase):
    """Tests various invalid usage scenarios to ensure they fail correctly."""

    def test_instantiating_non_specialized_type_fails(self) -> None:
        """A generic ParametricType cannot be instantiated directly."""

        class MyType(ParametricType, Generic[T]):
            type: ClassVar[type[T]]  # type: ignore [misc]

        with self.assertRaisesRegex(
            TypeError, "must be parametrized to instantiate"
        ):
            MyType()

    def test_respecializing_a_specialized_type_fails(self) -> None:
        """An already specialized type cannot be specialized again."""

        class MyType(ParametricType, Generic[T]):
            type: ClassVar[type[T]]  # type: ignore [misc]

        Specialized = MyType[int]
        with self.assertRaisesRegex(TypeError, "is already parametrized"):
            Specialized[str]  # type: ignore [type-arg]

    def test_missing_generic_base_fails(self) -> None:
        """A direct subclass of ParametricType must also be Generic."""
        with self.assertRaisesRegex(TypeError, "must be declared as Generic"):

            class MyType(ParametricType):
                pass

    def test_missing_classvar_for_typevar_fails(self) -> None:
        """A ClassVar must be declared for each TypeVar."""
        with self.assertRaisesRegex(
            TypeError, "missing ClassVar for generic parameter"
        ):

            class MyType(ParametricType, Generic[T]):
                pass

    def test_wrong_number_of_type_arguments_fails(self) -> None:
        """Providing the wrong number of type arguments raises a TypeError."""

        class Duo(ParametricType, Generic[T, U]):
            p1: ClassVar[type[T]]  # type: ignore [misc]
            p2: ClassVar[type[U]]  # type: ignore [misc]

        with self.assertRaisesRegex(
            TypeError, "expects 2 type parameter.*, got 1"
        ):
            Duo[int]  # type: ignore [misc]

        with self.assertRaisesRegex(
            TypeError, "expects 2 type parameter.*, got 3"
        ):
            Duo[int, str, bool]  # type: ignore [misc]

    def test_invalid_type_argument_fails(self) -> None:
        """Arguments for specialization must be valid types, not values."""

        class MyType(ParametricType, Generic[T]):
            type: ClassVar[type[T]]  # type: ignore [misc]

        with self.assertRaisesRegex(
            TypeError, "expects types as type parameters, got 42"
        ):
            MyType[42]  # type: ignore [valid-type]

    def test_inheriting_from_unspecialized_base_fails(self) -> None:
        """Inheriting from a ParametricType requires specifying its type
        arguments."""

        class Base(ParametricType, Generic[T]):
            type: ClassVar[type[T]]  # type: ignore [misc]

        with self.assertRaisesRegex(
            TypeError, "missing one or more type arguments for base 'Base'"
        ):

            class Child(Base):  # type: ignore [type-arg]
                pass

    def test_diamond_inheritance_with_non_type_arguments_fails(self) -> None:
        """Tests that diamond inheritance fails when non-type arguments are
        provided."""

        class Multi(ParametricType, Generic[T, U, V]):
            type1: ClassVar[type[T]]  # type: ignore [misc]
            type2: ClassVar[type[U]]  # type: ignore [misc]
            type3: ClassVar[type[V]]  # type: ignore [misc]

        # This should fail because 42 is not a type
        with self.assertRaisesRegex(
            TypeError, "expects types as type parameters, got 42"
        ):

            class BadTypes(Multi[42, str, bool]):  # type: ignore [valid-type]
                pass

    def test_multiple_parametric_inheritance_validation(self) -> None:
        """Tests validation of multiple parametric type inheritance
        scenarios."""

        class BaseA(ParametricType, Generic[T]):
            type: ClassVar[type[T]]  # type: ignore [misc]

        class BaseB(ParametricType, Generic[U, V]):
            key_type: ClassVar[type[U]]  # type: ignore [misc]
            value_type: ClassVar[type[V]]  # type: ignore [misc]

        # This should work - inheriting from multiple specialized parametric
        # types
        class ValidMultipleInheritance(BaseA[int], BaseB[str, bool]):
            pass

        self.assertIs(ValidMultipleInheritance.type, int)  # type: ignore [misc]
        self.assertIs(ValidMultipleInheritance.key_type, str)  # type: ignore [misc]
        self.assertIs(ValidMultipleInheritance.value_type, bool)  # type: ignore [misc]

        instance = ValidMultipleInheritance()
        self.assertIsInstance(instance, BaseA)
        self.assertIsInstance(instance, BaseB)

    def test_diamond_inheritance_with_wrong_type_argument_count_fails(
        self,
    ) -> None:
        """Tests that diamond inheritance fails when wrong number of type
        arguments provided."""

        class Multi(ParametricType, Generic[T, U, V]):
            p1: ClassVar[type[T]]  # type: ignore [misc]
            p2: ClassVar[type[U]]  # type: ignore [misc]
            p3: ClassVar[type[V]]  # type: ignore [misc]

        # This should fail because we provide wrong number of type arguments
        with self.assertRaisesRegex(
            TypeError, "expects 3 type parameter.*got 2"
        ):

            class BadArgumentCount(Multi[int, str]):  # type: ignore [type-arg]
                pass

    def test_reduce_is_not_implemented(self) -> None:
        """Tests that pickle/copy support is explicitly not implemented."""

        class MyType(ParametricType, Generic[T]):
            type: ClassVar[type[T]]  # type: ignore [misc]

        class Concrete(MyType[int]):
            pass

        instance = Concrete()
        with self.assertRaises(NotImplementedError):
            instance.__reduce__()


if __name__ == "__main__":
    unittest.main()
