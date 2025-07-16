# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

from __future__ import annotations
from typing import Any, Literal

import dataclasses
import enum
import pytest

from gel._internal._dataclass_extras import coerce_to_dataclass


class Color(enum.Enum):
    RED = "red"
    GREEN = "green"
    BLUE = "blue"


@dataclasses.dataclass
class SimpleDataclass:
    value: str
    number: int


@dataclasses.dataclass
class NestedDataclass:
    name: str
    child: SimpleDataclass


@dataclasses.dataclass
class OptionalDataclass:
    name: str
    optional_child: SimpleDataclass | None = None


@dataclasses.dataclass
class ListDataclass:
    name: str
    items: list[SimpleDataclass]


@dataclasses.dataclass
class DictDataclass:
    name: str
    mapping: dict[str, SimpleDataclass]


@dataclasses.dataclass
class ComplexNested:
    name: str
    dict_of_lists: dict[str, list[SimpleDataclass]]


@dataclasses.dataclass
class VeryComplexNested:
    name: str
    list_of_dicts: list[dict[str, SimpleDataclass]]


@dataclasses.dataclass
class TripleNested:
    name: str
    nested_structure: dict[str, dict[str, list[SimpleDataclass]]]


@dataclasses.dataclass
class UnionDataclass:
    name: str
    union_field: SimpleDataclass | NestedDataclass


@dataclasses.dataclass
class ListOfUnions:
    name: str
    items: list[SimpleDataclass | NestedDataclass]


@dataclasses.dataclass
class DictOfUnions:
    name: str
    mapping: dict[str, SimpleDataclass | NestedDataclass]


@dataclasses.dataclass
class OptionalInGenerics:
    name: str
    optional_list: list[SimpleDataclass | None]
    optional_dict: dict[str, SimpleDataclass | None]


@dataclasses.dataclass
class EnumDataclass:
    name: str
    color: Color


@dataclasses.dataclass
class ComplexWithOptionals:
    name: str
    optional_nested: dict[str, list[SimpleDataclass | None]] | None = None


@dataclasses.dataclass
class Animal:
    species: Literal["dog", "cat"]
    name: str


@dataclasses.dataclass
class Dog:
    species: Literal["dog"]
    name: str
    breed: str


@dataclasses.dataclass
class Cat:
    species: Literal["cat"]
    name: str
    indoor: bool


@dataclasses.dataclass
class Vehicle:
    type: Literal["car", "bike"]
    brand: str


@dataclasses.dataclass
class Car:
    type: Literal["car"]
    brand: str
    doors: int


@dataclasses.dataclass
class Bike:
    type: Literal["bike"]
    brand: str
    wheels: int


@dataclasses.dataclass
class InvalidDiscriminatedUnion:
    field1: str
    field2: int


@dataclasses.dataclass
class MultipleOptionalUnion:
    value: str | int | None


@dataclasses.dataclass
class UnsupportedGeneric:
    field: set[int]


# Helper classes for union tests (at module level to avoid scoping issues)
@dataclasses.dataclass
class RequiresValue:
    value: int


@dataclasses.dataclass
class UnionWithoutNone:
    field: RequiresValue | str


@dataclasses.dataclass
class StrictDataclass:
    value: int

    def __post_init__(self) -> None:
        if not isinstance(self.value, int):
            raise TypeError(f"Expected int, got {type(self.value)}")


@dataclasses.dataclass
class AnotherStrictDataclass:
    data: str

    def __post_init__(self) -> None:
        if not isinstance(self.data, str):
            raise TypeError(f"Expected str, got {type(self.data)}")


@dataclasses.dataclass
class UnionWithExceptions:
    field: StrictDataclass | AnotherStrictDataclass


@dataclasses.dataclass
class TaggedDog:
    species: Literal["dog"]
    name: str


@dataclasses.dataclass
class TaggedCat:
    species: Literal["cat"]
    name: str


@dataclasses.dataclass
class TaggedAnimalContainer:
    animal: TaggedDog | TaggedCat


@dataclasses.dataclass
class OptionalValue:
    value: str


@dataclasses.dataclass
class OptionalUnion:
    field: OptionalValue | None


@dataclasses.dataclass
class SimpleClass:
    value: str


@dataclasses.dataclass
class UnionWithNone:
    field: SimpleClass | None


@dataclasses.dataclass
class BasicUnion:
    field: int | str


@dataclasses.dataclass
class ExplicitNoneUnion:
    field: str | None


@dataclasses.dataclass
class EmptyComponentsUnion:
    field: int | str | float


class TestCoerceToDataclass:
    def test_simple_dataclass_from_dict(self) -> None:
        """Test basic dataclass coercion from dict"""
        data = {"value": "test", "number": 42}
        result = coerce_to_dataclass(SimpleDataclass, data)
        assert isinstance(result, SimpleDataclass)
        assert result.value == "test"
        assert result.number == 42

    def test_simple_dataclass_from_object(self) -> None:
        """Test basic dataclass coercion from object with attributes"""

        class DataSource:
            def __init__(self) -> None:
                self.value = "test"
                self.number = 42

        data = DataSource()
        result = coerce_to_dataclass(SimpleDataclass, data)
        assert isinstance(result, SimpleDataclass)
        assert result.value == "test"
        assert result.number == 42

    def test_nested_dataclass(self) -> None:
        """Test nested dataclass coercion"""
        data = {
            "name": "parent",
            "child": {"value": "child_value", "number": 123},
        }
        result = coerce_to_dataclass(NestedDataclass, data)
        assert isinstance(result, NestedDataclass)
        assert result.name == "parent"
        assert isinstance(result.child, SimpleDataclass)
        assert result.child.value == "child_value"
        assert result.child.number == 123

    def test_optional_dataclass_present(self) -> None:
        """Test optional dataclass field when present"""
        data = {
            "name": "parent",
            "optional_child": {"value": "child_value", "number": 123},
        }
        result = coerce_to_dataclass(OptionalDataclass, data)
        assert isinstance(result, OptionalDataclass)
        assert result.name == "parent"
        assert isinstance(result.optional_child, SimpleDataclass)
        assert result.optional_child.value == "child_value"

    def test_optional_dataclass_none(self) -> None:
        """Test optional dataclass field when None"""
        data = {"name": "parent", "optional_child": None}
        result = coerce_to_dataclass(OptionalDataclass, data)
        assert isinstance(result, OptionalDataclass)
        assert result.name == "parent"
        assert result.optional_child is None

    def test_optional_dataclass_missing(self) -> None:
        """Test optional dataclass field when missing"""
        data = {"name": "parent"}
        result = coerce_to_dataclass(OptionalDataclass, data)
        assert isinstance(result, OptionalDataclass)
        assert result.name == "parent"
        assert result.optional_child is None

    def test_list_of_dataclasses(self) -> None:
        """Test list of dataclasses"""
        data = {
            "name": "container",
            "items": [
                {"value": "item1", "number": 1},
                {"value": "item2", "number": 2},
            ],
        }
        result = coerce_to_dataclass(ListDataclass, data)
        assert isinstance(result, ListDataclass)
        assert result.name == "container"
        assert len(result.items) == 2
        assert all(isinstance(item, SimpleDataclass) for item in result.items)
        assert result.items[0].value == "item1"
        assert result.items[1].number == 2

    def test_dict_of_dataclasses(self) -> None:
        """Test dict of dataclasses"""
        data = {
            "name": "container",
            "mapping": {
                "key1": {"value": "item1", "number": 1},
                "key2": {"value": "item2", "number": 2},
            },
        }
        result = coerce_to_dataclass(DictDataclass, data)
        assert isinstance(result, DictDataclass)
        assert result.name == "container"
        assert len(result.mapping) == 2
        assert all(
            isinstance(v, SimpleDataclass) for v in result.mapping.values()
        )
        assert result.mapping["key1"].value == "item1"
        assert result.mapping["key2"].number == 2

    def test_dict_of_lists_of_dataclasses(self) -> None:
        """Test dict of lists of dataclasses - where the bug occurs"""
        data = {
            "name": "container",
            "dict_of_lists": {
                "group1": [
                    {"value": "item1", "number": 1},
                    {"value": "item2", "number": 2},
                ],
                "group2": [{"value": "item3", "number": 3}],
            },
        }
        result = coerce_to_dataclass(ComplexNested, data)
        assert isinstance(result, ComplexNested)
        assert result.name == "container"
        assert len(result.dict_of_lists) == 2
        assert len(result.dict_of_lists["group1"]) == 2
        assert len(result.dict_of_lists["group2"]) == 1

        # Check that all items are properly coerced
        for group_list in result.dict_of_lists.values():
            assert all(
                isinstance(item, SimpleDataclass) for item in group_list
            )

        assert result.dict_of_lists["group1"][0].value == "item1"
        assert result.dict_of_lists["group2"][0].number == 3

    def test_list_of_dicts_of_dataclasses(self) -> None:
        """Test list of dicts of dataclasses"""
        data = {
            "name": "container",
            "list_of_dicts": [
                {
                    "key1": {"value": "item1", "number": 1},
                    "key2": {"value": "item2", "number": 2},
                },
                {"key3": {"value": "item3", "number": 3}},
            ],
        }
        result = coerce_to_dataclass(VeryComplexNested, data)
        assert isinstance(result, VeryComplexNested)
        assert result.name == "container"
        assert len(result.list_of_dicts) == 2
        assert len(result.list_of_dicts[0]) == 2
        assert len(result.list_of_dicts[1]) == 1

        # Check that all items are properly coerced
        for dict_item in result.list_of_dicts:
            assert all(
                isinstance(v, SimpleDataclass) for v in dict_item.values()
            )

        assert result.list_of_dicts[0]["key1"].value == "item1"
        assert result.list_of_dicts[1]["key3"].number == 3

    def test_triple_nested_structure(self) -> None:
        """Test dict[str, dict[str, list[SimpleDataclass]]]"""
        data = {
            "name": "container",
            "nested_structure": {
                "level1": {
                    "level2a": [
                        {"value": "item1", "number": 1},
                        {"value": "item2", "number": 2},
                    ],
                    "level2b": [{"value": "item3", "number": 3}],
                },
                "level1b": {"level2c": [{"value": "item4", "number": 4}]},
            },
        }
        result = coerce_to_dataclass(TripleNested, data)
        assert isinstance(result, TripleNested)
        assert result.name == "container"
        assert len(result.nested_structure) == 2
        assert len(result.nested_structure["level1"]) == 2
        assert len(result.nested_structure["level1"]["level2a"]) == 2

        # Check that all items are properly coerced
        for level1_dict in result.nested_structure.values():
            for level2_list in level1_dict.values():
                assert all(
                    isinstance(item, SimpleDataclass) for item in level2_list
                )

        assert result.nested_structure["level1"]["level2a"][0].value == "item1"
        assert result.nested_structure["level1b"]["level2c"][0].number == 4

    def test_union_dataclass(self) -> None:
        """Test union types in dataclass fields"""
        # Test with SimpleDataclass
        data1 = {
            "name": "container",
            "union_field": {"value": "simple", "number": 42},
        }
        result1 = coerce_to_dataclass(UnionDataclass, data1)
        assert isinstance(result1, UnionDataclass)
        assert isinstance(result1.union_field, SimpleDataclass)
        assert result1.union_field.value == "simple"

        # Test with NestedDataclass
        data2 = {
            "name": "container",
            "union_field": {
                "name": "nested",
                "child": {"value": "child", "number": 123},
            },
        }
        result2 = coerce_to_dataclass(UnionDataclass, data2)
        assert isinstance(result2, UnionDataclass)
        assert isinstance(result2.union_field, NestedDataclass)
        assert result2.union_field.name == "nested"

    def test_list_of_unions(self) -> None:
        """Test list of union types"""
        data = {
            "name": "container",
            "items": [
                {"value": "simple", "number": 42},
                {"name": "nested", "child": {"value": "child", "number": 123}},
            ],
        }
        result = coerce_to_dataclass(ListOfUnions, data)
        assert isinstance(result, ListOfUnions)
        assert len(result.items) == 2
        assert isinstance(result.items[0], SimpleDataclass)
        assert isinstance(result.items[1], NestedDataclass)

    def test_dict_of_unions(self) -> None:
        """Test dict of union types"""
        data = {
            "name": "container",
            "mapping": {
                "simple": {"value": "simple", "number": 42},
                "nested": {
                    "name": "nested",
                    "child": {"value": "child", "number": 123},
                },
            },
        }
        result = coerce_to_dataclass(DictOfUnions, data)
        assert isinstance(result, DictOfUnions)
        assert len(result.mapping) == 2
        assert isinstance(result.mapping["simple"], SimpleDataclass)
        assert isinstance(result.mapping["nested"], NestedDataclass)

    def test_optional_in_generics(self) -> None:
        """Test optional types inside generic containers"""
        data = {
            "name": "container",
            "optional_list": [
                {"value": "item1", "number": 1},
                None,
                {"value": "item3", "number": 3},
            ],
            "optional_dict": {
                "key1": {"value": "item1", "number": 1},
                "key2": None,
                "key3": {"value": "item3", "number": 3},
            },
        }
        result = coerce_to_dataclass(OptionalInGenerics, data)
        assert isinstance(result, OptionalInGenerics)
        assert len(result.optional_list) == 3
        assert isinstance(result.optional_list[0], SimpleDataclass)
        assert result.optional_list[1] is None
        assert isinstance(result.optional_list[2], SimpleDataclass)

        assert len(result.optional_dict) == 3
        assert isinstance(result.optional_dict["key1"], SimpleDataclass)
        assert result.optional_dict["key2"] is None
        assert isinstance(result.optional_dict["key3"], SimpleDataclass)

    def test_enum_coercion(self) -> None:
        """Test enum coercion"""
        data = {"name": "test", "color": "red"}
        result = coerce_to_dataclass(EnumDataclass, data)
        assert isinstance(result, EnumDataclass)
        assert result.name == "test"
        assert result.color == Color.RED

    def test_complex_with_optionals(self) -> None:
        """Test complex nested structure with optional fields"""
        data = {
            "name": "container",
            "optional_nested": {
                "group1": [
                    {"value": "item1", "number": 1},
                    None,
                    {"value": "item3", "number": 3},
                ],
                "group2": [None, {"value": "item2", "number": 2}],
            },
        }
        result = coerce_to_dataclass(ComplexWithOptionals, data)
        assert isinstance(result, ComplexWithOptionals)
        assert result.name == "container"
        assert result.optional_nested is not None
        assert len(result.optional_nested) == 2

        # Check group1
        group1 = result.optional_nested["group1"]
        assert len(group1) == 3
        assert isinstance(group1[0], SimpleDataclass)
        assert group1[1] is None
        assert isinstance(group1[2], SimpleDataclass)

        # Check group2
        group2 = result.optional_nested["group2"]
        assert len(group2) == 2
        assert group2[0] is None
        assert isinstance(group2[1], SimpleDataclass)

    def test_complex_with_optionals_none(self) -> None:
        """Test complex nested structure with optional field set to None"""
        data = {"name": "container", "optional_nested": None}
        result = coerce_to_dataclass(ComplexWithOptionals, data)
        assert isinstance(result, ComplexWithOptionals)
        assert result.name == "container"
        assert result.optional_nested is None

    def test_cast_map_functionality(self) -> None:
        """Test cast_map parameter functionality"""

        @dataclasses.dataclass
        class WithCast:
            name: str
            value: int

        data = {"name": "test", "value": "42"}
        result = coerce_to_dataclass(WithCast, data, cast_map={int: (str,)})
        assert isinstance(result, WithCast)
        assert result.name == "test"
        assert result.value == 42
        assert isinstance(result.value, int)

    def test_replace_functionality(self) -> None:
        """Test replace parameter functionality"""
        data = {"value": "test", "number": 42}
        result = coerce_to_dataclass(
            SimpleDataclass, data, replace={"value": "replaced"}
        )
        assert isinstance(result, SimpleDataclass)
        assert result.value == "replaced"
        assert result.number == 42

    def test_error_non_dataclass(self) -> None:
        """Test error when target is not a dataclass"""

        class NotADataclass:
            pass

        with pytest.raises(TypeError, match="is not a dataclass"):
            coerce_to_dataclass(NotADataclass, {})

    def test_error_missing_required_field(self) -> None:
        """Test error when required field is missing"""
        data = {"value": "test"}  # missing 'number'
        with pytest.raises((KeyError, AttributeError)):
            coerce_to_dataclass(SimpleDataclass, data)

    def test_empty_containers(self) -> None:
        """Test empty containers are handled correctly"""
        data = {"name": "container", "items": []}
        result = coerce_to_dataclass(ListDataclass, data)
        assert isinstance(result, ListDataclass)
        assert result.name == "container"
        assert result.items == []

    def test_tuple_handling(self) -> None:
        """Test tuple handling in generic types"""

        @dataclasses.dataclass
        class TupleDataclass:
            name: str
            items: tuple[SimpleDataclass, ...]

        data = {
            "name": "container",
            "items": [
                {"value": "item1", "number": 1},
                {"value": "item2", "number": 2},
            ],
        }
        result = coerce_to_dataclass(TupleDataclass, data)
        assert isinstance(result, TupleDataclass)
        assert result.name == "container"
        assert isinstance(result.items, tuple)
        assert len(result.items) == 2
        assert all(isinstance(item, SimpleDataclass) for item in result.items)

    def test_set_handling(self) -> None:
        """Test set handling in generic types"""

        @dataclasses.dataclass
        class SetDataclass:
            name: str
            items: set[str]

        data = {
            "name": "container",
            "items": [
                "item1",
                "item2",
                "item1",
            ],  # duplicate should be removed
        }
        result = coerce_to_dataclass(SetDataclass, data)
        assert isinstance(result, SetDataclass)
        assert result.name == "container"
        assert isinstance(result.items, set)
        assert result.items == {"item1", "item2"}

    def test_deeply_nested_mixed_structures(self) -> None:
        """Test very deeply nested mixed structures"""

        @dataclasses.dataclass
        class DeepNested:
            name: str
            mixed: dict[str, list[dict[str, SimpleDataclass | None]]]

        data = {
            "name": "deep",
            "mixed": {
                "group1": [
                    {
                        "item1": {"value": "test1", "number": 1},
                        "item2": None,
                    },
                    {
                        "item3": {"value": "test3", "number": 3},
                    },
                ],
                "group2": [
                    {
                        "item4": None,
                        "item5": {"value": "test5", "number": 5},
                    }
                ],
            },
        }

        result = coerce_to_dataclass(DeepNested, data)
        assert isinstance(result, DeepNested)
        assert result.name == "deep"

        # Check group1
        group1 = result.mixed["group1"]
        assert len(group1) == 2
        assert isinstance(group1[0]["item1"], SimpleDataclass)
        assert group1[0]["item1"].value == "test1"
        assert group1[0]["item2"] is None
        assert isinstance(group1[1]["item3"], SimpleDataclass)

        # Check group2
        group2 = result.mixed["group2"]
        assert len(group2) == 1
        assert group2[0]["item4"] is None
        assert isinstance(group2[0]["item5"], SimpleDataclass)
        assert group2[0]["item5"].value == "test5"

    def test_recursive_generic_alias_direct_call(self) -> None:
        """Test calling coerce_to_dataclass directly with generic aliases"""
        # This should work with the refactored implementation
        list_type = list[SimpleDataclass]
        data = [
            {"value": "item1", "number": 1},
            {"value": "item2", "number": 2},
        ]

        result = coerce_to_dataclass(list_type, data)
        assert isinstance(result, list)
        assert len(result) == 2
        assert all(isinstance(item, SimpleDataclass) for item in result)
        assert result[0].value == "item1"
        assert result[1].number == 2

    def test_union_with_none_in_complex_structure(self) -> None:
        """Test union with None in complex nested structures"""

        @dataclasses.dataclass
        class ComplexUnion:
            name: str
            data: list[dict[str, SimpleDataclass | NestedDataclass | None]]

        data = {
            "name": "complex",
            "data": [
                {
                    "simple": {"value": "test", "number": 42},
                    "nested": {
                        "name": "nested_test",
                        "child": {"value": "child_test", "number": 123},
                    },
                    "none_value": None,
                }
            ],
        }

        result = coerce_to_dataclass(ComplexUnion, data)
        assert isinstance(result, ComplexUnion)
        assert result.name == "complex"
        assert len(result.data) == 1

        first_dict = result.data[0]
        assert isinstance(first_dict["simple"], SimpleDataclass)
        assert isinstance(first_dict["nested"], NestedDataclass)
        assert first_dict["none_value"] is None

    def test_empty_union_handling(self) -> None:
        """Test edge cases with union handling"""

        @dataclasses.dataclass
        class EdgeCaseUnion:
            name: str
            # Union with overlapping fields to test discrimination
            union_field: SimpleDataclass | NestedDataclass

        # Test data that could match either type
        ambiguous_data = {
            "name": "container",
            "union_field": {
                "name": "could_be_nested",
                "value": "could_be_simple",
                "number": 42,
            },
        }

        # Should succeed with one of the types
        result = coerce_to_dataclass(EdgeCaseUnion, ambiguous_data)
        assert isinstance(result, EdgeCaseUnion)
        assert result.name == "container"
        # The union should resolve to one of the types
        assert isinstance(
            result.union_field, (SimpleDataclass, NestedDataclass)
        )

    def test_discriminated_union_with_literals(self) -> None:
        """Test discriminated union using literal types - returns dict"""

        @dataclasses.dataclass
        class Dog:
            species: Literal["dog"]
            name: str
            breed: str

        @dataclasses.dataclass
        class Cat:
            species: Literal["cat"]
            name: str
            indoor: bool

        # Currently, discriminated unions through the main entry point
        # return dict as-is due to generic alias handling taking precedence
        dog_data = {
            "species": "dog",
            "name": "Buddy",
            "breed": "Golden Retriever",
        }
        result: Any = coerce_to_dataclass(Dog | Cat, dog_data)
        assert isinstance(result, dict)
        assert result == dog_data

        cat_data = {"species": "cat", "name": "Whiskers", "indoor": True}
        result = coerce_to_dataclass(Dog | Cat, cat_data)
        assert isinstance(result, dict)
        assert result == cat_data

    def test_discriminated_union_error_unknown_value(self) -> None:
        """Test discriminated union with unknown discriminator value -
        returns dict"""

        @dataclasses.dataclass
        class Dog:
            species: Literal["dog"]
            name: str

        @dataclasses.dataclass
        class Cat:
            species: Literal["cat"]
            name: str

        # Currently returns dict as-is due to generic alias handling
        bad_data = {"species": "bird", "name": "Tweety"}
        result: Any = coerce_to_dataclass(Dog | Cat, bad_data)
        assert isinstance(result, dict)
        assert result == bad_data

    def test_union_without_discriminator_fallback(self) -> None:
        """Test union without proper discriminator - returns dict"""

        @dataclasses.dataclass
        class NonDiscriminatedA:
            field1: str
            field2: int

        @dataclasses.dataclass
        class NonDiscriminatedB:
            field1: str
            field3: float

        # Currently returns dict as-is due to generic alias handling
        data = {"field1": "test", "field2": 42}
        result: Any = coerce_to_dataclass(
            NonDiscriminatedA | NonDiscriminatedB,
            data,
        )
        assert isinstance(result, dict)
        assert result == data

    def test_union_coercion_all_fail(self) -> None:
        """Test union coercion when all components fail - returns dict as-is"""

        @dataclasses.dataclass
        class RequiresInt:
            value: int

        @dataclasses.dataclass
        class RequiresStr:
            value: str

        # Currently returns dict as-is due to generic alias handling
        data = {"value": [1, 2, 3]}  # list can't be coerced to int or str
        result: Any = coerce_to_dataclass(RequiresInt | RequiresStr, data)
        assert isinstance(result, dict)
        assert result == data

    def test_union_with_none_not_in_union(self) -> None:
        """Test union coercion when None passed but None not in union"""

        # Test that None is handled appropriately when not in union
        result = coerce_to_dataclass(UnionWithoutNone, {"field": None})
        assert isinstance(result, UnionWithoutNone)
        assert result.field is None

    def test_union_coercion_failure_with_exception(self) -> None:
        """Test union coercion when all components fail with exceptions"""

        # Test that exception is raised when all components fail
        # Pass a value that will fail for both dataclasses
        with pytest.raises(TypeError):
            coerce_to_dataclass(
                UnionWithExceptions,
                {"field": {"value": "not_an_int", "data": 123}},
            )

    def test_unsupported_generic_alias(self) -> None:
        """Test generic alias that is not supported"""

        @dataclasses.dataclass
        class WithUnsupported:
            data: frozenset[str]

        # Should return the object as-is for unsupported generic types
        data = {"data": frozenset(["a", "b", "c"])}
        result = coerce_to_dataclass(WithUnsupported, data)
        assert isinstance(result, WithUnsupported)
        assert result.data == frozenset(["a", "b", "c"])

    def test_multiple_optional_args_in_union(self) -> None:
        """Test optional field with multiple non-None types"""

        @dataclasses.dataclass
        class MultipleOptional:
            value: str | int | None

        # Test with string value
        data1: dict[str, Any] = {"value": "test"}
        result = coerce_to_dataclass(MultipleOptional, data1)
        assert result.value == "test"

        # Test with int value
        data2: dict[str, Any] = {"value": 42}
        result = coerce_to_dataclass(MultipleOptional, data2)
        assert result.value == 42

        # Test with None value
        data3: dict[str, Any] = {"value": None}
        result = coerce_to_dataclass(MultipleOptional, data3)
        assert result.value is None

    def test_internal_tagged_union_functionality(self) -> None:
        """Test that tagged union logic works correctly through public API"""

        # Test direct coercion with tagged union through public API
        dog_data = {"animal": {"species": "dog", "name": "Buddy"}}
        result = coerce_to_dataclass(TaggedAnimalContainer, dog_data)
        assert isinstance(result, TaggedAnimalContainer)
        assert isinstance(result.animal, TaggedDog)
        assert result.animal.species == "dog"
        assert result.animal.name == "Buddy"

        # Test cat coercion
        cat_data = {"animal": {"species": "cat", "name": "Whiskers"}}
        result = coerce_to_dataclass(TaggedAnimalContainer, cat_data)
        assert isinstance(result, TaggedAnimalContainer)
        assert isinstance(result.animal, TaggedCat)
        assert result.animal.species == "cat"
        assert result.animal.name == "Whiskers"

        # Test discrimination error with unknown value
        with pytest.raises(LookupError):
            coerce_to_dataclass(
                TaggedAnimalContainer,
                {"animal": {"species": "bird", "name": "Tweety"}},
            )

    def test_union_with_none_in_args(self) -> None:
        """Test union coercion when None is in union args"""

        # Test None handling when None is explicitly in union
        result = coerce_to_dataclass(OptionalUnion, {"field": None})
        assert isinstance(result, OptionalUnion)
        assert result.field is None

    def test_union_skip_none_type_in_loop(self) -> None:
        """Test union coercion skips None type in component loop"""

        # Test with union that has None type - should skip
        # None and try SimpleClass
        result = coerce_to_dataclass(
            UnionWithNone, {"field": {"value": "test"}}
        )
        assert isinstance(result, UnionWithNone)
        assert isinstance(result.field, SimpleClass)
        assert result.field.value == "test"

    def test_union_no_errors_fallback(self) -> None:
        """Test union coercion when no errors occur but no coercion happens"""

        # Test with basic types that don't raise errors
        result = coerce_to_dataclass(BasicUnion, {"field": 42})
        assert isinstance(result, BasicUnion)
        assert result.field == 42

    def test_union_with_explicit_none_type(self) -> None:
        """Test union coercion when type(None) is explicitly in union"""

        # Test with explicit None type in union args
        result = coerce_to_dataclass(ExplicitNoneUnion, {"field": "test"})
        assert isinstance(result, ExplicitNoneUnion)
        assert result.field == "test"

        # Test None value with explicit None type
        result = coerce_to_dataclass(ExplicitNoneUnion, {"field": None})
        assert isinstance(result, ExplicitNoneUnion)
        assert result.field is None

    def test_union_empty_components_fallback(self) -> None:
        """Test union coercion fallback when no components are coerceable"""

        # Create a union with no dataclass components,
        # should return object as-is
        result = coerce_to_dataclass(
            EmptyComponentsUnion, {"field": "unchanged"}
        )
        assert isinstance(result, EmptyComponentsUnion)
        assert result.field == "unchanged"
