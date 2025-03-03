import unittest

from gel.ai import (
    MetadataFilter,
    CompositeFilter,
    FilterOperator,
    FilterCondition,
)


class TestAICompositeFilter(unittest.TestCase):

    # Test MetadataFilter with default EQ operator
    def test_metadata_EQ_filter(self):
        filter_obj = MetadataFilter(
            key="category", value="science", operator=FilterOperator.EQ
        )
        expected_repr = (
            'MetadataFilter(key="category", value="science", operator="=")'
        )

        self.assertEqual(repr(filter_obj), expected_repr)
        self.assertEqual(filter_obj.key, "category")
        self.assertEqual(filter_obj.value, "science")
        self.assertEqual(filter_obj.operator, FilterOperator.EQ)

    # Test MetadataFilter with NE operator
    def test_metadata_NE_filter(self):
        filter_obj = MetadataFilter(
            key="author", value="Alice", operator=FilterOperator.NE
        )
        expected_repr = (
            'MetadataFilter(key="author", value="Alice", operator="!=")'
        )

        self.assertEqual(repr(filter_obj), expected_repr)

    # Test CompositeFilter with AND condition
    def test_metadata_filters_and_condition(self):
        filters = CompositeFilter(
            [
                MetadataFilter(
                    key="category", value="AI", operator=FilterOperator.EQ
                ),
                MetadataFilter(
                    key="views", value=1000, operator=FilterOperator.GT
                ),
            ],
            condition=FilterCondition.AND,
        )
        expected_repr = (
            f'CompositeFilter(condition="and", filters=['
            f'MetadataFilter(key="category", value="AI", operator="="), '
            f'MetadataFilter(key="views", value=1000, operator=">")])'
        )

        self.assertEqual(repr(filters), expected_repr)
        self.assertEqual(len(filters.filters), 2)
        self.assertEqual(filters.condition, FilterCondition.AND)
        self.assertEqual(filters.filters[1].operator, FilterOperator.GT)
        self.assertEqual(filters.filters[1].value, 1000)
