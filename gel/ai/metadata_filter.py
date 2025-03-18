from __future__ import annotations
from dataclasses import dataclass
from typing import Union, List
from enum import Enum


class FilterOperator(str, Enum):
    EQ = "="
    NE = "!="
    GT = ">"
    LT = "<"
    GTE = ">="
    LTE = "<="
    IN = "in"
    NOT_IN = "not in"
    LIKE = "like"
    ILIKE = "ilike"
    ANY = "any"
    ALL = "all"
    CONTAINS = "contains"
    EXISTS = "exists"


class FilterCondition(str, Enum):
    AND = "and"
    OR = "or"


@dataclass
class MetadataFilter:
    """Represents a single metadata filter condition."""

    key: str
    value: Union[int, float, str]
    operator: FilterOperator = FilterOperator.EQ

    def __repr__(self):
        value = f'"{self.value}"' if isinstance(self.value, str) else self.value
        return (
            f'MetadataFilter(key="{self.key}", '
            f"value={value}, "
            f'operator="{self.operator.value}")'
        )


@dataclass
class CompositeFilter:
    """
    Allows grouping multiple MetadataFilter instances using AND/OR.
    """

    filters: List[Union[CompositeFilter, MetadataFilter]]
    condition: FilterCondition = FilterCondition.AND

    def __repr__(self):
        return (
            f'CompositeFilter(condition="{self.condition.value}", '
            f"filters={self.filters})"
        )


def get_filter_clause(filters: CompositeFilter) -> str:
    """
    Get the filter clause for a given CompositeFilter.
    """

    subclauses = []
    for filter in filters.filters:
        subclause = ""

        if isinstance(filter, CompositeFilter):
            subclause = get_filter_clause(filter)
        elif isinstance(filter, MetadataFilter):
            formatted_value = (
                f'"{filter.value}"'
                if isinstance(filter.value, str)
                else filter.value
            )

            # Simple comparison operators
            if filter.operator in {
                FilterOperator.EQ,
                FilterOperator.NE,
                FilterOperator.GT,
                FilterOperator.GTE,
                FilterOperator.LT,
                FilterOperator.LTE,
                FilterOperator.LIKE,
                FilterOperator.ILIKE,
            }:
                subclause = (
                    f'<typeof {formatted_value}>json_get(.metadata, "{filter.key}") '
                    f"{filter.operator.value} {formatted_value}"
                )
            # casting should be fixed
            elif filter.operator in {FilterOperator.IN, FilterOperator.NOT_IN}:
                subclause = (
                    f"{formatted_value} "
                    f"{filter.operator.value} "
                    f'<str>json_get(.metadata, "{filter.key}")'
                )
            # casting should be fixed
            # works only for equality, should be updated to support the rest,
            # example: select all({1, 2, 3, 4} < 4);
            elif filter.operator in {FilterOperator.ANY, FilterOperator.ALL}:
                subclause = (
                    f"{filter.operator.value} ("
                    f'<str>json_get(.metadata, "{filter.key}")'
                    f" = {formatted_value})"
                )

            elif filter.operator == FilterOperator.EXISTS:
                subclause = f'exists <str>json_get(.metadata, "{filter.key}")'

            # casting should be fixed
            # edgeql contains supports different types like range etc which
            # we don't support here
            elif filter.operator == FilterOperator.CONTAINS:
                subclause = (
                    f'contains (<str>json_get(.metadata, "{filter.key}"), '
                    f"{formatted_value})"
                )
            else:
                raise ValueError(f"Unknown operator: {filter.operator}")

        subclauses.append(subclause)

    if filters.condition in {FilterCondition.AND, FilterCondition.OR}:
        filter_clause = f" {filters.condition.value} ".join(subclauses)
        return (
            "(" + filter_clause + ")" if len(subclauses) > 1 else filter_clause
        )
    else:
        raise ValueError(f"Unknown condition: {filters.condition}")
